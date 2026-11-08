//! In-process API tests against [`FakeBus`]: every route, every error path, SSE
//! framing and shutdown, the OpenAPI document, and the dashboard.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use pipes_core::{
    bus::BusError,
    fake::FakeBus,
    hub::{Hub, HubConfig},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{router, test_support::state_with_config};

const TY: &str = "std_msgs/msg/String";
const H1: &str = "RIHS01_1111";

struct App {
    router: Router,
    bus: Arc<FakeBus>,
    hub: Hub,
    shutdown: tokio_util::sync::CancellationToken,
}

fn app_with(cfg: HubConfig) -> App {
    let bus = FakeBus::new();
    let (state, hub) = state_with_config(bus.clone(), cfg);
    let shutdown = state.shutdown().clone();
    App {
        router: router(state),
        bus,
        hub,
        shutdown,
    }
}

fn app() -> App {
    app_with(HubConfig::default())
}

async fn get(app: &App, uri: &str) -> (StatusCode, Bytes) {
    let res = app
        .router
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    (status, res.into_body().collect().await.unwrap().to_bytes())
}

async fn get_json(app: &App, uri: &str) -> (StatusCode, Value) {
    let (status, body) = get(app, uri).await;
    let json = serde_json::from_slice(&body).unwrap_or_else(|e| {
        panic!(
            "{uri} returned non-JSON ({e}): {}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, json)
}

async fn get_html(app: &App, uri: &str) -> (StatusCode, String) {
    let (status, body) = get(app, uri).await;
    (status, String::from_utf8(body.to_vec()).unwrap())
}

/// Publish `data` once the hub has subscribed to `topic`.
fn publish_when_subscribed(bus: &Arc<FakeBus>, topic: &'static str, data: Value) {
    let bus = bus.clone();
    tokio::spawn(async move {
        for _ in 0..500 {
            if bus.live_subscriptions(topic) > 0 {
                bus.publish(topic, data);
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("hub never subscribed to {topic}");
    });
}

// ---- OpenAPI ----------------------------------------------------------------

#[tokio::test]
async fn openapi_documents_every_api_route() {
    let app = app();
    let (status, doc) = get_json(&app, "/openapi.json").await;
    assert_eq!(status, 200);
    assert_eq!(doc["info"]["title"], "rmf-pipes");
    assert_eq!(doc["info"]["version"], env!("CARGO_PKG_VERSION"));
    let mut paths: Vec<&str> = doc["paths"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec![
            "/api/v1/audit",
            "/api/v1/auth/login",
            "/api/v1/auth/logout",
            "/api/v1/auth/me",
            "/api/v1/auth/password",
            "/api/v1/auth/tokens",
            "/api/v1/auth/tokens/{id}",
            "/api/v1/graph",
            "/api/v1/service/call",
            "/api/v1/topic",
            "/api/v1/topic/latest",
            "/api/v1/topic/publish",
            "/api/v1/topic/schema",
            "/api/v1/topic/stream",
            "/api/v1/topics",
            "/api/v1/users",
            "/api/v1/users/{username}",
            "/api/v1/writes",
            "/healthz",
            "/metrics",
        ]
    );
    for schema in [
        "Sample",
        "Schema",
        "FieldType",
        "TopicView",
        "TopicStatus",
        "GraphView",
        "ApiError",
    ] {
        assert!(
            doc["components"]["schemas"].get(schema).is_some(),
            "missing component schema {schema}"
        );
    }
}

#[tokio::test]
async fn every_documented_path_is_routed() {
    let app = app();
    let (_, doc) = get_json(&app, "/openapi.json").await;
    for path in doc["paths"].as_object().unwrap().keys() {
        if path.ends_with("/stream") {
            continue; // never completes; covered by the SSE tests
        }
        let (_, body) = get(&app, &format!("{path}?name=/x&wait_ms=1")).await;
        let text = String::from_utf8_lossy(&body);
        assert!(
            !text.contains("\"no_route\""),
            "{path} is documented but not routed"
        );
    }
    let (status, body) = get_json(&app, "/api/v1/nope").await;
    assert_eq!(
        (status, body["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("no_route"))
    );
}

#[tokio::test]
async fn swagger_ui_is_served_from_the_binary() {
    let app = app();
    let (status, html) = get_html(&app, "/docs/").await;
    assert_eq!(status, 200);
    assert!(html.contains("swagger"), "{html}");
    assert_no_external_urls("/docs/", &html);
}

// ---- graph and topics -------------------------------------------------------

#[tokio::test]
async fn graph_lists_topics_services_and_mismatches() {
    let app = app();
    app.bus.add_publisher("/ok", TY, H1);
    app.bus.add_publisher("/clash", TY, H1);
    app.bus
        .add_publisher("/clash", "std_msgs/msg/Int32", "RIHS01_2");
    app.hub.tick();
    let (status, g) = get_json(&app, "/api/v1/graph").await;
    assert_eq!(status, 200);
    assert_eq!(g["mismatches"], json!(["/clash"]));
    let names: Vec<&str> = g["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["/clash", "/ok"]);
    assert_eq!(g["topics"][0]["mismatch"], true);
    assert_eq!(
        (
            g["topics"][0]["publishers"].as_u64(),
            g["topics"][0]["subscriptions"].as_u64()
        ),
        (Some(2), Some(0))
    );
}

#[tokio::test]
async fn topic_info_combines_graph_and_subscription() {
    let app = app();
    let (status, err) = get_json(&app, "/api/v1/topic?name=/ghost").await;
    assert_eq!(
        (status, err["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("unknown_topic"))
    );

    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let (status, info) = get_json(&app, "/api/v1/topic?name=/chatter").await;
    assert_eq!(status, 200);
    assert_eq!(info["graph"]["types"][0]["type_name"], TY);
    assert_eq!(info["subscription"], Value::Null);
}

// ---- latest -------------------------------------------------------------------

#[tokio::test]
async fn latest_subscribes_on_demand_and_waits_for_the_first_sample() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    publish_when_subscribed(&app.bus, "/chatter", json!({"data": "hi"}));
    let (status, s) = get_json(&app, "/api/v1/topic/latest?name=/chatter&wait_ms=2000").await;
    assert_eq!(status, 200, "{s}");
    assert_eq!(s["data"], json!({"data": "hi"}));
    assert_eq!(s["topic"], "/chatter");
    assert_eq!(s["type_hash"], H1);
    assert!(s["received_at"].as_str().unwrap().ends_with('Z'));

    // Served from the warm subscription without resubscribing.
    let (status, _) = get_json(&app, "/api/v1/topic/latest?name=/chatter").await;
    assert_eq!(status, 200);
    assert_eq!(app.bus.subscribe_calls("/chatter"), 1);
}

#[tokio::test]
async fn latest_reports_each_failure_distinctly() {
    let app = app();
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=chatter").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_topic"))
    );

    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/nobody").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("no_publisher"))
    );
    assert!(
        app.hub.topic("/nobody").is_none(),
        "a 404 must not leave a subscription behind"
    );

    app.bus.add_publisher("/silent", TY, H1);
    app.bus.add_publisher("/opaque", TY, H1);
    app.bus.fail_subscribe(
        "/opaque",
        BusError::Schema {
            topic: "/opaque".into(),
            reason: "no type description".into(),
        },
    );
    app.hub.tick();
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/silent&wait_ms=50").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::GATEWAY_TIMEOUT, Some("no_sample"))
    );
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/opaque&wait_ms=2000").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("topic_failed"))
    );
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .contains("no type description")
    );
}

/// With paused time, an uncapped wait of ~11 days would show up as elapsed
/// (auto-advanced) time; the cap keeps it at `MAX_WAIT`.
#[tokio::test(start_paused = true)]
async fn wait_ms_is_capped() {
    let app = app();
    app.bus.add_publisher("/silent", TY, H1);
    app.hub.tick();
    let started = tokio::time::Instant::now();
    let (s, _) = get_json(&app, "/api/v1/topic/latest?name=/silent&wait_ms=999999999").await;
    assert_eq!(s, StatusCode::GATEWAY_TIMEOUT);
    let elapsed = started.elapsed();
    assert!(
        elapsed >= crate::api::MAX_WAIT && elapsed < crate::api::MAX_WAIT + Duration::from_secs(1),
        "waited {elapsed:?}"
    );
}

// ---- schema -------------------------------------------------------------------

#[tokio::test]
async fn schema_is_served_once_the_subscription_is_active() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let (status, s) = get_json(&app, "/api/v1/topic/schema?name=/chatter").await;
    assert_eq!(status, 200, "{s}");
    assert_eq!(s["type_name"], TY);
    assert_eq!(
        s["fields"][0],
        json!({"name": "data", "type": {"kind": "string", "max_len": null}})
    );
}

// ---- SSE ------------------------------------------------------------------------

/// Read SSE frames from `body` until `n` named events have been seen.
async fn sse_events(body: &mut Body, n: usize) -> Vec<(String, Value)> {
    let mut buf = String::new();
    let mut events = Vec::new();
    while events.len() < n {
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
            .await
            .expect("no SSE frame within 5s")
            .expect("stream ended early")
            .unwrap();
        let Ok(data) = frame.into_data() else {
            continue;
        };
        buf.push_str(std::str::from_utf8(&data).unwrap());
        while let Some(end) = buf.find("\n\n") {
            let block: String = buf.drain(..end + 2).collect();
            let mut name = None;
            let mut data = String::new();
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    name = Some(v.to_owned());
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data.push_str(v);
                }
            }
            if let Some(name) = name {
                events.push((name, serde_json::from_str(&data).unwrap()));
            }
        }
    }
    events
}

#[tokio::test]
async fn stream_sends_status_then_samples_and_ends_on_shutdown() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let res = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/v1/topic/stream?name=/chatter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    let mut body = res.into_body();

    let first = sse_events(&mut body, 1).await;
    assert_eq!(first[0].0, "status");
    publish_when_subscribed(&app.bus, "/chatter", json!({"data": "live"}));
    let mut seen = Vec::new();
    while !seen.iter().any(|(n, _): &(String, Value)| n == "sample") {
        seen.extend(sse_events(&mut body, 1).await);
    }
    let (_, sample) = seen.iter().find(|(n, _)| n == "sample").unwrap();
    assert_eq!(sample["data"], json!({"data": "live"}));
    assert!(
        seen.iter()
            .any(|(n, v)| n == "status" && v["state"] == "active")
    );
    assert_eq!(app.hub.topic("/chatter").unwrap().watchers, 1);

    app.shutdown.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match body.frame().await {
                None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("{e}"),
            }
        }
    })
    .await;
    assert!(end.is_ok(), "SSE stream did not end on shutdown");
    drop(body);
    tokio::task::yield_now().await;
    assert_eq!(
        app.hub.topic("/chatter").unwrap().watchers,
        0,
        "the watch must be released"
    );
}

#[tokio::test]
async fn stream_starts_with_the_latest_sample_when_there_is_one() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    publish_when_subscribed(&app.bus, "/chatter", json!({"data": "earlier"}));
    let (status, _) = get_json(&app, "/api/v1/topic/latest?name=/chatter&wait_ms=2000").await;
    assert_eq!(status, 200);

    let res = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/v1/topic/stream?name=/chatter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let ev = sse_events(&mut res.into_body(), 2).await;
    assert_eq!(ev[0].0, "status");
    assert_eq!(
        (ev[1].0.as_str(), &ev[1].1["data"]),
        ("sample", &json!({"data": "earlier"}))
    );
}

#[tokio::test]
async fn stream_reports_lag_for_slow_clients() {
    let app = app_with(HubConfig {
        watcher_buffer: 2,
        max_rate_hz: None,
        ..HubConfig::default()
    });
    app.bus.add_publisher("/fast", TY, H1);
    app.hub.tick();
    let res = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/v1/topic/stream?name=/fast")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = res.into_body();
    sse_events(&mut body, 1).await; // initial status
    for _ in 0..500 {
        if app.bus.live_subscriptions("/fast") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    for i in 0..20 {
        app.bus.publish("/fast", json!({"data": i}));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while app.hub.topic("/fast").unwrap().stats.delivered < 20 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let mut seen = Vec::new();
    while !seen.iter().any(|(n, _): &(String, Value)| n == "lagged") {
        seen.extend(sse_events(&mut body, 1).await);
        assert!(seen.len() < 30, "no lagged event: {seen:?}");
    }
    let (_, lag) = seen.iter().find(|(n, _)| n == "lagged").unwrap();
    assert!(lag["skipped"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn stream_rejects_invalid_names() {
    let app = app();
    let (s, e) = get_json(&app, "/api/v1/topic/stream?name=no-slash").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_topic"))
    );
}

// ---- dashboard --------------------------------------------------------------------

/// Fail if `html` (markup, inline CSS or inline JS) references anything that
/// looks like a network location: `http(s)://host`, protocol-relative `//host`
/// or `@import`. The dashboard must work air-gapped.
fn assert_no_external_urls(page: &str, html: &str) {
    assert!(!html.contains("@import"), "{page} uses @import");
    let bytes = html.as_bytes();
    for (i, _) in html.match_indices("//") {
        // `//host.tld` with a dotted, hostname-like run right after the slashes.
        let rest = &html[i + 2..];
        let host: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '.')
            .collect();
        let looks_like_host = host.contains('.') && host.split('.').all(|p| !p.is_empty());
        let after_scheme = i > 0 && bytes[i - 1] == b':';
        assert!(
            !(looks_like_host || (after_scheme && !host.is_empty())),
            "{page} references an external location near {:?}; the dashboard must work air-gapped",
            &html[i.saturating_sub(20)..(i + 40).min(html.len())]
        );
    }
}

#[tokio::test]
async fn dashboard_pages_render_and_are_self_contained() {
    let app = app();
    app.bus.add_publisher("/fleet/r1/state", TY, H1);
    app.bus.add_publisher("/clash", TY, H1);
    app.bus
        .add_publisher("/clash", "std_msgs/msg/Int32", "RIHS01_2");
    app.hub.tick();

    let (s, index) = get_html(&app, "/").await;
    assert_eq!(s, 200);
    assert!(index.contains(crate::ui::HTMX_PATH));
    assert!(index.contains("testdistro"));
    assert_no_external_urls("/", &index);

    let (s, overview) = get_html(&app, "/ui/fragments/overview").await;
    assert_eq!(s, 200);
    assert!(overview.contains("/fleet/r1/state"));
    assert!(
        overview.contains("/ui/topic?name=%2Ffleet%2Fr1%2Fstate"),
        "{overview}"
    );
    assert!(overview.contains("type mismatch"));

    let (s, page) = get_html(&app, "/ui/topic?name=/fleet/r1/state").await;
    assert_eq!(s, 200);
    assert!(page.contains(r#"data-topic="/fleet/r1/state""#), "{page}");
    assert!(page.contains("/api/v1/topic/stream"));
    assert_no_external_urls("/ui/topic", &page);

    let (s, info) = get_html(&app, "/ui/fragments/topic?name=/clash").await;
    assert_eq!(s, 200);
    assert!(info.contains("Type mismatch"), "{info}");
    assert!(info.contains("/fake_node"));

    let (s, _) = get(&app, "/ui/topic?name=bad%20name").await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn vendored_htmx_is_served() {
    let app = app();
    let res = app
        .router
        .clone()
        .oneshot(
            Request::get(crate::ui::HTMX_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert!(
        res.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/javascript")
    );
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert!(
        body.len() > 40_000 && body.starts_with(b"var htmx="),
        "unexpected htmx asset"
    );
}

// ---- review hardening (phase 2) ---------------------------------------------------

#[tokio::test]
async fn external_url_check_catches_what_it_should() {
    for bad in [
        r#"<script src="https://cdn.example.com/x.js"></script>"#,
        r#"<link href="//fonts.googleapis.com/css">"#,
        r#"new EventSource("https://events.example.org/s")"#,
        "<style>@import url(x.css)</style>",
    ] {
        let r = std::panic::catch_unwind(|| assert_no_external_urls("t", bad));
        assert!(r.is_err(), "missed: {bad}");
    }
    // Comments and relative paths are fine.
    assert_no_external_urls("t", "// a comment\n<a href=\"/ui/topic?name=%2Fa\">x</a>");
}

#[tokio::test]
async fn swagger_ui_never_contacts_the_online_validator() {
    let app = app();
    let (status, js) = get_html(&app, "/docs/swagger-initializer.js").await;
    assert_eq!(status, 200);
    assert!(
        js.contains(r#""validatorUrl": "none""#) || js.contains(r#""validatorUrl":"none""#),
        "{js}"
    );
    assert!(js.contains("/openapi.json"), "{js}");
}

#[tokio::test]
async fn latest_on_an_already_failed_topic_is_503_not_504() {
    let app = app();
    app.bus.add_publisher("/opaque", TY, H1);
    app.bus.fail_subscribe(
        "/opaque",
        BusError::Schema {
            topic: "/opaque".into(),
            reason: "no type description".into(),
        },
    );
    app.hub.tick();
    let (s, _) = get_json(&app, "/api/v1/topic/latest?name=/opaque&wait_ms=2000").await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    // The entry is now Failed (until retried); asking again must say so at once.
    let started = tokio::time::Instant::now();
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/opaque&wait_ms=2000").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("topic_failed"))
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "should not wait for wait_ms"
    );
}

#[tokio::test]
async fn a_watched_topic_without_publishers_is_404() {
    let app = app();
    let _w = app.hub.watch("/ghost").unwrap(); // e.g. an SSE client waiting for it
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/ghost&wait_ms=2000").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("no_publisher"))
    );
}

#[tokio::test]
async fn schema_reports_failure_and_timeout() {
    let app = app();
    app.bus.add_publisher("/opaque", TY, H1);
    app.bus.add_publisher("/slow", TY, H1);
    app.bus.fail_subscribe(
        "/opaque",
        BusError::Schema {
            topic: "/opaque".into(),
            reason: "refused".into(),
        },
    );
    app.bus.hang_subscribe("/slow");
    app.hub.tick();
    let (s, e) = get_json(&app, "/api/v1/topic/schema?name=/opaque&wait_ms=2000").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("topic_failed"))
    );
    let (s, e) = get_json(&app, "/api/v1/topic/schema?name=/slow&wait_ms=50").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::GATEWAY_TIMEOUT, Some("no_schema"))
    );
}

#[tokio::test]
async fn query_and_method_errors_are_json() {
    let app = app();
    for uri in [
        "/api/v1/topic/latest",
        "/api/v1/topic/latest?name=/a&wait_ms=abc",
        "/api/v1/topic/latest?name=/a&wait_ms=-1",
        "/api/v1/topic/stream",
        "/ui/topic",
    ] {
        let (s, e) = get_json(&app, uri).await;
        assert_eq!(
            (s, e["error"].as_str()),
            (StatusCode::BAD_REQUEST, Some("invalid_query")),
            "{uri}"
        );
    }
    let (s, e) = get_json(&app, "/api/v1/topic?name=nope").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_topic"))
    );
    let res = app
        .router
        .clone()
        .oneshot(Request::post("/api/v1/graph").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    let body: Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"], "method_not_allowed");
}

#[tokio::test]
async fn limits_on_streams_and_on_demand_topics_are_503() {
    let bus = FakeBus::new();
    let (state, hub) = crate::test_support::state_with_config(
        bus.clone(),
        HubConfig {
            max_on_demand_topics: 1,
            ..HubConfig::default()
        },
    );
    let router = router(state.with_max_streams(1));
    let app = App {
        router,
        bus,
        hub,
        shutdown: tokio_util::sync::CancellationToken::new(),
    };
    let first = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/v1/topic/stream?name=/a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    let (s, e) = get_json(&app, "/api/v1/topic/stream?name=/b").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("too_many_streams"))
    );
    app.bus.add_publisher("/b", TY, H1);
    app.hub.tick();
    let (s, e) = get_json(&app, "/api/v1/topic/latest?name=/b").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("too_many_topics"))
    );
    // Dropping the first stream frees its slot.
    drop(first);
    tokio::task::yield_now().await;
    let again = app
        .router
        .clone()
        .oneshot(
            Request::get("/api/v1/topic/stream?name=/a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(again.status(), 200);
}

async fn open_stream(app: &App, topic: &str) -> Body {
    let res = app
        .router
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/topic/stream?name={topic}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.into_body()
}

/// Read until the body ends, returning the events seen.
async fn drain_events(body: &mut Body) -> Vec<(String, Value)> {
    let mut all = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), sse_events(body, 1)).await {
            Ok(ev) => all.extend(ev),
            Err(_) => panic!("stream did not end; saw {all:?}"),
        }
        if all
            .last()
            .is_some_and(|(n, v)| n == "status" && v["state"] == "closed")
        {
            break;
        }
    }
    let end = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("no EOF after closed");
    assert!(end.is_none(), "data after the final closed status");
    all
}

#[tokio::test]
async fn stream_ends_with_closed_when_the_hub_closes_the_topic() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let mut body = open_stream(&app, "/chatter").await;
    sse_events(&mut body, 1).await;
    app.hub.close_all(); // not the server shutdown token
    let events = drain_events(&mut body).await;
    assert_eq!(events.last().unwrap().1["state"], "closed");
}

#[tokio::test]
async fn stream_sends_a_final_closed_status_on_shutdown() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let mut body = open_stream(&app, "/chatter").await;
    sse_events(&mut body, 1).await;
    app.shutdown.cancel();
    let events = drain_events(&mut body).await;
    assert_eq!(
        events.last().unwrap(),
        &("status".to_owned(), json!({"state": "closed"}))
    );
}

#[tokio::test]
async fn client_disconnect_releases_the_watch() {
    let app = app();
    app.bus.add_publisher("/chatter", TY, H1);
    app.hub.tick();
    let mut body = open_stream(&app, "/chatter").await;
    sse_events(&mut body, 1).await;
    assert_eq!(app.hub.topic("/chatter").unwrap().watchers, 1);
    drop(body); // the client goes away; no shutdown
    tokio::task::yield_now().await;
    assert_eq!(app.hub.topic("/chatter").unwrap().watchers, 0);
}

#[tokio::test(start_paused = true)]
async fn stream_sends_keep_alive_comments() {
    let app = app();
    app.bus.add_publisher("/quiet", TY, H1);
    app.hub.tick();
    let mut body = open_stream(&app, "/quiet").await;
    sse_events(&mut body, 1).await;
    let mut text = String::new();
    let deadline = tokio::time::Instant::now() + crate::api::KEEP_ALIVE * 2;
    while !text.lines().any(|l| l.starts_with(':')) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no keep-alive within 2 intervals: {text:?}"
        );
        if let Ok(Some(Ok(frame))) =
            tokio::time::timeout(Duration::from_secs(1), body.frame()).await
            && let Ok(data) = frame.into_data()
        {
            text.push_str(std::str::from_utf8(&data).unwrap());
        }
    }
}

#[tokio::test]
async fn hostile_topic_names_from_the_graph_are_escaped_and_not_linked() {
    let app = app();
    let evil = r#"/a"><script>alert(1)</script>"#;
    app.bus.add_publisher(evil, TY, H1);
    app.hub.tick();
    let (s, html) = get_html(&app, "/ui/fragments/overview").await;
    assert_eq!(s, 200);
    assert!(!html.contains("<script>alert(1)"), "unescaped: {html}");
    assert!(html.contains("&#60;script&#62;"), "{html}");
    assert!(
        !html.contains("/ui/topic?name=%2Fa%22"),
        "invalid names must not be linked: {html}"
    );
}
