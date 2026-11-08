//! Write-path tests: allowlist, roles, rate limit, fail-closed audit and error
//! mapping, against a real database (`#[sqlx::test]`) and [`FakeBus`].

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use pipes_core::{
    build_info::BuildInfo,
    bus::{Bus, WriteError},
    fake::FakeBus,
    graph::EndpointKind,
    hub::{Hub, HubConfig},
};
use pipes_db::{AuditQuery, Db, Role};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::{
    AppState,
    auth::{AuthConfig, AuthService},
    router,
    writes::{WritePolicy, WriteRule},
};

const PW: &str = "correct horse battery";

struct App {
    router: Router,
    db: Db,
    bus: Arc<FakeBus>,
    hub: Hub,
}

fn rules(rs: &[&str]) -> Vec<WriteRule> {
    rs.iter().map(|r| r.parse().unwrap()).collect()
}

async fn app(pool: PgPool, interval: Option<Duration>) -> App {
    app_with(
        pool,
        WritePolicy {
            publish: rules(&["door_requests", "fleet/*/estop=admin"]),
            call: rules(&["add_two_ints"]),
            min_interval: interval,
        },
    )
    .await
}

async fn app_with(pool: PgPool, policy: WritePolicy) -> App {
    let db = Db::from_pool(pool);
    for (u, r) in [
        ("admin", Role::Admin),
        ("op", Role::Operator),
        ("viewer", Role::Viewer),
    ] {
        db.create_user(u, PW, r).await.unwrap();
    }
    let bus = FakeBus::new();
    let hub = Hub::new(bus.clone(), HubConfig::default());
    let state = AppState::new(
        BuildInfo::new("t"),
        hub.clone(),
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_auth(AuthService::new(db.clone(), AuthConfig::default()))
    .with_writes(policy);
    App {
        router: router(state),
        db,
        bus,
        hub,
    }
}

async fn token(app: &App, user: &str) -> String {
    let u = app.db.user_by_name(user).await.unwrap().unwrap();
    app.db.create_api_token(u.id, "t", None).await.unwrap().0
}

async fn post(
    app: &App,
    uri: &str,
    tok: &str,
    body: Value,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {tok}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.router.clone().oneshot(req).await.unwrap();
    let (s, h) = (res.status(), res.headers().clone());
    let b = serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
        .unwrap_or(Value::Null);
    (s, h, b)
}

async fn audits(app: &App, action: &str) -> Vec<(String, String, String, Value)> {
    app.db
        .list_audit(&AuditQuery {
            action: Some(action.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.actor, r.target, r.outcome, r.detail))
        .collect()
}

fn subscriber(bus: &FakeBus, topic: &str) {
    bus.add_publisher(topic, "std_msgs/msg/String", "RIHS01_1");
    let mut g = bus.graph();
    if let Some(e) = g.endpoints.iter_mut().rev().find(|e| e.name == topic) {
        e.kind = EndpointKind::Subscription;
    }
    bus.set_graph(g);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn operators_publish_to_allowlisted_topics_and_it_is_audited_first(pool: PgPool) {
    let app = app(pool, None).await;
    subscriber(&app.bus, "/door_requests");
    let op = token(&app, "op").await;
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": {"data": "open"}}),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(
        (b["type_name"].as_str(), b["subscriptions"].as_u64()),
        (Some("std_msgs/msg/String"), Some(1))
    );
    assert_eq!(
        app.bus.published("/door_requests"),
        vec![json!({"data": "open"})]
    );
    // Newest first: the completion (with the type actually used), then the
    // pending intent (with the payload), written before the bus was touched.
    let a = audits(&app, "topic.publish").await;
    assert_eq!(a.len(), 2);
    assert_eq!(
        (a[1].0.as_str(), a[1].1.as_str(), a[1].2.as_str()),
        ("op", "/door_requests", "pending")
    );
    assert_eq!(
        (
            a[1].3["payload"].clone(),
            a[1].3["payload_truncated"].clone()
        ),
        (json!({"data": "open"}), json!(false))
    );
    assert_eq!(a[0].2, "ok");
    assert_eq!(
        (a[0].3["type_name"].as_str(), a[0].3["type_hash"].as_str()),
        (Some("std_msgs/msg/String"), Some("RIHS01_1"))
    );
    let intent = app.db.list_audit(&AuditQuery::default()).await.unwrap()[1].id;
    assert_eq!(a[0].3["intent"], json!(intent));
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn writes_outside_the_allowlist_or_below_the_role_are_refused_and_audited(pool: PgPool) {
    let app = app(pool, None).await;
    subscriber(&app.bus, "/cmd_vel");
    subscriber(&app.bus, "/door_requests");
    subscriber(&app.bus, "/fleet/r1/estop");
    let (op, viewer, admin) = (
        token(&app, "op").await,
        token(&app, "viewer").await,
        token(&app, "admin").await,
    );
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &admin,
        json!({"name": "/cmd_vel", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("not_allowlisted")),
        "not even admins"
    );
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &viewer,
        json!({"name": "/door_requests", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("forbidden"))
    );
    let (s, _, _) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/fleet/r1/estop", "data": {}}),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "admin-only rule");
    let (s, _, _) = post(
        &app,
        "/api/v1/topic/publish",
        &admin,
        json!({"name": "/fleet/r1/estop", "data": {}}),
    )
    .await;
    assert_eq!(s, 200);
    assert!(
        app.bus.published("/cmd_vel").is_empty() && app.bus.published("/door_requests").is_empty()
    );
    let denied: Vec<(String, String)> = audits(&app, "topic.publish")
        .await
        .into_iter()
        .filter(|a| a.2 == "denied")
        .map(|a| (a.0, a.1))
        .collect();
    assert_eq!(
        denied,
        vec![
            ("op".into(), "/fleet/r1/estop".into()),
            ("viewer".into(), "/door_requests".into()),
            ("admin".into(), "/cmd_vel".into())
        ]
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn writes_are_rate_limited_per_user_and_target(pool: PgPool) {
    let app = app(pool, Some(Duration::from_secs(60))).await;
    subscriber(&app.bus, "/door_requests");
    let (op, admin) = (token(&app, "op").await, token(&app, "admin").await);
    let msg = json!({"name": "/door_requests", "data": {}});
    assert_eq!(
        post(&app, "/api/v1/topic/publish", &op, msg.clone())
            .await
            .0,
        200
    );
    let (s, h, b) = post(&app, "/api/v1/topic/publish", &op, msg.clone()).await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("too_many_writes"))
    );
    assert!(
        h[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 50
    );
    assert_eq!(
        post(&app, "/api/v1/topic/publish", &admin, msg).await.0,
        200,
        "another user is not limited"
    );
    assert_eq!(app.bus.published("/door_requests").len(), 2);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn no_audit_entry_means_no_write(pool: PgPool) {
    let app = app(pool.clone(), None).await;
    subscriber(&app.bus, "/door_requests");
    let op = token(&app, "op").await;
    // The token lookup still works; only the audit insert fails.
    sqlx::query("REVOKE INSERT ON pipes.audit_log FROM CURRENT_USER")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE pipes.audit_log ADD CONSTRAINT no_more CHECK (false) NOT VALID")
        .execute(&pool)
        .await
        .unwrap();
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("audit_unavailable"))
    );
    assert!(
        app.bus.published("/door_requests").is_empty(),
        "published without an audit trail"
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn bus_failures_map_to_distinct_errors_and_are_audited(pool: PgPool) {
    let app = app(pool, None).await;
    let op = token(&app, "op").await;
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("no_endpoint"))
    );
    subscriber(&app.bus, "/door_requests");
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": [1]}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_message"))
    );
    for (err, status, code) in [
        (
            WriteError::Bus("down".into()),
            StatusCode::BAD_GATEWAY,
            "bus_error",
        ),
        (
            WriteError::Schema {
                target: "/door_requests".into(),
                reason: "no td".into(),
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "topic_failed",
        ),
    ] {
        app.bus.fail_writes("/door_requests", err);
        let (s, _, b) = post(
            &app,
            "/api/v1/topic/publish",
            &op,
            json!({"name": "/door_requests", "data": {}}),
        )
        .await;
        assert_eq!((s, b["error"].as_str()), (status, Some(code)));
    }
    let a = audits(&app, "topic.publish").await;
    let errors: Vec<&Value> = a.iter().filter(|e| e.2 == "error").map(|e| &e.3).collect();
    assert_eq!(errors.len(), 4);
    assert!(
        errors.iter().all(|d| d["intent"].is_i64()),
        "each error links to its intent entry: {errors:?}"
    );
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "bad name", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_topic"))
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn services_are_called_with_the_same_rules(pool: PgPool) {
    let app = app(pool, None).await;
    app.bus.serve(
        "/add_two_ints",
        "example_interfaces/srv/AddTwoInts",
        |req| {
            let a: i64 = req["a"].as_str().unwrap().parse().unwrap();
            let b: i64 = req["b"].as_str().unwrap().parse().unwrap();
            Ok(json!({"sum": (a + b).to_string()}))
        },
    );
    app.bus
        .serve("/shutdown", "std_srvs/srv/Trigger", |_| Ok(json!({})));
    let (op, viewer) = (token(&app, "op").await, token(&app, "viewer").await);
    let (s, _, b) = post(
        &app,
        "/api/v1/service/call",
        &op,
        json!({"name": "/add_two_ints", "request": {"a": "2", "b": "40"}}),
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(
        (b["type_name"].as_str(), b["response"].clone()),
        (
            Some("example_interfaces/srv/AddTwoInts"),
            json!({"sum": "42"})
        )
    );
    assert_eq!(
        post(
            &app,
            "/api/v1/service/call",
            &op,
            json!({"name": "/shutdown", "request": {}})
        )
        .await
        .0,
        403
    );
    assert_eq!(
        post(
            &app,
            "/api/v1/service/call",
            &viewer,
            json!({"name": "/add_two_ints", "request": {}})
        )
        .await
        .0,
        403
    );
    app.bus
        .fail_writes("/add_two_ints", WriteError::Timeout(Duration::from_secs(5)));
    let (s, _, b) = post(
        &app,
        "/api/v1/service/call",
        &op,
        json!({"name": "/add_two_ints", "request": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::GATEWAY_TIMEOUT, Some("timeout"))
    );
    // add: pending + ok; /shutdown and the viewer: denied; timeout: pending + error.
    assert_eq!(audits(&app, "service.call").await.len(), 6);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn the_policy_is_visible_to_viewers(pool: PgPool) {
    let app = app(pool, Some(Duration::from_millis(250))).await;
    let viewer = token(&app, "viewer").await;
    let req = Request::get("/api/v1/writes")
        .header(header::AUTHORIZATION, format!("Bearer {viewer}"))
        .body(Body::empty())
        .unwrap();
    let res = app.router.clone().oneshot(req).await.unwrap();
    let b: Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(b["writes_enabled"], true);
    assert_eq!(
        b["publish"][1],
        json!({"pattern": "fleet/*/estop", "role": "admin"})
    );
    assert_eq!(b["min_interval_ms"], 250);
    let _ = &app.hub;
}

#[tokio::test]
async fn without_auth_nothing_can_be_written() {
    let bus = FakeBus::new();
    let hub = Hub::new(bus.clone(), HubConfig::default());
    let state = AppState::new(
        BuildInfo::new("t"),
        hub,
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_writes(WritePolicy {
        publish: rules(&["**"]),
        ..WritePolicy::default()
    });
    let req = Request::post("/api/v1/topic/publish")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"name": "/x", "data": {}}).to_string()))
        .unwrap();
    let res = router(state).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(bus.published("/x").is_empty());
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn the_rate_limit_is_per_target_and_per_kind(pool: PgPool) {
    let app = app_with(
        pool,
        WritePolicy {
            publish: rules(&["door_requests", "fleet/*/mode"]),
            call: rules(&["door_requests"]),
            min_interval: Some(Duration::from_secs(60)),
        },
    )
    .await;
    subscriber(&app.bus, "/door_requests");
    subscriber(&app.bus, "/fleet/r1/mode");
    app.bus
        .serve("/door_requests", "std_srvs/srv/Trigger", |_| Ok(json!({})));
    let op = token(&app, "op").await;
    assert_eq!(
        post(
            &app,
            "/api/v1/topic/publish",
            &op,
            json!({"name": "/door_requests", "data": {}})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        post(
            &app,
            "/api/v1/topic/publish",
            &op,
            json!({"name": "/fleet/r1/mode", "data": {}})
        )
        .await
        .0,
        200,
        "another target"
    );
    assert_eq!(
        post(
            &app,
            "/api/v1/service/call",
            &op,
            json!({"name": "/door_requests", "request": {}})
        )
        .await
        .0,
        200,
        "a call is not a publish"
    );
    let (s, _, _) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": {}}),
    )
    .await;
    assert_eq!(s, 429);
    let limited = audits(&app, "topic.publish")
        .await
        .into_iter()
        .filter(|a| a.2 == "denied" && a.3["reason"] == "rate_limited")
        .count();
    assert_eq!(limited, 1, "rate-limited refusals are audited");
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn refusals_cannot_flood_the_audit_log(pool: PgPool) {
    let app = app(pool, None).await;
    let viewer = token(&app, "viewer").await;
    for i in 0..50 {
        let (s, _, _) = post(
            &app,
            "/api/v1/topic/publish",
            &viewer,
            json!({"name": format!("/t{i}"), "data": {}}),
        )
        .await;
        assert_eq!(s, 403);
    }
    let n = audits(&app, "topic.publish").await.len();
    assert_eq!(n, crate::writes::DENIAL_AUDITS_PER_MINUTE as usize);
    // Names are bounded too.
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &viewer,
        json!({"name": format!("/{}", "a".repeat(5000)), "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid_topic"))
    );
    assert!(b["message"].as_str().unwrap().len() < 400);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn call_timeouts_are_capped_below_the_buses_own(pool: PgPool) {
    let app = app(pool, None).await;
    app.bus
        .serve("/add_two_ints", "example_interfaces/srv/AddTwoInts", |_| {
            Ok(json!({"sum": "0"}))
        });
    let op = token(&app, "op").await;
    post(
        &app,
        "/api/v1/service/call",
        &op,
        json!({"name": "/add_two_ints", "request": {}, "timeout_ms": 60000}),
    )
    .await;
    post(
        &app,
        "/api/v1/service/call",
        &op,
        json!({"name": "/add_two_ints", "request": {}, "timeout_ms": 1500}),
    )
    .await;
    assert_eq!(
        app.bus.call_timeouts(),
        vec![crate::writes::MAX_CALL_TIMEOUT, Duration::from_millis(1500)]
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn ambiguous_targets_are_conflicts(pool: PgPool) {
    let app = app(pool, None).await;
    subscriber(&app.bus, "/door_requests");
    let op = token(&app, "op").await;
    app.bus
        .fail_writes("/door_requests", WriteError::Conflict("two types".into()));
    let (s, _, b) = post(
        &app,
        "/api/v1/topic/publish",
        &op,
        json!({"name": "/door_requests", "data": {}}),
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::CONFLICT, Some("type_conflict"))
    );
}
