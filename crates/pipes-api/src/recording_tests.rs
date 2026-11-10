//! Recording API tests: rules, recent records, stored records through the
//! database sink, roles, and the dashboard page.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use pipes_core::{
    build_info::BuildInfo,
    fake::FakeBus,
    hub::{Hub, HubConfig},
    record::{ConsoleSink, RecordSink},
    recorder::{Recorder, RecorderConfig, Sinks},
};
use pipes_db::{Db, Role};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::{
    AppState,
    auth::{AuthConfig, AuthService},
    recording::DbSink,
    router,
};

struct App {
    router: Router,
    bus: Arc<FakeBus>,
    hub: Hub,
    rec: Recorder,
}

fn rules() -> Vec<pipes_core::record::RecordRule> {
    serde_json::from_value(json!([
        {"name": "chatter", "match": "chatter", "kind": "topic", "sinks": ["console"]}
    ]))
    .unwrap()
}

fn app(db: Option<Db>, auth: bool) -> App {
    let bus = FakeBus::new();
    let hub = Hub::new(
        bus.clone(),
        HubConfig {
            max_rate_hz: None,
            ..HubConfig::default()
        },
    );
    let database = db.clone().map(|db| {
        let (sink, _task) = DbSink::spawn(db, CancellationToken::new());
        sink as Arc<dyn RecordSink>
    });
    let rec = Recorder::new(
        hub.clone(),
        RecorderConfig::default(),
        Sinks {
            console: Arc::new(ConsoleSink),
            database,
        },
        rules(),
    )
    .unwrap();
    let mut state = AppState::new(
        BuildInfo::new("t"),
        hub.clone(),
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_recorder(rec.clone());
    state = match (db, auth) {
        (Some(db), true) => state.with_auth(AuthService::new(db, AuthConfig::default())),
        (Some(db), false) => state.with_database(db).with_demo(),
        (None, _) => state.with_demo(),
    };
    App {
        router: router(state),
        bus,
        hub,
        rec,
    }
}

async fn send(
    app: &App,
    method: Method,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let body = match body {
        Some(b) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let res = app
        .router
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, v)
}

/// Open the recorder's streams and deliver one message to `/chatter`.
async fn record_one(app: &App, data: &str) {
    app.bus
        .add_publisher("/chatter", "std_msgs/msg/String", "RIHS01_1");
    app.hub.tick();
    app.rec.tick();
    for _ in 0..500 {
        if app.bus.live_subscriptions("/chatter") > 0 {
            app.bus.publish("/chatter", json!({"data": data}));
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("the recorder never subscribed");
}

async fn until<T>(what: &str, mut f: impl AsyncFnMut() -> Option<T>) -> T {
    for _ in 0..500 {
        if let Some(v) = f().await {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn rules_can_be_read_and_replaced() {
    let app = app(None, false);
    let (s, v) = send(&app, Method::GET, "/api/v1/recording", None, None).await;
    assert_eq!(s, 200);
    assert_eq!(v["rules"][0]["name"], "chatter");
    assert_eq!(v["rules"][0]["parts"], json!(["message"]), "normalized");
    assert_eq!(v["database"], json!(false));
    let new = json!({"rules": [
        {"name": "nav", "match": "/*/navigate_to_pose", "kind": "action", "parts": ["goal", "result"], "sinks": ["console"]}
    ]});
    let (s, v) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        None,
        Some(new),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(v[0]["name"], "nav");
    assert_eq!(app.rec.rules()[0].name, "nav");
    for (bad, needle) in [
        (
            json!({"rules": [{"name": "d", "match": "x", "kind": "topic", "sinks": ["database"]}]}),
            "none is configured",
        ),
        (
            json!({"rules": [{"name": "d", "match": "x", "kind": "topic", "parts": ["goal"], "sinks": ["console"]}]}),
            "no `goal` part",
        ),
    ] {
        let (s, v) = send(
            &app,
            Method::PUT,
            "/api/v1/recording/rules",
            None,
            Some(bad),
        )
        .await;
        assert_eq!(
            (s, v["error"].as_str()),
            (StatusCode::BAD_REQUEST, Some("invalid_rules")),
            "{v}"
        );
        assert!(v["message"].as_str().unwrap().contains(needle), "{v}");
    }
    let (s, v) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        None,
        Some(json!({"rules": [{"name": "x"}]})),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::BAD_REQUEST,
        "malformed rules are a JSON error: {v}"
    );
    assert_eq!(
        app.rec.rules()[0].name,
        "nav",
        "refused changes change nothing"
    );
}

#[tokio::test]
async fn recent_records_work_without_a_database_and_stored_ones_need_one() {
    let app = app(None, false);
    record_one(&app, "hello").await;
    let v = until("a recent record", async || {
        let (_, v) = send(
            &app,
            Method::GET,
            "/api/v1/recording/recent?limit=5",
            None,
            None,
        )
        .await;
        (v.as_array()?.len() == 1).then_some(v)
    })
    .await;
    assert_eq!(
        (&v[0]["rule"], &v[0]["name"], &v[0]["part"], &v[0]["data"]),
        (
            &json!("chatter"),
            &json!("/chatter"),
            &json!("message"),
            &json!({"data": "hello"})
        )
    );
    let (s, v) = send(&app, Method::GET, "/api/v1/records", None, None).await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("no_database"))
    );
    let (s, v) = send(&app, Method::GET, "/api/v1/recording", None, None).await;
    assert_eq!(s, 200);
    assert_eq!(v["streams"][0]["observes"], "/chatter");
    assert_eq!(v["streams"][0]["recorded"], 1);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn database_rules_store_records_that_can_be_queried(pool: PgPool) {
    let db = Db::from_pool(pool);
    let app = app(Some(db.clone()), false);
    let (s, v) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        None,
        Some(json!({"rules": [{"name": "chatter", "match": "chatter", "kind": "topic", "sinks": ["database"]}]})),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    record_one(&app, "stored").await;
    let v = until("a stored record", async || {
        let (_, v) = send(
            &app,
            Method::GET,
            "/api/v1/records?name=/chatter&part=message",
            None,
            None,
        )
        .await;
        (v.as_array()?.len() == 1).then_some(v)
    })
    .await;
    assert_eq!(v[0]["data"], json!({"data": "stored"}));
    assert_eq!(v[0]["kind"], "topic");
    let (s, v) = send(&app, Method::GET, "/api/v1/records?name=/other", None, None).await;
    assert_eq!((s, v.as_array().map(Vec::len)), (StatusCode::OK, Some(0)));
    let (s, v) = send(
        &app,
        Method::GET,
        "/api/v1/records?goal_id=nope",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let (s, _) = send(
        &app,
        Method::GET,
        "/api/v1/records?since=yesterday",
        None,
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, v) = send(&app, Method::GET, "/api/v1/records?name=%00", None, None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "NUL can't reach Postgres: {v}");
    let (s, _) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        None,
        Some(json!({"rules": [{"name": "x", "match": "x", "kind": "topic", "sinks": []}]})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // The rule change is in the audit log, by the demo user, and so is the
    // refused attempt.
    let audit = db
        .list_audit(&pipes_db::AuditQuery {
            action: Some("recording.update".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let got: Vec<(&str, &str, &str)> = audit
        .iter()
        .map(|a| (a.actor.as_str(), a.auth_method.as_str(), a.outcome.as_str()))
        .collect();
    assert_eq!(
        got,
        [("anonymous", "demo", "error"), ("anonymous", "demo", "ok")]
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn with_auth_viewers_read_recordings_but_only_admins_change_rules(pool: PgPool) {
    let db = Db::from_pool(pool);
    let app = app(Some(db.clone()), true);
    let mut tokens = Vec::new();
    for (u, r) in [
        ("viewer", Role::Viewer),
        ("op", Role::Operator),
        ("admin", Role::Admin),
    ] {
        let user = db.create_user(u, "correct horse battery", r).await.unwrap();
        tokens.push(db.create_api_token(user.id, "t", None).await.unwrap().0);
    }
    let (s, _) = send(&app, Method::GET, "/api/v1/recording", None, None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, _) = send(
        &app,
        Method::GET,
        "/api/v1/recording",
        Some(&tokens[0]),
        None,
    )
    .await;
    assert_eq!(s, 200);
    let (s, _) = send(&app, Method::GET, "/api/v1/records", Some(&tokens[0]), None).await;
    assert_eq!(s, 200);
    let body = json!({"rules": []});
    for t in &tokens[..2] {
        let (s, v) = send(
            &app,
            Method::PUT,
            "/api/v1/recording/rules",
            Some(t),
            Some(body.clone()),
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    }
    let (s, v) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        Some(&tokens[2]),
        Some(body),
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert!(app.rec.rules().is_empty());
    let audit = db
        .list_audit(&pipes_db::AuditQuery {
            action: Some("recording.update".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let outcomes: Vec<(&str, &str)> = audit
        .iter()
        .map(|a| (a.actor.as_str(), a.outcome.as_str()))
        .collect();
    assert_eq!(
        outcomes,
        [("admin", "ok"), ("op", "denied"), ("viewer", "denied")]
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn rule_changes_are_refused_when_the_audit_log_cannot_be_written(pool: PgPool) {
    let db = Db::from_pool(pool.clone());
    let app = app(Some(db), false);
    sqlx::query("DROP TABLE pipes.audit_log")
        .execute(&pool)
        .await
        .unwrap();
    let (s, v) = send(
        &app,
        Method::PUT,
        "/api/v1/recording/rules",
        None,
        Some(json!({"rules": []})),
    )
    .await;
    assert_eq!(
        (s, v["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("audit_unavailable"))
    );
    assert_eq!(app.rec.rules().len(), 1, "unchanged");
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn the_database_sink_writes_what_is_buffered_when_stopping(pool: PgPool) {
    let db = Db::from_pool(pool);
    let stop = CancellationToken::new();
    let (sink, task) = DbSink::spawn(db.clone(), stop.clone());
    let rec = |i: usize| {
        Arc::new(pipes_core::record::Record {
            at: time::OffsetDateTime::now_utc(),
            rule: "r".into(),
            kind: pipes_core::record::TargetKind::Topic,
            name: format!("/t{i}"),
            part: pipes_core::record::Part::Message,
            type_name: "std_msgs/msg/String".into(),
            goal_id: None,
            data: Some(json!({"i": i})),
        })
    };
    for i in 0..1200 {
        sink.accept(&rec(i));
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pipes.records")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 1200, "nothing buffered is lost on shutdown");
    assert_eq!(sink.dropped(), 0);
    sink.accept(&rec(0));
    assert_eq!(
        sink.dropped(),
        1,
        "after the writer stops, records are dropped and counted"
    );
}

#[tokio::test]
async fn the_recording_page_shows_rules_streams_and_records() {
    let app = app(None, false);
    record_one(&app, "on the page").await;
    let (s, v) = send(&app, Method::GET, "/ui/recording", None, None).await;
    assert_eq!(s, 200);
    let html = v.as_str().unwrap();
    assert!(html.contains("demo mode"), "the demo banner shows");
    assert!(
        html.contains("chatter"),
        "the rules are in the editor: {html}"
    );
    let html = until("the record on the page", async || {
        let (_, v) = send(&app, Method::GET, "/ui/fragments/recording", None, None).await;
        let h = v.as_str()?.to_owned();
        h.contains("on the page").then_some(h)
    })
    .await;
    assert!(html.contains("/chatter"));
}
