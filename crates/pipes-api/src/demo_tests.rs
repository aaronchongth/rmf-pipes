//! Demo mode: no authentication, every caller is the demo user (admin role),
//! writes and audits work with or without a database, and the CSRF rule
//! covers every non-GET request.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use pipes_core::{
    build_info::BuildInfo,
    fake::FakeBus,
    graph::EndpointKind,
    hub::{Hub, HubConfig},
};
use pipes_db::{AuditQuery, Db};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::{AppState, router, writes::WritePolicy};

fn demo_app(db: Option<Db>) -> (Router, Arc<FakeBus>) {
    let bus = FakeBus::new();
    let hub = Hub::new(bus.clone(), HubConfig::default());
    let mut state = AppState::new(
        BuildInfo::new("t"),
        hub,
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_writes(WritePolicy {
        publish: vec!["door_requests".parse().unwrap()],
        call: vec![],
        min_interval: None,
    })
    .with_demo();
    if let Some(db) = db {
        state = state.with_database(db);
    }
    bus.add_publisher("/door_requests", "std_msgs/msg/String", "RIHS01_1");
    let mut g = pipes_core::bus::Bus::graph(&*bus);
    for e in &mut g.endpoints {
        e.kind = EndpointKind::Subscription;
    }
    bus.set_graph(g);
    (router(state), bus)
}

async fn send(
    app: &Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header(header::CONTENT_TYPE, ct);
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

const PUBLISH: &str = r#"{"name": "/door_requests", "data": {"data": "open"}}"#;

#[tokio::test]
async fn demo_callers_write_without_credentials_and_without_a_database() {
    let (app, bus) = demo_app(None);
    let (s, me) = send(&app, Method::GET, "/api/v1/auth/me", None, "").await;
    assert_eq!(s, 200);
    assert_eq!(
        (
            &me["demo"],
            &me["auth_enabled"],
            &me["role"],
            &me["auth_method"]
        ),
        (&json!(true), &json!(false), &json!("admin"), &json!("demo"))
    );
    let (s, b) = send(
        &app,
        Method::POST,
        "/api/v1/topic/publish",
        Some("application/json"),
        PUBLISH,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(
        bus.published("/door_requests"),
        vec![json!({"data": "open"})]
    );
    let (s, b) = send(&app, Method::GET, "/api/v1/writes", None, "").await;
    assert_eq!((s, &b["writes_enabled"]), (StatusCode::OK, &json!(true)));
}

#[tokio::test]
async fn demo_mode_still_enforces_the_allowlist_and_has_no_accounts() {
    let (app, _) = demo_app(None);
    let (s, b) = send(
        &app,
        Method::POST,
        "/api/v1/topic/publish",
        Some("application/json"),
        r#"{"name": "/cmd_vel", "data": {}}"#,
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("not_allowlisted"))
    );
    let (s, b) = send(&app, Method::GET, "/api/v1/users", None, "").await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("auth_disabled"))
    );
}

#[tokio::test]
async fn demo_mode_refuses_requests_a_cross_site_page_could_send() {
    let (app, bus) = demo_app(None);
    // A plain HTML form can post text/plain or form data to localhost, but
    // not application/json without a CORS preflight.
    for ct in [
        None,
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
    ] {
        let (s, b) = send(&app, Method::POST, "/api/v1/topic/publish", ct, PUBLISH).await;
        assert_eq!(
            (s, b["error"].as_str()),
            (StatusCode::FORBIDDEN, Some("csrf")),
            "{ct:?}"
        );
    }
    assert!(bus.published("/door_requests").is_empty());
}

#[tokio::test]
async fn without_demo_mode_anonymous_callers_still_cannot_write() {
    let bus = FakeBus::new();
    let state = AppState::new(
        BuildInfo::new("t"),
        Hub::new(bus.clone(), HubConfig::default()),
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_writes(WritePolicy {
        publish: vec!["door_requests".parse().unwrap()],
        call: vec![],
        min_interval: None,
    });
    let app = router(state);
    let (s, b) = send(
        &app,
        Method::POST,
        "/api/v1/topic/publish",
        Some("application/json"),
        PUBLISH,
    )
    .await;
    assert_eq!(
        (s, b["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("auth_disabled"))
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn demo_writes_are_audited_in_the_database_when_there_is_one(pool: PgPool) {
    let db = Db::from_pool(pool);
    let (app, _) = demo_app(Some(db.clone()));
    let (s, b) = send(
        &app,
        Method::POST,
        "/api/v1/topic/publish",
        Some("application/json"),
        PUBLISH,
    )
    .await;
    assert_eq!(s, 200, "{b}");
    let a = db
        .list_audit(&AuditQuery {
            action: Some("topic.publish".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let got: Vec<_> = a
        .iter()
        .map(|r| (r.actor.as_str(), r.auth_method.as_str(), r.outcome.as_str()))
        .collect();
    assert_eq!(
        got,
        [
            ("anonymous", "demo", "ok"),
            ("anonymous", "demo", "pending")
        ]
    );
    // The audit log is readable in demo mode (the demo user is an admin).
    let (s, b) = send(&app, Method::GET, "/api/v1/audit", None, "").await;
    assert_eq!(s, 200, "{b}");
    assert_eq!(b.as_array().map(Vec::len), Some(2));
}

async fn send_with(app: &Router, method: Method, headers: &[(&str, &str)]) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method.clone())
        .uri(if method == Method::GET {
            "/api/v1/graph"
        } else {
            "/api/v1/topic/publish"
        });
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let body = if method == Method::GET {
        String::new()
    } else {
        PUBLISH.to_owned()
    };
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn demo_mode_refuses_rebound_hosts_and_foreign_origins() {
    let (app, bus) = demo_app(None);
    let json_ct = ("content-type", "application/json");
    for (headers, want) in [
        (vec![("host", "evil.example:18080")], Some("host")),
        (vec![("host", "localhost.evil.example")], Some("host")),
        (
            vec![
                ("host", "localhost:18080"),
                ("origin", "http://evil.example"),
            ],
            Some("origin"),
        ),
        (
            vec![("host", "localhost:18080"), ("origin", "null")],
            Some("origin"),
        ),
        (
            vec![
                ("host", "127.0.0.1:18080"),
                ("origin", "http://localhost:18080"),
            ],
            Some("origin"),
        ),
        (vec![("host", "localhost:18080")], None),
        (
            vec![
                ("host", "127.0.0.1:18080"),
                ("origin", "http://127.0.0.1:18080"),
            ],
            None,
        ),
        (vec![("host", "[::1]:18080")], None),
    ] {
        for method in [Method::GET, Method::POST] {
            let mut h = headers.clone();
            if method == Method::POST {
                h.push(json_ct);
            }
            let (s, b) = send_with(&app, method.clone(), &h).await;
            match want {
                Some(code) => assert_eq!(
                    (s, b["error"].as_str()),
                    (StatusCode::FORBIDDEN, Some(code)),
                    "{method} {headers:?}"
                ),
                None => assert!(s.is_success(), "{method} {headers:?}: {s} {b}"),
            }
        }
    }
    assert_eq!(
        bus.published("/door_requests").len(),
        3,
        "only the loopback POSTs went through"
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn demo_mode_never_applies_when_auth_is_on(pool: PgPool) {
    let db = Db::from_pool(pool);
    let state = AppState::new(
        BuildInfo::new("t"),
        Hub::new(FakeBus::new(), HubConfig::default()),
        Arc::new(String::new),
        CancellationToken::new(),
    )
    .with_demo()
    .with_auth(crate::auth::AuthService::new(
        db,
        crate::auth::AuthConfig::default(),
    ));
    assert!(!state.demo());
    let (s, _) = send(&router(state), Method::GET, "/api/v1/graph", None, "").await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}
