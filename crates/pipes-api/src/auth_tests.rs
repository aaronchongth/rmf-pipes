//! Auth tests against a real database (`#[sqlx::test]` gives each test a fresh
//! one; run with `pixi run test`).

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
};
use pipes_db::{AuditQuery, Db, Role};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::{
    AppState,
    auth::{AuthConfig, AuthService, SESSION_COOKIE},
    router,
};

const PW: &str = "correct horse battery";

struct App {
    router: Router,
    db: Db,
}

async fn app(pool: PgPool) -> App {
    let db = Db::from_pool(pool);
    db.create_user("admin", PW, Role::Admin).await.unwrap();
    db.create_user("op", PW, Role::Operator).await.unwrap();
    db.create_user("viewer", PW, Role::Viewer).await.unwrap();
    let hub = Hub::new(FakeBus::new(), HubConfig::default());
    let state = AppState::new(
        BuildInfo::new("testdistro"),
        hub,
        Arc::new(|| "# metrics\n".to_owned()),
        CancellationToken::new(),
    )
    .with_auth(AuthService::new(
        db.clone(),
        AuthConfig {
            login_max_failures: 3,
            login_window: Duration::from_secs(60),
            ..AuthConfig::default()
        },
    ));
    App {
        router: router(state),
        db,
    }
}

struct Res {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Value,
}

async fn send(app: &App, method: Method, uri: &str, auth: Auth<'_>, body: Option<Value>) -> Res {
    let mut b = Request::builder().method(method).uri(uri);
    match auth {
        Auth::None => {}
        Auth::Cookie(c) => b = b.header(header::COOKIE, format!("{SESSION_COOKIE}={c}")),
        Auth::Bearer(t) => b = b.header(header::AUTHORIZATION, format!("Bearer {t}")),
    }
    let req = match body {
        Some(v) => b
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(v.to_string())),
        None => b.body(Body::empty()),
    }
    .unwrap();
    let res = app.router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Res {
        status,
        headers,
        body,
    }
}

#[derive(Clone, Copy)]
enum Auth<'a> {
    None,
    Cookie(&'a str),
    Bearer(&'a str),
}

/// Log in and return the session cookie value.
async fn login(app: &App, user: &str) -> String {
    let r = send(
        app,
        Method::POST,
        "/api/v1/auth/login",
        Auth::None,
        Some(json!({"username": user, "password": PW})),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let set = r.headers[header::SET_COOKIE].to_str().unwrap().to_owned();
    assert!(
        set.contains("HttpOnly") && set.contains("SameSite=Strict"),
        "{set}"
    );
    set.split(';')
        .next()
        .unwrap()
        .split_once('=')
        .unwrap()
        .1
        .to_owned()
}

async fn token(app: &App, cookie: &str) -> (String, i64) {
    let r = send(
        app,
        Method::POST,
        "/api/v1/auth/tokens",
        Auth::Cookie(cookie),
        Some(json!({"name": "ci"})),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.body);
    (
        r.body["token"].as_str().unwrap().to_owned(),
        r.body["meta"]["id"].as_i64().unwrap(),
    )
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn every_protected_route_needs_credentials(pool: PgPool) {
    let app = app(pool).await;
    // Every documented operation except the public ones answers 401 JSON.
    let doc = send(&app, Method::GET, "/openapi.json", Auth::None, None).await;
    assert_eq!(doc.status, 200, "the OpenAPI document is public");
    let mut checked = 0;
    for (path, item) in doc.body["paths"].as_object().unwrap() {
        for method in item.as_object().unwrap().keys() {
            let m: Method = method.to_uppercase().parse().unwrap();
            if crate::auth::is_public(&m, path) {
                continue;
            }
            let uri = path.replace("{id}", "1").replace("{username}", "op") + "?name=/x";
            let body = (m != Method::GET).then(|| json!({}));
            let r = send(&app, m.clone(), &uri, Auth::None, body).await;
            assert_eq!(
                (r.status, r.body["error"].as_str()),
                (StatusCode::UNAUTHORIZED, Some("unauthenticated")),
                "{m} {path}"
            );
            checked += 1;
        }
    }
    assert!(checked >= 15, "checked {checked} operations");
    // Public routes stay reachable.
    assert_eq!(
        send(&app, Method::GET, "/healthz", Auth::None, None)
            .await
            .status,
        200
    );
    assert_eq!(
        send(&app, Method::GET, "/login", Auth::None, None)
            .await
            .status,
        200
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn pages_redirect_to_login_and_htmx_gets_hx_redirect(pool: PgPool) {
    let app = app(pool).await;
    let r = send(&app, Method::GET, "/ui/topic?name=/a", Auth::None, None).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert_eq!(
        r.headers[header::LOCATION],
        "/login?next=%2Fui%2Ftopic%3Fname%3D%2Fa"
    );
    let req = Request::get("/ui/fragments/overview")
        .header("hx-request", "true")
        .body(Body::empty())
        .unwrap();
    let res = app.router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert!(
        res.headers()["hx-redirect"]
            .to_str()
            .unwrap()
            .starts_with("/login?next=")
    );
    let cookie = login(&app, "viewer").await;
    let r = send(&app, Method::GET, "/", Auth::Cookie(&cookie), None).await;
    assert_eq!(r.status, 200);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn sessions_work_until_logout(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "OP").await;
    let me = send(
        &app,
        Method::GET,
        "/api/v1/auth/me",
        Auth::Cookie(&cookie),
        None,
    )
    .await;
    assert_eq!(
        (
            me.status,
            me.body["user"]["username"].as_str(),
            me.body["role"].as_str()
        ),
        (StatusCode::OK, Some("op"), Some("operator"))
    );
    assert_eq!(me.body["auth_method"], "session");
    assert_eq!(
        send(
            &app,
            Method::GET,
            "/api/v1/graph",
            Auth::Cookie(&cookie),
            None
        )
        .await
        .status,
        200
    );
    let out = send(
        &app,
        Method::POST,
        "/api/v1/auth/logout",
        Auth::Cookie(&cookie),
        Some(json!({})),
    )
    .await;
    assert_eq!(out.status, StatusCode::NO_CONTENT);
    assert!(
        out.headers[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert_eq!(
        send(
            &app,
            Method::GET,
            "/api/v1/graph",
            Auth::Cookie(&cookie),
            None
        )
        .await
        .status,
        401
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn failed_logins_look_the_same_and_are_audited(pool: PgPool) {
    let app = app(pool).await;
    app.db.create_user("off", PW, Role::Viewer).await.unwrap();
    app.db.set_disabled("off", true).await.unwrap();
    let mut bodies = Vec::new();
    for (u, p) in [("op", "wrong password!!"), ("nobody", PW), ("off", PW)] {
        let r = send(
            &app,
            Method::POST,
            "/api/v1/auth/login",
            Auth::None,
            Some(json!({"username": u, "password": p})),
        )
        .await;
        assert_eq!(r.status, 401, "{u}");
        bodies.push(r.body);
    }
    assert!(
        bodies.windows(2).all(|w| w[0] == w[1]),
        "responses must not reveal which part was wrong: {bodies:?}"
    );
    let reasons: Vec<String> = app
        .db
        .list_audit(&AuditQuery {
            action: Some("login".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_iter()
        .map(|r| format!("{}:{}", r.actor, r.detail["reason"].as_str().unwrap_or("")))
        .collect();
    assert_eq!(
        reasons,
        vec![
            "off:disabled",
            "nobody:bad_credentials",
            "op:bad_credentials"
        ]
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn repeated_failures_are_rate_limited(pool: PgPool) {
    let app = app(pool).await;
    for _ in 0..3 {
        let r = send(
            &app,
            Method::POST,
            "/api/v1/auth/login",
            Auth::None,
            Some(json!({"username": "op", "password": "nope nope nope"})),
        )
        .await;
        assert_eq!(r.status, 401);
    }
    // Even the right password is refused while limited.
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/login",
        Auth::None,
        Some(json!({"username": "op", "password": PW})),
    )
    .await;
    assert_eq!(
        (r.status, r.body["error"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("too_many_attempts"))
    );
    assert!(
        r.headers[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 1
    );
    // Other users are unaffected (no client address in in-process tests).
    login(&app, "viewer").await;
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn api_tokens_authenticate_scripts_until_revoked(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "viewer").await;
    let (tok, id) = token(&app, &cookie).await;
    assert!(tok.starts_with("rp_"));
    let me = send(
        &app,
        Method::GET,
        "/api/v1/auth/me",
        Auth::Bearer(&tok),
        None,
    )
    .await;
    assert_eq!(me.body["auth_method"], format!("token:{id}"));
    assert_eq!(
        send(&app, Method::GET, "/metrics", Auth::Bearer(&tok), None)
            .await
            .status,
        200
    );
    let list = send(
        &app,
        Method::GET,
        "/api/v1/auth/tokens",
        Auth::Bearer(&tok),
        None,
    )
    .await;
    assert_eq!(list.body.as_array().unwrap().len(), 1);
    assert!(
        list.body.to_string().find(&tok).is_none(),
        "listing must not reveal the secret"
    );
    // A bad bearer token is a 401 even alongside a valid cookie.
    let req = Request::get("/api/v1/graph")
        .header(header::COOKIE, format!("{SESSION_COOKIE}={cookie}"))
        .header(header::AUTHORIZATION, "Bearer rp_forged")
        .body(Body::empty())
        .unwrap();
    assert_eq!(app.router.clone().oneshot(req).await.unwrap().status(), 401);
    let r = send(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/tokens/{id}"),
        Auth::Bearer(&tok),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(
        send(&app, Method::GET, "/api/v1/graph", Auth::Bearer(&tok), None)
            .await
            .status,
        401
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn users_can_only_revoke_their_own_tokens_unless_admin(pool: PgPool) {
    let app = app(pool).await;
    let op = login(&app, "op").await;
    let viewer = login(&app, "viewer").await;
    let admin = login(&app, "admin").await;
    let (_, id) = token(&app, &op).await;
    let r = send(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/tokens/{id}"),
        Auth::Cookie(&viewer),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, 404);
    let r = send(
        &app,
        Method::GET,
        "/api/v1/auth/tokens?all=true",
        Auth::Cookie(&viewer),
        None,
    )
    .await;
    assert_eq!(r.status, 403);
    let r = send(
        &app,
        Method::GET,
        "/api/v1/auth/tokens?all=true",
        Auth::Cookie(&admin),
        None,
    )
    .await;
    assert_eq!(r.body.as_array().unwrap().len(), 1);
    let r = send(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/tokens/{id}"),
        Auth::Cookie(&admin),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, 204);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn roles_gate_user_management_and_the_audit_log(pool: PgPool) {
    let app = app(pool).await;
    let viewer = login(&app, "viewer").await;
    let admin = login(&app, "admin").await;
    for (m, uri) in [
        (Method::GET, "/api/v1/users"),
        (Method::GET, "/api/v1/audit"),
    ] {
        let r = send(&app, m.clone(), uri, Auth::Cookie(&viewer), None).await;
        assert_eq!(
            (r.status, r.body["error"].as_str()),
            (StatusCode::FORBIDDEN, Some("forbidden")),
            "{uri}"
        );
    }
    let r = send(
        &app,
        Method::POST,
        "/api/v1/users",
        Auth::Cookie(&viewer),
        Some(json!({"username": "x", "password": PW, "role": "admin"})),
    )
    .await;
    assert_eq!(r.status, 403, "a viewer can't create an admin");

    let r = send(
        &app,
        Method::POST,
        "/api/v1/users",
        Auth::Cookie(&admin),
        Some(json!({"username": "new", "password": PW, "role": "viewer"})),
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.body);
    let r = send(
        &app,
        Method::POST,
        "/api/v1/users",
        Auth::Cookie(&admin),
        Some(json!({"username": "NEW", "password": PW, "role": "viewer"})),
    )
    .await;
    assert_eq!(
        (r.status, r.body["error"].as_str()),
        (StatusCode::CONFLICT, Some("conflict"))
    );
    let r = send(
        &app,
        Method::POST,
        "/api/v1/users",
        Auth::Cookie(&admin),
        Some(json!({"username": "short", "password": "x", "role": "viewer"})),
    )
    .await;
    assert_eq!(
        (r.status, r.body["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid"))
    );
    let r = send(
        &app,
        Method::PATCH,
        "/api/v1/users/new",
        Auth::Cookie(&admin),
        Some(json!({"role": "operator", "disabled": true})),
    )
    .await;
    assert_eq!(
        (r.body["role"].as_str(), r.body["disabled"].as_bool()),
        (Some("operator"), Some(true))
    );
    let r = send(
        &app,
        Method::PATCH,
        "/api/v1/users/admin",
        Auth::Cookie(&admin),
        Some(json!({"role": "viewer"})),
    )
    .await;
    assert_eq!(r.status, 400, "the last admin can't be demoted: {}", r.body);
    let r = send(
        &app,
        Method::DELETE,
        "/api/v1/users/new",
        Auth::Cookie(&admin),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, 204);
    let r = send(
        &app,
        Method::DELETE,
        "/api/v1/users/new",
        Auth::Cookie(&admin),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, 404);

    let audit = send(
        &app,
        Method::GET,
        "/api/v1/audit?limit=50",
        Auth::Cookie(&admin),
        None,
    )
    .await;
    let actions: Vec<&str> = audit
        .body
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    for a in ["user.create", "user.update", "user.delete", "login"] {
        assert!(actions.contains(&a), "{a} not audited: {actions:?}");
    }
    let r = send(
        &app,
        Method::GET,
        "/api/v1/audit?action=user.delete",
        Auth::Cookie(&admin),
        None,
    )
    .await;
    assert_eq!(r.body[0]["target"], "new");
    assert_eq!(r.body[0]["actor"], "admin");
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn cookie_writes_must_be_json(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "viewer").await;
    let req = Request::post("/api/v1/auth/tokens")
        .header(header::COOKIE, format!("{SESSION_COOKIE}={cookie}"))
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from("name=evil"))
        .unwrap();
    let res = app.router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
    let body: Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"], "csrf");
    assert!(app.db.list_api_tokens(None).await.unwrap().is_empty());
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn password_change_requires_the_current_one_and_ends_sessions(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "op").await;
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/password",
        Auth::Cookie(&cookie),
        Some(json!({"current": "guess guess guess", "new": "a whole new password"})),
    )
    .await;
    assert_eq!(r.status, 401);
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/password",
        Auth::Cookie(&cookie),
        Some(json!({"current": PW, "new": "a whole new password"})),
    )
    .await;
    assert_eq!(r.status, 204);
    assert_eq!(
        send(
            &app,
            Method::GET,
            "/api/v1/graph",
            Auth::Cookie(&cookie),
            None
        )
        .await
        .status,
        401
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn disabled_users_lose_sessions_and_tokens(pool: PgPool) {
    let app = app(pool).await;
    let op = login(&app, "op").await;
    let (tok, _) = token(&app, &op).await;
    app.db.set_disabled("op", true).await.unwrap();
    assert_eq!(
        send(&app, Method::GET, "/api/v1/graph", Auth::Cookie(&op), None)
            .await
            .status,
        401
    );
    assert_eq!(
        send(&app, Method::GET, "/api/v1/graph", Auth::Bearer(&tok), None)
            .await
            .status,
        401
    );
}

#[tokio::test]
async fn without_a_database_the_api_is_anonymous_and_read_only() {
    let app = crate::test_support::state();
    let router = router(app);
    let get = |uri: &'static str| {
        let router = router.clone();
        async move {
            let res = router
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let s = res.status();
            let v: Value =
                serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
                    .unwrap_or(Value::Null);
            (s, v)
        }
    };
    let (s, me) = get("/api/v1/auth/me").await;
    assert_eq!(
        (s, me["auth_enabled"].as_bool(), me["role"].as_str()),
        (StatusCode::OK, Some(false), Some("viewer"))
    );
    assert_eq!(get("/api/v1/graph").await.0, 200);
    let (s, e) = get("/api/v1/users").await;
    assert_eq!(
        (s, e["error"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("auth_disabled"))
    );
    let req = Request::post("/api/v1/auth/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"username": "a", "password": "b"}).to_string(),
        ))
        .unwrap();
    assert_eq!(router.clone().oneshot(req).await.unwrap().status(), 503);
    // The login page just bounces back to the dashboard.
    assert_eq!(get("/login").await.0, StatusCode::SEE_OTHER);
}

// ---- review hardening (phase 3) ---------------------------------------------------

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn operators_cannot_change_or_delete_users_and_refusals_are_audited(pool: PgPool) {
    let app = app(pool).await;
    let op = login(&app, "op").await;
    let r = send(
        &app,
        Method::PATCH,
        "/api/v1/users/op",
        Auth::Cookie(&op),
        Some(json!({"role": "admin"})),
    )
    .await;
    assert_eq!(r.status, 403, "self-promotion");
    let r = send(
        &app,
        Method::DELETE,
        "/api/v1/users/viewer",
        Auth::Cookie(&op),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, 403);
    assert_eq!(
        app.db.user_by_name("op").await.unwrap().unwrap().role,
        Role::Operator
    );
    assert!(app.db.user_by_name("viewer").await.unwrap().is_some());
    let denied: Vec<String> = app
        .db
        .list_audit(&AuditQuery::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.outcome == "denied")
        .map(|r| format!("{}:{}", r.action, r.target))
        .collect();
    assert_eq!(denied, vec!["user.delete:viewer", "user.update:op"]);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn cookie_patch_and_delete_must_be_json_too(pool: PgPool) {
    let app = app(pool).await;
    let admin = login(&app, "admin").await;
    for (m, uri) in [
        (Method::PATCH, "/api/v1/users/op"),
        (Method::DELETE, "/api/v1/users/op"),
    ] {
        let req = Request::builder()
            .method(m.clone())
            .uri(uri)
            .header(header::COOKIE, format!("{SESSION_COOKIE}={admin}"))
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(r#"{"role":"viewer"}"#))
            .unwrap();
        assert_eq!(
            app.router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::FORBIDDEN,
            "{m}"
        );
    }
    assert_eq!(
        app.db.user_by_name("op").await.unwrap().unwrap().role,
        Role::Operator
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn a_rejected_update_changes_nothing(pool: PgPool) {
    let app = app(pool).await;
    let admin = login(&app, "admin").await;
    let r = send(
        &app,
        Method::PATCH,
        "/api/v1/users/op",
        Auth::Cookie(&admin),
        Some(json!({"role": "admin", "password": "short"})),
    )
    .await;
    assert_eq!(r.status, 400);
    assert_eq!(
        app.db.user_by_name("op").await.unwrap().unwrap().role,
        Role::Operator,
        "partially applied"
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn logging_in_again_retires_the_previous_session(pool: PgPool) {
    let app = app(pool).await;
    let first = login(&app, "op").await;
    let req = Request::post("/api/v1/auth/login")
        .header(header::COOKIE, format!("{SESSION_COOKIE}={first}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"username": "op", "password": PW}).to_string(),
        ))
        .unwrap();
    assert_eq!(app.router.clone().oneshot(req).await.unwrap().status(), 200);
    assert_eq!(
        send(
            &app,
            Method::GET,
            "/api/v1/graph",
            Auth::Cookie(&first),
            None
        )
        .await
        .status,
        401
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn token_lifetimes_are_bounded_and_tokens_cannot_mint_longer_ones(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "op").await;
    for bad in [0u64, 3651, 4_294_967_295] {
        let r = send(
            &app,
            Method::POST,
            "/api/v1/auth/tokens",
            Auth::Cookie(&cookie),
            Some(json!({"name": "x", "expires_in_days": bad})),
        )
        .await;
        assert_eq!(r.status, 400, "{bad}: {}", r.body);
    }
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/tokens",
        Auth::Cookie(&cookie),
        Some(json!({"name": "short", "expires_in_days": 1})),
    )
    .await;
    let short = r.body["token"].as_str().unwrap().to_owned();
    let limit = r.body["meta"]["expires_at"].as_str().unwrap().to_owned();
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/tokens",
        Auth::Bearer(&short),
        Some(json!({"name": "forever"})),
    )
    .await;
    assert_eq!(r.status, 201);
    assert_eq!(
        r.body["meta"]["expires_at"].as_str(),
        Some(limit.as_str()),
        "capped at the minting token's expiry"
    );
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn password_change_guesses_are_rate_limited(pool: PgPool) {
    let app = app(pool).await;
    let cookie = login(&app, "op").await;
    let (tok, _) = token(&app, &cookie).await;
    for _ in 0..3 {
        let r = send(
            &app,
            Method::POST,
            "/api/v1/auth/password",
            Auth::Bearer(&tok),
            Some(json!({"current": "guess guess guess", "new": "attacker password"})),
        )
        .await;
        assert_eq!(r.status, 401);
    }
    let r = send(
        &app,
        Method::POST,
        "/api/v1/auth/password",
        Auth::Bearer(&tok),
        Some(json!({"current": PW, "new": "attacker password"})),
    )
    .await;
    assert_eq!(r.status, 429);
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn unauthenticated_callers_cannot_flood_the_audit_log(pool: PgPool) {
    let app = app(pool).await;
    for _ in 0..20 {
        send(
            &app,
            Method::POST,
            "/api/v1/auth/login",
            Auth::None,
            Some(json!({"username": "op", "password": "nope nope nope"})),
        )
        .await;
    }
    // Garbage usernames are refused without touching anything.
    for name in ["x\u{0}y", &"a".repeat(5000), "has space"] {
        let r = send(
            &app,
            Method::POST,
            "/api/v1/auth/login",
            Auth::None,
            Some(json!({"username": name, "password": PW})),
        )
        .await;
        assert_eq!(
            (r.status, r.body["error"].as_str()),
            (StatusCode::UNAUTHORIZED, Some("bad_credentials"))
        );
    }
    let n = app
        .db
        .list_audit(&AuditQuery {
            action: Some("login".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .len();
    assert_eq!(
        n,
        3 + 1,
        "3 bad passwords + 1 rate-limit notice, not one row per request"
    );
}

#[test]
fn login_redirects_only_to_local_paths() {
    use crate::ui::safe_next;
    for ok in ["/", "/ui/topic?name=%2Fa", "/a/b"] {
        assert_eq!(safe_next(Some(ok)), ok);
    }
    for bad in [
        "//evil.example",
        "/\\evil.example",
        "/\t/evil.example/x",
        "/\n/evil",
        "https://evil.example",
        "evil",
        " /x",
        "",
    ] {
        assert_eq!(safe_next(Some(bad)), "/", "{bad:?}");
    }
    assert_eq!(safe_next(None), "/");
}
