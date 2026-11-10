//! Authentication and authorization.
//!
//! - **Auth disabled** (no database): every caller is anonymous with the
//!   viewer role. The API is read-only; writes and account endpoints answer
//!   `503 auth_disabled`. The binary only allows this on a loopback address
//!   unless told otherwise.
//! - **Demo mode** (`--demo`, loopback only, with or without a database):
//!   authentication is off and every caller is [`Principal::Demo`], with the
//!   admin role, so the allowlisted write path and recording rules work
//!   without logging in. Account endpoints answer `503 auth_disabled`. The
//!   CSRF rule below applies to every non-GET request, since any web page the
//!   browser visits could otherwise post to a localhost port. Against DNS
//!   rebinding (a page whose own domain is re-pointed at 127.0.0.1, making
//!   its requests same-origin), every request must also be addressed to
//!   `localhost` or a loopback IP (`Host`), and a browser `Origin`, if sent,
//!   must be that same host.
//! - **Auth enabled**: every route except [`is_public`] ones needs a session
//!   cookie (browser) or `Authorization: Bearer rp_…` (scripts). Missing or
//!   invalid credentials get `401` (API) or a redirect to `/login` (pages).
//!
//! CSRF: session cookies are `SameSite=Strict`, and any non-GET request
//! authenticated by cookie must send `Content-Type: application/json`, which a
//! cross-site form cannot do without a CORS preflight that we never allow.
//!
//! Login attempts are limited per username and per client address
//! ([`LoginLimiter`]).

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Mutex,
    time::Duration,
};

use axum::{
    extract::{ConnectInfo, FromRequestParts, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, request::Parts},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
};
use pipes_db::{AuditEntry, Credential, Db, Outcome, Role, User};
use tokio::time::Instant;

use crate::{AppState, error::Problem};

/// Name of the session cookie over plain HTTP.
pub const SESSION_COOKIE: &str = "rmf_pipes_session";
/// Name of the session cookie when it is `Secure`. The `__Host-` prefix makes
/// browsers refuse it unless it was set by this exact host over HTTPS with
/// `Path=/`, so a sibling subdomain can't plant or shadow it.
pub const SECURE_SESSION_COOKIE: &str = "__Host-rmf_pipes_session";

/// Auth settings.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// A session ends after this long without use.
    pub session_idle: Duration,
    /// A session ends this long after login, however much it is used.
    pub session_max: Duration,
    /// Mark the session cookie `Secure` (serve over HTTPS, or behind a TLS proxy).
    pub cookie_secure: bool,
    /// Failed logins allowed per username and per address within `login_window`.
    pub login_max_failures: usize,
    /// Window for `login_max_failures`.
    pub login_window: Duration,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            session_idle: Duration::from_secs(12 * 3600),
            session_max: Duration::from_secs(7 * 24 * 3600),
            cookie_secure: false,
            login_max_failures: 5,
            login_window: Duration::from_secs(15 * 60),
        }
    }
}

/// Auth with a database behind it.
#[derive(Debug)]
pub struct AuthService {
    /// The database with users, sessions, tokens and the audit log.
    pub db: Db,
    /// Settings.
    pub cfg: AuthConfig,
    /// Failed-login tracking.
    pub limiter: LoginLimiter,
}

impl AuthService {
    /// Auth over `db`.
    pub fn new(db: Db, cfg: AuthConfig) -> Self {
        let limiter = LoginLimiter::new(cfg.login_max_failures, cfg.login_window);
        Self { db, cfg, limiter }
    }

    /// The session cookie's name for this configuration.
    pub fn cookie_name(&self) -> &'static str {
        if self.cfg.cookie_secure {
            SECURE_SESSION_COOKIE
        } else {
            SESSION_COOKIE
        }
    }

    /// `Set-Cookie` value for a new session.
    pub fn session_cookie(&self, secret: &str) -> String {
        session_cookie(self.cookie_name(), secret, self.cfg.cookie_secure)
    }

    /// `Set-Cookie` value that removes the session cookie.
    pub fn clear_session_cookie(&self) -> String {
        format!(
            "{}; Max-Age=0",
            session_cookie(self.cookie_name(), "", self.cfg.cookie_secure)
        )
    }
}

/// Who is calling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// Auth is disabled; the caller may read.
    Anonymous,
    /// Demo mode: auth is disabled and every caller may do anything an admin
    /// can, except manage accounts.
    Demo,
    /// An authenticated user.
    User {
        /// The user.
        user: User,
        /// How they authenticated.
        credential: Credential,
    },
}

impl Principal {
    /// The caller's role (anonymous callers are viewers).
    pub fn role(&self) -> Role {
        match self {
            Self::Anonymous => Role::Viewer,
            Self::Demo => Role::Admin,
            Self::User { user, .. } => user.role,
        }
    }

    /// The user, if authenticated.
    pub fn user(&self) -> Option<&User> {
        match self {
            Self::Anonymous | Self::Demo => None,
            Self::User { user, .. } => Some(user),
        }
    }

    /// Name for logs and the audit trail.
    pub fn name(&self) -> &str {
        self.user().map_or("anonymous", |u| u.username.as_str())
    }

    /// `session`, `token:<id>` or `none`, for the audit trail.
    pub fn method(&self) -> String {
        match self {
            Self::Anonymous => "none".into(),
            Self::Demo => "demo".into(),
            Self::User {
                credential: Credential::Session,
                ..
            } => "session".into(),
            Self::User {
                credential: Credential::Token { id },
                ..
            } => format!("token:{id}"),
        }
    }

    /// Refuse unless the caller has at least `role`.
    pub fn require(&self, role: Role) -> Result<(), Problem> {
        if self.role() >= role {
            Ok(())
        } else {
            Err(Problem::forbidden(
                "forbidden",
                format!("this needs the {role} role; you are {}", self.role()),
            ))
        }
    }

    /// An audit entry for an action by this caller.
    pub fn audit(
        &self,
        action: &str,
        target: &str,
        outcome: Outcome,
        detail: serde_json::Value,
    ) -> AuditEntry {
        AuditEntry {
            actor_id: self.user().map(|u| u.id),
            actor: self.name().to_owned(),
            auth_method: self.method(),
            action: action.to_owned(),
            target: target.to_owned(),
            outcome,
            detail,
        }
    }
}

/// The caller, as resolved by [`authenticate`]. Handlers take it as an
/// extractor.
#[derive(Debug, Clone)]
pub struct Caller(pub Principal);

impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = Problem;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Problem> {
        parts
            .extensions
            .get::<Principal>()
            .cloned()
            .map(Caller)
            .ok_or_else(|| {
                Problem::unauthenticated("log in, or send `Authorization: Bearer <token>`")
            })
    }
}

/// Routes reachable without credentials.
pub fn is_public(method: &Method, path: &str) -> bool {
    matches!(path, "/healthz" | "/login" | "/openapi.json" | "/docs")
        || path.starts_with("/docs/")
        || path.starts_with("/ui/assets/")
        || (method == Method::POST && path == "/api/v1/auth/login")
}

/// Pages (HTML) get a redirect to `/login`; everything else gets JSON 401.
fn is_page(path: &str) -> bool {
    path == "/" || path.starts_with("/ui/")
}

/// Middleware: resolve the caller, enforce authentication and the CSRF rule.
pub async fn authenticate(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let Some(auth) = state.auth() else {
        let principal = if state.demo() {
            if let Err(p) = demo_origin_check(req.headers())
                .and_then(|()| csrf_check(req.method(), req.headers(), &Principal::Demo))
            {
                return p.into_response();
            }
            Principal::Demo
        } else {
            Principal::Anonymous
        };
        req.extensions_mut().insert(principal);
        return next.run(req).await;
    };
    let path = req.uri().path().to_owned();
    let public = is_public(req.method(), &path);
    let resolved = match resolve(auth, req.headers()).await {
        Ok(p) => p,
        Err(err) => {
            tracing::error!(%err, "credential lookup failed");
            return Problem::unavailable("auth_unavailable", "cannot check credentials right now")
                .into_response();
        }
    };
    match resolved {
        Some(principal) => {
            if let Err(p) = csrf_check(req.method(), req.headers(), &principal) {
                return p.into_response();
            }
            req.extensions_mut().insert(principal);
            next.run(req).await
        }
        None if public => next.run(req).await,
        None if is_page(&path) => login_redirect(req.headers(), req.uri()),
        None => Problem::unauthenticated("log in, or send `Authorization: Bearer <token>`")
            .into_response(),
    }
}

async fn resolve(
    auth: &AuthService,
    headers: &HeaderMap,
) -> Result<Option<Principal>, pipes_db::DbError> {
    if let Some(token) = bearer(headers) {
        // A presented-but-bad bearer token is not silently ignored in favour
        // of a cookie: the caller asked to be that token.
        return Ok(auth
            .db
            .token_user(token)
            .await?
            .map(|(user, id)| Principal::User {
                user,
                credential: Credential::Token { id },
            }));
    }
    if let Some(secret) = cookie(headers, auth.cookie_name()) {
        return Ok(auth
            .db
            .session_user(secret, auth.cfg.session_idle, auth.cfg.session_max)
            .await?
            .map(|user| Principal::User {
                user,
                credential: Credential::Session,
            }));
    }
    Ok(None)
}

fn csrf_check(method: &Method, headers: &HeaderMap, p: &Principal) -> Result<(), Problem> {
    // Requests a cross-site page can trigger: cookie-authenticated ones, and
    // in demo mode all of them.
    let ambient = matches!(
        p,
        Principal::Demo
            | Principal::User {
                credential: Credential::Session,
                ..
            }
    );
    if !ambient || matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return Ok(());
    }
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
        });
    if json {
        Ok(())
    } else {
        Err(Problem::forbidden(
            "csrf",
            "requests authenticated by session cookie (or any request in demo mode) must send Content-Type: application/json",
        ))
    }
}

/// Demo mode: refuse requests addressed to anything but a loopback name, and
/// browser requests from any other origin (DNS rebinding).
fn demo_origin_check(headers: &HeaderMap) -> Result<(), Problem> {
    let refuse = |what: &'static str| {
        Err(Problem::forbidden(
            what,
            "demo mode only answers requests to localhost or a loopback address, from no page or a page served by rmf-pipes itself",
        ))
    };
    let Some(host) = headers.get(header::HOST) else {
        // Not a browser (they always send Host); nothing to rebind.
        return Ok(());
    };
    let Ok(host) = host.to_str() else {
        return refuse("host");
    };
    if !is_loopback_host(host) {
        return refuse("host");
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let same = origin.to_str().is_ok_and(|o| {
            o.strip_prefix("http://")
                .or_else(|| o.strip_prefix("https://"))
                .is_some_and(|rest| rest.eq_ignore_ascii_case(host))
        });
        if !same {
            return refuse("origin");
        }
    }
    Ok(())
}

/// `localhost`, `127.x.y.z` or `[::1]`, with an optional port.
fn is_loopback_host(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(rest) => match rest.split_once(']') {
            Some((ip, port)) if port.is_empty() || port.strip_prefix(':').is_some_and(is_port) => {
                ip
            }
            _ => return false,
        },
        None => match host.rsplit_once(':') {
            Some((name, port)) if is_port(port) => name,
            Some(_) => return false,
            None => host,
        },
    };
    name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn is_port(p: &str) -> bool {
    !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit())
}

fn login_redirect(headers: &HeaderMap, uri: &axum::http::Uri) -> Response {
    let next = uri.path_and_query().map_or("/", |p| p.as_str());
    let target = format!("/login?next={}", urlencode(next));
    if headers.contains_key("hx-request") {
        // htmx fragment requests: tell htmx to navigate the whole page.
        let mut res = StatusCode::UNAUTHORIZED.into_response();
        if let Ok(v) = HeaderValue::from_str(&target) {
            res.headers_mut().insert("hx-redirect", v);
        }
        return res;
    }
    Redirect::to(&target).into_response()
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = v.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

/// The value of cookie `name`, if present.
pub fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|kv| {
            let (k, v) = kv.trim().split_once('=')?;
            (k == name).then_some(v)
        })
}

fn session_cookie(name: &str, secret: &str, secure: bool) -> String {
    format!(
        "{name}={secret}; Path=/; HttpOnly; SameSite=Strict{}",
        if secure { "; Secure" } else { "" }
    )
}

/// The client's address, when the server was started with connect info.
pub fn client_addr(parts: &Parts) -> Option<SocketAddr> {
    parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
}

/// Counts failed password checks per key in a sliding window.
///
/// Keys and limits (see [`login_keys`]): the (username, client address) pair
/// gets `max_failures`, and the client address alone gets `max_failures × 10`.
/// There is deliberately no per-username key across addresses: that would let
/// anyone lock a user out by guessing badly. The trade-off is that a guesser
/// with many addresses gets `max_failures` tries per address per window,
/// against argon2-hashed passwords of at least 10 characters.
///
/// Successful logins don't reset counts, so a valid account can't be used to
/// keep probing from one address. Memory is bounded: keys are short (only
/// valid usernames are counted) and the map holds at most [`MAX_KEYS`].
#[derive(Debug)]
pub struct LoginLimiter {
    max_failures: usize,
    window: Duration,
    state: Mutex<LimiterState>,
}

#[derive(Debug, Default)]
struct LimiterState {
    failures: HashMap<String, VecDeque<Instant>>,
    /// Keys whose current block has already been reported (audited).
    reported: std::collections::HashSet<String>,
}

/// Most keys the limiter tracks; beyond this the oldest are forgotten.
pub const MAX_KEYS: usize = 10_000;

/// Why [`LoginLimiter::check`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blocked {
    /// When to try again.
    pub retry_after: Duration,
    /// Whether this is the first refusal of this block (audit only this one,
    /// so unauthenticated callers can't flood the audit log).
    pub first: bool,
}

/// A limiter key with its own failure limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitKey {
    key: String,
    /// Multiple of `max_failures` allowed for this key.
    factor: usize,
}

impl LoginLimiter {
    /// Allow `max_failures` per key per `window` (scaled per key).
    pub fn new(max_failures: usize, window: Duration) -> Self {
        Self {
            max_failures: max_failures.max(1),
            window,
            state: Mutex::new(LimiterState::default()),
        }
    }

    /// Refuse if any key is over its limit.
    pub fn check(&self, keys: &[LimitKey]) -> Result<(), Blocked> {
        let now = Instant::now();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let LimiterState { failures, reported } = &mut *st;
        let mut blocked: Option<Blocked> = None;
        for k in keys {
            let Some(q) = failures.get_mut(&k.key) else {
                continue;
            };
            while q
                .front()
                .is_some_and(|t| now.duration_since(*t) >= self.window)
            {
                q.pop_front();
            }
            if q.len() >= self.max_failures * k.factor
                && let Some(oldest) = q.front()
            {
                let wait = self.window.saturating_sub(now.duration_since(*oldest));
                let first = reported.insert(k.key.clone());
                blocked = Some(match blocked {
                    None => Blocked {
                        retry_after: wait,
                        first,
                    },
                    Some(b) => Blocked {
                        retry_after: b.retry_after.max(wait),
                        first: b.first || first,
                    },
                });
            } else {
                reported.remove(&k.key);
            }
        }
        failures.retain(|_, q| !q.is_empty());
        reported.retain(|k| failures.contains_key(k));
        blocked.map_or(Ok(()), Err)
    }

    /// Record a failure against every key.
    pub fn fail(&self, keys: &[LimitKey]) {
        let now = Instant::now();
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if st.failures.len() >= MAX_KEYS {
            // Forget the keys whose newest failure is oldest.
            let mut by_age: Vec<(Instant, String)> = st
                .failures
                .iter()
                .filter_map(|(k, q)| q.back().map(|t| (*t, k.clone())))
                .collect();
            by_age.sort();
            for (_, k) in by_age.into_iter().take(MAX_KEYS / 10) {
                st.failures.remove(&k);
                st.reported.remove(&k);
            }
        }
        for k in keys {
            st.failures.entry(k.key.clone()).or_default().push_back(now);
        }
    }

    /// Keys tracked (for tests and metrics).
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .failures
            .len()
    }

    /// Whether no keys are tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Keys a password check is limited by: (username, address) and address.
/// Without a known address (in-process tests) the pair uses `unknown`.
pub fn login_keys(username: &str, addr: Option<SocketAddr>) -> Vec<LimitKey> {
    let ip = addr.map_or_else(|| "unknown".to_owned(), |a| a.ip().to_string());
    let mut keys = vec![LimitKey {
        key: format!("user:{}@{ip}", username.to_lowercase()),
        factor: 1,
    }];
    if addr.is_some() {
        keys.push(LimitKey {
            key: format!("addr:{ip}"),
            factor: 10,
        });
    }
    keys
}

/// Record an audit entry; failures are logged, not returned (the action the
/// entry describes has already been decided).
pub async fn audit(state: &AppState, entry: AuditEntry) {
    let _ = record_audit(state, &entry).await;
}

/// Record `entry` in the database's audit log, or without a database (demo
/// mode) in the process log under the `rmf_pipes::audit` target. Returns the
/// entry's id (database ids, or a per-process counter).
pub async fn record_audit(state: &AppState, entry: &AuditEntry) -> Result<i64, pipes_db::DbError> {
    static NEXT_LOCAL_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
    let Some(db) = state.db() else {
        let id = NEXT_LOCAL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !state.demo() {
            // Anonymous mode has no audit log; refusals are only worth a
            // debug line (they'd otherwise let anyone flood the log).
            tracing::debug!(action = %entry.action, outcome = ?entry.outcome, "not audited: no database");
            return Ok(id);
        }
        tracing::info!(
            target: "rmf_pipes::audit",
            id,
            actor = %entry.actor,
            auth_method = %entry.auth_method,
            action = %entry.action,
            target_name = %entry.target,
            outcome = ?entry.outcome,
            detail = %entry.detail,
            "audit"
        );
        return Ok(id);
    };
    db.record_audit(entry).await.inspect_err(|err| {
        tracing::error!(%err, action = %entry.action, "cannot write the audit log");
    })
}

/// Require auth to be enabled (account and write endpoints).
pub fn require_auth(state: &AppState) -> Result<&AuthService, Problem> {
    state.auth().ok_or_else(|| {
        Problem::unavailable(
            "auth_disabled",
            "authentication is disabled (no --database-url); this endpoint is unavailable",
        )
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, Method, header};

    use super::*;

    #[test]
    fn loopback_hosts_are_recognized() {
        for ok in [
            "localhost",
            "LOCALHOST:18080",
            "127.0.0.1",
            "127.0.0.1:18080",
            "127.9.9.9:1",
            "[::1]",
            "[::1]:18080",
        ] {
            assert!(is_loopback_host(ok), "{ok}");
        }
        for bad in [
            "evil.example",
            "evil.example:18080",
            "localhost.evil.example",
            "10.0.0.1:18080",
            "0.0.0.0:18080",
            "[::1]x",
            "[::2]:80",
            "localhost:",
            "localhost:abc",
            "::1",
        ] {
            assert!(!is_loopback_host(bad), "{bad}");
        }
    }

    #[test]
    fn public_routes_are_exactly_these() {
        for (m, p) in [
            (Method::GET, "/healthz"),
            (Method::GET, "/login"),
            (Method::GET, "/openapi.json"),
            (Method::GET, "/docs/"),
            (Method::GET, "/ui/assets/htmx-2.0.11.min.js"),
            (Method::POST, "/api/v1/auth/login"),
        ] {
            assert!(is_public(&m, p), "{m} {p}");
        }
        for (m, p) in [
            (Method::GET, "/"),
            (Method::GET, "/metrics"),
            (Method::GET, "/api/v1/graph"),
            (Method::GET, "/api/v1/auth/login"),
            (Method::GET, "/ui/topic"),
            (Method::GET, "/healthz/x"),
            (Method::GET, "/docsx"),
        ] {
            assert!(!is_public(&m, p), "{m} {p}");
        }
    }

    #[test]
    fn parses_cookies_and_bearer_tokens() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_static("a=1; rmf_pipes_session=abc; b=2"),
        );
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer rp_xyz"),
        );
        assert_eq!(cookie(&h, SESSION_COOKIE), Some("abc"));
        assert_eq!(cookie(&h, "missing"), None);
        assert_eq!(bearer(&h), Some("rp_xyz"));
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic Zm9v"),
        );
        assert_eq!(bearer(&h), None);
    }

    #[test]
    fn session_cookies_are_strict_and_http_only() {
        let c = session_cookie(SECURE_SESSION_COOKIE, "s", true);
        assert!(c.starts_with("__Host-") && c.contains("Path=/"), "{c}");
        assert!(
            c.contains("HttpOnly") && c.contains("SameSite=Strict") && c.contains("Secure"),
            "{c}"
        );
        assert!(!session_cookie(SESSION_COOKIE, "s", false).contains("Secure"));
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_blocks_a_user_at_an_address_and_recovers() {
        let l = LoginLimiter::new(3, Duration::from_secs(60));
        let here: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let there: SocketAddr = "10.0.0.2:5000".parse().unwrap();
        let keys = login_keys("Alice", Some(here));
        for _ in 0..3 {
            assert!(l.check(&keys).is_ok());
            l.fail(&keys);
        }
        let b = l.check(&keys).unwrap_err();
        assert!(
            b.first
                && b.retry_after <= Duration::from_secs(60)
                && b.retry_after > Duration::from_secs(50),
            "{b:?}"
        );
        assert!(
            !l.check(&keys).unwrap_err().first,
            "only the first refusal is reported"
        );
        assert!(
            l.check(&login_keys("alice", Some(here))).is_err(),
            "case-insensitive"
        );
        // The same user from elsewhere is not locked out (no lockout DoS).
        assert!(l.check(&login_keys("alice", Some(there))).is_ok());
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(l.check(&keys).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_caps_one_address_across_usernames() {
        let l = LoginLimiter::new(3, Duration::from_secs(60));
        let here: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        // 3 per username, 30 per address: spraying 10 names exhausts it.
        for i in 0..10 {
            let keys = login_keys(&format!("user{i}"), Some(here));
            for _ in 0..3 {
                l.fail(&keys);
            }
        }
        assert!(
            l.check(&login_keys("fresh", Some(here))).is_err(),
            "the address is over its ceiling"
        );
        assert!(
            l.check(&login_keys("fresh", Some("10.0.0.9:1".parse().unwrap())))
                .is_ok()
        );
    }

    #[test]
    fn limiter_memory_is_bounded() {
        let l = LoginLimiter::new(3, Duration::from_secs(60));
        for i in 0..(MAX_KEYS + 500) {
            l.fail(&login_keys(&format!("u{i}"), None));
        }
        assert!(l.len() <= MAX_KEYS, "{}", l.len());
    }

    #[test]
    fn principals_order_roles() {
        assert!(Principal::Anonymous.require(Role::Viewer).is_ok());
        assert_eq!(
            Principal::Anonymous
                .require(Role::Operator)
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );
    }
}
