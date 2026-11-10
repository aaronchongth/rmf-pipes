//! Account endpoints: login/logout, the current user, API tokens, user
//! management (admin) and the audit log (admin). Every change is audited.

use axum::{
    Json,
    extract::{FromRequestParts, Path, State},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use pipes_db::{ApiToken, AuditQuery, AuditRecord, LoginOutcome, Outcome, Role, User};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    api::ApiQuery,
    auth::{self, Caller, Principal, audit, require_auth},
    error::{ApiError, Problem},
};

/// Login request.
#[derive(Debug, Deserialize, ToSchema)]
pub struct Login {
    /// Username (case-insensitive).
    pub username: String,
    /// Password.
    pub password: String,
}

/// Who the caller is.
#[derive(Debug, Serialize, ToSchema)]
pub struct Me {
    /// Whether authentication is enabled (a database is configured, and not
    /// in demo mode).
    pub auth_enabled: bool,
    /// Demo mode: no authentication, and every caller acts as an admin.
    pub demo: bool,
    /// The user; `null` when auth is disabled.
    pub user: Option<User>,
    /// The caller's effective role.
    pub role: Role,
    /// `session`, `token:<id>`, `demo` or `none`.
    pub auth_method: String,
}

/// JSON body extractor whose rejections are JSON [`Problem`]s.
#[derive(Debug)]
pub struct ApiJson<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for ApiJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Problem;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Problem> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(v)| Self(v))
            .map_err(|e| Problem::bad_request("invalid_body", e.body_text()))
    }
}

/// The client address, if known.
#[derive(Debug)]
pub struct ClientAddr(pub Option<std::net::SocketAddr>);

impl<S: Send + Sync> FromRequestParts<S> for ClientAddr {
    type Rejection = std::convert::Infallible;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(auth::client_addr(parts)))
    }
}

/// Log in with a username and password; sets the session cookie.
#[utoipa::path(post, path = "/api/v1/auth/login", tag = "auth", request_body = Login, security(()),
    responses(
        (status = 200, description = "Logged in; the `rmf_pipes_session` cookie is set", body = Me),
        (status = 401, description = "Wrong username or password, or the user is disabled (`bad_credentials`)", body = ApiError),
        (status = 429, description = "Too many failed attempts for this user or address (`too_many_attempts`); see `Retry-After`", body = ApiError),
        (status = 503, description = "Auth is disabled (`auth_disabled`)", body = ApiError),
    ))]
pub async fn login(
    State(state): State<AppState>,
    ClientAddr(addr): ClientAddr,
    headers: axum::http::HeaderMap,
    ApiJson(req): ApiJson<Login>,
) -> Result<Response, Problem> {
    let auth = require_auth(&state)?;
    let bad = || Problem::unauthorized("bad_credentials", "wrong username or password");
    // A name that can't exist is refused before it costs a hash, a database
    // query, a limiter key or an audit row.
    if pipes_db::validate_username(&req.username).is_err() {
        return Err(bad());
    }
    let keys = auth::login_keys(&req.username, addr);
    let ip = addr.map(|a| a.ip().to_string());
    if let Err(blocked) = auth.limiter.check(&keys) {
        if blocked.first {
            audit(
                &state,
                attempt(
                    &req.username,
                    Outcome::Denied,
                    json!({"reason": "rate_limited", "addr": ip}),
                ),
            )
            .await;
        }
        return Err(Problem::too_many(
            "too_many_attempts",
            "too many failed logins; try again later",
            blocked.retry_after,
        ));
    }
    let outcome = auth.db.check_login(&req.username, &req.password).await?;
    let user = match outcome {
        LoginOutcome::Ok(user) => user,
        LoginOutcome::BadCredentials | LoginOutcome::Disabled => {
            auth.limiter.fail(&keys);
            let reason = if outcome == LoginOutcome::Disabled {
                "disabled"
            } else {
                "bad_credentials"
            };
            audit(
                &state,
                attempt(
                    &req.username,
                    Outcome::Denied,
                    json!({"reason": reason, "addr": ip}),
                ),
            )
            .await;
            return Err(bad());
        }
    };
    // Don't leave the session the browser came with lying around.
    if let Some(old) = auth::cookie(&headers, auth.cookie_name()) {
        auth.db.delete_session(old).await?;
    }
    let secret = auth
        .db
        .create_session(user.id, auth.cfg.session_idle)
        .await?;
    let principal = Principal::User {
        user: user.clone(),
        credential: pipes_db::Credential::Session,
    };
    audit(
        &state,
        principal.audit("login", &user.username, Outcome::Ok, json!({"addr": ip})),
    )
    .await;
    let body = Me {
        auth_enabled: true,
        demo: false,
        role: user.role,
        user: Some(user),
        auth_method: "session".into(),
    };
    Ok((
        [(header::SET_COOKIE, auth.session_cookie(&secret))],
        Json(body),
    )
        .into_response())
}

fn attempt(username: &str, outcome: Outcome, detail: serde_json::Value) -> pipes_db::AuditEntry {
    pipes_db::AuditEntry {
        actor_id: None,
        actor: username.chars().take(64).collect(),
        auth_method: "password".into(),
        action: "login".into(),
        target: username.chars().take(64).collect(),
        outcome,
        detail,
    }
}

/// End the current session and clear its cookie.
#[utoipa::path(post, path = "/api/v1/auth/logout", tag = "auth",
    responses((status = 204, description = "Logged out")))]
pub async fn logout(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: axum::http::HeaderMap,
) -> Result<Response, Problem> {
    let auth = require_auth(&state)?;
    if let Some(secret) = auth::cookie(&headers, auth.cookie_name()) {
        auth.db.delete_session(secret).await?;
    }
    audit(
        &state,
        caller.audit("logout", caller.name(), Outcome::Ok, json!({})),
    )
    .await;
    Ok((
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, auth.clear_session_cookie())],
    )
        .into_response())
}

/// The caller.
#[utoipa::path(get, path = "/api/v1/auth/me", tag = "auth",
    responses((status = 200, description = "The caller", body = Me)))]
pub async fn me(State(state): State<AppState>, Caller(caller): Caller) -> Json<Me> {
    Json(Me {
        auth_enabled: state.auth().is_some(),
        demo: state.demo(),
        user: caller.user().cloned(),
        role: caller.role(),
        auth_method: caller.method(),
    })
}

/// Password change request.
#[derive(Debug, Deserialize, ToSchema)]
pub struct PasswordChange {
    /// The current password.
    pub current: String,
    /// The new password (at least 10 characters).
    pub new: String,
}

/// Change your own password. Ends all your sessions, including this one.
#[utoipa::path(post, path = "/api/v1/auth/password", tag = "auth", request_body = PasswordChange,
    responses(
        (status = 204, description = "Changed; log in again"),
        (status = 400, description = "The new password is too short (`invalid`)", body = ApiError),
        (status = 401, description = "The current password is wrong (`bad_credentials`)", body = ApiError),
    ))]
pub async fn change_password(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ClientAddr(addr): ClientAddr,
    ApiJson(req): ApiJson<PasswordChange>,
) -> Result<StatusCode, Problem> {
    let auth = require_auth(&state)?;
    let user = caller
        .user()
        .ok_or_else(|| Problem::unauthenticated("log in first"))?;
    // Guessing the current password (e.g. with a stolen token) is limited
    // like logins.
    let keys = auth::login_keys(&user.username, addr);
    if let Err(blocked) = auth.limiter.check(&keys) {
        if blocked.first {
            audit(
                &state,
                caller.audit(
                    "password.change",
                    &user.username,
                    Outcome::Denied,
                    json!({"reason": "rate_limited"}),
                ),
            )
            .await;
        }
        return Err(Problem::too_many(
            "too_many_attempts",
            "too many wrong passwords; try again later",
            blocked.retry_after,
        ));
    }
    match auth.db.check_login(&user.username, &req.current).await? {
        LoginOutcome::Ok(_) => {}
        _ => {
            auth.limiter.fail(&keys);
            audit(
                &state,
                caller.audit(
                    "password.change",
                    &user.username,
                    Outcome::Denied,
                    json!({"reason": "bad_credentials"}),
                ),
            )
            .await;
            return Err(Problem::unauthorized(
                "bad_credentials",
                "the current password is wrong",
            ));
        }
    }
    // A self-service change keeps the user's API tokens; an admin or CLI
    // reset (account recovery) revokes them.
    let entry = caller.audit("password.change", &user.username, Outcome::Ok, json!({}));
    auth.db
        .change_user(
            &user.username,
            &pipes_db::UserChanges {
                password: Some(req.new.clone()),
                ..Default::default()
            },
            Some(&entry),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Token creation request.
#[derive(Debug, Deserialize, ToSchema)]
pub struct NewToken {
    /// A label, e.g. `grafana-scraper`.
    pub name: String,
    /// Expire after this many days (1-3650); omit for no expiry. A token
    /// created with a token can't outlive it.
    pub expires_in_days: Option<u32>,
}

/// A created token. `token` is shown only now.
#[derive(Debug, Serialize, ToSchema)]
pub struct CreatedToken {
    /// The secret. Send it as `Authorization: Bearer <token>`. It can't be
    /// retrieved again.
    pub token: String,
    /// Its metadata.
    pub meta: ApiToken,
}

/// Which tokens to list.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TokenListQuery {
    /// Admins only: list every user's tokens.
    pub all: Option<bool>,
}

/// Your API tokens (admins: everyone's with `all=true`).
#[utoipa::path(get, path = "/api/v1/auth/tokens", tag = "auth", params(TokenListQuery),
    responses(
        (status = 200, description = "Tokens, newest first", body = Vec<ApiToken>),
        (status = 403, description = "`all=true` without the admin role", body = ApiError),
    ))]
pub async fn list_tokens(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiQuery(q): ApiQuery<TokenListQuery>,
) -> Result<Json<Vec<ApiToken>>, Problem> {
    let auth = require_auth(&state)?;
    let user = caller
        .user()
        .ok_or_else(|| Problem::unauthenticated("log in first"))?;
    let owner = if q.all.unwrap_or(false) {
        require_role(&state, &caller, Role::Admin, "token.list_all", "").await?;
        None
    } else {
        Some(user.id)
    };
    Ok(Json(auth.db.list_api_tokens(owner).await?))
}

/// Create an API token for yourself.
#[utoipa::path(post, path = "/api/v1/auth/tokens", tag = "auth", request_body = NewToken,
    responses(
        (status = 201, description = "Created; copy `token` now", body = CreatedToken),
        (status = 400, description = "Bad name or expiry (`invalid`)", body = ApiError),
    ))]
pub async fn create_token(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiJson(req): ApiJson<NewToken>,
) -> Result<(StatusCode, Json<CreatedToken>), Problem> {
    let auth = require_auth(&state)?;
    let user = caller
        .user()
        .ok_or_else(|| Problem::unauthenticated("log in first"))?;
    let mut expires_at = match req.expires_in_days {
        Some(d) if (1..=MAX_TOKEN_DAYS).contains(&d) => {
            Some(time::OffsetDateTime::now_utc() + time::Duration::days(i64::from(d)))
        }
        Some(_) => {
            return Err(Problem::bad_request(
                "invalid",
                format!("expires_in_days must be 1-{MAX_TOKEN_DAYS}"),
            ));
        }
        None => None,
    };
    // A token can't mint a longer-lived one: cap at the caller's own expiry.
    if let Principal::User {
        credential: pipes_db::Credential::Token { id },
        ..
    } = &caller
        && let Some(limit) = auth.db.api_token(*id).await?.and_then(|t| t.expires_at)
    {
        expires_at = Some(expires_at.map_or(limit, |e| e.min(limit)));
    }
    let (token, meta) = auth
        .db
        .create_api_token(user.id, &req.name, expires_at)
        .await?;
    audit(
        &state,
        caller.audit(
            "token.create",
            &meta.id.to_string(),
            Outcome::Ok,
            json!({"name": meta.name, "expires_at": meta.expires_at.map(|t| t.to_string())}),
        ),
    )
    .await;
    Ok((StatusCode::CREATED, Json(CreatedToken { token, meta })))
}

/// Longest token lifetime, in days.
pub const MAX_TOKEN_DAYS: u32 = 3650;

/// Revoke one of your tokens (admins: anyone's).
#[utoipa::path(delete, path = "/api/v1/auth/tokens/{id}", tag = "auth",
    params(("id" = i64, Path, description = "Token id")),
    responses(
        (status = 204, description = "Revoked"),
        (status = 404, description = "No such (unrevoked) token of yours (`not_found`)", body = ApiError),
    ))]
pub async fn revoke_token(
    State(state): State<AppState>,
    Caller(caller): Caller,
    Path(id): Path<i64>,
) -> Result<StatusCode, Problem> {
    let auth = require_auth(&state)?;
    let user = caller
        .user()
        .ok_or_else(|| Problem::unauthenticated("log in first"))?;
    let owner = (user.role < Role::Admin).then_some(user.id);
    if auth.db.revoke_api_token(id, owner).await? {
        audit(
            &state,
            caller.audit("token.revoke", &id.to_string(), Outcome::Ok, json!({})),
        )
        .await;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(Problem::not_found("not_found", "no such token"))
    }
}

/// User creation request.
#[derive(Debug, Deserialize, ToSchema)]
pub struct NewUser {
    /// Login name: 1-64 letters, digits, `_`, `-` or `.`.
    pub username: String,
    /// Initial password (at least 10 characters).
    pub password: String,
    /// Role.
    pub role: Role,
}

/// User update request; omitted fields are unchanged.
#[derive(Debug, Deserialize, ToSchema)]
pub struct UserUpdate {
    /// New role.
    pub role: Option<Role>,
    /// Disable or enable.
    pub disabled: Option<bool>,
    /// New password (ends the user's sessions).
    pub password: Option<String>,
}

/// Every user (admin).
#[utoipa::path(get, path = "/api/v1/users", tag = "users",
    responses((status = 200, description = "Users by name", body = Vec<User>), (status = 403, body = ApiError)))]
pub async fn list_users(
    State(state): State<AppState>,
    Caller(caller): Caller,
) -> Result<Json<Vec<User>>, Problem> {
    let auth = require_auth(&state)?;
    require_role(&state, &caller, Role::Admin, "user.list", "").await?;
    Ok(Json(auth.db.list_users().await?))
}

/// Create a user (admin).
#[utoipa::path(post, path = "/api/v1/users", tag = "users", request_body = NewUser,
    responses(
        (status = 201, description = "Created", body = User),
        (status = 400, description = "Bad username or password (`invalid`)", body = ApiError),
        (status = 403, body = ApiError),
        (status = 409, description = "The username is taken (`conflict`)", body = ApiError),
    ))]
pub async fn create_user(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiJson(req): ApiJson<NewUser>,
) -> Result<(StatusCode, Json<User>), Problem> {
    let auth = require_auth(&state)?;
    require_role(&state, &caller, Role::Admin, "user.create", &req.username).await?;
    let user = auth
        .db
        .create_user(&req.username, &req.password, req.role)
        .await?;
    audit(
        &state,
        caller.audit(
            "user.create",
            &user.username,
            Outcome::Ok,
            json!({"role": user.role}),
        ),
    )
    .await;
    Ok((StatusCode::CREATED, Json(user)))
}

/// Change a user's role, enabled state or password (admin). All-or-nothing:
/// if any part is invalid, nothing changes. A password set here is an
/// account-recovery reset: it also revokes the user's API tokens.
#[utoipa::path(patch, path = "/api/v1/users/{username}", tag = "users", request_body = UserUpdate,
    params(("username" = String, Path, description = "Username")),
    responses(
        (status = 200, description = "Updated", body = User),
        (status = 400, description = "Invalid, e.g. demoting the last admin (`invalid`)", body = ApiError),
        (status = 403, body = ApiError),
        (status = 404, body = ApiError),
    ))]
pub async fn update_user(
    State(state): State<AppState>,
    Caller(caller): Caller,
    Path(username): Path<String>,
    ApiJson(req): ApiJson<UserUpdate>,
) -> Result<Json<User>, Problem> {
    let auth = require_auth(&state)?;
    require_role(&state, &caller, Role::Admin, "user.update", &username).await?;
    let detail = json!({"role": req.role, "disabled": req.disabled, "password_changed": req.password.is_some()});
    let entry = caller.audit("user.update", &username, Outcome::Ok, detail);
    let changes = pipes_db::UserChanges {
        role: req.role,
        disabled: req.disabled,
        revoke_tokens: req.password.is_some(),
        password: req.password,
    };
    Ok(Json(
        auth.db
            .change_user(&username, &changes, Some(&entry))
            .await?,
    ))
}

/// Delete a user with their sessions and tokens (admin).
#[utoipa::path(delete, path = "/api/v1/users/{username}", tag = "users",
    params(("username" = String, Path, description = "Username")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 400, description = "The last admin can't be deleted (`invalid`)", body = ApiError),
        (status = 403, body = ApiError),
        (status = 404, body = ApiError),
    ))]
pub async fn delete_user(
    State(state): State<AppState>,
    Caller(caller): Caller,
    Path(username): Path<String>,
) -> Result<StatusCode, Problem> {
    let auth = require_auth(&state)?;
    require_role(&state, &caller, Role::Admin, "user.delete", &username).await?;
    let entry = caller.audit("user.delete", &username, Outcome::Ok, json!({}));
    auth.db.delete_user(&username, Some(&entry)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// [`Principal::require`], auditing the refusal.
pub async fn require_role(
    state: &AppState,
    caller: &Principal,
    role: Role,
    action: &str,
    target: &str,
) -> Result<(), Problem> {
    if let Err(p) = caller.require(role) {
        audit(
            state,
            caller.audit(
                action,
                target,
                Outcome::Denied,
                json!({"reason": "forbidden", "needs": role}),
            ),
        )
        .await;
        return Err(p);
    }
    Ok(())
}

/// Audit log filters.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AuditParams {
    /// At most this many (default 100, at most 1000).
    pub limit: Option<i64>,
    /// Only entries older than this id (page backwards).
    pub before: Option<i64>,
    /// Only this action, e.g. `login` or `topic.publish`.
    pub action: Option<String>,
    /// Only this actor.
    pub actor: Option<String>,
}

/// The audit log, newest first (admin).
#[utoipa::path(get, path = "/api/v1/audit", tag = "users", params(AuditParams),
    responses((status = 200, description = "Entries, newest first", body = Vec<AuditRecord>), (status = 403, body = ApiError)))]
pub async fn list_audit(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiQuery(q): ApiQuery<AuditParams>,
) -> Result<Json<Vec<AuditRecord>>, Problem> {
    // Demo mode with a database has an audit log but no auth.
    let db = state.db().ok_or_else(|| {
        Problem::unavailable(
            "no_database",
            "the audit log needs a database (--database-url); without one it goes to the process log",
        )
    })?;
    require_role(&state, &caller, Role::Admin, "audit.list", "").await?;
    Ok(Json(
        db.list_audit(&AuditQuery {
            limit: q.limit,
            before: q.before,
            action: q.action,
            actor: q.actor,
        })
        .await?,
    ))
}
