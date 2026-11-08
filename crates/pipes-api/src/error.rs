//! API errors: a status code plus a small JSON body. Messages are written for
//! clients and never include raw internal error chains.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use pipes_core::hub::HubError;
use serde::Serialize;
use utoipa::ToSchema;

/// Body of every error response.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ApiError {
    /// Stable, machine-readable code: `unauthenticated`, `bad_credentials`,
    /// `forbidden`, `csrf`, `too_many_attempts`, `auth_disabled`,
    /// `auth_unavailable`, `invalid`, `conflict`, `not_found`, `internal`,
    /// `not_allowlisted`, `too_many_writes`, `audit_unavailable`,
    /// `invalid_message`, `no_endpoint`, `bus_error`, `timeout`,
    /// `invalid_topic`, `invalid_query`,
    /// `unknown_topic`, `no_publisher`, `topic_failed`, `closed`,
    /// `too_many_topics`, `too_many_streams`, `shutting_down`, `no_sample`,
    /// `no_schema`, `no_route`, `method_not_allowed`.
    pub error: &'static str,
    /// Human-readable explanation.
    pub message: String,
}

/// An error with its HTTP status.
#[derive(Debug, Clone)]
pub struct Problem {
    status: StatusCode,
    body: ApiError,
    retry_after: Option<std::time::Duration>,
}

impl Problem {
    fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiError {
                error,
                message: message.into(),
            },
            retry_after: None,
        }
    }

    /// 401: no valid credentials.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    /// 401 with a specific code (e.g. `bad_credentials`).
    pub fn unauthorized(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, error, message)
    }

    /// 403: authenticated, but not allowed.
    pub fn forbidden(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, error, message)
    }

    /// 502: a downstream system (the robot bus) failed.
    pub fn bad_gateway(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, error, message)
    }

    /// 409: conflicts with existing state.
    pub fn conflict(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, error, message)
    }

    /// 429 with a `Retry-After` header.
    pub fn too_many(
        error: &'static str,
        message: impl Into<String>,
        retry_after: std::time::Duration,
    ) -> Self {
        Self {
            retry_after: Some(retry_after),
            ..Self::new(StatusCode::TOO_MANY_REQUESTS, error, message)
        }
    }

    /// 500 for internal failures; the cause must be logged by the caller.
    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error; see the server log",
        )
    }

    /// The machine-readable code.
    pub fn code(&self) -> &'static str {
        self.body.error
    }

    /// 400: the request is malformed.
    pub fn bad_request(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error, message)
    }

    /// 404: nothing to serve for this request.
    pub fn not_found(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, error, message)
    }

    /// 503: the topic can't be served right now (e.g. its schema is unavailable).
    pub fn unavailable(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, error, message)
    }

    /// 504: nothing arrived within the requested wait.
    pub fn timeout(error: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, error, message)
    }

    /// The HTTP status.
    pub fn status(&self) -> StatusCode {
        self.status
    }
}

impl From<HubError> for Problem {
    fn from(e: HubError) -> Self {
        match e {
            HubError::InvalidTopic(_) => Self::bad_request("invalid_topic", e.to_string()),
            HubError::ShuttingDown => Self::unavailable("shutting_down", e.to_string()),
            HubError::TooManyTopics(_) => Self::unavailable("too_many_topics", e.to_string()),
        }
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let mut res = (self.status, Json(self.body)).into_response();
        if let Some(d) = self.retry_after
            && let Ok(v) = axum::http::HeaderValue::from_str(&d.as_secs().max(1).to_string())
        {
            res.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
        }
        res
    }
}

impl From<pipes_db::DbError> for Problem {
    fn from(e: pipes_db::DbError) -> Self {
        match e {
            pipes_db::DbError::Invalid(m) => Self::bad_request("invalid", m),
            pipes_db::DbError::Conflict(m) => Self::conflict("conflict", m),
            pipes_db::DbError::NotFound => Self::not_found("not_found", "no such item"),
            pipes_db::DbError::Sqlx(err) => {
                tracing::error!(%err, "database error");
                Self::internal()
            }
        }
    }
}
