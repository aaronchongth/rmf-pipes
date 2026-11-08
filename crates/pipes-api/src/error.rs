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
    /// Stable, machine-readable code: `invalid_topic`, `invalid_query`,
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
}

impl Problem {
    fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiError {
                error,
                message: message.into(),
            },
        }
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
        (self.status, Json(self.body)).into_response()
    }
}
