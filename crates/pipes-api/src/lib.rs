//! HTTP layer of rmf-pipes. Depends only on `pipes-core`, never on hiroz, so the
//! whole API can be exercised in-process with a fake bus.
//!
//! - `/api/v1/*`, `/healthz`, `/metrics`: documented in `/openapi.json`
//!   ([`api`]); Swagger UI at `/docs/`.
//! - `/` and `/ui/*`: the introspection dashboard ([`ui`]).

pub mod accounts;
pub mod api;
pub mod auth;
pub mod error;
pub mod health;
pub mod metrics;
pub mod serve;
mod state;
pub mod ui;
pub mod writes;

use axum::{Json, Router, http::StatusCode, response::IntoResponse};
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;
use utoipa_swagger_ui::{Config, SwaggerUi};

pub use state::{AppState, DEFAULT_MAX_STREAMS, MetricsRender};

/// Build the full HTTP router.
pub fn router(state: AppState) -> Router {
    let (api, openapi) = api::router();
    Router::new()
        .merge(api)
        .merge(ui::routes())
        .merge(
            SwaggerUi::new("/docs")
                .url("/openapi.json", openapi)
                // Swagger UI's default validator badge loads an image from
                // validator.swagger.io (leaking the spec URL); air-gapped
                // sites must not reach out.
                .config(Config::from("/openapi.json").validator_url("none")),
        )
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ))
        // Bounds every JSON body (messages for the write path included).
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024))
        // 5xx here are expected outcomes (a client's `wait_ms` ran out, a
        // topic can't be decoded); handlers log real problems themselves.
        .layer(TraceLayer::new_for_http().on_failure(DefaultOnFailure::new().level(Level::DEBUG)))
        .with_state(state)
}

/// Everything is read-only (GET); other methods get a JSON 405.
async fn method_not_allowed() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(error::ApiError {
            error: "method_not_allowed",
            message: "this endpoint doesn't support that method; see /docs/".into(),
        }),
    )
}

/// Unknown paths get a JSON error, like every other API error.
async fn no_route() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(error::ApiError {
            error: "no_route",
            message: "no such endpoint; see /docs/".into(),
        }),
    )
}

#[cfg(test)]
mod api_tests;
#[cfg(test)]
mod auth_tests;
#[cfg(test)]
mod writes_tests;

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use pipes_core::{
        build_info::BuildInfo,
        fake::FakeBus,
        hub::{Hub, HubConfig},
    };
    use tokio_util::sync::CancellationToken;

    use crate::AppState;

    /// App state over a fresh [`FakeBus`], reporting distro `testdistro`.
    pub fn state() -> AppState {
        state_with_config(FakeBus::new(), HubConfig::default()).0
    }

    /// App state over `bus` with `cfg`, plus its hub for direct manipulation.
    pub fn state_with_config(bus: Arc<FakeBus>, cfg: HubConfig) -> (AppState, Hub) {
        let hub = Hub::new(bus, cfg);
        let state = AppState::new(
            BuildInfo::new("testdistro"),
            hub.clone(),
            Arc::new(|| "# test metrics\n".to_owned()),
            CancellationToken::new(),
        );
        (state, hub)
    }
}
