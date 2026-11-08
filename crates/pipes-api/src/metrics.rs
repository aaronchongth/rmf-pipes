//! `GET /metrics` in Prometheus text format.

use axum::{extract::State, http::header, response::IntoResponse};

use crate::AppState;

/// Prometheus metrics (text exposition format 0.0.4). See the hub's module
/// docs for the metric names.
#[utoipa::path(get, path = "/metrics", tag = "ops",
    responses((status = 200, description = "Metrics in Prometheus text format", content_type = "text/plain", body = String)))]
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.render_metrics(),
    )
}
