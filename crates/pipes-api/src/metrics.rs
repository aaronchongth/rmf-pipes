//! `GET /metrics` in Prometheus text format.

use axum::{Router, extract::State, http::header, response::IntoResponse, routing::get};

use crate::AppState;

/// Routes for the metrics endpoint.
pub fn routes() -> Router<AppState> {
    Router::new().route("/metrics", get(metrics))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.render_metrics(),
    )
}
