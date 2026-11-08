//! HTTP layer of rmf-pipes. Depends only on `pipes-core`, never on hiroz, so the
//! whole API can be exercised in-process with a fake bus.

pub mod health;
pub mod serve;
mod state;

use axum::Router;
use tower_http::trace::TraceLayer;

pub use state::AppState;

/// Build the full HTTP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(health::routes())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
