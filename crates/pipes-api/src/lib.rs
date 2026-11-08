//! HTTP layer of rmf-pipes. Depends only on `pipes-core`, never on hiroz, so the
//! whole API can be exercised in-process with a fake bus.

pub mod health;
pub mod metrics;
pub mod serve;
mod state;

use axum::Router;
use tower_http::trace::TraceLayer;

pub use state::{AppState, MetricsRender};

/// Build the full HTTP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(health::routes())
        .merge(metrics::routes())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

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
        state_with(FakeBus::new()).0
    }

    /// App state over `bus`, plus its hub for direct manipulation.
    pub fn state_with(bus: Arc<FakeBus>) -> (AppState, Hub) {
        let hub = Hub::new(bus, HubConfig::default());
        let state = AppState::new(
            BuildInfo::new("testdistro"),
            hub.clone(),
            Arc::new(|| "# test metrics\n".to_owned()),
            CancellationToken::new(),
        );
        (state, hub)
    }
}
