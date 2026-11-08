use std::{fmt, sync::Arc};

use pipes_core::{build_info::BuildInfo, hub::Hub};
use tokio_util::sync::CancellationToken;

/// Renders the process's metrics in Prometheus text format. Injected so the API
/// does not depend on a particular metrics exporter.
pub type MetricsRender = Arc<dyn Fn() -> String + Send + Sync>;

/// Shared state handed to every handler. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    build: BuildInfo,
    hub: Hub,
    metrics: MetricsRender,
    shutdown: CancellationToken,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState")
            .field("build", &self.inner.build)
            .finish_non_exhaustive()
    }
}

impl AppState {
    /// State for a server reporting `build`, serving data from `hub`. Streaming
    /// handlers end when `shutdown` is cancelled.
    pub fn new(
        build: BuildInfo,
        hub: Hub,
        metrics: MetricsRender,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                build,
                hub,
                metrics,
                shutdown,
            }),
        }
    }

    /// Facts about the running build.
    pub fn build(&self) -> &BuildInfo {
        &self.inner.build
    }

    /// The topic hub.
    pub fn hub(&self) -> &Hub {
        &self.inner.hub
    }

    /// Current metrics in Prometheus text format.
    pub fn render_metrics(&self) -> String {
        (self.inner.metrics)()
    }

    /// Cancelled when the server starts shutting down.
    pub fn shutdown(&self) -> &CancellationToken {
        &self.inner.shutdown
    }
}
