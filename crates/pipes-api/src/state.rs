use std::{fmt, sync::Arc};

use pipes_core::{build_info::BuildInfo, hub::Hub};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
    streams: Arc<Semaphore>,
}

/// Default for [`AppState::with_max_streams`].
pub const DEFAULT_MAX_STREAMS: usize = 256;

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
                streams: Arc::new(Semaphore::new(DEFAULT_MAX_STREAMS)),
            }),
        }
    }

    /// Cap concurrent SSE streams at `max` (default [`DEFAULT_MAX_STREAMS`]).
    #[must_use]
    pub fn with_max_streams(self, max: usize) -> Self {
        let inner = Inner {
            build: self.inner.build.clone(),
            hub: self.inner.hub.clone(),
            metrics: self.inner.metrics.clone(),
            shutdown: self.inner.shutdown.clone(),
            streams: Arc::new(Semaphore::new(max)),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A slot for one SSE stream, held for the stream's lifetime, or `None`
    /// when the cap is reached.
    pub fn stream_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.streams.clone().try_acquire_owned().ok()
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
