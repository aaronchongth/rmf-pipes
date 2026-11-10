use std::{fmt, sync::Arc};

use pipes_core::{build_info::BuildInfo, hub::Hub};

use crate::{
    auth::AuthService,
    writes::{WriteLimiter, WritePolicy},
};
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
    auth: Option<Arc<AuthService>>,
    db: Option<pipes_db::Db>,
    demo: bool,
    recorder: Option<pipes_core::recorder::Recorder>,
    writes: Arc<WritePolicy>,
    write_limiter: Arc<WriteLimiter>,
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
                auth: None,
                db: None,
                demo: false,
                recorder: None,
                writes: Arc::new(WritePolicy::default()),
                write_limiter: Arc::new(WriteLimiter::default()),
            }),
        }
    }

    /// Enable authentication (see [`crate::auth`]), with its database also
    /// used for the audit log. Without it the API is anonymous and read-only,
    /// unless [`AppState::with_demo`].
    #[must_use]
    pub fn with_auth(self, auth: AuthService) -> Self {
        self.rebuild(|i| {
            i.db = Some(auth.db.clone());
            i.auth = Some(Arc::new(auth));
        })
    }

    /// Use `db` for the audit log (and recordings) without enabling
    /// authentication.
    #[must_use]
    pub fn with_database(self, db: pipes_db::Db) -> Self {
        self.rebuild(|i| i.db = Some(db))
    }

    /// Demo mode: authentication is off and every caller acts as the
    /// anonymous demo user with the admin role (see [`crate::auth`]). The
    /// binary only allows it on a loopback address.
    #[must_use]
    pub fn with_demo(self) -> Self {
        self.rebuild(|i| i.demo = true)
    }

    /// Serve the recorder's rules, streams and records (see
    /// [`crate::recording`]).
    #[must_use]
    pub fn with_recorder(self, recorder: pipes_core::recorder::Recorder) -> Self {
        self.rebuild(|i| i.recorder = Some(recorder))
    }

    /// The recorder, when configured.
    pub fn recorder(&self) -> Option<&pipes_core::recorder::Recorder> {
        self.inner.recorder.as_ref()
    }

    /// The database, when one is configured (with or without auth).
    pub fn db(&self) -> Option<&pipes_db::Db> {
        self.inner.db.as_ref()
    }

    /// Whether demo mode is on.
    pub fn demo(&self) -> bool {
        self.inner.demo && self.inner.auth.is_none()
    }

    /// Set the write allowlist (default: nothing may be written).
    #[must_use]
    pub fn with_writes(self, policy: WritePolicy) -> Self {
        self.rebuild(|i| i.writes = Arc::new(policy))
    }

    /// The write allowlist.
    pub fn writes(&self) -> &WritePolicy {
        &self.inner.writes
    }

    /// Per-user write rate limiting.
    pub fn write_limiter(&self) -> &WriteLimiter {
        &self.inner.write_limiter
    }

    /// The auth service, when enabled.
    pub fn auth(&self) -> Option<&AuthService> {
        self.inner.auth.as_deref()
    }

    fn rebuild(self, f: impl FnOnce(&mut Inner)) -> Self {
        let mut inner = Inner {
            build: self.inner.build.clone(),
            hub: self.inner.hub.clone(),
            metrics: self.inner.metrics.clone(),
            shutdown: self.inner.shutdown.clone(),
            streams: self.inner.streams.clone(),
            auth: self.inner.auth.clone(),
            db: self.inner.db.clone(),
            demo: self.inner.demo,
            recorder: self.inner.recorder.clone(),
            writes: self.inner.writes.clone(),
            write_limiter: self.inner.write_limiter.clone(),
        };
        f(&mut inner);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Cap concurrent SSE streams at `max` (default [`DEFAULT_MAX_STREAMS`]).
    #[must_use]
    pub fn with_max_streams(self, max: usize) -> Self {
        self.rebuild(|i| i.streams = Arc::new(Semaphore::new(max)))
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
