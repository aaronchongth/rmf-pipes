use std::sync::Arc;

use pipes_core::build_info::BuildInfo;

/// Shared state handed to every handler. Cheap to clone.
#[derive(Debug, Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    build: BuildInfo,
}

impl AppState {
    /// State for a server reporting `build`.
    pub fn new(build: BuildInfo) -> Self {
        Self {
            inner: Arc::new(Inner { build }),
        }
    }

    /// Facts about the running build.
    pub fn build(&self) -> &BuildInfo {
        &self.inner.build
    }
}
