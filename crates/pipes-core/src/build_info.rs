//! Static facts about this build, reported by `/healthz`.

use serde::Serialize;

/// Facts about the running binary. The ROS distro is supplied by the bus crate,
/// which is the only crate that knows which hiroz distro feature was compiled in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildInfo {
    /// Crate version of rmf-pipes.
    pub version: &'static str,
    /// ROS 2 distro whose rmw_zenoh conventions hiroz was compiled for.
    pub ros_distro: &'static str,
}

impl BuildInfo {
    /// Build info for this crate version and the given distro.
    pub const fn new(ros_distro: &'static str) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            ros_distro,
        }
    }
}
