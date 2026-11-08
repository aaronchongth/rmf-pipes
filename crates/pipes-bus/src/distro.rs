//! Compile-time ROS 2 distro selection.
//!
//! hiroz's distro features are mutually exclusive but hiroz does not enforce it:
//! with several enabled it silently picks one by priority. We refuse to build
//! instead, so a stray `--features jazzy` on top of the default fails loudly.

#[cfg(not(any(
    feature = "jazzy",
    feature = "kilted",
    feature = "lyrical",
    feature = "rolling"
)))]
compile_error!(
    "pipes-bus: select exactly one ROS 2 distro feature: jazzy, kilted, lyrical or rolling"
);

#[cfg(any(
    all(
        feature = "jazzy",
        any(feature = "kilted", feature = "lyrical", feature = "rolling")
    ),
    all(feature = "kilted", any(feature = "lyrical", feature = "rolling")),
    all(feature = "lyrical", feature = "rolling"),
))]
compile_error!(
    "pipes-bus: more than one ROS 2 distro feature is enabled; use --no-default-features --features <distro>"
);

/// The ROS 2 distro this build speaks, e.g. `"lyrical"`.
pub const ROS_DISTRO: &str = if cfg!(feature = "jazzy") {
    "jazzy"
} else if cfg!(feature = "kilted") {
    "kilted"
} else if cfg!(feature = "lyrical") {
    "lyrical"
} else {
    "rolling"
};

/// Build facts for this binary, including the selected distro.
pub const BUILD_INFO: pipes_core::build_info::BuildInfo =
    pipes_core::build_info::BuildInfo::new(ROS_DISTRO);
