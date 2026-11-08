//! Helpers for interop tests against a real ROS 2 install with rmw_zenoh.
//!
//! Interop tests are `#[ignore]`d so `pixi run test` stays hermetic; run them
//! with `pixi run interop`. When no suitable install is found they print a
//! `SKIP` line and pass, unless `PIPES_INTEROP_REQUIRED=1` (set in CI's interop
//! job), which turns the skip into a failure.

use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

/// A ROS 2 install with rmw_zenoh, found under `/opt/ros/$ROS_DISTRO`.
#[derive(Debug, Clone)]
pub struct RosEnv {
    /// Distro name, e.g. `lyrical`.
    pub distro: String,
    /// The install prefix, e.g. `/opt/ros/lyrical`.
    pub prefix: PathBuf,
}

impl RosEnv {
    /// Find the install for `$ROS_DISTRO` (default: the distro this build
    /// targets), or explain why there is none.
    pub fn detect() -> Result<Self, String> {
        let distro = std::env::var("ROS_DISTRO")
            .unwrap_or_else(|_| pipes_bus::distro::ROS_DISTRO.to_owned());
        if distro != pipes_bus::distro::ROS_DISTRO {
            return Err(format!(
                "ROS_DISTRO={distro} but this build targets {}; rebuild with --features {distro}",
                pipes_bus::distro::ROS_DISTRO
            ));
        }
        let prefix = PathBuf::from("/opt/ros").join(&distro);
        for needed in ["setup.bash", "lib/rmw_zenoh_cpp/rmw_zenohd", "bin/ros2"] {
            if !prefix.join(needed).exists() {
                return Err(format!("{} not found", prefix.join(needed).display()));
            }
        }
        Ok(Self { distro, prefix })
    }

    /// [`RosEnv::detect`], or `None` after printing a skip notice. Panics
    /// instead when `PIPES_INTEROP_REQUIRED=1`.
    pub fn require_or_skip(test: &str) -> Option<Self> {
        match Self::detect() {
            Ok(env) => Some(env),
            Err(why) if std::env::var("PIPES_INTEROP_REQUIRED").as_deref() == Ok("1") => {
                panic!("{test}: interop required but unavailable: {why}")
            }
            Err(why) => {
                eprintln!("SKIP {test}: {why}");
                None
            }
        }
    }

    /// Start `rmw_zenohd` listening on `127.0.0.1:<port>`.
    pub fn router(&self, port: u16) -> RosProcess {
        let zenohd = self.prefix.join("lib/rmw_zenoh_cpp/rmw_zenohd");
        let proc = self.spawn(
            "rmw_zenohd",
            &format!("exec {}", zenohd.display()),
            &format!(
                r#"listen/endpoints=["tcp/127.0.0.1:{port}"];scouting/multicast/enabled=false"#
            ),
        );
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        for _ in 0..250 {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
                return proc;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("rmw_zenohd did not listen on {port}");
    }

    /// Run `ros2 <args>` (e.g. `topic pub ...`) on rmw_zenoh, connected to the
    /// router on `port`. Arguments are passed through the shell, so quote them.
    pub fn ros2(&self, port: u16, args: &str) -> RosProcess {
        self.spawn(
            "ros2",
            &format!("exec ros2 {args}"),
            &format!(
                r#"connect/endpoints=["tcp/127.0.0.1:{port}"];scouting/multicast/enabled=false"#
            ),
        )
    }

    fn spawn(&self, label: &str, script: &str, zenoh_override: &str) -> RosProcess {
        let setup = self.prefix.join("setup.bash");
        let log_dir = std::env::temp_dir().join("rmf-pipes-interop-ros-logs");
        let child = Command::new("bash")
            .arg("-c")
            .arg(format!("source {} && {script}", setup.display()))
            .env("RMW_IMPLEMENTATION", "rmw_zenoh_cpp")
            .env("ROS_DOMAIN_ID", "0")
            .env("ZENOH_CONFIG_OVERRIDE", zenoh_override)
            .env("ROS_LOG_DIR", log_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {label}: {e}"));
        RosProcess {
            label: label.to_owned(),
            child,
        }
    }
}

/// A child process that is killed and reaped on drop.
#[derive(Debug)]
pub struct RosProcess {
    label: String,
    child: Child,
}

impl RosProcess {
    /// Fail the test if the process has already exited.
    pub fn assert_running(&mut self) {
        if let Ok(Some(status)) = self.child.try_wait() {
            panic!("{} exited early: {status}", self.label);
        }
    }
}

impl Drop for RosProcess {
    fn drop(&mut self) {
        // `exec` in the script means the child is the ROS process itself.
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        for _ in 0..50 {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
