//! Boots the real binary, talks HTTP to it, and shuts it down with SIGTERM.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread::JoinHandle,
    time::{Duration, Instant},
};

const BIN: &str = env!("CARGO_BIN_EXE_rmf-pipes");

/// Kills and reaps the child on drop, so a failing test never leaks a process
/// holding a port.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Server {
    child: ChildGuard,
    addr: String,
    /// Log lines consumed while waiting for `listening`, in order.
    seen: Vec<serde_json::Value>,
    logs: mpsc::Receiver<serde_json::Value>,
    reader: Option<JoinHandle<()>>,
}

impl Server {
    /// Start on a free port with a clean environment and wait for the
    /// `listening` log line.
    fn start() -> Self {
        let mut child = ChildGuard(
            Command::new(BIN)
                .env_clear()
                .env("RUST_LOG", "info")
                .args([
                    "--listen",
                    "127.0.0.1:0",
                    "--log-format",
                    "json",
                    "--shutdown-drain",
                    "2s",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (tx, logs) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    let _ = tx.send(v);
                }
            }
        });
        let mut seen = Vec::new();
        let addr = loop {
            let line = logs
                .recv_timeout(Duration::from_secs(20))
                .expect("server never logged `listening`");
            seen.push(line.clone());
            if line["message"] == "listening" {
                break line["addr"].as_str().unwrap().to_owned();
            }
        };
        Self {
            child,
            addr,
            seen,
            logs,
            reader: Some(reader),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn sigterm(&self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.0.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Wait for exit, then collect every remaining log line (the reader thread
    /// ends when stdout closes, so nothing is lost to a race).
    fn wait_exit(
        &mut self,
        timeout: Duration,
    ) -> (std::process::ExitStatus, Vec<serde_json::Value>) {
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = self.child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "server did not exit in {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        self.reader.take().unwrap().join().unwrap();
        (status, self.logs.try_iter().collect())
    }
}

#[tokio::test]
async fn serves_healthz_and_stops_cleanly_on_sigterm() {
    let mut server = Server::start();

    let health: serde_json::Value = reqwest::get(server.url("/healthz"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["ros_distro"], pipes_bus::distro::ROS_DISTRO);
    assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));

    server.sigterm();
    let (status, logs) = server.wait_exit(Duration::from_secs(10));
    assert!(status.success(), "exit status {status:?}");
    assert!(
        logs.iter()
            .any(|l| l["message"] == "stopped" && l["outcome"] == "Drained"),
        "expected a `stopped` log with outcome Drained, got {logs:?}"
    );
}

/// Regression test: a SIGTERM sent the instant `listening` appears must be
/// handled gracefully, so the handlers must be installed before the
/// announcement. Checked by log order, which is deterministic; racing a real
/// signal against handler installation is not.
#[test]
fn signal_handlers_are_installed_before_listening_is_logged() {
    let mut server = Server::start();
    server.sigterm();
    let (status, logs) = server.wait_exit(Duration::from_secs(10));
    assert!(status.success(), "exit status {status:?}");
    let pos = |msg: &str| {
        server
            .seen
            .iter()
            .chain(&logs)
            .position(|l| l["message"] == msg)
    };
    let installed = pos("signal handlers installed").expect("no `signal handlers installed` log");
    let listening = pos("listening").expect("no `listening` log");
    assert!(
        installed < listening,
        "handlers installed after `listening` was logged"
    );
}
