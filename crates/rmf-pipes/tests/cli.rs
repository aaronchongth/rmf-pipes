//! End-to-end checks on the command line surface of the real binary.

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rmf-pipes");

#[test]
fn help_never_prints_environment_values() {
    let marker = "env-value-that-must-not-leak-4d1c";
    let out = Command::new(BIN)
        .env_clear()
        .arg("--help")
        .env("RMF_PIPES_LISTEN", format!("127.0.0.1:1#{marker}"))
        .env("RMF_PIPES_SHUTDOWN_DRAIN", marker)
        .output()
        .unwrap();
    assert!(out.status.success());
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("RMF_PIPES_LISTEN"),
        "help should name the env var:\n{help}"
    );
    assert!(!help.contains(marker), "help leaked an env value:\n{help}");
}

#[test]
fn version_flag_works() {
    let out = Command::new(BIN)
        .env_clear()
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        String::from_utf8(out.stdout)
            .unwrap()
            .contains(env!("CARGO_PKG_VERSION"))
    );
}
