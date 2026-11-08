//! Command-line and environment configuration.
//!
//! Every option can be set by flag or by `RMF_PIPES_*` environment variable.
//! Every env-backed argument sets `hide_env_values`: `--help` must never echo
//! the current value of an environment variable, because some of them will hold
//! secrets (database URLs, tokens). This is a lesson from rmf-console, and a
//! test below fails if a new argument forgets it.
//!
//! clap echoes the offending value in *parse* errors, so an argument that may
//! carry a secret must be parsed as a plain string (no fallible
//! `value_parser`) and validated afterwards with a redacted error. The first
//! such argument arrives with Postgres in phase 3, together with its test.

use std::{net::SocketAddr, time::Duration};

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Parser)]
#[command(name = "rmf-pipes", version, about)]
pub struct Config {
    /// Address for the HTTP server. Use port 0 to pick a free port; the bound
    /// address is logged as `listening` with field `addr`.
    #[arg(
        long,
        env = "RMF_PIPES_LISTEN",
        hide_env_values = true,
        default_value = "127.0.0.1:18080"
    )]
    pub listen: SocketAddr,

    /// Log output format.
    #[arg(long, env = "RMF_PIPES_LOG_FORMAT", hide_env_values = true, value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,

    /// After a shutdown signal, how long open connections (e.g. SSE streams)
    /// may take to finish before they are abandoned.
    #[arg(long, env = "RMF_PIPES_SHUTDOWN_DRAIN", hide_env_values = true, value_parser = humantime::parse_duration, default_value = "5s")]
    pub shutdown_drain: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable, for terminals.
    Pretty,
    /// One JSON object per line, for log collectors and tests.
    Json,
}

#[cfg(test)]
mod tests {
    use clap::{Command, CommandFactory};

    use super::Config;

    #[test]
    fn clap_definition_is_valid() {
        Config::command().debug_assert();
    }

    /// Defaults are read from the definition, not by parsing, so the test does
    /// not depend on `RMF_PIPES_*` variables in the caller's environment.
    #[test]
    fn defaults() {
        let cmd = Config::command();
        let default = |id: &str| -> String {
            let arg = cmd.get_arguments().find(|a| a.get_id() == id).unwrap();
            arg.get_default_values()[0].to_string_lossy().into_owned()
        };
        assert_eq!(default("listen"), "127.0.0.1:18080");
        assert_eq!(default("log_format"), "pretty");
        assert_eq!(default("shutdown_drain"), "5s");
    }

    #[test]
    fn every_env_backed_argument_hides_its_value() {
        // Guards the rule in the module docs; the end-to-end check lives in
        // tests/cli.rs.
        fn check(cmd: &Command, checked: &mut usize) {
            for arg in cmd.get_arguments() {
                if arg.get_env().is_some() {
                    *checked += 1;
                    assert!(
                        arg.is_hide_env_values_set(),
                        "`{}` --{} reads an env var but does not set hide_env_values",
                        cmd.get_name(),
                        arg.get_id()
                    );
                }
            }
            for sub in cmd.get_subcommands() {
                check(sub, checked);
            }
        }
        let mut checked = 0;
        check(&Config::command(), &mut checked);
        assert!(
            checked >= 3,
            "expected env-backed arguments, checked {checked}"
        );
    }
}
