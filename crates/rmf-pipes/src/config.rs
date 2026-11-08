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

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, ValueEnum};
use pipes_core::pattern::TopicPattern;

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

    /// zenoh endpoints to connect to (comma-separated). The default is the
    /// local `rmw_zenohd` router.
    #[arg(
        long,
        env = "RMF_PIPES_ZENOH_CONNECT",
        hide_env_values = true,
        value_delimiter = ',',
        default_value = "tcp/127.0.0.1:7447"
    )]
    pub zenoh_connect: Vec<String>,

    /// zenoh session mode.
    #[arg(long, env = "RMF_PIPES_ZENOH_MODE", hide_env_values = true, value_enum, default_value_t = ZenohMode::Client)]
    pub zenoh_mode: ZenohMode,

    /// Optional zenoh JSON5 config file, applied before the options above.
    #[arg(long, env = "RMF_PIPES_ZENOH_CONFIG", hide_env_values = true)]
    pub zenoh_config: Option<PathBuf>,

    /// ROS domain id. Reads the standard `ROS_DOMAIN_ID` variable.
    #[arg(
        long,
        env = "ROS_DOMAIN_ID",
        hide_env_values = true,
        default_value_t = 0
    )]
    pub domain_id: usize,

    /// Name of the ROS node rmf-pipes appears as.
    #[arg(
        long,
        env = "RMF_PIPES_NODE_NAME",
        hide_env_values = true,
        default_value = "rmf_pipes"
    )]
    pub node_name: String,

    /// Topic patterns to subscribe to permanently while they have publishers
    /// (comma-separated; `*` is one segment, `**` any number), e.g.
    /// `fleet/*/state,door_states`. Other topics are subscribed on demand.
    #[arg(
        long,
        env = "RMF_PIPES_INGEST",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub ingest: Vec<TopicPattern>,

    /// How long an unwatched on-demand topic stays subscribed.
    #[arg(long, env = "RMF_PIPES_IDLE_TIMEOUT", hide_env_values = true, value_parser = humantime::parse_duration, default_value = "30s")]
    pub idle_timeout: Duration,

    /// Per-topic rate cap in samples per second; 0 disables it. Within each
    /// interval only the newest sample is delivered.
    #[arg(long, env = "RMF_PIPES_MAX_RATE_HZ", hide_env_values = true, value_parser = parse_rate, default_value_t = 20.0)]
    pub max_rate_hz: f64,

    /// Per-subscription queue between zenoh and the hub. When the hub falls
    /// behind, the oldest samples are dropped (ROS keep-last semantics).
    #[arg(long, env = "RMF_PIPES_QUEUE_DEPTH", hide_env_values = true, value_parser = clap::value_parser!(u32).range(1..=10_000), default_value_t = 16)]
    pub queue_depth: u32,

    /// Samples with a larger serialized payload are reported without `data`.
    #[arg(long, env = "RMF_PIPES_MAX_PAYLOAD_BYTES", hide_env_values = true, default_value_t = 1024 * 1024)]
    pub max_payload_bytes: usize,

    /// How long to wait for a publisher's type description when subscribing.
    #[arg(long, env = "RMF_PIPES_DISCOVERY_TIMEOUT", hide_env_values = true, value_parser = humantime::parse_duration, default_value = "5s")]
    pub discovery_timeout: Duration,
}

/// `--max-rate-hz`: 0 (unlimited) or a finite rate between 0.001 and 1e6 Hz.
fn parse_rate(s: &str) -> Result<f64, String> {
    let hz: f64 = s.parse().map_err(|e| format!("{e}"))?;
    if hz == 0.0 || (hz.is_finite() && (0.001..=1e6).contains(&hz)) {
        Ok(hz)
    } else {
        Err("must be 0 (unlimited) or between 0.001 and 1000000".into())
    }
}

/// zenoh session mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ZenohMode {
    /// Connect through a router (`rmw_zenohd`), as rmw_zenoh nodes do.
    Client,
    /// Peer-to-peer.
    Peer,
}

impl ZenohMode {
    /// The zenoh config value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Peer => "peer",
        }
    }
}

impl Config {
    /// Bus settings.
    pub fn bus(&self) -> pipes_bus::BusConfig {
        pipes_bus::BusConfig {
            connect: self.zenoh_connect.clone(),
            mode: self.zenoh_mode.as_str().to_owned(),
            domain_id: self.domain_id,
            node_name: self.node_name.clone(),
            zenoh_config: self.zenoh_config.clone(),
            discovery_timeout: self.discovery_timeout,
            queue_depth: self.queue_depth as usize,
        }
    }

    /// Hub settings.
    pub fn hub(&self) -> pipes_core::hub::HubConfig {
        pipes_core::hub::HubConfig {
            patterns: self.ingest.clone(),
            idle_timeout: self.idle_timeout,
            max_rate_hz: (self.max_rate_hz > 0.0).then_some(self.max_rate_hz),
            max_payload_bytes: self.max_payload_bytes,
            ..pipes_core::hub::HubConfig::default()
        }
    }
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
    use clap::{Command, CommandFactory, Parser};

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
        assert_eq!(default("zenoh_connect"), "tcp/127.0.0.1:7447");
        assert_eq!(default("zenoh_mode"), "client");
        assert_eq!(default("domain_id"), "0");
    }

    #[test]
    fn ingest_patterns_are_validated_at_parse_time() {
        let cfg =
            Config::try_parse_from(["rmf-pipes", "--ingest", "fleet/*/state,door_states"]).unwrap();
        assert_eq!(cfg.hub().patterns.len(), 2);
        assert!(cfg.hub().patterns[0].matches("/fleet/r1/state"));
        let err = Config::try_parse_from(["rmf-pipes", "--ingest", "a//b"]).unwrap_err();
        assert!(err.to_string().contains("empty segment"), "{err}");
    }

    #[test]
    fn rate_and_queue_depth_are_validated() {
        for bad in ["-1", "1e-20", "inf", "NaN", "2e6"] {
            assert!(
                Config::try_parse_from(["rmf-pipes", "--max-rate-hz", bad]).is_err(),
                "{bad}"
            );
        }
        assert!(Config::try_parse_from(["rmf-pipes", "--max-rate-hz", "0.5"]).is_ok());
        assert!(Config::try_parse_from(["rmf-pipes", "--queue-depth", "0"]).is_err());
        let cfg = Config::try_parse_from(["rmf-pipes", "--queue-depth", "64"]).unwrap();
        assert_eq!(cfg.bus().queue_depth, 64);
    }

    #[test]
    fn zero_rate_disables_the_cap() {
        let cfg = Config::try_parse_from(["rmf-pipes", "--max-rate-hz", "0"]).unwrap();
        assert_eq!(cfg.hub().max_rate_hz, None);
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
            checked >= 14,
            "expected env-backed arguments, checked {checked}"
        );
    }
}
