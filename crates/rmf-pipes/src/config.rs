//! Command-line and environment configuration.
//!
//! Every option can be set by flag or by `RMF_PIPES_*` environment variable.
//! Every env-backed argument sets `hide_env_values`: `--help` must never echo
//! the current value of an environment variable, because some of them will hold
//! secrets (database URLs, tokens). This is a lesson from rmf-console, and a
//! test below fails if a new argument forgets it.
//!
//! clap echoes the offending value in *parse* errors, so an argument that may
//! carry a secret (`--database-url`, `--bootstrap-password`) is parsed as a
//! plain string and validated afterwards with a redacted error.
//!
//! Without a subcommand the binary serves; `rmf-pipes user …` manages users.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use pipes_core::pattern::TopicPattern;

#[derive(Debug, Clone, Parser)]
#[command(name = "rmf-pipes", version, about)]
pub struct Config {
    /// What to do; without one, serve.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Postgres URL (`postgres://user:password@host:5432/db`). Enables
    /// authentication: users, sessions, API tokens and the audit log. Without
    /// it the API is anonymous and read-only.
    #[arg(
        long,
        env = "RMF_PIPES_DATABASE_URL",
        hide_env_values = true,
        global = true
    )]
    pub database_url: Option<String>,

    /// How long to wait for Postgres at startup.
    #[arg(long, env = "RMF_PIPES_DATABASE_CONNECT_TIMEOUT", hide_env_values = true, global = true, value_parser = humantime::parse_duration, default_value = "10s")]
    pub database_connect_timeout: Duration,

    /// With a database: if there are no users yet, create this admin at
    /// startup, with the password from `--bootstrap-password`.
    #[arg(long, env = "RMF_PIPES_BOOTSTRAP_ADMIN", hide_env_values = true)]
    pub bootstrap_admin: Option<String>,

    /// Password for `--bootstrap-admin`. Prefer the environment variable, so
    /// the password doesn't show in the process list.
    #[arg(long, env = "RMF_PIPES_BOOTSTRAP_PASSWORD", hide_env_values = true)]
    pub bootstrap_password: Option<String>,

    /// A browser session ends after this long without use (1 minute to 30 days).
    #[arg(long, env = "RMF_PIPES_SESSION_IDLE", hide_env_values = true, value_parser = parse_session_duration, default_value = "12h")]
    pub session_idle: Duration,

    /// A browser session ends this long after login, however much it is used
    /// (1 minute to 30 days, at least `--session-idle`).
    #[arg(long, env = "RMF_PIPES_SESSION_MAX", hide_env_values = true, value_parser = parse_session_duration, default_value = "7d")]
    pub session_max: Duration,

    /// Mark the session cookie `Secure`. Set it when clients reach rmf-pipes
    /// over HTTPS (e.g. behind a TLS-terminating proxy).
    #[arg(long, env = "RMF_PIPES_COOKIE_SECURE", hide_env_values = true, action = ArgAction::SetTrue)]
    pub cookie_secure: bool,

    /// Allow serving without authentication on a non-loopback address.
    /// Anyone who can reach the port can then read everything.
    #[arg(long, env = "RMF_PIPES_INSECURE_NO_AUTH", hide_env_values = true, action = ArgAction::SetTrue)]
    pub insecure_no_auth: bool,

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

    /// Most topics subscribed on demand at once (standing topics don't count).
    #[arg(
        long,
        env = "RMF_PIPES_MAX_TOPICS",
        hide_env_values = true,
        default_value_t = 256
    )]
    pub max_topics: usize,

    /// Most concurrent SSE streams.
    #[arg(
        long,
        env = "RMF_PIPES_MAX_STREAMS",
        hide_env_values = true,
        default_value_t = 256
    )]
    pub max_streams: usize,

    /// Topics that may be published to via the API (needs a database),
    /// comma-separated `pattern` or `pattern=role` (role: operator, the
    /// default, or admin), e.g. `door_requests,fleet/*/estop=admin`.
    #[arg(
        long,
        env = "RMF_PIPES_ALLOW_PUBLISH",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub allow_publish: Vec<String>,

    /// Services that may be called via the API, like `--allow-publish`.
    #[arg(
        long,
        env = "RMF_PIPES_ALLOW_CALL",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub allow_call: Vec<String>,

    /// Minimum time between writes by one user to one topic or service; `0s`
    /// disables the limit.
    #[arg(long, env = "RMF_PIPES_WRITE_INTERVAL", hide_env_values = true, value_parser = humantime::parse_duration, default_value = "200ms")]
    pub write_interval: Duration,

    /// How long to wait for a publisher's type description when subscribing.
    #[arg(long, env = "RMF_PIPES_DISCOVERY_TIMEOUT", hide_env_values = true, value_parser = humantime::parse_duration, default_value = "5s")]
    pub discovery_timeout: Duration,
}

/// Session durations: 1 minute to 30 days.
fn parse_session_duration(s: &str) -> Result<Duration, String> {
    let d = humantime::parse_duration(s).map_err(|e| e.to_string())?;
    if (Duration::from_secs(60)..=Duration::from_secs(30 * 24 * 3600)).contains(&d) {
        Ok(d)
    } else {
        Err("must be between 1m and 30d".into())
    }
}

/// Subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Manage users (needs `--database-url`). Passwords are read from the
    /// first line of stdin, so they don't end up in shell history or `ps`.
    User {
        /// What to do.
        #[command(subcommand)]
        action: UserAction,
    },
}

/// `rmf-pipes user …`
#[derive(Debug, Clone, Subcommand)]
pub enum UserAction {
    /// Create a user; the password is read from stdin.
    Add {
        /// Login name.
        username: String,
        /// Role.
        #[arg(long, value_enum, default_value_t = RoleArg::Viewer)]
        role: RoleArg,
    },
    /// List users.
    List,
    /// Reset a user's password (read from stdin); ends their sessions.
    SetPassword {
        /// Login name.
        username: String,
    },
    /// Change a user's role.
    SetRole {
        /// Login name.
        username: String,
        /// New role.
        #[arg(value_enum)]
        role: RoleArg,
    },
    /// Disable a user: no login, and their sessions and tokens stop working.
    Disable {
        /// Login name.
        username: String,
    },
    /// Re-enable a user.
    Enable {
        /// Login name.
        username: String,
    },
    /// Delete a user with their sessions and tokens.
    Delete {
        /// Login name.
        username: String,
    },
}

/// A role on the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RoleArg {
    /// Read everything.
    Viewer,
    /// Read, and write to allowlisted topics.
    Operator,
    /// Everything, including user management and the audit log.
    Admin,
}

impl From<RoleArg> for pipes_db::Role {
    fn from(r: RoleArg) -> Self {
        match r {
            RoleArg::Viewer => Self::Viewer,
            RoleArg::Operator => Self::Operator,
            RoleArg::Admin => Self::Admin,
        }
    }
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

    /// The write allowlist. Empty entries (e.g. an empty env var) are
    /// ignored; invalid ones are an error naming the entry.
    pub fn writes(&self) -> Result<pipes_api::writes::WritePolicy, String> {
        let parse =
            |flag: &str, raw: &[String]| -> Result<Vec<pipes_api::writes::WriteRule>, String> {
                raw.iter()
                    .map(|r| r.trim())
                    .filter(|r| !r.is_empty())
                    .map(|r| r.parse().map_err(|e| format!("--{flag} `{r}`: {e}")))
                    .collect()
            };
        Ok(pipes_api::writes::WritePolicy {
            publish: parse("allow-publish", &self.allow_publish)?,
            call: parse("allow-call", &self.allow_call)?,
            min_interval: (!self.write_interval.is_zero()).then_some(self.write_interval),
        })
    }

    /// Auth settings.
    pub fn auth(&self) -> pipes_api::auth::AuthConfig {
        pipes_api::auth::AuthConfig {
            session_idle: self.session_idle,
            session_max: self.session_max.max(self.session_idle),
            cookie_secure: self.cookie_secure,
            ..pipes_api::auth::AuthConfig::default()
        }
    }

    /// Hub settings.
    pub fn hub(&self) -> pipes_core::hub::HubConfig {
        pipes_core::hub::HubConfig {
            patterns: self.ingest.clone(),
            idle_timeout: self.idle_timeout,
            max_rate_hz: (self.max_rate_hz > 0.0).then_some(self.max_rate_hz),
            max_payload_bytes: self.max_payload_bytes,
            max_on_demand_topics: self.max_topics,
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
    use std::time::Duration;

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
    fn session_durations_are_bounded() {
        for bad in ["10s", "300000years", "0s"] {
            assert!(
                Config::try_parse_from(["rmf-pipes", "--session-idle", bad]).is_err(),
                "{bad}"
            );
        }
        let cfg =
            Config::try_parse_from(["rmf-pipes", "--session-idle", "2d", "--session-max", "1d"])
                .unwrap();
        assert_eq!(
            cfg.auth().session_max,
            Duration::from_secs(2 * 24 * 3600),
            "max is at least idle"
        );
    }

    #[test]
    fn write_rules_ignore_empty_entries_and_report_bad_ones() {
        let cfg = Config::try_parse_from([
            "rmf-pipes",
            "--allow-publish",
            ",door_requests,,fleet/*/estop=admin",
        ])
        .unwrap();
        let w = cfg.writes().unwrap();
        assert_eq!(w.publish.len(), 2);
        let cfg = Config::try_parse_from(["rmf-pipes", "--allow-call", "door-requests"]).unwrap();
        assert!(
            cfg.writes()
                .unwrap_err()
                .contains("--allow-call `door-requests`")
        );
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
            checked >= 27,
            "expected env-backed arguments, checked {checked}"
        );
    }
}
