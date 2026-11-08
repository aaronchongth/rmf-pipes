//! Persistence for rmf-pipes: users, browser sessions, API tokens and the
//! audit log, in the `pipes` schema of a Postgres database.
//!
//! - Queries are runtime-checked (`sqlx::query`), and every query is exercised
//!   by a `#[sqlx::test]` against a fresh database. Run them with
//!   `pixi run test`, which starts a local Postgres and sets `DATABASE_URL`.
//! - Passwords are argon2id PHC strings (the `password-auth` crate).
//! - Sessions and API tokens are 256-bit random values; only their SHA-256 is
//!   stored, so a database leak does not leak usable credentials.
//! - Migrations are embedded in the binary and applied at startup.

pub mod audit;
pub mod credentials;
pub mod users;

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

pub use audit::{AuditEntry, AuditQuery, AuditRecord, Outcome};
pub use credentials::{ApiToken, Credential};
pub use users::{LoginOutcome, MIN_PASSWORD_LEN, User, UserChanges, validate_username};

/// The embedded migrations (`crates/pipes-db/migrations`).
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// A user's privileges. Ordered: each role includes the ones below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Read everything the API serves.
    Viewer,
    /// Viewer, plus publish to and call allowlisted topics and services.
    Operator,
    /// Operator, plus manage users, any API token and read the audit log.
    Admin,
}

impl Role {
    /// The database and API spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Role {
    type Err = DbError;
    fn from_str(s: &str) -> Result<Self, DbError> {
        match s {
            "viewer" => Ok(Self::Viewer),
            "operator" => Ok(Self::Operator),
            "admin" => Ok(Self::Admin),
            other => Err(DbError::Invalid(format!(
                "unknown role `{other}` (expected viewer, operator or admin)"
            ))),
        }
    }
}

/// Why a database call failed. `Invalid`, `Conflict` and `NotFound` carry
/// messages safe to show to clients; `Sqlx` must only be logged.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The request violates a rule (bad username, short password, removing
    /// the last admin, …).
    #[error("{0}")]
    Invalid(String),
    /// Something with that name already exists.
    #[error("{0}")]
    Conflict(String),
    /// Nothing matched.
    #[error("not found")]
    NotFound,
    /// The database itself failed.
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Why connecting failed. The message names host, port and database, never
/// the password or the full URL.
#[derive(Debug, thiserror::Error)]
#[error("cannot use the database: {0}")]
pub struct ConnectError(String);

/// A handle to the database. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    /// Connect to `url` (a `postgres://` URL), waiting up to `timeout` for the
    /// server, and apply pending migrations.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnectError> {
        // The parse error of a malformed URL may quote it, so don't forward it.
        let opts = PgConnectOptions::from_str(url)
            .map_err(|_| ConnectError("the database URL is not a valid postgres:// URL".into()))?;
        let target = format!(
            "{}:{}/{}",
            opts.get_host(),
            opts.get_port(),
            opts.get_database().unwrap_or("(default)")
        );
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(timeout)
            .connect_with(opts)
            .await
            .map_err(|e| ConnectError(format!("connecting to {target}: {}", redact(&e))))?;
        let db = Self { pool };
        MIGRATOR
            .run(&db.pool)
            .await
            .map_err(|e| ConnectError(format!("migrating {target}: {e}")))?;
        Ok(db)
    }

    /// Wrap an existing pool (tests: `#[sqlx::test]` hands one out).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Whether the database answers a trivial query.
    pub async fn ping(&self) -> bool {
        sqlx::query("SELECT 1").execute(&self.pool).await.is_ok()
    }
}

/// sqlx connection errors describe the failure (refused, auth failed, …) and
/// do not include the URL, but keep only the error kind to be safe.
fn redact(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Io(io) => format!("I/O error: {}", io.kind()),
        sqlx::Error::Database(db) => format!("server refused: {}", db.message()),
        sqlx::Error::PoolTimedOut => "timed out".into(),
        sqlx::Error::Tls(_) => "TLS error".into(),
        _ => "connection failed".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_ordered_by_privilege() {
        assert!(Role::Viewer < Role::Operator && Role::Operator < Role::Admin);
        for r in [Role::Viewer, Role::Operator, Role::Admin] {
            assert_eq!(r.as_str().parse::<Role>().unwrap(), r);
        }
        assert!("root".parse::<Role>().is_err());
    }

    #[tokio::test]
    async fn connect_errors_never_contain_the_password() {
        let secret = "s3cr3t-pa55-db";
        for url in [
            format!("postgres://u:{secret}@127.0.0.1:1/db"),
            format!("not a url {secret}"),
        ] {
            let err = Db::connect(&url, 1, std::time::Duration::from_millis(200))
                .await
                .unwrap_err()
                .to_string();
            assert!(!err.contains(secret), "leaked: {err}");
        }
    }
}
