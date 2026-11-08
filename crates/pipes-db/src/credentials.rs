//! Browser sessions and API tokens.
//!
//! Both are 32 random bytes from the OS, base64url-encoded. API tokens carry an
//! `rp_` prefix so they are recognisable in logs and secret scanners. Only the
//! SHA-256 of a credential is stored; lookups hash the presented value.

use std::time::Duration;

use base64::Engine as _;
use serde::Serialize;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::{Db, DbError, User, users::user_columns_u};

/// Prefix of every API token.
pub const API_TOKEN_PREFIX: &str = "rp_";

/// How often (at most) a credential's last-use time is written, so busy
/// clients don't turn every request into a write.
const TOUCH_INTERVAL: time::Duration = time::Duration::seconds(60);

/// How a request authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Credential {
    /// A browser session cookie.
    Session,
    /// An API token (`Authorization: Bearer rp_…`).
    Token {
        /// The token's id.
        id: i64,
    },
}

/// An API token's metadata. The token itself is only returned at creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, sqlx::FromRow)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ApiToken {
    /// Token id, for listing and revoking.
    pub id: i64,
    /// Owner's username.
    pub username: String,
    /// A label chosen at creation (e.g. `grafana-scraper`).
    pub name: String,
    /// When it was created.
    #[serde(with = "time::serde::rfc3339")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    pub created_at: OffsetDateTime,
    /// Last successful use, to about a minute.
    #[serde(with = "time::serde::rfc3339::option")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    pub last_used_at: Option<OffsetDateTime>,
    /// After this it stops working; `None` never expires.
    #[serde(with = "time::serde::rfc3339::option")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    pub expires_at: Option<OffsetDateTime>,
    /// Set once revoked; revoked tokens never work again.
    #[serde(with = "time::serde::rfc3339::option")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = DateTime))]
    pub revoked_at: Option<OffsetDateTime>,
}

/// 32 random bytes, base64url without padding.
fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .unwrap_or_else(|e| panic!("the OS random number generator failed: {e}"));
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// A user joined with one of their API tokens.
#[derive(sqlx::FromRow)]
struct TokenUserRow {
    id: i64,
    username: String,
    role: String,
    disabled: bool,
    created_at: OffsetDateTime,
    token_id: i64,
    last_used_at: Option<OffsetDateTime>,
}

fn digest(secret: &str) -> Vec<u8> {
    Sha256::digest(secret.as_bytes()).to_vec()
}

/// `now + d`, refusing durations that overflow a timestamp.
fn after(now: OffsetDateTime, d: Duration) -> Result<OffsetDateTime, DbError> {
    time::Duration::try_from(d)
        .ok()
        .and_then(|d| now.checked_add(d))
        .ok_or_else(|| DbError::Invalid(format!("duration {d:?} is out of range")))
}

/// `now - d`, clamped.
fn before(now: OffsetDateTime, d: Duration) -> OffsetDateTime {
    time::Duration::try_from(d)
        .ok()
        .and_then(|d| now.checked_sub(d))
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

macro_rules! token_columns {
    () => {
        "t.id, u.username, t.name, t.created_at, t.last_used_at, t.expires_at, t.revoked_at"
    };
}

impl Db {
    /// Start a session for `user_id` that expires after `idle` without use.
    /// Returns the cookie value.
    pub async fn create_session(&self, user_id: i64, idle: Duration) -> Result<String, DbError> {
        let secret = random_secret();
        sqlx::query(
            "INSERT INTO pipes.sessions (token_hash, user_id, expires_at) VALUES ($1, $2, $3)",
        )
        .bind(digest(&secret))
        .bind(user_id)
        .bind(after(OffsetDateTime::now_utc(), idle)?)
        .execute(self.pool())
        .await?;
        Ok(secret)
    }

    /// The user behind a session cookie, if the session is live (used within
    /// `idle`, and younger than `max_age`) and the user enabled. Using a
    /// session extends it by `idle`.
    pub async fn session_user(
        &self,
        secret: &str,
        idle: Duration,
        max_age: Duration,
    ) -> Result<Option<User>, DbError> {
        let hash = digest(secret);
        let row: Option<(i64, String, String, bool, OffsetDateTime, OffsetDateTime)> =
            sqlx::query_as(concat!(
                "SELECT ",
                user_columns_u!(),
                ", s.last_seen_at FROM pipes.sessions s JOIN pipes.users u ON u.id = s.user_id \
             WHERE s.token_hash = $1 AND s.expires_at > now() AND s.created_at > $2 AND NOT u.disabled"
            ))
            .bind(&hash)
            .bind(before(OffsetDateTime::now_utc(), max_age))
            .fetch_optional(self.pool())
            .await?;
        let Some((id, username, role, disabled, created_at, last_seen)) = row else {
            return Ok(None);
        };
        let now = OffsetDateTime::now_utc();
        // Slide at most every TOUCH_INTERVAL, but at least twice per idle
        // period so short idle timeouts still slide.
        let touch =
            TOUCH_INTERVAL.min(time::Duration::try_from(idle / 2).unwrap_or(TOUCH_INTERVAL));
        if now - last_seen >= touch {
            sqlx::query("UPDATE pipes.sessions SET last_seen_at = $2, expires_at = $3 WHERE token_hash = $1")
                .bind(&hash)
                .bind(now)
                .bind(after(now, idle)?)
                .execute(self.pool())
                .await?;
        }
        Ok(Some(User {
            id,
            username,
            role: role.parse()?,
            disabled,
            created_at,
        }))
    }

    /// End a session (logout). Unknown sessions are ignored.
    pub async fn delete_session(&self, secret: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM pipes.sessions WHERE token_hash = $1")
            .bind(digest(secret))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// End every session of a user (password change, disable).
    pub async fn delete_sessions_of(&self, user_id: i64) -> Result<u64, DbError> {
        Ok(sqlx::query("DELETE FROM pipes.sessions WHERE user_id = $1")
            .bind(user_id)
            .execute(self.pool())
            .await?
            .rows_affected())
    }

    /// Delete expired sessions; returns how many.
    pub async fn purge_expired_sessions(&self) -> Result<u64, DbError> {
        Ok(
            sqlx::query("DELETE FROM pipes.sessions WHERE expires_at <= now()")
                .execute(self.pool())
                .await?
                .rows_affected(),
        )
    }

    /// Create an API token for `user_id`. Returns the plaintext token (shown
    /// once) and its metadata.
    pub async fn create_api_token(
        &self,
        user_id: i64,
        name: &str,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<(String, ApiToken), DbError> {
        let name = name.trim();
        if name.is_empty() || name.len() > 100 {
            return Err(DbError::Invalid("token names are 1-100 characters".into()));
        }
        if expires_at.is_some_and(|t| t <= OffsetDateTime::now_utc()) {
            return Err(DbError::Invalid("expires_at must be in the future".into()));
        }
        let token = format!("{API_TOKEN_PREFIX}{}", random_secret());
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO pipes.api_tokens (user_id, name, token_hash, expires_at) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(name)
        .bind(digest(&token))
        .bind(expires_at)
        .fetch_one(self.pool())
        .await?;
        let meta = self.api_token(id).await?.ok_or(DbError::NotFound)?;
        Ok((token, meta))
    }

    /// One token's metadata.
    pub async fn api_token(&self, id: i64) -> Result<Option<ApiToken>, DbError> {
        Ok(sqlx::query_as(concat!(
            "SELECT ",
            token_columns!(),
            " FROM pipes.api_tokens t JOIN pipes.users u ON u.id = t.user_id WHERE t.id = $1"
        ))
        .bind(id)
        .fetch_optional(self.pool())
        .await?)
    }

    /// Tokens of one user, or of everyone when `user_id` is `None`, newest first.
    pub async fn list_api_tokens(&self, user_id: Option<i64>) -> Result<Vec<ApiToken>, DbError> {
        Ok(sqlx::query_as(concat!(
            "SELECT ",
            token_columns!(),
            " FROM pipes.api_tokens t JOIN pipes.users u ON u.id = t.user_id \
             WHERE ($1::bigint IS NULL OR t.user_id = $1) ORDER BY t.id DESC"
        ))
        .bind(user_id)
        .fetch_all(self.pool())
        .await?)
    }

    /// Revoke a token. With `owner`, only that user's token can be revoked.
    /// Returns false if no such (unrevoked) token exists.
    pub async fn revoke_api_token(&self, id: i64, owner: Option<i64>) -> Result<bool, DbError> {
        Ok(sqlx::query(
            "UPDATE pipes.api_tokens SET revoked_at = now() \
             WHERE id = $1 AND revoked_at IS NULL AND ($2::bigint IS NULL OR user_id = $2)",
        )
        .bind(id)
        .bind(owner)
        .execute(self.pool())
        .await?
        .rows_affected()
            == 1)
    }

    /// The user behind an API token, if it is unrevoked, unexpired and its
    /// user enabled. Also returns the token id.
    pub async fn token_user(&self, token: &str) -> Result<Option<(User, i64)>, DbError> {
        if !token.starts_with(API_TOKEN_PREFIX) {
            return Ok(None);
        }
        let row: Option<TokenUserRow> = sqlx::query_as(concat!(
            "SELECT ",
            user_columns_u!(),
            ", t.id AS token_id, t.last_used_at FROM pipes.api_tokens t JOIN pipes.users u ON u.id = t.user_id \
                 WHERE t.token_hash = $1 AND t.revoked_at IS NULL \
                   AND (t.expires_at IS NULL OR t.expires_at > now()) AND NOT u.disabled"
        ))
        .bind(digest(token))
        .fetch_optional(self.pool())
        .await?;
        let Some(TokenUserRow {
            id,
            username,
            role,
            disabled,
            created_at,
            token_id,
            last_used_at: last_used,
        }) = row
        else {
            return Ok(None);
        };
        let now = OffsetDateTime::now_utc();
        if last_used.is_none_or(|t| now - t >= TOUCH_INTERVAL) {
            sqlx::query("UPDATE pipes.api_tokens SET last_used_at = $2 WHERE id = $1")
                .bind(token_id)
                .bind(now)
                .execute(self.pool())
                .await?;
        }
        let user = User {
            id,
            username,
            role: role.parse()?,
            disabled,
            created_at,
        };
        Ok(Some((user, token_id)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::PgPool;
    use time::OffsetDateTime;

    use crate::{Db, DbError, Role};

    const PW: &str = "correct horse battery";
    const IDLE: Duration = Duration::from_secs(3600);
    const MAX: Duration = Duration::from_secs(7 * 24 * 3600);

    #[sqlx::test]
    async fn sessions_work_until_logout_and_stop_for_disabled_users(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", PW, Role::Admin).await.unwrap();
        let op = db.create_user("op", PW, Role::Operator).await.unwrap();
        let s = db.create_session(op.id, IDLE).await.unwrap();
        assert!(s.len() >= 43, "256-bit secret");
        assert_eq!(
            db.session_user(&s, IDLE, MAX)
                .await
                .unwrap()
                .unwrap()
                .username,
            "op"
        );
        assert!(
            db.session_user("forged", IDLE, MAX)
                .await
                .unwrap()
                .is_none()
        );

        db.set_disabled("op", true).await.unwrap();
        assert!(
            db.session_user(&s, IDLE, MAX).await.unwrap().is_none(),
            "disabling ends sessions"
        );
        db.set_disabled("op", false).await.unwrap();
        let s2 = db.create_session(op.id, IDLE).await.unwrap();
        db.delete_session(&s2).await.unwrap();
        assert!(db.session_user(&s2, IDLE, MAX).await.unwrap().is_none());

        let s3 = db.create_session(op.id, IDLE).await.unwrap();
        db.set_password("op", "another long password", false)
            .await
            .unwrap();
        assert!(
            db.session_user(&s3, IDLE, MAX).await.unwrap().is_none(),
            "password change ends sessions"
        );
    }

    #[sqlx::test]
    async fn sessions_expire_and_are_purged(pool: PgPool) {
        let db = Db::from_pool(pool);
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        let s = db
            .create_session(u.id, Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(db.session_user(&s, IDLE, MAX).await.unwrap().is_none());
        assert_eq!(db.purge_expired_sessions().await.unwrap(), 1);
    }

    #[sqlx::test]
    async fn using_a_session_slides_its_expiry(pool: PgPool) {
        let db = Db::from_pool(pool);
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        let idle = Duration::from_millis(1500);
        let s = db.create_session(u.id, idle).await.unwrap();
        // Used every 900ms (more than idle/2), it outlives its first 1.5s.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(900)).await;
            assert!(
                db.session_user(&s, idle, MAX).await.unwrap().is_some(),
                "expired despite use"
            );
        }
        tokio::time::sleep(Duration::from_millis(1700)).await;
        assert!(
            db.session_user(&s, idle, MAX).await.unwrap().is_none(),
            "idle sessions expire"
        );
    }

    #[sqlx::test]
    async fn sessions_have_an_absolute_lifetime(pool: PgPool) {
        let db = Db::from_pool(pool);
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        let s = db.create_session(u.id, IDLE).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            db.session_user(&s, IDLE, Duration::from_millis(10))
                .await
                .unwrap()
                .is_none()
        );
        assert!(db.session_user(&s, IDLE, MAX).await.unwrap().is_some());
    }

    /// Separately from `set_disabled` deleting sessions: a session row of a
    /// disabled user must not authenticate.
    #[sqlx::test]
    async fn disabled_users_sessions_never_authenticate(pool: PgPool) {
        let db = Db::from_pool(pool.clone());
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        let op = db.create_user("op", PW, Role::Operator).await.unwrap();
        let s = db.create_session(op.id, IDLE).await.unwrap();
        sqlx::query("UPDATE pipes.users SET disabled = TRUE WHERE id = $1")
            .bind(op.id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(db.session_user(&s, IDLE, MAX).await.unwrap().is_none());
        // And set_disabled removes the rows themselves.
        db.create_session(u.id, IDLE).await.unwrap();
        db.create_user("root2", PW, Role::Admin).await.unwrap();
        db.set_disabled("root", true).await.unwrap();
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pipes.sessions WHERE user_id = $1")
                .bind(u.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(left, 0);
    }

    #[sqlx::test]
    async fn absurd_durations_are_errors_not_panics(pool: PgPool) {
        let db = Db::from_pool(pool);
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        assert!(matches!(
            db.create_session(u.id, Duration::MAX).await,
            Err(DbError::Invalid(_))
        ));
    }

    #[sqlx::test]
    async fn only_hashes_are_stored(pool: PgPool) {
        let db = Db::from_pool(pool.clone());
        let u = db.create_user("root", PW, Role::Admin).await.unwrap();
        let s = db.create_session(u.id, IDLE).await.unwrap();
        let (t, _) = db.create_api_token(u.id, "ci", None).await.unwrap();
        let dump: String = sqlx::query_scalar(
            "SELECT (SELECT string_agg(encode(token_hash, 'escape'), ',') FROM pipes.sessions) || \
                    (SELECT string_agg(encode(token_hash, 'escape'), ',') FROM pipes.api_tokens)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!dump.contains(&s) && !dump.contains(&t));
        let hash: String = sqlx::query_scalar("SELECT password_hash FROM pipes.users")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(hash.starts_with("$argon2id$"), "{hash}");
    }

    #[sqlx::test]
    async fn api_tokens_authenticate_until_revoked_or_expired(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", PW, Role::Admin).await.unwrap();
        let op = db.create_user("op", PW, Role::Operator).await.unwrap();
        let (token, meta) = db.create_api_token(op.id, " grafana ", None).await.unwrap();
        assert!(token.starts_with("rp_"));
        assert_eq!(
            (meta.name.as_str(), meta.username.as_str()),
            ("grafana", "op")
        );
        let (user, id) = db.token_user(&token).await.unwrap().unwrap();
        assert_eq!((user.username.as_str(), id), ("op", meta.id));
        assert!(
            db.api_token(meta.id)
                .await
                .unwrap()
                .unwrap()
                .last_used_at
                .is_some()
        );
        assert!(db.token_user("rp_forged").await.unwrap().is_none());
        assert!(db.token_user("no-prefix").await.unwrap().is_none());

        // Disabled users' tokens stop working, and work again when re-enabled.
        db.set_disabled("op", true).await.unwrap();
        assert!(db.token_user(&token).await.unwrap().is_none());
        db.set_disabled("op", false).await.unwrap();
        assert!(db.token_user(&token).await.unwrap().is_some());

        // Only the owner (or an admin, owner = None) can revoke.
        let root = db.user_by_name("root").await.unwrap().unwrap();
        assert!(!db.revoke_api_token(meta.id, Some(root.id)).await.unwrap());
        assert!(db.revoke_api_token(meta.id, Some(op.id)).await.unwrap());
        assert!(
            !db.revoke_api_token(meta.id, None).await.unwrap(),
            "already revoked"
        );
        assert!(db.token_user(&token).await.unwrap().is_none());

        let soon = OffsetDateTime::now_utc() + time::Duration::milliseconds(50);
        let (short, _) = db
            .create_api_token(op.id, "short", Some(soon))
            .await
            .unwrap();
        assert!(db.token_user(&short).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(db.token_user(&short).await.unwrap().is_none());

        assert!(matches!(
            db.create_api_token(op.id, "  ", None).await,
            Err(DbError::Invalid(_))
        ));
        let past = OffsetDateTime::now_utc() - time::Duration::hours(1);
        assert!(matches!(
            db.create_api_token(op.id, "old", Some(past)).await,
            Err(DbError::Invalid(_))
        ));
        assert_eq!(db.list_api_tokens(Some(op.id)).await.unwrap().len(), 2);
        assert_eq!(db.list_api_tokens(Some(root.id)).await.unwrap().len(), 0);
        assert_eq!(db.list_api_tokens(None).await.unwrap().len(), 2);
    }
}
