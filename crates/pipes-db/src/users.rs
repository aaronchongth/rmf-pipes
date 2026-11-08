//! Users and password checks.

use std::sync::LazyLock;

use serde::Serialize;
use time::OffsetDateTime;

use crate::{Db, DbError, Role};

/// Shortest acceptable password.
pub const MIN_PASSWORD_LEN: usize = 10;
/// Longest acceptable password (bounds hashing cost).
pub const MAX_PASSWORD_LEN: usize = 1024;

/// A user, without credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct User {
    /// Database id.
    pub id: i64,
    /// Login name (case-insensitive, unique).
    pub username: String,
    /// Privileges.
    pub role: Role,
    /// Disabled users cannot log in, and their sessions and tokens stop working.
    pub disabled: bool,
    /// When the user was created.
    #[serde(with = "time::serde::rfc3339")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    pub created_at: OffsetDateTime,
}

/// Result of a password check. The API answers the same for every failure;
/// the distinction is for the audit log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginOutcome {
    /// Credentials are valid and the user is enabled.
    Ok(User),
    /// Unknown user or wrong password.
    BadCredentials,
    /// Correct password, but the user is disabled.
    Disabled,
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: i64,
    username: String,
    role: String,
    disabled: bool,
    created_at: OffsetDateTime,
}

impl TryFrom<UserRow> for User {
    type Error = DbError;
    fn try_from(r: UserRow) -> Result<Self, DbError> {
        Ok(Self {
            id: r.id,
            username: r.username,
            role: r.role.parse()?,
            disabled: r.disabled,
            created_at: r.created_at,
        })
    }
}

/// User columns, as a literal for `concat!` (sqlx only accepts static SQL).
macro_rules! user_columns {
    () => {
        "id, username, role, disabled, created_at"
    };
}
/// The same, qualified with the `u` alias.
macro_rules! user_columns_u {
    () => {
        "u.id, u.username, u.role, u.disabled, u.created_at"
    };
}
pub(crate) use user_columns_u;

/// A hash to verify against when the user doesn't exist, so that unknown and
/// known usernames take the same time to reject.
static DUMMY_HASH: LazyLock<String> =
    LazyLock::new(|| password_auth::generate_hash("rmf-pipes-dummy-password"));

/// Check a username's shape (1-64 of letters, digits, `_`, `-`, `.`).
pub fn validate_username(name: &str) -> Result<(), DbError> {
    let ok = (1..=64).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if ok {
        Ok(())
    } else {
        Err(DbError::Invalid(
            "usernames are 1-64 characters: letters, digits, `_`, `-` or `.`".into(),
        ))
    }
}

fn validate_password(pw: &str) -> Result<(), DbError> {
    let n = pw.chars().count();
    if n < MIN_PASSWORD_LEN {
        return Err(DbError::Invalid(format!(
            "passwords must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }
    if pw.len() > MAX_PASSWORD_LEN {
        return Err(DbError::Invalid(format!(
            "passwords must be at most {MAX_PASSWORD_LEN} bytes"
        )));
    }
    Ok(())
}

/// argon2 is CPU-heavy by design; keep it off the async workers.
async fn hash_password(pw: &str) -> String {
    let pw = pw.to_owned();
    tokio::task::spawn_blocking(move || password_auth::generate_hash(pw))
        .await
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}

async fn verify_hash(pw: &str, hash: &str) -> bool {
    let (pw, hash) = (pw.to_owned(), hash.to_owned());
    tokio::task::spawn_blocking(move || password_auth::verify_password(pw, &hash).is_ok())
        .await
        .unwrap_or(false)
}

impl Db {
    /// Create a user. Usernames are unique case-insensitively.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<User, DbError> {
        validate_username(username)?;
        validate_password(password)?;
        let hash = hash_password(password).await;
        let row: Result<UserRow, sqlx::Error> = sqlx::query_as(concat!(
            "INSERT INTO pipes.users (username, password_hash, role) VALUES ($1, $2, $3) RETURNING ",
            user_columns!()
        ))
        .bind(username)
        .bind(hash)
        .bind(role.as_str())
        .fetch_one(self.pool())
        .await;
        match row {
            Ok(r) => r.try_into(),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Err(DbError::Conflict(
                format!("user `{username}` already exists"),
            )),
            Err(e) => Err(e.into()),
        }
    }

    /// Look a user up by name (case-insensitive).
    pub async fn user_by_name(&self, username: &str) -> Result<Option<User>, DbError> {
        let row: Option<UserRow> = sqlx::query_as(concat!(
            "SELECT ",
            user_columns!(),
            " FROM pipes.users WHERE lower(username) = lower($1)"
        ))
        .bind(username)
        .fetch_optional(self.pool())
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Every user, by name.
    pub async fn list_users(&self) -> Result<Vec<User>, DbError> {
        let rows: Vec<UserRow> = sqlx::query_as(concat!(
            "SELECT ",
            user_columns!(),
            " FROM pipes.users ORDER BY lower(username)"
        ))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Number of users (0 means bootstrapping is allowed).
    pub async fn count_users(&self) -> Result<i64, DbError> {
        Ok(sqlx::query_scalar("SELECT count(*) FROM pipes.users")
            .fetch_one(self.pool())
            .await?)
    }

    /// Check a username and password.
    pub async fn check_login(
        &self,
        username: &str,
        password: &str,
    ) -> Result<LoginOutcome, DbError> {
        let row: Option<(String, bool)> = if password.len() > MAX_PASSWORD_LEN {
            None
        } else {
            sqlx::query_as(
                "SELECT password_hash, disabled FROM pipes.users WHERE lower(username) = lower($1)",
            )
            .bind(username)
            .fetch_optional(self.pool())
            .await?
        };
        let Some((hash, disabled)) = row else {
            // Same work as a real check, so timing doesn't reveal valid names.
            let _ = verify_hash(password, &DUMMY_HASH).await;
            return Ok(LoginOutcome::BadCredentials);
        };
        if !verify_hash(password, &hash).await {
            return Ok(LoginOutcome::BadCredentials);
        }
        if disabled {
            return Ok(LoginOutcome::Disabled);
        }
        match self.user_by_name(username).await? {
            Some(u) => Ok(LoginOutcome::Ok(u)),
            None => Ok(LoginOutcome::BadCredentials),
        }
    }

    /// Set a user's password. Ends all their sessions; with `revoke_tokens`
    /// (an admin or CLI reset, i.e. account recovery) also revokes their API
    /// tokens.
    pub async fn set_password(
        &self,
        username: &str,
        password: &str,
        revoke_tokens: bool,
    ) -> Result<User, DbError> {
        self.change_user(
            username,
            &UserChanges {
                password: Some(password.to_owned()),
                revoke_tokens,
                ..UserChanges::default()
            },
            None,
        )
        .await
    }

    /// Change a user's role. The last enabled admin can't be demoted.
    pub async fn set_role(&self, username: &str, role: Role) -> Result<User, DbError> {
        self.change_user(
            username,
            &UserChanges {
                role: Some(role),
                ..UserChanges::default()
            },
            None,
        )
        .await
    }

    /// Enable or disable a user. Disabling ends their sessions (their API
    /// tokens stop working while disabled). The last enabled admin can't be
    /// disabled.
    pub async fn set_disabled(&self, username: &str, disabled: bool) -> Result<User, DbError> {
        self.change_user(
            username,
            &UserChanges {
                disabled: Some(disabled),
                ..UserChanges::default()
            },
            None,
        )
        .await
    }

    /// Apply `changes` atomically: everything is validated first, then
    /// applied in one transaction together with `audit` (if given), so a
    /// request is either fully applied and audited or not at all. Serialised
    /// with other admin-affecting changes, so the last enabled admin can't be
    /// removed by two concurrent requests.
    pub async fn change_user(
        &self,
        username: &str,
        changes: &UserChanges,
        audit: Option<&crate::AuditEntry>,
    ) -> Result<User, DbError> {
        let hash = match &changes.password {
            Some(pw) => {
                validate_password(pw)?;
                Some(hash_password(pw).await)
            }
            None => None,
        };
        let mut tx = self.pool().begin().await?;
        lock_admins(&mut tx).await?;
        let user = find_user(&mut tx, username)
            .await?
            .ok_or(DbError::NotFound)?;
        let demotes =
            changes.role.is_some_and(|r| r != Role::Admin) || changes.disabled == Some(true);
        if demotes {
            guard_last_admin(&mut tx, &user).await?;
        }
        if let Some(role) = changes.role {
            sqlx::query("UPDATE pipes.users SET role = $2, updated_at = now() WHERE id = $1")
                .bind(user.id)
                .bind(role.as_str())
                .execute(&mut *tx)
                .await?;
        }
        if let Some(disabled) = changes.disabled {
            sqlx::query("UPDATE pipes.users SET disabled = $2, updated_at = now() WHERE id = $1")
                .bind(user.id)
                .bind(disabled)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(hash) = hash {
            sqlx::query(
                "UPDATE pipes.users SET password_hash = $2, updated_at = now() WHERE id = $1",
            )
            .bind(user.id)
            .bind(hash)
            .execute(&mut *tx)
            .await?;
        }
        if changes.password.is_some() || changes.disabled == Some(true) {
            sqlx::query("DELETE FROM pipes.sessions WHERE user_id = $1")
                .bind(user.id)
                .execute(&mut *tx)
                .await?;
        }
        if changes.revoke_tokens {
            sqlx::query("UPDATE pipes.api_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL")
                .bind(user.id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(entry) = audit {
            crate::audit::insert(&mut *tx, entry).await?;
        }
        let updated = find_user(&mut tx, username)
            .await?
            .ok_or(DbError::NotFound)?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Delete a user with their sessions and tokens, atomically with `audit`
    /// (if given). Audit entries keep the name. The last enabled admin can't
    /// be deleted.
    pub async fn delete_user(
        &self,
        username: &str,
        audit: Option<&crate::AuditEntry>,
    ) -> Result<(), DbError> {
        let mut tx = self.pool().begin().await?;
        lock_admins(&mut tx).await?;
        let user = find_user(&mut tx, username)
            .await?
            .ok_or(DbError::NotFound)?;
        guard_last_admin(&mut tx, &user).await?;
        sqlx::query("DELETE FROM pipes.users WHERE id = $1")
            .bind(user.id)
            .execute(&mut *tx)
            .await?;
        if let Some(entry) = audit {
            crate::audit::insert(&mut *tx, entry).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// Changes for [`Db::change_user`]; `None` fields are unchanged.
#[derive(Debug, Clone, Default)]
pub struct UserChanges {
    /// New role.
    pub role: Option<Role>,
    /// Disable (true) or enable (false).
    pub disabled: Option<bool>,
    /// New password (ends the user's sessions).
    pub password: Option<String>,
    /// Also revoke all the user's API tokens (account recovery).
    pub revoke_tokens: bool,
}

type Tx = sqlx::Transaction<'static, sqlx::Postgres>;

/// Serialise every change that could remove an admin (a transaction-scoped
/// advisory lock; the key is arbitrary but fixed).
async fn lock_admins(tx: &mut Tx) -> Result<(), DbError> {
    sqlx::query("SELECT pg_advisory_xact_lock(7426_1101)")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn find_user(tx: &mut Tx, username: &str) -> Result<Option<User>, DbError> {
    let row: Option<UserRow> = sqlx::query_as(concat!(
        "SELECT ",
        user_columns!(),
        " FROM pipes.users WHERE lower(username) = lower($1)"
    ))
    .bind(username)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(TryInto::try_into).transpose()
}

/// Refuse if `user` is the only enabled admin. Call under [`lock_admins`].
async fn guard_last_admin(tx: &mut Tx, user: &User) -> Result<(), DbError> {
    if user.role != Role::Admin || user.disabled {
        return Ok(());
    }
    let admins: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pipes.users WHERE role = 'admin' AND NOT disabled",
    )
    .fetch_one(&mut **tx)
    .await?;
    if admins <= 1 {
        Err(DbError::Invalid(format!(
            "`{}` is the last enabled admin; create another admin first",
            user.username
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use crate::{Db, DbError, LoginOutcome, Role};

    const PW: &str = "correct horse battery";

    #[sqlx::test]
    async fn create_and_find_users_case_insensitively(pool: PgPool) {
        let db = Db::from_pool(pool);
        let alice = db.create_user("Alice", PW, Role::Admin).await.unwrap();
        assert_eq!(
            (alice.username.as_str(), alice.role, alice.disabled),
            ("Alice", Role::Admin, false)
        );
        assert_eq!(
            db.user_by_name("alice").await.unwrap().unwrap().id,
            alice.id
        );
        assert!(matches!(
            db.create_user("ALICE", PW, Role::Viewer).await,
            Err(DbError::Conflict(_))
        ));
        db.create_user("bob", PW, Role::Viewer).await.unwrap();
        let names: Vec<String> = db
            .list_users()
            .await
            .unwrap()
            .into_iter()
            .map(|u| u.username)
            .collect();
        assert_eq!(names, vec!["Alice", "bob"]);
        assert_eq!(db.count_users().await.unwrap(), 2);
    }

    #[sqlx::test]
    async fn usernames_and_passwords_are_validated(pool: PgPool) {
        let db = Db::from_pool(pool);
        for bad in ["", "has space", "semi;colon", &"x".repeat(65)] {
            assert!(
                matches!(
                    db.create_user(bad, PW, Role::Viewer).await,
                    Err(DbError::Invalid(_))
                ),
                "{bad:?}"
            );
        }
        assert!(matches!(
            db.create_user("carol", "short", Role::Viewer).await,
            Err(DbError::Invalid(_))
        ));
        assert!(matches!(
            db.create_user("carol", &"x".repeat(2000), Role::Viewer)
                .await,
            Err(DbError::Invalid(_))
        ));
    }

    #[sqlx::test]
    async fn login_checks_password_and_disabled(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("admin", PW, Role::Admin).await.unwrap();
        db.create_user("op", PW, Role::Operator).await.unwrap();
        assert!(
            matches!(db.check_login("OP", PW).await.unwrap(), LoginOutcome::Ok(u) if u.username == "op")
        );
        assert_eq!(
            db.check_login("op", "wrong password!").await.unwrap(),
            LoginOutcome::BadCredentials
        );
        assert_eq!(
            db.check_login("nobody", PW).await.unwrap(),
            LoginOutcome::BadCredentials
        );
        assert_eq!(
            db.check_login("op", &"x".repeat(5000)).await.unwrap(),
            LoginOutcome::BadCredentials
        );
        db.set_disabled("op", true).await.unwrap();
        assert_eq!(
            db.check_login("op", PW).await.unwrap(),
            LoginOutcome::Disabled
        );
        db.set_disabled("op", false).await.unwrap();
        db.set_password("op", "a brand new password", false)
            .await
            .unwrap();
        assert_eq!(
            db.check_login("op", PW).await.unwrap(),
            LoginOutcome::BadCredentials
        );
        assert!(matches!(
            db.check_login("op", "a brand new password").await.unwrap(),
            LoginOutcome::Ok(_)
        ));
    }

    /// Two admins demoting each other at the same time must not leave zero.
    #[sqlx::test]
    async fn concurrent_demotions_keep_one_admin(pool: PgPool) {
        let db = Db::from_pool(pool);
        for round in 0..10 {
            let (a, b) = (format!("a{round}"), format!("b{round}"));
            db.create_user(&a, PW, Role::Admin).await.unwrap();
            db.create_user(&b, PW, Role::Admin).await.unwrap();
            let (r1, r2) =
                tokio::join!(db.set_role(&a, Role::Viewer), db.set_role(&b, Role::Viewer));
            assert!(
                r1.is_ok() != r2.is_ok(),
                "exactly one demotion must succeed: {r1:?} {r2:?}"
            );
            let admins = db
                .list_users()
                .await
                .unwrap()
                .into_iter()
                .filter(|u| u.role == Role::Admin && !u.disabled)
                .count();
            assert_eq!(admins, 1, "round {round}");
            // Leave no admin from this round, so the next round starts from two.
            let survivor = if r1.is_ok() { &b } else { &a };
            db.create_user(&format!("keep{round}"), PW, Role::Admin)
                .await
                .unwrap();
            db.set_role(survivor, Role::Viewer).await.unwrap();
            db.set_role(&format!("keep{round}"), Role::Viewer)
                .await
                .unwrap_err();
            db.delete_user(&format!("keep{round}"), None)
                .await
                .unwrap_err();
            sqlx::query("UPDATE pipes.users SET role = 'viewer' WHERE username = $1")
                .bind(format!("keep{round}"))
                .execute(db.pool())
                .await
                .unwrap();
        }
    }

    #[sqlx::test]
    async fn changes_are_all_or_nothing_and_audited_together(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", PW, Role::Admin).await.unwrap();
        db.create_user("op", PW, Role::Operator).await.unwrap();
        let bad = crate::UserChanges {
            role: Some(Role::Admin),
            password: Some("short".into()),
            ..Default::default()
        };
        assert!(matches!(
            db.change_user("op", &bad, None).await,
            Err(DbError::Invalid(_))
        ));
        assert_eq!(
            db.user_by_name("op").await.unwrap().unwrap().role,
            Role::Operator,
            "nothing applied"
        );
        let entry = crate::AuditEntry {
            actor_id: None,
            actor: "root".into(),
            auth_method: "session".into(),
            action: "user.update".into(),
            target: "op".into(),
            outcome: crate::Outcome::Ok,
            detail: serde_json::json!({}),
        };
        let good = crate::UserChanges {
            role: Some(Role::Viewer),
            disabled: Some(true),
            ..Default::default()
        };
        let u = db.change_user("op", &good, Some(&entry)).await.unwrap();
        assert_eq!((u.role, u.disabled), (Role::Viewer, true));
        assert_eq!(
            db.list_audit(&crate::AuditQuery::default())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[sqlx::test]
    async fn a_recovery_reset_revokes_tokens(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", PW, Role::Admin).await.unwrap();
        let op = db.create_user("op", PW, Role::Operator).await.unwrap();
        let (t1, _) = db.create_api_token(op.id, "a", None).await.unwrap();
        db.set_password("op", "first new password", false)
            .await
            .unwrap();
        assert!(
            db.token_user(&t1).await.unwrap().is_some(),
            "a self-service change keeps tokens"
        );
        db.set_password("op", "second new password", true)
            .await
            .unwrap();
        assert!(
            db.token_user(&t1).await.unwrap().is_none(),
            "a recovery reset revokes them"
        );
    }

    #[sqlx::test]
    async fn the_last_enabled_admin_is_protected(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", PW, Role::Admin).await.unwrap();
        for r in [
            db.set_role("root", Role::Viewer).await.map(|_| ()),
            db.set_disabled("root", true).await.map(|_| ()),
            db.delete_user("root", None).await,
        ] {
            assert!(
                matches!(r, Err(DbError::Invalid(ref m)) if m.contains("last enabled admin")),
                "{r:?}"
            );
        }
        db.create_user("second", PW, Role::Admin).await.unwrap();
        db.set_role("root", Role::Operator).await.unwrap();
        assert!(
            db.set_disabled("second", true).await.is_err(),
            "second is now the last admin"
        );
        assert!(matches!(
            db.delete_user("ghost", None).await,
            Err(DbError::NotFound)
        ));
        assert!(matches!(
            db.set_role("ghost", Role::Admin).await,
            Err(DbError::NotFound)
        ));
    }
}
