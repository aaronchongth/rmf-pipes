//! The audit log: append-only records of logins, credential and user changes,
//! and every write to the robot bus.

use serde::Serialize;
use time::OffsetDateTime;

use crate::{Db, DbError};

/// How an audited action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// It happened.
    Ok,
    /// It was refused (bad credentials, missing role, not allowlisted, rate
    /// limited).
    Denied,
    /// It was allowed but failed (e.g. the bus rejected the message).
    Error,
    /// About to be attempted; a later `ok` or `error` entry with
    /// `detail.intent` = this entry's id records how it ended.
    Pending,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Denied => "denied",
            Self::Error => "error",
            Self::Pending => "pending",
        }
    }
}

/// Something to record.
#[derive(Debug, Clone)]
pub struct AuditEntry {
    /// The acting user, if known.
    pub actor_id: Option<i64>,
    /// The acting user's name, or what was claimed (e.g. a failed login's
    /// username), or `anonymous`/`cli`.
    pub actor: String,
    /// `session`, `token:<id>`, `cli` or `none`.
    pub auth_method: String,
    /// What was attempted, e.g. `login`, `token.create`, `topic.publish`.
    pub action: String,
    /// What it was done to (username, token id, topic name).
    pub target: String,
    /// How it ended.
    pub outcome: Outcome,
    /// Action-specific details (the published message, the reason refused).
    pub detail: serde_json::Value,
}

/// A recorded entry.
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuditRecord {
    /// Increasing id; pass the smallest one seen as `before` to page back.
    pub id: i64,
    /// When it happened.
    #[serde(with = "time::serde::rfc3339")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    pub at: OffsetDateTime,
    /// Acting user's id, if known and the user still exists.
    pub actor_id: Option<i64>,
    /// Acting user's name (kept after the user is deleted).
    pub actor: String,
    /// How the actor authenticated.
    pub auth_method: String,
    /// What was attempted.
    pub action: String,
    /// What it was done to.
    pub target: String,
    /// `ok`, `denied`, `error` or `pending` (a write about to be attempted).
    pub outcome: String,
    /// Action-specific details.
    #[cfg_attr(feature = "openapi", schema(value_type = Object))]
    pub detail: serde_json::Value,
}

/// Filters for [`Db::list_audit`].
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    /// At most this many (capped at 1000; default 100).
    pub limit: Option<i64>,
    /// Only entries with a smaller id (paging backwards).
    pub before: Option<i64>,
    /// Only this action (e.g. `topic.publish`).
    pub action: Option<String>,
    /// Only this actor.
    pub actor: Option<String>,
}

/// Insert `e` with any executor (the pool, or a transaction so an entry
/// commits together with the change it records).
pub(crate) async fn insert<'e>(
    ex: impl sqlx::PgExecutor<'e>,
    e: &AuditEntry,
) -> Result<i64, DbError> {
    Ok(sqlx::query_scalar(
        "INSERT INTO pipes.audit_log (actor_id, actor, auth_method, action, target, outcome, detail) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind(e.actor_id)
    .bind(e.actor.replace('\0', "\u{FFFD}"))
    .bind(&e.auth_method)
    .bind(&e.action)
    .bind(e.target.replace('\0', "\u{FFFD}"))
    .bind(e.outcome.as_str())
    .bind(sqlx::types::Json(strip_nul(&e.detail)))
    .fetch_one(ex)
    .await?)
}

/// Postgres `jsonb` rejects `\u0000`; replace it so attacker-chosen strings
/// can't make an audit write fail.
pub(crate) fn strip_nul(v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::String(s) => Value::String(s.replace('\0', "\u{FFFD}")),
        Value::Array(a) => Value::Array(a.iter().map(strip_nul).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.replace('\0', "\u{FFFD}"), strip_nul(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

impl Db {
    /// Append an entry.
    pub async fn record_audit(&self, e: &AuditEntry) -> Result<i64, DbError> {
        insert(self.pool(), e).await
    }

    /// Entries, newest first.
    pub async fn list_audit(&self, q: &AuditQuery) -> Result<Vec<AuditRecord>, DbError> {
        let limit = q.limit.unwrap_or(100).clamp(1, 1000);
        Ok(sqlx::query_as(
            "SELECT id, at, actor_id, actor, auth_method, action, target, outcome, detail FROM pipes.audit_log \
             WHERE ($1::bigint IS NULL OR id < $1) AND ($2::text IS NULL OR action = $2) \
               AND ($3::text IS NULL OR lower(actor) = lower($3)) \
             ORDER BY id DESC LIMIT $4",
        )
        .bind(q.before)
        .bind(q.action.as_deref())
        .bind(q.actor.as_deref())
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::PgPool;

    use super::*;
    use crate::Role;

    fn entry(action: &str, actor: &str) -> AuditEntry {
        AuditEntry {
            actor_id: None,
            actor: actor.into(),
            auth_method: "session".into(),
            action: action.into(),
            target: "/door_requests".into(),
            outcome: Outcome::Ok,
            detail: json!({"data": {"door": "main"}}),
        }
    }

    #[sqlx::test]
    async fn entries_are_listed_newest_first_with_filters_and_paging(pool: PgPool) {
        let db = Db::from_pool(pool);
        for i in 0..5 {
            db.record_audit(&entry(
                if i % 2 == 0 { "topic.publish" } else { "login" },
                "op",
            ))
            .await
            .unwrap();
        }
        let all = db.list_audit(&AuditQuery::default()).await.unwrap();
        assert_eq!(all.len(), 5);
        assert!(all.windows(2).all(|w| w[0].id > w[1].id));
        assert_eq!(all[0].detail, json!({"data": {"door": "main"}}));
        let publishes = db
            .list_audit(&AuditQuery {
                action: Some("topic.publish".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(publishes.len(), 3);
        let page = db
            .list_audit(&AuditQuery {
                limit: Some(2),
                before: Some(all[1].id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![all[2].id, all[3].id]
        );
        assert_eq!(
            db.list_audit(&AuditQuery {
                actor: Some("OP".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
            5
        );
    }

    #[sqlx::test]
    async fn nul_bytes_cannot_break_an_audit_write(pool: PgPool) {
        let db = Db::from_pool(pool);
        let e = AuditEntry {
            target: "a\0b".into(),
            detail: json!({"k\0": "v\0", "n": [1, "x\0"]}),
            outcome: Outcome::Pending,
            ..entry("topic.publish", "o\0p")
        };
        db.record_audit(&e).await.unwrap();
        let r = &db.list_audit(&AuditQuery::default()).await.unwrap()[0];
        assert_eq!(r.outcome, "pending");
        assert!(!r.detail.to_string().contains("\\u0000") && !r.target.contains('\0'));
    }

    #[sqlx::test]
    async fn entries_outlive_their_actor(pool: PgPool) {
        let db = Db::from_pool(pool);
        db.create_user("root", "correct horse battery", Role::Admin)
            .await
            .unwrap();
        let op = db
            .create_user("op", "correct horse battery", Role::Operator)
            .await
            .unwrap();
        db.record_audit(&AuditEntry {
            actor_id: Some(op.id),
            ..entry("topic.publish", "op")
        })
        .await
        .unwrap();
        db.delete_user("op", None).await.unwrap();
        let r = &db.list_audit(&AuditQuery::default()).await.unwrap()[0];
        assert_eq!((r.actor_id, r.actor.as_str()), (None, "op"));
    }
}
