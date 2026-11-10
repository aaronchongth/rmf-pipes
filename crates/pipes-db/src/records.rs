//! Recordings: rows written by the recorder's `database` sink.

use serde::Serialize;
use time::OffsetDateTime;

use crate::{Db, DbError, audit::strip_nul};

/// A record to store.
#[derive(Debug, Clone, PartialEq)]
pub struct NewRecord {
    /// When it was observed.
    pub at: OffsetDateTime,
    /// The recording rule.
    pub rule: String,
    /// `topic`, `service` or `action`.
    pub kind: String,
    /// Topic, service or action name.
    pub name: String,
    /// `message`, `request`, `goal`, `feedback`, `status` or `result`.
    pub part: String,
    /// ROS type.
    pub type_name: String,
    /// Action goal UUID (canonical text form), if any.
    pub goal_id: Option<String>,
    /// The payload.
    pub data: Option<serde_json::Value>,
}

/// A stored record.
#[derive(Debug, Clone, PartialEq, Serialize, sqlx::FromRow)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoredRecord {
    /// Increasing id; pass the smallest one seen as `before` to page back.
    pub id: i64,
    /// When it was observed.
    #[serde(with = "time::serde::rfc3339")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    pub at: OffsetDateTime,
    /// The recording rule.
    pub rule: String,
    /// `topic`, `service` or `action`.
    pub kind: String,
    /// Topic, service or action name.
    pub name: String,
    /// `message`, `request`, `goal`, `feedback`, `status` or `result`.
    pub part: String,
    /// ROS type.
    pub type_name: String,
    /// Action goal UUID, if any.
    pub goal_id: Option<String>,
    /// The payload (JSON per `docs/json-mapping.md`).
    #[cfg_attr(feature = "openapi", schema(value_type = Option<Object>))]
    pub data: Option<serde_json::Value>,
}

/// Filters for [`Db::list_records`]. Every filter is optional.
#[derive(Debug, Clone, Default)]
pub struct RecordQuery {
    /// At most this many (capped at 1000; default 100).
    pub limit: Option<i64>,
    /// Only records with a smaller id (paging backwards).
    pub before: Option<i64>,
    /// Only this rule.
    pub rule: Option<String>,
    /// Only this topic, service or action.
    pub name: Option<String>,
    /// Only this kind.
    pub kind: Option<String>,
    /// Only this part.
    pub part: Option<String>,
    /// Only this action goal (UUID text).
    pub goal_id: Option<String>,
    /// Only records at or after this time.
    pub since: Option<OffsetDateTime>,
    /// Only records before this time.
    pub until: Option<OffsetDateTime>,
}

/// Most rows per insert statement.
pub const MAX_BATCH: usize = 1000;
/// Rows deleted per statement by [`Db::purge_records`].
pub const PURGE_CHUNK: i64 = 10_000;

impl Db {
    /// Store `records` in one statement (at most [`MAX_BATCH`]). A goal id
    /// that is not a UUID is stored as `NULL` rather than failing the batch.
    pub async fn insert_records(&self, records: &[NewRecord]) -> Result<u64, DbError> {
        if records.is_empty() {
            return Ok(0);
        }
        if records.len() > MAX_BATCH {
            return Err(DbError::Invalid(format!(
                "at most {MAX_BATCH} records per batch"
            )));
        }
        let clean = |s: &str| s.replace('\0', "\u{FFFD}");
        let mut at = Vec::with_capacity(records.len());
        let (mut rule, mut kind, mut name, mut part, mut ty, mut goal, mut data) = (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        for r in records {
            at.push(r.at);
            rule.push(clean(&r.rule));
            kind.push(r.kind.clone());
            name.push(clean(&r.name));
            part.push(r.part.clone());
            ty.push(clean(&r.type_name));
            goal.push(r.goal_id.clone().filter(|g| is_uuid(g)));
            data.push(r.data.as_ref().map(|d| sqlx::types::Json(strip_nul(d))));
        }
        let done = sqlx::query(
            "INSERT INTO pipes.records (at, rule, kind, name, part, type_name, goal_id, data) \
             SELECT at, rule, kind, name, part, type_name, goal_id::uuid, data \
             FROM UNNEST($1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[], \
                         $6::text[], $7::text[], $8::jsonb[]) \
                  AS t(at, rule, kind, name, part, type_name, goal_id, data)",
        )
        .bind(at)
        .bind(rule)
        .bind(kind)
        .bind(name)
        .bind(part)
        .bind(ty)
        .bind(goal)
        .bind(data)
        .execute(self.pool())
        .await?;
        Ok(done.rows_affected())
    }

    /// Records, newest first.
    pub async fn list_records(&self, q: &RecordQuery) -> Result<Vec<StoredRecord>, DbError> {
        let limit = q.limit.unwrap_or(100).clamp(1, 1000);
        // Postgres text can't hold NUL; such a filter can't match anything,
        // so say so instead of failing in the database.
        for f in [&q.rule, &q.name, &q.kind, &q.part].into_iter().flatten() {
            if f.contains('\0') {
                return Err(DbError::Invalid(
                    "filters must not contain NUL characters".into(),
                ));
            }
        }
        if let Some(g) = &q.goal_id
            && !is_uuid(g)
        {
            return Err(DbError::Invalid(format!(
                "goal_id `{}` is not a UUID",
                g.chars().take(40).collect::<String>()
            )));
        }
        Ok(sqlx::query_as(
            "SELECT id, at, rule, kind, name, part, type_name, goal_id::text AS goal_id, data \
             FROM pipes.records \
             WHERE ($1::bigint IS NULL OR id < $1) \
               AND ($2::text IS NULL OR rule = $2) \
               AND ($3::text IS NULL OR name = $3) \
               AND ($4::text IS NULL OR kind = $4) \
               AND ($5::text IS NULL OR part = $5) \
               AND ($6::uuid IS NULL OR goal_id = $6::uuid) \
               AND ($7::timestamptz IS NULL OR at >= $7) \
               AND ($8::timestamptz IS NULL OR at < $8) \
             ORDER BY id DESC LIMIT $9",
        )
        .bind(q.before)
        .bind(q.rule.as_deref())
        .bind(q.name.as_deref())
        .bind(q.kind.as_deref())
        .bind(q.part.as_deref())
        .bind(q.goal_id.as_deref())
        .bind(q.since)
        .bind(q.until)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    /// Delete records observed before `cutoff`, in chunks of
    /// [`PURGE_CHUNK`] so no single statement holds locks for long; returns
    /// how many.
    pub async fn purge_records(&self, cutoff: OffsetDateTime) -> Result<u64, DbError> {
        let mut total = 0;
        loop {
            let n = sqlx::query(
                "DELETE FROM pipes.records WHERE id IN \
                 (SELECT id FROM pipes.records WHERE at < $1 LIMIT $2)",
            )
            .bind(cutoff)
            .bind(PURGE_CHUNK)
            .execute(self.pool())
            .await?
            .rows_affected();
            total += n;
            if n < PURGE_CHUNK as u64 {
                return Ok(total);
            }
        }
    }
}

/// Canonical `8-4-4-4-12` hex UUID text.
fn is_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::PgPool;
    use time::Duration;

    use super::*;

    const G: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";

    fn rec(name: &str, part: &str, goal: Option<&str>, at: OffsetDateTime) -> NewRecord {
        NewRecord {
            at,
            rule: "nav".into(),
            kind: "action".into(),
            name: name.into(),
            part: part.into(),
            type_name: "nav2_msgs/action/NavigateToPose".into(),
            goal_id: goal.map(Into::into),
            data: Some(json!({"pose": {"x": 1.5}, "nul": "a\u{0}b"})),
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn records_round_trip_with_filters_and_paging(pool: PgPool) {
        let db = Db::from_pool(pool);
        let now = OffsetDateTime::now_utc();
        let batch = vec![
            rec("/r0/nav", "goal", Some(G), now - Duration::minutes(2)),
            rec("/r0/nav", "feedback", Some(G), now - Duration::minutes(1)),
            rec("/r1/nav", "goal", Some("not-a-uuid"), now),
        ];
        assert_eq!(db.insert_records(&batch).await.unwrap(), 3);
        let all = db.list_records(&RecordQuery::default()).await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|w| w[0].id > w[1].id), "newest first");
        assert_eq!(all[0].goal_id, None, "a bad goal id is stored as NULL");
        assert_eq!(all[2].goal_id.as_deref(), Some(G));
        assert_eq!(
            all[2].data,
            Some(json!({"pose": {"x": 1.5}, "nul": "a\u{FFFD}b"}))
        );
        let by_goal = db
            .list_records(&RecordQuery {
                goal_id: Some(G.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_goal.len(), 2);
        let q = |f: fn(&mut RecordQuery)| {
            let mut q = RecordQuery::default();
            f(&mut q);
            q
        };
        for (query, n) in [
            (q(|q| q.name = Some("/r1/nav".into())), 1),
            (q(|q| q.part = Some("goal".into())), 2),
            (q(|q| q.kind = Some("topic".into())), 0),
            (q(|q| q.rule = Some("nav".into())), 3),
            (q(|q| q.limit = Some(1)), 1),
        ] {
            assert_eq!(db.list_records(&query).await.unwrap().len(), n, "{query:?}");
        }
        let since = db
            .list_records(&RecordQuery {
                since: Some(now - Duration::seconds(90)),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(since.len(), 2);
        let older = db
            .list_records(&RecordQuery {
                before: Some(all[0].id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(older.len(), 2);
        assert!(matches!(
            db.list_records(&RecordQuery {
                goal_id: Some("x".into()),
                ..Default::default()
            })
            .await,
            Err(DbError::Invalid(_))
        ));
        assert_eq!(
            db.purge_records(now - Duration::seconds(30)).await.unwrap(),
            2
        );
        assert_eq!(
            db.list_records(&RecordQuery::default())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn filters_bounds_and_order_are_exact(pool: PgPool) {
        let db = Db::from_pool(pool);
        let t0 = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();
        let mut other = rec("/x\u{0}y", "goal", None, t0 + Duration::seconds(10));
        other.rule = "other".into();
        // Inserted last but observed first: newest-first is by id, not time.
        let batch = vec![
            rec("/a", "goal", None, t0),
            other,
            rec("/a", "goal", None, t0 - Duration::seconds(10)),
        ];
        db.insert_records(&batch).await.unwrap();
        let ids = |v: Vec<StoredRecord>| v.into_iter().map(|r| r.at).collect::<Vec<_>>();
        let all = db.list_records(&RecordQuery::default()).await.unwrap();
        assert_eq!(all[0].at, t0 - Duration::seconds(10), "ordered by id");
        assert_eq!(all[1].name, "/x\u{FFFD}y", "NUL in names is replaced");
        let q = |f: fn(&mut RecordQuery, OffsetDateTime), t0| {
            let mut q = RecordQuery::default();
            f(&mut q, t0);
            q
        };
        let by_rule = db
            .list_records(&q(|q, _| q.rule = Some("other".into()), t0))
            .await
            .unwrap();
        assert_eq!(by_rule.len(), 1);
        let since = db
            .list_records(&q(|q, t| q.since = Some(t), t0))
            .await
            .unwrap();
        assert_eq!(ids(since).len(), 2, "since is inclusive");
        let until = db
            .list_records(&q(|q, t| q.until = Some(t), t0))
            .await
            .unwrap();
        assert_eq!(
            ids(until),
            [t0 - Duration::seconds(10)],
            "until is exclusive"
        );
        for bad in [
            q(|q, _| q.name = Some("a\u{0}".into()), t0),
            q(|q, _| q.rule = Some("\u{0}".into()), t0),
        ] {
            assert!(matches!(
                db.list_records(&bad).await,
                Err(DbError::Invalid(_))
            ));
        }
        assert_eq!(
            db.purge_records(t0).await.unwrap(),
            1,
            "purge is strictly before the cutoff"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn listing_is_capped_and_purging_works_in_chunks(pool: PgPool) {
        let db = Db::from_pool(pool);
        let old = OffsetDateTime::now_utc() - Duration::days(30);
        let batch = vec![rec("/x", "feedback", None, old); MAX_BATCH];
        for _ in 0..11 {
            db.insert_records(&batch).await.unwrap();
        }
        let page = db
            .list_records(&RecordQuery {
                limit: Some(5000),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.len(), 1000, "at most 1000 per page");
        assert_eq!(
            db.purge_records(OffsetDateTime::now_utc()).await.unwrap(),
            11 * MAX_BATCH as u64,
            "more than one chunk"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn batches_are_bounded(pool: PgPool) {
        let db = Db::from_pool(pool);
        assert_eq!(db.insert_records(&[]).await.unwrap(), 0);
        let now = OffsetDateTime::now_utc();
        let too_many = vec![rec("/x", "goal", None, now); MAX_BATCH + 1];
        assert!(matches!(
            db.insert_records(&too_many).await,
            Err(DbError::Invalid(_))
        ));
    }

    #[test]
    fn uuids_are_recognized() {
        assert!(is_uuid(G));
        assert!(!is_uuid("00010203-0405-0607-0809-0a0b0c0d0e0"));
        assert!(!is_uuid("0001020304050607080900a0b0c0d0e0f000"));
    }
}
