//! Recording: the API over the [`Recorder`] (rules, streams, recent records),
//! stored records, and the database sink.
//!
//! - `GET /api/v1/recording`: rules in force and the streams they run.
//! - `PUT /api/v1/recording/rules`: replace the rules (admin; audited before
//!   the change, which is refused if the audit log can't be written). They
//!   take effect at once and last until the next change or restart (the
//!   `--record-config` file is not rewritten).
//! - `GET /api/v1/recording/recent`: the latest records from every sink, kept
//!   in memory, so this works without a database.
//! - `GET /api/v1/records`: stored records, filtered (needs a database).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{Json, extract::State};
use pipes_core::{
    record::{Record, RecordRule, RecordSink, validate_rules},
    recorder::{Recorder, RecordingView},
};
use pipes_db::{Db, NewRecord, Outcome, RecordQuery, Role, StoredRecord};
use serde::Deserialize;
use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use utoipa::{IntoParams, ToSchema};

use crate::{
    AppState,
    accounts::{ApiJson, require_role},
    api::ApiQuery,
    auth::{Caller, audit, record_audit},
    error::{ApiError, Problem},
};

/// Records buffered for the database; beyond this new ones are dropped
/// (counted as `db_backlog`).
pub const DB_BACKLOG: usize = 10_000;
/// Records written per insert.
const DB_BATCH: usize = 500;
/// Longest a record waits in the buffer before a (partial) batch is written.
const DB_FLUSH: Duration = Duration::from_millis(250);
/// Wait before retrying a failed batch once.
const DB_RETRY: Duration = Duration::from_secs(1);

/// The `database` sink: a bounded buffer drained by a writer task in batches.
#[derive(Debug)]
pub struct DbSink {
    tx: mpsc::Sender<Arc<Record>>,
    dropped: AtomicU64,
}

impl DbSink {
    /// Start the writer task over `db`. It stops when `shutdown` is
    /// cancelled, after writing what is already buffered.
    pub fn spawn(db: Db, shutdown: CancellationToken) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(DB_BACKLOG);
        let task = tokio::spawn(write_batches(db, rx, shutdown));
        (
            Arc::new(Self {
                tx,
                dropped: AtomicU64::new(0),
            }),
            task,
        )
    }

    /// Records dropped because the buffer was full or the writer had stopped.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl RecordSink for DbSink {
    fn accept(&self, record: &Arc<Record>) {
        if self.tx.try_send(record.clone()).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed);
            ::metrics::counter!("rmf_pipes_records_dropped_total", "rule" => record.rule.clone(), "reason" => "db_backlog")
                .increment(1);
            if n.is_power_of_two() {
                tracing::warn!(
                    dropped = n + 1,
                    "the database can't keep up with recording; records are being dropped"
                );
            }
        }
    }
}

async fn write_batches(db: Db, mut rx: mpsc::Receiver<Arc<Record>>, shutdown: CancellationToken) {
    let mut batch: Vec<NewRecord> = Vec::with_capacity(DB_BATCH);
    loop {
        let first = tokio::select! {
            () = shutdown.cancelled() => None,
            r = rx.recv() => r,
        };
        let Some(first) = first else { break };
        batch.push(new_record(&first));
        let deadline = tokio::time::Instant::now() + DB_FLUSH;
        while batch.len() < DB_BATCH {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(r)) => batch.push(new_record(&r)),
                Ok(None) | Err(_) => break,
            }
        }
        flush(&db, &mut batch).await;
    }
    // Shutting down: write what is already buffered, without waiting for more.
    while let Ok(r) = rx.try_recv() {
        batch.push(new_record(&r));
        if batch.len() >= DB_BATCH {
            flush(&db, &mut batch).await;
        }
    }
    flush(&db, &mut batch).await;
}

async fn flush(db: &Db, batch: &mut Vec<NewRecord>) {
    if batch.is_empty() {
        return;
    }
    // One retry covers a transient failure (a restarted database, a dropped
    // connection); the writer must not stall the buffer for longer.
    let mut result = db.insert_records(batch).await;
    if result.is_err() {
        tokio::time::sleep(DB_RETRY).await;
        result = db.insert_records(batch).await;
    }
    if let Err(err) = result {
        ::metrics::counter!("rmf_pipes_records_dropped_total", "rule" => "", "reason" => "db_error")
            .increment(batch.len() as u64);
        tracing::error!(%err, records = batch.len(), "cannot store records; dropping the batch");
    }
    batch.clear();
}

fn new_record(r: &Record) -> NewRecord {
    NewRecord {
        at: r.at,
        rule: r.rule.clone(),
        kind: r.kind.as_str().to_owned(),
        name: r.name.clone(),
        part: r.part.as_str().to_owned(),
        type_name: r.type_name.clone(),
        goal_id: r.goal_id.clone(),
        data: r.data.clone(),
    }
}

fn recorder(state: &AppState) -> Result<&Recorder, Problem> {
    state
        .recorder()
        .ok_or_else(|| Problem::unavailable("recording_disabled", "recording is not configured"))
}

/// The recording rules in force and the streams they run.
#[utoipa::path(get, path = "/api/v1/recording", tag = "recording",
    responses((status = 200, description = "Rules and streams", body = RecordingView)))]
pub async fn view(State(state): State<AppState>) -> Result<Json<RecordingView>, Problem> {
    Ok(Json(recorder(&state)?.view()))
}

/// A new rule set.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RulesBody {
    /// Replaces every rule. An empty list stops all recording.
    pub rules: Vec<RecordRule>,
}

/// Replace the recording rules (admin).
///
/// Takes effect at once: streams of removed or changed rules stop, new ones
/// start within a second. Lasts until the next change or a restart.
#[utoipa::path(put, path = "/api/v1/recording/rules", tag = "recording", request_body = RulesBody,
    responses(
        (status = 200, description = "The rules in force, normalized", body = Vec<RecordRule>),
        (status = 400, description = "Invalid rules", body = ApiError),
        (status = 403, body = ApiError)))]
pub async fn set_rules(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiJson(body): ApiJson<RulesBody>,
) -> Result<Json<Vec<RecordRule>>, Problem> {
    let rec = recorder(&state)?;
    require_role(&state, &caller, Role::Admin, "recording.update", "").await?;
    let rules = match validate_rules(body.rules, rec.database_available()) {
        Ok(r) => r,
        Err(e) => {
            audit(
                &state,
                caller.audit(
                    "recording.update",
                    "",
                    Outcome::Error,
                    json!({ "reason": e.0 }),
                ),
            )
            .await;
            return Err(Problem::bad_request("invalid_rules", e.0));
        }
    };
    // Like writes, fail closed: no audit entry, no change.
    record_audit(
        &state,
        &caller.audit(
            "recording.update",
            "",
            Outcome::Ok,
            json!({ "rules": rules }),
        ),
    )
    .await
    .map_err(|_| {
        Problem::unavailable(
            "audit_unavailable",
            "the audit log can't be written, so the rules were not changed",
        )
    })?;
    let rules = rec
        .set_rules(rules)
        .map_err(|e| Problem::bad_request("invalid_rules", e.0))?;
    tracing::info!(by = %caller.name(), rules = rules.len(), "recording rules replaced");
    Ok(Json(rules))
}

/// `?limit=` for recent records.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RecentParams {
    /// At most this many (default 50, at most 200).
    pub limit: Option<usize>,
}

/// The latest records from every sink, newest first (kept in memory).
#[utoipa::path(get, path = "/api/v1/recording/recent", tag = "recording", params(RecentParams),
    responses((status = 200, description = "Records, newest first", body = Vec<Record>)))]
pub async fn recent(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<RecentParams>,
) -> Result<Json<Vec<Record>>, Problem> {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    Ok(Json(
        recorder(&state)?
            .recent(limit)
            .iter()
            .map(|r| r.as_ref().clone())
            .collect(),
    ))
}

/// Filters for stored records.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RecordParams {
    /// At most this many (default 100, at most 1000).
    pub limit: Option<i64>,
    /// Only records with a smaller id (page backwards).
    pub before: Option<i64>,
    /// Only this rule.
    pub rule: Option<String>,
    /// Only this topic, service or action.
    pub name: Option<String>,
    /// Only `topic`, `service` or `action`.
    pub kind: Option<String>,
    /// Only `message`, `request`, `goal`, `feedback`, `status` or `result`.
    pub part: Option<String>,
    /// Only this action goal (UUID).
    pub goal_id: Option<String>,
    /// Only records at or after this time (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    #[param(value_type = Option<String>, format = DateTime)]
    pub since: Option<OffsetDateTime>,
    /// Only records before this time (RFC 3339).
    #[serde(default, with = "time::serde::rfc3339::option")]
    #[param(value_type = Option<String>, format = DateTime)]
    pub until: Option<OffsetDateTime>,
}

/// Stored records, newest first (needs a database).
#[utoipa::path(get, path = "/api/v1/records", tag = "recording", params(RecordParams),
    responses(
        (status = 200, description = "Records, newest first", body = Vec<StoredRecord>),
        (status = 400, body = ApiError),
        (status = 503, description = "No database", body = ApiError)))]
pub async fn records(
    State(state): State<AppState>,
    ApiQuery(q): ApiQuery<RecordParams>,
) -> Result<Json<Vec<StoredRecord>>, Problem> {
    let db = state.db().ok_or_else(|| {
        Problem::unavailable(
            "no_database",
            "stored records need a database (--database-url); see /api/v1/recording/recent",
        )
    })?;
    Ok(Json(
        db.list_records(&RecordQuery {
            limit: q.limit,
            before: q.before,
            rule: q.rule,
            name: q.name,
            kind: q.kind,
            part: q.part,
            goal_id: q.goal_id,
            since: q.since,
            until: q.until,
        })
        .await?,
    ))
}
