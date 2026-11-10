//! The recorder: applies [`RecordRule`]s to the live graph and sends each
//! observed message, request, goal, feedback, status change and result to the
//! rule's sinks.
//!
//! Every [`Recorder::tick`] (once a second when spawned) matches the rules
//! against the hub's latest graph and reconciles a set of *streams*, one per
//! (rule, target, source):
//!
//! | Source | For | Observed by |
//! |---|---|---|
//! | `message` | topic rules | [`Hub::watch`] of the topic |
//! | `request` | service rules | [`Bus::tap`](crate::bus::Bus::tap) of the service |
//! | `goal` | action rules with `goal` | tap of `<action>/_action/send_goal` |
//! | `feedback` | action rules with `feedback` | watch of `<action>/_action/feedback` |
//! | `status` | action rules with `status` or `result` | watch of `<action>/_action/status`; a goal reaching a terminal status triggers a `get_result` call |
//!
//! Streams of targets that disappear, or of rules that are removed or changed,
//! are stopped; a stream that fails or ends is retried after `retry_after`.
//! At most `max_streams` streams run; matches beyond that are skipped
//! (reported in [`RecordingView::skipped`] and logged once per change).
//!
//! Topic samples arrive through the hub, so the hub's own rate and size caps
//! apply first. A rule's `max_rate_hz` then drops (and counts) messages,
//! requests and feedback beyond the rate, per stream. Goals, statuses and
//! results are never throttled.
//!
//! When a status stream starts, goals it first sees already finished (from
//! the topic's transient-local history) are taken as a baseline and not
//! recorded, so restarting rmf-pipes doesn't re-record old goals. The same
//! applies when a stream restarts (retry, changed rule): goals that finished
//! while it was down are not recorded, and goals still running are recorded
//! again from their current status.
//!
//! Metrics: `rmf_pipes_records_total` (counter; `rule`, `part`),
//! `rmf_pipes_records_dropped_total` (counter; `rule`, `reason` =
//! `rate_limited` | `lagged` | `decode_error` | `result_unavailable` |
//! `result_backlog`),
//! `rmf_pipes_recording_streams` (gauge).

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::{
    sync::{Semaphore, broadcast::error::RecvError},
    task::AbortHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    graph::GraphSnapshot,
    hub::{Hub, TopicEvent, TopicStatus},
    record::{
        Part, Record, RecordRule, RecordSink, RuleError, Sink, TargetKind, uuid_text,
        validate_rules,
    },
    sample::Sample,
};

/// Recorder tuning.
#[derive(Debug, Clone)]
pub struct RecorderConfig {
    /// How often [`Recorder::spawn`] calls [`Recorder::tick`].
    pub tick_interval: Duration,
    /// How long a failed or ended stream waits before it is retried.
    pub retry_after: Duration,
    /// Most streams at once.
    pub max_streams: usize,
    /// Records kept in memory for [`Recorder::recent`].
    pub recent: usize,
    /// Timeout of each `get_result` call.
    pub result_timeout: Duration,
    /// Most `get_result` calls in flight at once; further finished goals wait.
    pub max_result_calls: usize,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(1),
            retry_after: Duration::from_secs(5),
            max_streams: 256,
            recent: 200,
            result_timeout: Duration::from_secs(5),
            max_result_calls: 8,
        }
    }
}

/// Where records can go.
#[derive(Debug, Clone)]
pub struct Sinks {
    /// For rules with the `console` sink.
    pub console: Arc<dyn RecordSink>,
    /// For rules with the `database` sink; `None` when no database is
    /// configured (such rules are then refused).
    pub database: Option<Arc<dyn RecordSink>>,
}

/// What a stream observes.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// A topic's messages.
    Message,
    /// A service's requests.
    Request,
    /// An action's goals.
    Goal,
    /// An action's feedback.
    Feedback,
    /// An action's goal statuses (and, from them, results).
    Status,
}

/// Where a stream is in its lifecycle.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StreamState {
    /// Subscribing or tapping.
    Opening,
    /// Recording.
    Active,
    /// Not recording; `error` says why. Retried after `retry_after` if the
    /// stream ended, or by the hub if its topic failed.
    Failed {
        /// Human-readable reason.
        error: String,
    },
}

/// One running stream.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize)]
pub struct StreamView {
    /// The rule it belongs to.
    pub rule: String,
    /// The rule's kind.
    pub kind: TargetKind,
    /// Topic, service or action name.
    pub name: String,
    /// What it observes.
    pub source: Source,
    /// The topic or service it observes, e.g. `/robot0/navigate_to_pose/_action/feedback`.
    pub observes: String,
    /// Lifecycle state.
    pub status: StreamState,
    /// Records produced.
    pub recorded: u64,
    /// Events dropped (rate limit, lag, decode errors, unavailable results).
    pub dropped: u64,
}

/// The recorder's rules and streams.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Serialize)]
pub struct RecordingView {
    /// The rules in force.
    pub rules: Vec<RecordRule>,
    /// Running streams, sorted by rule, name and source.
    pub streams: Vec<StreamView>,
    /// Matches not recorded because `max_streams` streams already run.
    pub skipped: usize,
    /// Whether the `database` sink is available.
    pub database: bool,
}

/// Cheap-to-clone handle to the recorder.
#[derive(Debug, Clone)]
pub struct Recorder {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    hub: Hub,
    cfg: RecorderConfig,
    sinks: Sinks,
    rules: RwLock<Arc<Vec<RecordRule>>>,
    streams: Mutex<HashMap<Key, Entry>>,
    skipped: AtomicUsize,
    recent: Mutex<VecDeque<Arc<Record>>>,
    results: Arc<Semaphore>,
    shutdown: CancellationToken,
}

/// Identifies a stream. `rule` is the whole rule, serialized, so changing a
/// rule restarts its streams.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Key {
    rule_name: String,
    name: String,
    source: SourceOrd,
    rule: String,
    /// For actions, the action type (from the `send_goal` server), e.g.
    /// `nav2_msgs/action/NavigateToPose`.
    action_type: Option<String>,
}

/// [`Source`], ordered for deterministic stream lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SourceOrd {
    Message,
    Request,
    Goal,
    Feedback,
    Status,
}

impl SourceOrd {
    fn public(self) -> Source {
        match self {
            Self::Message => Source::Message,
            Self::Request => Source::Request,
            Self::Goal => Source::Goal,
            Self::Feedback => Source::Feedback,
            Self::Status => Source::Status,
        }
    }
}

impl Key {
    /// The topic or service this stream observes.
    fn observes(&self) -> String {
        match self.source {
            SourceOrd::Message | SourceOrd::Request => self.name.clone(),
            SourceOrd::Goal => format!("{}/_action/send_goal", self.name),
            SourceOrd::Feedback => format!("{}/_action/feedback", self.name),
            SourceOrd::Status => format!("{}/_action/status", self.name),
        }
    }
}

#[derive(Debug)]
struct Entry {
    rule: RecordRule,
    shared: Arc<StreamShared>,
    task: Option<AbortHandle>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

#[derive(Debug)]
struct StreamShared {
    state: Mutex<(StreamState, Option<Instant>)>,
    recorded: AtomicU64,
    dropped: AtomicU64,
}

impl StreamShared {
    fn set(&self, s: StreamState) {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        st.0 = s;
    }

    /// The stream's task ended: mark it for retry.
    fn ended(&self, error: String) {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        *st = (StreamState::Failed { error }, Some(Instant::now()));
    }

    fn ended_at(&self) -> Option<Instant> {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).1
    }

    fn view(&self) -> StreamState {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .0
            .clone()
    }
}

impl Recorder {
    /// A recorder over `hub` (and its bus) with `rules`, validated. Nothing
    /// runs until [`Recorder::spawn`] or [`Recorder::tick`].
    pub fn new(
        hub: Hub,
        cfg: RecorderConfig,
        sinks: Sinks,
        rules: Vec<RecordRule>,
    ) -> Result<Self, RuleError> {
        let rules = validate_rules(rules, sinks.database.is_some())?;
        Ok(Self {
            inner: Arc::new(Inner {
                hub,
                results: Arc::new(Semaphore::new(cfg.max_result_calls.max(1))),
                cfg,
                sinks,
                rules: RwLock::new(Arc::new(rules)),
                streams: Mutex::new(HashMap::new()),
                skipped: AtomicUsize::new(0),
                recent: Mutex::new(VecDeque::new()),
                shutdown: CancellationToken::new(),
            }),
        })
    }

    /// Replace the rules (validated, see [`validate_rules`]) and reconcile at
    /// once. Returns the rules as normalized.
    pub fn set_rules(&self, rules: Vec<RecordRule>) -> Result<Vec<RecordRule>, RuleError> {
        let rules = validate_rules(rules, self.inner.sinks.database.is_some())?;
        *self.inner.rules.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(rules.clone());
        self.tick();
        Ok(rules)
    }

    /// The rules in force.
    pub fn rules(&self) -> Arc<Vec<RecordRule>> {
        self.inner.rules()
    }

    /// Whether rules may use the `database` sink.
    pub fn database_available(&self) -> bool {
        self.inner.sinks.database.is_some()
    }

    /// Rules and streams.
    pub fn view(&self) -> RecordingView {
        let streams = self.inner.lock();
        let mut views: Vec<(&Key, StreamView)> = streams
            .iter()
            .map(|(k, e)| {
                (
                    k,
                    StreamView {
                        rule: e.rule.name.clone(),
                        kind: e.rule.kind,
                        name: k.name.clone(),
                        source: k.source.public(),
                        observes: k.observes(),
                        status: e.shared.view(),
                        recorded: e.shared.recorded.load(Ordering::Relaxed),
                        dropped: e.shared.dropped.load(Ordering::Relaxed),
                    },
                )
            })
            .collect();
        views.sort_by(|a, b| a.0.cmp(b.0));
        RecordingView {
            rules: self.rules().as_ref().clone(),
            streams: views.into_iter().map(|(_, v)| v).collect(),
            skipped: self.inner.skipped.load(Ordering::Relaxed),
            database: self.database_available(),
        }
    }

    /// The most recent records (every sink), newest first, at most `limit`.
    pub fn recent(&self, limit: usize) -> Vec<Arc<Record>> {
        let recent = self.inner.recent.lock().unwrap_or_else(|p| p.into_inner());
        recent.iter().rev().take(limit).cloned().collect()
    }

    /// Run [`Recorder::tick`] every `tick_interval` until `shutdown`, then
    /// stop every stream.
    pub fn spawn(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let rec = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(rec.inner.cfg.tick_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = interval.tick() => rec.tick(),
                }
            }
            rec.close_all();
        })
    }

    /// Stop every stream; later ticks start none.
    pub fn close_all(&self) {
        self.inner.shutdown.cancel();
        let drained: Vec<Entry> = self.inner.lock().drain().map(|(_, e)| e).collect();
        drop(drained);
        metrics::gauge!("rmf_pipes_recording_streams").set(0.0);
    }

    /// One reconciliation against the hub's latest graph.
    pub fn tick(&self) {
        if self.inner.shutdown.is_cancelled() {
            return;
        }
        // Rules are read under the streams lock, so a tick that read older
        // rules can't run after (and undo) the tick of a newer `set_rules`.
        let mut streams = self.inner.lock();
        let graph = self.inner.hub.graph();
        let rules = self.rules();
        let (wanted, skipped) = desired(&rules, &graph, self.inner.cfg.max_streams);
        let before = self.inner.skipped.swap(skipped, Ordering::Relaxed);
        if before != skipped && skipped > 0 {
            tracing::warn!(
                skipped,
                max_streams = self.inner.cfg.max_streams,
                "recording rules match more targets than the stream limit; some are not recorded"
            );
        }
        let now = Instant::now();
        let wanted_keys: HashSet<&Key> = wanted.iter().map(|(k, _)| k).collect();
        streams.retain(|k, _| wanted_keys.contains(k));
        for (key, rule) in &wanted {
            let retry = streams.get(key).is_some_and(|e| {
                e.shared
                    .ended_at()
                    .is_some_and(|at| now.duration_since(at) >= self.inner.cfg.retry_after)
            });
            if retry || !streams.contains_key(key) {
                let entry = self.inner.start(key.clone(), rule.clone());
                streams.insert(key.clone(), entry);
            }
        }
        metrics::gauge!("rmf_pipes_recording_streams").set(streams.len() as f64);
    }
}

/// The streams `rules` want on `graph`, in deterministic order, capped at
/// `max`; also how many were skipped by the cap.
fn desired(
    rules: &[RecordRule],
    graph: &GraphSnapshot,
    max: usize,
) -> (Vec<(Key, RecordRule)>, usize) {
    // Names that aren't valid ROS names (a participant can advertise
    // anything) are never watched or tapped: they could carry key-expression
    // wildcards.
    let internal =
        |name: &str| name.contains("/_action/") || crate::hub::validate_topic(name).is_err();
    let topics: BTreeSet<String> = graph
        .topics()
        .into_iter()
        .filter(|t| t.publishers > 0 && !internal(&t.name))
        .map(|t| t.name)
        .collect();
    let services = graph.services();
    let plain_services: BTreeSet<&str> = services
        .iter()
        .filter(|s| s.servers > 0 && !internal(&s.name))
        .map(|s| s.name.as_str())
        .collect();
    // Action name -> action type, from the `send_goal` servers.
    let mut actions: std::collections::BTreeMap<&str, String> = Default::default();
    for s in services.iter().filter(|s| s.servers > 0) {
        if let Some(action) = s.name.strip_suffix("/_action/send_goal")
            && crate::hub::validate_topic(&s.name).is_ok()
        {
            let ty = s
                .type_name
                .strip_suffix("_SendGoal")
                .unwrap_or(&s.type_name)
                .to_owned();
            actions.entry(action).or_insert(ty);
        }
    }
    let mut out = Vec::new();
    for rule in rules {
        let fingerprint = serde_json::to_string(rule).unwrap_or_default();
        let key = |name: &str, source, action_type: Option<&String>| Key {
            rule_name: rule.name.clone(),
            name: name.to_owned(),
            source,
            rule: fingerprint.clone(),
            action_type: action_type.cloned(),
        };
        match rule.kind {
            TargetKind::Topic => out.extend(
                topics
                    .iter()
                    .filter(|t| rule.pattern.matches(t))
                    .map(|t| (key(t, SourceOrd::Message, None), rule.clone())),
            ),
            TargetKind::Service => out.extend(
                plain_services
                    .iter()
                    .filter(|s| rule.pattern.matches(s))
                    .map(|s| (key(s, SourceOrd::Request, None), rule.clone())),
            ),
            TargetKind::Action => {
                for (name, ty) in actions.iter().filter(|(a, _)| rule.pattern.matches(a)) {
                    let mut add = |src| out.push((key(name, src, Some(ty)), rule.clone()));
                    if rule.records(Part::Goal) {
                        add(SourceOrd::Goal);
                    }
                    if rule.records(Part::Feedback) {
                        add(SourceOrd::Feedback);
                    }
                    if rule.records(Part::Status) || rule.records(Part::Result) {
                        add(SourceOrd::Status);
                    }
                }
            }
        }
    }
    let skipped = out.len().saturating_sub(max);
    out.truncate(max);
    (out, skipped)
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Key, Entry>> {
        self.streams.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn rules(&self) -> Arc<Vec<RecordRule>> {
        self.rules.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn start(self: &Arc<Self>, key: Key, rule: RecordRule) -> Entry {
        let shared = Arc::new(StreamShared {
            state: Mutex::new((StreamState::Opening, None)),
            recorded: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        });
        let stream = Stream {
            inner: Arc::downgrade(self),
            hub: self.hub.clone(),
            key: key.clone(),
            rule: rule.clone(),
            shared: shared.clone(),
            throttle: Throttle::new(rule.min_interval()),
            results: tokio::task::JoinSet::new(),
        };
        let task = tokio::spawn(async move {
            let shared = stream.shared.clone();
            let observes = stream.key.observes();
            let error = match stream.run().await {
                Ok(()) => format!("`{observes}` ended"),
                Err(e) => e,
            };
            tracing::debug!(observes, error, "recording stream stopped; retrying later");
            shared.ended(error);
        })
        .abort_handle();
        Entry {
            rule,
            shared,
            task: Some(task),
        }
    }

    fn emit(&self, rule: &RecordRule, shared: &StreamShared, record: Record) {
        let record = Arc::new(record);
        metrics::counter!(
            "rmf_pipes_records_total",
            "rule" => rule.name.clone(),
            "part" => record.part.as_str()
        )
        .increment(1);
        shared.recorded.fetch_add(1, Ordering::Relaxed);
        {
            let mut recent = self.recent.lock().unwrap_or_else(|p| p.into_inner());
            if recent.len() >= self.cfg.recent.max(1) {
                recent.pop_front();
            }
            recent.push_back(record.clone());
        }
        if rule.sends_to(Sink::Console) {
            self.sinks.console.accept(&record);
        }
        if rule.sends_to(Sink::Database)
            && let Some(db) = &self.sinks.database
        {
            db.accept(&record);
        }
    }
}

/// Leading-edge rate limit: admit one event per `min` interval.
#[derive(Debug)]
struct Throttle {
    min: Option<Duration>,
    last: Option<Instant>,
}

impl Throttle {
    fn new(min: Option<Duration>) -> Self {
        Self { min, last: None }
    }

    fn admit(&mut self) -> bool {
        let Some(min) = self.min else {
            return true;
        };
        let now = Instant::now();
        if self.last.is_some_and(|l| now.duration_since(l) < min) {
            return false;
        }
        self.last = Some(now);
        true
    }
}

/// A stream's task state.
struct Stream {
    inner: Weak<Inner>,
    hub: Hub,
    key: Key,
    rule: RecordRule,
    shared: Arc<StreamShared>,
    throttle: Throttle,
    /// Pending `get_result` fetches (status streams), at most
    /// [`MAX_PENDING_RESULTS`]; aborted with the stream.
    results: tokio::task::JoinSet<()>,
}

/// Most `get_result` fetches one status stream keeps waiting for a call slot;
/// beyond that, finished goals' results are dropped (counted as
/// `result_backlog`).
const MAX_PENDING_RESULTS: usize = 64;

impl Stream {
    async fn run(mut self) -> Result<(), String> {
        match self.key.source {
            SourceOrd::Request | SourceOrd::Goal => self.run_tap().await,
            SourceOrd::Message | SourceOrd::Feedback | SourceOrd::Status => self.run_watch().await,
        }
    }

    fn drop_event(&self, reason: &'static str) {
        self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "rmf_pipes_records_dropped_total",
            "rule" => self.rule.name.clone(),
            "reason" => reason
        )
        .increment(1);
    }

    fn emit(&self, part: Part, type_name: String, goal_id: Option<String>, data: Option<Value>) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        inner.emit(
            &self.rule,
            &self.shared,
            Record {
                at: OffsetDateTime::now_utc(),
                rule: self.rule.name.clone(),
                kind: self.rule.kind,
                name: self.key.name.clone(),
                part,
                type_name,
                goal_id,
                data,
            },
        );
    }

    fn action_type(&self, fallback: &str) -> String {
        self.key
            .action_type
            .clone()
            .unwrap_or_else(|| fallback.to_owned())
    }

    async fn run_tap(&mut self) -> Result<(), String> {
        let observes = self.key.observes();
        let mut tap = self
            .hub
            .bus()
            .tap(&observes)
            .await
            .map_err(|e| e.to_string())?;
        self.shared.set(StreamState::Active);
        while let Some(raw) = tap.samples.next().await {
            let goal = self.key.source == SourceOrd::Goal;
            if !goal && !self.throttle.admit() {
                self.drop_event("rate_limited");
                continue;
            }
            // The hub's payload cap applies to tapped requests too.
            if raw.size_bytes > self.hub.config().max_payload_bytes {
                let (part, ty) = if goal {
                    (Part::Goal, self.action_type(&tap.type_name))
                } else {
                    (Part::Request, tap.type_name.clone())
                };
                self.emit(part, ty, None, None);
                continue;
            }
            let data = match raw.decode() {
                Ok(d) => d,
                Err(e) => {
                    tracing::debug!(%e, observes, "cannot decode a tapped request");
                    self.drop_event("decode_error");
                    continue;
                }
            };
            if goal {
                let goal_id = data.get("goal_id").and_then(uuid_text);
                let ty = self.action_type(&tap.type_name);
                self.emit(Part::Goal, ty, goal_id, data.get("goal").cloned());
            } else {
                self.emit(Part::Request, tap.type_name.clone(), None, Some(data));
            }
        }
        Ok(())
    }

    async fn run_watch(&mut self) -> Result<(), String> {
        let observes = self.key.observes();
        let mut watch = self
            .hub
            .watch_pinned(&observes)
            .map_err(|e| e.to_string())?;
        self.mirror(&watch.initial_status);
        let mut goals = GoalTracker::default();
        if self.key.source == SourceOrd::Status
            && let Some(s) = &watch.initial_latest
        {
            self.on_status(&mut goals, s);
        }
        loop {
            match watch.events.recv().await {
                Ok(TopicEvent::Sample(s)) => {
                    self.shared.set(StreamState::Active);
                    match self.key.source {
                        SourceOrd::Status => self.on_status(&mut goals, &s),
                        SourceOrd::Feedback => self.on_feedback(&s),
                        _ => self.on_message(&s),
                    }
                }
                Ok(TopicEvent::Status(TopicStatus::Closed)) | Err(RecvError::Closed) => {
                    return Err(format!("`{observes}` was closed"));
                }
                Ok(TopicEvent::Status(st)) => self.mirror(&st),
                Err(RecvError::Lagged(n)) => {
                    for _ in 0..n {
                        self.drop_event("lagged");
                    }
                }
            }
        }
    }

    fn mirror(&self, st: &TopicStatus) {
        self.shared.set(match st {
            TopicStatus::Opening => StreamState::Opening,
            TopicStatus::Active => StreamState::Active,
            TopicStatus::Failed { error } => StreamState::Failed {
                error: error.clone(),
            },
            TopicStatus::Closed => StreamState::Failed {
                error: "closed".into(),
            },
        });
    }

    fn on_message(&mut self, s: &Sample) {
        if !self.throttle.admit() {
            self.drop_event("rate_limited");
            return;
        }
        self.emit(Part::Message, s.type_name.clone(), None, s.data.clone());
    }

    fn on_feedback(&mut self, s: &Sample) {
        if !self.throttle.admit() {
            self.drop_event("rate_limited");
            return;
        }
        let goal_id = s
            .data
            .as_ref()
            .and_then(|d| d.get("goal_id"))
            .and_then(uuid_text);
        let feedback = s.data.as_ref().and_then(|d| d.get("feedback").cloned());
        let ty = self.action_type(&s.type_name);
        self.emit(Part::Feedback, ty, goal_id, feedback);
    }

    fn on_status(&mut self, goals: &mut GoalTracker, s: &Sample) {
        let Some(data) = &s.data else { return };
        for change in goals.update(data) {
            let ty = self.action_type(&s.type_name);
            if self.rule.records(Part::Status) {
                self.emit(
                    Part::Status,
                    ty.clone(),
                    Some(change.id.clone()),
                    Some(json!({"status": status_name(change.code), "code": change.code})),
                );
            }
            if change.terminal() && self.rule.records(Part::Result) {
                self.fetch_result(change, ty);
            }
        }
    }

    /// Ask the server for a finished goal's result (it answers at once) and
    /// record it. Runs in a task owned by this stream (so it ends with it),
    /// waiting for one of `max_result_calls` slots; at most
    /// [`MAX_PENDING_RESULTS`] wait per stream.
    fn fetch_result(&mut self, change: Change, ty: String) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        while self.results.try_join_next().is_some() {}
        if self.results.len() >= MAX_PENDING_RESULTS {
            tracing::warn!(goal_id = %change.id, action = %self.key.name, "too many results waiting to be fetched; dropping one");
            self.drop_event("result_backlog");
            return;
        }
        let bus = self.hub.bus().clone();
        let service = format!("{}/_action/get_result", self.key.name);
        let timeout = inner.cfg.result_timeout;
        let permits = inner.results.clone();
        let this = Stream {
            inner: self.inner.clone(),
            hub: self.hub.clone(),
            key: self.key.clone(),
            rule: self.rule.clone(),
            shared: self.shared.clone(),
            throttle: Throttle::new(None),
            results: tokio::task::JoinSet::new(),
        };
        self.results.spawn(async move {
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };
            match bus
                .call(&service, &json!({"goal_id": change.goal_id}), timeout)
                .await
            {
                Ok(called) => {
                    let code = called.response.get("status").and_then(Value::as_i64);
                    let data = json!({
                        "status": code.map(status_name),
                        "result": called.response.get("result"),
                    });
                    this.emit(Part::Result, ty, Some(change.id), Some(data));
                }
                Err(e) => {
                    tracing::warn!(%e, service, goal_id = %change.id, "cannot fetch an action result");
                    this.drop_event("result_unavailable");
                }
            }
        });
    }
}

/// `action_msgs/msg/GoalStatus` codes.
fn status_name(code: i64) -> &'static str {
    match code {
        1 => "accepted",
        2 => "executing",
        3 => "canceling",
        4 => "succeeded",
        5 => "canceled",
        6 => "aborted",
        _ => "unknown",
    }
}

/// A goal's status changed.
#[derive(Debug, Clone, PartialEq)]
struct Change {
    /// UUID text.
    id: String,
    /// The goal id as JSON (`{"uuid": "<base64>"}`), for `get_result`.
    goal_id: Value,
    code: i64,
}

impl Change {
    fn terminal(&self) -> bool {
        matches!(self.code, 4..=6)
    }
}

/// Most goals tracked per action (servers list only current goals, so this
/// is generous).
const MAX_TRACKED_GOALS: usize = 4096;

/// Last seen status per goal, to turn `GoalStatusArray` snapshots into
/// changes.
#[derive(Debug, Default)]
struct GoalTracker {
    known: HashMap<String, i64>,
    seen_any: bool,
}

impl GoalTracker {
    /// Changes in `snapshot` (a `GoalStatusArray` as JSON) since the last one.
    /// In the first snapshot, goals that already finished are a baseline, not
    /// changes. Goals no longer listed are forgotten.
    fn update(&mut self, snapshot: &Value) -> Vec<Change> {
        let first = !self.seen_any;
        self.seen_any = true;
        let mut listed = HashSet::new();
        let mut changes = Vec::new();
        let list = snapshot
            .get("status_list")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice);
        for st in list {
            let Some(goal_id) = st.get("goal_info").and_then(|g| g.get("goal_id")) else {
                continue;
            };
            let (Some(id), Some(code)) =
                (uuid_text(goal_id), st.get("status").and_then(Value::as_i64))
            else {
                continue;
            };
            listed.insert(id.clone());
            if self.known.len() >= MAX_TRACKED_GOALS && !self.known.contains_key(&id) {
                continue;
            }
            let prev = self.known.insert(id.clone(), code);
            let change = Change {
                id,
                goal_id: goal_id.clone(),
                code,
            };
            if prev != Some(code) && !(first && prev.is_none() && change.terminal()) {
                changes.push(change);
            }
        }
        self.known.retain(|k, _| listed.contains(k));
        changes
    }
}

#[cfg(test)]
#[path = "recorder_tests.rs"]
mod tests;
