//! The topic hub: owns every subscription, keeps the latest sample and stats per
//! topic, and fans samples out to watchers (SSE clients, the dashboard).
//!
//! Lifecycle of a topic entry:
//!
//! - **Opened** on demand by [`Hub::watch`], or by [`Hub::tick`] when a topic
//!   with publishers matches a standing pattern.
//! - **Kept** while it has watchers or is standing.
//! - **Reaped** by `tick` once it has had no watchers and is not standing for
//!   `idle_timeout`.
//! - **Retried** by `tick` if opening failed and someone is still interested,
//!   after `retry_after`, doubling on each consecutive failure up to
//!   `max_retry_after`; uninteresting failed entries are reaped.
//! - **Resubscribed** by `tick` when its publishers now advertise a different
//!   type hash than the one it subscribed with (a restarted node with a new
//!   message definition).
//!
//! - **Closed** when the entry is dropped (reaped, or [`Hub::close_all`]):
//!   watchers get a final `Closed` status and then `RecvError::Closed`.
//!
//! Admission, per sample, before decoding: payloads over `max_payload_bytes`
//! are delivered with `data: null` and `omitted: too_large`. `max_rate_hz` is a
//! trailing-edge throttle: within an interval only the newest sample is kept
//! and delivered when the interval ends, so `latest` is never left stale by a
//! burst; the samples it replaced are counted as `dropped_rate_limited`.
//!
//! Every subscription task carries a generation number; a task whose entry has
//! since been restarted (resubscribe, retry) cannot write state or events.
//!
//! All waiting uses `tokio::time`, so tests drive the hub with paused time.
//!
//! Metrics (via the `metrics` facade; the binary exports them as Prometheus):
//!
//! | Name | Kind | Labels |
//! |---|---|---|
//! | `rmf_pipes_samples_received_total` | counter | `topic` |
//! | `rmf_pipes_samples_delivered_total` | counter | `topic` |
//! | `rmf_pipes_samples_dropped_total` | counter | `topic`, `reason` = `rate_limited` \| `decode_error` |
//! | `rmf_pipes_samples_omitted_total` | counter | `topic`, `reason` = `too_large` |
//! | `rmf_pipes_topics` | gauge | `state` = `opening` \| `active` \| `failed` |
//! | `rmf_pipes_watchers` | gauge | |
//! | `rmf_pipes_graph_nodes`, `rmf_pipes_graph_topics` | gauge | |
//! | `rmf_pipes_type_mismatches` | gauge | number of topics with more than one type hash |

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, Weak},
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use serde::Serialize;
use time::OffsetDateTime;
use tokio::{
    sync::broadcast,
    task::AbortHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    bus::{Bus, RawSample, Subscription},
    graph::GraphSnapshot,
    pattern::{TopicPattern, any_match},
    sample::{OmittedReason, Sample},
    schema::Schema,
};

/// Hub tuning. Defaults suit a dashboard watching a typical RMF site.
#[derive(Debug, Clone)]
pub struct HubConfig {
    /// Topics matching these get standing subscriptions while they have publishers.
    pub patterns: Vec<TopicPattern>,
    /// How often [`Hub::spawn`] calls [`Hub::tick`].
    pub tick_interval: Duration,
    /// How long an unwatched, non-standing topic stays subscribed.
    pub idle_timeout: Duration,
    /// How long to wait before retrying a topic that failed to open. Doubles
    /// with each consecutive failure.
    pub retry_after: Duration,
    /// Upper bound for the retry backoff.
    pub max_retry_after: Duration,
    /// Per-topic admission cap. `None` disables rate limiting.
    pub max_rate_hz: Option<f64>,
    /// Payloads larger than this are delivered without `data`.
    pub max_payload_bytes: usize,
    /// Per-topic event buffer for each watcher; a watcher that falls further
    /// behind than this gets a lag notice instead of the missed events.
    pub watcher_buffer: usize,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            patterns: Vec::new(),
            tick_interval: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(30),
            retry_after: Duration::from_secs(5),
            max_retry_after: Duration::from_secs(60),
            max_rate_hz: Some(20.0),
            max_payload_bytes: 1024 * 1024,
            watcher_buffer: 64,
        }
    }
}

impl HubConfig {
    /// The admission interval for `max_rate_hz`, or `None` when unlimited.
    /// Rates that don't give a representable interval (zero, negative, NaN,
    /// absurdly small) also mean unlimited; the binary rejects them up front.
    pub fn min_interval(&self) -> Option<Duration> {
        let hz = self.max_rate_hz?;
        if !(hz.is_finite() && hz > 0.0) {
            return None;
        }
        Duration::try_from_secs_f64(1.0 / hz).ok()
    }

    /// Retry delay after `failures` consecutive failures (1-based).
    fn retry_delay(&self, failures: u32) -> Duration {
        let factor = 1u32 << failures.saturating_sub(1).min(16);
        self.retry_after
            .saturating_mul(factor)
            .min(self.max_retry_after.max(self.retry_after))
    }
}

/// Where a topic's subscription is in its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TopicStatus {
    /// Discovering the schema and subscribing.
    Opening,
    /// Subscribed; samples flow.
    Active,
    /// Opening failed or the subscription ended; retried after `retry_after`
    /// while someone is interested.
    Failed {
        /// Human-readable and safe to show to clients.
        error: String,
    },
    /// The entry was closed (idle, or shutting down). Final event.
    Closed,
}

/// What a topic's subscription is bound to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionInfo {
    /// ROS type name.
    pub type_name: String,
    /// RIHS01 hash the subscription is bound to.
    pub type_hash: String,
}

/// Counters for one topic since its entry was created.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TopicStats {
    /// Samples received from the bus.
    pub received: u64,
    /// Samples delivered to watchers and stored as latest (including ones with
    /// `data` omitted for size).
    pub delivered: u64,
    /// Samples dropped by the rate cap.
    pub dropped_rate_limited: u64,
    /// Delivered samples whose `data` was omitted for size.
    pub omitted_too_large: u64,
    /// Samples that failed to decode.
    pub decode_errors: u64,
    /// The most recent decode error message.
    pub last_decode_error: Option<String>,
    /// When the last sample arrived.
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_received_at: Option<OffsetDateTime>,
}

/// An event on a watched topic.
#[derive(Debug, Clone)]
pub enum TopicEvent {
    /// A new sample.
    Sample(Arc<Sample>),
    /// The topic's status changed.
    Status(TopicStatus),
}

/// A read-only view of one topic entry.
#[derive(Debug, Clone, Serialize)]
pub struct TopicView {
    /// Topic name.
    pub name: String,
    /// Lifecycle state.
    pub status: TopicStatus,
    /// What the subscription is bound to, once active.
    pub subscription: Option<SubscriptionInfo>,
    /// Counters since the entry was created.
    pub stats: TopicStats,
    /// Subscribed because it matches a standing pattern.
    pub standing: bool,
    /// Number of active watchers.
    pub watchers: usize,
}

/// Why a hub call was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HubError {
    /// The topic name is not a valid fully-qualified ROS name.
    #[error(
        "invalid topic name `{0}`: must be fully qualified (start with `/`), use only letters, digits, `_` and `/`, with no empty segments"
    )]
    InvalidTopic(String),
    /// The hub is closing; no new watches.
    #[error("rmf-pipes is shutting down")]
    ShuttingDown,
}

/// Cheap-to-clone handle to the hub.
#[derive(Debug, Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    bus: Arc<dyn Bus>,
    cfg: HubConfig,
    topics: Mutex<HashMap<String, Entry>>,
    graph: RwLock<Arc<GraphSnapshot>>,
    /// Topics with type mismatches at the last tick, to warn only on change.
    mismatches: Mutex<Vec<String>>,
    shutdown: CancellationToken,
}

#[derive(Debug)]
struct Entry {
    shared: Arc<Shared>,
    task: Option<AbortHandle>,
    standing: bool,
    watchers: usize,
    /// Set when the entry stops being wanted (no watchers, not standing).
    idle_since: Option<Instant>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.shared.close();
    }
}

/// State shared between an entry, its subscription task and its watchers.
///
/// The broadcast sender lives inside the state (not alongside it) so that
/// [`Shared::close`] can drop it: watchers hold `Arc<Shared>`, and a sender
/// they kept alive would mean their receivers never see `Closed`.
#[derive(Debug)]
struct Shared {
    topic: String,
    state: Mutex<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    tx: Option<broadcast::Sender<TopicEvent>>,
    /// Bumped by every (re)start; tasks with an older number are stale.
    generation: u64,
    status: TopicStatus,
    subscription: Option<SubscriptionInfo>,
    schema: Option<Schema>,
    latest: Option<Arc<Sample>>,
    stats: TopicStats,
    failed_at: Option<Instant>,
    /// Consecutive failures, for the retry backoff.
    failures: u32,
}

impl Shared {
    fn new(topic: &str, buffer: usize) -> Arc<Self> {
        let (tx, _) = broadcast::channel(buffer.max(1));
        Arc::new(Self {
            topic: topic.to_owned(),
            state: Mutex::new(SharedState {
                tx: Some(tx),
                generation: 0,
                status: TopicStatus::Opening,
                subscription: None,
                schema: None,
                latest: None,
                stats: TopicStats::default(),
                failed_at: None,
                failures: 0,
            }),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SharedState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Subscribe to events, unless the entry is closed.
    fn subscribe(&self) -> Option<broadcast::Receiver<TopicEvent>> {
        self.lock().tx.as_ref().map(broadcast::Sender::subscribe)
    }

    /// Begin a new generation in `Opening`; returns its number.
    fn restart(&self) -> u64 {
        let mut st = self.lock();
        st.generation += 1;
        st.subscription = None;
        st.schema = None;
        st.failed_at = None;
        st.status = TopicStatus::Opening;
        if let Some(tx) = &st.tx {
            let _ = tx.send(TopicEvent::Status(TopicStatus::Opening));
        }
        st.generation
    }

    /// Apply `f` and broadcast `event` if `generation` is still current.
    /// Returns false for a stale task, which must stop.
    fn update(
        &self,
        generation: u64,
        f: impl FnOnce(&mut SharedState) -> Option<TopicEvent>,
    ) -> bool {
        let mut st = self.lock();
        if st.generation != generation {
            return false;
        }
        if let Some(event) = f(&mut st)
            && let Some(tx) = &st.tx
        {
            // No receivers is fine.
            let _ = tx.send(event);
        }
        true
    }

    fn fail(&self, generation: u64, error: String) {
        self.update(generation, |st| {
            st.failed_at = Some(Instant::now());
            st.failures = st.failures.saturating_add(1);
            st.status = TopicStatus::Failed { error };
            Some(TopicEvent::Status(st.status.clone()))
        });
    }

    /// Send the final `Closed` status and drop the sender, so receivers end.
    fn close(&self) {
        let mut st = self.lock();
        st.generation += 1;
        st.status = TopicStatus::Closed;
        if let Some(tx) = st.tx.take() {
            let _ = tx.send(TopicEvent::Status(TopicStatus::Closed));
        }
    }
}

/// A live interest in one topic. Dropping it releases the interest.
#[derive(Debug)]
pub struct Watch {
    /// Status changes and samples, in order. Lagging receivers get
    /// `RecvError::Lagged` and skip ahead.
    pub events: broadcast::Receiver<TopicEvent>,
    shared: Arc<Shared>,
    _guard: WatchGuard,
}

impl Watch {
    /// The topic's latest sample, if any has arrived.
    pub fn latest(&self) -> Option<Arc<Sample>> {
        self.shared.lock().latest.clone()
    }

    /// The topic's current status.
    pub fn status(&self) -> TopicStatus {
        self.shared.lock().status.clone()
    }

    /// The watched topic's name.
    pub fn topic(&self) -> &str {
        &self.shared.topic
    }
}

#[derive(Debug)]
struct WatchGuard {
    inner: Weak<Inner>,
    topic: String,
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.release(&self.topic);
        }
    }
}

impl Hub {
    /// A hub over `bus`. Nothing runs until [`Hub::spawn`] or [`Hub::tick`].
    pub fn new(bus: Arc<dyn Bus>, cfg: HubConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                bus,
                cfg,
                topics: Mutex::new(HashMap::new()),
                graph: RwLock::new(Arc::new(GraphSnapshot::default())),
                mismatches: Mutex::new(Vec::new()),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    /// The configuration the hub was created with.
    pub fn config(&self) -> &HubConfig {
        &self.inner.cfg
    }

    /// Run [`Hub::tick`] every `tick_interval` until `shutdown` is cancelled,
    /// then close every subscription.
    pub fn spawn(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let hub = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(hub.inner.cfg.tick_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = interval.tick() => hub.tick(),
                }
            }
            hub.close_all();
        })
    }

    /// Stop accepting watches and drop every subscription.
    pub fn close_all(&self) {
        self.inner.shutdown.cancel();
        let drained: Vec<Entry> = self.inner.lock().drain().map(|(_, e)| e).collect();
        drop(drained);
    }

    /// One reconciliation step: refresh the graph, then open standing topics,
    /// resubscribe on hash changes, retry failures and reap idle entries.
    pub fn tick(&self) {
        let graph = Arc::new(self.inner.bus.graph().normalized());
        *self.inner.graph.write().unwrap_or_else(|p| p.into_inner()) = graph.clone();
        self.inner.reconcile(&graph, Instant::now());
        self.inner.report(&graph);
    }

    /// The graph as of the last tick.
    pub fn graph(&self) -> Arc<GraphSnapshot> {
        self.inner
            .graph
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Register interest in `topic`, subscribing if needed.
    pub fn watch(&self, topic: &str) -> Result<Watch, HubError> {
        validate_topic(topic)?;
        let mut topics = self.inner.lock();
        // Checked under the lock: `close_all` cancels before it drains, so a
        // watch either sees the cancellation or is drained with the rest.
        if self.inner.shutdown.is_cancelled() {
            return Err(HubError::ShuttingDown);
        }
        let entry = topics
            .entry(topic.to_owned())
            .or_insert_with(|| self.inner.new_entry(topic, false));
        entry.watchers += 1;
        entry.idle_since = None;
        let shared = entry.shared.clone();
        let events = shared.subscribe().ok_or(HubError::ShuttingDown)?;
        Ok(Watch {
            events,
            shared,
            _guard: WatchGuard {
                inner: Arc::downgrade(&self.inner),
                topic: topic.to_owned(),
            },
        })
    }

    /// The latest sample of `topic`, if it is subscribed and has received one.
    pub fn latest(&self, topic: &str) -> Option<Arc<Sample>> {
        let topics = self.inner.lock();
        topics
            .get(topic)
            .and_then(|e| e.shared.lock().latest.clone())
    }

    /// The schema of `topic`, once its subscription is active.
    pub fn schema(&self, topic: &str) -> Option<Schema> {
        let topics = self.inner.lock();
        topics
            .get(topic)
            .and_then(|e| e.shared.lock().schema.clone())
    }

    /// A view of `topic`'s entry, if it has one.
    pub fn topic(&self, topic: &str) -> Option<TopicView> {
        let topics = self.inner.lock();
        topics.get(topic).map(view)
    }

    /// Every subscribed (or opening, or failed) topic, sorted by name.
    pub fn topics(&self) -> Vec<TopicView> {
        let topics = self.inner.lock();
        let mut views: Vec<TopicView> = topics.values().map(view).collect();
        views.sort_by(|a, b| a.name.cmp(&b.name));
        views
    }
}

fn view(e: &Entry) -> TopicView {
    let st = e.shared.lock();
    TopicView {
        name: e.shared.topic.clone(),
        status: st.status.clone(),
        subscription: st.subscription.clone(),
        stats: st.stats.clone(),
        standing: e.standing,
        watchers: e.watchers,
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.topics.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn new_entry(self: &Arc<Self>, topic: &str, standing: bool) -> Entry {
        let shared = Shared::new(topic, self.cfg.watcher_buffer);
        let task = self.start(&shared);
        Entry {
            shared,
            task: Some(task),
            standing,
            watchers: 0,
            idle_since: None,
        }
    }

    fn start(self: &Arc<Self>, shared: &Arc<Shared>) -> AbortHandle {
        let generation = shared.restart();
        let task = tokio::spawn(run_topic(
            self.bus.clone(),
            shared.clone(),
            self.cfg.clone(),
            generation,
        ));
        task.abort_handle()
    }

    /// Warn when the set of mismatched topics changes, and update gauges.
    fn report(&self, graph: &GraphSnapshot) {
        let summaries = graph.topics();
        let mismatched: Vec<String> = summaries
            .iter()
            .filter(|t| t.mismatch)
            .map(|t| t.name.clone())
            .collect();
        {
            let mut last = self.mismatches.lock().unwrap_or_else(|p| p.into_inner());
            if *last != mismatched {
                for t in summaries
                    .iter()
                    .filter(|t| t.mismatch && !last.contains(&t.name))
                {
                    let types: Vec<String> = t
                        .types
                        .iter()
                        .map(|ty| format!("{} ({})", ty.type_name, ty.type_hash))
                        .collect();
                    tracing::warn!(
                        topic = %t.name,
                        types = ?types,
                        "type mismatch: endpoints on this topic use different message definitions and will not see each other's data"
                    );
                }
                *last = mismatched.clone();
            }
        }
        metrics::gauge!("rmf_pipes_type_mismatches").set(mismatched.len() as f64);
        metrics::gauge!("rmf_pipes_graph_nodes").set(graph.nodes.len() as f64);
        metrics::gauge!("rmf_pipes_graph_topics").set(summaries.len() as f64);

        let topics = self.lock();
        let (mut opening, mut active, mut failed, mut watchers) = (0.0, 0.0, 0.0, 0.0);
        for e in topics.values() {
            watchers += e.watchers as f64;
            match e.shared.lock().status {
                TopicStatus::Opening => opening += 1.0,
                TopicStatus::Active => active += 1.0,
                TopicStatus::Failed { .. } => failed += 1.0,
                TopicStatus::Closed => {}
            }
        }
        metrics::gauge!("rmf_pipes_topics", "state" => "opening").set(opening);
        metrics::gauge!("rmf_pipes_topics", "state" => "active").set(active);
        metrics::gauge!("rmf_pipes_topics", "state" => "failed").set(failed);
        metrics::gauge!("rmf_pipes_watchers").set(watchers);
    }

    fn release(&self, topic: &str) {
        let mut topics = self.lock();
        if let Some(e) = topics.get_mut(topic) {
            e.watchers = e.watchers.saturating_sub(1);
            if e.watchers == 0 && !e.standing {
                e.idle_since = Some(Instant::now());
            }
        }
    }

    fn reconcile(self: &Arc<Self>, graph: &GraphSnapshot, now: Instant) {
        if self.shutdown.is_cancelled() {
            return;
        }
        let summaries = graph.topics();
        let mut topics = self.lock();

        // Standing patterns: topics with at least one publisher.
        for t in &summaries {
            if t.publishers() > 0 && any_match(&self.cfg.patterns, &t.name) {
                let entry = topics
                    .entry(t.name.clone())
                    .or_insert_with(|| self.new_entry(&t.name, true));
                entry.standing = true;
                entry.idle_since = None;
            }
        }

        let mut reap = Vec::new();
        for (name, e) in topics.iter_mut() {
            let publisher_hashes: Vec<&str> = graph
                .publishers_of(name)
                .map(|p| p.type_hash.as_str())
                .collect();

            // No longer standing: its publishers went away or it never matched.
            if e.standing && (publisher_hashes.is_empty() || !any_match(&self.cfg.patterns, name)) {
                e.standing = false;
                if e.watchers == 0 {
                    e.idle_since = Some(now);
                }
            }

            let (failed_at, failures, subscribed_hash) = {
                let st = e.shared.lock();
                (
                    st.failed_at,
                    st.failures,
                    st.subscription.as_ref().map(|s| s.type_hash.clone()),
                )
            };
            let wanted = e.watchers > 0 || e.standing;

            if let Some(failed_at) = failed_at {
                if now.duration_since(failed_at) >= self.cfg.retry_delay(failures) {
                    if wanted {
                        e.task = Some(self.start(&e.shared));
                    } else {
                        reap.push(name.clone());
                    }
                }
                continue;
            }

            // Resubscribe when every publisher now advertises a different hash.
            if let Some(hash) = subscribed_hash
                && !publisher_hashes.is_empty()
                && !publisher_hashes.contains(&hash.as_str())
            {
                tracing::info!(topic = %name, old_hash = %hash, "publishers changed type hash; resubscribing");
                if let Some(old) = e.task.take() {
                    old.abort();
                }
                e.task = Some(self.start(&e.shared));
            }

            if let Some(since) = e.idle_since
                && !wanted
                && now.duration_since(since) >= self.cfg.idle_timeout
            {
                reap.push(name.clone());
            }
        }
        for name in reap {
            tracing::debug!(topic = %name, "closing idle topic");
            topics.remove(&name);
        }
    }
}

async fn run_topic(bus: Arc<dyn Bus>, shared: Arc<Shared>, cfg: HubConfig, generation: u64) {
    let sub = match bus.subscribe(&shared.topic).await {
        Ok(sub) => sub,
        Err(err) => {
            tracing::warn!(topic = %shared.topic, %err, "cannot open topic");
            shared.fail(generation, err.to_string());
            return;
        }
    };
    let Subscription {
        type_name,
        type_hash,
        schema,
        mut samples,
    } = sub;
    let activated = shared.update(generation, |st| {
        st.subscription = Some(SubscriptionInfo {
            type_name: type_name.clone(),
            type_hash: type_hash.clone(),
        });
        st.schema = Some(schema);
        st.failures = 0;
        st.status = TopicStatus::Active;
        Some(TopicEvent::Status(TopicStatus::Active))
    });
    if !activated {
        return;
    }
    tracing::info!(topic = %shared.topic, %type_name, %type_hash, "subscribed");

    let mut out = Deliver {
        shared: &shared,
        generation,
        max_payload_bytes: cfg.max_payload_bytes,
        type_name,
        type_hash,
        metrics: TopicMetrics::new(&shared.topic),
    };
    let min_interval = cfg.min_interval();
    let mut last_delivered: Option<Instant> = None;
    // The newest sample held back by the rate cap, if any.
    let mut pending: Option<RawSample> = None;

    loop {
        let flush_at = match (&pending, last_delivered, min_interval) {
            (Some(_), Some(last), Some(min)) => Some(last + min),
            _ => None,
        };
        let next = tokio::select! {
            biased;
            raw = samples.next() => raw.map(Some),
            () = sleep_until_opt(flush_at) => Some(None),
        };
        match next {
            // Stream ended: deliver what we held back, then stop.
            None => {
                if let Some(raw) = pending.take() {
                    out.deliver(raw);
                }
                break;
            }
            // Flush timer: the interval is over, deliver the held-back sample.
            Some(None) => {
                if let Some(raw) = pending.take() {
                    if !out.deliver(raw) {
                        return;
                    }
                    last_delivered = Some(Instant::now());
                }
            }
            Some(Some(raw)) => {
                let now = Instant::now();
                if !out.received(&raw) {
                    return;
                }
                if pending.replace(raw).is_some() && !out.rate_limited() {
                    return;
                }
                let allowed = match (min_interval, last_delivered) {
                    (Some(min), Some(last)) => now.duration_since(last) >= min,
                    _ => true,
                };
                if allowed && let Some(raw) = pending.take() {
                    if !out.deliver(raw) {
                        return;
                    }
                    last_delivered = Some(now);
                }
            }
        }
    }
    shared.fail(generation, "subscription ended".into());
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

struct TopicMetrics {
    received: metrics::Counter,
    delivered: metrics::Counter,
    rate_limited: metrics::Counter,
    decode_errors: metrics::Counter,
    too_large: metrics::Counter,
}

impl TopicMetrics {
    fn new(topic: &str) -> Self {
        let t = topic.to_owned();
        Self {
            received: metrics::counter!("rmf_pipes_samples_received_total", "topic" => t.clone()),
            delivered: metrics::counter!("rmf_pipes_samples_delivered_total", "topic" => t.clone()),
            rate_limited: metrics::counter!("rmf_pipes_samples_dropped_total", "topic" => t.clone(), "reason" => "rate_limited"),
            decode_errors: metrics::counter!("rmf_pipes_samples_dropped_total", "topic" => t.clone(), "reason" => "decode_error"),
            too_large: metrics::counter!("rmf_pipes_samples_omitted_total", "topic" => t, "reason" => "too_large"),
        }
    }
}

/// Writes a topic's samples, stats and events for one generation. Every method
/// returns false once the generation is stale.
struct Deliver<'a> {
    shared: &'a Shared,
    generation: u64,
    max_payload_bytes: usize,
    type_name: String,
    type_hash: String,
    metrics: TopicMetrics,
}

impl Deliver<'_> {
    fn received(&self, raw: &RawSample) -> bool {
        self.metrics.received.increment(1);
        let at = to_offset(raw.received_at);
        self.shared.update(self.generation, |st| {
            st.stats.received += 1;
            st.stats.last_received_at = Some(at);
            None
        })
    }

    fn rate_limited(&self) -> bool {
        self.metrics.rate_limited.increment(1);
        self.shared.update(self.generation, |st| {
            st.stats.dropped_rate_limited += 1;
            None
        })
    }

    fn deliver(&mut self, raw: RawSample) -> bool {
        let size_bytes = raw.size_bytes;
        let received_at = to_offset(raw.received_at);
        let source_timestamp = raw.source_timestamp.map(to_offset);
        let sequence = raw.sequence;
        let (data, omitted) = if size_bytes > self.max_payload_bytes {
            (None, Some(OmittedReason::TooLarge))
        } else {
            match raw.decode() {
                Ok(v) => (Some(v), None),
                Err(err) => {
                    self.metrics.decode_errors.increment(1);
                    return self.shared.update(self.generation, |st| {
                        st.stats.decode_errors += 1;
                        st.stats.last_decode_error = Some(err.to_string());
                        None
                    });
                }
            }
        };
        let sample = Arc::new(Sample {
            topic: self.shared.topic.clone(),
            type_name: self.type_name.clone(),
            type_hash: self.type_hash.clone(),
            received_at,
            source_timestamp,
            sequence,
            size_bytes,
            data,
            omitted,
        });
        self.metrics.delivered.increment(1);
        if omitted.is_some() {
            self.metrics.too_large.increment(1);
        }
        self.shared.update(self.generation, |st| {
            st.stats.delivered += 1;
            if omitted.is_some() {
                st.stats.omitted_too_large += 1;
            }
            st.latest = Some(sample.clone());
            Some(TopicEvent::Sample(sample))
        })
    }
}

fn to_offset(t: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(t)
}

/// Check that `topic` is a fully qualified ROS topic name: absolute, letters,
/// digits, `_` and `/` only, no empty segments, no trailing slash.
pub fn validate_topic(topic: &str) -> Result<(), HubError> {
    let ok = topic.len() > 1
        && topic.starts_with('/')
        && !topic.ends_with('/')
        && !topic[1..].split('/').any(str::is_empty)
        && topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '/');
    if ok {
        Ok(())
    } else {
        Err(HubError::InvalidTopic(topic.to_owned()))
    }
}

#[cfg(test)]
#[path = "hub_tests.rs"]
mod tests;
