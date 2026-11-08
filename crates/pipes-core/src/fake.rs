//! An in-memory [`Bus`] for tests of everything above the bus (hub, API).
//! Enabled for this crate's tests and, for other crates, by the `fake` feature.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{
    bus::{Bus, BusError, DecodeError, RawSample, Subscription},
    graph::{Endpoint, EndpointKind, GraphSnapshot, Qos},
    schema::{Field, FieldType, Schema},
};

/// A scriptable bus. Topics are declared with [`FakeBus::add_publisher`];
/// samples are injected with [`FakeBus::publish`].
#[derive(Debug, Default)]
pub struct FakeBus {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    graph: GraphSnapshot,
    failures: HashMap<String, BusError>,
    subscribers: HashMap<String, Vec<mpsc::UnboundedSender<RawSample>>>,
    subscribe_calls: HashMap<String, usize>,
}

impl FakeBus {
    /// A fake bus with an empty graph.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Replace the whole graph.
    pub fn set_graph(&self, graph: GraphSnapshot) {
        self.lock().graph = graph;
    }

    /// Add a publisher of `topic` with the given type and hash to the graph.
    pub fn add_publisher(&self, topic: &str, type_name: &str, type_hash: &str) {
        self.lock().graph.endpoints.push(Endpoint {
            kind: EndpointKind::Publisher,
            name: topic.to_owned(),
            type_name: type_name.to_owned(),
            type_hash: type_hash.to_owned(),
            node: Some("/fake_node".to_owned()),
            qos: Qos {
                reliable: true,
                transient_local: false,
                depth: Some(10),
            },
        });
    }

    /// Remove every endpoint of `topic` from the graph.
    pub fn remove_topic(&self, topic: &str) {
        self.lock().graph.endpoints.retain(|e| e.name != topic);
    }

    /// Make subscribing to `topic` fail with `err` until [`FakeBus::clear_failure`].
    pub fn fail_subscribe(&self, topic: &str, err: BusError) {
        self.lock().failures.insert(topic.to_owned(), err);
    }

    /// Undo [`FakeBus::fail_subscribe`].
    pub fn clear_failure(&self, topic: &str) {
        self.lock().failures.remove(topic);
    }

    /// Deliver a sample that decodes to `data` to every live subscription of
    /// `topic`. Returns how many subscriptions received it.
    pub fn publish(&self, topic: &str, data: serde_json::Value) -> usize {
        let size = data.to_string().len();
        self.publish_raw(topic, size, move || Ok(data))
    }

    /// Deliver a sample with an explicit size and decoder.
    pub fn publish_raw(
        &self,
        topic: &str,
        size_bytes: usize,
        decode: impl FnOnce() -> Result<serde_json::Value, DecodeError> + Send + Clone + 'static,
    ) -> usize {
        let mut st = self.lock();
        let Some(senders) = st.subscribers.get_mut(topic) else {
            return 0;
        };
        senders.retain(|s| !s.is_closed());
        let mut delivered = 0;
        for s in senders.iter() {
            let raw = RawSample::new(SystemTime::now(), size_bytes, decode.clone())
                .with_attachment(Some(SystemTime::now()), Some(delivered as i64));
            if s.send(raw).is_ok() {
                delivered += 1;
            }
        }
        delivered
    }

    /// End every live subscription of `topic`, as if the bus dropped it.
    pub fn end_subscriptions(&self, topic: &str) {
        self.lock().subscribers.remove(topic);
    }

    /// Subscriptions to `topic` whose consumer is still alive.
    pub fn live_subscriptions(&self, topic: &str) -> usize {
        self.lock()
            .subscribers
            .get(topic)
            .map_or(0, |v| v.iter().filter(|s| !s.is_closed()).count())
    }

    /// How many times [`Bus::subscribe`] was called for `topic`.
    pub fn subscribe_calls(&self, topic: &str) -> usize {
        self.lock().subscribe_calls.get(topic).copied().unwrap_or(0)
    }
}

/// The schema the fake reports for every topic: `{ data: string }`.
pub fn fake_schema(type_name: &str) -> Schema {
    Schema {
        type_name: type_name.to_owned(),
        fields: vec![Field {
            name: "data".to_owned(),
            field_type: FieldType::String { max_len: None },
        }],
    }
}

#[async_trait::async_trait]
impl Bus for FakeBus {
    fn graph(&self) -> GraphSnapshot {
        self.lock().graph.clone()
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, BusError> {
        let mut st = self.lock();
        *st.subscribe_calls.entry(topic.to_owned()).or_default() += 1;
        if let Some(err) = st.failures.get(topic) {
            return Err(err.clone());
        }
        let Some(publisher) = st.graph.publishers_of(topic).next().cloned() else {
            return Err(BusError::NoPublisher(topic.to_owned()));
        };
        let (tx, rx) = mpsc::unbounded_channel();
        st.subscribers.entry(topic.to_owned()).or_default().push(tx);
        Ok(Subscription {
            schema: fake_schema(&publisher.type_name),
            type_name: publisher.type_name,
            type_hash: publisher.type_hash,
            samples: Box::pin(UnboundedReceiverStream::new(rx).map(|s| s)),
        })
    }
}
