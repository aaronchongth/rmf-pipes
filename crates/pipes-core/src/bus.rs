//! The boundary between rmf-pipes and the ROS 2 bus. `pipes-bus` implements it
//! with hiroz; tests use [`crate::fake::FakeBus`].

use std::{fmt, time::SystemTime};

use futures::stream::BoxStream;

use crate::{graph::GraphSnapshot, schema::Schema};

/// Something that can describe the ROS graph and subscribe to topics.
#[async_trait::async_trait]
pub trait Bus: Send + Sync + fmt::Debug + 'static {
    /// The current graph, excluding rmf-pipes' own entities. Must be cheap
    /// enough to call about once per second.
    fn graph(&self) -> GraphSnapshot;

    /// Discover the topic's schema from its publishers and subscribe, using the
    /// publishers' durability and reliability. Dropping the returned
    /// [`Subscription`] unsubscribes.
    async fn subscribe(&self, topic: &str) -> Result<Subscription, BusError>;
}

/// An open subscription. Dropping it (including its stream) unsubscribes.
pub struct Subscription {
    /// ROS type name, e.g. `std_msgs/msg/String`.
    pub type_name: String,
    /// RIHS01 hash the subscription is bound to (the publishers').
    pub type_hash: String,
    /// The discovered message schema.
    pub schema: Schema,
    /// Samples in arrival order. The bus buffers a bounded number and drops the
    /// oldest when the consumer falls behind (ROS keep-last semantics).
    pub samples: BoxStream<'static, RawSample>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("type_name", &self.type_name)
            .field("type_hash", &self.type_hash)
            .finish_non_exhaustive()
    }
}

/// A received, not yet decoded, sample. Decoding is deferred so the hub can drop
/// samples (rate or size caps) without paying for it.
pub struct RawSample {
    /// When rmf-pipes received the sample.
    pub received_at: SystemTime,
    /// Publisher timestamp from the rmw_zenoh attachment, if present.
    pub source_timestamp: Option<SystemTime>,
    /// Publisher sequence number from the rmw_zenoh attachment, if present.
    pub sequence: Option<i64>,
    /// Size of the serialized (CDR) payload in bytes.
    pub size_bytes: usize,
    decoder: Box<dyn FnOnce() -> Result<serde_json::Value, DecodeError> + Send>,
}

impl RawSample {
    /// A sample of `size_bytes` whose payload `decoder` turns into JSON.
    pub fn new(
        received_at: SystemTime,
        size_bytes: usize,
        decoder: impl FnOnce() -> Result<serde_json::Value, DecodeError> + Send + 'static,
    ) -> Self {
        Self {
            received_at,
            source_timestamp: None,
            sequence: None,
            size_bytes,
            decoder: Box::new(decoder),
        }
    }

    /// Attach the publisher-side timestamp and sequence number.
    pub fn with_attachment(
        mut self,
        source_timestamp: Option<SystemTime>,
        sequence: Option<i64>,
    ) -> Self {
        self.source_timestamp = source_timestamp;
        self.sequence = sequence;
        self
    }

    /// Decode the payload into JSON (see `docs/json-mapping.md`).
    pub fn decode(self) -> Result<serde_json::Value, DecodeError> {
        (self.decoder)()
    }
}

impl fmt::Debug for RawSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawSample")
            .field("received_at", &self.received_at)
            .field("sequence", &self.sequence)
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

/// A payload could not be decoded with the topic's schema.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("decode failed: {0}")]
pub struct DecodeError(pub String);

/// Why [`Bus::subscribe`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BusError {
    /// Nobody publishes the topic, so there is no schema to discover.
    #[error("no publisher of `{0}` is visible")]
    NoPublisher(String),
    /// Schema discovery failed (e.g. the publisher has no type description service).
    #[error("could not get the schema of `{topic}`: {reason}")]
    Schema {
        /// The topic.
        topic: String,
        /// What went wrong.
        reason: String,
    },
    /// The bus refused the subscription.
    #[error("could not subscribe to `{topic}`: {reason}")]
    Subscribe {
        /// The topic.
        topic: String,
        /// What went wrong.
        reason: String,
    },
}
