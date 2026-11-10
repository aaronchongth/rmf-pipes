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

    /// Publish `data` (JSON per `docs/json-mapping.md`) on `topic`, with the
    /// type its existing endpoints (subscribers, else publishers) advertise.
    async fn publish(&self, topic: &str, data: &serde_json::Value)
    -> Result<Published, WriteError>;

    /// Call `service` with `request` (JSON) and wait up to `timeout` for the
    /// response.
    async fn call(
        &self,
        service: &str,
        request: &serde_json::Value,
        timeout: std::time::Duration,
    ) -> Result<Called, WriteError>;

    /// Observe the requests clients send to `service` (including the
    /// `_action/send_goal` service of an action), without answering them: the
    /// real server still does, and clients are unaffected. The returned
    /// [`Subscription`]'s `type_name` is the service type, its `schema` the
    /// request message, and its `samples` the requests. Dropping it stops
    /// observing. Fails with [`BusError::NoServer`] when nothing serves the
    /// service, since there is then nothing to observe and no type to decode
    /// with. Responses are not observable this way.
    async fn tap(&self, service: &str) -> Result<Subscription, BusError>;
}

/// What a publish was bound to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Published {
    /// ROS type name the message was encoded as.
    pub type_name: String,
    /// RIHS01 hash it was published with.
    pub type_hash: String,
    /// Matching subscriptions in the graph when it was sent.
    pub subscriptions: usize,
}

/// A service call's result.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Called {
    /// ROS service type, e.g. `example_interfaces/srv/AddTwoInts`.
    pub type_name: String,
    /// The response (JSON per `docs/json-mapping.md`).
    #[cfg_attr(feature = "openapi", schema(value_type = Object))]
    pub response: serde_json::Value,
}

/// Why a publish or call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    /// Nothing in the graph uses the topic or serves the service, so its type
    /// is unknown.
    #[error("no endpoint of `{0}` is visible, so its type is unknown")]
    NoEndpoint(String),
    /// The type description could not be obtained.
    #[error("could not get the type of `{target}`: {reason}")]
    Schema {
        /// Topic or service.
        target: String,
        /// What went wrong.
        reason: String,
    },
    /// The JSON doesn't fit the message type.
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    /// The target is ambiguous: its subscribers expect more than one type, or
    /// more than one server provides the service.
    #[error("refusing an ambiguous write: {0}")]
    Conflict(String),
    /// No response within the timeout.
    #[error("no response within {0:?}")]
    Timeout(std::time::Duration),
    /// The bus refused or failed.
    #[error("bus error: {0}")]
    Bus(String),
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
    /// Nothing serves the service, so there is nothing to observe.
    #[error("no server of `{0}` is visible")]
    NoServer(String),
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
