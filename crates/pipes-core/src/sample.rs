//! A decoded sample as served to clients. The envelope fields are rmf-pipes'
//! own; `data` follows `docs/json-mapping.md`.

use serde::Serialize;
use time::OffsetDateTime;

/// One received message with rmf-pipes' envelope.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sample {
    /// Topic name.
    pub topic: String,
    /// ROS type name.
    pub type_name: String,
    /// RIHS01 type hash the subscription is bound to.
    pub type_hash: String,
    /// When rmf-pipes received the sample.
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
    /// Publisher-side timestamp from the rmw_zenoh attachment, when present.
    #[serde(with = "time::serde::rfc3339::option")]
    pub source_timestamp: Option<OffsetDateTime>,
    /// Publisher sequence number from the rmw_zenoh attachment, when present.
    pub sequence: Option<i64>,
    /// Size of the serialized (CDR) payload.
    pub size_bytes: usize,
    /// The decoded message, or `null` when omitted (see `omitted`).
    pub data: Option<serde_json::Value>,
    /// Why `data` is null, if it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted: Option<OmittedReason>,
}

/// Why a sample's `data` is `null`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OmittedReason {
    /// The payload exceeded the configured `max_payload_bytes`.
    TooLarge,
}
