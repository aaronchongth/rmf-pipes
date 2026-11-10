//! Recording rules and records: what to record from the bus, and where to.
//!
//! A [`RecordRule`] names a set of topics, services or actions by pattern, the
//! [`Part`]s of them to record, and the [`Sink`]s that receive each
//! [`Record`]. The [`crate::recorder::Recorder`] applies the rules to the live
//! graph.
//!
//! | `kind` | Parts | Observed by |
//! |---|---|---|
//! | `topic` | `message` | subscribing |
//! | `service` | `request` | a request tap ([`crate::bus::Bus::tap`]); responses are not observable |
//! | `action` | `goal`, `feedback`, `status`, `result` | tapping `_action/send_goal`; subscribing to `_action/feedback` and `_action/status`; calling `_action/get_result` once a goal ends |
//!
//! Topic and service rules never match an action's internal `_action/…`
//! topics and services; use `kind = "action"` for those.

use std::{collections::HashSet, fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::pattern::TopicPattern;

/// Most rules at once.
pub const MAX_RULES: usize = 64;
/// Longest rule name.
pub const MAX_RULE_NAME: usize = 64;

/// What a rule matches.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    /// Topics with at least one publisher.
    Topic,
    /// Services with at least one server.
    Service,
    /// Actions (found by their `_action/send_goal` server).
    Action,
}

impl TargetKind {
    /// Every part this kind has, in recording order.
    pub fn parts(self) -> &'static [Part] {
        match self {
            Self::Topic => &[Part::Message],
            Self::Service => &[Part::Request],
            Self::Action => &[Part::Goal, Part::Feedback, Part::Status, Part::Result],
        }
    }

    /// The API and database spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Service => "service",
            Self::Action => "action",
        }
    }
}

/// One kind of thing recorded from a target.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    /// A topic message.
    Message,
    /// A service request.
    Request,
    /// An action goal as sent (the goal fields; `goal_id` is in the record).
    Goal,
    /// An action feedback message.
    Feedback,
    /// A goal's status changing (`accepted`, `executing`, `canceling`,
    /// `succeeded`, `canceled`, `aborted`).
    Status,
    /// A finished goal's result, fetched from the action server.
    Result,
}

impl Part {
    /// The API and database spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Request => "request",
            Self::Goal => "goal",
            Self::Feedback => "feedback",
            Self::Status => "status",
            Self::Result => "result",
        }
    }
}

/// Where records go.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sink {
    /// The process log (target `rmf_pipes::record`), with long payloads cut.
    Console,
    /// The `pipes.records` table (needs `--database-url`).
    Database,
}

/// What to record and where to. JSON (API) and TOML (`--record-config`) use
/// the same shape.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRule {
    /// Unique name, stored with every record: letters, digits, `_` and `-`.
    pub name: String,
    /// Names to match (`*` one segment, `**` any number), e.g.
    /// `/robot*/navigate_to_pose` won't work (no sub-segment wildcards) but
    /// `/*/navigate_to_pose` will.
    #[serde(rename = "match")]
    #[cfg_attr(feature = "openapi", schema(value_type = String))]
    pub pattern: TopicPattern,
    /// What the pattern matches.
    pub kind: TargetKind,
    /// Parts to record; empty or omitted means every part of `kind`.
    #[serde(default)]
    pub parts: Vec<Part>,
    /// Where records go (at least one).
    pub sinks: Vec<Sink>,
    /// Most messages, requests or feedback recorded per second per target;
    /// extra ones are dropped (counted). Goals, statuses and results are
    /// never dropped. Omit for no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rate_hz: Option<f64>,
}

impl RecordRule {
    /// Whether this rule records `part`.
    pub fn records(&self, part: Part) -> bool {
        self.parts.contains(&part)
    }

    /// Whether this rule sends to `sink`.
    pub fn sends_to(&self, sink: Sink) -> bool {
        self.sinks.contains(&sink)
    }

    /// The minimum time between throttled records, if any.
    pub fn min_interval(&self) -> Option<std::time::Duration> {
        self.max_rate_hz
            .and_then(|hz| std::time::Duration::try_from_secs_f64(1.0 / hz).ok())
    }
}

/// Why a rule set was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RuleError(pub String);

/// Validate and normalize `rules`: unique valid names, parts that exist for
/// the kind (empty means all; sorted, deduplicated), at least one sink
/// (deduplicated), `database` only when `database_available`, and a sane rate.
pub fn validate_rules(
    mut rules: Vec<RecordRule>,
    database_available: bool,
) -> Result<Vec<RecordRule>, RuleError> {
    if rules.len() > MAX_RULES {
        return Err(RuleError(format!("at most {MAX_RULES} rules")));
    }
    let mut names = HashSet::new();
    for r in &mut rules {
        let n = &r.name;
        if n.is_empty()
            || n.len() > MAX_RULE_NAME
            || !n
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(RuleError(format!(
                "rule name `{}` must be 1-{MAX_RULE_NAME} letters, digits, `_` or `-`",
                crate::hub::shorten(n)
            )));
        }
        if !names.insert(n.clone()) {
            return Err(RuleError(format!("rule name `{n}` is used twice")));
        }
        let allowed = r.kind.parts();
        if r.parts.is_empty() {
            r.parts = allowed.to_vec();
        }
        if let Some(bad) = r.parts.iter().find(|p| !allowed.contains(p)) {
            return Err(RuleError(format!(
                "rule `{n}`: a {} has no `{}` part (it has: {})",
                r.kind.as_str(),
                bad.as_str(),
                allowed
                    .iter()
                    .map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        r.parts.sort();
        r.parts.dedup();
        if r.sinks.is_empty() {
            return Err(RuleError(format!("rule `{n}` has no sinks")));
        }
        let mut seen = HashSet::new();
        r.sinks.retain(|s| seen.insert(*s));
        if r.sends_to(Sink::Database) && !database_available {
            return Err(RuleError(format!(
                "rule `{n}` records to the database, but none is configured (--database-url)"
            )));
        }
        if let Some(hz) = r.max_rate_hz
            && !(hz.is_finite() && (0.001..=1e6).contains(&hz))
        {
            return Err(RuleError(format!(
                "rule `{n}`: max_rate_hz must be between 0.001 and 1000000"
            )));
        }
    }
    Ok(rules)
}

/// One recorded event.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Record {
    /// When rmf-pipes observed it.
    #[serde(with = "time::serde::rfc3339")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    pub at: OffsetDateTime,
    /// The rule that recorded it.
    pub rule: String,
    /// What was observed.
    pub kind: TargetKind,
    /// Topic, service or action name.
    pub name: String,
    /// Which part.
    pub part: Part,
    /// ROS type of `data`'s source: the topic's message type, the service
    /// type, or for actions the part's message type (e.g.
    /// `nav2_msgs/action/NavigateToPose_FeedbackMessage`).
    pub type_name: String,
    /// The action goal this belongs to (UUID), for action parts.
    pub goal_id: Option<String>,
    /// The payload (JSON per `docs/json-mapping.md`): the message, request,
    /// goal, feedback, `{status}` or `{status, result}`. `null` when the
    /// payload was too large to decode.
    #[cfg_attr(feature = "openapi", schema(value_type = Option<Object>))]
    pub data: Option<serde_json::Value>,
}

/// Receives records. Must not block: sinks buffer (boundedly) and do their
/// own I/O.
pub trait RecordSink: Send + Sync + fmt::Debug {
    /// Take one record.
    fn accept(&self, record: &Arc<Record>);
}

/// Longest payload text the console sink prints, in bytes.
pub const CONSOLE_DATA_MAX: usize = 2048;

/// Logs each record at `info` under the `rmf_pipes::record` target.
#[derive(Debug, Default)]
pub struct ConsoleSink;

impl RecordSink for ConsoleSink {
    fn accept(&self, r: &Arc<Record>) {
        let data = r
            .data
            .as_ref()
            .map_or_else(|| "null".to_owned(), serde_json::Value::to_string);
        let data = clip_text(&data, CONSOLE_DATA_MAX);
        tracing::info!(
            target: "rmf_pipes::record",
            rule = %r.rule,
            kind = r.kind.as_str(),
            name = %r.name,
            part = r.part.as_str(),
            goal_id = r.goal_id.as_deref().unwrap_or(""),
            type_name = %r.type_name,
            data = %data,
            "record"
        );
    }
}

/// `s` cut to at most `max` bytes on a char boundary, with the cut marked.
fn clip_text(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes)", &s[..end], s.len())
}

/// A ROS `unique_identifier_msgs/UUID` as JSON (`{"uuid": "<base64 of 16
/// bytes>"}`, per the JSON mapping) to its canonical text form.
pub fn uuid_text(goal_id: &serde_json::Value) -> Option<String> {
    use base64::Engine;
    let b64 = goal_id.get("uuid")?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let b: [u8; 16] = bytes.try_into().ok()?;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rule(v: serde_json::Value) -> RecordRule {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn rules_get_every_part_by_default_and_are_normalized() {
        let r = validate_rules(
            vec![rule(json!({
                "name": "nav", "match": "/*/navigate_to_pose", "kind": "action",
                "sinks": ["console", "console"]
            }))],
            false,
        )
        .unwrap();
        assert_eq!(
            r[0].parts,
            [Part::Goal, Part::Feedback, Part::Status, Part::Result]
        );
        assert_eq!(r[0].sinks, [Sink::Console]);
        let r = validate_rules(
            vec![rule(json!({
                "name": "n", "match": "x", "kind": "action",
                "parts": ["result", "goal", "goal"], "sinks": ["console"]
            }))],
            false,
        )
        .unwrap();
        assert_eq!(r[0].parts, [Part::Goal, Part::Result]);
    }

    #[test]
    fn invalid_rules_are_refused_with_the_reason() {
        let base = json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["console"]});
        let with = |k: &str, v: serde_json::Value| {
            let mut r = base.clone();
            r[k] = v;
            rule(r)
        };
        for (r, needle) in [
            (with("name", json!("has space")), "letters"),
            (with("name", json!("")), "letters"),
            (with("parts", json!(["goal"])), "has no `goal` part"),
            (with("sinks", json!([])), "no sinks"),
            (with("sinks", json!(["database"])), "none is configured"),
            (with("max_rate_hz", json!(0.0)), "max_rate_hz"),
            (with("max_rate_hz", json!(1e9)), "max_rate_hz"),
        ] {
            let err = validate_rules(vec![r], false).unwrap_err();
            assert!(err.0.contains(needle), "{err} should mention {needle}");
        }
        let twice = vec![rule(base.clone()), rule(base.clone())];
        assert!(
            validate_rules(twice, false)
                .unwrap_err()
                .0
                .contains("twice")
        );
        assert!(validate_rules(vec![with("sinks", json!(["database"]))], true).is_ok());
        let many = (0..=MAX_RULES)
            .map(|i| with("name", json!(format!("r{i}"))))
            .collect();
        assert!(validate_rules(many, false).is_err());
    }

    #[test]
    fn rules_reject_unknown_fields_and_bad_patterns() {
        let bad: Result<RecordRule, _> = serde_json::from_value(
            json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["console"], "sink": "console"}),
        );
        assert!(bad.is_err());
        let bad: Result<RecordRule, _> = serde_json::from_value(
            json!({"name": "a", "match": "robot*/x", "kind": "topic", "sinks": ["console"]}),
        );
        assert!(bad.unwrap_err().to_string().contains("unsupported"));
    }

    #[test]
    fn goal_ids_render_as_uuids() {
        // 16 bytes 0x00..0x0f.
        let id = json!({"uuid": "AAECAwQFBgcICQoLDA0ODw=="});
        assert_eq!(
            uuid_text(&id).as_deref(),
            Some("00010203-0405-0607-0809-0a0b0c0d0e0f")
        );
        assert_eq!(uuid_text(&json!({"uuid": "AAE="})), None);
        assert_eq!(uuid_text(&json!({})), None);
    }

    #[test]
    fn console_payloads_are_cut_on_char_boundaries() {
        let s = "é".repeat(10);
        let c = clip_text(&s, 5);
        assert!(c.starts_with("éé…"), "{c}");
        assert!(c.ends_with("(20 bytes)"));
        assert_eq!(clip_text("short", 10), "short");
    }
}
