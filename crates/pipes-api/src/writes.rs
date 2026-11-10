//! The write path: publishing to topics and calling services on the robot
//! bus. Every write is:
//!
//! 1. **authenticated**: auth must be enabled, or demo mode on (every caller
//!    is then the demo user); plain anonymous mode is read-only;
//! 2. **allowlisted**: the target must match a `--allow-publish` /
//!    `--allow-call` pattern, which also sets the minimum role (default
//!    operator); when several match, the most permissive applies;
//! 3. **rate-limited**: at most one write per user per target per
//!    `--write-interval`;
//! 4. **audited before it happens**: a `pending` entry (with the payload) is
//!    written first, and if that fails nothing is sent (fail closed). Without
//!    a database (demo mode only) entries go to the process log instead. The
//!    write itself then runs in its own task, so a client hanging up can't
//!    cut it short, and a second entry (`ok`, with the type and hash used, or
//!    `error`) records how it ended, linked by `detail.intent`.
//!
//! Refusals are audited too, at most [`DENIAL_AUDITS_PER_MINUTE`] per user and
//! action, so a client can't flood the audit log with refused requests. At
//! most [`MAX_CONCURRENT_WRITES`] writes run at once.

use std::{collections::HashMap, str::FromStr, sync::Mutex, time::Duration};

use axum::{Json, extract::State};
use pipes_core::{
    bus::{Called, Published, WriteError},
    hub::validate_topic,
    pattern::TopicPattern,
};
use pipes_db::{Outcome, Role};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;
use utoipa::ToSchema;

use crate::{
    AppState,
    accounts::ApiJson,
    auth::{Caller, Principal, audit, record_audit},
    error::{ApiError, Problem},
};

/// Longest payload kept verbatim in an audit entry, in bytes.
const AUDIT_PAYLOAD_MAX: usize = 4096;
/// Default and maximum service-call timeouts. hiroz's querier gives up after
/// 10s on its own, so the maximum stays below that.
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest `timeout_ms` honoured.
pub const MAX_CALL_TIMEOUT: Duration = Duration::from_secs(9);
/// Refusals audited per user and action per minute; more are only counted in
/// the log.
pub const DENIAL_AUDITS_PER_MINUTE: u32 = 20;
/// Writes in flight at once (each can hold a service call open).
pub const MAX_CONCURRENT_WRITES: usize = 32;

/// One allowlist entry: `pattern` or `pattern=role`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRule {
    /// Topic or service names it covers (`*`, `**` wildcards).
    pub pattern: TopicPattern,
    /// Minimum role to write.
    pub role: Role,
}

impl FromStr for WriteRule {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let (pattern, role) = match s.split_once('=') {
            Some((p, r)) => (p, r.trim().parse::<Role>().map_err(|e| e.to_string())?),
            None => (s, Role::Operator),
        };
        if role == Role::Viewer {
            return Err("write rules need at least the operator role".into());
        }
        let pattern = pattern.trim();
        // Literal segments must be possible ROS names, or the rule could
        // never match (a typo would silently allow nothing).
        let bad = pattern.trim_start_matches('/').split('/').find(|seg| {
            *seg != "*"
                && *seg != "**"
                && !seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
        if let Some(seg) = bad {
            return Err(format!(
                "`{seg}` is not a valid ROS name segment (letters, digits and `_` only)"
            ));
        }
        Ok(Self {
            pattern: TopicPattern::parse(pattern).map_err(|e| e.to_string())?,
            role,
        })
    }
}

impl WriteRule {
    /// Whether the rule matches everything (`**`), worth a warning.
    pub fn matches_everything(&self) -> bool {
        self.pattern.as_str().trim_start_matches('/') == "**"
    }
}

/// Which writes are allowed, and how often.
#[derive(Debug, Clone, Default)]
pub struct WritePolicy {
    /// Topics that may be published to.
    pub publish: Vec<WriteRule>,
    /// Services that may be called.
    pub call: Vec<WriteRule>,
    /// Minimum time between writes by one user to one target; `None` = no limit.
    pub min_interval: Option<Duration>,
}

impl WritePolicy {
    fn required(rules: &[WriteRule], name: &str) -> Option<Role> {
        rules
            .iter()
            .filter(|r| r.pattern.matches(name))
            .map(|r| r.role)
            .min()
    }
}

/// Start of the current one-minute window and refusals audited in it.
type DenialWindow = (Instant, u32);

/// Last write per (user, kind, target), denial-audit budget per user, and
/// writes in flight.
#[derive(Debug)]
pub struct WriteLimiter {
    last: Mutex<HashMap<(i64, &'static str, String), Instant>>,
    denials: Mutex<HashMap<(i64, &'static str), DenialWindow>>,
    in_flight: std::sync::Arc<tokio::sync::Semaphore>,
}

impl Default for WriteLimiter {
    fn default() -> Self {
        Self {
            last: Mutex::default(),
            denials: Mutex::default(),
            in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_WRITES)),
        }
    }
}

impl WriteLimiter {
    /// Admit a write now, or say how long to wait.
    fn admit(&self, key: (i64, &'static str, String), min: Duration) -> Result<(), Duration> {
        let now = Instant::now();
        let mut map = self.last.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() > 10_000 {
            map.retain(|_, t| now.duration_since(*t) < min);
        }
        if let Some(t) = map.get(&key) {
            let since = now.duration_since(*t);
            if since < min {
                return Err(min - since);
            }
        }
        map.insert(key, now);
        Ok(())
    }

    /// Whether this refusal should be audited (the per-user budget isn't spent).
    fn audit_denial(&self, user: i64, action: &'static str) -> bool {
        let now = Instant::now();
        let mut map = self.denials.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() > 10_000 {
            map.retain(|_, (t, _)| now.duration_since(*t) < Duration::from_secs(60));
        }
        let e = map.entry((user, action)).or_insert((now, 0));
        if now.duration_since(e.0) >= Duration::from_secs(60) {
            *e = (now, 0);
        }
        e.1 += 1;
        e.1 <= DENIAL_AUDITS_PER_MINUTE
    }
}

/// `POST /api/v1/topic/publish`
#[derive(Debug, Deserialize, ToSchema)]
pub struct PublishRequest {
    /// Fully qualified topic name.
    pub name: String,
    /// The message, as JSON per docs/json-mapping.md (omitted fields take
    /// their defaults).
    #[schema(value_type = Object)]
    pub data: Value,
}

/// `POST /api/v1/service/call`
#[derive(Debug, Deserialize, ToSchema)]
pub struct CallRequest {
    /// Fully qualified service name.
    pub name: String,
    /// The request, as JSON per docs/json-mapping.md.
    #[schema(value_type = Object)]
    pub request: Value,
    /// How long to wait for the response (default 5000, at most 9000).
    pub timeout_ms: Option<u64>,
}

/// An allowlist entry, as shown to clients.
#[derive(Debug, Serialize, ToSchema)]
pub struct RuleView {
    /// Pattern (`*` one segment, `**` any).
    pub pattern: String,
    /// Minimum role.
    pub role: Role,
}

/// What may be written, by whom, and how often.
#[derive(Debug, Serialize, ToSchema)]
pub struct PolicyView {
    /// Writes need authentication; without it nothing may be written.
    pub writes_enabled: bool,
    /// Publishable topics.
    pub publish: Vec<RuleView>,
    /// Callable services.
    pub call: Vec<RuleView>,
    /// Minimum milliseconds between writes by one user to one target.
    pub min_interval_ms: Option<u64>,
}

fn views(rules: &[WriteRule]) -> Vec<RuleView> {
    rules
        .iter()
        .map(|r| RuleView {
            pattern: r.pattern.to_string(),
            role: r.role,
        })
        .collect()
}

/// The write allowlist.
#[utoipa::path(get, path = "/api/v1/writes", tag = "writes",
    responses((status = 200, description = "What may be written, by whom, and how often", body = PolicyView)))]
pub async fn policy(State(state): State<AppState>) -> Json<PolicyView> {
    let p = state.writes();
    Json(PolicyView {
        writes_enabled: state.auth().is_some() || state.demo(),
        publish: views(&p.publish),
        call: views(&p.call),
        min_interval_ms: p
            .min_interval
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
    })
}

/// Publish a message on an allowlisted topic (operator, or as configured).
///
/// The message type is the one the topic's subscribers (else publishers)
/// advertise, so something must already use the topic.
#[utoipa::path(post, path = "/api/v1/topic/publish", tag = "writes", request_body = PublishRequest,
    responses(
        (status = 200, description = "Published", body = Published),
        (status = 400, description = "Bad name or the message doesn't fit the type (`invalid_topic`, `invalid_message`)", body = ApiError),
        (status = 403, description = "Not allowlisted, or the role is too low (`not_allowlisted`, `forbidden`)", body = ApiError),
        (status = 404, description = "Nothing uses the topic, so its type is unknown (`no_endpoint`)", body = ApiError),
        (status = 409, description = "The topic's subscribers expect different types (`type_conflict`)", body = ApiError),
        (status = 429, description = "Too many writes to this topic (`too_many_writes`); see Retry-After", body = ApiError),
        (status = 502, description = "The bus failed (`bus_error`)", body = ApiError),
        (status = 503, description = "Auth is disabled, the type is unavailable, or the audit log can't be written (`auth_disabled`, `topic_failed`, `audit_unavailable`)", body = ApiError),
    ))]
pub async fn publish(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiJson(req): ApiJson<PublishRequest>,
) -> Result<Json<Published>, Problem> {
    let ticket = authorize(&state, &caller, Kind::Publish, &req.name, &req.data).await?;
    let bus = state.hub().bus().clone();
    let (name, data) = (req.name.clone(), req.data);
    let result = complete(&state, &caller, Kind::Publish, &req.name, ticket, async move {
        let r = bus.publish(&name, &data).await;
        let detail = r.as_ref().ok().map(|p| json!({"type_name": p.type_name, "type_hash": p.type_hash, "subscriptions": p.subscriptions}));
        (r, detail)
    })
    .await?;
    Ok(Json(result))
}

/// Call an allowlisted service (operator, or as configured).
#[utoipa::path(post, path = "/api/v1/service/call", tag = "writes", request_body = CallRequest,
    responses(
        (status = 200, description = "The response", body = Called),
        (status = 400, description = "Bad name or the request doesn't fit the type (`invalid_topic`, `invalid_message`)", body = ApiError),
        (status = 403, description = "Not allowlisted, or the role is too low (`not_allowlisted`, `forbidden`)", body = ApiError),
        (status = 404, description = "No server (`no_endpoint`)", body = ApiError),
        (status = 409, description = "More than one server (`type_conflict`): a call would run on all of them", body = ApiError),
        (status = 429, description = "Too many calls to this service (`too_many_writes`)", body = ApiError),
        (status = 502, description = "The bus failed (`bus_error`)", body = ApiError),
        (status = 503, description = "`auth_disabled`, `topic_failed`, `audit_unavailable`", body = ApiError),
        (status = 504, description = "No response within `timeout_ms` (`timeout`)", body = ApiError),
    ))]
pub async fn call(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiJson(req): ApiJson<CallRequest>,
) -> Result<Json<Called>, Problem> {
    let timeout = req
        .timeout_ms
        .map_or(DEFAULT_CALL_TIMEOUT, Duration::from_millis)
        .min(MAX_CALL_TIMEOUT);
    let ticket = authorize(&state, &caller, Kind::Call, &req.name, &req.request).await?;
    let bus = state.hub().bus().clone();
    let (name, request) = (req.name.clone(), req.request);
    let result = complete(&state, &caller, Kind::Call, &req.name, ticket, async move {
        let r = bus.call(&name, &request, timeout).await;
        let detail = r.as_ref().ok().map(|c| json!({"type_name": c.type_name}));
        (r, detail)
    })
    .await?;
    Ok(Json(result))
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Publish,
    Call,
}

impl Kind {
    fn action(self) -> &'static str {
        match self {
            Self::Publish => "topic.publish",
            Self::Call => "service.call",
        }
    }
}

/// Permission to perform one write: the id of its `pending` audit entry and
/// a slot among the writes in flight.
struct Ticket {
    intent: i64,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

/// Steps 1-4.
async fn authorize(
    state: &AppState,
    caller: &Principal,
    kind: Kind,
    name: &str,
    payload: &Value,
) -> Result<Ticket, Problem> {
    // The demo user (id 0: real ids start at 1) shares one rate-limit bucket.
    let (actor_id, actor) = match caller {
        Principal::User { user, .. } => (user.id, user.username.clone()),
        Principal::Demo => (0, caller.name().to_owned()),
        Principal::Anonymous if state.auth().is_some() => {
            return Err(Problem::unauthenticated("log in first"));
        }
        Principal::Anonymous => {
            return Err(Problem::unavailable(
                "auth_disabled",
                "writes need authentication (--database-url) or --demo; rmf-pipes is read-only",
            ));
        }
    };
    validate_topic(name)?;
    let action = kind.action();
    let limiter = state.write_limiter();
    let deny = |reason: &'static str| {
        let audited = limiter.audit_denial(actor_id, action);
        let entry = caller.audit(action, name, Outcome::Denied, json!({"reason": reason}));
        async move {
            if audited {
                audit(state, entry).await;
            } else {
                tracing::debug!(user = %actor, action, reason, "refusal not audited: per-user budget spent");
            }
        }
    };
    let policy = state.writes();
    let rules = match kind {
        Kind::Publish => &policy.publish,
        Kind::Call => &policy.call,
    };
    let Some(role) = WritePolicy::required(rules, name) else {
        deny("not_allowlisted").await;
        return Err(Problem::forbidden(
            "not_allowlisted",
            format!("`{name}` is not on the write allowlist"),
        ));
    };
    if let Err(p) = caller.require(role) {
        deny("forbidden").await;
        return Err(p);
    }
    if let Some(min) = policy.min_interval
        && let Err(wait) = limiter.admit((actor_id, action, name.to_owned()), min)
    {
        deny("rate_limited").await;
        return Err(Problem::too_many(
            "too_many_writes",
            format!("wait before writing to `{name}` again"),
            wait,
        ));
    }
    let Ok(slot) = limiter.in_flight.clone().try_acquire_owned() else {
        deny("too_many_in_flight").await;
        return Err(Problem::unavailable(
            "too_many_writes_in_flight",
            "too many writes are in progress; try again",
        ));
    };
    // Fail closed: no audit entry, no write.
    let (payload, truncated) = clip(payload);
    let entry = caller.audit(
        action,
        name,
        Outcome::Pending,
        json!({"payload": payload, "payload_truncated": truncated}),
    );
    let intent = record_audit(state, &entry).await.map_err(|_| {
        tracing::error!(%action, target = %name, "cannot write the audit log; refusing the write");
        Problem::unavailable(
            "audit_unavailable",
            "the audit log can't be written, so the write was refused",
        )
    })?;
    Ok(Ticket {
        intent,
        _slot: slot,
    })
}

/// Run the write in its own task (a disconnecting client must not cut it
/// short or leave its intent unresolved) and record how it ended.
async fn complete<T: Send + 'static>(
    state: &AppState,
    caller: &Principal,
    kind: Kind,
    name: &str,
    ticket: Ticket,
    write: impl std::future::Future<Output = (Result<T, WriteError>, Option<Value>)> + Send + 'static,
) -> Result<T, Problem> {
    let (state, caller, name) = (state.clone(), caller.clone(), name.to_owned());
    let task = tokio::spawn(async move {
        let Ticket { intent, _slot } = ticket;
        let (result, detail) = write.await;
        let entry = match (&result, detail) {
            (Ok(_), detail) => {
                let mut d = detail.unwrap_or_else(|| json!({}));
                d["intent"] = json!(intent);
                caller.audit(kind.action(), &name, Outcome::Ok, d)
            }
            (Err(e), _) => caller.audit(
                kind.action(),
                &name,
                Outcome::Error,
                json!({"intent": intent, "reason": e.to_string()}),
            ),
        };
        audit(&state, entry).await;
        result
    });
    match task.await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(problem(e)),
        Err(err) => {
            tracing::error!(%err, "write task failed");
            Err(Problem::internal())
        }
    }
}

fn problem(e: WriteError) -> Problem {
    match e {
        WriteError::NoEndpoint(_) => Problem::not_found("no_endpoint", e.to_string()),
        WriteError::Conflict(_) => Problem::conflict("type_conflict", e.to_string()),
        WriteError::Schema { .. } => Problem::unavailable("topic_failed", e.to_string()),
        WriteError::InvalidMessage(m) => Problem::bad_request("invalid_message", m),
        WriteError::Timeout(_) => Problem::timeout("timeout", e.to_string()),
        WriteError::Bus(_) => Problem::bad_gateway("bus_error", e.to_string()),
    }
}

/// The payload for the audit log and whether it was truncated. A truncated
/// payload is its JSON text cut at [`AUDIT_PAYLOAD_MAX`] bytes; the flag sits
/// outside the payload so a client can't forge it.
fn clip(v: &Value) -> (Value, bool) {
    let s = v.to_string();
    if s.len() <= AUDIT_PAYLOAD_MAX {
        return (v.clone(), false);
    }
    let mut end = AUDIT_PAYLOAD_MAX;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (Value::String(s[..end].to_owned()), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_parse_with_optional_roles() {
        let r: WriteRule = "door_requests".parse().unwrap();
        assert_eq!(r.role, Role::Operator);
        let r: WriteRule = "fleet/*/estop=admin".parse().unwrap();
        assert!(r.pattern.matches("/fleet/r1/estop") && r.role == Role::Admin);
        assert!(
            "x=viewer".parse::<WriteRule>().is_err(),
            "viewers never write"
        );
        assert!("x=root".parse::<WriteRule>().is_err());
        assert!("a//b".parse::<WriteRule>().is_err());
    }

    #[test]
    fn the_most_permissive_matching_rule_applies() {
        let rules: Vec<WriteRule> = ["fleet/**=admin", "fleet/*/mode"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(
            WritePolicy::required(&rules, "/fleet/r1/mode"),
            Some(Role::Operator)
        );
        assert_eq!(
            WritePolicy::required(&rules, "/fleet/r1/estop"),
            Some(Role::Admin)
        );
        assert_eq!(WritePolicy::required(&rules, "/doors"), None);
    }

    #[test]
    fn large_payloads_are_clipped_for_the_audit_log() {
        let (c, truncated) = clip(&json!({"data": "é".repeat(10_000)}));
        assert!(truncated);
        assert!(
            c.as_str().unwrap().len() <= AUDIT_PAYLOAD_MAX,
            "bytes, not chars"
        );
        assert_eq!(clip(&json!({"a": 1})), (json!({"a": 1}), false));
        // A payload shaped like a truncation marker is just a payload.
        let fake = json!({"truncated": true, "size": 1, "head": "x"});
        assert_eq!(clip(&fake), (fake.clone(), false));
    }

    #[test]
    fn rules_reject_names_that_can_never_match() {
        for bad in [
            "door-requests",
            "door requests",
            "door=admin=operator",
            "fleet/*/e-stop",
        ] {
            assert!(bad.parse::<WriteRule>().is_err(), "{bad}");
        }
        assert!("**".parse::<WriteRule>().unwrap().matches_everything());
        assert!(
            !"fleet/**"
                .parse::<WriteRule>()
                .unwrap()
                .matches_everything()
        );
    }
}
