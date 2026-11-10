//! Recorder tests against [`FakeBus`] and a real [`Hub`], with paused time.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};

use super::*;
use crate::{
    bus::BusError,
    fake::FakeBus,
    hub::HubConfig,
    record::{Record, RecordSink},
};

#[derive(Debug, Default)]
struct Collect(Mutex<Vec<Arc<Record>>>);

impl RecordSink for Collect {
    fn accept(&self, r: &Arc<Record>) {
        self.0.lock().unwrap().push(r.clone());
    }
}

impl Collect {
    fn parts(&self) -> Vec<(String, Part, Option<String>, Option<Value>)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|r| (r.name.clone(), r.part, r.goal_id.clone(), r.data.clone()))
            .collect()
    }

    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

struct Rig {
    bus: Arc<FakeBus>,
    hub: Hub,
    rec: Recorder,
    console: Arc<Collect>,
    db: Arc<Collect>,
}

fn rule(v: Value) -> RecordRule {
    serde_json::from_value(v).unwrap()
}

fn rig(rules: Vec<Value>) -> Rig {
    rig_with(rules, RecorderConfig::default())
}

fn rig_with(rules: Vec<Value>, cfg: RecorderConfig) -> Rig {
    rig_full(
        rules,
        cfg,
        HubConfig {
            max_rate_hz: None,
            ..HubConfig::default()
        },
    )
}

fn rig_full(rules: Vec<Value>, cfg: RecorderConfig, hub_cfg: HubConfig) -> Rig {
    let bus = FakeBus::new();
    let hub = Hub::new(bus.clone(), hub_cfg);
    let console = Arc::new(Collect::default());
    let db = Arc::new(Collect::default());
    let rec = Recorder::new(
        hub.clone(),
        cfg,
        Sinks {
            console: console.clone(),
            database: Some(db.clone()),
        },
        rules.into_iter().map(rule).collect(),
    )
    .unwrap();
    Rig {
        bus,
        hub,
        rec,
        console,
        db,
    }
}

impl Rig {
    /// Refresh the graph and reconcile, then let the streams open.
    async fn tick(&self) {
        self.hub.tick();
        self.rec.tick();
        settle().await;
    }

    fn states(&self) -> Vec<(String, Source, StreamState)> {
        self.rec
            .view()
            .streams
            .into_iter()
            .map(|s| (s.name, s.source, s.status))
            .collect()
    }
}

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

const ACTION: &str = "/robot0/navigate_to_pose";
const GOAL_A: &str = "AAECAwQFBgcICQoLDA0ODw=="; // 00010203-0405-0607-0809-0a0b0c0d0e0f
const UUID_A: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
const GOAL_B: &str = "/////////////////////w=="; // ffffffff-...

/// An action as rmw_zenoh shows it: send_goal and get_result servers,
/// feedback and status publishers.
fn add_action(bus: &FakeBus, name: &str) {
    bus.serve(
        &format!("{name}/_action/send_goal"),
        "nav2_msgs/action/NavigateToPose_SendGoal",
        |_| Ok(json!({"accepted": true})),
    );
    bus.serve(
        &format!("{name}/_action/get_result"),
        "nav2_msgs/action/NavigateToPose_GetResult",
        |req| Ok(json!({"status": 4, "result": {"error_code": 0, "asked": req["goal_id"]}})),
    );
    bus.add_publisher(
        &format!("{name}/_action/feedback"),
        "nav2_msgs/action/NavigateToPose_FeedbackMessage",
        "RIHS01_f",
    );
    bus.add_publisher(
        &format!("{name}/_action/status"),
        "action_msgs/msg/GoalStatusArray",
        "RIHS01_s",
    );
}

fn statuses(goals: &[(&str, i64)]) -> Value {
    json!({"status_list": goals.iter().map(|(g, s)| json!({
        "goal_info": {"goal_id": {"uuid": g}, "stamp": {"sec": 0, "nanosec": 0}},
        "status": s,
    })).collect::<Vec<_>>()})
}

#[tokio::test(start_paused = true)]
async fn topic_rules_record_messages_to_their_sinks() {
    let r = rig(vec![
        json!({"name": "states", "match": "fleet/*/state", "kind": "topic", "sinks": ["console"]}),
        json!({"name": "doors", "match": "doors", "kind": "topic", "sinks": ["database", "console"]}),
    ]);
    for t in ["/fleet/r1/state", "/fleet/r2/state", "/doors", "/other"] {
        r.bus.add_publisher(t, "std_msgs/msg/String", "RIHS01_1");
    }
    r.tick().await;
    let names: Vec<String> = r.states().into_iter().map(|s| s.0).collect();
    assert_eq!(names, ["/doors", "/fleet/r1/state", "/fleet/r2/state"]);
    r.bus.publish("/fleet/r1/state", json!({"data": "a"}));
    r.bus.publish("/doors", json!({"data": "open"}));
    r.bus.publish("/other", json!({"data": "ignored"}));
    settle().await;
    let console = r.console.parts();
    assert_eq!(console.len(), 2, "{console:?}");
    assert!(console.contains(&(
        "/fleet/r1/state".into(),
        Part::Message,
        None,
        Some(json!({"data": "a"}))
    )));
    assert_eq!(
        r.db.parts(),
        [(
            "/doors".into(),
            Part::Message,
            None,
            Some(json!({"data": "open"}))
        )],
        "only the database rule reaches the database"
    );
    let recent = r.rec.recent(10);
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].name, "/doors", "newest first");
    assert_eq!(recent[0].rule, "doors");
}

#[tokio::test(start_paused = true)]
async fn streams_stop_when_targets_or_rules_go_away() {
    let r = rig(vec![
        json!({"name": "all", "match": "**", "kind": "topic", "sinks": ["console"]}),
    ]);
    r.bus.add_publisher("/a", "std_msgs/msg/String", "RIHS01_1");
    r.bus.add_publisher("/b", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    assert_eq!(r.states().len(), 2);
    assert_eq!(r.bus.live_subscriptions("/a"), 1);
    r.bus.remove_topic("/a");
    r.tick().await;
    assert_eq!(r.states().len(), 1);
    r.rec.set_rules(vec![]).unwrap();
    settle().await;
    assert!(r.states().is_empty());
    r.bus.publish("/b", json!({"data": "late"}));
    settle().await;
    assert_eq!(r.console.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn topic_and_service_rules_ignore_action_internals() {
    let r = rig(vec![
        json!({"name": "t", "match": "**", "kind": "topic", "sinks": ["console"]}),
        json!({"name": "s", "match": "**", "kind": "service", "sinks": ["console"]}),
    ]);
    add_action(&r.bus, ACTION);
    r.tick().await;
    assert!(r.states().is_empty(), "{:?}", r.states());
}

#[tokio::test(start_paused = true)]
async fn rate_limits_drop_and_count_extra_messages() {
    let r = rig(vec![json!({
        "name": "slow", "match": "chatter", "kind": "topic", "sinks": ["console"], "max_rate_hz": 1.0
    })]);
    r.bus
        .add_publisher("/chatter", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    for i in 0..5 {
        r.bus.publish("/chatter", json!({"data": i.to_string()}));
        settle().await;
    }
    assert_eq!(r.console.len(), 1);
    tokio::time::advance(Duration::from_millis(999)).await;
    r.bus.publish("/chatter", json!({"data": "too soon"}));
    settle().await;
    assert_eq!(r.console.len(), 1, "still inside the interval");
    tokio::time::advance(Duration::from_millis(1)).await;
    r.bus.publish("/chatter", json!({"data": "later"}));
    settle().await;
    assert_eq!(r.console.len(), 2);
    let v = r.rec.view();
    assert_eq!((v.streams[0].recorded, v.streams[0].dropped), (2, 5));
}

#[tokio::test(start_paused = true)]
async fn service_rules_record_requests_from_a_tap() {
    let r = rig(vec![
        json!({"name": "s", "match": "add_two_ints", "kind": "service", "sinks": ["console"]}),
    ]);
    r.bus
        .serve("/add_two_ints", "example_interfaces/srv/AddTwoInts", |_| {
            Ok(json!({}))
        });
    r.tick().await;
    assert_eq!(r.bus.live_taps("/add_two_ints"), 1);
    assert_eq!(r.states()[0].2, StreamState::Active);
    r.bus.request("/add_two_ints", json!({"a": "1", "b": "2"}));
    settle().await;
    let recs = r.rec.recent(1);
    assert_eq!(
        (
            recs[0].part,
            recs[0].type_name.as_str(),
            recs[0].data.clone()
        ),
        (
            Part::Request,
            "example_interfaces/srv/AddTwoInts",
            Some(json!({"a": "1", "b": "2"}))
        )
    );
}

#[tokio::test(start_paused = true)]
async fn action_rules_record_goals_feedback_status_and_results() {
    let r = rig(vec![
        json!({"name": "nav", "match": "/*/navigate_to_pose", "kind": "action", "sinks": ["console"]}),
    ]);
    add_action(&r.bus, ACTION);
    add_action(&r.bus, "/robot0/inner/navigate_to_pose"); // not matched by `/*/…`
    r.tick().await;
    let sources: Vec<(String, Source)> = r.states().into_iter().map(|s| (s.0, s.1)).collect();
    assert_eq!(
        sources,
        [
            (ACTION.into(), Source::Goal),
            (ACTION.into(), Source::Feedback),
            (ACTION.into(), Source::Status)
        ]
    );
    let feedback = format!("{ACTION}/_action/feedback");
    let status = format!("{ACTION}/_action/status");
    // An empty status snapshot first (what a fresh server publishes).
    r.bus.publish(&status, statuses(&[]));
    r.bus.request(
        &format!("{ACTION}/_action/send_goal"),
        json!({"goal_id": {"uuid": GOAL_A}, "goal": {"pose": {"x": 1}}}),
    );
    settle().await;
    r.bus.publish(&status, statuses(&[(GOAL_A, 2)]));
    r.bus.publish(
        &feedback,
        json!({"goal_id": {"uuid": GOAL_A}, "feedback": {"distance_remaining": 3.5}}),
    );
    settle().await;
    r.bus.publish(&status, statuses(&[(GOAL_A, 4)]));
    settle().await;
    let id = Some(UUID_A.to_owned());
    assert_eq!(
        r.console.parts(),
        [
            (
                ACTION.into(),
                Part::Goal,
                id.clone(),
                Some(json!({"pose": {"x": 1}}))
            ),
            (
                ACTION.into(),
                Part::Status,
                id.clone(),
                Some(json!({"status": "executing", "code": 2}))
            ),
            (
                ACTION.into(),
                Part::Feedback,
                id.clone(),
                Some(json!({"distance_remaining": 3.5}))
            ),
            (
                ACTION.into(),
                Part::Status,
                id.clone(),
                Some(json!({"status": "succeeded", "code": 4}))
            ),
            (
                ACTION.into(),
                Part::Result,
                id,
                Some(
                    json!({"status": "succeeded", "result": {"error_code": 0, "asked": {"uuid": GOAL_A}}})
                )
            ),
        ]
    );
    let types: Vec<String> = r
        .rec
        .recent(10)
        .iter()
        .map(|r| r.type_name.clone())
        .collect();
    assert!(
        types.iter().all(|t| t == "nav2_msgs/action/NavigateToPose"),
        "action records carry the action type: {types:?}"
    );
    assert_eq!(
        r.bus
            .calls()
            .iter()
            .filter(|(s, _)| s.ends_with("get_result"))
            .count(),
        1,
        "one result fetch per finished goal; repeated snapshots don't refetch"
    );
    r.bus.publish(&status, statuses(&[(GOAL_A, 4)]));
    settle().await;
    assert_eq!(r.console.len(), 5);
}

#[tokio::test(start_paused = true)]
async fn goals_already_finished_when_recording_starts_are_not_recorded() {
    let r = rig(vec![
        json!({"name": "nav", "match": "/*/navigate_to_pose", "kind": "action", "parts": ["status", "result"], "sinks": ["console"]}),
    ]);
    add_action(&r.bus, ACTION);
    r.tick().await;
    let status = format!("{ACTION}/_action/status");
    // Transient-local history: one old finished goal, one still running.
    r.bus
        .publish(&status, statuses(&[(GOAL_A, 4), (GOAL_B, 2)]));
    settle().await;
    let parts = r.console.parts();
    assert_eq!(parts.len(), 1, "{parts:?}");
    assert_eq!(parts[0].1, Part::Status);
    assert_eq!(parts[0].3, Some(json!({"status": "executing", "code": 2})));
    assert!(
        r.bus.calls().is_empty(),
        "no result fetched for the old goal"
    );
}

#[tokio::test(start_paused = true)]
async fn action_parts_can_be_chosen() {
    let r = rig(vec![
        json!({"name": "results", "match": "**", "kind": "action", "parts": ["result"], "sinks": ["database"]}),
    ]);
    add_action(&r.bus, ACTION);
    r.tick().await;
    let sources: Vec<Source> = r.states().into_iter().map(|s| s.1).collect();
    assert_eq!(
        sources,
        [Source::Status],
        "results come from status changes"
    );
    let status = format!("{ACTION}/_action/status");
    r.bus.publish(&status, statuses(&[]));
    settle().await;
    r.bus.publish(&status, statuses(&[(GOAL_A, 2)]));
    r.bus.publish(&status, statuses(&[(GOAL_A, 6)]));
    settle().await;
    let parts: Vec<Part> = r.db.parts().into_iter().map(|p| p.1).collect();
    assert_eq!(
        parts,
        [Part::Result],
        "statuses are not recorded unless asked for"
    );
    assert_eq!(r.console.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn failed_streams_are_retried_after_the_delay() {
    let r = rig_with(
        vec![json!({"name": "s", "match": "svc", "kind": "service", "sinks": ["console"]})],
        RecorderConfig {
            retry_after: Duration::from_secs(5),
            ..RecorderConfig::default()
        },
    );
    r.bus.serve("/svc", "x/srv/S", |_| Ok(json!({})));
    r.bus.fail_subscribe(
        "/svc",
        BusError::Schema {
            topic: "/svc".into(),
            reason: "no type description".into(),
        },
    );
    r.tick().await;
    assert!(
        matches!(&r.states()[0].2, StreamState::Failed { error } if error.contains("no type description")),
        "{:?}",
        r.states()
    );
    r.bus.clear_failure("/svc");
    r.tick().await;
    assert!(
        matches!(r.states()[0].2, StreamState::Failed { .. }),
        "not before the delay"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    r.tick().await;
    assert_eq!(r.states()[0].2, StreamState::Active);
}

#[tokio::test(start_paused = true)]
async fn streams_are_capped() {
    let r = rig_with(
        vec![json!({"name": "all", "match": "**", "kind": "topic", "sinks": ["console"]})],
        RecorderConfig {
            max_streams: 2,
            ..RecorderConfig::default()
        },
    );
    for t in ["/a", "/b", "/c"] {
        r.bus.add_publisher(t, "std_msgs/msg/String", "RIHS01_1");
    }
    r.tick().await;
    let v = r.rec.view();
    assert_eq!((v.streams.len(), v.skipped), (2, 1));
}

#[tokio::test(start_paused = true)]
async fn invalid_rule_changes_leave_the_rules_alone() {
    let r = rig(vec![
        json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["console"]}),
    ]);
    let err = r
        .rec
        .set_rules(vec![rule(
            json!({"name": "b", "match": "x", "kind": "topic", "sinks": []}),
        )])
        .unwrap_err();
    assert!(err.0.contains("no sinks"));
    assert_eq!(r.rec.rules()[0].name, "a");
}

#[tokio::test(start_paused = true)]
async fn changing_a_rule_restarts_its_streams_with_the_new_settings() {
    let r = rig(vec![
        json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["console"]}),
    ]);
    r.bus.add_publisher("/x", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    r.rec
        .set_rules(vec![rule(
            json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["database"]}),
        )])
        .unwrap();
    settle().await;
    r.bus.publish("/x", json!({"data": "1"}));
    settle().await;
    assert_eq!((r.console.len(), r.db.len()), (0, 1));
    assert_eq!(r.states().len(), 1, "the old stream is gone");
}

#[tokio::test(start_paused = true)]
async fn database_rules_need_a_database() {
    let bus = FakeBus::new();
    let hub = Hub::new(bus, HubConfig::default());
    let err = Recorder::new(
        hub,
        RecorderConfig::default(),
        Sinks {
            console: Arc::new(Collect::default()),
            database: None,
        },
        vec![rule(
            json!({"name": "a", "match": "x", "kind": "topic", "sinks": ["database"]}),
        )],
    )
    .unwrap_err();
    assert!(err.0.contains("--database-url"), "{err}");
}

#[test]
fn goal_tracker_reports_changes_and_forgets_expired_goals() {
    let mut t = GoalTracker::default();
    assert!(t.update(&statuses(&[(GOAL_A, 6)])).is_empty(), "baseline");
    let c = t.update(&statuses(&[(GOAL_A, 6), (GOAL_B, 1)]));
    assert_eq!(c.iter().map(|c| c.code).collect::<Vec<_>>(), [1]);
    let c = t.update(&statuses(&[(GOAL_B, 5)]));
    assert_eq!((c.len(), c[0].code, c[0].terminal()), (1, 5, true));
    assert!(
        !t.known.contains_key(UUID_A),
        "goal A expired from the server"
    );
    assert!(t.update(&json!({"garbage": true})).is_empty());
    assert!(t.known.is_empty());
}

/// Rig with an action at [`ACTION`] whose status stream is open, after an
/// empty first snapshot.
async fn action_rig(rule: Value) -> Rig {
    let r = rig(vec![rule]);
    add_action(&r.bus, ACTION);
    r.tick().await;
    r.bus
        .publish(&format!("{ACTION}/_action/status"), statuses(&[]));
    settle().await;
    r
}

#[tokio::test(start_paused = true)]
async fn goals_first_seen_finished_after_the_baseline_are_recorded() {
    // The hub's trailing-edge cap can merge a fast goal's snapshots, so its
    // first sighting is already terminal.
    let r = action_rig(json!({"name": "n", "match": "**", "kind": "action", "parts": ["status", "result"], "sinks": ["console"]})).await;
    r.bus.publish(
        &format!("{ACTION}/_action/status"),
        statuses(&[(GOAL_A, 4)]),
    );
    settle().await;
    let parts: Vec<Part> = r.console.parts().into_iter().map(|p| p.1).collect();
    assert_eq!(parts, [Part::Status, Part::Result]);
}

#[tokio::test(start_paused = true)]
async fn canceling_is_not_finished_but_canceled_is() {
    let r = action_rig(json!({"name": "n", "match": "**", "kind": "action", "parts": ["result"], "sinks": ["console"]})).await;
    let status = format!("{ACTION}/_action/status");
    r.bus.publish(&status, statuses(&[(GOAL_A, 3)]));
    settle().await;
    assert!(r.bus.calls().is_empty(), "no result while canceling");
    r.bus.publish(&status, statuses(&[(GOAL_A, 5)]));
    settle().await;
    assert_eq!(r.bus.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_first_snapshot_of_an_already_open_status_topic_is_the_baseline() {
    let r = rig(vec![]);
    add_action(&r.bus, ACTION);
    let status = format!("{ACTION}/_action/status");
    // Someone else (an API client) already watches the status topic.
    let _other = r.hub.watch(&status).unwrap();
    for _ in 0..500 {
        if r.bus.live_subscriptions(&status) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    r.bus.publish(&status, statuses(&[(GOAL_A, 4)]));
    settle().await;
    r.rec
        .set_rules(vec![rule(json!({"name": "n", "match": "**", "kind": "action", "parts": ["status"], "sinks": ["console"]}))])
        .unwrap();
    r.hub.tick();
    r.rec.tick();
    settle().await;
    r.bus
        .publish(&status, statuses(&[(GOAL_A, 4), (GOAL_B, 4)]));
    settle().await;
    let ids: Vec<Option<String>> = r.console.parts().into_iter().map(|p| p.2).collect();
    assert_eq!(ids.len(), 1, "only goal B is new: {ids:?}");
    assert_ne!(ids[0].as_deref(), Some(UUID_A));
}

#[tokio::test(start_paused = true)]
async fn goals_are_never_throttled_but_requests_and_feedback_are() {
    let r = rig(vec![
        json!({"name": "g", "match": "**", "kind": "action", "parts": ["goal", "feedback"], "sinks": ["console"], "max_rate_hz": 0.01}),
        json!({"name": "s", "match": "svc", "kind": "service", "sinks": ["database"], "max_rate_hz": 0.01}),
    ]);
    add_action(&r.bus, ACTION);
    r.bus.serve("/svc", "x/srv/S", |_| Ok(json!({})));
    r.tick().await;
    for i in 0..3 {
        r.bus.request(
            &format!("{ACTION}/_action/send_goal"),
            json!({"goal_id": {"uuid": GOAL_A}, "goal": {"i": i}}),
        );
        r.bus.publish(
            &format!("{ACTION}/_action/feedback"),
            json!({"goal_id": {"uuid": GOAL_A}, "feedback": {"i": i}}),
        );
        r.bus.request("/svc", json!({"i": i}));
        settle().await;
    }
    let parts: Vec<Part> = r.console.parts().into_iter().map(|p| p.1).collect();
    assert_eq!(parts.iter().filter(|p| **p == Part::Goal).count(), 3);
    assert_eq!(parts.iter().filter(|p| **p == Part::Feedback).count(), 1);
    assert_eq!(r.db.len(), 1, "requests are throttled");
}

#[tokio::test(start_paused = true)]
async fn oversized_tapped_requests_are_recorded_without_data() {
    let r = rig_full(
        vec![json!({"name": "s", "match": "svc", "kind": "service", "sinks": ["console"]})],
        RecorderConfig::default(),
        HubConfig {
            max_payload_bytes: 100,
            ..HubConfig::default()
        },
    );
    r.bus.serve("/svc", "x/srv/S", |_| Ok(json!({})));
    r.tick().await;
    r.bus
        .request_raw("/svc", 101, || panic!("must not be decoded"));
    r.bus.request("/svc", json!({"small": true}));
    settle().await;
    let data: Vec<Option<Value>> = r.console.parts().into_iter().map(|p| p.3).collect();
    assert_eq!(data, [None, Some(json!({"small": true}))]);
}

#[tokio::test(start_paused = true)]
async fn drops_are_counted_per_reason() {
    let r = rig_full(
        vec![
            json!({"name": "t", "match": "fast", "kind": "topic", "sinks": ["console"]}),
            json!({"name": "s", "match": "svc", "kind": "service", "sinks": ["console"]}),
            json!({"name": "a", "match": "**", "kind": "action", "parts": ["result"], "sinks": ["console"]}),
        ],
        RecorderConfig::default(),
        HubConfig {
            max_rate_hz: None,
            watcher_buffer: 2,
            ..HubConfig::default()
        },
    );
    r.bus
        .add_publisher("/fast", "std_msgs/msg/String", "RIHS01_1");
    r.bus.serve("/svc", "x/srv/S", |_| Ok(json!({})));
    // An action without a get_result server: results can't be fetched.
    r.bus.serve(
        &format!("{ACTION}/_action/send_goal"),
        "p/action/A_SendGoal",
        |_| Ok(json!({})),
    );
    r.bus.add_publisher(
        &format!("{ACTION}/_action/status"),
        "action_msgs/msg/GoalStatusArray",
        "RIHS01_s",
    );
    r.tick().await;
    for i in 0..10 {
        r.bus.publish("/fast", json!({"data": i.to_string()}));
    }
    r.bus
        .request_raw("/svc", 5, || Err(crate::bus::DecodeError("bad".into())));
    let status = format!("{ACTION}/_action/status");
    r.bus.publish(&status, statuses(&[]));
    settle().await;
    r.bus.publish(&status, statuses(&[(GOAL_A, 6)]));
    settle().await;
    let dropped = |name: &str| {
        r.rec
            .view()
            .streams
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.dropped)
            .unwrap()
    };
    assert!(dropped("/fast") > 0, "lagged");
    assert_eq!(dropped("/svc"), 1, "decode error");
    assert_eq!(dropped(ACTION), 1, "result unavailable");
}

#[tokio::test(start_paused = true)]
async fn only_live_valid_targets_are_matched() {
    let r = rig(vec![
        json!({"name": "t", "match": "**", "kind": "topic", "sinks": ["console"]}),
        json!({"name": "a", "match": "**", "kind": "action", "sinks": ["console"]}),
    ]);
    // A topic with only a subscriber, an action with only a client, and a
    // name that isn't a valid ROS name (it could carry key wildcards).
    let mut g = crate::bus::Bus::graph(&*r.bus);
    let ep = |kind, name: &str, ty: &str| crate::graph::Endpoint {
        kind,
        name: name.into(),
        type_name: ty.into(),
        type_hash: "RIHS01_1".into(),
        node: Some("/n".into()),
        qos: crate::graph::Qos {
            reliable: true,
            transient_local: false,
            depth: Some(1),
        },
    };
    use crate::graph::EndpointKind as K;
    g.endpoints.push(ep(
        K::Subscription,
        "/only_subscribed",
        "std_msgs/msg/String",
    ));
    g.endpoints.push(ep(
        K::ServiceClient,
        "/a/_action/send_goal",
        "p/action/A_SendGoal",
    ));
    g.endpoints
        .push(ep(K::Publisher, "/bad*name", "std_msgs/msg/String"));
    g.endpoints.push(ep(
        K::ServiceServer,
        "/b$*/_action/send_goal",
        "p/action/A_SendGoal",
    ));
    r.bus.set_graph(g);
    r.tick().await;
    assert!(r.states().is_empty(), "{:?}", r.states());
}

#[tokio::test(start_paused = true)]
async fn recent_records_are_capped() {
    let r = rig_with(
        vec![json!({"name": "t", "match": "x", "kind": "topic", "sinks": ["console"]})],
        RecorderConfig {
            recent: 2,
            ..RecorderConfig::default()
        },
    );
    r.bus.add_publisher("/x", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    for i in 0..5 {
        r.bus.publish("/x", json!({"data": i.to_string()}));
        settle().await;
    }
    let recent = r.rec.recent(10);
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].data, Some(json!({"data": "4"})));
}

#[tokio::test(start_paused = true)]
async fn close_all_stops_every_stream_for_good() {
    let r = rig(vec![
        json!({"name": "t", "match": "x", "kind": "topic", "sinks": ["console"]}),
    ]);
    r.bus.add_publisher("/x", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    assert_eq!(r.bus.live_subscriptions("/x"), 1);
    r.rec.close_all();
    r.tick().await;
    assert!(r.states().is_empty(), "a later tick starts nothing");
    r.bus.publish("/x", json!({"data": "late"}));
    settle().await;
    assert_eq!(r.console.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_closed_topic_ends_the_stream() {
    let r = rig(vec![
        json!({"name": "t", "match": "x", "kind": "topic", "sinks": ["console"]}),
    ]);
    r.bus.add_publisher("/x", "std_msgs/msg/String", "RIHS01_1");
    r.tick().await;
    assert_eq!(r.states()[0].2, StreamState::Active);
    r.hub.close_all();
    settle().await;
    assert!(
        matches!(&r.states()[0].2, StreamState::Failed { error } if error.contains("closed")),
        "{:?}",
        r.states()
    );
}

#[test]
fn the_goal_cap_still_tracks_known_goals() {
    let mut t = GoalTracker::default();
    let many: Vec<(String, i64)> = (0..MAX_TRACKED_GOALS)
        .map(|i| {
            use base64::Engine;
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&(i as u64).to_le_bytes());
            (base64::engine::general_purpose::STANDARD.encode(b), 2)
        })
        .collect();
    let snap = |extra: &[(&str, i64)], first_code| {
        let mut v: Vec<(&str, i64)> = many.iter().map(|(g, _)| (g.as_str(), 2)).collect();
        v[0].1 = first_code;
        v.extend_from_slice(extra);
        statuses(&v)
    };
    assert_eq!(t.update(&snap(&[], 2)).len(), MAX_TRACKED_GOALS);
    let c = t.update(&snap(&[(GOAL_B, 1)], 4));
    assert_eq!(
        c.len(),
        1,
        "a known goal's change is still seen; a new one is not tracked"
    );
    assert_eq!(c[0].code, 4);
}
