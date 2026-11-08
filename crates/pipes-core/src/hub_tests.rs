//! Hub lifecycle tests against [`FakeBus`], with paused tokio time so idle and
//! retry timeouts are deterministic.

use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use super::*;
use crate::{
    bus::{BusError, DecodeError},
    fake::FakeBus,
    pattern::TopicPattern,
};

const TY: &str = "std_msgs/msg/String";
const H1: &str = "RIHS01_1111";
const H2: &str = "RIHS01_2222";

fn cfg() -> HubConfig {
    HubConfig {
        patterns: vec![],
        tick_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(30),
        retry_after: Duration::from_secs(5),
        max_retry_after: Duration::from_secs(40),
        max_rate_hz: None,
        max_payload_bytes: 1024,
        watcher_buffer: 16,
    }
}

fn hub(bus: &Arc<FakeBus>, cfg: HubConfig) -> Hub {
    Hub::new(bus.clone(), cfg)
}

/// Let spawned tasks run until `cond` holds. With paused time each sleep
/// auto-advances the clock by 1ms, so keep the budget small relative to the
/// timeouts under test.
async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for: {what}");
}

/// Let every runnable task run to its next await point.
async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

async fn next_sample(w: &mut Watch) -> Arc<Sample> {
    loop {
        match tokio::time::timeout(Duration::from_millis(500), w.events.recv())
            .await
            .expect("no event within 500ms")
        {
            Ok(TopicEvent::Sample(s)) => return s,
            Ok(TopicEvent::Status(_)) => {}
            Err(e) => panic!("recv error: {e}"),
        }
    }
}

fn status(hub: &Hub, topic: &str) -> Option<TopicStatus> {
    hub.topic(topic).map(|t| t.status)
}

#[tokio::test(start_paused = true)]
async fn watch_opens_on_demand_and_delivers_samples() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());

    let mut w = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    assert_eq!(bus.live_subscriptions("/chatter"), 1);

    assert_eq!(bus.publish("/chatter", json!({"data": "hi"})), 1);
    let s = next_sample(&mut w).await;
    assert_eq!(s.data, Some(json!({"data": "hi"})));
    assert_eq!(
        (s.topic.as_str(), s.type_name.as_str(), s.type_hash.as_str()),
        ("/chatter", TY, H1)
    );
    assert_eq!(hub.latest("/chatter").unwrap().data, s.data);
    assert_eq!(hub.schema("/chatter").unwrap().type_name, TY);
    let v = hub.topic("/chatter").unwrap();
    assert_eq!(
        (v.stats.received, v.stats.delivered, v.watchers, v.standing),
        (1, 1, 1, false)
    );
}

#[tokio::test(start_paused = true)]
async fn two_watchers_share_one_subscription() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    let mut a = hub.watch("/chatter").unwrap();
    let mut b = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    assert_eq!(bus.subscribe_calls("/chatter"), 1);
    bus.publish("/chatter", json!({"data": "x"}));
    assert_eq!(
        next_sample(&mut a).await.data,
        next_sample(&mut b).await.data
    );
}

#[tokio::test(start_paused = true)]
async fn unwatched_topic_is_reaped_only_after_idle_timeout() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    let w = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    drop(w);

    tokio::time::advance(Duration::from_secs(29)).await;
    hub.tick();
    assert!(
        hub.topic("/chatter").is_some(),
        "reaped before idle_timeout"
    );
    assert_eq!(bus.live_subscriptions("/chatter"), 1);

    tokio::time::advance(Duration::from_secs(2)).await;
    hub.tick();
    assert!(
        hub.topic("/chatter").is_none(),
        "not reaped after idle_timeout"
    );
    until("bus unsubscribed", || {
        bus.live_subscriptions("/chatter") == 0
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn rewatching_an_idle_topic_cancels_reaping() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    drop(hub.watch("/chatter").unwrap());
    tokio::time::advance(Duration::from_secs(20)).await;
    let _w = hub.watch("/chatter").unwrap();
    tokio::time::advance(Duration::from_secs(20)).await;
    hub.tick();
    assert!(hub.topic("/chatter").is_some());
    assert_eq!(
        bus.subscribe_calls("/chatter"),
        1,
        "re-watch must reuse the entry"
    );
}

#[tokio::test(start_paused = true)]
async fn standing_patterns_follow_publishers() {
    let bus = FakeBus::new();
    let hub = hub(
        &bus,
        HubConfig {
            patterns: vec![TopicPattern::parse("fleet/*/state").unwrap()],
            ..cfg()
        },
    );
    bus.add_publisher("/fleet/r1/state", TY, H1);
    bus.add_publisher("/fleet/r1/other", TY, H1);
    bus.add_publisher("/unrelated", TY, H1);
    hub.tick();
    until("standing active", || {
        status(&hub, "/fleet/r1/state") == Some(TopicStatus::Active)
    })
    .await;
    let names: Vec<String> = hub.topics().into_iter().map(|t| t.name).collect();
    assert_eq!(
        names,
        vec!["/fleet/r1/state".to_owned()],
        "only the matching topic is subscribed"
    );
    assert!(hub.topic("/fleet/r1/state").unwrap().standing);

    // Publisher leaves: no longer standing, reaped after the idle timeout.
    bus.remove_topic("/fleet/r1/state");
    hub.tick();
    assert!(!hub.topic("/fleet/r1/state").unwrap().standing);
    tokio::time::advance(Duration::from_secs(31)).await;
    hub.tick();
    assert!(hub.topic("/fleet/r1/state").is_none());
}

#[tokio::test(start_paused = true)]
async fn failed_open_is_retried_while_watched_and_reaped_when_not() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    bus.fail_subscribe(
        "/chatter",
        BusError::Schema {
            topic: "/chatter".into(),
            reason: "no type description service".into(),
        },
    );
    let hub = hub(&bus, cfg());
    let w = hub.watch("/chatter").unwrap();
    until("failed", || {
        matches!(status(&hub, "/chatter"), Some(TopicStatus::Failed { .. }))
    })
    .await;
    assert_eq!(w.status(), status(&hub, "/chatter").unwrap());

    tokio::time::advance(Duration::from_secs(4)).await;
    hub.tick();
    settle().await;
    assert_eq!(
        bus.subscribe_calls("/chatter"),
        1,
        "retried before retry_after"
    );

    bus.clear_failure("/chatter");
    tokio::time::advance(Duration::from_secs(2)).await;
    hub.tick();
    until("recovered", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    assert_eq!(bus.subscribe_calls("/chatter"), 2);

    // Now fail again with nobody watching: reaped at the next retry point.
    drop(w);
    bus.fail_subscribe("/chatter", BusError::NoPublisher("/chatter".into()));
    bus.end_subscriptions("/chatter");
    until("failed again", || {
        matches!(status(&hub, "/chatter"), Some(TopicStatus::Failed { .. }))
    })
    .await;
    tokio::time::advance(Duration::from_secs(6)).await;
    hub.tick();
    settle().await;
    assert!(hub.topic("/chatter").is_none());
    assert_eq!(
        bus.subscribe_calls("/chatter"),
        2,
        "an unwatched failed topic must not be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn resubscribes_when_publishers_change_type_hash() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    let mut w = hub.watch("/chatter").unwrap();
    hub.tick();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;

    // Same hash still present: no churn.
    hub.tick();
    assert_eq!(bus.subscribe_calls("/chatter"), 1);

    bus.remove_topic("/chatter");
    bus.add_publisher("/chatter", TY, H2);
    hub.tick();
    until("resubscribed", || {
        hub.topic("/chatter")
            .and_then(|t| t.subscription)
            .is_some_and(|s| s.type_hash == H2)
    })
    .await;
    assert_eq!(bus.subscribe_calls("/chatter"), 2);
    until("old subscription dropped", || {
        bus.live_subscriptions("/chatter") == 1
    })
    .await;

    // The watcher keeps receiving across the resubscribe.
    bus.publish("/chatter", json!({"data": "after"}));
    let s = next_sample(&mut w).await;
    assert_eq!(
        (s.type_hash.as_str(), s.data.clone()),
        (H2, Some(json!({"data": "after"})))
    );
}

#[tokio::test(start_paused = true)]
async fn rate_cap_keeps_the_newest_sample_of_a_burst() {
    let bus = FakeBus::new();
    bus.add_publisher("/fast", TY, H1);
    let hub = hub(
        &bus,
        HubConfig {
            max_rate_hz: Some(10.0),
            ..cfg()
        },
    );
    let mut w = hub.watch("/fast").unwrap();
    until("active", || {
        status(&hub, "/fast") == Some(TopicStatus::Active)
    })
    .await;

    for i in 0..5 {
        bus.publish("/fast", json!({"data": i}));
    }
    until("all received", || {
        hub.topic("/fast").unwrap().stats.received == 5
    })
    .await;
    // The first goes out at once; 1..=3 are replaced by newer ones and dropped;
    // 4 is held back for the end of the interval.
    let st = hub.topic("/fast").unwrap().stats;
    assert_eq!((st.delivered, st.dropped_rate_limited), (1, 3));
    assert_eq!(next_sample(&mut w).await.data, Some(json!({"data": 0})));
    assert_eq!(hub.latest("/fast").unwrap().data, Some(json!({"data": 0})));

    // No further publications: the held-back newest sample still arrives.
    assert_eq!(next_sample(&mut w).await.data, Some(json!({"data": 4})));
    assert_eq!(hub.latest("/fast").unwrap().data, Some(json!({"data": 4})));
    assert_eq!(hub.topic("/fast").unwrap().stats.delivered, 2);
}

#[tokio::test(start_paused = true)]
async fn rate_cap_does_not_delay_samples_that_respect_it() {
    let bus = FakeBus::new();
    bus.add_publisher("/slow", TY, H1);
    let hub = hub(
        &bus,
        HubConfig {
            max_rate_hz: Some(10.0),
            ..cfg()
        },
    );
    let mut w = hub.watch("/slow").unwrap();
    until("active", || {
        status(&hub, "/slow") == Some(TopicStatus::Active)
    })
    .await;
    for i in 0..3 {
        bus.publish("/slow", json!({"data": i}));
        assert_eq!(next_sample(&mut w).await.data, Some(json!({"data": i})));
        tokio::time::advance(Duration::from_millis(150)).await;
    }
    assert_eq!(hub.topic("/slow").unwrap().stats.dropped_rate_limited, 0);
}

#[test]
fn min_interval_rejects_unrepresentable_rates() {
    let with = |hz| HubConfig {
        max_rate_hz: hz,
        ..HubConfig::default()
    };
    assert_eq!(
        with(Some(10.0)).min_interval(),
        Some(Duration::from_millis(100))
    );
    for bad in [Some(0.0), Some(-1.0), Some(f64::NAN), Some(1e-300), None] {
        assert_eq!(with(bad).min_interval(), None, "{bad:?}");
    }
}

#[test]
fn retry_delay_doubles_up_to_the_cap() {
    let c = cfg();
    let secs: Vec<u64> = (1..=6).map(|n| c.retry_delay(n).as_secs()).collect();
    assert_eq!(secs, vec![5, 10, 20, 40, 40, 40]);
}

#[tokio::test(start_paused = true)]
async fn oversized_payload_is_delivered_without_data_and_never_decoded() {
    let bus = FakeBus::new();
    bus.add_publisher("/map", TY, H1);
    let hub = hub(&bus, cfg());
    let mut w = hub.watch("/map").unwrap();
    until("active", || {
        status(&hub, "/map") == Some(TopicStatus::Active)
    })
    .await;

    bus.publish_raw("/map", 4096, || {
        panic!("decoder must not run for an oversized payload")
    });
    let s = next_sample(&mut w).await;
    assert_eq!(
        (s.data.clone(), s.omitted, s.size_bytes),
        (None, Some(OmittedReason::TooLarge), 4096)
    );
    assert_eq!(hub.topic("/map").unwrap().stats.omitted_too_large, 1);
}

#[tokio::test(start_paused = true)]
async fn decode_errors_are_counted_not_delivered() {
    let bus = FakeBus::new();
    bus.add_publisher("/bad", TY, H1);
    let hub = hub(&bus, cfg());
    let mut w = hub.watch("/bad").unwrap();
    until("active", || {
        status(&hub, "/bad") == Some(TopicStatus::Active)
    })
    .await;

    bus.publish_raw("/bad", 3, || Err(DecodeError("truncated CDR".into())));
    bus.publish("/bad", json!({"data": "good"}));
    assert_eq!(
        next_sample(&mut w).await.data,
        Some(json!({"data": "good"}))
    );
    let st = hub.topic("/bad").unwrap().stats;
    assert_eq!((st.received, st.delivered, st.decode_errors), (2, 1, 1));
    assert!(st.last_decode_error.unwrap().contains("truncated CDR"));
}

#[tokio::test(start_paused = true)]
async fn slow_watcher_lags_instead_of_blocking() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(
        &bus,
        HubConfig {
            watcher_buffer: 2,
            ..cfg()
        },
    );
    let mut slow = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    for i in 0..10 {
        bus.publish("/chatter", json!({"data": i}));
    }
    until("all delivered", || {
        hub.topic("/chatter").unwrap().stats.delivered == 10
    })
    .await;
    // The first receive after falling behind reports the lag, then resumes.
    assert!(matches!(
        slow.events.recv().await,
        Err(RecvError::Lagged(_))
    ));
    assert!(slow.events.recv().await.is_ok());
    assert_eq!(
        hub.latest("/chatter").unwrap().data,
        Some(json!({"data": 9}))
    );
}

#[tokio::test(start_paused = true)]
async fn rejects_invalid_topic_names() {
    let bus = FakeBus::new();
    let hub = hub(&bus, cfg());
    for bad in ["", "/", "chatter", "/a//b", "/a/", "/a b", "/a/**"] {
        assert!(
            matches!(hub.watch(bad), Err(HubError::InvalidTopic(_))),
            "accepted {bad:?}"
        );
    }
    assert!(hub.watch("/ok/_hidden/Name2").is_ok());
}

#[tokio::test(start_paused = true)]
async fn shutdown_closes_subscriptions_and_refuses_new_watches() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    let shutdown = CancellationToken::new();
    let loop_task = hub.spawn(shutdown.clone());
    let _w = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;

    shutdown.cancel();
    loop_task.await.unwrap();
    assert!(hub.topics().is_empty());
    until("bus unsubscribed", || {
        bus.live_subscriptions("/chatter") == 0
    })
    .await;
    assert_eq!(hub.watch("/chatter").unwrap_err(), HubError::ShuttingDown);
}

#[tokio::test(start_paused = true)]
async fn tick_publishes_the_normalized_graph() {
    let bus = FakeBus::new();
    bus.add_publisher("/b", TY, H1);
    bus.add_publisher("/a", TY, H1);
    let hub = hub(&bus, cfg());
    assert!(
        hub.graph().endpoints.is_empty(),
        "graph is empty before the first tick"
    );
    hub.tick();
    let names: Vec<_> = hub
        .graph()
        .endpoints
        .iter()
        .map(|e| e.name.clone())
        .collect();
    assert_eq!(names, vec!["/a", "/b"]);
}

#[tokio::test(start_paused = true)]
async fn payload_exactly_at_the_cap_is_decoded() {
    let bus = FakeBus::new();
    bus.add_publisher("/edge", TY, H1);
    let hub = hub(&bus, cfg());
    let mut w = hub.watch("/edge").unwrap();
    until("active", || {
        status(&hub, "/edge") == Some(TopicStatus::Active)
    })
    .await;
    bus.publish_raw("/edge", 1024, || Ok(json!({"data": "fits"})));
    let s = next_sample(&mut w).await;
    assert_eq!(
        (s.data.clone(), s.omitted),
        (Some(json!({"data": "fits"})), None)
    );
}

#[tokio::test(start_paused = true)]
async fn failures_back_off_exponentially() {
    let bus = FakeBus::new();
    bus.add_publisher("/flaky", TY, H1);
    bus.fail_subscribe("/flaky", BusError::NoPublisher("/flaky".into()));
    let hub = hub(&bus, cfg());
    let _w = hub.watch("/flaky").unwrap();
    settle().await;
    // calls[i] = subscribe calls after i+1 seconds. Attempts at 0s, then 5s
    // later, then 10s after that (15s), then 20s after that (35s).
    let mut calls = Vec::new();
    for _ in 0..20 {
        tokio::time::advance(Duration::from_secs(1)).await;
        hub.tick();
        settle().await;
        calls.push(bus.subscribe_calls("/flaky"));
    }
    assert_eq!(calls[3], 1, "{calls:?}");
    assert_eq!(calls[5], 2, "{calls:?}");
    assert_eq!(calls[13], 2, "{calls:?}");
    assert_eq!(calls[15], 3, "{calls:?}");
    assert_eq!(calls[19], 3, "{calls:?}");
}

#[tokio::test(start_paused = true)]
async fn watchers_see_closed_when_the_entry_goes_away() {
    let bus = FakeBus::new();
    bus.add_publisher("/chatter", TY, H1);
    let hub = hub(&bus, cfg());
    let mut w = hub.watch("/chatter").unwrap();
    until("active", || {
        status(&hub, "/chatter") == Some(TopicStatus::Active)
    })
    .await;
    hub.close_all();
    let mut saw_closed = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(1), w.events.recv())
            .await
            .expect("the stream must end, not hang")
        {
            Ok(TopicEvent::Status(TopicStatus::Closed)) => saw_closed = true,
            Ok(_) | Err(RecvError::Lagged(_)) => {}
            Err(RecvError::Closed) => break,
        }
    }
    assert!(
        saw_closed,
        "a final Closed status must precede the end of the stream"
    );
    assert_eq!(w.status(), TopicStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn tick_after_close_all_opens_nothing() {
    let bus = FakeBus::new();
    bus.add_publisher("/fleet/r1/state", TY, H1);
    let hub = hub(
        &bus,
        HubConfig {
            patterns: vec![TopicPattern::parse("fleet/**").unwrap()],
            ..cfg()
        },
    );
    hub.close_all();
    hub.tick();
    settle().await;
    assert!(hub.topics().is_empty());
    assert_eq!(bus.subscribe_calls("/fleet/r1/state"), 0);
}
