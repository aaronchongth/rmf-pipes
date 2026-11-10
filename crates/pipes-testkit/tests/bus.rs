//! Hermetic L2 tests: typed hiroz publishers → in-process zenoh router →
//! `HirozBus` (and the hub on top of it). No ROS install needed.

#![allow(clippy::unwrap_used)]

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use hiroz::{
    Builder,
    qos::{QosDurability, QosHistory, QosProfile},
};
use hiroz_msgs::{
    builtin_interfaces::Time,
    geometry_msgs::{Twist, Vector3},
    sensor_msgs::Image,
    std_msgs::{Header, Int32, String as RosString},
};
use pipes_bus::HirozBus;
use pipes_core::{
    bus::{Bus, BusError},
    graph::EndpointKind,
    hub::{Hub, HubConfig, TopicStatus},
    pattern::TopicPattern,
};
use pipes_testkit::{TestRouter, bus_config, fixture_node, init_logging, wait_until};
use serde_json::json;

const T: Duration = Duration::from_secs(15);

fn connect(router: &TestRouter) -> Arc<HirozBus> {
    Arc::new(HirozBus::connect(bus_config(&router.endpoint())).unwrap())
}

/// Keep publishing `msg` every 50ms until the returned guard is dropped.
fn keep_publishing<M: hiroz::msg::ZMessage + Clone + Send + Sync + 'static>(
    publisher: hiroz::pubsub::ZPub<M, M::Serdes>,
    msg: M,
) -> tokio::task::JoinHandle<()>
where
    M::Serdes: Send + Sync,
{
    tokio::spawn(async move {
        loop {
            publisher.publish(&msg).unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_shows_remote_endpoints_with_type_and_hash_and_hides_our_own() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, talker) = fixture_node(&router.endpoint(), "talker");
    let _pub = talker.create_pub::<RosString>("/chatter").build().unwrap();
    let bus = connect(&router);

    let ep = wait_until(T, "the /chatter publisher in the graph", || {
        let g = bus.graph();
        async move {
            g.publishers_of("/chatter")
                .next()
                .cloned()
                .ok_or_else(|| format!("{:?}", g.endpoints))
        }
    })
    .await;
    assert_eq!(ep.type_name, "std_msgs/msg/String");
    assert!(
        ep.type_hash.starts_with("RIHS01_"),
        "hash: {}",
        ep.type_hash
    );
    assert_eq!(ep.node.as_deref(), Some("/talker"));
    let g = bus.graph();
    assert!(g.nodes.iter().any(|n| n.fqn == "/talker"));
    assert!(
        !g.nodes.iter().any(|n| n.name == "rmf_pipes"),
        "rmf-pipes' own node must be hidden: {:?}",
        g.nodes
    );

    // Our own subscription must not show up as a subscriber.
    let _sub = bus.subscribe("/chatter").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let subs = bus
        .graph()
        .endpoints
        .into_iter()
        .filter(|e| e.kind == EndpointKind::Subscription && e.name == "/chatter")
        .count();
    assert_eq!(subs, 0, "rmf-pipes' own subscription leaked into the graph");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_discovers_the_schema_and_decodes_nested_messages() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "teleop");
    let publisher = node.create_pub::<Twist>("/cmd_vel").build().unwrap();
    let twist = Twist {
        linear: Vector3 {
            x: 0.5,
            y: 0.0,
            z: 0.0,
        },
        angular: Vector3 {
            x: 0.0,
            y: 0.0,
            z: -1.25,
        },
    };
    let _task = keep_publishing(publisher, twist);
    let bus = connect(&router);
    wait_until(T, "a /cmd_vel publisher", || {
        let n = bus.graph().publishers_of("/cmd_vel").count();
        async move { (n > 0).then_some(()).ok_or(format!("{n} publishers")) }
    })
    .await;

    let mut sub = bus.subscribe("/cmd_vel").await.unwrap();
    assert_eq!(sub.type_name, "geometry_msgs/msg/Twist");
    assert_eq!(sub.schema.fields.len(), 2);
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .unwrap()
        .unwrap();
    assert!(raw.size_bytes > 0);
    assert!(
        raw.sequence.is_some(),
        "rmw_zenoh attachment should carry a sequence number"
    );
    assert!(raw.source_timestamp.is_some());
    assert_eq!(
        raw.decode().unwrap(),
        json!({"linear": {"x": 0.5, "y": 0.0, "z": 0.0}, "angular": {"x": 0.0, "y": 0.0, "z": -1.25}})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_arrays_and_times_decode_per_the_json_mapping() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "camera");
    let publisher = node.create_pub::<Image>("/image").build().unwrap();
    let image = Image {
        header: Header {
            stamp: Time {
                sec: 42,
                nanosec: 7,
            },
            frame_id: "cam".into(),
        },
        height: 1,
        width: 3,
        encoding: "mono8".into(),
        is_bigendian: 0,
        step: 3,
        data: vec![1u8, 2, 255].into(),
    };
    let _task = keep_publishing(publisher, image);
    let bus = connect(&router);
    wait_until(T, "an /image publisher", || {
        let n = bus.graph().publishers_of("/image").count();
        async move { (n > 0).then_some(()).ok_or(format!("{n} publishers")) }
    })
    .await;
    let mut sub = bus.subscribe("/image").await.unwrap();
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.decode().unwrap(),
        json!({
            "header": {"stamp": {"sec": 42, "nanosec": 7}, "frame_id": "cam"},
            "height": 1, "width": 3, "encoding": "mono8", "is_bigendian": 0, "step": 3,
            "data": "AQL/"
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_local_late_joiner_receives_history() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "map_server");
    let latched = QosProfile {
        durability: QosDurability::TransientLocal,
        history: QosHistory::from_depth(1),
        ..QosProfile::default()
    };
    let publisher = node
        .create_pub::<RosString>("/map")
        .with_qos(latched)
        .build()
        .unwrap();
    // Published exactly once, before rmf-pipes even connects.
    publisher
        .publish(&RosString {
            data: "the map".into(),
        })
        .unwrap();

    let bus = connect(&router);
    let ep = wait_until(T, "a /map publisher", || {
        let g = bus.graph();
        async move {
            g.publishers_of("/map")
                .next()
                .cloned()
                .ok_or("none".to_owned())
        }
    })
    .await;
    assert!(
        ep.qos.transient_local,
        "publisher QoS should be advertised as transient_local"
    );
    let mut sub = bus.subscribe("/map").await.unwrap();
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .expect("late joiner got no history")
        .unwrap();
    assert_eq!(raw.decode().unwrap(), json!({"data": "the map"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_types_on_one_topic_are_flagged() {
    init_logging();
    let router = TestRouter::start();
    let (_c1, a) = fixture_node(&router.endpoint(), "old_node");
    let (_c2, b) = fixture_node(&router.endpoint(), "new_node");
    let _p1 = a.create_pub::<RosString>("/status").build().unwrap();
    let _p2 = b.create_pub::<Int32>("/status").build().unwrap();
    let bus = connect(&router);
    let summary = wait_until(T, "both /status publishers", || {
        let t = bus.graph().topic("/status");
        async move {
            match t {
                Some(t) if t.publishers == 2 => Ok(t),
                other => Err(format!("{other:?}")),
            }
        }
    })
    .await;
    assert!(summary.mismatch);
    let types: Vec<&str> = summary.types.iter().map(|t| t.type_name.as_str()).collect();
    assert_eq!(types, vec!["std_msgs/msg/Int32", "std_msgs/msg/String"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_errors_are_specific() {
    init_logging();
    let router = TestRouter::start();
    let bus = connect(&router);
    assert_eq!(
        bus.subscribe("/nobody").await.unwrap_err(),
        BusError::NoPublisher("/nobody".into())
    );

    // A publisher whose node does not serve type descriptions cannot be decoded.
    let ctx = pipes_testkit::fixture_context(&router.endpoint());
    let opaque = ctx
        .create_node("opaque")
        .without_parameters()
        .build()
        .unwrap();
    let _p = opaque.create_pub::<RosString>("/opaque").build().unwrap();
    wait_until(T, "an /opaque publisher", || {
        let n = bus.graph().publishers_of("/opaque").count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let quick = Arc::new(
        HirozBus::connect(pipes_bus::BusConfig {
            discovery_timeout: Duration::from_millis(500),
            ..bus_config(&router.endpoint())
        })
        .unwrap(),
    );
    let err = quick.subscribe("/opaque").await.unwrap_err();
    assert!(matches!(err, BusError::Schema { .. }), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_with_hiroz_bus_serves_standing_topics() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "fleet_adapter");
    let publisher = node
        .create_pub::<RosString>("/fleet/robot1/state")
        .build()
        .unwrap();
    let _task = keep_publishing(
        publisher,
        RosString {
            data: "idle".into(),
        },
    );
    let bus = connect(&router);
    let hub = Hub::new(
        bus,
        HubConfig {
            patterns: vec![TopicPattern::parse("fleet/*/state").unwrap()],
            tick_interval: Duration::from_millis(100),
            ..HubConfig::default()
        },
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = hub.spawn(shutdown.clone());

    let latest = wait_until(T, "a latest sample on the standing topic", || {
        let s = hub.latest("/fleet/robot1/state");
        let st = hub.topic("/fleet/robot1/state");
        async move { s.ok_or_else(|| format!("{st:?}")) }
    })
    .await;
    assert_eq!(latest.data, Some(json!({"data": "idle"})));
    let view = hub.topic("/fleet/robot1/state").unwrap();
    assert_eq!(view.status, TopicStatus::Active);
    assert!(view.standing);

    shutdown.cancel();
    task.await.unwrap();
}

/// Guards the decision to bind subscriptions to the publisher's advertised
/// hash (hiroz's `create_sub_impl`) instead of a locally computed one
/// (`create_dyn_sub`): this publisher advertises a hash hiroz would never
/// compute for its schema, and serves its own type description. Subscribing
/// with a locally computed hash would receive nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_binds_to_the_advertised_hash() {
    use hiroz::{
        ServiceTypeInfo,
        dynamic::{
            FieldType, GetTypeDescription, GetTypeDescriptionResponse, MessageSchema,
            schema_to_wire_type_description,
        },
        entity::{TypeHash, TypeInfo},
    };
    init_logging();
    let router = TestRouter::start();
    let ctx = pipes_testkit::fixture_context(&router.endpoint());
    let node = ctx
        .create_node("forger")
        .without_parameters()
        .build()
        .unwrap();
    let schema = MessageSchema::builder("std_msgs/msg/String")
        .field("data", FieldType::String)
        .build()
        .unwrap();
    // The hash hiroz computes for std_msgs/String (and rclcpp advertises).
    let real = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18";
    let forged = TypeHash::new(1, [0xab; 32]);
    assert_ne!(forged.to_rihs_string(), real);

    let mut server = node
        .create_service_impl::<GetTypeDescription>(
            "/forger/get_type_description",
            Some(GetTypeDescription::service_type_info()),
        )
        .build()
        .unwrap();
    let wire = schema_to_wire_type_description(&schema).unwrap();
    let _serve = tokio::spawn(async move {
        while let Ok(req) = server.async_take_request().await {
            let (_, reply) = req.into_parts();
            let _ = reply
                .reply(&GetTypeDescriptionResponse {
                    successful: true,
                    failure_reason: String::new(),
                    type_description: wire.clone(),
                    type_sources: vec![],
                    extra_information: vec![],
                })
                .await;
        }
    });
    let publisher = node
        .create_pub_impl::<RosString>(
            "/forged",
            Some(TypeInfo::new(
                "std_msgs::msg::dds_::String_",
                forged.clone(),
            )),
        )
        .build()
        .unwrap();
    let _task = keep_publishing(
        publisher,
        RosString {
            data: "forged".into(),
        },
    );

    let bus = connect(&router);
    let ep = wait_until(T, "the /forged publisher", || {
        let g = bus.graph();
        async move {
            g.publishers_of("/forged")
                .next()
                .cloned()
                .ok_or("none".to_owned())
        }
    })
    .await;
    assert_eq!(ep.type_hash, forged.to_rihs_string());
    let mut sub = bus.subscribe("/forged").await.unwrap();
    assert_eq!(sub.type_hash, forged.to_rihs_string());
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .expect("no samples: the subscription is not bound to the advertised hash")
        .unwrap();
    assert_eq!(raw.decode().unwrap(), json!({"data": "forged"}));
}

/// Port 1 (tcpmux) is privileged, so no test router can be listening on it.
#[test]
fn connect_fails_fast_without_a_router() {
    let err = HirozBus::connect(bus_config("tcp/127.0.0.1:1")).unwrap_err();
    assert!(err.to_string().contains("zenoh"), "{err}");
}

// ---- write path (phase 4) -----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_reaches_a_typed_subscriber() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "door_adapter");
    let sub = node
        .create_sub::<RosString>("/door_requests")
        .build()
        .unwrap();
    let bus = connect(&router);
    wait_until(T, "the /door_requests subscriber", || {
        let n = bus
            .graph()
            .endpoints
            .iter()
            .filter(|e| e.name == "/door_requests" && e.kind == EndpointKind::Subscription)
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let receipt = bus
        .publish("/door_requests", &json!({"data": "open main"}))
        .await
        .unwrap();
    assert_eq!(
        (receipt.type_name.as_str(), receipt.subscriptions),
        ("std_msgs/msg/String", 1)
    );
    let got = tokio::task::spawn_blocking(move || sub.recv_timeout(T))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.data, "open main");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_errors_are_specific() {
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "door_adapter");
    let _sub = node
        .create_sub::<RosString>("/door_requests")
        .build()
        .unwrap();
    let bus = connect(&router);
    assert_eq!(
        bus.publish("/nobody", &json!({})).await.unwrap_err(),
        pipes_core::bus::WriteError::NoEndpoint("/nobody".into())
    );
    wait_until(T, "the subscriber", || {
        let n = bus
            .graph()
            .endpoints
            .iter()
            .filter(|e| e.name == "/door_requests")
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let err = bus
        .publish("/door_requests", &json!({"data": 5}))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, pipes_core::bus::WriteError::InvalidMessage(m) if m.contains("data: expected a string")),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_calls_reach_a_hiroz_server() {
    use hiroz_msgs::example_interfaces::{AddTwoIntsResponse, srv::AddTwoInts};
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "adder");
    let mut server = node
        .create_service::<AddTwoInts>("/add_two_ints")
        .build()
        .unwrap();
    let _serve = tokio::spawn(async move {
        while let Ok(req) = server.async_take_request().await {
            let (msg, reply) = req.into_parts();
            let _ = reply
                .reply(&AddTwoIntsResponse { sum: msg.a + msg.b })
                .await;
        }
    });
    let bus = connect(&router);
    wait_until(T, "the /add_two_ints server", || {
        let n = bus
            .graph()
            .services()
            .iter()
            .filter(|s| s.name == "/add_two_ints")
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let called = bus
        .call("/add_two_ints", &json!({"a": "40", "b": 2}), T)
        .await
        .unwrap();
    assert_eq!(called.type_name, "example_interfaces/srv/AddTwoInts");
    assert_eq!(called.response, json!({"sum": "42"}));
    let err = bus
        .call("/add_two_ints", &json!({"a": "x"}), T)
        .await
        .unwrap_err();
    assert!(
        matches!(err, pipes_core::bus::WriteError::InvalidMessage(_)),
        "{err:?}"
    );
    let err = bus.call("/nobody", &json!({}), T).await.unwrap_err();
    assert!(
        matches!(err, pipes_core::bus::WriteError::NoEndpoint(_)),
        "{err:?}"
    );
}

/// Wait until the graph shows exactly `n` remote subscriptions of `topic`.
async fn wait_subs(bus: &HirozBus, topic: &str, n: usize) {
    wait_until(T, &format!("{n} subscriptions of {topic}"), || {
        let c = bus
            .graph()
            .endpoints
            .iter()
            .filter(|e| e.name == topic && e.kind == EndpointKind::Subscription)
            .count();
        async move { (c == n).then_some(()).ok_or(format!("{c}")) }
    })
    .await;
}

/// Publishers bind to the subscribers' advertised hash, and the publisher
/// cache is keyed by it: when the subscriber is replaced by one with another
/// hash, a new publisher is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishes_bind_to_the_subscribers_advertised_hash() {
    use hiroz::entity::TypeInfo;
    init_logging();
    let router = TestRouter::start();
    let bus = connect(&router);

    // 1. A normal subscriber.
    let (ctx1, normal) = fixture_node(&router.endpoint(), "normal");
    let sub1 = normal.create_sub::<RosString>("/swap").build().unwrap();
    wait_subs(&bus, "/swap", 1).await;
    bus.publish("/swap", &json!({"data": "one"})).await.unwrap();
    let got = tokio::task::spawn_blocking(move || sub1.recv_timeout(T))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.data, "one");
    drop(normal);
    drop(ctx1);
    wait_subs(&bus, "/swap", 0).await;

    // 2. A subscriber advertising a hash hiroz would never compute.
    let ctx2 = pipes_testkit::fixture_context(&router.endpoint());
    let forger = ctx2
        .create_node("forger")
        .without_parameters()
        .build()
        .unwrap();
    let _td = pipes_testkit::serve_type_description(&forger, pipes_testkit::string_schema());
    let forged = pipes_testkit::forged_hash(0xcd);
    let sub2 = forger
        .create_sub_impl::<RosString>(
            "/swap",
            Some(TypeInfo::new(
                "std_msgs::msg::dds_::String_",
                forged.clone(),
            )),
        )
        .build()
        .unwrap();
    wait_subs(&bus, "/swap", 1).await;
    let receipt = bus.publish("/swap", &json!({"data": "two"})).await.unwrap();
    assert_eq!(receipt.type_hash, forged.to_rihs_string());
    let got = tokio::task::spawn_blocking(move || sub2.recv_timeout(T))
        .await
        .unwrap()
        .expect("not delivered: the publisher isn't bound to the advertised hash");
    assert_eq!(got.data, "two");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_picks_the_subscribers_type_and_counts_only_remote_matches() {
    init_logging();
    let router = TestRouter::start();
    let (_c1, a) = fixture_node(&router.endpoint(), "consumer_a");
    let (_c2, b) = fixture_node(&router.endpoint(), "consumer_b");
    let (_c3, p) = fixture_node(&router.endpoint(), "producer");
    let _s1 = a.create_sub::<RosString>("/mixed").build().unwrap();
    let _s2 = b.create_sub::<RosString>("/mixed").build().unwrap();
    // A publisher of another type on the same topic doesn't decide the type.
    let _p = p.create_pub::<Int32>("/mixed").build().unwrap();
    let bus = connect(&router);
    wait_subs(&bus, "/mixed", 2).await;
    // rmf-pipes' own subscription must not count.
    let _own = bus.subscribe("/mixed").await;
    let receipt = bus
        .publish("/mixed", &json!({"data": "for strings"}))
        .await
        .unwrap();
    assert_eq!(
        (receipt.type_name.as_str(), receipt.subscriptions),
        ("std_msgs/msg/String", 2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_writes_are_refused() {
    use hiroz_msgs::example_interfaces::{AddTwoIntsResponse, srv::AddTwoInts};
    init_logging();
    let router = TestRouter::start();
    let (_c1, a) = fixture_node(&router.endpoint(), "string_consumer");
    let (_c2, b) = fixture_node(&router.endpoint(), "int_consumer");
    let _s1 = a.create_sub::<RosString>("/clash").build().unwrap();
    let _s2 = b.create_sub::<Int32>("/clash").build().unwrap();
    let mut servers = Vec::new();
    for node in [&a, &b] {
        let mut srv = node.create_service::<AddTwoInts>("/twice").build().unwrap();
        servers.push(tokio::spawn(async move {
            while let Ok(req) = srv.async_take_request().await {
                let (msg, reply) = req.into_parts();
                let _ = reply
                    .reply(&AddTwoIntsResponse { sum: msg.a + msg.b })
                    .await;
            }
        }));
    }
    let bus = connect(&router);
    wait_subs(&bus, "/clash", 2).await;
    let err = bus
        .publish("/clash", &json!({"data": 1}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, pipes_core::bus::WriteError::Conflict(_)),
        "{err:?}"
    );
    wait_until(T, "two /twice servers", || {
        let n = bus
            .graph()
            .services()
            .iter()
            .filter(|s| s.name == "/twice")
            .map(|s| s.servers)
            .sum::<usize>();
        async move { (n == 2).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let err = bus
        .call("/twice", &json!({"a": 1, "b": 2}), T)
        .await
        .unwrap_err();
    assert!(
        matches!(err, pipes_core::bus::WriteError::Conflict(_)),
        "{err:?}"
    );
}

/// A tap sees every request a client sends, decoded with the service's request
/// type, while the real server still answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taps_observe_requests_without_disturbing_the_server() {
    use hiroz_msgs::example_interfaces::{AddTwoIntsRequest, AddTwoIntsResponse, srv::AddTwoInts};
    init_logging();
    let router = TestRouter::start();
    let (_ctx, node) = fixture_node(&router.endpoint(), "adder");
    let mut server = node
        .create_service::<AddTwoInts>("/add_two_ints")
        .build()
        .unwrap();
    let _serve = tokio::spawn(async move {
        while let Ok(req) = server.async_take_request().await {
            let (msg, reply) = req.into_parts();
            let _ = reply
                .reply(&AddTwoIntsResponse { sum: msg.a + msg.b })
                .await;
        }
    });
    let (_cctx, caller) = fixture_node(&router.endpoint(), "caller");
    let client = caller
        .create_client::<AddTwoInts>("/add_two_ints")
        .build()
        .unwrap();
    let bus = connect(&router);
    let err = bus.tap("/nobody_serves_this").await.unwrap_err();
    assert!(matches!(err, BusError::NoServer(_)), "{err:?}");
    wait_until(T, "the /add_two_ints server", || {
        let n = bus
            .graph()
            .services()
            .iter()
            .filter(|s| s.name == "/add_two_ints" && s.servers > 0)
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let mut tap = bus.tap("/add_two_ints").await.unwrap();
    assert_eq!(tap.type_name, "example_interfaces/srv/AddTwoInts");
    assert_eq!(
        tap.schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    // The tap's queryable reaches the router asynchronously, so the first
    // calls may not be routed to it yet; every call must still be answered.
    let mut seen = None;
    for i in 0..50 {
        let started = std::time::Instant::now();
        let res = tokio::time::timeout(T, client.call(&AddTwoIntsRequest { a: 40, b: i }))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res.sum, 40 + i, "the real server still answers");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the tap must not hold the call open: {:?}",
            started.elapsed()
        );
        if let Ok(Some(s)) =
            tokio::time::timeout(Duration::from_millis(200), tap.samples.next()).await
        {
            seen = Some((i, s));
            break;
        }
    }
    let (i, seen) = seen.expect("the tap saw no request");
    assert!(seen.sequence.is_some(), "the rmw_zenoh attachment is read");
    assert_eq!(
        seen.decode().unwrap(),
        json!({"a": "40", "b": i.to_string()})
    );
    drop(tap);
    let res = tokio::time::timeout(T, client.call(&AddTwoIntsRequest { a: 1, b: 2 }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.sum, 3, "calls work after the tap is gone");
}

/// Type descriptions are per type, so a publisher that can't describe its own
/// type (rclrs, `ros2 topic pub --once`) is decoded with a description from
/// any other node using the same type and hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_discovery_falls_back_to_other_nodes_using_the_type() {
    init_logging();
    let router = TestRouter::start();
    let ctx = pipes_testkit::fixture_context(&router.endpoint());
    let mute = ctx
        .create_node("mute")
        .without_parameters()
        .build()
        .unwrap();
    let _mute_pub = keep_publishing(
        mute.create_pub::<RosString>("/mute").build().unwrap(),
        RosString {
            data: "quiet".into(),
        },
    );
    let bus = connect(&router);
    wait_until(T, "the /mute publisher", || {
        let g = bus.graph();
        async move {
            g.publishers_of("/mute")
                .next()
                .map(|_| ())
                .ok_or("none".to_owned())
        }
    })
    .await;
    let err = bus.subscribe("/mute").await.unwrap_err();
    assert!(
        matches!(err, BusError::Schema { .. }),
        "nobody can describe the type yet: {err:?}"
    );
    // Another node uses std_msgs/String (on another topic) and can describe it.
    let (_ctx2, talker) = fixture_node(&router.endpoint(), "talker");
    let _other = talker
        .create_sub::<RosString>("/elsewhere")
        .build()
        .unwrap();
    wait_until(T, "the /elsewhere subscription", || {
        let g = bus.graph();
        let n = g
            .endpoints
            .iter()
            .filter(|e| e.name == "/elsewhere")
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let mut sub = bus.subscribe("/mute").await.unwrap();
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.decode().unwrap(), json!({"data": "quiet"}));
}

/// The same fallback for services: a server that can't describe its type is
/// tapped with the description from another node serving the same type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_schemas_fall_back_to_other_nodes_using_the_type() {
    use hiroz_msgs::example_interfaces::{AddTwoIntsRequest, AddTwoIntsResponse, srv::AddTwoInts};
    init_logging();
    let router = TestRouter::start();
    let ctx = pipes_testkit::fixture_context(&router.endpoint());
    let mute = ctx
        .create_node("mute")
        .without_parameters()
        .build()
        .unwrap();
    let mut server = mute
        .create_service::<AddTwoInts>("/mute_add")
        .build()
        .unwrap();
    let _serve = tokio::spawn(async move {
        while let Ok(req) = server.async_take_request().await {
            let (msg, reply) = req.into_parts();
            let _ = reply
                .reply(&AddTwoIntsResponse { sum: msg.a + msg.b })
                .await;
        }
    });
    let bus = connect(&router);
    wait_until(T, "the /mute_add server", || {
        let n = bus
            .graph()
            .services()
            .iter()
            .filter(|s| s.name == "/mute_add")
            .count();
        async move { (n > 0).then_some(()).ok_or(format!("{n}")) }
    })
    .await;
    let err = bus.tap("/mute_add").await.unwrap_err();
    assert!(matches!(err, BusError::Schema { .. }), "{err:?}");
    let (_ctx2, other) = fixture_node(&router.endpoint(), "describer");
    let _other = other
        .create_service::<AddTwoInts>("/other_add")
        .build()
        .unwrap();
    let (_ctx3, caller) = fixture_node(&router.endpoint(), "caller");
    let client = caller
        .create_client::<AddTwoInts>("/mute_add")
        .build()
        .unwrap();
    // Failed discoveries are cached briefly; wait out the cache.
    let mut tap = wait_until(
        Duration::from_secs(30),
        "a tap described by the other node",
        || {
            let bus = bus.clone();
            async move { bus.tap("/mute_add").await.map_err(|e| e.to_string()) }
        },
    )
    .await;
    // As in `taps_observe_…`: retry until the queryable has propagated.
    let mut seen = None;
    for _ in 0..50 {
        let _ = tokio::time::timeout(T, client.call(&AddTwoIntsRequest { a: 2, b: 3 }))
            .await
            .unwrap()
            .unwrap();
        if let Ok(Some(s)) =
            tokio::time::timeout(Duration::from_millis(200), tap.samples.next()).await
        {
            seen = Some(s);
            break;
        }
    }
    assert_eq!(
        seen.expect("the tap saw no request").decode().unwrap(),
        json!({"a": "2", "b": "3"})
    );
    let err = bus.tap("/bad*name").await.unwrap_err();
    assert!(
        matches!(err, BusError::Subscribe { .. }),
        "names with key syntax are refused: {err:?}"
    );
}
