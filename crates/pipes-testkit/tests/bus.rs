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
                Some(t) if t.publishers() == 2 => Ok(t),
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
