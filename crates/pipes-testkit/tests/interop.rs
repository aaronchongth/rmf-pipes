//! L4 interop: real ROS 2 (`rmw_zenohd` + `ros2 topic pub` on rmw_zenoh_cpp)
//! → `HirozBus`. Ignored by default; run with `pixi run interop`. Each test
//! skips loudly when no ROS install is found (see `pipes_testkit::ros`).

#![allow(clippy::unwrap_used)]

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use pipes_bus::HirozBus;
use pipes_core::bus::Bus;
use pipes_testkit::{bus_config, free_port, init_logging, ros::RosEnv, wait_until};
use serde_json::{Value, json};

/// ROS CLI startup (Python) plus discovery can take several seconds.
const T: Duration = Duration::from_secs(30);

async fn first_sample(bus: &HirozBus, topic: &str) -> (pipes_core::bus::Subscription, Value) {
    let mut sub = bus.subscribe(topic).await.unwrap();
    let raw = tokio::time::timeout(T, sub.samples.next())
        .await
        .unwrap_or_else(|_| panic!("no sample on {topic}"))
        .unwrap();
    let json = raw.decode().unwrap();
    (sub, json)
}

async fn wait_for_publisher(bus: &HirozBus, topic: &str) -> pipes_core::graph::Endpoint {
    wait_until(T, &format!("a publisher of {topic}"), || {
        let g = bus.graph();
        async move {
            g.publishers_of(topic).next().cloned().ok_or_else(|| {
                format!(
                    "topics: {:?}",
                    g.topics()
                        .iter()
                        .map(|t| t.name.clone())
                        .collect::<Vec<_>>()
                )
            })
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a ROS 2 install with rmw_zenoh; run with `pixi run interop`"]
async fn rclpy_publishers_on_rmw_zenoh_are_discovered_and_decoded() {
    init_logging();
    let Some(ros) = RosEnv::require_or_skip("interop") else {
        return;
    };
    let port = free_port();
    let mut router = ros.router(port);
    let endpoint = format!("tcp/127.0.0.1:{port}");

    let mut chatter = ros.ros2(
        port,
        r#"topic pub -r 10 /chatter std_msgs/msg/String "{data: hello}""#,
    );
    let mut twist = ros.ros2(
        port,
        r#"topic pub -r 10 /cmd_vel geometry_msgs/msg/Twist "{linear: {x: 0.5}, angular: {z: -1.25}}""#,
    );
    let mut image = ros.ros2(
        port,
        r#"topic pub -r 10 /image sensor_msgs/msg/Image "{header: {stamp: {sec: 42, nanosec: 7}, frame_id: cam}, height: 1, width: 3, encoding: mono8, step: 3, data: [1, 2, 255]}""#,
    );
    let mut int64 = ros.ros2(
        port,
        r#"topic pub -r 10 /big std_msgs/msg/Int64 "{data: 9007199254740993}""#,
    );
    let bus = Arc::new(HirozBus::connect(bus_config(&endpoint)).unwrap());

    let ep = wait_for_publisher(&bus, "/chatter").await;
    assert_eq!(ep.type_name, "std_msgs/msg/String");
    assert_eq!(
        ep.type_hash, "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
        "hash must match the one rmw_zenoh_cpp advertises for std_msgs/String"
    );
    let (_s1, json) = first_sample(&bus, "/chatter").await;
    assert_eq!(json, json!({"data": "hello"}));

    wait_for_publisher(&bus, "/cmd_vel").await;
    let (_s2, json) = first_sample(&bus, "/cmd_vel").await;
    assert_eq!(
        json,
        json!({"linear": {"x": 0.5, "y": 0.0, "z": 0.0}, "angular": {"x": 0.0, "y": 0.0, "z": -1.25}})
    );

    wait_for_publisher(&bus, "/image").await;
    let (_s3, json) = first_sample(&bus, "/image").await;
    assert_eq!(
        json["header"],
        json!({"stamp": {"sec": 42, "nanosec": 7}, "frame_id": "cam"})
    );
    assert_eq!(json["data"], json!("AQL/"));

    wait_for_publisher(&bus, "/big").await;
    let (_s4, json) = first_sample(&bus, "/big").await;
    assert_eq!(json, json!({"data": "9007199254740993"}));

    for p in [
        &mut router,
        &mut chatter,
        &mut twist,
        &mut image,
        &mut int64,
    ] {
        p.assert_running();
    }
}

/// Types hiroz 0.2.1's own type-description converter rejects (`byte`,
/// `string<=N`); rmf-pipes converts them itself (pipes-bus `type_desc`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a ROS 2 install with rmw_zenoh; run with `pixi run interop`"]
async fn byte_and_bounded_string_fields_are_decoded() {
    init_logging();
    let Some(ros) = RosEnv::require_or_skip("interop") else {
        return;
    };
    let port = free_port();
    let _router = ros.router(port);
    let endpoint = format!("tcp/127.0.0.1:{port}");
    let _diag = ros.ros2(
        port,
        r#"topic pub -r 10 /diagnostics diagnostic_msgs/msg/DiagnosticArray "{status: [{level: !!binary Ag==, name: motor, message: hot, hardware_id: m1, values: [{key: temp, value: '80'}]}]}""#,
    );
    let _entities = ros.ros2(
        port,
        r#"topic pub -r 10 /entities rmw_dds_common/msg/NodeEntitiesInfo "{node_namespace: /fleet, node_name: adapter}""#,
    );
    let bus = Arc::new(HirozBus::connect(bus_config(&endpoint)).unwrap());

    wait_for_publisher(&bus, "/diagnostics").await;
    let (sub, json) = first_sample(&bus, "/diagnostics").await;
    assert_eq!(
        json["status"],
        json!([{"level": 2, "name": "motor", "message": "hot", "hardware_id": "m1", "values": [{"key": "temp", "value": "80"}]}])
    );
    drop(sub);

    wait_for_publisher(&bus, "/entities").await;
    let (sub, json) = first_sample(&bus, "/entities").await;
    assert_eq!(
        json,
        json!({"node_namespace": "/fleet", "node_name": "adapter", "reader_gid_seq": [], "writer_gid_seq": []})
    );
    let bounded = sub
        .schema
        .fields
        .iter()
        .find(|f| f.name == "node_name")
        .unwrap();
    assert_eq!(
        bounded.field_type,
        pipes_core::schema::FieldType::String { max_len: Some(256) }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a ROS 2 install with rmw_zenoh; run with `pixi run interop`"]
async fn transient_local_from_rmw_zenoh_reaches_a_late_joiner() {
    init_logging();
    let Some(ros) = RosEnv::require_or_skip("interop") else {
        return;
    };
    let port = free_port();
    let _router = ros.router(port);
    let endpoint = format!("tcp/127.0.0.1:{port}");

    // `ros2 topic pub` publishes immediately, then every 1/rate seconds while
    // spinning (which also serves the type description). At 0.05 Hz the next
    // live publication is 20s away, so a sample within a few seconds of
    // subscribing can only be transient_local history. (`--once` can't be used:
    // it sleeps without spinning, so its type description never answers.)
    let _latched = ros.ros2(
        port,
        r#"topic pub -r 0.05 --qos-durability transient_local --qos-depth 1 /map std_msgs/msg/String "{data: the map}""#,
    );
    let bus = Arc::new(HirozBus::connect(bus_config(&endpoint)).unwrap());
    let ep = wait_for_publisher(&bus, "/map").await;
    assert!(ep.qos.transient_local, "publisher QoS: {:?}", ep.qos);
    // Make sure the first (and, for 20s, only) publication is behind us.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let started = std::time::Instant::now();
    let (_sub, json) = first_sample(&bus, "/map").await;
    assert_eq!(json, json!({"data": "the map"}));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "sample took {:?}: that was the next live publication, not history",
        started.elapsed()
    );
}
