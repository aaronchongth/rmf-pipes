//! Test support for rmf-pipes.
//!
//! - [`TestRouter`]: an in-process zenoh router on a free localhost port, so
//!   bus tests never depend on a router already running.
//! - [`fixture_node`]: a hiroz node (with the type description service) to
//!   publish typed fixture messages from.
//! - [`bus_config`]: a [`pipes_bus::BusConfig`] pointing at a router.
//! - [`wait_until`]: poll with a deadline and a useful failure message.
//!
//! Interop helpers for a real ROS install live in [`ros`].

pub mod ros;

use std::{future::Future, time::Duration};

use hiroz::{Builder, context::ZContextBuilder, node::ZNode};
use zenoh::{Wait, config::WhatAmI};

/// An in-process zenoh router listening on `127.0.0.1:<free port>`.
pub struct TestRouter {
    port: u16,
    _session: zenoh::Session,
}

impl std::fmt::Debug for TestRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestRouter")
            .field("port", &self.port)
            .finish()
    }
}

impl TestRouter {
    /// Start a router. Retries a few times if another process grabs the port
    /// between choosing it and binding it.
    pub fn start() -> Self {
        for _ in 0..5 {
            let port = free_port();
            let mut config = zenoh::Config::default();
            config.set_mode(Some(WhatAmI::Router)).expect("router mode");
            config
                .insert_json5("listen/endpoints", &format!(r#"["tcp/127.0.0.1:{port}"]"#))
                .expect("listen endpoint");
            config
                .insert_json5("scouting/multicast/enabled", "false")
                .expect("scouting");
            // zenoh 1.9's `gateway.south: auto` can stop routing between
            // sessions of different zenoh versions; hiroz's own tests disable it.
            let _ = config.insert_json5("gateway/south", "null");
            if let Ok(session) = zenoh::open(config).wait() {
                wait_for_tcp(port);
                return Self {
                    port,
                    _session: session,
                };
            }
        }
        panic!("could not start an in-process zenoh router");
    }

    /// The router's zenoh endpoint, e.g. `tcp/127.0.0.1:41234`.
    pub fn endpoint(&self) -> String {
        format!("tcp/127.0.0.1:{}", self.port)
    }

    /// The listening port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Ask the OS for a free localhost TCP port.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("bind 127.0.0.1:0")
}

fn wait_for_tcp(port: u16) {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    for _ in 0..100 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The ROS domain every hermetic test uses, regardless of the caller's
/// `ROS_DOMAIN_ID` (hiroz honours that variable when no domain is given).
pub const TEST_DOMAIN: usize = 0;

/// Panic if the caller's environment would silently reconfigure every zenoh
/// session in the test process. hiroz reads these variables directly.
fn assert_hermetic_env() {
    for var in ["ZENOH_CONFIG_OVERRIDE", "ZENOH_SESSION_CONFIG_URI"] {
        if std::env::var_os(var).is_some() {
            panic!(
                "{var} is set; unset it to run the hermetic tests (it reconfigures every hiroz session)"
            );
        }
    }
}

/// A [`pipes_bus::BusConfig`] connecting to `router` as a client.
pub fn bus_config(router_endpoint: &str) -> pipes_bus::BusConfig {
    assert_hermetic_env();
    pipes_bus::BusConfig {
        connect: vec![router_endpoint.to_owned()],
        domain_id: TEST_DOMAIN,
        discovery_timeout: Duration::from_secs(10),
        ..pipes_bus::BusConfig::default()
    }
}

/// A hiroz node connected to `router_endpoint` that serves type descriptions,
/// for publishing fixture messages. Keep the returned context alive.
pub fn fixture_node(router_endpoint: &str, name: &str) -> (hiroz::context::ZContext, ZNode) {
    let ctx = fixture_context(router_endpoint);
    let node = ctx
        .create_node(name)
        .with_type_description_service()
        .without_parameters()
        .build()
        .expect("fixture node");
    (ctx, node)
}

/// A hiroz context on [`TEST_DOMAIN`] connected to `router_endpoint`.
pub fn fixture_context(router_endpoint: &str) -> hiroz::context::ZContext {
    assert_hermetic_env();
    ZContextBuilder::default()
        .with_domain_id(TEST_DOMAIN)
        .with_mode("client")
        .with_connect_endpoints([router_endpoint])
        .disable_multicast_scouting()
        .build()
        .expect("fixture context")
}

/// Poll `cond` every 20ms until it returns `Some`, or panic after `timeout`
/// with `what` and the last observation (the `Err` text).
pub async fn wait_until<T, F, Fut>(timeout: Duration, what: &str, mut cond: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let last = match cond().await {
            Ok(v) => return v,
            Err(seen) => seen,
        };
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {timeout:?} waiting for {what}; last seen: {last}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Install a test log subscriber honouring `RUST_LOG` (once per process).
pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// A `std_msgs/String` fixture publisher running in the background. Stops when
/// dropped.
pub struct StringPublisher {
    task: tokio::task::JoinHandle<()>,
    _ctx: hiroz::context::ZContext,
}

impl std::fmt::Debug for StringPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringPublisher").finish_non_exhaustive()
    }
}

impl Drop for StringPublisher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Publish `message(i)` on `topic` every `period` from a fixture node called
/// `node_name`, for crates that must not depend on hiroz themselves (the
/// binary's end-to-end tests). Needs a tokio runtime.
pub fn spawn_string_publisher(
    router_endpoint: &str,
    node_name: &str,
    topic: &str,
    period: Duration,
    message: impl Fn(u64) -> String + Send + 'static,
) -> StringPublisher {
    use hiroz_msgs::std_msgs::String as RosString;
    let (ctx, node) = fixture_node(router_endpoint, node_name);
    let publisher = node
        .create_pub::<RosString>(topic)
        .build()
        .expect("fixture publisher");
    let task = tokio::spawn(async move {
        let _node = node;
        let mut i = 0u64;
        loop {
            let _ = publisher.publish(&RosString { data: message(i) });
            i += 1;
            tokio::time::sleep(period).await;
        }
    });
    StringPublisher { task, _ctx: ctx }
}

/// A `std_msgs/String` fixture subscriber collecting what it receives.
pub struct StringSubscriber {
    received: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    _sub: Box<dyn std::any::Any + Send>,
    _node: ZNode,
    _ctx: hiroz::context::ZContext,
}

impl std::fmt::Debug for StringSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringSubscriber").finish_non_exhaustive()
    }
}

impl StringSubscriber {
    /// Everything received so far.
    pub fn received(&self) -> Vec<String> {
        self.received
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// Subscribe to `topic` from a fixture node called `node_name`, for crates
/// that must not depend on hiroz themselves.
pub fn spawn_string_subscriber(
    router_endpoint: &str,
    node_name: &str,
    topic: &str,
) -> StringSubscriber {
    use hiroz_msgs::std_msgs::String as RosString;
    let (ctx, node) = fixture_node(router_endpoint, node_name);
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = received.clone();
    let sub = node
        .create_sub::<RosString>(topic)
        .build_with_callback(move |msg: RosString| {
            sink.lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(msg.data);
        })
        .expect("fixture subscriber");
    StringSubscriber {
        received,
        _sub: Box::new(sub),
        _node: node,
        _ctx: ctx,
    }
}

/// Serve `~/get_type_description` for `node` (created without hiroz's own
/// type description service), answering every request with `schema`, whatever
/// hash is asked for. With [`forged_hash`], this makes a fixture whose
/// advertised hash hiroz would never compute, to prove rmf-pipes binds to
/// advertised hashes. Keep the returned task alive.
pub fn serve_type_description(
    node: &ZNode,
    schema: std::sync::Arc<hiroz::dynamic::MessageSchema>,
) -> tokio::task::JoinHandle<()> {
    use hiroz::{
        ServiceTypeInfo,
        dynamic::{
            GetTypeDescription, GetTypeDescriptionResponse, schema_to_wire_type_description,
        },
    };
    let name = format!("/{}/get_type_description", node.name());
    let mut server = node
        .create_service_impl::<GetTypeDescription>(
            &name,
            Some(GetTypeDescription::service_type_info()),
        )
        .build()
        .expect("type description server");
    let wire = schema_to_wire_type_description(&schema).expect("wire description");
    tokio::spawn(async move {
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
    })
}

/// A RIHS01 hash no real message has.
pub fn forged_hash(byte: u8) -> hiroz::entity::TypeHash {
    hiroz::entity::TypeHash::new(1, [byte; 32])
}

/// The `std_msgs/String` schema, built by hand.
pub fn string_schema() -> std::sync::Arc<hiroz::dynamic::MessageSchema> {
    hiroz::dynamic::MessageSchema::builder("std_msgs/msg/String")
        .field("data", hiroz::dynamic::FieldType::String)
        .build()
        .expect("schema")
}
