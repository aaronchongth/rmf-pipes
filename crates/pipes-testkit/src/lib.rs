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
