//! The ROS graph as rmf-pipes sees it: a bus-agnostic snapshot built by the bus
//! from liveliness tokens, plus derived views (per-type counts, mismatches).

use std::collections::BTreeMap;

use serde::Serialize;

/// A point-in-time view of the ROS graph. Entities owned by rmf-pipes itself are
/// excluded by the bus.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct GraphSnapshot {
    /// Nodes, sorted after [`GraphSnapshot::normalized`].
    pub nodes: Vec<Node>,
    /// Every publisher, subscription, service server and client.
    pub endpoints: Vec<Endpoint>,
}

/// A ROS node.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Node {
    /// Fully qualified name, e.g. `/fleet/tinyRobot1_adapter`.
    pub fqn: String,
    /// Node name, e.g. `tinyRobot1_adapter`.
    pub name: String,
    /// Namespace, e.g. `/fleet` (`/` for the root).
    pub namespace: String,
}

/// What an [`Endpoint`] is. Variants are self-describing.
#[allow(missing_docs)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Publisher,
    Subscription,
    ServiceServer,
    ServiceClient,
}

impl EndpointKind {
    /// Publishers and subscriptions, as opposed to service endpoints.
    pub fn is_topic(self) -> bool {
        matches!(self, Self::Publisher | Self::Subscription)
    }
}

/// One publisher, subscription, service server or client.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Endpoint {
    /// What kind of endpoint this is.
    pub kind: EndpointKind,
    /// Topic or service name, e.g. `/chatter`.
    pub name: String,
    /// ROS type name, e.g. `std_msgs/msg/String`.
    pub type_name: String,
    /// RIHS01 type hash string as advertised, e.g. `RIHS01_df66…`.
    pub type_hash: String,
    /// Fully qualified name of the owning node, when advertised.
    pub node: Option<String>,
    /// Advertised QoS.
    pub qos: Qos,
}

/// The subset of QoS that matters for subscribing and for diagnosing.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Qos {
    /// Reliable (vs best effort).
    pub reliable: bool,
    /// Transient local (late joiners get history) vs volatile.
    pub transient_local: bool,
    /// `None` means keep-all.
    pub depth: Option<usize>,
}

/// One (type name, hash) pair seen on a topic, with how many endpoints use it.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicType {
    /// ROS type name.
    pub type_name: String,
    /// RIHS01 type hash.
    pub type_hash: String,
    /// Publishers using this type.
    pub publishers: usize,
    /// Subscriptions using this type.
    pub subscriptions: usize,
}

/// Summary of one topic across all its endpoints.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicSummary {
    /// Topic name, e.g. `/chatter`.
    pub name: String,
    /// Every (type, hash) pair seen on the topic.
    pub types: Vec<TopicType>,
    /// Total publishers across all types.
    pub publishers: usize,
    /// Total subscriptions across all types.
    pub subscriptions: usize,
    /// More than one (type, hash) pair on this topic. rmw_zenoh puts the hash in
    /// the key expression, so mismatched endpoints silently never see each
    /// other's data.
    pub mismatch: bool,
}

/// Summary of one service.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceSummary {
    /// Service name.
    pub name: String,
    /// Service type name.
    pub type_name: String,
    /// Number of servers.
    pub servers: usize,
    /// Number of clients.
    pub clients: usize,
}

impl GraphSnapshot {
    /// Sort nodes and endpoints so snapshots compare and render deterministically.
    pub fn normalized(mut self) -> Self {
        self.nodes.sort();
        self.nodes.dedup();
        self.endpoints.sort();
        self
    }

    /// Per-topic summaries, sorted by name.
    pub fn topics(&self) -> Vec<TopicSummary> {
        // topic -> (type name, hash) -> (publishers, subscriptions)
        type Counts<'a> = BTreeMap<(&'a str, &'a str), (usize, usize)>;
        let mut by_topic: BTreeMap<&str, Counts<'_>> = BTreeMap::new();
        for ep in self.endpoints.iter().filter(|e| e.kind.is_topic()) {
            let counts = by_topic
                .entry(&ep.name)
                .or_default()
                .entry((&ep.type_name, &ep.type_hash))
                .or_default();
            match ep.kind {
                EndpointKind::Publisher => counts.0 += 1,
                EndpointKind::Subscription => counts.1 += 1,
                _ => {}
            }
        }
        by_topic
            .into_iter()
            .map(|(name, types)| {
                let types: Vec<TopicType> = types
                    .into_iter()
                    .map(
                        |((type_name, type_hash), (publishers, subscriptions))| TopicType {
                            type_name: type_name.to_owned(),
                            type_hash: type_hash.to_owned(),
                            publishers,
                            subscriptions,
                        },
                    )
                    .collect();
                TopicSummary {
                    name: name.to_owned(),
                    mismatch: types.len() > 1,
                    publishers: types.iter().map(|t| t.publishers).sum(),
                    subscriptions: types.iter().map(|t| t.subscriptions).sum(),
                    types,
                }
            })
            .collect()
    }

    /// One topic's summary, if it has any endpoints.
    pub fn topic(&self, name: &str) -> Option<TopicSummary> {
        self.topics().into_iter().find(|t| t.name == name)
    }

    /// Service summaries, sorted by name. A service with mismatched types is
    /// listed once per type.
    pub fn services(&self) -> Vec<ServiceSummary> {
        let mut by: BTreeMap<(&str, &str), (usize, usize)> = BTreeMap::new();
        for ep in &self.endpoints {
            let counts = by.entry((&ep.name, &ep.type_name)).or_default();
            match ep.kind {
                EndpointKind::ServiceServer => counts.0 += 1,
                EndpointKind::ServiceClient => counts.1 += 1,
                _ => {}
            }
        }
        by.into_iter()
            .filter(|(_, (s, c))| s + c > 0)
            .map(|((name, type_name), (servers, clients))| ServiceSummary {
                name: name.to_owned(),
                type_name: type_name.to_owned(),
                servers,
                clients,
            })
            .collect()
    }

    /// Publishers of `topic`.
    pub fn publishers_of<'a>(&'a self, topic: &'a str) -> impl Iterator<Item = &'a Endpoint> + 'a {
        self.endpoints
            .iter()
            .filter(move |e| e.kind == EndpointKind::Publisher && e.name == topic)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn ep(kind: EndpointKind, name: &str, ty: &str, hash: &str) -> Endpoint {
        Endpoint {
            kind,
            name: name.into(),
            type_name: ty.into(),
            type_hash: hash.into(),
            node: Some("/n".into()),
            qos: Qos {
                reliable: true,
                transient_local: false,
                depth: Some(10),
            },
        }
    }

    #[test]
    fn counts_and_mismatch() {
        use EndpointKind::*;
        let g = GraphSnapshot {
            nodes: vec![],
            endpoints: vec![
                ep(Publisher, "/a", "std_msgs/msg/String", "RIHS01_1"),
                ep(Publisher, "/a", "std_msgs/msg/String", "RIHS01_1"),
                ep(Subscription, "/a", "std_msgs/msg/String", "RIHS01_1"),
                ep(Publisher, "/b", "x/msg/T", "RIHS01_2"),
                ep(Subscription, "/b", "x/msg/T", "RIHS01_3"),
                ep(ServiceServer, "/s", "x/srv/S", "RIHS01_4"),
            ],
        }
        .normalized();
        let topics = g.topics();
        assert_eq!(topics.len(), 2, "services must not appear as topics");
        let a = &topics[0];
        assert_eq!(
            (a.name.as_str(), a.publishers, a.subscriptions, a.mismatch),
            ("/a", 2, 1, false)
        );
        let b = &topics[1];
        assert!(b.mismatch, "same topic, two hashes");
        assert_eq!(b.types.len(), 2);
        let services = g.services();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].servers, 1);
    }
}
