//! [`HirozBus`]: the hiroz implementation of [`pipes_core::bus::Bus`].

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use hiroz::{
    Builder,
    attachment::Attachment,
    context::{ZContext, ZContextBuilder},
    dynamic::ros_type_name_from_dds,
    dynamic::{DynamicMessage, DynamicSerdeCdrSerdes, MessageSchema},
    entity::{EndpointEntity, EndpointKind, Entity, NodeEntity, TypeInfo},
    graph::Graph,
    node::ZNode,
    qos::{QosDurability, QosHistory, QosProfile, QosReliability},
};
use pipes_core::{
    bus::{Bus, BusError, DecodeError, RawSample, Subscription},
    graph::{GraphSnapshot, Node},
};

use crate::{convert, json::message_to_json, type_desc};

/// How to join the zenoh network.
#[derive(Debug, Clone)]
pub struct BusConfig {
    /// zenoh endpoints to connect to, e.g. `tcp/127.0.0.1:7447` (the default
    /// `rmw_zenohd` router).
    pub connect: Vec<String>,
    /// zenoh session mode: `client` (connect through a router, the rmw_zenoh
    /// default) or `peer`.
    pub mode: String,
    /// ROS domain id (the first segment of every rmw_zenoh key expression).
    pub domain_id: usize,
    /// Name of the ROS node rmf-pipes appears as. Its own entities are hidden
    /// from the graph it reports.
    pub node_name: String,
    /// Optional zenoh JSON5 config file, applied before the options above.
    pub zenoh_config: Option<PathBuf>,
    /// How long to wait for a publisher's type description.
    pub discovery_timeout: Duration,
    /// Queue depth of each subscription. The oldest samples are dropped when the
    /// hub falls behind.
    pub queue_depth: usize,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            connect: vec!["tcp/127.0.0.1:7447".into()],
            mode: "client".into(),
            domain_id: 0,
            node_name: "rmf_pipes".into(),
            zenoh_config: None,
            discovery_timeout: Duration::from_secs(5),
            queue_depth: 16,
        }
    }
}

/// Why the bus could not start.
#[derive(Debug, thiserror::Error)]
#[error("cannot join the zenoh network: {0}")]
pub struct ConnectError(String);

/// A ROS 2 bus over zenoh, via hiroz.
pub struct HirozBus {
    cfg: BusConfig,
    // Field order matters for drop: the node before its context.
    node: Arc<ZNode>,
    _ctx: ZContext,
}

impl std::fmt::Debug for HirozBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HirozBus")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl HirozBus {
    /// Open a zenoh session and create rmf-pipes' ROS node.
    ///
    /// In client mode this fails if no router is reachable, so a
    /// misconfigured deployment fails at startup instead of serving an empty
    /// graph.
    pub fn connect(cfg: BusConfig) -> Result<Self, ConnectError> {
        let mut builder = ZContextBuilder::default();
        if let Some(path) = &cfg.zenoh_config {
            builder = builder.with_config_file(path.clone());
        }
        let ctx = builder
            .with_domain_id(cfg.domain_id)
            .with_mode(cfg.mode.clone())
            .with_connect_endpoints(cfg.connect.iter().map(String::as_str))
            .disable_multicast_scouting()
            .build()
            .map_err(|e| ConnectError(e.to_string()))?;
        let node = ctx
            .create_node(&cfg.node_name)
            .without_parameters()
            .build()
            .map_err(|e| ConnectError(e.to_string()))?;
        Ok(Self {
            cfg,
            node: Arc::new(node),
            _ctx: ctx,
        })
    }

    fn graph_handle(&self) -> &Arc<Graph> {
        self.node.graph()
    }

    /// Endpoints of `kind` on `name`, excluding rmf-pipes' own.
    fn remote_entities(&self, kind: EndpointKind, name: &str, service: bool) -> Vec<Arc<Entity>> {
        let graph = self.graph_handle();
        let all = if service {
            graph.get_entities_by_service(kind, name)
        } else {
            graph.get_entities_by_topic(kind, name)
        };
        all.into_iter()
            .filter(|e| !graph.is_entity_local(e))
            .collect()
    }
}

#[async_trait::async_trait]
impl Bus for HirozBus {
    fn graph(&self) -> GraphSnapshot {
        let graph = self.graph_handle();
        let mut endpoints = Vec::new();
        let mut push = |entities: Vec<Arc<Entity>>| {
            for e in entities {
                if let Entity::Endpoint(ep) = &*e
                    && let Some(ep) = convert::endpoint(ep)
                {
                    endpoints.push(ep);
                }
            }
        };
        for (topic, _) in graph.get_topic_names_and_types() {
            push(self.remote_entities(EndpointKind::Publisher, &topic, false));
            push(self.remote_entities(EndpointKind::Subscription, &topic, false));
        }
        for (service, _) in graph.get_service_names_and_types() {
            push(self.remote_entities(EndpointKind::Service, &service, true));
            push(self.remote_entities(EndpointKind::Client, &service, true));
        }
        // Compare fully qualified names: the graph reports the root namespace
        // as `/` while the node itself reports it as an empty string.
        let own = convert::fqn(self.node.namespace(), self.node.name());
        let nodes = graph
            .get_node_names()
            .into_iter()
            .map(|(name, namespace)| Node {
                fqn: convert::fqn(&namespace, &name),
                name,
                namespace,
            })
            .filter(|n| n.fqn != own)
            .collect();
        GraphSnapshot { nodes, endpoints }
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, BusError> {
        let publishers: Vec<EndpointEntity> = self
            .remote_entities(EndpointKind::Publisher, topic, false)
            .into_iter()
            .filter_map(|e| match &*e {
                Entity::Endpoint(ep) => Some(ep.clone()),
                Entity::Node(_) => None,
            })
            .collect();
        let Some(chosen) = choose_publishers(&publishers) else {
            return Err(BusError::NoPublisher(topic.to_owned()));
        };
        let type_name = ros_type_name_from_dds(&chosen.type_info.name);
        let type_hash = chosen.type_info.hash.to_rihs_string();

        // Any publisher of the chosen type can describe it; try each in turn.
        let mut errors = Vec::new();
        let mut schema = None;
        for node in &chosen.nodes {
            match type_desc::fetch(
                &self.node,
                &node.namespace,
                &node.name,
                &type_name,
                &type_hash,
                self.cfg.discovery_timeout,
            )
            .await
            {
                Ok(s) => {
                    schema = Some(s);
                    break;
                }
                Err(e) => errors.push(e),
            }
        }
        let schema: Arc<MessageSchema> = schema.ok_or_else(|| BusError::Schema {
            topic: topic.to_owned(),
            reason: if errors.is_empty() {
                "no publisher advertises a node to ask for the type description".to_owned()
            } else {
                errors.join("; ")
            },
        })?;

        // `create_sub_impl` is hiroz's (doc-hidden) builder that takes an
        // explicit TypeInfo: the public `create_dyn_sub` recomputes the hash
        // locally, which must not differ from the publisher's or rmw_zenoh would
        // route nothing to us. hiroz is pinned exactly, and
        // `subscription_binds_to_the_advertised_hash` guards this call.
        let qos = subscription_qos(&chosen.qos, self.cfg.queue_depth);
        let sub = self
            .node
            .create_sub_impl::<DynamicMessage>(topic, Some(chosen.type_info.clone()))
            .with_serdes::<DynamicSerdeCdrSerdes>()
            .with_dyn_schema(schema.clone())
            .with_qos(qos)
            .build()
            .map_err(|e| BusError::Subscribe {
                topic: topic.to_owned(),
                reason: e.to_string(),
            })?;
        let sub = Arc::new(sub);

        let decode_schema = schema.clone();
        let samples = futures::stream::unfold(sub, move |sub| {
            let schema = decode_schema.clone();
            async move {
                let sample = sub.async_recv_serialized().await.ok()?;
                Some((raw_sample(sample, schema), sub))
            }
        });

        Ok(Subscription {
            type_name,
            type_hash,
            schema: convert::schema(&schema),
            samples: Box::pin(samples),
        })
    }
}

/// The publishers a subscription binds to: those advertising the most common
/// (type, hash), ties broken by hash for determinism.
#[derive(Debug, Clone, PartialEq)]
struct Chosen {
    type_info: TypeInfo,
    /// Nodes of those publishers, to ask for the type description.
    nodes: Vec<NodeEntity>,
    /// Their QoS, merged: see [`subscription_qos`].
    qos: Vec<hiroz_protocol::qos::QosProfile>,
}

fn choose_publishers(publishers: &[EndpointEntity]) -> Option<Chosen> {
    let mut groups: Vec<(TypeInfo, Vec<&EndpointEntity>)> = Vec::new();
    for p in publishers {
        let Some(ti) = &p.type_info else { continue };
        match groups.iter_mut().find(|(t, _)| t == ti) {
            Some((_, members)) => members.push(p),
            None => groups.push((ti.clone(), vec![p])),
        }
    }
    groups.sort_by(|(a, am), (b, bm)| {
        bm.len()
            .cmp(&am.len())
            .then_with(|| a.hash.to_rihs_string().cmp(&b.hash.to_rihs_string()))
    });
    let (type_info, members) = groups.into_iter().next()?;
    let mut nodes: Vec<NodeEntity> = Vec::new();
    for m in &members {
        if let Some(n) = &m.node
            && !nodes
                .iter()
                .any(|x| x.name == n.name && x.namespace == n.namespace)
        {
            nodes.push(n.clone());
        }
    }
    Some(Chosen {
        type_info,
        nodes,
        qos: members.iter().map(|m| m.qos).collect(),
    })
}

/// QoS for our subscription: reliable if any publisher is reliable,
/// transient_local if any is (so late-join history is never lost; zenoh has no
/// QoS incompatibility to avoid), and a bounded depth that still fits the
/// largest keep-last history a transient_local publisher holds.
fn subscription_qos(publishers: &[hiroz_protocol::qos::QosProfile], depth: usize) -> QosProfile {
    use hiroz_protocol::qos as p;
    /// Never request more history than this per subscription.
    const MAX_HISTORY: usize = 100;
    let reliable = publishers
        .iter()
        .any(|q| q.reliability == p::QosReliability::Reliable);
    let transient_local = publishers
        .iter()
        .any(|q| q.durability == p::QosDurability::TransientLocal);
    let history = publishers
        .iter()
        .filter(|q| q.durability == p::QosDurability::TransientLocal)
        .map(|q| match q.history {
            p::QosHistory::KeepLast(d) => d,
            p::QosHistory::KeepAll => MAX_HISTORY,
        })
        .max()
        .unwrap_or(0)
        .min(MAX_HISTORY);
    QosProfile {
        reliability: if reliable {
            QosReliability::Reliable
        } else {
            QosReliability::BestEffort
        },
        durability: if transient_local {
            QosDurability::TransientLocal
        } else {
            QosDurability::Volatile
        },
        history: QosHistory::from_depth(depth.max(history).max(1)),
        ..QosProfile::default()
    }
}

fn raw_sample(sample: zenoh::sample::Sample, schema: Arc<MessageSchema>) -> RawSample {
    let size = sample.payload().len();
    let attachment = sample
        .attachment()
        .and_then(|a| Attachment::try_from(a).ok());
    let source_timestamp = attachment.as_ref().and_then(|a| {
        u64::try_from(a.source_timestamp)
            .ok()
            .filter(|ns| *ns > 0)
            .map(|ns| UNIX_EPOCH + Duration::from_nanos(ns))
    });
    let sequence = attachment.map(|a| a.sequence_number);
    RawSample::new(SystemTime::now(), size, move || {
        let bytes = sample.payload().to_bytes();
        DynamicMessage::from_cdr(&bytes, &schema)
            .map(|m| message_to_json(&m))
            .map_err(|e| DecodeError(e.to_string()))
    })
    .with_attachment(source_timestamp, sequence)
}

#[cfg(test)]
mod tests {
    use hiroz::{
        entity::{EndpointEntity, EndpointKind, NodeEntity, TypeHash, TypeInfo},
        qos::{QosDurability, QosHistory, QosReliability},
    };
    use hiroz_protocol::qos as p;

    use super::{choose_publishers, subscription_qos};

    fn qos(reliable: bool, transient_local: bool, depth: usize) -> p::QosProfile {
        p::QosProfile {
            reliability: if reliable {
                p::QosReliability::Reliable
            } else {
                p::QosReliability::BestEffort
            },
            durability: if transient_local {
                p::QosDurability::TransientLocal
            } else {
                p::QosDurability::Volatile
            },
            history: p::QosHistory::KeepLast(depth),
            ..Default::default()
        }
    }

    fn publisher(node: &str, hash_byte: u8, q: p::QosProfile) -> EndpointEntity {
        EndpointEntity {
            id: 1,
            node: Some(NodeEntity::new(
                0,
                "1234567890abcdef1234567890abcdef".parse().unwrap(),
                1,
                node.into(),
                "/".into(),
                String::new(),
            )),
            kind: EndpointKind::Publisher,
            topic: "/t".into(),
            type_info: Some(TypeInfo::new(
                "std_msgs::msg::dds_::String_",
                TypeHash::new(1, [hash_byte; 32]),
            )),
            qos: q,
        }
    }

    #[test]
    fn qos_is_reliable_and_transient_local_if_any_publisher_is() {
        let q = subscription_qos(&[qos(false, false, 10), qos(true, true, 5)], 16);
        assert_eq!(q.reliability, QosReliability::Reliable);
        assert_eq!(q.durability, QosDurability::TransientLocal);
        let q = subscription_qos(&[qos(false, false, 10)], 16);
        assert_eq!(q.reliability, QosReliability::BestEffort);
        assert_eq!(q.durability, QosDurability::Volatile);
    }

    #[test]
    fn depth_covers_transient_local_history_within_a_cap() {
        let depth = |pubs: &[p::QosProfile], d| match subscription_qos(pubs, d).history {
            QosHistory::KeepLast(n) => n.get(),
            QosHistory::KeepAll => usize::MAX,
        };
        assert_eq!(
            depth(&[qos(true, false, 50)], 16),
            16,
            "volatile publishers don't raise the depth"
        );
        assert_eq!(depth(&[qos(true, true, 50)], 16), 50);
        assert_eq!(depth(&[qos(true, true, 5000)], 16), 100);
        assert_eq!(depth(&[qos(true, true, 1)], 0), 1);
    }

    #[test]
    fn binds_to_the_most_common_type_and_lists_its_nodes() {
        let pubs = vec![
            publisher("old", 1, qos(true, false, 10)),
            publisher("new_a", 2, qos(true, false, 10)),
            publisher("new_b", 2, qos(true, true, 1)),
        ];
        let c = choose_publishers(&pubs).unwrap();
        assert_eq!(c.type_info.hash, TypeHash::new(1, [2; 32]));
        let nodes: Vec<&str> = c.nodes.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(nodes, vec!["new_a", "new_b"]);
        assert_eq!(c.qos.len(), 2);
        assert!(choose_publishers(&[]).is_none());
    }
}
