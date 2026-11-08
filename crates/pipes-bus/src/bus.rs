//! [`HirozBus`]: the hiroz implementation of [`pipes_core::bus::Bus`].

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
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
use hiroz::{dynamic::DynPub, service::ZClient};
use pipes_core::{
    bus::{Bus, BusError, Called, DecodeError, Published, RawSample, Subscription, WriteError},
    graph::{GraphSnapshot, Node},
};

use crate::{
    convert,
    json::{json_to_message, message_to_json},
    raw::{RawCdr, RawService},
    type_desc,
};

/// A cache keyed by (name, type hash).
type Cache<V> = Mutex<HashMap<(String, String), V>>;

/// Most cached publishers, clients and schemas; beyond this the cache is
/// cleared (they are recreated on demand).
const CACHE_CAP: usize = 256;

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
    /// Message schemas by (type name, hash).
    schemas: Cache<Arc<MessageSchema>>,
    /// Service (request, response) schemas by (service type, hash).
    service_schemas: Cache<(Arc<MessageSchema>, Arc<MessageSchema>)>,
    /// Write-path publishers by (topic, hash), with when they were last used.
    publishers: Cache<(Arc<DynPub>, std::time::Instant)>,
    /// Recent type discovery failures by (target, hash).
    discovery_failures: Cache<(String, std::time::Instant)>,
    /// Service clients by (service, hash).
    clients: Cache<Arc<ZClient<RawService>>>,
    // Field order matters for drop: endpoints, then the node, then its context.
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
            schemas: Mutex::default(),
            service_schemas: Mutex::default(),
            publishers: Mutex::default(),
            discovery_failures: Mutex::default(),
            clients: Mutex::default(),
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

    /// Remote endpoints of `kind` on `name`, as entities.
    fn remote_endpoints(
        &self,
        kind: EndpointKind,
        name: &str,
        service: bool,
    ) -> Vec<EndpointEntity> {
        self.remote_entities(kind, name, service)
            .into_iter()
            .filter_map(|e| match &*e {
                Entity::Endpoint(ep) => Some(ep.clone()),
                Entity::Node(_) => None,
            })
            .collect()
    }

    /// The schema of `type_info`, from the cache or any of `nodes`.
    async fn message_schema(
        &self,
        target: &str,
        type_info: &TypeInfo,
        nodes: &[NodeEntity],
    ) -> Result<Arc<MessageSchema>, String> {
        let type_name = ros_type_name_from_dds(&type_info.name);
        let hash = type_info.hash.to_rihs_string();
        let key = (type_name.clone(), hash.clone());
        if let Some(s) = lock(&self.schemas).get(&key) {
            return Ok(s.clone());
        }
        let mut errors = Vec::new();
        for node in nodes {
            match type_desc::fetch(
                &self.node,
                &node.namespace,
                &node.name,
                &type_name,
                &hash,
                self.cfg.discovery_timeout,
            )
            .await
            {
                Ok(schema) => {
                    insert_capped(&self.schemas, key, schema.clone());
                    return Ok(schema);
                }
                Err(e) => errors.push(e),
            }
        }
        Err(if errors.is_empty() {
            format!("no endpoint of `{target}` advertises a node to ask for its type description")
        } else {
            errors.join("; ")
        })
    }

    /// Request and response schemas of a service type. rclcpp/rclpy describe
    /// the service type itself (fields `request_message`/`response_message`);
    /// hiroz servers register `pkg/msg/NameRequest`/`NameResponse`. Try both.
    async fn service_schema(
        &self,
        service: &str,
        type_info: &TypeInfo,
        nodes: &[NodeEntity],
    ) -> Result<(Arc<MessageSchema>, Arc<MessageSchema>), String> {
        let srv = ros_type_name_from_dds(&type_info.name);
        let hash = type_info.hash.to_rihs_string();
        let key = (srv.clone(), hash.clone());
        if let Some(s) = lock(&self.service_schemas).get(&key) {
            return Ok(s.clone());
        }
        let t = self.cfg.discovery_timeout;
        let mut errors = Vec::new();
        for node in nodes {
            let (ns, name) = (&node.namespace, &node.name);
            match type_desc::fetch(&self.node, ns, name, &srv, &hash, t).await {
                Ok(composite) => match (
                    nested(&composite, "request_message"),
                    nested(&composite, "response_message"),
                ) {
                    (Some(req), Some(res)) => {
                        insert_capped(&self.service_schemas, key, (req.clone(), res.clone()));
                        return Ok((req, res));
                    }
                    _ => errors.push(format!(
                        "{srv}: the description has no request/response messages"
                    )),
                },
                Err(e) => errors.push(e),
            }
            if let Some((pkg, base)) = srv.split_once("/srv/") {
                for (req_name, res_name) in [
                    (
                        format!("{pkg}/srv/{base}_Request"),
                        format!("{pkg}/srv/{base}_Response"),
                    ),
                    (
                        format!("{pkg}/msg/{base}Request"),
                        format!("{pkg}/msg/{base}Response"),
                    ),
                ] {
                    let req = type_desc::fetch(&self.node, ns, name, &req_name, "", t).await;
                    let res = type_desc::fetch(&self.node, ns, name, &res_name, "", t).await;
                    match (req, res) {
                        (Ok(req), Ok(res)) => {
                            insert_capped(&self.service_schemas, key, (req.clone(), res.clone()));
                            return Ok((req, res));
                        }
                        (Err(e), _) | (_, Err(e)) => errors.push(e),
                    }
                }
            }
        }
        Err(if errors.is_empty() {
            format!("no server of `{service}` advertises a node to ask for its type description")
        } else {
            errors.join("; ")
        })
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

        // The schema's own name may be the internal `/msg/` alias of a
        // service or action part; report what the graph says.
        let mut reported = convert::schema(&schema);
        reported.type_name = type_name.clone();
        Ok(Subscription {
            type_name,
            type_hash,
            schema: reported,
            samples: Box::pin(samples),
        })
    }

    async fn publish(
        &self,
        topic: &str,
        data: &serde_json::Value,
    ) -> Result<Published, WriteError> {
        // The type that matters is the one subscribers expect; fall back to
        // what publishers send. Refuse if they disagree among themselves:
        // whichever we picked, some endpoints would silently miss the message.
        let subs = self.remote_endpoints(EndpointKind::Subscription, topic, false);
        let pubs = self.remote_endpoints(EndpointKind::Publisher, topic, false);
        let pool = if subs.is_empty() { &pubs } else { &subs };
        let chosen =
            choose_publishers(pool).ok_or_else(|| WriteError::NoEndpoint(topic.to_owned()))?;
        if chosen.distinct_types > 1 {
            return Err(WriteError::Conflict(format!(
                "the {} of `{topic}` use {} different message types",
                if subs.is_empty() {
                    "publishers"
                } else {
                    "subscribers"
                },
                chosen.distinct_types
            )));
        }
        let schema = self
            .bounded_discovery(
                topic,
                &chosen.type_info,
                self.message_schema(topic, &chosen.type_info, &chosen.nodes),
            )
            .await?;
        let msg = json_to_message(&schema, data).map_err(WriteError::InvalidMessage)?;
        let hash = chosen.type_info.hash.to_rihs_string();
        let key = (topic.to_owned(), hash.clone());
        let existing = {
            let mut cache = lock(&self.publishers);
            // Undeclare publishers nobody has used for a while, so they don't
            // linger in the ROS graph.
            cache.retain(|_, (_, used)| used.elapsed() < PUBLISHER_IDLE);
            cache.get_mut(&key).map(|(p, used)| {
                *used = std::time::Instant::now();
                p.clone()
            })
        };
        let publisher = match existing {
            Some(p) => p,
            None => {
                let qos = QosProfile {
                    reliability: QosReliability::Reliable,
                    durability: QosDurability::Volatile,
                    history: QosHistory::from_depth(1),
                    ..QosProfile::default()
                };
                // Same reason as subscriptions: bind to the advertised hash.
                // (No wait for matching: in client mode publications go
                // through the router, which already knows the subscribers.)
                let p = Arc::new(
                    self.node
                        .create_pub_impl::<DynamicMessage>(topic, Some(chosen.type_info.clone()))
                        .with_serdes::<DynamicSerdeCdrSerdes>()
                        .with_dyn_schema(schema.clone())
                        .with_qos(qos)
                        .build()
                        .map_err(|e| WriteError::Bus(e.to_string()))?,
                );
                insert_capped(
                    &self.publishers,
                    key,
                    (p.clone(), std::time::Instant::now()),
                );
                p
            }
        };
        publisher
            .publish(&msg)
            .map_err(|e| WriteError::Bus(e.to_string()))?;
        Ok(Published {
            type_name: ros_type_name_from_dds(&chosen.type_info.name),
            type_hash: hash,
            subscriptions: if subs.is_empty() { 0 } else { chosen.members },
        })
    }

    async fn call(
        &self,
        service: &str,
        request: &serde_json::Value,
        timeout: Duration,
    ) -> Result<Called, WriteError> {
        let servers = self.remote_endpoints(EndpointKind::Service, service, true);
        let chosen = choose_publishers(&servers)
            .ok_or_else(|| WriteError::NoEndpoint(service.to_owned()))?;
        // A zenoh query reaches every server; refuse rather than run the
        // request several times and report one arbitrary answer.
        if chosen.distinct_types > 1 || servers.len() > 1 {
            return Err(WriteError::Conflict(format!(
                "`{service}` has {} servers; calls must reach exactly one",
                servers.len()
            )));
        }
        let (req_schema, res_schema) = self
            .bounded_discovery(
                service,
                &chosen.type_info,
                self.service_schema(service, &chosen.type_info, &chosen.nodes),
            )
            .await?;
        let req = json_to_message(&req_schema, request).map_err(WriteError::InvalidMessage)?;
        let cdr = req
            .to_cdr()
            .map_err(|e| WriteError::InvalidMessage(e.to_string()))?;
        let key = (service.to_owned(), chosen.type_info.hash.to_rihs_string());
        let existing = lock(&self.clients).get(&key).cloned();
        let client = match existing {
            Some(c) => c,
            None => {
                let c = Arc::new(
                    self.node
                        .create_client_impl::<RawService>(service, Some(chosen.type_info.clone()))
                        .build()
                        .map_err(|e| WriteError::Bus(e.to_string()))?,
                );
                insert_capped(&self.clients, key, c.clone());
                c
            }
        };
        let response = tokio::time::timeout(timeout, client.call(&RawCdr(cdr)))
            .await
            .map_err(|_| WriteError::Timeout(timeout))?
            .map_err(|e| {
                // hiroz's querier gives up on its own after ~10s.
                let msg = e.to_string();
                if msg.contains("before any response") {
                    WriteError::Timeout(timeout)
                } else {
                    WriteError::Bus(msg)
                }
            })?;
        let decoded = DynamicMessage::from_cdr(&response.0, &res_schema)
            .map_err(|e| WriteError::Bus(format!("cannot decode the response: {e}")))?;
        Ok(Called {
            type_name: ros_type_name_from_dds(&chosen.type_info.name),
            response: message_to_json(&decoded),
        })
    }
}

/// An unused write publisher is undeclared after this long.
const PUBLISHER_IDLE: Duration = Duration::from_secs(60);
/// A failed type discovery is not retried for this long (each attempt can
/// cost several round trips).
const DISCOVERY_FAILURE_TTL: Duration = Duration::from_secs(10);

impl HirozBus {
    /// Run a discovery, bounded to twice `discovery_timeout` overall, with
    /// recent failures served from a short-lived cache.
    async fn bounded_discovery<T>(
        &self,
        target: &str,
        type_info: &TypeInfo,
        fut: impl std::future::Future<Output = Result<T, String>>,
    ) -> Result<T, WriteError> {
        let key = (target.to_owned(), type_info.hash.to_rihs_string());
        if let Some((reason, at)) = lock(&self.discovery_failures).get(&key)
            && at.elapsed() < DISCOVERY_FAILURE_TTL
        {
            return Err(WriteError::Schema {
                target: target.to_owned(),
                reason: format!("{reason} (cached; retried after {DISCOVERY_FAILURE_TTL:?})"),
            });
        }
        let reason = match tokio::time::timeout(self.cfg.discovery_timeout * 2, fut).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(reason)) => reason,
            Err(_) => format!(
                "type discovery took longer than {:?}",
                self.cfg.discovery_timeout * 2
            ),
        };
        insert_capped(
            &self.discovery_failures,
            key,
            (reason.clone(), std::time::Instant::now()),
        );
        Err(WriteError::Schema {
            target: target.to_owned(),
            reason,
        })
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn insert_capped<K: std::hash::Hash + Eq, V>(m: &Mutex<HashMap<K, V>>, k: K, v: V) {
    let mut map = lock(m);
    if map.len() >= CACHE_CAP {
        map.clear();
    }
    map.insert(k, v);
}

/// The message type of field `name`, if it is a nested message.
fn nested(schema: &MessageSchema, name: &str) -> Option<Arc<MessageSchema>> {
    match &schema.field(name)?.field_type {
        hiroz::dynamic::FieldType::Message(m) => Some(m.clone()),
        _ => None,
    }
}

/// The endpoints a subscription, publish or call binds to: those advertising the most common
/// (type, hash), ties broken by hash for determinism.
#[derive(Debug, Clone, PartialEq)]
struct Chosen {
    type_info: TypeInfo,
    /// How many distinct (type, hash) pairs the endpoints advertise.
    distinct_types: usize,
    /// How many endpoints advertise the chosen one.
    members: usize,
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
    let distinct_types = groups.len();
    let (type_info, members) = groups.into_iter().next()?;
    let count = members.len();
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
        distinct_types,
        members: count,
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
