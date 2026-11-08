//! Conversions from hiroz types to `pipes-core` types. Pure functions, so they
//! are unit-tested without a session.

use hiroz::{
    dynamic::{FieldType as HFieldType, MessageSchema, ros_type_name_from_dds},
    entity::{EndpointEntity, EndpointKind as HKind},
};
use hiroz_protocol::qos::{QosDurability, QosHistory, QosReliability};
use pipes_core::{
    graph::{Endpoint, EndpointKind, Qos},
    schema::{Field, FieldType, Schema},
};

/// A hiroz schema as a `pipes-core` schema.
pub fn schema(s: &MessageSchema) -> Schema {
    Schema {
        type_name: s.type_name.clone(),
        fields: s
            .fields
            .iter()
            .map(|f| Field {
                name: f.name.clone(),
                field_type: field_type(&f.field_type),
            })
            .collect(),
    }
}

fn field_type(t: &HFieldType) -> FieldType {
    match t {
        HFieldType::Bool => FieldType::Bool,
        HFieldType::Int8 => FieldType::Int8,
        HFieldType::Int16 => FieldType::Int16,
        HFieldType::Int32 => FieldType::Int32,
        HFieldType::Int64 => FieldType::Int64,
        HFieldType::Uint8 => FieldType::Uint8,
        HFieldType::Uint16 => FieldType::Uint16,
        HFieldType::Uint32 => FieldType::Uint32,
        HFieldType::Uint64 => FieldType::Uint64,
        HFieldType::Float32 => FieldType::Float32,
        HFieldType::Float64 => FieldType::Float64,
        HFieldType::String => FieldType::String { max_len: None },
        HFieldType::BoundedString(n) => FieldType::String { max_len: Some(*n) },
        HFieldType::Message(m) => FieldType::Message {
            schema: Box::new(schema(m)),
        },
        HFieldType::Array(e, n) => FieldType::Array {
            element: Box::new(field_type(e)),
            len: *n,
        },
        HFieldType::Sequence(e) => FieldType::Sequence {
            element: Box::new(field_type(e)),
            max_len: None,
        },
        HFieldType::BoundedSequence(e, n) => FieldType::Sequence {
            element: Box::new(field_type(e)),
            max_len: Some(*n),
        },
    }
}

/// A hiroz graph endpoint as a `pipes-core` endpoint. Endpoints without type
/// information cannot be subscribed to or checked for mismatches, so they are
/// dropped.
pub fn endpoint(e: &EndpointEntity) -> Option<Endpoint> {
    let type_info = e.type_info.as_ref()?;
    let kind = match e.kind {
        HKind::Publisher => EndpointKind::Publisher,
        HKind::Subscription => EndpointKind::Subscription,
        HKind::Service => EndpointKind::ServiceServer,
        HKind::Client => EndpointKind::ServiceClient,
    };
    Some(Endpoint {
        kind,
        name: e.topic.clone(),
        type_name: ros_type_name_from_dds(&type_info.name),
        type_hash: type_info.hash.to_rihs_string(),
        node: e.node.as_ref().map(|n| fqn(&n.namespace, &n.name)),
        qos: Qos {
            reliable: e.qos.reliability == QosReliability::Reliable,
            transient_local: e.qos.durability == QosDurability::TransientLocal,
            depth: match e.qos.history {
                QosHistory::KeepLast(d) => Some(d),
                QosHistory::KeepAll => None,
            },
        },
    })
}

/// `/ns` + `name` → `/ns/name`; root namespace → `/name`.
pub fn fqn(namespace: &str, name: &str) -> String {
    let ns = namespace.trim_end_matches('/');
    if ns.is_empty() {
        format!("/{name}")
    } else if ns.starts_with('/') {
        format!("{ns}/{name}")
    } else {
        format!("/{ns}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use hiroz::{
        dynamic::{FieldType as H, MessageSchema},
        entity::{EndpointEntity, EndpointKind, NodeEntity, TypeHash, TypeInfo},
    };
    use pipes_core::{graph, schema::FieldType};

    use super::*;

    #[test]
    fn fqn_joins_namespaces() {
        assert_eq!(fqn("/", "talker"), "/talker");
        assert_eq!(fqn("", "talker"), "/talker");
        assert_eq!(fqn("/fleet", "a"), "/fleet/a");
        assert_eq!(fqn("/fleet/", "a"), "/fleet/a");
        assert_eq!(fqn("fleet", "a"), "/fleet/a");
    }

    #[test]
    fn schema_maps_every_field_type() {
        let inner = MessageSchema::builder("x/msg/In")
            .field("v", H::Bool)
            .build()
            .unwrap();
        let s = MessageSchema::builder("x/msg/Out")
            .field("s", H::String)
            .field("bs", H::BoundedString(4))
            .field("m", H::Message(inner))
            .field("a", H::Array(Box::new(H::Uint8), 2))
            .field("q", H::Sequence(Box::new(H::Int64)))
            .field("bq", H::BoundedSequence(Box::new(H::Float32), 3))
            .build()
            .unwrap();
        let out = schema(&s);
        let types: Vec<&FieldType> = out.fields.iter().map(|f| &f.field_type).collect();
        assert_eq!(types[0], &FieldType::String { max_len: None });
        assert_eq!(types[1], &FieldType::String { max_len: Some(4) });
        assert!(
            matches!(types[2], FieldType::Message { schema } if schema.type_name == "x/msg/In")
        );
        assert!(types[3].is_byte_blob());
        assert_eq!(
            types[4],
            &FieldType::Sequence {
                element: Box::new(FieldType::Int64),
                max_len: None
            }
        );
        assert_eq!(
            types[5],
            &FieldType::Sequence {
                element: Box::new(FieldType::Float32),
                max_len: Some(3)
            }
        );
    }

    #[test]
    fn endpoint_maps_names_hash_node_and_qos() {
        let hash = TypeHash::from_rihs_string(
            "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
        )
        .unwrap();
        let qos = hiroz_protocol::qos::QosProfile {
            durability: QosDurability::TransientLocal,
            history: QosHistory::KeepLast(1),
            ..Default::default()
        };
        let node = NodeEntity::new(
            0,
            "1234567890abcdef1234567890abcdef".parse().unwrap(),
            1,
            "talker".into(),
            "/demo".into(),
            String::new(),
        );
        let e = EndpointEntity {
            id: 7,
            node: Some(node),
            kind: EndpointKind::Publisher,
            topic: "/chatter".into(),
            type_info: Some(TypeInfo::new("std_msgs::msg::dds_::String_", hash)),
            qos,
        };
        let ep = endpoint(&e).unwrap();
        assert_eq!(ep.kind, graph::EndpointKind::Publisher);
        assert_eq!(ep.name, "/chatter");
        assert_eq!(ep.type_name, "std_msgs/msg/String");
        assert!(ep.type_hash.starts_with("RIHS01_df668c74"));
        assert_eq!(ep.node.as_deref(), Some("/demo/talker"));
        assert_eq!(
            ep.qos,
            graph::Qos {
                reliable: true,
                transient_local: true,
                depth: Some(1)
            }
        );

        let untyped = EndpointEntity {
            type_info: None,
            ..e
        };
        assert!(endpoint(&untyped).is_none());
    }
}
