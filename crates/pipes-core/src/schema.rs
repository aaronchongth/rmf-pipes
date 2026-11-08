//! Message schemas in a bus-agnostic form, served by the schema endpoint and
//! (later) turned into JSON Schema for OpenAPI. The JSON shape of a value of
//! each field type is defined in `docs/json-mapping.md`.

use serde::Serialize;

/// A ROS message type's structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Schema {
    /// e.g. `geometry_msgs/msg/Twist`.
    pub type_name: String,
    /// Fields in declaration order.
    pub fields: Vec<Field>,
}

/// One field of a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Field {
    /// Field name.
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
}

/// Field types, mirroring ROS 2 IDL as supported by hiroz. Primitive variants
/// are named after their IDL type.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    String {
        /// `string<=N`; `None` when unbounded.
        max_len: Option<usize>,
    },
    Message {
        /// The nested message's schema.
        schema: Box<Schema>,
    },
    /// Fixed-size `T[N]`.
    Array {
        /// Element type.
        element: Box<FieldType>,
        /// Number of elements.
        len: usize,
    },
    /// `sequence<T>` or `sequence<T, N>`.
    Sequence {
        /// Element type.
        element: Box<FieldType>,
        /// `sequence<T, N>` bound; `None` when unbounded.
        max_len: Option<usize>,
    },
}

impl FieldType {
    /// Whether values of this type are encoded as a base64 string: any array or
    /// sequence of `uint8` (`byte[]`, `uint8[]`, `char[]` in ROS IDL).
    pub fn is_byte_blob(&self) -> bool {
        match self {
            Self::Array { element, .. } | Self::Sequence { element, .. } => {
                **element == FieldType::Uint8
            }
            _ => false,
        }
    }
}
