//! Fetch a publisher's message definition from its node's standard
//! `~/get_type_description` service, and convert it to a hiroz schema.
//!
//! hiroz 0.2.1 has this built in (`ZNode::discover_topic_schema`), but its
//! converter does not know the `byte`, `char`, bounded-string and
//! fixed-string type ids, so it fails on common messages such as
//! `diagnostic_msgs/DiagnosticStatus` or anything with `byte[]`. We use hiroz's
//! public service type and wire structs, and own only the id table below.
//! Remove this module once hiroz maps every id.

use std::{collections::HashMap, sync::Arc, time::Duration};

use hiroz::{
    Builder,
    dynamic::{
        FieldType, GetTypeDescription, GetTypeDescriptionRequest, MessageSchema,
        WireIndividualTypeDescription, WireTypeDescription,
    },
    node::ZNode,
};

/// ROS 2 `type_description_interfaces/msg/FieldType` ids. Collections add
/// [`ARRAY`], [`BOUNDED_SEQUENCE`] or [`UNBOUNDED_SEQUENCE`] to the base id.
pub mod id {
    #![allow(missing_docs)]
    pub const NESTED_TYPE: u8 = 1;
    pub const INT8: u8 = 2;
    pub const UINT8: u8 = 3;
    pub const INT16: u8 = 4;
    pub const UINT16: u8 = 5;
    pub const INT32: u8 = 6;
    pub const UINT32: u8 = 7;
    pub const INT64: u8 = 8;
    pub const UINT64: u8 = 9;
    pub const FLOAT: u8 = 10;
    pub const DOUBLE: u8 = 11;
    pub const LONG_DOUBLE: u8 = 12;
    pub const CHAR: u8 = 13;
    pub const WCHAR: u8 = 14;
    pub const BOOLEAN: u8 = 15;
    pub const BYTE: u8 = 16;
    pub const STRING: u8 = 17;
    pub const WSTRING: u8 = 18;
    pub const FIXED_STRING: u8 = 19;
    pub const FIXED_WSTRING: u8 = 20;
    pub const BOUNDED_STRING: u8 = 21;
    pub const BOUNDED_WSTRING: u8 = 22;
    pub const ARRAY: u8 = 48;
    pub const BOUNDED_SEQUENCE: u8 = 96;
    pub const UNBOUNDED_SEQUENCE: u8 = 144;
}

/// Ask node `namespace`/`name` for the definition of `type_name` with hash
/// `type_hash` (both as advertised in the graph).
pub async fn fetch(
    node: &ZNode,
    namespace: &str,
    name: &str,
    type_name: &str,
    type_hash: &str,
    timeout: Duration,
) -> Result<Arc<MessageSchema>, String> {
    let service = format!(
        "{}/get_type_description",
        crate::convert::fqn(namespace, name)
    );
    let client = node
        .create_client::<GetTypeDescription>(&service)
        .build()
        .map_err(|e| format!("cannot create a client for {service}: {e}"))?;
    let request = GetTypeDescriptionRequest {
        type_name: type_name.to_owned(),
        type_hash: type_hash.to_owned(),
        include_type_sources: false,
    };
    let response = match tokio::time::timeout(timeout, client.call(&request)).await {
        Err(_) => {
            return Err(format!(
                "no answer from {service} within {timeout:?} (is the node spinning, and is its type description service enabled?)"
            ));
        }
        Ok(Err(e)) => return Err(format!("{service} failed: {e}")),
        Ok(Ok(r)) => r,
    };
    if !response.successful {
        return Err(format!(
            "{service} refused {type_name}: {}",
            response.failure_reason
        ));
    }
    // Don't trust a node to describe a different type than asked for: the
    // result is cached by (type, hash) and used for everyone's writes.
    let described = &response.type_description.type_description.type_name;
    if described != type_name {
        return Err(format!(
            "{service} described `{described}` when asked for `{type_name}`"
        ));
    }
    to_schema(&response.type_description)
}

/// Convert a wire type description (the main type plus every referenced type)
/// to a hiroz schema.
pub fn to_schema(td: &WireTypeDescription) -> Result<Arc<MessageSchema>, String> {
    let referenced: HashMap<&str, &WireIndividualTypeDescription> = td
        .referenced_type_descriptions
        .iter()
        .map(|t| (t.type_name.as_str(), t))
        .collect();
    let mut done = HashMap::new();
    convert(
        &td.type_description,
        &referenced,
        &mut done,
        &mut Vec::new(),
    )
}

fn convert(
    t: &WireIndividualTypeDescription,
    referenced: &HashMap<&str, &WireIndividualTypeDescription>,
    done: &mut HashMap<String, Arc<MessageSchema>>,
    stack: &mut Vec<String>,
) -> Result<Arc<MessageSchema>, String> {
    if let Some(s) = done.get(&t.type_name) {
        return Ok(s.clone());
    }
    if stack.contains(&t.type_name) {
        return Err(format!("recursive type {}", t.type_name));
    }
    stack.push(t.type_name.clone());
    // hiroz's schema builder only accepts `pkg/msg/Name`; service and action
    // parts (`pkg/srv/Name_Request`, `pkg/action/Name_Goal`) decode the same,
    // so name them as messages.
    let schema_name = t
        .type_name
        .replacen("/srv/", "/msg/", 1)
        .replacen("/action/", "/msg/", 1);
    let mut builder = MessageSchema::builder(&schema_name);
    for f in &t.fields {
        let ft = &f.field_type;
        let (base, collection) = split(ft.type_id);
        let base_type = match base {
            id::NESTED_TYPE => {
                let nested = referenced
                    .get(ft.nested_type_name.as_str())
                    .ok_or_else(|| {
                        format!(
                            "{}.{}: referenced type {} is missing",
                            t.type_name, f.name, ft.nested_type_name
                        )
                    })?;
                FieldType::Message(convert(nested, referenced, done, stack)?)
            }
            id::INT8 => FieldType::Int8,
            id::UINT8 | id::BYTE | id::CHAR => FieldType::Uint8,
            id::INT16 => FieldType::Int16,
            id::UINT16 => FieldType::Uint16,
            id::INT32 => FieldType::Int32,
            id::UINT32 => FieldType::Uint32,
            id::INT64 => FieldType::Int64,
            id::UINT64 => FieldType::Uint64,
            id::FLOAT => FieldType::Float32,
            id::DOUBLE => FieldType::Float64,
            id::BOOLEAN => FieldType::Bool,
            id::STRING if ft.string_capacity == 0 => FieldType::String,
            id::STRING | id::BOUNDED_STRING | id::FIXED_STRING => FieldType::BoundedString(
                usize::try_from(ft.string_capacity).map_err(|e| e.to_string())?,
            ),
            id::LONG_DOUBLE | id::WCHAR | id::WSTRING | id::FIXED_WSTRING | id::BOUNDED_WSTRING => {
                return Err(format!(
                    "{}.{}: type id {base} (long double / wide char / wide string) is not supported",
                    t.type_name, f.name
                ));
            }
            other => {
                return Err(format!(
                    "{}.{}: unknown type id {other}",
                    t.type_name, f.name
                ));
            }
        };
        let capacity = usize::try_from(ft.capacity).map_err(|e| e.to_string())?;
        let field_type = match collection {
            Collection::Single => base_type,
            Collection::Array => FieldType::Array(Box::new(base_type), capacity),
            Collection::Bounded => FieldType::BoundedSequence(Box::new(base_type), capacity),
            Collection::Unbounded => FieldType::Sequence(Box::new(base_type)),
        };
        builder = match parse_default(&field_type, &f.default_value) {
            Some(default) => builder.field_with_default(&f.name, field_type, default),
            None => builder.field(&f.name, field_type),
        };
    }
    stack.pop();
    let schema = builder
        .build()
        .map_err(|e| format!("{}: {e}", t.type_name))?;
    done.insert(t.type_name.clone(), schema.clone());
    Ok(schema)
}

/// A field's default from its type description (the `.msg` default, e.g.
/// `1.0` for `geometry_msgs/Quaternion.w`), so omitted fields in writes get
/// what ROS would give them. Unparseable defaults are ignored (zero/empty).
fn parse_default(ty: &FieldType, raw: &str) -> Option<hiroz::dynamic::DynamicValue> {
    use hiroz::dynamic::DynamicValue as V;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let unquote = |s: &str| -> String {
        let s = s.trim();
        let quoted = s.len() >= 2
            && ((s.starts_with('"') && s.ends_with('"'))
                || (s.starts_with('\'') && s.ends_with('\'')));
        if quoted {
            s[1..s.len() - 1].to_owned()
        } else {
            s.to_owned()
        }
    };
    let scalar = |t: &FieldType, s: &str| -> Option<V> {
        let s = s.trim();
        Some(match t {
            FieldType::Bool => V::Bool(match s.to_ascii_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return None,
            }),
            FieldType::Int8 => V::Int8(s.parse().ok()?),
            FieldType::Int16 => V::Int16(s.parse().ok()?),
            FieldType::Int32 => V::Int32(s.parse().ok()?),
            FieldType::Int64 => V::Int64(s.parse().ok()?),
            FieldType::Uint8 => V::Uint8(s.parse().ok()?),
            FieldType::Uint16 => V::Uint16(s.parse().ok()?),
            FieldType::Uint32 => V::Uint32(s.parse().ok()?),
            FieldType::Uint64 => V::Uint64(s.parse().ok()?),
            FieldType::Float32 => V::Float32(s.parse().ok()?),
            FieldType::Float64 => V::Float64(s.parse().ok()?),
            FieldType::String | FieldType::BoundedString(_) => V::String(unquote(s)),
            _ => return None,
        })
    };
    match ty {
        FieldType::Array(e, _) | FieldType::Sequence(e) | FieldType::BoundedSequence(e, _) => {
            let inner = raw.strip_prefix('[')?.strip_suffix(']')?;
            let items: Option<Vec<V>> = if inner.trim().is_empty() {
                Some(Vec::new())
            } else {
                inner.split(',').map(|i| scalar(e, i)).collect()
            };
            let items = items?;
            if let FieldType::Array(_, n) = ty
                && items.len() != *n
            {
                return None;
            }
            if matches!(ty, FieldType::Sequence(_)) && **e == FieldType::Uint8 {
                return Some(V::Bytes(
                    items.into_iter().filter_map(|v| v.as_u8()).collect(),
                ));
            }
            Some(V::Array(items))
        }
        FieldType::Message(_) => None,
        t => scalar(t, raw),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Collection {
    Single,
    Array,
    Bounded,
    Unbounded,
}

fn split(type_id: u8) -> (u8, Collection) {
    if type_id > id::UNBOUNDED_SEQUENCE {
        (type_id - id::UNBOUNDED_SEQUENCE, Collection::Unbounded)
    } else if type_id > id::BOUNDED_SEQUENCE {
        (type_id - id::BOUNDED_SEQUENCE, Collection::Bounded)
    } else if type_id > id::ARRAY {
        (type_id - id::ARRAY, Collection::Array)
    } else {
        (type_id, Collection::Single)
    }
}

#[cfg(test)]
mod tests {
    use hiroz::dynamic::{
        FieldType as H, WireField, WireFieldType, WireIndividualTypeDescription,
        WireTypeDescription,
    };

    use super::{id, to_schema};

    fn field(
        name: &str,
        type_id: u8,
        capacity: u64,
        string_capacity: u64,
        nested: &str,
    ) -> WireField {
        WireField {
            name: name.into(),
            field_type: WireFieldType {
                type_id,
                capacity,
                string_capacity,
                nested_type_name: nested.into(),
            },
            default_value: String::new(),
        }
    }

    fn td(
        fields: Vec<WireField>,
        referenced: Vec<WireIndividualTypeDescription>,
    ) -> WireTypeDescription {
        WireTypeDescription {
            type_description: WireIndividualTypeDescription {
                type_name: "pipes_test_msgs/msg/All".into(),
                fields,
            },
            referenced_type_descriptions: referenced,
        }
    }

    #[test]
    fn maps_every_supported_id_and_collection() {
        let point = WireIndividualTypeDescription {
            type_name: "geometry_msgs/msg/Point".into(),
            fields: vec![field("x", id::DOUBLE, 0, 0, "")],
        };
        let t = td(
            vec![
                field("i8", id::INT8, 0, 0, ""),
                field("u8", id::UINT8, 0, 0, ""),
                field("i16", id::INT16, 0, 0, ""),
                field("u16", id::UINT16, 0, 0, ""),
                field("i32", id::INT32, 0, 0, ""),
                field("u32", id::UINT32, 0, 0, ""),
                field("i64", id::INT64, 0, 0, ""),
                field("u64", id::UINT64, 0, 0, ""),
                field("f32", id::FLOAT, 0, 0, ""),
                field("f64", id::DOUBLE, 0, 0, ""),
                field("c", id::CHAR, 0, 0, ""),
                field("b", id::BOOLEAN, 0, 0, ""),
                field("byte", id::BYTE, 0, 0, ""),
                field("s", id::STRING, 0, 0, ""),
                field("fs", id::FIXED_STRING, 0, 8, ""),
                field("bs", id::BOUNDED_STRING, 0, 16, ""),
                field("p", id::NESTED_TYPE, 0, 0, "geometry_msgs/msg/Point"),
                field("bytes", id::BYTE + id::UNBOUNDED_SEQUENCE, 0, 0, ""),
                field("fixed", id::INT32 + id::ARRAY, 3, 0, ""),
                field(
                    "bounded",
                    id::BOUNDED_STRING + id::BOUNDED_SEQUENCE,
                    4,
                    10,
                    "",
                ),
                field(
                    "points",
                    id::NESTED_TYPE + id::UNBOUNDED_SEQUENCE,
                    0,
                    0,
                    "geometry_msgs/msg/Point",
                ),
            ],
            vec![point],
        );
        let s = to_schema(&t).unwrap();
        let ty = |n: &str| s.field(n).unwrap().field_type.clone();
        assert_eq!(ty("i8"), H::Int8);
        assert_eq!(ty("u8"), H::Uint8);
        assert_eq!(ty("i16"), H::Int16);
        assert_eq!(ty("u16"), H::Uint16);
        assert_eq!(ty("i32"), H::Int32);
        assert_eq!(ty("u32"), H::Uint32);
        assert_eq!(ty("i64"), H::Int64);
        assert_eq!(ty("u64"), H::Uint64);
        assert_eq!(ty("f32"), H::Float32);
        assert_eq!(ty("f64"), H::Float64);
        assert_eq!(ty("c"), H::Uint8);
        assert_eq!(ty("b"), H::Bool);
        assert_eq!(ty("byte"), H::Uint8);
        assert_eq!(ty("s"), H::String);
        assert_eq!(ty("fs"), H::BoundedString(8));
        assert_eq!(ty("bs"), H::BoundedString(16));
        assert!(matches!(ty("p"), H::Message(m) if m.type_name == "geometry_msgs/msg/Point"));
        assert_eq!(ty("bytes"), H::Sequence(Box::new(H::Uint8)));
        assert_eq!(ty("fixed"), H::Array(Box::new(H::Int32), 3));
        assert_eq!(
            ty("bounded"),
            H::BoundedSequence(Box::new(H::BoundedString(10)), 4)
        );
        assert!(matches!(ty("points"), H::Sequence(e) if matches!(*e, H::Message(_))));
        // Field order is preserved.
        let names: Vec<&str> = s.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names.first(), Some(&"i8"));
        assert_eq!(names.last(), Some(&"points"));
    }

    #[test]
    fn rejects_wide_and_unknown_types_with_the_field_name() {
        for (type_id, what) in [
            (id::WSTRING, "not supported"),
            (id::LONG_DOUBLE, "not supported"),
            (99 - id::BOUNDED_SEQUENCE + 40, "unknown"),
        ] {
            let err = to_schema(&td(vec![field("bad", type_id, 0, 0, "")], vec![])).unwrap_err();
            assert!(
                err.contains("pipes_test_msgs/msg/All.bad") && err.contains(what),
                "{type_id}: {err}"
            );
        }
    }

    #[test]
    fn service_part_types_are_accepted() {
        let req = WireIndividualTypeDescription {
            type_name: "example_interfaces/srv/AddTwoInts_Request".into(),
            fields: vec![field("a", id::INT64, 0, 0, "")],
        };
        let t = WireTypeDescription {
            type_description: WireIndividualTypeDescription {
                type_name: "example_interfaces/srv/AddTwoInts".into(),
                fields: vec![field(
                    "request_message",
                    id::NESTED_TYPE,
                    0,
                    0,
                    "example_interfaces/srv/AddTwoInts_Request",
                )],
            },
            referenced_type_descriptions: vec![req],
        };
        let s = to_schema(&t).unwrap();
        assert!(
            matches!(&s.field("request_message").unwrap().field_type, H::Message(m) if m.field("a").is_some())
        );
    }

    #[test]
    fn message_defaults_are_kept() {
        let mut w = field("w", id::DOUBLE, 0, 0, "");
        w.default_value = "1.0".into();
        let mut name = field("name", id::STRING, 0, 0, "");
        name.default_value = "\"tiny\"".into();
        let mut flags = field("flags", id::BOOLEAN + id::ARRAY, 2, 0, "");
        flags.default_value = "[true, false]".into();
        let mut junk = field("junk", id::INT32, 0, 0, "");
        junk.default_value = "not a number".into();
        let s = to_schema(&td(vec![w, name, flags, junk], vec![])).unwrap();
        let m = hiroz::dynamic::DynamicMessage::new(&s);
        assert_eq!(m.get::<f64>("w").unwrap(), 1.0);
        assert_eq!(m.get::<String>("name").unwrap(), "tiny");
        assert_eq!(
            m.get::<i32>("junk").unwrap(),
            0,
            "unparseable defaults fall back to zero"
        );
    }

    #[test]
    fn missing_referenced_type_is_an_error() {
        let err = to_schema(&td(
            vec![field("p", id::NESTED_TYPE, 0, 0, "x/msg/Gone")],
            vec![],
        ))
        .unwrap_err();
        assert!(err.contains("x/msg/Gone"), "{err}");
    }
}
