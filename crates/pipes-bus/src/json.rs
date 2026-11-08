//! hiroz dynamic messages → JSON, as specified in `docs/json-mapping.md`.
//!
//! The mapping is driven by the schema, not just the value, so that e.g. an
//! empty `uint8[]` is still the empty base64 string `""` rather than `[]`.
//! When a value does not match its declared type (which would be a hiroz bug)
//! the value's own shape wins, so decoding never panics.

use base64::Engine as _;
use hiroz::dynamic::{DynamicMessage, DynamicValue, FieldType};
use serde_json::{Map, Number, Value};

/// Convert a decoded message to JSON.
pub fn message_to_json(msg: &DynamicMessage) -> Value {
    let mut obj = Map::with_capacity(msg.field_count());
    for (field, value) in msg.schema().fields.iter().zip(msg.values()) {
        obj.insert(field.name.clone(), value_to_json(&field.field_type, value));
    }
    Value::Object(obj)
}

fn value_to_json(ty: &FieldType, value: &DynamicValue) -> Value {
    match value {
        DynamicValue::Bool(b) => Value::Bool(*b),
        DynamicValue::Int8(v) => Value::from(*v),
        DynamicValue::Int16(v) => Value::from(*v),
        DynamicValue::Int32(v) => Value::from(*v),
        DynamicValue::Uint8(v) => Value::from(*v),
        DynamicValue::Uint16(v) => Value::from(*v),
        DynamicValue::Uint32(v) => Value::from(*v),
        // 64-bit integers are strings: JSON numbers lose precision above 2^53.
        DynamicValue::Int64(v) => Value::String(v.to_string()),
        DynamicValue::Uint64(v) => Value::String(v.to_string()),
        DynamicValue::Float32(v) => float32(*v),
        DynamicValue::Float64(v) => float64(*v),
        DynamicValue::String(s) => Value::String(s.clone()),
        DynamicValue::Bytes(b) => Value::String(base64(b)),
        DynamicValue::Message(m) => message_to_json(m),
        DynamicValue::Array(items) => {
            let element = ty.element_type();
            if element == Some(&FieldType::Uint8) {
                let bytes: Option<Vec<u8>> = items
                    .iter()
                    .map(|v| match v {
                        DynamicValue::Uint8(b) => Some(*b),
                        _ => None,
                    })
                    .collect();
                if let Some(bytes) = bytes {
                    return Value::String(base64(&bytes));
                }
            }
            let element = element.unwrap_or(ty);
            Value::Array(items.iter().map(|v| value_to_json(element, v)).collect())
        }
    }
}

fn base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// `f32` values are emitted with their shortest round-trip decimal form, so
/// `1.1f32` becomes `1.1` rather than `1.100000023841858`.
fn float32(v: f32) -> Value {
    if !v.is_finite() {
        return non_finite(f64::from(v));
    }
    v.to_string()
        .parse::<f64>()
        .ok()
        .and_then(Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

fn float64(v: f64) -> Value {
    Number::from_f64(v).map_or_else(|| non_finite(v), Value::Number)
}

/// JSON has no NaN or infinities; they become strings.
fn non_finite(v: f64) -> Value {
    let s = if v.is_nan() {
        "NaN"
    } else if v > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    };
    Value::String(s.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hiroz::dynamic::{DynamicMessage, DynamicValue, FieldType, MessageSchema};
    use serde_json::json;

    use super::message_to_json;

    fn point() -> Arc<MessageSchema> {
        MessageSchema::builder("geometry_msgs/msg/Point")
            .field("x", FieldType::Float64)
            .field("y", FieldType::Float64)
            .field("z", FieldType::Float64)
            .build()
            .unwrap()
    }

    /// One field of every hiroz `FieldType`, each element type of a
    /// collection, and the edge values the mapping singles out.
    fn everything() -> Arc<MessageSchema> {
        let u8_ = || Box::new(FieldType::Uint8);
        MessageSchema::builder("pipes_test_msgs/msg/Everything")
            .field("b", FieldType::Bool)
            .field("i8", FieldType::Int8)
            .field("i16", FieldType::Int16)
            .field("i32", FieldType::Int32)
            .field("i64", FieldType::Int64)
            .field("u8", FieldType::Uint8)
            .field("u16", FieldType::Uint16)
            .field("u32", FieldType::Uint32)
            .field("u64", FieldType::Uint64)
            .field("f32", FieldType::Float32)
            .field("f64", FieldType::Float64)
            .field("f64_nan", FieldType::Float64)
            .field("f32_neg_inf", FieldType::Float32)
            .field("s", FieldType::String)
            .field("bs", FieldType::BoundedString(8))
            .field("nested", FieldType::Message(point()))
            .field("fixed_i32", FieldType::Array(Box::new(FieldType::Int32), 3))
            .field("fixed_u8", FieldType::Array(u8_(), 4))
            .field("seq_u8", FieldType::Sequence(u8_()))
            .field("seq_u8_empty", FieldType::Sequence(u8_()))
            .field("bseq_u8", FieldType::BoundedSequence(u8_(), 4))
            .field("seq_i64", FieldType::Sequence(Box::new(FieldType::Int64)))
            .field("seq_str", FieldType::Sequence(Box::new(FieldType::String)))
            .field(
                "seq_msg",
                FieldType::Sequence(Box::new(FieldType::Message(point()))),
            )
            .field(
                "bseq_f32",
                FieldType::BoundedSequence(Box::new(FieldType::Float32), 2),
            )
            .build()
            .unwrap()
    }

    fn pt(x: f64, y: f64, z: f64) -> DynamicValue {
        let mut m = DynamicMessage::new(&point());
        m.set("x", x).unwrap();
        m.set("y", y).unwrap();
        m.set("z", z).unwrap();
        DynamicValue::Message(Box::new(m))
    }

    /// Build a message, put it through CDR (as it arrives off the wire), and
    /// check the JSON. Exercises hiroz's decoder, not just our walker.
    #[test]
    fn every_field_type_maps_as_documented() {
        let schema = everything();
        let mut m = DynamicMessage::new(&schema);
        let set = |m: &mut DynamicMessage, k: &str, v: DynamicValue| m.set_dynamic(k, v).unwrap();
        set(&mut m, "b", DynamicValue::Bool(true));
        set(&mut m, "i8", DynamicValue::Int8(-8));
        set(&mut m, "i16", DynamicValue::Int16(-16));
        set(&mut m, "i32", DynamicValue::Int32(-32));
        set(&mut m, "i64", DynamicValue::Int64(-9_007_199_254_740_993)); // 2^53 + 1
        set(&mut m, "u8", DynamicValue::Uint8(255));
        set(&mut m, "u16", DynamicValue::Uint16(65535));
        set(&mut m, "u32", DynamicValue::Uint32(4_294_967_295));
        set(&mut m, "u64", DynamicValue::Uint64(u64::MAX));
        set(&mut m, "f32", DynamicValue::Float32(1.1));
        set(&mut m, "f64", DynamicValue::Float64(-0.25));
        set(&mut m, "f64_nan", DynamicValue::Float64(f64::NAN));
        set(
            &mut m,
            "f32_neg_inf",
            DynamicValue::Float32(f32::NEG_INFINITY),
        );
        set(&mut m, "s", DynamicValue::String("héllo \"q\"".into()));
        set(&mut m, "bs", DynamicValue::String("short".into()));
        set(&mut m, "nested", pt(1.0, 2.0, 3.0));
        set(
            &mut m,
            "fixed_i32",
            DynamicValue::Array(vec![
                DynamicValue::Int32(1),
                DynamicValue::Int32(2),
                DynamicValue::Int32(3),
            ]),
        );
        set(
            &mut m,
            "fixed_u8",
            DynamicValue::Array([0xde, 0xad, 0xbe, 0xef].map(DynamicValue::Uint8).to_vec()),
        );
        set(&mut m, "seq_u8", DynamicValue::Bytes(b"hello".to_vec()));
        set(&mut m, "seq_u8_empty", DynamicValue::Bytes(vec![]));
        set(
            &mut m,
            "bseq_u8",
            DynamicValue::Array(vec![DynamicValue::Uint8(1), DynamicValue::Uint8(2)]),
        );
        set(
            &mut m,
            "seq_i64",
            DynamicValue::Array(vec![DynamicValue::Int64(1), DynamicValue::Int64(i64::MIN)]),
        );
        set(
            &mut m,
            "seq_str",
            DynamicValue::Array(vec![
                DynamicValue::String("a".into()),
                DynamicValue::String("".into()),
            ]),
        );
        set(
            &mut m,
            "seq_msg",
            DynamicValue::Array(vec![pt(0.5, 0.0, 0.0), pt(0.0, 0.0, -1.0)]),
        );
        set(
            &mut m,
            "bseq_f32",
            DynamicValue::Array(vec![DynamicValue::Float32(0.1)]),
        );

        let cdr = m.to_cdr().unwrap();
        let decoded = DynamicMessage::from_cdr(&cdr, &schema).unwrap();
        let json = message_to_json(&decoded);

        assert_eq!(
            json,
            json!({
                "b": true,
                "i8": -8, "i16": -16, "i32": -32,
                "i64": "-9007199254740993",
                "u8": 255, "u16": 65535, "u32": 4_294_967_295u32,
                "u64": "18446744073709551615",
                "f32": 1.1,
                "f64": -0.25,
                "f64_nan": "NaN",
                "f32_neg_inf": "-Infinity",
                "s": "héllo \"q\"",
                "bs": "short",
                "nested": {"x": 1.0, "y": 2.0, "z": 3.0},
                "fixed_i32": [1, 2, 3],
                "fixed_u8": "3q2+7w==",
                "seq_u8": "aGVsbG8=",
                "seq_u8_empty": "",
                "bseq_u8": "AQI=",
                "seq_i64": ["1", "-9223372036854775808"],
                "seq_str": ["a", ""],
                "seq_msg": [{"x": 0.5, "y": 0.0, "z": 0.0}, {"x": 0.0, "y": 0.0, "z": -1.0}],
                "bseq_f32": [0.1],
            })
        );
        // Field order follows the message definition.
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let fields: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(keys, fields);
    }
}
