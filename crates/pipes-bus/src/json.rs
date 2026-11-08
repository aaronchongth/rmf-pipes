//! hiroz dynamic messages ↔ JSON, as specified in `docs/json-mapping.md`.
//! [`message_to_json`] serves samples; [`json_to_message`] (strict) builds
//! messages for the write path.
//!
//! The mapping is driven by the schema, not just the value, so that e.g. an
//! empty `uint8[]` is still the empty base64 string `""` rather than `[]`.
//! When a value does not match its declared type (which would be a hiroz bug)
//! the value's own shape wins, so decoding never panics.

use base64::Engine as _;
use std::sync::Arc;

use hiroz::dynamic::{DynamicMessage, DynamicValue, FieldType, MessageSchema};
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

/// Build a message from JSON following `docs/json-mapping.md`, strictly:
/// unknown fields, wrong types, out-of-range numbers and violated bounds are
/// errors naming the field path (e.g. `waypoints[2].x: expected a number`).
/// Omitted fields keep their defaults.
pub fn json_to_message(
    schema: &Arc<MessageSchema>,
    value: &Value,
) -> Result<DynamicMessage, String> {
    let mut budget = MAX_ELEMENTS;
    to_message(schema, value, &mut budget).map_err(|e| e.to_string())
}

/// Most messages plus collection elements one write may build. Bounds the
/// memory and CPU a request can cost (each element becomes a dynamic value),
/// independently of the HTTP body limit. `uint8` sequences count one per 64
/// bytes.
pub const MAX_ELEMENTS: usize = 100_000;

/// An error with the path to the offending field.
#[derive(Debug)]
struct PathError {
    /// Innermost last: field names and `[index]`.
    path: Vec<String>,
    msg: String,
}

impl PathError {
    fn new(msg: impl Into<String>) -> Self {
        Self {
            path: Vec::new(),
            msg: msg.into(),
        }
    }

    fn at(mut self, segment: String) -> Self {
        self.path.insert(0, segment);
        self
    }
}

impl From<String> for PathError {
    fn from(msg: String) -> Self {
        Self::new(msg)
    }
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut path = String::new();
        for seg in &self.path {
            if !seg.starts_with('[') && !path.is_empty() {
                path.push('.');
            }
            path.push_str(seg);
        }
        if path.is_empty() {
            write!(f, "{}", self.msg)
        } else {
            write!(f, "{path}: {}", self.msg)
        }
    }
}

fn spend(budget: &mut usize, n: usize) -> Result<(), PathError> {
    if *budget < n {
        return Err(PathError::new(format!(
            "the message is too large (more than {MAX_ELEMENTS} elements)"
        )));
    }
    *budget -= n;
    Ok(())
}

fn to_message(
    schema: &Arc<MessageSchema>,
    value: &Value,
    budget: &mut usize,
) -> Result<DynamicMessage, PathError> {
    spend(budget, 1)?;
    let Value::Object(obj) = value else {
        return Err(PathError::new(format!(
            "expected a JSON object for {}",
            schema.type_name
        )));
    };
    if let Some(unknown) = obj.keys().find(|k| schema.field(k).is_none()) {
        return Err(PathError::new(format!(
            "unknown field `{}` for {}",
            clean(unknown),
            schema.type_name
        )));
    }
    let mut msg = DynamicMessage::new(schema);
    for field in &schema.fields {
        if let Some(v) = obj.get(&field.name) {
            let dv = value_from_json(&field.field_type, v, budget)
                .map_err(|e| e.at(field.name.clone()))?;
            msg.set_dynamic(&field.name, dv)
                .map_err(|e| PathError::new(e.to_string()).at(field.name.clone()))?;
        }
    }
    Ok(msg)
}

/// Client text for error messages: short, without control characters.
fn clean(s: &str) -> String {
    let c: String = s.chars().filter(|c| !c.is_control()).take(64).collect();
    if s.chars().count() > 64 {
        format!("{c}…")
    } else {
        c
    }
}

fn string(v: &Value, bound: Option<usize>) -> Result<String, PathError> {
    let s = v
        .as_str()
        .ok_or_else(|| PathError::new("expected a string"))?;
    if s.contains('\0') {
        return Err(PathError::new("strings can't contain NUL"));
    }
    if let Some(n) = bound
        && s.len() > n
    {
        return Err(PathError::new(format!(
            "longer than its bound of {n} bytes"
        )));
    }
    Ok(s.to_owned())
}

fn value_from_json(
    ty: &FieldType,
    v: &Value,
    budget: &mut usize,
) -> Result<DynamicValue, PathError> {
    use FieldType as F;
    Ok(match ty {
        F::Bool => DynamicValue::Bool(
            v.as_bool()
                .ok_or_else(|| PathError::new("expected a boolean"))?,
        ),
        F::Int8 => DynamicValue::Int8(int(v)?),
        F::Int16 => DynamicValue::Int16(int(v)?),
        F::Int32 => DynamicValue::Int32(int(v)?),
        F::Int64 => DynamicValue::Int64(int64(v)?),
        F::Uint8 => DynamicValue::Uint8(int(v)?),
        F::Uint16 => DynamicValue::Uint16(int(v)?),
        F::Uint32 => DynamicValue::Uint32(int(v)?),
        F::Uint64 => DynamicValue::Uint64(int64(v)?),
        F::Float32 => DynamicValue::Float32(float32_from_json(v)?),
        F::Float64 => DynamicValue::Float64(float(v)?),
        F::String => DynamicValue::String(string(v, None)?),
        F::BoundedString(n) => DynamicValue::String(string(v, Some(*n))?),
        F::Message(m) => DynamicValue::Message(Box::new(to_message(m, v, budget)?)),
        // Unbounded uint8 sequences: straight to bytes (hiroz encodes them
        // from `Bytes`), never one dynamic value per byte.
        F::Sequence(el) if **el == F::Uint8 => {
            let bytes = byte_blob(v)?;
            spend(budget, bytes.len() / 64)?;
            DynamicValue::Bytes(bytes)
        }
        F::Array(el, n) => {
            let items = collection(el, v, budget)?;
            if items.len() != *n {
                return Err(PathError::new(format!(
                    "expected exactly {n} elements, got {}",
                    items.len()
                )));
            }
            DynamicValue::Array(items)
        }
        F::Sequence(el) => DynamicValue::Array(collection(el, v, budget)?),
        F::BoundedSequence(el, n) => {
            let items = collection(el, v, budget)?;
            if items.len() > *n {
                return Err(PathError::new(format!(
                    "more than its bound of {n} elements"
                )));
            }
            DynamicValue::Array(items)
        }
    })
}

/// A `uint8` blob: a base64 string (the form samples are served in) or an
/// array of 0-255 numbers.
fn byte_blob(v: &Value) -> Result<Vec<u8>, PathError> {
    match v {
        Value::String(s) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|err| PathError::new(format!("invalid base64: {err}"))),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| int::<u8>(item).map_err(|e| e.at(format!("[{i}]"))))
            .collect(),
        _ => Err(PathError::new(
            "expected a base64 string or an array of bytes",
        )),
    }
}

/// Elements of a fixed array or sequence (for `uint8` elements also a base64
/// string).
fn collection(
    element: &FieldType,
    v: &Value,
    budget: &mut usize,
) -> Result<Vec<DynamicValue>, PathError> {
    if *element == FieldType::Uint8 {
        let bytes = byte_blob(v)?;
        spend(budget, bytes.len())?;
        return Ok(bytes.into_iter().map(DynamicValue::Uint8).collect());
    }
    let Value::Array(items) = v else {
        return Err(PathError::new("expected an array"));
    };
    spend(budget, items.len())?;
    items
        .iter()
        .enumerate()
        .map(|(i, item)| value_from_json(element, item, budget).map_err(|e| e.at(format!("[{i}]"))))
        .collect()
}

/// An integer from a JSON number, range-checked. Floats (even `1.0`) and
/// strings are refused.
fn int<T: TryFrom<i128>>(v: &Value) -> Result<T, PathError> {
    let n: i128 = match v {
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i128::from(i),
            (_, Some(u)) => i128::from(u),
            _ => return Err(PathError::new(format!("expected an integer, got {n}"))),
        },
        _ => return Err(PathError::new("expected an integer")),
    };
    T::try_from(n).map_err(|_| {
        PathError::new(format!(
            "{n} is out of range for {}",
            std::any::type_name::<T>()
        ))
    })
}

/// A 64-bit integer: a JSON number, or a decimal string (the form samples are
/// served in, since JSON numbers lose precision above 2^53).
fn int64<T: TryFrom<i128>>(v: &Value) -> Result<T, PathError> {
    match v {
        Value::String(s) => {
            let valid = !s.is_empty()
                && s.strip_prefix('-')
                    .unwrap_or(s)
                    .bytes()
                    .all(|b| b.is_ascii_digit());
            if !valid {
                return Err(PathError::new(format!(
                    "expected a decimal integer string, got {:?}",
                    clean(s)
                )));
            }
            let n: i128 = s
                .parse()
                .map_err(|_| PathError::new(format!("{} is out of range", clean(s))))?;
            T::try_from(n).map_err(|_| {
                PathError::new(format!(
                    "{n} is out of range for {}",
                    std::any::type_name::<T>()
                ))
            })
        }
        _ => int(v),
    }
}

fn float(v: &Value) -> Result<f64, PathError> {
    match v {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| PathError::new(format!("expected a number, got {n}"))),
        Value::String(s) => match s.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            other => Err(PathError::new(format!(
                "expected a number, got {:?}",
                clean(other)
            ))),
        },
        _ => Err(PathError::new("expected a number")),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn float32_from_json(v: &Value) -> Result<f32, PathError> {
    let f = float(v)?;
    if f.is_finite() && f.abs() > f64::from(f32::MAX) {
        return Err(PathError::new(format!("{f} is out of range for a float32")));
    }
    Ok(f as f32)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hiroz::dynamic::{DynamicMessage, DynamicValue, FieldType, MessageSchema};
    use serde_json::json;

    use super::{json_to_message, message_to_json};

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
        // And back: JSON → message → CDR → message → the same JSON.
        let again = json_to_message(&schema, &json).unwrap();
        let back = DynamicMessage::from_cdr(&again.to_cdr().unwrap(), &schema).unwrap();
        assert_eq!(message_to_json(&back), json, "the mapping must round-trip");

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

    #[test]
    fn json_to_message_is_strict_and_names_the_field() {
        let schema = everything();
        let err = |v: serde_json::Value| json_to_message(&schema, &v).unwrap_err();
        assert!(err(json!({"nope": 1})).contains("unknown field `nope`"));
        assert!(
            err(json!({"i8": 300})).contains("i8: 300 is out of range"),
            "{}",
            err(json!({"i8": 300}))
        );
        assert!(err(json!({"u64": -1})).contains("u64"));
        assert!(err(json!({"b": "yes"})).contains("b: expected a boolean"));
        assert!(err(json!({"bs": "far too long"})).contains("bs: longer than its bound"));
        assert!(err(json!({"fixed_i32": [1, 2]})).contains("fixed_i32: expected exactly 3"));
        assert!(err(json!({"bseq_u8": [1, 2, 3, 4, 5]})).contains("bseq_u8: more than its bound"));
        assert!(err(json!({"seq_u8": "!!!"})).contains("seq_u8: invalid base64"));
        assert_eq!(
            err(json!({"seq_msg": [{"x": 1.0}, {"x": "fast"}]})),
            "seq_msg[1].x: expected a number, got \"fast\""
        );
        assert!(err(json!([1])).contains("expected a JSON object"));
        // Decimal strings only for 64-bit; no floats into ints; float32 range.
        assert!(err(json!({"i8": "5"})).contains("i8: expected an integer"));
        assert!(err(json!({"i32": 1.0})).contains("i32: expected an integer"));
        assert!(err(json!({"i64": "+3"})).contains("decimal integer string"));
        assert!(err(json!({"i64": " 7"})).contains("decimal integer string"));
        assert!(err(json!({"f32": 1e300})).contains("out of range for a float32"));
        assert!(err(json!({"s": "a\u{0}b"})).contains("NUL"));
        // Exactly at a bound is fine.
        assert!(
            json_to_message(&schema, &json!({"bs": "12345678", "bseq_u8": [1, 2, 3, 4]})).is_ok()
        );
        // Byte blobs may also be arrays of numbers.
        assert!(err(json!({"seq_u8": [1, 256]})).contains("seq_u8[1]: 256 is out of range"));
        // Omitted fields keep defaults; 64-bit values accept numbers too.
        let m = json_to_message(&schema, &json!({"i64": 5, "u64": "7", "seq_u8": [1, 2]})).unwrap();
        let j = message_to_json(&m);
        assert_eq!(
            (
                j["i64"].clone(),
                j["u64"].clone(),
                j["seq_u8"].clone(),
                j["b"].clone()
            ),
            (json!("5"), json!("7"), json!("AQI="), json!(false))
        );
    }

    #[test]
    fn huge_requests_are_refused_before_they_cost_memory() {
        let schema = everything();
        let many: Vec<serde_json::Value> = (0..200_000).map(|_| json!({})).collect();
        let e = json_to_message(&schema, &json!({"seq_msg": many})).unwrap_err();
        assert!(e.contains("too large"), "{e}");
        let ints: Vec<i64> = vec![1; 200_000];
        let e = json_to_message(&schema, &json!({"seq_i64": ints})).unwrap_err();
        assert!(e.contains("too large"), "{e}");
        // A 1 MB byte blob is fine (counted per 64 bytes).
        let blob = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            vec![7u8; 1_000_000],
        );
        assert!(json_to_message(&schema, &json!({"seq_u8": blob})).is_ok());
    }
}
