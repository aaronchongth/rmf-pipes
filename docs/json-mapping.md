# JSON mapping of ROS messages

This is a **public contract**. rmf-pipes decodes every message at runtime, from the schema its publisher reports, and serves it as JSON with the rules below. API clients and generated OpenAPI schemas rely on them, so a change here is a breaking change and must be recorded in `decisions.md`.

The implementation is `crates/pipes-bus/src/json.rs`, and the test that pins it is `every_field_type_maps_as_documented`.

## Values

| ROS IDL type | JSON | Example |
|---|---|---|
| `bool` | boolean | `true` |
| `int8` `int16` `int32` `uint8` `uint16` `uint32`, and scalar `byte`/`char` | number | `-32`, `4294967295` |
| `int64` `uint64` | **string** (decimal) | `"9007199254740993"` |
| `float32` | number, shortest decimal that round-trips the `f32` | `1.1` (not `1.100000023841858`) |
| `float64` | number | `-0.25` |
| NaN, +∞, −∞ (either float type) | string | `"NaN"`, `"Infinity"`, `"-Infinity"` |
| `string`, `string<=N` (and IDL fixed strings) | string | `"hello"` |
| nested message | object, with fields in definition order | `{"x": 1.0, "y": 2.0, "z": 3.0}` |
| `T[N]`, `sequence<T>`, `sequence<T, N>` | array of `T` | `[1, 2, 3]` |
| **`uint8`/`byte`/`char` arrays and sequences** | **string**, standard base64 with padding | `"AQL/"` for `[1, 2, 255]`; `""` when empty |

## Why
- **64-bit integers as strings.** JSON numbers are IEEE doubles in most clients (JavaScript in particular), and lose precision above 2^53. Strings are lossless everywhere.
- **Byte blobs as base64.** Images, maps and point clouds would otherwise be huge arrays of numbers.
- **Non-finite floats as strings.** JSON has no representation for them.
- **No special cases for well-known types.** `builtin_interfaces/Time` stays `{"sec": …, "nanosec": …}`, enums stay integers, and so on. The mapping is purely structural, which keeps it reversible and predictable.

Not supported: `wstring`, `wchar` and `long double`, which hiroz cannot decode. A topic whose type uses them shows status `failed`, with an error naming the field.

## Writing (publish and service calls)
The same mapping is accepted in the other direction, strictly:
- Unknown fields are an error.
- Omitted fields take the message's defaults from its definition (e.g. `w: 1.0` for `geometry_msgs/Quaternion`), else zero, empty or `false`.
- Integers must be JSON integers that fit their type: `1.0` is refused for an `int32`.
- 64-bit integers may also be decimal strings (`"-7"`; no `+` or spaces). Narrower integers may not.
- `float32` values must fit (`1e300` is refused). Floats accept `"NaN"`, `"Infinity"` and `"-Infinity"`.
- Strings can't contain NUL.
- `uint8`/`byte`/`char` arrays may be base64 strings or arrays of 0–255.
- Bounded strings and sequences, and fixed arrays, are length-checked.
- One message may build at most 100 000 elements (nested messages plus collection items; byte blobs count one per 64 bytes).

Errors name the path, e.g. `waypoints[2].x: expected a number, got "fast"`.

## Sample envelope

Every sample served by the API is wrapped like this:

```json
{
  "topic": "/chatter",
  "type_name": "std_msgs/msg/String",
  "type_hash": "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
  "received_at": "2026-11-08T03:58:13.435401Z",
  "source_timestamp": "2026-11-08T03:58:13.431002Z",
  "sequence": 42,
  "size_bytes": 24,
  "data": { "data": "hello" }
}
```

| Field | Meaning |
|---|---|
| `received_at` | When rmf-pipes received the sample (RFC 3339, UTC). |
| `source_timestamp` | The publisher's timestamp from the rmw_zenoh attachment, or `null`. |
| `sequence` | The publisher's sequence number from the attachment, or `null`. This envelope field is a JSON number; it is far below 2^53 in practice. |
| `size_bytes` | Size of the serialized (CDR) payload. |
| `data` | The message, mapped as above. It is `null` when `omitted` is present. |
| `omitted` | Only present when `data` is `null`. `"too_large"` means `size_bytes` exceeded `--max-payload-bytes`. |
