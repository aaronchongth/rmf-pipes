# Recording

rmf-pipes can record what happens on the ROS 2 bus: topic messages, service requests, and the goals, feedback, status changes and results of actions. **Recording rules** say what to record and where to send it: the **console** (the process log) or the **database** (Postgres). Rules load from a file at startup and can be replaced at runtime through the API or the dashboard.

Everything is observed passively. rmf-pipes never relays traffic, so robots and their clients behave exactly as they would without it.

## Rules

A rule has a name, a pattern, a kind, the parts to record, and one or more sinks:

```toml
# --record-config file: a list of [[rules]] tables
[[rules]]
name = "nav-goals"                  # unique; stored with every record
match = "/*/navigate_to_pose"       # `*` = one name segment, `**` = any number
kind = "action"                     # topic | service | action
parts = ["goal", "status", "result"] # optional; default = every part of the kind
sinks = ["console", "database"]     # at least one
# max_rate_hz = 1.0                 # optional; see "Rate limits"
```

The API takes the same shape as JSON: `{"rules": [{"name": "nav-goals", "match": "/*/navigate_to_pose", "kind": "action", ...}]}`.

| `kind` | Matches | Parts |
|---|---|---|
| `topic` | topics with at least one publisher | `message` |
| `service` | services with at least one server | `request` |
| `action` | actions with a `send_goal` server | `goal`, `feedback`, `status`, `result` |

Notes on matching:
- Patterns match whole name segments only. `/*/navigate_to_pose` matches `/robot0/navigate_to_pose` but not `/robot0/inner/navigate_to_pose`. There are no sub-segment wildcards: `/robot*/…` is rejected, so use `/*/…`.
- Topic and service rules never match an action's internal `…/_action/…` topics and services. Use `kind = "action"` for those.
- Two rules may match the same target. Each records independently, under its own name.

Validation happens when rules are loaded or replaced, and an invalid set is refused as a whole. These are refused:
- unknown fields
- duplicate or invalid names (letters, digits, `_`, `-`, up to 64)
- parts that the kind doesn't have
- no sinks
- `database` when no `--database-url` is configured
- a rate outside 0.001–1e6
- more than 64 rules

## How each part is observed

| Part | Observed by |
|---|---|
| `message` | Subscribing to the topic, through the topic hub (shared with the API and dashboard). |
| `request` | **Tapping** the service: see below. Responses go only to the caller, so they can't be observed. |
| `goal` | Tapping `<action>/_action/send_goal`. The record holds the goal fields, and `goal_id` holds the goal's UUID. |
| `feedback` | Subscribing to `<action>/_action/feedback`. |
| `status` | Subscribing to `<action>/_action/status` (a `GoalStatusArray`). Each goal's status *change* is a record: `accepted`, `executing`, `canceling`, `succeeded`, `canceled` or `aborted`. |
| `result` | When a goal reaches `succeeded`, `canceled` or `aborted`, rmf-pipes calls `<action>/_action/get_result` for it. That's a read-only call any client may make, and the server answers at once because the goal is done. The record holds `{status, result}`. |

Every action record carries the goal's UUID in `goal_id`, so one goal's whole history is a single query. It's the same ID `ros2 action send_goal` prints.

When a status stream starts, goals that are *already finished* in the first snapshot are a baseline: they come from the topic's transient-local history, so they aren't re-recorded each time rmf-pipes restarts. Goals still running in that snapshot are recorded from then on. A goal that first appears already finished in a *later* snapshot is recorded. That happens with fast goals, since the hub's rate cap can merge their snapshots.

A stream restarts after a failure, or when its rule changes, and then re-baselines the same way:
- goals that finished while it was down get no status or result
- goals still running are recorded again, from their current status

### Taps: observing requests without answering them
In rmw_zenoh, a service call is a zenoh query to the server's key expression. A tap declares a zenoh **queryable** on that exact key:
- It is declared `complete`, because rmw_zenoh clients send queries only to complete queryables (`AllComplete`). A non-complete tap sees nothing.
- It never replies, and drops each query at once. Dropping finalises the query, so the client still waits only for the real server's answer.
- It has no ROS liveliness token, so it doesn't show up as a second server in `ros2 service info` or in rmf-pipes' own graph.

This was verified against rmw_zenoh_cpp on Lyrical (0.10.6) and Jazzy (0.2.11), including rclpy, rclcpp (Nav2) and rclrs servers. With a tap in place, goals still succeed, the real result comes back, and latency is unchanged (`pixi run interop` and the hermetic test `taps_observe_requests_without_disturbing_the_server` check this).

The limits that keep taps safe:
- Taps exist only for services and actions that a rule matches *and* that have a server, and only for valid ROS names. A name advertised with key-expression syntax (`*`, `$`) is never tapped. The API has no way to tap an arbitrary key.
- Tapped requests over `--max-payload-bytes` are recorded with `data: null`, without being decoded.
- A new tap reaches the router asynchronously, so requests sent in the first moments after it starts may not be seen. The stream shows `active` once it's declared locally.
- A zenoh query from a **non-ROS** zenoh application using the default `BestMatching` target could be routed to the tap alone and go unanswered. ROS clients never do this. Keep `service` and `action` rules as narrow as the job needs, and avoid `**`.
- rmf-pipes' own calls (`get_result`, API service calls) to a tapped service are observed like anyone else's.

### Type descriptions
Decoding needs each type's description. rmf-pipes asks the nodes using the topic or service first, then up to 3 other nodes that use the same type and hash. The fallback is needed because some client libraries never answer the query: rclrs nodes (such as the prototype's `nav2_traffic`), and `ros2 topic pub --once`.

## Sinks
- **console** logs each record at `info` under the `rmf_pipes::record` target, with the payload cut at 2 KiB:
  ```
  INFO rmf_pipes::record: record rule=nav-goals kind="action" name=/robot1/navigate_to_pose part="result" goal_id="a2449533-…" type_name=nav2_msgs/action/NavigateToPose data={"status":"succeeded","result":{"error_code":0,"error_msg":""}}
  ```
  `RUST_LOG=rmf_pipes::record=info,warn` shows records and little else. `--log-format json` gives one JSON object per record.
- **database** writes to `pipes.records`. Records are buffered (up to 10 000) and written in batches of up to 500, at least every 250 ms.
  - A failed batch is retried once after a second, then dropped and counted (`db_error`).
  - If the database falls behind, new records are dropped and counted (`db_backlog`), and the bus is never slowed down.
  - On shutdown, what is already buffered is written before exit.

Every record from every sink also goes into an in-memory list of the latest 200, which serves `GET /api/v1/recording/recent` and the dashboard, with or without a database.

## Rate limits
Topic samples pass the hub's own caps first (`--max-rate-hz`, `--max-payload-bytes`). A rule's `max_rate_hz` then keeps at most that many **messages, requests or feedback** per second per target, keeping the first in each interval and dropping (and counting) the rest. Goals, statuses and results are never throttled. Nav2 publishes feedback at about 20 Hz, so the demo records it at 1 Hz.

## Limits
- At most `--max-record-streams` streams run at once (default 256). A topic or service rule uses one stream per match. An action uses up to three: goal, feedback, and status (which also covers results). Further matches are skipped, reported as `skipped` and logged when the count changes.
- A stream that fails, for example because its type description is unavailable, is retried every 5 s. A topic that fails inside the hub is retried by the hub.
- At most 8 `get_result` calls run at once, each with a 5 s timeout. Each status stream lets at most 64 finished goals wait for a call; beyond that, results are dropped and counted (`result_backlog`). Waiting and running fetches end with their stream.
- Recording's topic subscriptions don't count toward `--max-topics`, which stays for API clients; `--max-record-streams` bounds them instead.
- Rule changes take effect at once. A changed rule restarts its streams; unchanged rules keep theirs. Changes last until the next change or a restart: the `--record-config` file is never rewritten.

## API
| | |
|---|---|
| `GET /api/v1/recording` | The rules in force, every stream (what it observes, state, recorded and dropped counts), skipped matches, and whether the database sink is available. |
| `PUT /api/v1/recording/rules` | Replace every rule (`{"rules": [...]}`; admin). Returns the rules as normalised, or `400 invalid_rules` with the reason. Audited as `recording.update`: refused attempts as `error`, and accepted changes as `ok`, written *before* the change, which is refused (`503 audit_unavailable`) if the audit log can't be written. |
| `GET /api/v1/recording/recent?limit=50` | The latest records from every sink, newest first (in memory). |
| `GET /api/v1/records` | Stored records, newest first. Filters: `rule`, `name`, `kind`, `part`, `goal_id`, `since`, `until` (RFC 3339), `before` (id, for paging), `limit` (≤ 1000). `503 no_database` without a database. |

Without auth (anonymous mode), the rules can be read but not replaced. In demo mode anyone on the machine can replace them, and with auth only admins can.

The dashboard's **Recording** page (`/ui/recording`) shows the rules in an editor (save replaces them), the streams, and the latest records, refreshed every 2 s.

## Querying the database
```sql
-- One goal's whole story
SELECT at, part, data FROM pipes.records
WHERE goal_id = 'a2449533-c5cd-4fad-ac0f-f57186fc9290' ORDER BY at;

-- How each robot's goals ended in the last hour
SELECT name, data->>'status' AS status, count(*) FROM pipes.records
WHERE part = 'result' AND at > now() - interval '1 hour'
GROUP BY 1, 2 ORDER BY 1, 2;

-- Goal targets, with their outcome
SELECT g.name, g.goal_id,
       g.data #>> '{pose,pose,position,x}' AS x, g.data #>> '{pose,pose,position,y}' AS y,
       r.data->>'status' AS outcome, r.at - g.at AS took
FROM pipes.records g
LEFT JOIN pipes.records r ON r.goal_id = g.goal_id AND r.part = 'result'
WHERE g.part = 'goal' ORDER BY g.at DESC LIMIT 20;
```

Columns: `id`, `at` (when rmf-pipes observed it), `rule`, `kind`, `name`, `part`, `type_name`, `goal_id` (uuid), and `data` (jsonb, per [json-mapping.md](json-mapping.md); 64-bit integers are strings).
- Indexes: `(name, at)`, `goal_id`, and a BRIN index on `at`.
- `--record-retention` (default `7d`, `0s` = keep forever) deletes older rows hourly, 10 000 rows per statement.
- For action records, `type_name` is the action type (e.g. `nav2_msgs/action/NavigateToPose`). For topics it is the message type, and for services the service type.

## Metrics
| Name | Kind | Labels |
|---|---|---|
| `rmf_pipes_records_total` | counter | `rule`, `part` |
| `rmf_pipes_records_dropped_total` | counter | `rule`, `reason` = `rate_limited` \| `lagged` \| `decode_error` \| `result_unavailable` \| `result_backlog` \| `db_backlog` \| `db_error` |
| `rmf_pipes_recording_streams` | gauge | |

## Relation to the zenoh storage backend
Recording stores **decoded JSON**, keyed by ROS names, which SQL and Grafana can read directly. The planned zenoh Postgres storage backend (roadmap phase 5) stores raw zenoh samples (CDR bytes) under zenoh key expressions, for any zenoh application to query with `get`. They solve different problems; recording came first because it's what the demo and RMF operators need.
