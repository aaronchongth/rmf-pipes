# Architecture

Status markers: ✅ built · 🚧 in progress · 📋 planned. See `roadmap.md`.

## Shape

```
ROS 2 nodes (rmw_zenoh_cpp) ──► zenoh router (rmw_zenohd) ◄── rmf-pipes (hiroz session, client mode)
                                                                ├─ graph watcher (liveliness)
                                                                ├─ topic hub: latest sample + fan-out (memory)
                                                                ├─ recorder: rules → streams (hub watches, request taps, get_result)
                                                                │     └─ sinks: console (log) · database (batched)
                                                                ├─ write path: allowlisted publish / call, audited
                                                                └─ HTTP: REST · SSE · /openapi.json · /docs · dashboard · /metrics
Browser / scripts ──HTTP──► rmf-pipes only                     Postgres: users, sessions, tokens, audit log, records
```

- **Built:**
  - ✅ the live data plane (phases 0–2)
  - ✅ auth (phase 3)
  - ✅ the audited write path (phase 4)
  - ✅ demo mode and recording (phase 4.5)
- **Next:** the zenoh Postgres storage backend, which runs in a separate zenohd so it can't stall robot traffic.

## Crates

| Crate | Role | May depend on hiroz/zenoh? |
|---|---|---|
| `pipes-core` | ✅ data model (graph, samples, schema), `Bus` trait + `FakeBus`, topic hub, recording rules + recorder + console sink, pattern matching, metrics | **No** |
| `pipes-bus` | ✅ distro guard, `HirozBus`: session, graph snapshots, dynamic subscriptions, request taps, writes, CDR ↔ JSON mapping | Yes (the only one) |
| `pipes-api` | ✅ REST + SSE under `/api/v1`, OpenAPI (`/openapi.json`, Swagger UI at `/docs/`), dashboard (`/`, `/ui/*`), auth, writes, recording API + database sink, `/healthz`, `/metrics`, `serve` with drain deadline | **No** |
| `pipes-testkit` | ✅ in-process zenoh router, hiroz fixture nodes, hermetic bus tests, interop tests (`rmw_zenohd` + `ros2`) | Yes (tests only) |
| `pipes-db` | ✅ Postgres: users, sessions, API tokens, audit log, records; embedded migrations | **No** |
| `rmf-pipes` | ✅ binary: config, logging, signals, wiring, `user` CLI | No (goes through `pipes-bus`) |

`pixi run invariants` enforces the last column.

Dependency direction: `rmf-pipes → {pipes-api, pipes-bus} → pipes-core`. `pipes-api` gets the bus as `Arc<dyn Bus>`, so API tests use a fake bus.

## Data flow

1. **Graph.** `pipes-bus` reads hiroz's liveliness graph (nodes, topics, services, plus each endpoint's type name, RIHS01 hash and QoS) and turns it into `pipes-core` graph types. `pipes-core` flags **type mismatches**, meaning a topic seen with more than one (type, hash) pair. rmw_zenoh silently drops traffic in that case, so we surface it loudly in logs, the API and the metrics.
2. **Subscriptions.** There are two kinds:
   - **Standing**: topics with publishers that match `--ingest` patterns. The patterns use the `*`/`**` subset of zenoh key-expression syntax over topic names, e.g. `fleet/*/state`.
   - **On demand**: opened when a client asks for a topic. They're ref-counted, and closed after an idle timeout once the last client leaves.

   Either way, `pipes-bus` asks a publisher's `~/get_type_description` service for the schema, then, if none answers, up to 3 other nodes using the same (type, hash). Answers are cached by (type, hash). It converts the answer with its own type-id table (`type_desc.rs`), because hiroz 0.2.1's converter misses `byte`, `char` and bounded strings. It then opens a hiroz dynamic subscription bound to the **publishers' advertised type hash**. The subscription is reliable if any publisher is, and transient_local if any is, so late-join history is never lost.
   - The subscription is rebuilt automatically if the publishers' hash changes.
   - A failed open is retried while someone is still interested. The delay starts at `retry_after` and doubles up to `max_retry_after`.
   - When an entry closes (idle, or shutdown), watchers get a final `closed` status and their stream ends.
3. **Admission.** Before decoding, each sample passes a per-topic rate cap and a payload-size cap.
   - The rate cap is a trailing-edge throttle: within an interval only the newest sample is kept, and it is delivered when the interval ends, so bursts never leave `latest` stale.
   - Oversized samples are reported with `data: null` and are never decoded.
   - hiroz's per-subscription queue is bounded (`--queue-depth`) and drops the oldest sample.
   - Each subscription task has a generation number, so a task replaced by a resubscribe can't write stale state.
4. **Decoding.** CDR → hiroz `DynamicMessage` → JSON, following `json-mapping.md`.
5. **Hub.** `pipes-core` keeps the latest sample and stats per topic, and fans out to watchers over a bounded broadcast channel. A slow watcher skips ahead (and, in phase 2's SSE, gets a `lagged` event) instead of blocking anyone.
6. **HTTP** (`pipes-api`).
   - **REST**: `/api/v1/graph`, `/api/v1/topics`, `/api/v1/topic`, `/api/v1/topic/latest`, `/api/v1/topic/schema`.
     - `latest` and `schema` subscribe on demand and wait up to `wait_ms` (capped at 10s). A topic with no publisher is `404`; one that already failed is `503` at once.
     - Every error, including bad query strings, unknown paths and non-GET methods, is JSON `{error, message}` with a stable code (listed on `ApiError` in the OpenAPI document).
   - **SSE**: `/api/v1/topic/stream` sends `status`, `sample` and `lagged` events. It always ends with a final `status: closed`, whether the topic was closed or the server is shutting down. A client disconnect releases its watch at once.
   - **Limits**: at most `--max-topics` on-demand topics and `--max-streams` SSE streams, then `503`. The trace layer logs 5xx at DEBUG, since they are expected outcomes such as a client's `wait_ms` running out.
   - **OpenAPI**: `/api`, `/healthz` and `/metrics` routes are registered only through utoipa-axum's `OpenApiRouter` in `api.rs` (`pixi run invariants` rejects a plain `.route("/api…")`), so they can't exist undocumented. Swagger UI is vendored, and its online validator badge is disabled, so `/docs/` never contacts the internet.
   - **Dashboard**: askama templates, refreshed every 2s by vendored htmx, plus a small inline script for the live SSE view.
     - It loads no external resources: a test scans every page, inline JS included, for network locations.
     - Topic names from the graph are always HTML-escaped. Names that aren't valid ROS names are shown but not linked.
   - **Metrics**: `/metrics` in Prometheus format.

## Authentication (phase 3)
- `pipes-api::auth::authenticate` is a middleware on every route. It resolves the caller from `Authorization: Bearer rp_…` or the session cookie, then:
  - routes not in `auth::is_public` get `401` (API) or a redirect to `/login` (pages; htmx gets `HX-Redirect`);
  - it enforces the JSON-only rule for cookie writes.
- Handlers take `Caller` and call `require(Role::…)`.
- Account endpoints live in `pipes-api::accounts`: login/logout/me/password, tokens, users, audit.
- What goes to `pipes.audit_log`, from the API and from the `rmf-pipes user` CLI alike:
  - every login attempt (except repeated rate-limit refusals)
  - credential changes
  - role refusals on admin endpoints
  - user changes, written in the same transaction as the change itself
- With no database configured, the middleware marks everyone anonymous and the API is read-only.
- In demo mode (`--demo`, loopback only) the middleware marks everyone `Principal::Demo` (admin role, no account) and applies the JSON-only rule to every non-GET request. A database, if configured, is used for the audit log and records but not for auth (`AppState::with_database`). `auth::record_audit` writes to it, or to the process log under `rmf_pipes::audit` without one.

## Writes (phase 4)
`pipes-api::writes` handles every write the same way: it checks authentication, the allowlist and the role (with audited refusals), then the per-user/target rate limit, then **writes the audit entry**, then calls `Bus::publish` / `Bus::call`.

In `pipes-bus`:
- `publish` picks the one type the subscribers advertise (refusing if there's more than one), fetches its schema (cached, discovery bounded, failures cached for 10 s), builds the message strictly from JSON (`json::json_to_message`, honouring defaults), and publishes on a cached publisher bound to that hash. Publishers idle for 60 s are undeclared.
- `call` does the same for the service's single server (refusing if there are several), through a cached `ZClient<RawService>`.

## Recording (phase 4.5)
See [recording.md](recording.md) for the user-facing behaviour.
- `pipes-core::record` defines rules, records and the `RecordSink` trait. `ConsoleSink` logs under `rmf_pipes::record`.
- `pipes-core::recorder::Recorder` ticks once a second against the hub's graph. It turns rules into a bounded set of streams, each a task keyed by (rule, target, source), and reconciles them: it starts new ones, stops ones whose target, rule or rule settings went away, and retries ended ones after 5 s. The kinds of stream:
  - **message**, **feedback** and **status** streams are `Hub::watch_pinned` watches, so they share the hub's subscription, caps, and transient-local handling with the API, but don't use up its `--max-topics` slots.
  - **request** and **goal** streams use `Bus::tap`.
  - **status** streams diff `GoalStatusArray` snapshots per goal and, for finished goals, call `get_result` through `Bus::call`: at most 8 at once overall, at most 64 waiting per stream, in a `JoinSet` owned by the stream so they end with it.
- `Bus::tap` in `pipes-bus` declares a `complete` zenoh queryable on the server's exact rmw_zenoh key (`KeyExprFormat::topic_key_expr` of the server entity), never replies, and drops each query on receipt. Its buffer is a 64-entry ring that drops the oldest.
- `pipes-api::recording::DbSink` is a 10 000-record bounded channel. A writer task inserts in batches with one `UNNEST` statement (`pipes-db::records`), and drains the buffer on shutdown after the recorder has stopped.
- Recent records (200) are kept in memory for the API and dashboard.

## Shutdown
SIGINT or SIGTERM cancels a `CancellationToken`.
- The HTTP server stops accepting connections.
- SSE streams end themselves.
- `serve` waits at most `--shutdown-drain`, then abandons stragglers.
- The recorder stops its streams, then the database sink writes what is buffered, then the hub closes its topics; each step waits at most `--shutdown-drain`.
- Bus subscriptions and taps are dropped with the runtime.
