# Architecture

Status markers: ✅ built · 🚧 in progress · 📋 planned. See `roadmap.md`.

## Shape

```
ROS 2 nodes (rmw_zenoh_cpp) ──► zenoh router (rmw_zenohd) ◄── rmf-pipes (hiroz session, client mode)
                                                                ├─ graph watcher (liveliness)
                                                                ├─ topic hub: latest sample + fan-out (memory)
                                                                └─ HTTP: REST · SSE · /openapi.json · /docs · dashboard · /metrics
Browser / scripts ──HTTP──► rmf-pipes only
```

- **Phases 0–2** cover the read-only live data plane. Nothing is persisted.
- **Later phases** add:
  - auth (Postgres)
  - an audited write path
  - the zenoh Postgres storage backend, which runs in a separate zenohd so it can't stall robot traffic

## Crates

| Crate | Role | May depend on hiroz/zenoh? |
|---|---|---|
| `pipes-core` | ✅ data model (graph, samples, schema), `Bus` trait + `FakeBus`, topic hub, pattern matching, metrics | **No** |
| `pipes-bus` | ✅ distro guard, `HirozBus`: session, graph snapshots, dynamic subscriptions, CDR → JSON mapping | Yes (the only one) |
| `pipes-api` | ✅ `/healthz`, `/metrics`, `serve` with drain deadline · 📋 REST, SSE, OpenAPI, dashboard | **No** |
| `pipes-testkit` | ✅ in-process zenoh router, hiroz fixture nodes, hermetic bus tests, interop tests (`rmw_zenohd` + `ros2`) | Yes (tests only) |
| `rmf-pipes` | ✅ binary: config, logging, signals, wiring | No (goes through `pipes-bus`) |

`pixi run invariants` enforces the last column.

Dependency direction: `rmf-pipes → {pipes-api, pipes-bus} → pipes-core`. `pipes-api` gets the bus as `Arc<dyn Bus>`, so API tests use a fake bus.

## Data flow (✅ phase 1 · 📋 HTTP in phase 2)

1. **Graph.** `pipes-bus` reads hiroz's liveliness graph (nodes, topics, services, plus each endpoint's type name, RIHS01 hash and QoS) and turns it into `pipes-core` graph types. `pipes-core` flags **type mismatches**, meaning a topic seen with more than one (type, hash) pair. rmw_zenoh silently drops traffic in that case, so we surface it loudly in logs, the API and the metrics.
2. **Subscriptions.** There are two kinds:
   - **Standing**: topics with publishers that match `--ingest` patterns. The patterns use the `*`/`**` subset of zenoh key-expression syntax over topic names, e.g. `fleet/*/state`.
   - **On demand**: opened when a client asks for a topic. They're ref-counted, and closed after an idle timeout once the last client leaves.

   Either way, `pipes-bus` asks a publisher's `~/get_type_description` service for the schema. It converts the answer with its own type-id table (`type_desc.rs`), because hiroz 0.2.1's converter misses `byte`, `char` and bounded strings. It then opens a hiroz dynamic subscription bound to the **publishers' advertised type hash**. The subscription is reliable if any publisher is, and transient_local if any is, so late-join history is never lost.
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
6. **HTTP.** REST for the graph, topics, latest values and schemas; SSE for live samples; `/metrics` in Prometheus format; a server-rendered dashboard.

## Shutdown
SIGINT or SIGTERM cancels a `CancellationToken`.
- The HTTP server stops accepting connections.
- SSE streams end themselves.
- `serve` waits at most `--shutdown-drain`, then abandons stragglers.
- Bus subscriptions are dropped with the runtime.
