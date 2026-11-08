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
| `pipes-core` | ✅ build info · 📋 data model (graph, samples, stats), `Bus` trait, topic hub, pattern matching | **No** |
| `pipes-bus` | ✅ distro guard · 📋 hiroz implementation of `Bus`: session, graph snapshots, dynamic subscriptions, CDR → JSON mapping | Yes (the only one) |
| `pipes-api` | ✅ `/healthz`, `serve` with drain deadline · 📋 REST, SSE, OpenAPI, dashboard, metrics | **No** |
| `pipes-testkit` | 📋 in-process zenoh router, typed hiroz fixture publishers, hermetic bus/e2e tests | Yes (tests only) |
| `rmf-pipes` | ✅ binary: config, logging, signals, wiring | No (goes through `pipes-bus`) |

`pixi run invariants` enforces the last column.

Dependency direction: `rmf-pipes → {pipes-api, pipes-bus} → pipes-core`. `pipes-api` gets the bus as `Arc<dyn Bus>`, so API tests use a fake bus.

## Data flow (📋 phase 1–2)

1. **Graph.** `pipes-bus` reads hiroz's liveliness graph (nodes, topics, services, plus each endpoint's type name, RIHS01 hash and QoS) and turns it into `pipes-core` graph types. `pipes-core` flags **type mismatches**, meaning a topic seen with more than one (type, hash) pair. rmw_zenoh silently drops traffic in that case, so we surface it loudly in logs, the API and the metrics.
2. **Subscriptions.** There are two kinds:
   - **Standing**: topics matching the configured ingest patterns. These use zenoh key-expression syntax over topic names, e.g. `fleet/*/state`.
   - **On demand**: opened when a client asks for a topic. They're ref-counted, and closed after an idle timeout once the last client leaves.

   Either way, `pipes-bus` asks the publisher's `~/get_type_description` service for the schema, then opens a hiroz dynamic subscription that copies the publisher's durability (so transient_local late-joins work).
3. **Admission.** Before decoding, each sample passes a per-topic rate cap and a payload-size cap. Samples over the cap are counted and dropped; oversized ones are reported with `data: null`.
4. **Decoding.** CDR → hiroz `DynamicMessage` → JSON, following `json-mapping.md`.
5. **Hub.** `pipes-core` keeps the latest sample and stats per topic, and fans out to SSE clients over a bounded broadcast channel. Slow clients get a `lagged` event instead of blocking anyone.
6. **HTTP.** REST for the graph, topics, latest values and schemas; SSE for live samples; `/metrics` in Prometheus format; a server-rendered dashboard.

## Shutdown
SIGINT or SIGTERM cancels a `CancellationToken`.
- The HTTP server stops accepting connections.
- SSE streams end themselves.
- `serve` waits at most `--shutdown-drain`, then abandons stragglers.
- Bus subscriptions are dropped with the runtime.
