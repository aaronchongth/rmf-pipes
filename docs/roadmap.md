# Roadmap

Status: ✅ done · 🚧 in progress · 📋 planned. Each phase ends with `pixi run ci` green, a clean-agent review, and a demo.

## Phase 0: Foundations ✅
- Cargo workspace, `rust-toolchain.toml` (1.99.0), pixi tasks, GitHub CI
- Exactly-one-distro guard, plus a build check for every distro (`pixi run distros`)
- Dependency invariants (`pixi run invariants`) and the `missing_docs` lint
- Binary skeleton:
  - clap config with hidden env values (tested)
  - pretty/JSON logging
  - `/healthz`
  - SIGINT/SIGTERM handlers installed before the `listening` log (deterministic test)
  - drain-deadline shutdown (non-vacuous test, mutation-checked)
- Agent and design docs

## Phase 1: Live data plane (read-only, no auth, binds to localhost by default) ✅
- `Bus` trait in `pipes-core`, with a hiroz implementation in `pipes-bus`
- Graph snapshots with type hashes and QoS, plus type-mismatch detection
- Runtime schema discovery and dynamic decoding, with the JSON mapping (`json-mapping.md`)
- Standing (pattern) and on-demand (ref-counted, idle-timeout) subscriptions
- Rate and size caps
- Topic hub: latest sample, stats, bounded fan-out
- `/metrics`
- Exit:
  - hermetic tests through an in-process router, covering every hiroz `FieldType` and transient_local late join
  - `pixi run interop` passes against real `rmw_zenoh_cpp`

## Phase 2: API, OpenAPI and dashboard ✅
- REST: graph, topics, latest, schema. SSE: live samples, lag notices, ends on shutdown.
- utoipa OpenAPI at `/openapi.json`, with a vendored Swagger UI at `/docs`
- Server-rendered dashboard (askama + vendored htmx): overview, graph, topic live view, mismatches
- Exit: in-process API tests with a fake bus, plus an e2e test (real binary + in-process router + hiroz publisher, over HTTP/SSE)

## Phase 3: Auth ✅
Postgres via sqlx and pixi. Local users with a bootstrap admin, sessions, hashed API tokens, roles, login rate limit, password-reset CLI. OIDC is deferred to a follow-up (📋).

## Phase 4: Write path ✅
Allowlisted publish and service calls per topic and role, rate-limited, with an audit log in Postgres.

## Phase 5: zenoh Postgres storage backend 📋
`crates/zenoh-backend-postgres`:
- `Latest` + `All`
- key-expression queries
- the `_time` selector
- retention by time partition
- benchmarks

It runs in its own zenohd. Afterwards: rmf-pipes `/history` via zenoh `get`, and an optional Grafana example over the Postgres data source.

## Phase 6: Packaging and interop 📋
Container image, compose stack, distro release matrix, nightly interop job, Zenoh 2.0 readiness.
