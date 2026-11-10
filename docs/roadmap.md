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

## Phase 4.5: Local end-to-end demo and recording ✅
Reprioritised on 2026-11-10: demo the whole stack locally before more security work (see `decisions.md`).
- `--demo` mode: no auth, loopback only, JSON-only non-GETs. A database can be used without auth.
- `Bus::tap`: passive observation of service requests and action goals over rmw_zenoh.
- Type-description fallback to other nodes using the type (rclrs nodes don't answer).
- Recording rules (topics, services, actions: goal, feedback, status, result) with console and database sinks, from `--record-config` and replaceable at runtime through the API and the dashboard's Recording page. Records are queryable via `GET /api/v1/records` and SQL (`docs/recording.md`).
- The Nav2 demo (`demo/nav2/README.md`): next_gen_prototype's Nav2 stack in Docker, rmf-pipes on the host, `pixi run demo-up / demo / demo-goal / demo-check / demo-down`.
- Exit criteria:
  - hermetic recorder, tap and API tests
  - an interop test against an rclpy action server and `ros2 action send_goal`
  - `pixi run demo-check` passing against the live Nav2 stack

## Phase 5: zenoh Postgres storage backend 📋
`crates/zenoh-backend-postgres`:
- `Latest` + `All`
- key-expression queries
- the `_time` selector
- retention by time partition
- benchmarks

It runs in its own zenohd. Afterwards: rmf-pipes `/history` via zenoh `get`, and an optional Grafana example over the Postgres data source (which can also read `pipes.records`).

## Phase 6: Packaging and interop 📋
Container image, compose stack, distro release matrix, nightly interop job, Zenoh 2.0 readiness.
