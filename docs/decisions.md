# Decisions

These are settled decisions, newest first. Reopen one only with new information, and record the change here.

## 2026-11-08: Defer Postgres to phase 3
Nothing in phases 0–2 persists anything: live state is in memory. Postgres (sqlx, migrations, `sqlx::test`, pixi-managed `postgresql`) arrives with auth in phase 3. Until then, the server runs with no external dependencies apart from a zenoh router, which keeps the agent inner loop trivial.

## 2026-11-08: Wildcards are matched on the ROS graph, and hiroz owns the subscriptions
Next-gen topic taxonomies need wildcard subscriptions, such as `fleet/*/state`. rmf-pipes does **not** subscribe to raw zenoh key expressions. Instead:
1. Patterns are zenoh key-expression syntax applied to ROS topic names, without the leading `/`.
2. They are matched against topics in hiroz's liveliness-based graph.
3. Each match gets its own hiroz dynamic subscription, built with the publisher's QoS.

This keeps rmw_zenoh specifics inside hiroz: key mangling, type hashes, transient_local history, and the `_buf_cpu` buffer groups. It also keeps unmatched high-rate traffic off the server.

## 2026-11-08: Messages are decoded at runtime, and we own the JSON mapping
Topics are decoded with hiroz `dynamic` messages. The schema is fetched from the publishing node's `~/get_type_description` service, so there's no per-type codegen. hiroz has no JSON form for dynamic messages, so rmf-pipes defines one in `docs/json-mapping.md`. **That mapping is a public contract.**

## 2026-11-08: Default distro is Lyrical, one build per distro
hiroz selects the rmw_zenoh conventions with mutually exclusive compile-time features. rmf-pipes forwards `jazzy | kilted | lyrical | rolling` and refuses to build with more than one. The default is Lyrical, the newest LTS: it's the distro installed for local interop tests, and Open-RMF has a Lyrical release. Humble isn't supported (no type hashes). Use `hiroz-bridge` if you ever need it.

## 2026-11-08: Pin hiroz 0.2.1 and zenoh 1.9.0 exactly
hiroz is v0.x, so it's allowed to break between minors. It requires `zenoh ^1.9.0`, but it's built and tested against 1.9.0, and zenoh 1.10 is already on crates.io. Both crates are pinned with `=`. Upgrades happen in their own commit and must pass the interop tests. **Watch Zenoh 2.0** (planned for H2 2026; 1.x compatibility hasn't been announced).

## 2026-11-08: Keep an API server between the web and the robot bus
The browser and web clients never speak zenoh. The API server:
- is the rate, validation and authorization boundary
- keeps DB stalls and plugin crashes out of the router that carries robot traffic
- gives end-user identity (zenoh ACL only identifies sessions)
- fits normal web deployment

## 2026-11-08: Auth is built in, Grafana-style
This arrives in phase 3: local users, optional OIDC, sessions and hashed API tokens. We don't bundle an identity provider. **Grafana isn't an identity provider.** Its role is an optional metrics and history UI, reading `/metrics` and the Postgres storage tables.

## 2026-11-08: Postgres-first zenoh storage backend, prototyped in this repo
It will live in `crates/zenoh-backend-postgres` (phase 5) with no rmf-pipes dependencies, licensed so it can move to open-rmf or eclipse-zenoh once stable. Postgres is the "boring, reliable SQL", and it covers Supabase/Neon/RDS for free. SQLite may follow as a second dialect.

## 2026-11-08: Minimal dashboard, OpenAPI for everything else
The dashboard only verifies that the data layer works (graph, topics, live samples, mismatches, metrics). There are no operational or domain features. The API is described by OpenAPI (utoipa) with a vendored Swagger UI at `/docs`. **No Node toolchain.**

## Inherited from rmf-web modernization / rmf-console (Sept 2026)
- rmw_zenoh is the only supported RMW.
- Postgres via sqlx, no ORM.
- Tests are hermetic by default, with a nightly interop check against real rmw_zenoh.
- Lessons kept:
  - `--help` must not leak secrets
  - `/healthz` must not leak raw DB errors
  - shutdown needs a drain deadline
  - message type-hash mismatches cause **silent drops**, so detect them at runtime
