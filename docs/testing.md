# Testing

The goal is that **an agent can verify any change with one command, without ROS or docker.**

Database tests (`#[sqlx::test]`) need Postgres. `pixi run test` starts a throwaway local server from the pixi environment (`scripts/db.sh`, data in `.pixi/pgdata`, port 54329) and sets `DATABASE_URL`. Each test gets a fresh database with the migrations applied. Stop the server with `pixi run db-stop`; delete `.pixi/pgdata` to reset it.

Status: ✅ in place.

| Level | What | Where | Command | Needs |
|---|---|---|---|---|
| ✅ L0 static | rustfmt, clippy `-D warnings`, `missing_docs`, dependency invariants, build of every distro feature, distro guard | whole workspace | `pixi run ci` | Rust |
| ✅ L1 unit | pure logic: config, serve/drain, JSON mapping (every hiroz `FieldType`, through CDR), type-description conversion (every ROS type id), subscription QoS and publisher choice, pattern matching, graph summaries, hub lifecycle against `FakeBus` with paused time; recording rules and the recorder (topic, service and action streams, goal-status diffing and the finished-goal baseline, `get_result` once per goal, rate limits, retries, stream cap, rule changes) against `FakeBus` and a real hub with paused time | `#[cfg(test)]` in each crate | `pixi run test` | Rust |
| ✅ L2 bus (hermetic) | typed hiroz publishers → in-process zenoh router → `HirozBus` (and the hub): graph, decoding, transient_local late join, mismatches, errors, binding to an advertised hash hiroz wouldn't compute; request taps (the server still answers, quickly, and works after the tap is gone); type-description fallback to another node using the type | `crates/pipes-testkit/tests/bus.rs` | `pixi run test` | Rust |
| ✅ L3 API (in-process) | every route and error path against `FakeBus` (`invalid_query`, `invalid_topic`, `no_publisher`, `topic_failed`, `no_sample`, `no_schema`, `too_many_*`, 405); SSE framing, lag, keep-alive, latest-first, final `closed` on shutdown and on topic close, watch released on disconnect; OpenAPI lists every route, every documented path is routed, Swagger validator off; dashboard pages with no external URLs, hostile names escaped | `crates/pipes-api/src/api_tests.rs` | `pixi run test` | Rust |
| ✅ DB | every `pipes-db` query: users (validation, case-insensitivity, last-admin guard), login checks, sessions (expiry, logout, disable, password change), API tokens (revoke, expiry, owner checks), audit paging and filters; only hashes stored | `crates/pipes-db/src/*.rs` | `pixi run test` | Postgres (pixi) |
| ✅ L3 auth (in-process) | every protected operation is 401 without credentials, page redirects and `HX-Redirect`, sessions and logout, identical failure responses + audit reasons, login rate limit, tokens and bearer precedence, role checks, CSRF rule, password change, disabled users, anonymous mode | `crates/pipes-api/src/auth_tests.rs` | `pixi run test` | Postgres (pixi) |
| ✅ L3 demo + recording (in-process) | demo mode: writes without credentials, allowlist still enforced, no accounts, CSRF refusal of non-JSON posts, audit to the database or the log; recording API: rules read/replace/validate, recent records without a database, stored records with filters, admin-only rule changes (audited), the database sink flushing on shutdown and counting drops, the Recording page | `crates/pipes-api/src/{demo_tests,recording_tests}.rs` | `pixi run test` | Postgres (pixi) |
| ✅ L3 writes (in-process) | allowlist (incl. for admins), roles, admin-only rules, per-user rate limit, fail-closed audit (no audit → no write), error mapping with linked `error` entries, service calls, the policy endpoint, anonymous mode refusing writes | `crates/pipes-api/src/writes_tests.rs` | `pixi run test` | Postgres (pixi) |
| ✅ L3 e2e (hermetic) | real `rmf-pipes` binary + in-process router + hiroz publisher, HTTP only: `/healthz`, `/metrics`, SIGTERM (idle, and with an SSE client connected), fail-fast without a router, graph, latest, schema, SSE with increasing sequence numbers, standing patterns, docs, dashboard | `crates/rmf-pipes/tests/` | `pixi run test` | Rust |
| ✅ L4 interop | `ros2 topic pub` (rclpy) on `rmw_zenoh_cpp` via a real `rmw_zenohd` → `HirozBus`: hashes, nested, `uint8[]`, int64, `byte`, `string<=N`, transient_local late join; and back: publishing to `ros2 topic echo`, calling an rclpy node's `get_parameters`; recording an rclpy action server's goal, feedback, status and result while `ros2 action send_goal` still gets its result, all under the client's goal id | `crates/pipes-testkit/tests/interop.rs` (`#[ignore]`) | `pixi run interop` | `/opt/ros/$ROS_DISTRO` with rmw_zenoh |

| L5 demo | the whole Nav2 demo: robot0 gets a goal via `ros2 action send_goal`, and its goal, statuses and result must reach `GET /api/v1/records` | `scripts/demo.sh check` | `pixi run demo-check` | Docker, `../next_gen_prototype`, the stack and `pixi run demo` running (see `demo/nav2/README.md`) |

## Rules
- **Hermetic by default.** zenoh tests start an in-process router on a free localhost port, with multicast scouting off. No test depends on a router being already running.
- **Pinned domain.** Test sessions use domain `0` explicitly; hiroz would otherwise read the caller's `ROS_DOMAIN_ID`. The test kit refuses to run if `ZENOH_CONFIG_OVERRIDE` or `ZENOH_SESSION_CONFIG_URI` is set, because hiroz applies them to every session.
- **No vacuous tests.** Assert the precondition before testing the behaviour: the request is in flight, the subscription has matched, the publisher is in the graph.
- **Poll with a deadline, don't sleep.** Waiting on discovery uses a helper that retries until a deadline and reports what it last saw.
- **L5 is manual.** The demo check needs Docker and a 32 GB image, so it stays out of `pixi run ci`; run it after changes to recording, taps or type discovery.
- **L4 skips loudly.** If ROS isn't found, interop tests print `SKIP …` (the task runs with `--nocapture`) and pass. Setting `PIPES_INTEROP_REQUIRED=1` turns the skip into a failure, for a future CI interop job.
- **Distro-consistent tests.** `--features <distro>` on `rmf-pipes` or `pipes-testkit` also selects the distro for hiroz-msgs, and `pixi run distros` compiles the tests for every distro.
- **Clean environment.** Tests that spawn the binary call `env_clear()`, and config tests read defaults from the clap definition, not by parsing.
- **Mutation-check regressions.** Break the code, watch the test fail, restore.
- **Snapshots** (insta) are only for large JSON shapes. Review `.snap.new` files and never accept them blindly.

## Running one thing
```
cargo test -p pipes-core --features fake recorder   # the recorder against FakeBus
cargo test -p pipes-api serve::            # one module
cargo test -p rmf-pipes --test server      # one integration test file
RUST_LOG=hiroz=debug cargo test -p pipes-testkit --test bus -- --nocapture
pixi run interop                           # needs /opt/ros/lyrical with rmw_zenoh
```
