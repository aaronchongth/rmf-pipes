# Testing

The goal is that **an agent can verify any change with one command, without ROS or docker.**

Status: ✅ in place · 📋 arrives in phase 2.

| Level | What | Where | Command | Needs |
|---|---|---|---|---|
| ✅ L0 static | rustfmt, clippy `-D warnings`, `missing_docs`, dependency invariants, build of every distro feature, distro guard | whole workspace | `pixi run ci` | Rust |
| ✅ L1 unit | pure logic: config, serve/drain, JSON mapping (every hiroz `FieldType`, through CDR), type-description conversion (every ROS type id), subscription QoS and publisher choice, pattern matching, graph summaries, hub lifecycle against `FakeBus` with paused time | `#[cfg(test)]` in each crate | `pixi run test` | Rust |
| ✅ L2 bus (hermetic) | typed hiroz publishers → in-process zenoh router → `HirozBus` (and the hub): graph, decoding, transient_local late join, mismatches, errors, binding to an advertised hash hiroz wouldn't compute | `crates/pipes-testkit/tests/bus.rs` | `pixi run test` | Rust |
| ✅ L3 e2e (hermetic) | real `rmf-pipes` binary + in-process router over HTTP: `/healthz`, `/metrics`, SIGTERM, fail-fast without a router; 📋 SSE + sample flow | `crates/rmf-pipes/tests/` | `pixi run test` | Rust |
| ✅ L4 interop | `ros2 topic pub` (rclpy) on `rmw_zenoh_cpp` via a real `rmw_zenohd` → `HirozBus`: hashes, nested, `uint8[]`, int64, `byte`, `string<=N`, transient_local late join | `crates/pipes-testkit/tests/interop.rs` (`#[ignore]`) | `pixi run interop` | `/opt/ros/$ROS_DISTRO` with rmw_zenoh |

## Rules
- **Hermetic by default.** zenoh tests start an in-process router on a free localhost port, with multicast scouting off. No test depends on a router being already running.
- **Pinned domain.** Test sessions use domain `0` explicitly; hiroz would otherwise read the caller's `ROS_DOMAIN_ID`. The test kit refuses to run if `ZENOH_CONFIG_OVERRIDE` or `ZENOH_SESSION_CONFIG_URI` is set, because hiroz applies them to every session.
- **No vacuous tests.** Assert the precondition before testing the behaviour: the request is in flight, the subscription has matched, the publisher is in the graph.
- **Poll with a deadline, don't sleep.** Waiting on discovery uses a helper that retries until a deadline and reports what it last saw.
- **L4 skips loudly.** If ROS isn't found, interop tests print `SKIP …` (the task runs with `--nocapture`) and pass. Setting `PIPES_INTEROP_REQUIRED=1` turns the skip into a failure, for a future CI interop job.
- **Distro-consistent tests.** `--features <distro>` on `rmf-pipes` or `pipes-testkit` also selects the distro for hiroz-msgs, and `pixi run distros` compiles the tests for every distro.
- **Clean environment.** Tests that spawn the binary call `env_clear()`, and config tests read defaults from the clap definition, not by parsing.
- **Mutation-check regressions.** Break the code, watch the test fail, restore.
- **Snapshots** (insta) are only for large JSON shapes. Review `.snap.new` files and never accept them blindly.

## Running one thing
```
cargo test -p pipes-api serve::            # one module
cargo test -p rmf-pipes --test server      # one integration test file
RUST_LOG=hiroz=debug cargo test -p pipes-testkit --test bus -- --nocapture
pixi run interop                           # needs /opt/ros/lyrical with rmw_zenoh
```
