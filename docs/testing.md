# Testing

The goal is that **an agent can verify any change with one command, without ROS or docker.**

Status: ✅ in place · 📋 arrives in phase 1.

| Level | What | Where | Command | Needs |
|---|---|---|---|---|
| ✅ L0 static | rustfmt, clippy `-D warnings`, `missing_docs`, dependency invariants, build of every distro feature, distro guard | whole workspace | `pixi run ci` | Rust |
| ✅ L1 unit | pure logic: config, serve/drain; 📋 JSON mapping, pattern matching, hub lifecycle, mismatch detection | `#[cfg(test)]` in each crate | `pixi run test` | Rust |
| 📋 L2 bus (hermetic) | hiroz publishers → in-process zenoh router → `pipes-bus` → asserted JSON | `crates/pipes-testkit/tests/` | `pixi run test` | Rust |
| ✅ L3 e2e (hermetic) | real `rmf-pipes` binary over HTTP, SIGTERM; 📋 plus in-process router + hiroz publisher, SSE | `crates/rmf-pipes/tests/` | `pixi run test` | Rust |
| 📋 L4 interop | real `rclcpp` publishers on `rmw_zenoh_cpp` via `rmw_zenohd` | `crates/pipes-testkit/tests/interop.rs` | `pixi run interop` | `/opt/ros/$ROS_DISTRO` with rmw_zenoh |

## Rules
- **Hermetic by default.** zenoh tests start an in-process router on a free localhost port, with multicast scouting off. No test depends on a router being already running.
- **No vacuous tests.** Assert the precondition before testing the behaviour: the request is in flight, the subscription has matched, the publisher is in the graph.
- **Poll with a deadline, don't sleep.** Waiting on discovery uses a helper that retries until a deadline and reports what it last saw.
- **L4 skips loudly.** If ROS isn't found, interop tests print why and pass trivially. 📋 A CI interop job will set `PIPES_INTEROP_REQUIRED=1`, which turns that skip into a failure.
- **Clean environment.** Tests that spawn the binary call `env_clear()`, and config tests read defaults from the clap definition, not by parsing.
- **Mutation-check regressions.** Break the code, watch the test fail, restore.
- **Snapshots** (insta) are only for large JSON shapes. Review `.snap.new` files and never accept them blindly.

## Running one thing
```
cargo test -p pipes-api serve::            # one module
cargo test -p rmf-pipes --test server      # one integration test file
RUST_LOG=pipes_bus=debug cargo test -p pipes-testkit -- --nocapture   # phase 1
```
