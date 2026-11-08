# Agent guide for rmf-pipes

This file is the single source of instructions for coding agents (any provider). `CLAUDE.md` only points here.

## What this is
rmf-pipes is the data layer for next-generation Open-RMF. It's a Rust server that:
- watches the ROS 2 graph over zenoh, using [hiroz](https://github.com/ZettaScaleLabs/hiroz) and rmw_zenoh conventions
- decodes messages at runtime
- serves them over REST, SSE, OpenAPI and a small introspection dashboard

Read `docs/architecture.md` before changing structure, and `docs/decisions.md` before reopening a settled question.

## Commands (run from the repo root)
| Command | What it does | Needs |
|---|---|---|
| `pixi run check` | fmt check, clippy `-D warnings`, all hermetic tests | Rust (rustup) |
| `pixi run ci` | `check` + dependency invariants + every ROS distro build + distro guard | Rust |
| `pixi run test` | hermetic tests only | Rust |
| `pixi run invariants` | only `pipes-bus` uses hiroz/zenoh; each crate's tests compile alone | Rust |
| `pixi run distros` | build-check each distro feature; zero or two distros must fail | Rust |
| `pixi run run -- --help` | run the server | Rust |
| `pixi run interop` | L4 tests against a real `rmw_zenohd` + `ros2 topic pub` (skips loudly without ROS) | `/opt/ros/<distro>` with rmw_zenoh |

Without pixi, the tasks in `pixi.toml` are plain `cargo` commands, so you can run them directly.

**Definition of done for any change:** `pixi run ci` passes, and new behaviour has a test that fails without the change.

## Invariants (don't break these without updating docs/decisions.md)
1. **Only `pipes-bus` (and the `pipes-testkit` test crate) may depend on `hiroz` or `zenoh`.** `pipes-core` and `pipes-api` are bus-agnostic and are tested with in-memory fakes. Enforced by `pixi run invariants`.
2. **Exactly one ROS distro feature per build.** The default is `lyrical`. Other builds use `--no-default-features --features <distro>`. The guard in `pipes-bus/src/distro.rs` enforces it, and `pixi run invariants` checks hiroz's effective features across the build, including dev-dependencies.
3. **`hiroz` and `zenoh` are pinned exactly** in the workspace `Cargo.toml`. Upgrade deliberately, in its own commit, gated by the interop tests.
4. **Never print secrets.**
   - Every env-backed CLI arg sets `hide_env_values`, and a test enforces it.
   - clap echoes rejected values in parse errors, so a secret-bearing arg must be parsed as a plain `String` and validated afterwards with a redacted error.
   - Health responses never include raw dependency errors; log the details instead.
5. **Every server-side stream ends on shutdown.** The HTTP server has a drain deadline (`pipes-api/src/serve.rs`), but SSE handlers must still watch the shutdown token.
6. **Bounded everything.** Channels, queues and caches have explicit capacities and a documented drop policy. Nothing subscribes to `**` by default.
7. **No Node/npm.** The dashboard is server-rendered, and its vendored assets are committed under `crates/pipes-api/assets/`.

## Testing rules
- Tests are hermetic by default: no ROS install, no docker, no network beyond localhost. zenoh tests use `pipes_testkit::TestRouter` (an in-process router on a free port) and `fixture_node` (hiroz publishers with type descriptions).
- Tests that use zenoh need a multi-thread tokio runtime: `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`.
- Tests that spawn the binary use `env_clear()`, so the caller's `RUST_LOG`/`RMF_PIPES_*` can't change the outcome.
- Interop tests that need a real ROS install are `#[ignore]`d and run with `pixi run interop`. They skip with a loud message when ROS is missing.
- Avoid vacuous tests. Prove the precondition first, for example "the request is in flight" before testing shutdown. The drain-deadline test in `serve.rs` is the model.
- Before calling a regression test done, check that it fails against the bug: break the code, run the test, restore. Prefer deterministic checks (log order, state) to racing real timing.
- Prefer polling with a deadline over a fixed `sleep`.
- See `docs/testing.md` for the levels.

## Workflow
- Work happens on `dev`, one commit per reviewed step. **Never push.** The maintainer merges.
- Keep `docs/` current in the same commit as the code it describes. `docs/roadmap.md` tracks phase status.
- Code style:
  - match the surrounding code
  - doc comments on public items (the `missing_docs` lint enforces this)
  - `thiserror` for library errors, `anyhow` only in the binary
