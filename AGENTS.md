# Agent guide for rmf-pipes

This file is the single source of instructions for coding agents (any provider). `CLAUDE.md` only points here.

## What this is
rmf-pipes is the data layer for next-generation Open-RMF. It's a Rust server that:
- watches the ROS 2 graph over zenoh, using [hiroz](https://github.com/ZettaScaleLabs/hiroz) and rmw_zenoh conventions
- decodes messages at runtime
- serves them over REST, SSE, OpenAPI and a small introspection dashboard
- records chosen topics, service requests and action parts to the console or Postgres (`docs/recording.md`)

Read `docs/architecture.md` before changing structure, and `docs/decisions.md` before reopening a settled question.

## Commands (run from the repo root)
| Command | What it does | Needs |
|---|---|---|
| `pixi run check` | fmt check, clippy `-D warnings`, all hermetic tests | Rust (rustup) |
| `pixi run ci` | `check` + dependency invariants + every ROS distro build + distro guard | Rust |
| `pixi run test` | hermetic tests only; starts the local test Postgres first | Rust, pixi |
| `pixi run db-start` / `db-stop` | the local test Postgres (`DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres`) | pixi |
| `pixi run invariants` | only `pipes-bus` uses hiroz/zenoh; each crate's tests compile alone | Rust |
| `pixi run distros` | build-check each distro feature; zero or two distros must fail | Rust |
| `pixi run run -- --help` | run the server | Rust |
| `pixi run interop` | L4 tests against a real `rmw_zenohd` + `ros2` CLI and rclpy nodes (skips loudly without ROS) | `/opt/ros/<distro>` with rmw_zenoh |
| `pixi run demo-up` / `demo` / `demo-check` / `demo-down` | the Nav2 demo: next_gen_prototype's stack in Docker, rmf-pipes in `--demo` mode on the host, and an end-to-end check (`demo/nav2/README.md`) | Docker, `../next_gen_prototype` |

Without pixi, the tasks in `pixi.toml` are plain `cargo` commands, so you can run them directly.

**Definition of done for any change:** `pixi run ci` passes, and new behaviour has a test that fails without the change.

## Invariants (don't break these without updating docs/decisions.md)
1. **Only `pipes-bus` (and the `pipes-testkit` test crate) may depend on `hiroz` or `zenoh`.** `pipes-core` and `pipes-api` are bus-agnostic and are tested with in-memory fakes. Enforced by `pixi run invariants`.
2. **Exactly one ROS distro feature per build.** The default is `lyrical`. Other builds use `--no-default-features --features <distro>`. The guard in `pipes-bus/src/distro.rs` enforces it, and `pixi run invariants` checks hiroz's effective features across the build, including dev-dependencies.
3. **`hiroz` and `zenoh` are pinned exactly** in the workspace `Cargo.toml`. Upgrade deliberately, in its own commit, gated by the interop tests.
4. **Never print secrets.**
   - `--database-url` and `--bootstrap-password` are plain strings, and `pipes_db::Db::connect` reports host, port and database only. Tests prove the password never appears.
   - API tokens and session secrets are never logged or stored in plaintext; only SHA-256 hashes go to the database.
   - Every env-backed CLI arg sets `hide_env_values`, and a test enforces it.
   - clap echoes rejected values in parse errors, so a secret-bearing arg must be parsed as a plain `String` and validated afterwards with a redacted error.
   - Health responses never include raw dependency errors; log the details instead.
5. **Every server-side stream ends on shutdown.** The HTTP server has a drain deadline (`pipes-api/src/serve.rs`), but SSE handlers must still watch the shutdown token.
6. **Bounded everything.** Channels, queues and caches have explicit capacities and a documented drop policy. Nothing subscribes to `**` by default, and nothing is recorded or tapped without a rule.
7. **Every route outside `auth::is_public` requires credentials when auth is on.** `auth_tests::every_protected_route_needs_credentials` walks the OpenAPI document to prove it. A new public route must be added to `is_public` deliberately.
8. **No Node/npm.** The dashboard is server-rendered, and its vendored assets are committed under `crates/pipes-api/assets/`.
9. **Taps are passive and rule-scoped.** `Bus::tap` queryables are `complete`, never reply, and exist only for services matched by a recording rule that have a server. No API input becomes a tapped key expression.
10. **Demo mode is loopback-only.** `--demo` makes every caller an admin; the binary refuses it on a non-loopback address, and the CSRF (JSON-only) rule covers every non-GET request in that mode.

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
