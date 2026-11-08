# rmf-pipes

rmf-pipes is the data layer for next-generation [Open-RMF](https://github.com/open-rmf). It's a Rust server that:
- watches a ROS 2 system over [zenoh](https://zenoh.io), via [hiroz](https://github.com/ZettaScaleLabs/hiroz) and rmw_zenoh
- decodes any topic at runtime
- serves the data over REST, server-sent events and OpenAPI, plus a minimal introspection dashboard

Status: early development, see [docs/roadmap.md](docs/roadmap.md).

## Quick start
```
pixi run ci                           # format, lint, every hermetic test, every distro build
pixi run run -- --help                # options (all also settable as RMF_PIPES_* env vars)
pixi run run                          # serve on 127.0.0.1:18080
curl -s localhost:18080/healthz
```
You need Rust via [rustup](https://rustup.rs); `rust-toolchain.toml` pins the version. [pixi](https://pixi.sh) is only a task runner here, so every task is a plain `cargo` command in `pixi.toml`.

## API
- `GET /api/v1/graph`: nodes, topics (with type hashes), services, type mismatches
- `GET /api/v1/topics`: what rmf-pipes is subscribed to, with stats
- `GET /api/v1/topic?name=/chatter`: one topic's graph summary and subscription state
- `GET /api/v1/topic/latest?name=/chatter`: latest sample, subscribing on demand
- `GET /api/v1/topic/schema?name=/chatter`: the message definition
- `GET /api/v1/topic/stream?name=/chatter`: live samples as server-sent events
- `GET /openapi.json`, `GET /docs/`: the OpenAPI description and Swagger UI
- `GET /`: the introspection dashboard. `GET /metrics`: Prometheus metrics.

Payloads follow [docs/json-mapping.md](docs/json-mapping.md), and all options are in [docs/configuration.md](docs/configuration.md).

Try it against a local ROS 2 system on rmw_zenoh:
```
source /opt/ros/lyrical/setup.bash
export RMW_IMPLEMENTATION=rmw_zenoh_cpp
ros2 run rmw_zenoh_cpp rmw_zenohd &
ros2 topic pub /chatter std_msgs/msg/String "{data: hi}" &
pixi run run -- --ingest '**'     # then open http://127.0.0.1:18080/
```
`--ingest '**'` subscribes to everything that has a publisher, which is fine for a demo. On a real site, list the topics you need.

## Offline / air-gapped builds
`--locked` still needs the crates.io index. For a site with no network:
1. run `cargo vendor` on a connected machine
2. ship the `vendor/` directory together with the `.cargo/config.toml` snippet it prints
3. build with `cargo build --frozen`

## ROS 2 distros
Each build targets one distro. The default is Lyrical:
```
cargo build -p rmf-pipes --no-default-features --features jazzy   # or kilted, rolling
```

## Docs
- [architecture](docs/architecture.md): components, data flow, invariants
- [decisions](docs/decisions.md): what's settled, and why
- [testing](docs/testing.md): test levels and how to run them
- [roadmap](docs/roadmap.md): phases and status
- [AGENTS.md](AGENTS.md): the working guide for coding agents (and humans)

## License
Apache-2.0
