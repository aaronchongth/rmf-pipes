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
