#!/usr/bin/env bash
# Enforce the dependency invariants in AGENTS.md:
#   1. only pipes-bus (and the test kit) may depend on hiroz or zenoh;
#   2. every workspace crate builds and its tests compile on their own (no
#      reliance on feature unification from sibling crates).
# Run via `pixi run invariants`.
set -euo pipefail
cd "$(dirname "$0")/.."

for crate in pipes-core pipes-api; do
  echo "== ${crate} must not depend on hiroz/zenoh"
  # Normal and build edges only: dev-only test helpers are allowed.
  if cargo tree --locked -p "${crate}" -e normal,build --prefix none --format '{p}' \
      | grep -E '^(hiroz|zenoh)[a-z_-]* ' ; then
    echo "ERROR: ${crate} depends on hiroz/zenoh (see above); only pipes-bus may" >&2
    exit 1
  fi
done

for crate in $(cargo metadata --locked --no-deps --format-version 1 \
    | grep -oE '"name":"[a-z0-9_-]+","version"' | sed -E 's/"name":"([^"]+)".*/\1/'); do
  echo "== ${crate} tests compile on their own"
  cargo test --locked -p "${crate}" --no-run --quiet
done
echo "ok: dependency invariants hold"
