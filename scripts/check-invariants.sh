#!/usr/bin/env bash
# Enforce the dependency invariants in AGENTS.md:
#   1. only pipes-bus (and the test kit) may depend on hiroz or zenoh;
#   2. hiroz ends up with exactly one ROS distro feature, counting every crate
#      and dev-dependency in the build (pipes-bus's guard only sees its own);
#   3. every workspace crate builds and its tests compile on their own (no
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

echo "== hiroz has exactly one distro feature (all crates, dev-deps included)"
distros=$(cargo tree --locked --workspace -e features,normal,build,dev -i hiroz --prefix none \
  | grep -oE '^hiroz feature "(humble|jazzy|kilted|lyrical|rolling)"' | sort -u)
if [ "$(grep -c . <<<"${distros}")" -ne 1 ]; then
  echo "ERROR: expected exactly one hiroz distro feature, got:" >&2
  echo "${distros:-<none>}" >&2
  exit 1
fi
echo "   ${distros}"

for crate in $(cargo metadata --locked --no-deps --format-version 1 \
    | grep -oE '"name":"[a-z0-9_-]+","version"' | sed -E 's/"name":"([^"]+)".*/\1/'); do
  echo "== ${crate} tests compile on their own"
  cargo test --locked -p "${crate}" --no-run --quiet
done
echo "ok: dependency invariants hold"
