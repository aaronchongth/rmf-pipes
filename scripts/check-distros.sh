#!/usr/bin/env bash
# Build-check every supported ROS 2 distro feature, and prove that the distro
# guard rejects zero or several distros. Run via `pixi run distros`.
set -euo pipefail
cd "$(dirname "$0")/.."

for distro in jazzy kilted lyrical rolling; do
  echo "== cargo check --features ${distro}"
  cargo check --locked -p rmf-pipes --no-default-features --features "${distro}" --quiet
done

# expect_guard <description> <expected error text> <cargo feature args...>
expect_guard() {
  local what=$1 expected=$2
  shift 2
  echo "== ${what} must fail"
  local out
  if out=$(cargo check --locked -p rmf-pipes "$@" --quiet 2>&1); then
    echo "ERROR: ${what} built successfully; the distro guard is broken" >&2
    exit 1
  fi
  if ! grep -q "${expected}" <<<"${out}"; then
    echo "ERROR: ${what} failed, but not because of the distro guard:" >&2
    echo "${out}" >&2
    exit 1
  fi
}

expect_guard "two distros" "more than one ROS 2 distro feature" --no-default-features --features jazzy,lyrical
expect_guard "no distro" "select exactly one ROS 2 distro feature" --no-default-features
echo "ok: the distro guard rejects zero and multiple distros"
