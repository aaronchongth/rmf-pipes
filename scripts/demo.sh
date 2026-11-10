#!/usr/bin/env bash
# The local end-to-end demo: next_gen_prototype's Nav2 stack (Gazebo, two
# TurtleBot3s on Nav2, the prototype's traffic nodes, an rmw_zenoh router) in
# Docker, and rmf-pipes on the host recording their navigation actions.
#
#   scripts/demo.sh up               # build (first time) and start the Nav2 stack
#   scripts/demo.sh run              # start rmf-pipes in demo mode (foreground; Ctrl-C stops)
#   scripts/demo.sh goal robot0 5 5  # send robot0 a navigate_to_pose goal to (5, 5)
#   scripts/demo.sh check            # end-to-end check against a running demo
#   scripts/demo.sh down             # stop the Nav2 stack
#
# Normally run through pixi (pixi run demo-up, demo, demo-goal, demo-check,
# demo-down), which provides Postgres. See demo/nav2/README.md.
#
# Environment:
#   NEXT_GEN_PROTOTYPE  checkout of open-rmf/next_gen_prototype (default: ../next_gen_prototype)
#   PIPES_DEMO_ZENOH_PORT  host port of the stack's zenoh router (default 7447)
#   PIPES_DEMO_LISTEN      rmf-pipes address (default 127.0.0.1:18080)
#   PIPES_DEMO_DISTRO      ROS distro rmf-pipes is built for (default jazzy, the stack's)
set -euo pipefail
cd "$(dirname "$0")/.."

PROTO="${NEXT_GEN_PROTOTYPE:-../next_gen_prototype}"
ZENOH_PORT="${PIPES_DEMO_ZENOH_PORT:-7447}"
LISTEN="${PIPES_DEMO_LISTEN:-127.0.0.1:18080}"
DISTRO="${PIPES_DEMO_DISTRO:-jazzy}"
PG_PORT="${PIPES_PG_PORT:-54329}"
DB_NAME="rmf_pipes_demo"
DB_URL="postgres://postgres@127.0.0.1:${PG_PORT}/${DB_NAME}"
API="http://${LISTEN}"

die() { echo "ERROR: $*" >&2; exit 1; }

compose() {
  [ -f "${PROTO}/docker/docker-compose.yml" ] ||
    die "next_gen_prototype not found at ${PROTO}; clone it there or set NEXT_GEN_PROTOTYPE"
  docker info >/dev/null 2>&1 ||
    die "cannot talk to Docker; is it running, and are you in the docker group (log out and in after adding yourself)?"
  local here
  here="$(pwd)"
  (cd "${PROTO}/docker" && PIPES_DEMO_ZENOH_PORT="${ZENOH_PORT}" docker compose -p pipes-nav2 \
    -f docker-compose.yml \
    -f docker-compose.zenoh.override.yml \
    -f docker-compose.headless.override.yml \
    -f "${here}/demo/nav2/compose.override.yml" "$@")
}

wait_port() { # host port timeout_s what
  local i
  for ((i = 0; i < $2 * 2; i++)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  die "$3 is not reachable on 127.0.0.1:$1 after $2s"
}

goal_yaml() { # x y
  printf '{pose: {header: {frame_id: map}, pose: {position: {x: %s, y: %s}, orientation: {w: 1.0}}}}' "$1" "$2"
}

case "${1:-}" in
  up)
    if ! docker image inspect ros2_rust_base:jazzy >/dev/null 2>&1; then
      echo "== building the next_gen_prototype image (first run only; this takes a while)"
      compose build next_gen_prototype
    fi
    echo "== starting the Nav2 stack"
    compose up -d
    wait_port "${ZENOH_PORT}" 60 "the stack's zenoh router"
    echo "== up: zenoh router on tcp/127.0.0.1:${ZENOH_PORT}. Nav2 takes about a minute to become active."
    echo "   next: pixi run demo"
    ;;
  down)
    compose down
    ;;
  run)
    command -v psql >/dev/null || die "psql not found; run this through pixi (pixi run demo)"
    if ! psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" postgres | grep -q 1; then
      psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -qc "CREATE DATABASE ${DB_NAME}" postgres
    fi
    wait_port "${ZENOH_PORT}" 5 "the stack's zenoh router (run: pixi run demo-up)"
    echo "== rmf-pipes (${DISTRO}) in demo mode: dashboard http://${LISTEN}/ui/recording, API docs http://${LISTEN}/docs/"
    # A separate target dir, so switching distro features doesn't rebuild the
    # default (lyrical) build. The stack uses ROS domain 0.
    exec cargo run --locked --no-default-features --features "${DISTRO}" \
      --target-dir target/demo-"${DISTRO}" -p rmf-pipes -- \
      --demo \
      --listen "${LISTEN}" \
      --zenoh-connect "tcp/127.0.0.1:${ZENOH_PORT}" \
      --domain-id 0 \
      --database-url "${DB_URL}" \
      --record-config demo/nav2/record.toml
    ;;
  goal)
    robot="${2:-robot0}" x="${3:-5.0}" y="${4:-5.0}"
    # These end up in a shell command inside the container.
    [[ "${robot}" =~ ^[A-Za-z0-9_]+$ ]] || die "robot name must be letters, digits or _: ${robot}"
    for n in "$x" "$y"; do
      [[ "$n" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || die "coordinates must be numbers: $n"
    done
    docker exec next_gen_prototype bash -c \
      "source /nav2_traffic_ws/install/setup.bash && ros2 action send_goal ${robot}/navigate_to_pose nav2_msgs/action/NavigateToPose '$(goal_yaml "$x" "$y")'"
    ;;
  check)
    command -v curl >/dev/null || die "curl not found"
    curl -sf "${API}/healthz" >/dev/null || die "rmf-pipes is not running at ${API} (run: pixi run demo)"
    echo "== waiting for the recording streams of robot0's navigate_to_pose"
    for i in $(seq 1 60); do
      active=$(curl -sf "${API}/api/v1/recording" |
        python3 -c 'import json,sys; v=json.load(sys.stdin); print(sum(1 for s in v["streams"] if s["name"]=="/robot0/navigate_to_pose" and s["status"]["state"]=="active"))')
      [ "${active}" -ge 3 ] && break
      [ "$i" -eq 60 ] && die "robot0's recording streams are not active; see ${API}/ui/recording"
      sleep 1
    done
    echo "== sending robot0 a goal to (1.0, 5.0)"
    out=$("$0" goal robot0 1.0 5.0)
    echo "${out}" | grep -E "Goal (accepted|finished)" || true
    id=$(echo "${out}" | sed -n 's/^Goal accepted with ID: \([0-9a-f]*\).*/\1/p')
    [ -n "${id}" ] || die "the goal was not accepted: ${out}"
    uuid="${id:0:8}-${id:8:4}-${id:12:4}-${id:16:4}-${id:20:12}"
    echo "== looking for goal ${uuid} in the database"
    for i in $(seq 1 20); do
      parts=$(curl -sf "${API}/api/v1/records?goal_id=${uuid}" |
        python3 -c 'import json,sys; print(" ".join(sorted({r["part"] for r in json.load(sys.stdin)})))')
      missing=""
      for want in goal status result; do
        case " ${parts} " in *" ${want} "*) ;; *) missing="${missing} ${want}" ;; esac
      done
      [ -z "${missing}" ] && break
      [ "$i" -eq 20 ] && die "goal ${uuid} was not fully recorded (missing:${missing})"
      sleep 1
    done
    echo "== recorded parts: ${parts}"
    echo "OK: the goal, its status changes and its result are in the database"
    ;;
  *)
    sed -n '2,20p' "$0" >&2
    exit 2
    ;;
esac
