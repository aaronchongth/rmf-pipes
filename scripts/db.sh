#!/usr/bin/env bash
# A throwaway local Postgres for tests and development, from the pixi env.
#   scripts/db.sh start   # init on first use, start if not running (idempotent)
#   scripts/db.sh stop
#   scripts/db.sh url     # print the DATABASE_URL
# Data lives in .pixi/pgdata (gitignored); `rm -rf .pixi/pgdata` resets it.
# Listens on 127.0.0.1:${PIPES_PG_PORT:-54329} only, with trust auth: never
# use this for anything but local testing.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PIPES_PG_PORT:-54329}"
DATA=".pixi/pgdata"
URL="postgres://postgres@127.0.0.1:${PORT}/postgres"

need() {
  command -v "$1" >/dev/null || {
    echo "ERROR: $1 not found; run this through pixi (pixi run db-start)" >&2
    exit 1
  }
}

case "${1:-}" in
  start)
    need initdb
    need pg_ctl
    if [ ! -f "${DATA}/PG_VERSION" ]; then
      echo "== initdb ${DATA}"
      initdb --pgdata "${DATA}" --username postgres --auth trust --encoding UTF8 --no-instructions >/dev/null
    fi
    if pg_ctl --pgdata "${DATA}" status >/dev/null 2>&1; then
      echo "== postgres already running: ${URL}"
    else
      echo "== starting postgres: ${URL}"
      pg_ctl --pgdata "${DATA}" --log "${DATA}/server.log" --wait --silent \
        -o "-p ${PORT} -c listen_addresses=127.0.0.1 -k $(pwd)/${DATA} -c max_connections=200 -c fsync=off" start
    fi
    ;;
  stop)
    need pg_ctl
    if [ -f "${DATA}/PG_VERSION" ] && pg_ctl --pgdata "${DATA}" status >/dev/null 2>&1; then
      pg_ctl --pgdata "${DATA}" --wait --silent stop
      echo "== postgres stopped"
    fi
    ;;
  url)
    echo "${URL}"
    ;;
  *)
    echo "usage: $0 start|stop|url" >&2
    exit 2
    ;;
esac
