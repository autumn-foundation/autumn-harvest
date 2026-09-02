#!/usr/bin/env bash
# The one documented command for the end-to-end benchmark suite (issue #941).
#
#   ./benchmarks/run.sh
#
# Brings up the four-shard Postgres topology in `docker-compose.yml`, runs every
# scenario at 1, 2 and 4 shards, writes the report to
# `benchmarks/results/<timestamp>.md`, and tears the topology down again.
#
# Options (environment):
#   HARVEST_BENCH_KEEP=1     leave the containers running afterwards
#   HARVEST_BENCH_CHECK=1    also compare this run against the published
#                            baselines at the documented tolerance and print a
#                            per-number verdict
#   HARVEST_BENCH_OUT=<path> write the report somewhere else
#
# Written for bash 3.2 so it runs on a stock macOS shell as well as Linux.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE_FILE="$HERE/docker-compose.yml"

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required: see https://docs.docker.com/compose/install/" >&2
  exit 1
fi

cleanup() {
  if [ "${HARVEST_BENCH_KEEP:-0}" = "1" ]; then
    echo "==> leaving the benchmark topology running (HARVEST_BENCH_KEEP=1)"
    return
  fi
  echo "==> tearing down the benchmark topology"
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> starting 4 Postgres shards"
docker compose -f "$COMPOSE_FILE" up -d --wait

SHARD_URLS="postgres://postgres:postgres@127.0.0.1:55432/postgres"
SHARD_URLS="$SHARD_URLS,postgres://postgres:postgres@127.0.0.1:55433/postgres"
SHARD_URLS="$SHARD_URLS,postgres://postgres:postgres@127.0.0.1:55434/postgres"
SHARD_URLS="$SHARD_URLS,postgres://postgres:postgres@127.0.0.1:55435/postgres"

OUT="${HARVEST_BENCH_OUT:-$HERE/results/$(date -u +%Y%m%dT%H%M%SZ).md}"
mkdir -p "$(dirname "$OUT")"

echo "==> running the suite (this takes roughly 20-40 minutes)"
cd "$ROOT"
HARVEST_BENCH_SHARD_URLS="$SHARD_URLS" \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench \
  | tee "$OUT"

echo "==> report written to $OUT"
