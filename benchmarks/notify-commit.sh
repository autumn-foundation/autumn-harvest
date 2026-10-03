#!/usr/bin/env bash
# Commit throughput with NOTIFY inside and after the write transaction
# (issue #1796).
#
#   ./benchmarks/notify-commit.sh
#
# Starts one throwaway Postgres 16, runs `notify_commit_bench` against it,
# writes the report to `benchmarks/results/notify-commit-<timestamp>.md`, and
# removes the container.
#
# Options (environment):
#   HARVEST_NOTIFY_BENCH_SECS=<n>        measured window per scenario (default 5)
#   HARVEST_NOTIFY_BENCH_WRITERS=<list>  writer counts (default 1,4,16,32)
#   HARVEST_BENCH_OUT=<path>             write the report somewhere else
#
# Written for bash 3.2 so it runs on a stock macOS shell as well as Linux.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
NAME="harvest-notify-bench-$$"
PORT="${HARVEST_NOTIFY_BENCH_PORT:-55440}"

if ! docker version >/dev/null 2>&1; then
  echo "docker is required: see https://docs.docker.com/get-docker/" >&2
  exit 1
fi

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> starting Postgres 16 on port $PORT"
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres -p "$PORT:5432" postgres:16 >/dev/null
# The init server listens on a socket only, so a TCP check waits for the real one.
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

OUT="${HARVEST_BENCH_OUT:-$HERE/results/notify-commit-$(date -u +%Y%m%dT%H%M%SZ).md}"
mkdir -p "$(dirname "$OUT")"

echo "==> running notify_commit_bench"
cd "$ROOT"
HARVEST_NOTIFY_BENCH_URL="postgres://postgres:postgres@127.0.0.1:$PORT/postgres" \
  cargo bench -p autumn-harvest --features db --bench notify_commit_bench \
  | tee "$OUT"

echo "==> report written to $OUT"
