#!/usr/bin/env bash
# Usage: census.sh <label> <e2e_bench binary> <outdir>
# Runs the throughput cell at 1 shard with track_planning on, and writes the
# census of docs/perf-artifacts/hot-path-statement-cache/census.sql.
set -u
label=$1; bin=$2; out=$3
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$out"
PSQL="psql -h 127.0.0.1 -p 55432 -U postgres -At -q"
$PSQL -c "ALTER SYSTEM SET pg_stat_statements.track_planning = on" -c "SELECT pg_reload_conf()" >/dev/null
$PSQL -c "SELECT pg_stat_statements_reset()" >/dev/null
HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
  "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
echo "exit=$?"
psql -h 127.0.0.1 -p 55432 -U postgres -F' | ' -At \
  -f "$here/../hot-path-statement-cache/census.sql" > "$out/$label-census.txt"
$PSQL -c "ALTER SYSTEM SET pg_stat_statements.track_planning = off" -c "SELECT pg_reload_conf()" >/dev/null
