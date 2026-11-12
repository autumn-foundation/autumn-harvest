#!/usr/bin/env bash
# Usage: run_cell.sh <label> <binary> <scenarios> <outdir>
# Runs one e2e_bench invocation at 1 shard, and records the claim census.
set -u
label=$1; bin=$2; scen=$3; out=$4
mkdir -p "$out"
PSQL="psql -h 127.0.0.1 -p 55432 -U postgres -At -q"
$PSQL -c "SELECT pg_stat_statements_reset()" >/dev/null
start=$(date +%s.%N)
HARVEST_BENCH_SHARD_URLS=postgres://postgres:postgres@127.0.0.1:55432/postgres \
HARVEST_BENCH_SCENARIOS=$scen HARVEST_BENCH_SHARDS=1 \
  "$bin" --bench > "$out/$label.md" 2>"$out/$label.err"
code=$?
end=$(date +%s.%N)
census=$($PSQL -F' ' -c "
SELECT coalesce(sum(calls),0), coalesce(sum(rows),0)
  FROM pg_stat_statements
 WHERE dbid NOT IN (SELECT oid FROM pg_database)
   AND query LIKE 'WITH worker_info AS%'")
set -- $census
calls=$1; rows=$2
wall=$(echo "$end - $start" | bc)
empty=$((calls - rows))
eps=$(echo "scale=1; $empty / $wall" | bc)
echo "$label exit=$code wall_s=$wall claim_calls=$calls claimed_rows=$rows empty_polls=$empty empty_polls_per_s=$eps" | tee -a "$out/census.txt"
