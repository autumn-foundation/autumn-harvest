#!/usr/bin/env bash
# Usage: census.sh <label> <retention_reclaim_bench binary> <outdir>
#
# Runs retention_reclaim_bench at the small scale with track_planning on.
# Writes the census of census.sql for the unpartitioned arm.
# The bench keeps its databases, so the script drops them first.
set -u
label=$1; bin=$2; out=$3
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$out"
PGHOST=${PGHOST:-127.0.0.1}; PGPORT=${PGPORT:-5432}
PSQL="psql -h $PGHOST -p $PGPORT -U postgres -d postgres -At -q"
for db in $($PSQL -c "SELECT datname FROM pg_database WHERE datname LIKE 'harvest_bench_%'"); do
  $PSQL -c "DROP DATABASE $db"
done
$PSQL -c "ALTER SYSTEM SET pg_stat_statements.track_planning = on" -c "SELECT pg_reload_conf()" >/dev/null
$PSQL -c "SELECT pg_stat_statements_reset()" >/dev/null
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@$PGHOST:$PGPORT/postgres \
  "$bin" --bench > "$out/$label-bench.md" 2>"$out/$label-bench.err"
echo "exit=$?"
flat=$($PSQL -c "SELECT oid FROM pg_database WHERE datname LIKE 'harvest_bench_flat_%'")
# The small scale seeds 20,000 executions, and half of them expire.
psql -h "$PGHOST" -p "$PGPORT" -U postgres -d postgres -F' | ' -At \
  -v flat="$flat" -v candidates=10000 \
  -f "$here/census.sql" > "$out/$label-census.txt"
$PSQL -c "ALTER SYSTEM SET pg_stat_statements.track_planning = off" -c "SELECT pg_reload_conf()" >/dev/null
