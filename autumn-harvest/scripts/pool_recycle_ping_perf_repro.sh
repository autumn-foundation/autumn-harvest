#!/usr/bin/env bash
# Ledger capture: pool recycle pings per claimed activity on the e2e workload.
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/postgres \
#     ./autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh <label>
#
# The server needs pg_stat_statements in shared_preload_libraries. The script
# resets the statistics, runs the e2e throughput scenario, and writes
# docs/perf-artifacts/pool-recycle-ping/<label>.txt with the calls ranking and
# the pings per claimed activity. Wall-clock is not recorded.
#
# Knobs (all optional):
#   HARVEST_BENCH_WORKFLOWS  measured completions (default 200)
#   POOL_RECYCLE_OUT         artifact directory
set -euo pipefail

LABEL="${1:?usage: $0 <label>}"
URL="${HARVEST_TEST_DATABASE_URL:?set HARVEST_TEST_DATABASE_URL (admin URL)}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${POOL_RECYCLE_OUT:-$REPO_ROOT/docs/perf-artifacts/pool-recycle-ping}"
mkdir -p "$OUT"
cd "$REPO_ROOT"

psql "$URL" -X -qAt -c "SELECT pg_stat_statements_reset()" >/dev/null

HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
  HARVEST_BENCH_WORKFLOWS="${HARVEST_BENCH_WORKFLOWS:-200}" \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench \
  >"$OUT/$LABEL-bench.log" 2>&1 || {
  echo "FATAL: bench failed. See $OUT/$LABEL-bench.log" >&2
  exit 1
}

{
  echo "== label: $LABEL"
  psql "$URL" -X -c "
    WITH s AS (SELECT * FROM pg_stat_statements
               WHERE query NOT ILIKE '%pg_stat_statements%'),
    t AS (SELECT sum(calls) AS c FROM s),
    a AS (SELECT sum(calls) AS n FROM s
          WHERE query LIKE 'UPDATE harvest_task_queue SET state = \$3, worker_id = \$4, started_at = \$5, scheduled_at%')
    SELECT (SELECT n FROM a) AS claimed_activities,
           (SELECT c FROM t) AS total_calls,
           coalesce(sum(calls) FILTER (WHERE query = 'SELECT \$1'), 0) AS ping_calls,
           round(coalesce(sum(calls) FILTER (WHERE query = 'SELECT \$1'), 0)::numeric
                 / nullif((SELECT n FROM a), 0), 3) AS pings_per_activity,
           round(100.0 * coalesce(sum(calls) FILTER (WHERE query = 'SELECT \$1'), 0)
                 / (SELECT c FROM t), 2) AS ping_pct_of_calls,
           sum(shared_blks_hit + shared_blks_read) AS total_blocks,
           sum(temp_blks_written) AS temp_blks_written,
           sum(wal_bytes) AS wal_bytes
    FROM s"
  echo "== top 10 by calls"
  psql "$URL" -X -c "
    SELECT calls, shared_blks_hit + shared_blks_read AS blks,
           left(regexp_replace(query, '\s+', ' ', 'g'), 100) AS query
    FROM pg_stat_statements WHERE query NOT ILIKE '%pg_stat_statements%'
    ORDER BY calls DESC LIMIT 10"
  echo "== EXPLAIN of the ping statement"
  psql "$URL" -X -c "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT 1"
} | tee "$OUT/$LABEL.txt"
echo "== done: $OUT/$LABEL.txt"
