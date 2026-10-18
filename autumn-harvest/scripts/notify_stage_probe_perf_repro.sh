#!/usr/bin/env bash
# Ledger capture: NOTIFY stage probes per workflow on the e2e workload
# (issue #2125).
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5433/postgres \
#     ./autumn-harvest/scripts/notify_stage_probe_perf_repro.sh <label>
#
# The server needs pg_stat_statements in shared_preload_libraries. The script
# resets the statistics, runs the e2e throughput scenario, and writes
# docs/perf-artifacts/notify-stage-probe/<label>.txt with the probe calls per
# workflow and the calls ranking. Wall-clock is not recorded.
#
# Knobs (all optional):
#   HARVEST_BENCH_WORKFLOWS  measured completions (default 200)
#   NOTIFY_STAGE_PROBE_OUT   artifact directory
set -euo pipefail

LABEL="${1:?usage: $0 <label>}"
URL="${HARVEST_TEST_DATABASE_URL:?set HARVEST_TEST_DATABASE_URL (admin URL)}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${NOTIFY_STAGE_PROBE_OUT:-$REPO_ROOT/docs/perf-artifacts/notify-stage-probe}"
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
    w AS (SELECT sum(calls) AS n FROM s
          WHERE query LIKE 'INSERT INTO \"harvest_workflow_executions\"%'),
    p AS (SELECT coalesce(sum(calls), 0) AS n FROM s
          WHERE query LIKE 'SELECT txid_current%AS txid, current_database()%')
    SELECT (SELECT n FROM w) AS workflows,
           sum(calls) AS total_calls,
           (SELECT n FROM p) AS probe_calls,
           round((SELECT n FROM p)::numeric / nullif((SELECT n FROM w), 0), 3)
             AS probes_per_workflow,
           round(100.0 * (SELECT n FROM p) / sum(calls), 2) AS probe_pct_of_calls,
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
} | tee "$OUT/$LABEL.txt"
echo "== done: $OUT/$LABEL.txt"
