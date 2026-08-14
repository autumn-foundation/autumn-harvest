#!/usr/bin/env bash
# Reproduction harness for the `min_history_events` / `history_bloat_min_events`
# buffer-cost measurement documented in `docs/performance-history-bloat-filter.md`.
#
# Seeds a production-shaped `harvest_events` fixture (5,000 ordinary
# executions with 5-80 events each, plus 15 long-running "bloated"
# executions with 50k-450k events each — the exact shape the
# `history_bloat_min_events` early-warning filter, issue #704, exists to
# find) directly against a real, migrated Postgres database, then captures
# `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` for the pre- and post-fix
# forms of the correlated-subquery threshold predicate used by
# `load_workflows`, `load_stalled_workflows`, and `load_history_bloat_workflows`
# (`autumn-harvest-plugin/src/api.rs`).
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh
#
# `HARVEST_TEST_DATABASE_URL` is treated as an ADMIN URL (mirrors
# `claim_bench_support.rs`): a fresh, uniquely-named database is created,
# migrated, seeded, measured, and then dropped. The role named by the URL
# must be able to `CREATE DATABASE`/`DROP DATABASE`.
#
# Takes roughly 2-3 minutes: seeding ~2.9M event rows dominates the runtime.
set -euo pipefail

ADMIN_URL="${HARVEST_TEST_DATABASE_URL:?set HARVEST_TEST_DATABASE_URL to an admin connection string, e.g. postgres://postgres:postgres@localhost:5432/postgres}"
DB_NAME="harvest_history_bloat_perf_$$_$(date +%s)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MIGRATIONS_DIR="$REPO_ROOT/autumn-harvest/migrations"
OUT_DIR="${1:-$REPO_ROOT/docs/perf-artifacts/history-bloat-filter}"

echo "== creating $DB_NAME =="
psql "$ADMIN_URL" -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${DB_NAME};" >/dev/null

# Admin URL with the database segment swapped for the fresh database.
DB_URL="$(python3 - "$ADMIN_URL" "$DB_NAME" <<'PY'
import sys
from urllib.parse import urlsplit, urlunsplit
url, dbname = sys.argv[1], sys.argv[2]
parts = urlsplit(url)
new = parts._replace(path="/" + dbname)
print(urlunsplit(new))
PY
)"

cleanup() {
  echo "== dropping $DB_NAME =="
  psql "$ADMIN_URL" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE);" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== applying migrations =="
for d in $(ls -d "$MIGRATIONS_DIR"/*/ | sort); do cat "$d/up.sql"; echo; done \
  | psql "$DB_URL" -v ON_ERROR_STOP=1 -q -f - >/dev/null

echo "== seeding fixture (this is the slow part) =="
psql "$DB_URL" -v ON_ERROR_STOP=1 -q << 'SQL'
-- 5,000 ordinary RUNNING executions, 5-80 recorded events each.
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, state, input, queue_name)
SELECT gen_random_uuid(), 'order_flow', 'healthy-' || i, 0, 'RUNNING', '{}'::jsonb, 'default'
FROM generate_series(1, 5000) AS s(i);

-- 15 long-running RUNNING executions, 50k-450k recorded events each --
-- the shape `history_bloat_min_events` (issue #704) exists to surface.
INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, state, input, queue_name)
SELECT gen_random_uuid(), 'poll_loop', 'bloated-' || i, 0, 'RUNNING', '{}'::jsonb, 'default'
FROM generate_series(1, 15) AS s(i);

-- The per-row event count is derived deterministically from `md5(workflow_id)`
-- so re-runs reproduce identical counts. `('x' || hex)::bit(32)::int` gives a
-- SIGNED 32-bit interpretation of the hash bits -- roughly half of all hashes
-- have the high bit set and cast to a NEGATIVE int, which would make `% N`
-- (Postgres preserves the sign of the dividend) negative too, collapsing the
-- lower bound below zero and silently seeding a zero-event row via an empty
-- `generate_series` instead of the intended range. Casting to `bigint` before
-- `abs()` avoids the one remaining edge case (`abs(int4 MIN)` overflows int4;
-- it fits comfortably in bigint), so the modulus operand is always
-- non-negative and the intended [lower, lower+N) range is honored for every
-- hash. See PR #1173 review discussion for the measured impact of the bug
-- this replaced (~46% of rows in both cohorts previously got zero events).
INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT
    e.id,
    ev.i,
    CASE WHEN ev.i % 2 = 0 THEN 'ActivityScheduled' ELSE 'ActivityCompleted' END,
    CASE WHEN ev.i % 2 = 0 THEN
        jsonb_build_object('type','ActivityScheduled','data',jsonb_build_object(
            'activity_id', gen_random_uuid(), 'name','charge_card','input',
            jsonb_build_object('order_id', e.workflow_id, 'amount_cents', 1999, 'items', jsonb_build_array('sku-1','sku-2'))))
    ELSE
        jsonb_build_object('type','ActivityCompleted','data',jsonb_build_object(
            'activity_id', gen_random_uuid(), 'output',
            jsonb_build_object('charge_id','ch_' || ev.i, 'status','ok')))
    END,
    NOW() - ((80 - ev.i) || ' seconds')::interval
FROM harvest_workflow_executions e
CROSS JOIN LATERAL generate_series(1, 5 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 75)) AS ev(i)
WHERE e.workflow_name = 'order_flow';

INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
SELECT
    e.id,
    ev.i,
    CASE WHEN ev.i % 3 = 0 THEN 'TimerFired' ELSE 'MarkerRecorded' END,
    CASE WHEN ev.i % 3 = 0 THEN
        jsonb_build_object('type','TimerFired','data',jsonb_build_object('timer_id','poll-' || ev.i))
    ELSE
        jsonb_build_object('type','MarkerRecorded','data',jsonb_build_object(
            'name','poll_iteration', 'details', jsonb_build_object('iteration', ev.i, 'batch', jsonb_build_array(1,2,3,4,5))))
    END,
    NOW() - (((50000 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 400000)) - ev.i) || ' seconds')::interval
FROM harvest_workflow_executions e
CROSS JOIN LATERAL generate_series(1, 50000 + (abs((('x' || substr(md5(e.workflow_id), 1, 8))::bit(32)::int)::bigint) % 400000)) AS ev(i)
WHERE e.workflow_name = 'poll_loop';

-- VACUUM (not just ANALYZE) sets the visibility map. An un-vacuumed table
-- can make an Index Only Scan's "Heap Fetches" count differ from run to run
-- purely based on autovacuum timing, independent of query shape -- this bit
-- the real-endpoint measurement in PR #1173 review (see
-- docs/performance-history-bloat-filter.md "Verification against the real
-- production entry point" and real_query_probe.rs). VACUUM here so every
-- capture below reads from matched, deterministic visibility-map state.
VACUUM ANALYZE harvest_events;
VACUUM ANALYZE harvest_workflow_executions;
SQL

# Pick one representative execution from each cohort — the bloated one with
# the largest count (the worst case COUNT(*) has to fully materialize) and an
# arbitrary healthy one.
BLOATED_ID="$(psql "$DB_URL" -tAc \
  "SELECT id FROM harvest_workflow_executions e JOIN LATERAL (SELECT COUNT(*) c FROM harvest_events WHERE workflow_exec_id = e.id) c ON true ORDER BY c.c DESC LIMIT 1;")"
HEALTHY_ID="$(psql "$DB_URL" -tAc "SELECT id FROM harvest_workflow_executions WHERE workflow_id = 'healthy-1';")"
BLOATED_COUNT="$(psql "$DB_URL" -tAc "SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}';")"
HEALTHY_COUNT="$(psql "$DB_URL" -tAc "SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '${HEALTHY_ID}';")"

TOTAL_EVENTS="$(psql "$DB_URL" -tAc "SELECT COUNT(*) FROM harvest_events;")"
TOTAL_EXECUTIONS="$(psql "$DB_URL" -tAc "SELECT COUNT(*) FROM harvest_workflow_executions;")"

echo "== bloated execution: ${BLOATED_ID} (${BLOATED_COUNT} events) =="
echo "== healthy execution: ${HEALTHY_ID} (${HEALTHY_COUNT} events) =="
echo "== fixture totals: ${TOTAL_EVENTS} events across ${TOTAL_EXECUTIONS} executions =="

mkdir -p "$OUT_DIR"
{
  echo "bloated_execution_id=${BLOATED_ID}"
  echo "bloated_execution_events=${BLOATED_COUNT}"
  echo "healthy_execution_id=${HEALTHY_ID}"
  echo "healthy_execution_events=${HEALTHY_COUNT}"
  echo "fixture_total_events=${TOTAL_EVENTS}"
  echo "fixture_total_executions=${TOTAL_EXECUTIONS}"
} > "${OUT_DIR}/fixture-summary.txt"

capture() {
  local label="$1" sql="$2"
  echo "-- ${label} --" > "${OUT_DIR}/${label}.explain.txt"
  psql "$DB_URL" -v ON_ERROR_STOP=1 -c "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) ${sql}" \
    >> "${OUT_DIR}/${label}.explain.txt"
  echo "captured ${OUT_DIR}/${label}.explain.txt"
}

THRESHOLD=10000

capture "before-count-star-bloated" \
  "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}') >= ${THRESHOLD} AS result;"
capture "before-count-star-healthy" \
  "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '${HEALTHY_ID}') >= ${THRESHOLD} AS result;"
capture "after-bounded-exists-bloated" \
  "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}' ORDER BY event_id OFFSET $((THRESHOLD - 1)) LIMIT 1) AS result;"
capture "after-bounded-exists-healthy" \
  "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '${HEALTHY_ID}' ORDER BY event_id OFFSET $((THRESHOLD - 1)) LIMIT 1) AS result;"

# Same bounded EXISTS/OFFSET/LIMIT form but WITHOUT the ORDER BY -- captured
# to substantiate the "ORDER BY is load-bearing, not decorative" claim in
# `apply_min_history_events_filter`'s doc comment: without an explicit sort
# target, the planner has no reason to prefer walking the composite index in
# order, and for a high-selectivity `workflow_exec_id` predicate may still
# fall back to a Seq Scan even though the bound is present.
capture "after-bounded-exists-no-order-by-bloated" \
  "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}' OFFSET $((THRESHOLD - 1)) LIMIT 1) AS result;"

# Isolated correlated-SubPlan comparison against the FULL outer table (no
# other endpoint-specific filters/columns) -- substantiates the
# "docs/performance-history-bloat-filter.md#verification-against-the-real-production-entry-point"
# per-scan buffer-count claim. Wrapped in `SELECT count(*) FROM (...) sub` to
# force full materialization of the WHERE-clause predicate across every row
# rather than stopping at the first match. These are the SAME two queries
# that produced the committed
# `correlated-before-count-star.explain.txt`/`correlated-after-bounded-exists.explain.txt`
# artifacts -- capturing them here means a script rerun after a fixture or
# query change regenerates ALL committed evidence from one code path, so a
# stale manually-captured artifact can never sit alongside freshly
# regenerated ones (PR #1173 review, discussion_r3787605008).
capture "correlated-before-count-star" \
  "SELECT count(*) FROM (SELECT id FROM harvest_workflow_executions WHERE (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= ${THRESHOLD}) sub;"
capture "correlated-after-bounded-exists" \
  "SELECT count(*) FROM (SELECT id FROM harvest_workflow_executions WHERE EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id ORDER BY event_id OFFSET $((THRESHOLD - 1)) LIMIT 1)) sub;"

echo "== pg_stat_statements snapshot (requires the extension) =="
psql "$DB_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null 2>&1 || true
psql "$DB_URL" -c "SELECT pg_stat_statements_reset();" >/dev/null 2>&1 || true
psql "$DB_URL" -c "SELECT (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}') >= ${THRESHOLD};" >/dev/null
psql "$DB_URL" -c "SELECT EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = '${BLOATED_ID}' ORDER BY event_id OFFSET $((THRESHOLD - 1)) LIMIT 1);" >/dev/null
psql "$DB_URL" -c "SELECT query, calls, shared_blks_hit + shared_blks_read AS total_buffers, shared_blks_read, temp_blks_written FROM pg_stat_statements WHERE query ILIKE '%harvest_events%' ORDER BY total_buffers DESC;" \
  > "${OUT_DIR}/pg_stat_statements.txt" 2>&1 || echo "pg_stat_statements unavailable (extension not preloaded) — skipped" | tee "${OUT_DIR}/pg_stat_statements.txt"

echo "== done. Artifacts in ${OUT_DIR} =="
