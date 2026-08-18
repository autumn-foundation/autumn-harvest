-- Corroboration script for docs/performance-capability-labels.md's
-- "Corroboration 1" table. Isolates the row-width-growth mechanism directly
-- via pg_relation_size/pg_column_size, independent of the EXPLAIN-based
-- measurement in the .explain.txt files alongside this artifact.
--
-- Run against ANY database that already has autumn-harvest's Diesel
-- migrations applied (any dev DB, or a fresh bench DB produced by
-- claim_bench_support::setup_bench_db()). It TRUNCATEs and reseeds
-- harvest_task_queue three times in place -- do not run this against a
-- database whose harvest_task_queue contents you want to keep.
--
--   psql "$DATABASE_URL" -f docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.sql
--
-- pg_relation_size and pg_column_size are pure catalog/storage reads, not
-- EXPLAIN estimates -- both are admissible evidence under this repo's
-- performance-measurement discipline (docs/performance.md).
\set ON_ERROR_STOP on
\timing off

-- 1. Fresh INSERT, required_capabilities left NULL (today's default).
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'fresh-insert-no-caps' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       round(avg(pg_column_size(harvest_task_queue.*))) AS avg_row_bytes
FROM harvest_task_queue;

-- 2. Fresh INSERT, required_capabilities populated from birth (the fix).
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at, required_capabilities)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second',
       '[{"Exact":{"key":"region","value":"us-east"}}]'::jsonb
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'fresh-insert-with-caps' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       round(avg(pg_column_size(harvest_task_queue.*))) AS avg_row_bytes,
       round(avg(pg_column_size(required_capabilities))) AS avg_caps_col_bytes
FROM harvest_task_queue;

-- 3. Fresh INSERT (no caps) THEN UPDATE to add caps -- the confounded/buggy
--    seeding strategy this pass caught and fixed, reproduced here to confirm
--    the MVCC dead-tuple bloat it documents is real, not misremembered.
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
FROM generate_series(0, 9999) AS s(i);
UPDATE harvest_task_queue
SET required_capabilities = '[{"Exact":{"key":"region","value":"us-east"}}]'::jsonb;
ANALYZE harvest_task_queue;  -- deliberately no VACUUM -- matches production reality: nothing vacuums between a claim-path seed and an EXPLAIN capture in the buggy harness version.
SELECT 'fresh-insert-then-update' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       round(avg(pg_column_size(harvest_task_queue.*))) AS avg_row_bytes
FROM harvest_task_queue;

-- cleanup: leave the scratch DB empty again.
TRUNCATE harvest_task_queue RESTART IDENTITY;
