-- The depth counts with an empty visibility map (issue #2356).
--
-- An enqueue burst leaves new heap pages that no vacuum has marked
-- all-visible. An index-only scan then visits the heap for each row. This
-- script measures four counts. They are the exact count, a walk as the
-- planner picks it, a walk with sorts off, and the shipped query of
-- queue::bounded_queue_depths with sorts off. The shipped query picks the
-- walk or one pass from the visibility map share. Each runs twice, so the
-- second run reads a warm cache. The script runs three heap orders of the
-- same 300,000 eligible rows in 20 queues with a cubic skew, plus 300,000
-- terminal rows:
--
--   fifo      scheduled_at follows the insert order, as for new tasks.
--   mixed     one row in five has a random scheduled_at, as for retries.
--   random    every scheduled_at is random.
--
-- Run it on a scratch database that holds the harvest schema, for example
-- one that autumn_harvest::test_init_sql() migrated:
--
--   psql -X -d scratch -f docs/perf-artifacts/queue-depth-gauge/empty-visibility-map.sql
\set ON_ERROR_STOP on
\pset pager off
ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false);

\set queues '''{q0,q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19}'''
\set exact 'SELECT queue_name, COUNT(*)::BIGINT AS depth FROM harvest_task_queue WHERE queue_name = ANY(' :queues ') AND state = ''PENDING'' AND scheduled_at <= NOW() GROUP BY queue_name'
\set bounded 'SELECT q.queue_name, c.depth FROM (SELECT DISTINCT queue_name FROM unnest(' :queues '::text[]) AS u(queue_name)) q CROSS JOIN LATERAL (SELECT COUNT(*)::BIGINT AS depth FROM (SELECT 1 FROM harvest_task_queue t WHERE t.queue_name = q.queue_name AND t.state = ''PENDING'' AND t.scheduled_at <= NOW() ORDER BY t.scheduled_at, t.id LIMIT 10000) bounded) c WHERE c.depth > 0'

\set shipped 'WITH queues AS (SELECT DISTINCT queue_name FROM unnest(' :queues '::text[]) AS u(queue_name)), walk AS (SELECT COALESCE((SELECT (1 - c.relallvisible::float8 / GREATEST(c.relpages, 1)) * (SELECT count(*) FROM queues) * 10000 < c.relpages FROM pg_class c WHERE c.oid = ''harvest_task_queue''::regclass), true) AS chosen) SELECT q.queue_name, b.depth FROM queues q CROSS JOIN LATERAL (SELECT COUNT(*)::BIGINT AS depth FROM (SELECT 1 FROM harvest_task_queue t WHERE t.queue_name = q.queue_name AND t.state = ''PENDING'' AND t.scheduled_at <= NOW() ORDER BY t.scheduled_at, t.id LIMIT 10000) bounded) b WHERE b.depth > 0 AND (SELECT chosen FROM walk) UNION ALL SELECT t.queue_name, LEAST(COUNT(*), 10000)::BIGINT AS depth FROM harvest_task_queue t WHERE t.queue_name = ANY(' :queues ') AND t.state = ''PENDING'' AND t.scheduled_at <= NOW() AND NOT (SELECT chosen FROM walk) GROUP BY t.queue_name'

-- seed: fills the table in the order that :order names, then analyzes it.
\set seed 'TRUNCATE harvest_task_queue; SELECT setseed(0.42); INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at, activity_name) SELECT ''q'' || floor(20 * power(random(), 3))::int, ''activity'', ''{}''::jsonb, ''PENDING'', CASE WHEN g % 20 = 0 THEN now() + interval ''1 hour'' WHEN :order = ''random'' OR (:order = ''mixed'' AND random() < 0.2) THEN now() - random() * interval ''6 hours'' ELSE now() - (300000 - g) * interval ''70 milliseconds'' END, ''act'' || (g % 60) FROM generate_series(1, 300000) g; INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, completed_at) SELECT ''q'' || floor(20 * power(random(), 3))::int, ''activity'', ''{}''::jsonb, ''COMPLETED'', ''act'' || (g % 60), now() FROM generate_series(1, 300000) g; ANALYZE harvest_task_queue;'

-- measure: each query twice, so the second run reads a warm cache.
\set measure 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :exact '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :exact '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :bounded '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :bounded '; BEGIN; SET LOCAL enable_sort = off; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :bounded '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :bounded '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :shipped '; EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) ' :shipped '; COMMIT;'

\echo '=== fifo: exact, walk as planned, walk with sorts off, shipped query (each twice)'
\set order '''fifo'''
:seed
:measure

\echo '=== mixed: exact, walk as planned, walk with sorts off, shipped query (each twice)'
\set order '''mixed'''
:seed
:measure

\echo '=== random: exact, walk as planned, walk with sorts off, shipped query (each twice)'
\set order '''random'''
:seed
:measure

-- After a vacuum the map covers the table, and the shipped query walks.
\echo '=== mixed, vacuumed: exact, shipped query (each twice)'
\set order '''mixed'''
:seed
VACUUM ANALYZE harvest_task_queue;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) :exact;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) :exact;
BEGIN;
SET LOCAL enable_sort = off;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) :shipped;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) :shipped;
COMMIT;

TRUNCATE harvest_task_queue;
ALTER TABLE harvest_task_queue RESET (autovacuum_enabled);
