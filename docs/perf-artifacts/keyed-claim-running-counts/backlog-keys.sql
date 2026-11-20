-- The full-scan count over a backlog of many due keys. 8,000 due rows on
-- queue w, each with its own key and cap 3, inside a rolled-back
-- transaction. The pending keys are those of concurrency_pending_keys.
BEGIN;
INSERT INTO harvest_task_queue (queue_name, task_type, input, state, activity_name, concurrency_key, concurrency_cap, scheduled_at)
SELECT 'w', 'activity', '{}'::jsonb, 'PENDING', 'act', 'dk' || g, 3, now() - interval '1 second' FROM generate_series(1, 8000) g;
ANALYZE harvest_task_queue;
SET LOCAL jit = off; SET LOCAL plan_cache_mode = force_generic_plan; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_sort = off;
PREPARE single_pass(text[]) AS
WITH concurrency_pending_keys AS MATERIALIZED ( SELECT DISTINCT concurrency_key, task_type FROM harvest_task_queue WHERE queue_name = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW() AND concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL ),
concurrency_running_counts AS MATERIALIZED ( SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count FROM harvest_task_queue t WHERE t.state = 'RUNNING' AND t.worker_id IS NOT NULL AND t.concurrency_key IN (SELECT concurrency_key FROM concurrency_pending_keys) GROUP BY t.concurrency_key, t.task_type )
SELECT count(*) FROM concurrency_running_counts;
PREPARE per_key(text[]) AS
WITH concurrency_pending_keys AS MATERIALIZED ( SELECT DISTINCT concurrency_key, task_type FROM harvest_task_queue WHERE queue_name = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW() AND concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL ),
concurrency_running_counts AS MATERIALIZED ( SELECT k.concurrency_key, c.task_type, c.running_count FROM (SELECT DISTINCT concurrency_key FROM concurrency_pending_keys) k CROSS JOIN LATERAL ( SELECT t.task_type, COUNT(*) AS running_count FROM harvest_task_queue t WHERE t.concurrency_key = k.concurrency_key AND t.state = 'RUNNING' AND t.worker_id IS NOT NULL GROUP BY t.task_type ) c )
SELECT count(*) FROM concurrency_running_counts;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) EXECUTE single_pass(ARRAY['w']);
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) EXECUTE per_key(ARRAY['w']);
ROLLBACK;
