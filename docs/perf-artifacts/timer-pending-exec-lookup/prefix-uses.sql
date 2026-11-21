-- The other readers of the old index: cascade delete and the rebalance copy.
\pset pager off
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) DELETE FROM harvest_timers WHERE workflow_exec_id = '00000000-0000-4000-8000-000000000003';
EXPLAIN (ANALYZE, BUFFERS) DELETE FROM harvest_workflow_executions WHERE id = '00000000-0000-4000-8000-000000000004';
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM harvest_timers WHERE workflow_exec_id = ANY (ARRAY['00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-0000000001f6']::uuid[]);
ROLLBACK;
