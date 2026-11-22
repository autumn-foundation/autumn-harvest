-- EXPLAIN the terminal-sweep key read on a seeded fixture.
-- (a) an execution that never touched a mutex: the common terminal case.
-- (b) an execution queued on the deepest hot key.
-- (c) the holder of a one-row key.
PREPARE terminal_keys(uuid) AS
SELECT 0 AS part, lock_key FROM harvest_mutex_locks WHERE holder_exec_id = $1
UNION ALL
SELECT DISTINCT 1 AS part, lock_key FROM harvest_mutex_waiters WHERE waiter_exec_id = $1
ORDER BY part, lock_key;
SET plan_cache_mode = force_generic_plan;
\echo '== (a) no-mutex execution'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) EXECUTE terminal_keys('00000000-0000-0000-0000-000000000001');
\echo '== (b) hot-key waiter'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) EXECUTE terminal_keys(md5('hot-7')::uuid);
\echo '== (c) one-row key holder'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) EXECUTE terminal_keys(md5('order-3')::uuid);
