-- Ported by value from claim_bench_support.rs::seed_backlog (PENDING
-- backlog) and claim_budget_tests.rs's hot-contention seed (RUNNING rows
-- spread across the backlog's own keys). Invoke with:
--   psql -v backlog=10000 -v queues=4 -v keys=256 -v running_rows=0 -f seed.sql
TRUNCATE harvest_task_queue;

INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (i % :queues),
       'activity',
       'PENDING',
       0,
       NOW() - INTERVAL '1 second',
       'bench-ck-' || (i % :keys),
       1000000
FROM generate_series(0, :backlog - 1) AS s(i);

-- Hot-contention RUNNING rows, round-robin across the backlog's own distinct
-- concurrency keys -- mirrors claim_budget_tests.rs's HOT_CONTENTION_ROWS
-- seed exactly (same join-on-distinct-keys shape, same round-robin worker
-- spread, same state/worker_id set).
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, worker_id, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (s.i % :queues),
       'activity',
       'RUNNING',
       0,
       NOW() - INTERVAL '1 second',
       'bench-worker-' || (1 + s.i % 63),
       k.concurrency_key,
       1000000
FROM generate_series(0, :running_rows - 1) AS s(i)
JOIN (SELECT DISTINCT concurrency_key, row_number() OVER () - 1 AS rn
      FROM harvest_task_queue WHERE state = 'PENDING') k
  ON k.rn = s.i % :keys
WHERE :running_rows > 0;

ANALYZE harvest_task_queue;
