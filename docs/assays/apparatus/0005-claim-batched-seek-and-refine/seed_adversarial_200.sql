-- L5 fixture: same construction as seed_adversarial_50.sql at 4x poison
-- depth -- 200 highest-priority PENDING rows round-robined across the same
-- 10 saturated concurrency keys, one claimable row immediately behind them.
-- With B=50 this forces exactly ceil(200/50) = 4 batches to resolve.
-- Invoke with:
--   psql -v backlog=10000 -v queues=4 -v keys=256 -f seed_adversarial_200.sql
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

-- 10 poison keys, cap=2 each, already saturated with exactly 2 RUNNING rows.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, worker_id, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (p.k % :queues),
       'activity',
       'RUNNING',
       0,
       NOW() - INTERVAL '1 second',
       'bench-worker-poison-' || r.rn,
       'bench-poison-key-' || p.k,
       2
FROM generate_series(0, 9) AS p(k)
CROSS JOIN generate_series(0, 1) AS r(rn);

-- 200 PENDING rows at priority=100, round-robin across the 10 saturated
-- keys, ordered by scheduled_at ascending (earliest first) so the batch
-- walk visits them in a fixed, reproducible order.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (i % :queues),
       'activity',
       'PENDING',
       100,
       NOW() - INTERVAL '2 seconds' - (200 - i) * INTERVAL '1 millisecond',
       'bench-poison-key-' || (i % 10),
       2
FROM generate_series(0, 199) AS s(i);

-- The one claimable row, priority=99.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
VALUES ('bench-q-0', 'activity', 'PENDING', 99, NOW() - INTERVAL '1 second', NULL, NULL);

ANALYZE harvest_task_queue;
