-- L4 fixture: byte-for-byte ledger #4's seed_adversarial.sql. 50 highest-
-- priority PENDING rows keyed to already-saturated concurrency keys, one
-- claimable row immediately behind them, normal 10,000-row backlog beneath.
-- Invoke with:
--   psql -v backlog=10000 -v queues=4 -v keys=256 -f seed_adversarial_50.sql
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

-- 50 PENDING rows at priority=100, round-robin across the 10 saturated keys.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (i % :queues),
       'activity',
       'PENDING',
       100,
       NOW() - INTERVAL '2 seconds',
       'bench-poison-key-' || (i % 10),
       2
FROM generate_series(0, 49) AS s(i);

-- The one claimable row, priority=99.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
VALUES ('bench-q-0', 'activity', 'PENDING', 99, NOW() - INTERVAL '1 second', NULL, NULL);

ANALYZE harvest_task_queue;
