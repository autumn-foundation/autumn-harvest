-- L4 fixture: 50 highest-priority PENDING rows keyed to already-saturated
-- concurrency keys (RUNNING count == cap for each), one claimable row
-- immediately behind them at the next priority tier, and a normal
-- 10,000-row backlog underneath so the adversarial rows aren't the only
-- thing in the table. Invoke with:
--   psql -v backlog=10000 -v queues=4 -v keys=256 -f seed_adversarial.sql
TRUNCATE harvest_task_queue;

-- Normal backlog, priority 0 (lowest), never reached by the retry loop
-- unless something is broken.
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

-- 50 PENDING rows at priority=100 (above everything else), round-robin
-- across the 10 saturated poison keys -- the retry loop must fail all 50
-- before it can reach a claimable row.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
SELECT 'bench-q-' || (i % :queues),
       'activity',
       'PENDING',
       100,
       NOW() - INTERVAL '2 seconds', -- earlier scheduled_at than the good row, same priority tier
       'bench-poison-key-' || (i % 10),
       2
FROM generate_series(0, 49) AS s(i);

-- The one claimable row, priority=99 -- sorts immediately after the 50
-- poison rows and before the priority=0 backlog.
INSERT INTO harvest_task_queue
    (queue_name, task_type, state, priority, scheduled_at, concurrency_key, concurrency_cap)
VALUES ('bench-q-0', 'activity', 'PENDING', 99, NOW() - INTERVAL '1 second', NULL, NULL);

ANALYZE harvest_task_queue;
