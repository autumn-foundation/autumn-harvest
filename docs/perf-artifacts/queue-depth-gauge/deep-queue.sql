-- One deep queue: the exact depth count against the bounded count (issue #2356).
--
-- The table holds the columns that the two queries and the three pending
-- indexes read. The index definitions are the ones of the migrations. One
-- queue holds 1,000,000 eligible rows and 1,000,000 terminal rows. Run it on a
-- scratch database:
--
--   psql -d scratch -f docs/perf-artifacts/queue-depth-gauge/deep-queue.sql
CREATE TABLE harvest_task_queue (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name text NOT NULL,
    task_type text NOT NULL DEFAULT 'activity',
    state text NOT NULL,
    priority int NOT NULL DEFAULT 0,
    scheduled_at timestamptz NOT NULL,
    new_start bool NOT NULL DEFAULT false,
    attempt int NOT NULL DEFAULT 0
);
CREATE INDEX idx_harvest_tq_poll ON harvest_task_queue
    (queue_name, state, priority DESC, scheduled_at) WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_coverage_sample ON harvest_task_queue
    (queue_name, scheduled_at, id) WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_claim_seek ON harvest_task_queue
    (queue_name, task_type, (new_start AND attempt = 0), priority DESC, scheduled_at)
    WHERE state = 'PENDING';
INSERT INTO harvest_task_queue (queue_name, state, scheduled_at)
SELECT 'default',
       CASE WHEN g % 2 = 0 THEN 'PENDING' ELSE 'COMPLETED' END,
       now() - g * interval '1 millisecond'
FROM generate_series(1, 2000000) g;
VACUUM ANALYZE harvest_task_queue;

-- The exact count of queue::queue_depths.
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)
SELECT queue_name, COUNT(*)::BIGINT AS depth
FROM harvest_task_queue
WHERE queue_name = ANY('{default}') AND state = 'PENDING' AND scheduled_at <= NOW()
GROUP BY queue_name;

-- The shipped query of queue::bounded_queue_depths, at the gauge cap, with
-- sorts off. After the vacuum it walks the index.
BEGIN;
SET LOCAL enable_sort = off;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)
WITH queues AS (
    SELECT DISTINCT queue_name FROM unnest('{default}'::text[]) AS u(queue_name)
),
walk AS (
    SELECT COALESCE((
        SELECT (1 - c.relallvisible::float8 / GREATEST(c.relpages, 1))
               * (SELECT count(*) FROM queues) * 10000 < c.relpages
        FROM pg_class c
        WHERE c.oid = 'harvest_task_queue'::regclass
    ), true) AS chosen
)
SELECT q.queue_name, b.depth
FROM queues q
CROSS JOIN LATERAL (
    SELECT COUNT(*)::BIGINT AS depth
    FROM (
        SELECT 1
        FROM harvest_task_queue t
        WHERE t.queue_name = q.queue_name
          AND t.state = 'PENDING'
          AND t.scheduled_at <= NOW()
        ORDER BY t.scheduled_at, t.id
        LIMIT 10000
    ) bounded
) b
WHERE b.depth > 0 AND (SELECT chosen FROM walk)
UNION ALL
SELECT t.queue_name, LEAST(COUNT(*), 10000)::BIGINT AS depth
FROM harvest_task_queue t
WHERE t.queue_name = ANY('{default}')
  AND t.state = 'PENDING'
  AND t.scheduled_at <= NOW()
  AND NOT (SELECT chosen FROM walk)
GROUP BY t.queue_name;
COMMIT;
