-- Same query as control.sql, no EXPLAIN wrapper, for a same-instrumentation
-- wall-clock comparison against claim_deferred()'s raw \timing numbers
-- (EXPLAIN ANALYZE's own per-node instrumentation overhead would otherwise
-- bias a wall-clock comparison against the EXPLAIN'd side).
WITH concurrency_pending_keys AS MATERIALIZED (
    SELECT DISTINCT concurrency_key, task_type
    FROM harvest_task_queue
    WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
      AND state = 'PENDING'
      AND scheduled_at <= NOW()
      AND concurrency_key IS NOT NULL
      AND concurrency_cap IS NOT NULL
),
concurrency_running_counts AS MATERIALIZED (
    SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
      AND t.concurrency_key IN (SELECT concurrency_key FROM concurrency_pending_keys)
    GROUP BY t.concurrency_key, t.task_type
)
SELECT id, task_type, concurrency_key, concurrency_cap
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
  AND (
      concurrency_key IS NULL
      OR concurrency_cap IS NULL
      OR COALESCE((
          SELECT rc.running_count FROM concurrency_running_counts rc
          WHERE rc.concurrency_key = harvest_task_queue.concurrency_key
            AND rc.task_type = harvest_task_queue.task_type
      ), 0) < harvest_task_queue.concurrency_cap
  )
ORDER BY priority DESC, scheduled_at ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;
