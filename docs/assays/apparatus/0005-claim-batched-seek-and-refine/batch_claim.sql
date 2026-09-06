-- Candidate's single-round-trip batch query, isolated for EXPLAIN at the
-- non-adversarial scenarios (first batch only, no keyset cursor needed).
-- The recheck CTE is scoped to the batch's own distinct concurrency keys,
-- not the backlog's global key cardinality -- the specific mechanism this
-- assay is testing against ledger #3's failure mode.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
WITH candidates AS (
    SELECT id, task_type, concurrency_key, concurrency_cap, priority, scheduled_at
    FROM harvest_task_queue
    WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
      AND state = 'PENDING'
      AND scheduled_at <= NOW()
    ORDER BY priority DESC, scheduled_at ASC
    LIMIT :batch_size
    FOR UPDATE SKIP LOCKED
),
batch_keys AS MATERIALIZED (
    SELECT DISTINCT concurrency_key, task_type
    FROM candidates
    WHERE concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL
),
running_counts AS MATERIALIZED (
    SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
      AND (t.concurrency_key, t.task_type) IN (SELECT concurrency_key, task_type FROM batch_keys)
    GROUP BY t.concurrency_key, t.task_type
)
SELECT c.id, c.priority, c.scheduled_at
FROM candidates c
WHERE c.concurrency_key IS NULL
   OR c.concurrency_cap IS NULL
   OR COALESCE((
        SELECT rc.running_count FROM running_counts rc
        WHERE rc.concurrency_key = c.concurrency_key AND rc.task_type = c.task_type
      ), 0) < c.concurrency_cap
ORDER BY c.priority DESC, c.scheduled_at ASC
LIMIT 1;
