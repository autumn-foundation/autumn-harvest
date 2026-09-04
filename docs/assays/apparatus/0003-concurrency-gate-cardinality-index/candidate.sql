-- Candidate: correlated subquery against the base table, in the same shape
-- as the `claimed` CTE's authoritative recheck, backed by
-- idx_harvest_tq_concurrency_running (see candidate_index.sql).
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, concurrency_key, concurrency_cap
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
  AND (
      concurrency_key IS NULL
      OR concurrency_cap IS NULL
      OR (
          SELECT COUNT(*) FROM harvest_task_queue recheck
          WHERE recheck.concurrency_key = harvest_task_queue.concurrency_key
            AND recheck.task_type = harvest_task_queue.task_type
            AND recheck.state = 'RUNNING'
            AND recheck.worker_id IS NOT NULL
      ) < harvest_task_queue.concurrency_cap
  )
ORDER BY priority DESC, scheduled_at ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;
