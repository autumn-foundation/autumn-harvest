-- Candidate's own selection query, isolated: no concurrency predicate at
-- all -- the whole premise of this shape. Same queue_name/state/scheduled_at
-- filter and ORDER BY/LIMIT as control's candidate selection, minus the
-- concurrency CTEs and the correlated subquery against them.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, concurrency_key, concurrency_cap
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC
LIMIT 1 FOR UPDATE SKIP LOCKED;
