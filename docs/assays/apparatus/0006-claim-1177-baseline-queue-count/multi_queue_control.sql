-- Control: re-run ledger #5's own forced_index_no_tiebreak_diagnostic.sql
-- verbatim (4-queue ANY binding) against this apparatus's Postgres
-- instance, at the same 10,000-row depth but seeded single-queue-style is
-- not applicable here -- this control reseeds queues=4 (ledger #5's own
-- seed shape) to confirm the discrepancy still reproduces on *this*
-- instance before trusting the single-queue comparison.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, concurrency_key, concurrency_cap, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
ROLLBACK;
