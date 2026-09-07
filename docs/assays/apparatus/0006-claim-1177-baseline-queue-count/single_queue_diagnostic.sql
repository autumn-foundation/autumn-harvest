-- Byte-for-byte forced_index_no_tiebreak_diagnostic.sql from ledger #5,
-- with the only change being the queue predicate: a single-queue scalar
-- equality (queue_name = 'bench-q-0') instead of a 4-queue ANY(ARRAY[...]).
-- Same enable_seqscan/enable_bitmapscan bias, same no-residual-predicate
-- base query, same ORDER BY (no id tiebreak, matching #1177's own).
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = 'bench-q-0'
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
ROLLBACK;
