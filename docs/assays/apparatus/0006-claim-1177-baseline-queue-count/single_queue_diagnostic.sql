-- Secondary arm, not itself production-representative (post-review,
-- Codex, P1): the production claim path (autumn-harvest/src/queue.rs:641)
-- always binds queue_name = ANY($2), so a real single-queue worker never
-- takes a scalar-equality path. This file is kept only as a sanity check
-- that scalar equality behaves the same as single-element ANY() below --
-- see single_queue_any_diagnostic.sql for the arm this assay's verdict
-- actually rests on. Byte-for-byte forced_index_no_tiebreak_diagnostic.sql
-- from ledger #5, with only the queue predicate changed to scalar
-- equality (queue_name = 'bench-q-0') instead of a 4-queue
-- ANY(ARRAY[...]). Same enable_seqscan/enable_bitmapscan bias, same
-- no-residual-predicate base query, same ORDER BY (no id tiebreak,
-- matching #1177's own).
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
