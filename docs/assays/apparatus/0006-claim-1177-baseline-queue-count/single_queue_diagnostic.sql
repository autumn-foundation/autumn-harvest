-- Ledger #6's own preregistered arm (docs/rnd/2026-09-07-claim-1177-
-- baseline-queue-count-preregistration.md, lines 38-48): this assay's
-- verdict is graded against this file's result, not against
-- single_queue_any_diagnostic.sql's. Scalar equality is not itself a
-- production code path (autumn-harvest/src/queue.rs:641 always binds
-- queue_name = ANY($2), even for one queue) -- that gap is exactly what
-- single_queue_any_diagnostic.sql and ledger #7 exist to test separately;
-- see ledger #6's own report for why that question was spun into its own
-- re-charter rather than graded here. Byte-for-byte
-- forced_index_no_tiebreak_diagnostic.sql from ledger #5, with only the
-- queue predicate changed to scalar equality (queue_name = 'bench-q-0')
-- instead of a 4-queue ANY(ARRAY[...]). Same enable_seqscan/
-- enable_bitmapscan bias, same no-residual-predicate base query, same
-- ORDER BY (no id tiebreak, matching #1177's own).
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
