-- Blind confirmation (docs/rnd/2026-09-07-claim-any-cardinality-blind-
-- confirmation-preregistration.md): a genuinely unseen point between
-- ledger #7's cardinality-1 result and ledger #6's cardinality-4
-- control. Never run before this file's own pre-registration was
-- committed. Same bias, same base predicate, same ORDER BY/LIMIT/lock
-- clause as every other arm in this apparatus.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0', 'bench-q-1'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
ROLLBACK;
