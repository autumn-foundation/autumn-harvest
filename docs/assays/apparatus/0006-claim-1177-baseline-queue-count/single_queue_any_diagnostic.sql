-- Post-review (Codex, P1): the production claim path
-- (autumn-harvest/src/queue.rs:641) always binds queue_name = ANY($2),
-- even for a worker polling exactly one queue -- there is no scalar-
-- equality code path. single_queue_diagnostic.sql's scalar-equality test
-- therefore does not establish what a real single-queue deployment's plan
-- looks like; it only shows scalar equality is cheap, which was never in
-- question. This file is the corrected, production-representative test:
-- ANY() over a single-element array, same queue predicate shape as every
-- other arm in this apparatus, varying only cardinality (1 element here
-- vs. 4 in multi_queue_control.sql).
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
ROLLBACK;
