-- Post-review (Codex, round 2) rebuttal check: a reviewer suggested the
-- `Sort` node in forced_index_diagnostic.sql's output is explained by this
-- assay's own `id ASC` tiebreak (absent from `idx_harvest_tq_poll` and
-- from docs/performance.md's #1177 baseline's `ORDER BY`), not by table
-- size or the multi-queue `$1` binding, and that the "unresolved
-- discrepancy" the report flags is therefore not a discrepancy at all.
--
-- This is the same query as forced_index_diagnostic.sql with the `id`
-- tiebreak removed -- i.e. byte-for-byte #1177's own baseline `ORDER BY`
-- (`priority DESC, scheduled_at ASC`, no residual predicate), same
-- enable_seqscan/enable_bitmapscan bias. If the tiebreak were the cause,
-- this query should show a clean Index Scan with no Sort node, matching
-- #1177's own reported result. It does not (see the report): the `Sort`
-- node and `actual rows=10000` both persist identically without the
-- tiebreak, directly refuting that specific explanation. The discrepancy
-- against #1177's baseline stands as unresolved by this assay.
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
