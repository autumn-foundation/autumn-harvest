-- Post-review (Codex) diagnostic: batch_claim.sql's `candidates` CTE plans
-- as a Seq Scan of the whole 10,000-row backlog + a top-N Sort at this
-- apparatus's fixture depth, not an index-ordered seek through
-- idx_harvest_tq_poll -- so neither the report's "via idx_harvest_tq_poll"
-- framing nor a claim that batching bounds cost independent of backlog
-- depth is actually established by the natural-planner numbers. This
-- mirrors docs/performance.md's own diagnostic technique for the sticky
-- predicate (session-local enable_seqscan/enable_bitmapscan OFF, inside a
-- rolled-back transaction, to bias -- not force -- the planner toward the
-- index) to answer a narrower question: is an index-ordered plan even
-- reachable for this exact query shape, and what does it cost if so.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT id, task_type, concurrency_key, concurrency_cap, priority, scheduled_at
FROM harvest_task_queue
WHERE queue_name = ANY(ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'])
  AND state = 'PENDING'
  AND scheduled_at <= NOW()
ORDER BY priority DESC, scheduled_at ASC, id ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
ROLLBACK;
