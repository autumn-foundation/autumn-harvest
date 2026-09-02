-- Supporting index for the idle rate-limit-bucket GC (issue #1127).
--
-- The sweep in `queue::sweep_idle_rate_limit_buckets` refuses to collect a
-- bucket that any NON-TERMINAL task still references: both the claim-time gate
-- and `queue::try_consume_rate_limit_token` fail CLOSED on a missing bucket
-- row, and nothing re-registers a bucket for an already-enqueued task, so
-- collecting one would strand that task forever.
--
-- The pre-existing `idx_harvest_task_queue_rate_limit_key` is partial on
-- `state = 'PENDING'`, which cannot serve that anti-join: a RUNNING task (a
-- circuit-breaker activity debits its token at dispatch, not at claim) and a
-- PAUSED one pin their buckets just as hard, and a retry puts a RUNNING task
-- back to PENDING. Without this index the anti-join degenerates to a sequential
-- scan of `harvest_task_queue` per candidate bucket.
--
-- The predicate is written as a NOT IN over the THREE terminal states rather than
-- an IN over the non-terminal ones so that a future state added to
-- `harvest_task_queue` is covered by both the index and the sweep's matching
-- WHERE clause automatically -- failing safe toward RETAINING a bucket.
--
-- Nothing else changes: no column is added, no row is rewritten, and
-- `harvest_events` is not touched.
CREATE INDEX IF NOT EXISTS idx_harvest_task_queue_rate_limit_key_live
    ON harvest_task_queue (rate_limit_key)
    WHERE rate_limit_key IS NOT NULL
      AND state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
