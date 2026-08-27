-- Per-activity-type pause/resume (issue #807).
--
-- An activity pause is an operator HOLD on dispatch for a named activity TYPE:
-- while a row exists here, `claim_task` skips every PENDING task of that
-- activity, so during a known downstream incident the affected work is held
-- intact rather than failed, retried, or dead-lettered.
--
-- It is the surgical complement to the two brakes either side of it. The queue
-- pause (#619) is the same shape one level up, and is too coarse when many
-- activity types share the `default` queue: holding the queue to stop one
-- broken call also halts email, inventory and notifications. The circuit
-- breaker (#369) shares this granularity but is a reflex, not a control -- it
-- only trips AFTER retryable failures have already burned retry budget and fed
-- the DLQ, and only for activities whose author declared a policy at compile
-- time. This table is the operator's own hand on the same valve: proactive,
-- available for every activity, and holding rather than failing.
--
-- Enforcement is on the claim path, exactly like the queue pause (#619), the
-- PAUSED-execution skip (#383) and the rate-limit gate (#332), rather than an
-- in-process registry like the breaker's. That makes "durable, and applied on
-- every shard and every worker" true BY CONSTRUCTION: there is no boot window,
-- no refresh interval, and no fleet-wide staleness -- a worker that starts
-- mid-incident observes the hold on its very first claim.
--
-- Scope: enforcement is per-database, so a pause row written to one shard's
-- database holds only that shard. A fleet-wide pause writes the row to every
-- shard; `scope_shard_id` records the operator's INTENT for the read API
-- (NULL = fleet-wide), not its coverage -- coverage is derived from which
-- shards actually hold a row.
--
-- This table is pure dispatch metadata: nothing here is ever written to
-- `harvest_events`, so replay of an execution that touched a paused activity is
-- byte-identical to one that never paused.

CREATE TABLE harvest_activity_pauses (
    activity_name   TEXT        PRIMARY KEY,
    reason          TEXT        NOT NULL,
    paused_by       TEXT        NOT NULL DEFAULT 'anonymous',
    paused_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- NULL = fleet-wide (the default); otherwise the single shard the operator
    -- scoped the pause to. Recorded for the read API; enforcement is implied by
    -- which shard database the row lives in.
    scope_shard_id  INTEGER
);

-- Serve the four `activity_name`-leading predicates this feature introduces.
--
-- Unlike the queue-pause sibling (#619), whose every predicate leads with
-- `queue_name` and is served by the existing poll index, these four narrow by
-- ACTIVITY first and carry no queue to seek on:
--
--   * `held_task_count_query`            -- once per pause, per shard
--   * `list_paused_activities_query`     -- a CORRELATED count, once per paused
--                                           activity, on every read of
--                                           `GET /activities`
--   * `resume_shift_scheduled_at_query`  -- a bulk UPDATE that also takes row
--                                           locks for the duration of the scan
--   * `resumed_activity_notify_task_query`
--
-- No pre-existing index leads with `activity_name`, so without this every one
-- of them is a sequential scan of `harvest_task_queue` -- and the correlated
-- count is O(paused_activities x task_queue). That is worst exactly when the
-- feature is used: mid-incident, with the backlog at its largest and an
-- operator refreshing the list view during triage.
--
-- PARTIAL, gated on the same `task_type = 'activity'` scoping every pause
-- predicate carries (see `activity_pause::activity_pause_anti_join` for why
-- that scoping is load-bearing rather than cosmetic). Workflow rows never enter
-- the index, so the hot enqueue path pays nothing for them, and `state` is
-- included because all four queries narrow by it.
--
-- On a live deployment prefer the concurrent form, which cannot run inside
-- Diesel's migration transaction:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_activity_pause
--       ON harvest_task_queue (activity_name, state)
--       WHERE task_type = 'activity' AND activity_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_harvest_tq_activity_pause
    ON harvest_task_queue (activity_name, state)
    WHERE task_type = 'activity' AND activity_name IS NOT NULL;
