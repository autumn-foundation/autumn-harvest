-- Task-queue pause/resume (issue #619).
--
-- A queue pause is an operator HOLD on dispatch for a named task queue: while
-- a row exists here, `claim_task` skips every PENDING task on that queue, so
-- during a scoped downstream outage the affected work is held intact rather
-- than failed, retried, or dead-lettered. It is the deliberate opposite of the
-- circuit breaker (#369, fast-fail) and complements the admission gate (#377,
-- which halts new workflow *starts* but lets in-flight runs keep scheduling).
--
-- Enforcement is a `NOT EXISTS` anti-join on the claim path — exactly like the
-- PAUSED-execution skip (#383) and the rate-limit gate (#332) — rather than an
-- in-process cache. That makes "durable and re-applied before the worker pool
-- begins claiming" true BY CONSTRUCTION: there is no boot window, no refresh
-- interval, and no fleet-wide staleness. The table is bounded by the number of
-- distinct queue names, so the anti-join is a primary-key probe.
--
-- Scope: enforcement is per-database, so a pause row written to one shard's
-- database holds only that shard. A fleet-wide pause writes the row to every
-- shard; `scope_shard_id` records the operator's intent for the read API
-- (NULL = fleet-wide).
--
-- This table is pure queue metadata: nothing here is ever written to
-- `harvest_events`, so replay of an execution that touched a paused queue is
-- byte-identical to one that never paused.

CREATE TABLE harvest_queue_pauses (
    queue_name      TEXT        PRIMARY KEY,
    reason          TEXT        NOT NULL,
    paused_by       TEXT        NOT NULL DEFAULT 'anonymous',
    paused_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- NULL = fleet-wide (the default); otherwise the single shard the operator
    -- scoped the pause to. Recorded for the read API; enforcement is implied by
    -- which shard database the row lives in.
    scope_shard_id  INTEGER
);
