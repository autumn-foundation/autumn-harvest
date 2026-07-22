-- Request-scoped idempotency keys for plain workflow starts (issue #808).
--
-- One row per (workflow_name, idempotency_key). The row is a claim: the first
-- start carrying a given key inserts it (pointing at the freshly-created
-- execution); a second start carrying the SAME key within the retention window
-- observes the existing row and returns the SAME workflow_exec_id as a no-op —
-- no second WorkflowStarted event, no second enqueued task, no 409.
--
-- Dedup scope is (workflow_name, idempotency_key), INDEPENDENT of workflow_id —
-- so it converges even when workflow_id is auto-generated per request. This is a
-- *delivery-identity* dedupe (this exact request) layered in front of the
-- *business-identity* reuse-policy matrix keyed on workflow_id; the idempotency
-- check short-circuits the reuse-policy matrix entirely.
--
-- Retention: after the configured window elapses, the same key is reusable — the
-- ON CONFLICT ... WHERE created_at <= now() - window upsert overwrites the stale
-- row, and a background sweep deletes expired rows so the table can't grow
-- unbounded for keys that are never reused.
--
-- Scoping: shard-local. The claim row and its execution live on the same shard
-- (no cross-shard coordination), consistent with per-key concurrency (#247) and
-- the start throttle (#607). There is deliberately NO foreign key to
-- harvest_workflow_executions: robustness against a claim pointing at a
-- retention-deleted execution is handled by a defensive reclaim path in the
-- reserve function plus the expiry sweep.
CREATE TABLE IF NOT EXISTS harvest_start_idempotency (
    workflow_name    TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    workflow_exec_id UUID        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Shard this claim was routed to (matches the execution's shard). Used by
    -- the per-shard expiry sweep.
    shard_id         INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (workflow_name, idempotency_key)
);

-- Drives the background expiry sweep (DELETE ... WHERE shard_id = $ AND
-- created_at <= now() - window).
CREATE INDEX IF NOT EXISTS idx_harvest_start_idempotency_sweep
    ON harvest_start_idempotency (shard_id, created_at);
