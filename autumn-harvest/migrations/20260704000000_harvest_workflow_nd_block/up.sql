-- Non-terminal replay-non-determinism block marker (issue #603).
--
-- When the replay engine detects a divergence (HarvestError::NonDeterministic)
-- on an in-flight execution, the worker no longer writes a terminal
-- WorkflowFailed event: the execution stays RUNNING, the workflow task is
-- re-pended with a capped-exponential backoff (5s * 2^count, capped at 300s),
-- and these columns record the blocked observation so operators can discover
-- the affected cohort (GET /workflows?nd_blocked=true) and read the divergence
-- diagnostic (expected/actual/event_index/workflow_type/build_id are stamped
-- into search_attrs, mirroring the pre-#603 terminal path).
--
-- Properties:
--   - nd_blocked_at: wall-clock of the MOST RECENT divergence observation
--     (updated on every re-block, so it answers "is it still diverging");
--     NULL = not blocked.
--   - nd_block_reason: the divergence error string from the blocked cycle.
--   - nd_block_count: consecutive block entries for the current incident;
--     drives the re-dispatch backoff; reset to 0 on the next clean cycle.
--   - All three are cleared when a later dispatch replays cleanly (rollback
--     recovery) — no operator action needed.
--   - state is NOT changed: a blocked row stays RUNNING and re-claimable, so
--     cancel/terminate/pause and the execution-timeout scanner all compose
--     unchanged.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS nd_blocked_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS nd_block_reason TEXT NULL,
    ADD COLUMN IF NOT EXISTS nd_block_count INTEGER NOT NULL DEFAULT 0;

-- Discovery index for GET /workflows?nd_blocked=true. Blocked rows are rare
-- (only during a divergent deploy incident), so the partial index stays tiny.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_nd_blocked
    ON harvest_workflow_executions (nd_blocked_at)
    WHERE nd_blocked_at IS NOT NULL;
