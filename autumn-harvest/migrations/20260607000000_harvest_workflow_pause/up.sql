-- Pause/resume primitive for individual workflow executions (issue #383).
--
-- A paused execution carries a non-terminal 'PAUSED' state. While paused the
-- executor refuses to dispatch new commands: the claim query skips workflow
-- tasks whose execution is PAUSED, so timers/signals/activity-completion wakes
-- are deferred until resume without dropping any work.
--
-- These columns track the audit metadata of the active pause so operators can
-- see who paused an execution and why, and so the bounded-pause auto-resume
-- scanner can find pauses that have exceeded `max_workflow_pause_duration`.
--
-- paused_at  : wall-clock instant the pause was applied (NULL when not paused).
-- pause_reason: optional operator-supplied reason (max 500 chars, enforced at
--               the API boundary).
-- pause_actor : identity that requested the pause, from the audit trail.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS pause_reason TEXT NULL,
    ADD COLUMN IF NOT EXISTS pause_actor TEXT NULL;

-- Partial index for the bounded-pause auto-resume scanner. Only PAUSED
-- executions need to be scanned for expiry, so this stays small.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_paused
    ON harvest_workflow_executions (paused_at)
    WHERE state = 'PAUSED' AND paused_at IS NOT NULL;
