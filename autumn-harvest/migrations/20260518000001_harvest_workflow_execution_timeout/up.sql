-- Add deadline_at column for execution-level timeout enforcement (issue #243).
--
-- deadline_at is computed at workflow start time as:
--   started_at + execution_timeout
--
-- NULL means no execution timeout is configured (legacy behaviour preserved).
-- The timeout scanner performs: WHERE state = 'RUNNING' AND deadline_at < NOW()
-- which is an efficient index scan when indexed on (state, deadline_at).

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ NULL;

-- Partial index for efficient timeout scanner queries.
-- Only RUNNING executions with a configured deadline need to be scanned.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_deadline
    ON harvest_workflow_executions (deadline_at)
    WHERE state = 'RUNNING' AND deadline_at IS NOT NULL;

-- Parent-close policy for detached child workflows (issue #347).
-- NULL = awaited child (no cascade). Non-NULL: 'ABANDON', 'REQUEST_CANCEL', 'TERMINATE'.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS parent_close_policy TEXT NULL;
