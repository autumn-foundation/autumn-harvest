-- Revert the state CHECK constraint to the pre-PAUSED set (matching
-- 20260503000000_harvest_workflow_reset). Assumes no PAUSED rows remain.
ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_state_check;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_state_check
        CHECK (state IN (
            'RUNNING',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW',
            'TERMINATED'
        ));

-- Revert: drop pause metadata columns and the auto-resume scanner index.
DROP INDEX IF EXISTS idx_harvest_executions_paused;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS paused_at,
    DROP COLUMN IF EXISTS pause_reason,
    DROP COLUMN IF EXISTS pause_actor;
