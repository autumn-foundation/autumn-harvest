-- Revert: drop pause metadata columns and the auto-resume scanner index.
DROP INDEX IF EXISTS idx_harvest_executions_paused;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS paused_at,
    DROP COLUMN IF EXISTS pause_reason,
    DROP COLUMN IF EXISTS pause_actor;
