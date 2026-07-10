DROP INDEX IF EXISTS idx_harvest_executions_legal_hold;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS legal_hold_set_at,
    DROP COLUMN IF EXISTS legal_hold_until,
    DROP COLUMN IF EXISTS legal_hold_reason,
    DROP COLUMN IF EXISTS legal_hold_actor;
