ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS completion_callbacks;

DROP INDEX IF EXISTS idx_harvest_completion_deliveries_exec;
DROP INDEX IF EXISTS idx_harvest_completion_deliveries_due;
DROP TABLE IF EXISTS harvest_completion_deliveries;
