-- Revert: drop deadline_at column and its index.
DROP INDEX IF EXISTS idx_harvest_executions_deadline;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS deadline_at;
