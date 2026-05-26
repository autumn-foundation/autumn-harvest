-- Revert: drop deadline_at and parent_close_policy columns.
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS parent_close_policy;
DROP INDEX IF EXISTS idx_harvest_executions_deadline;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS deadline_at;
