-- Revert: drop chain-timeout columns and index (issue #617).
DROP INDEX IF EXISTS idx_harvest_executions_chain_deadline;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS chain_deadline_at,
    DROP COLUMN IF EXISTS chain_execution_timeout;
