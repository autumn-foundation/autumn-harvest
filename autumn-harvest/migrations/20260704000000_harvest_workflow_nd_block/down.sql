DROP INDEX IF EXISTS idx_harvest_executions_nd_blocked;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS nd_blocked_at,
    DROP COLUMN IF EXISTS nd_block_reason,
    DROP COLUMN IF EXISTS nd_block_count;
