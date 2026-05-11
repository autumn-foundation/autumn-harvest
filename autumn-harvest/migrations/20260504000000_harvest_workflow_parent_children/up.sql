-- Expose parent -> children traversal efficiently across shard-local workflow tables.
--
-- Multi-shard deployments may store a child workflow on a different database
-- than its parent, so parent_id cannot be enforced with a shard-local foreign
-- key. Keep it as an operator-visible relationship and index the nullable FK
-- value for per-shard lookup fan-out.

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_parent_id_fkey;

CREATE INDEX IF NOT EXISTS idx_harvest_we_parent_id
    ON harvest_workflow_executions (parent_id)
    WHERE parent_id IS NOT NULL;
