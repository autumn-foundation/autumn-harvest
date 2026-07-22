-- Support keyset pagination on GET /workflows (issue #498).
-- The (created_at DESC, id DESC) composite index lets each per-shard page fetch
-- walk the index in sort order, delivering O(log n) latency regardless of how
-- deep into history the operator has paged.
CREATE INDEX IF NOT EXISTS idx_harvest_we_created_id
    ON harvest_workflow_executions (created_at DESC, id DESC);
