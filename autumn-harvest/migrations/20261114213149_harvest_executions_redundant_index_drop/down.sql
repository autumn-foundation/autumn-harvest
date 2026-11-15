-- Revert #2684 for `harvest_workflow_executions`. Restore the four indexes as
-- they were built.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2684 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_we_state
    ON harvest_workflow_executions (state)
    WHERE state = 'RUNNING';

-- lock-safety: allow blocking-index #2684 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_we_shard
    ON harvest_workflow_executions (shard_id);

-- lock-safety: allow blocking-index #2684 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_we_quota_reconcile_candidates
    ON harvest_workflow_executions (id)
    WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');

-- lock-safety: allow blocking-index #2684 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS harvest_wf_exec_detached_running
    ON harvest_workflow_executions (parent_id, parent_close_policy)
    WHERE parent_close_policy IS NOT NULL AND state = 'RUNNING';
