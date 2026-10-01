-- Revert: restore the id-only candidate index (issue #1631).
CREATE INDEX IF NOT EXISTS idx_harvest_we_quota_reconcile_candidates
    ON harvest_workflow_executions (id)
    WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED');

DROP INDEX IF EXISTS idx_harvest_we_quota_reconcile_name_id;
