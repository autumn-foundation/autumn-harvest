-- Revert: drop per-tenant resource quota columns and indexes (issue #946).
DROP INDEX IF EXISTS idx_harvest_dl_quota;

ALTER TABLE harvest_dead_letters
    DROP COLUMN IF EXISTS quota_key;

ALTER TABLE harvest_dead_letters
    DROP COLUMN IF EXISTS workflow_name;

DROP INDEX IF EXISTS idx_harvest_we_quota_active;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS quota_key;
