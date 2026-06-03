DROP INDEX IF EXISTS idx_harvest_we_owner_created;
DROP INDEX IF EXISTS idx_harvest_we_severity_created;
DROP INDEX IF EXISTS idx_harvest_dl_owner;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS owner,
    DROP COLUMN IF EXISTS runbook_url,
    DROP COLUMN IF EXISTS severity;

ALTER TABLE harvest_dead_letters
    DROP COLUMN IF EXISTS owner,
    DROP COLUMN IF EXISTS severity;
