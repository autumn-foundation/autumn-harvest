ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS owner,
    DROP COLUMN IF EXISTS runbook_url,
    DROP COLUMN IF EXISTS severity;

ALTER TABLE harvest_dead_letters
    DROP COLUMN IF EXISTS owner,
    DROP COLUMN IF EXISTS severity;
