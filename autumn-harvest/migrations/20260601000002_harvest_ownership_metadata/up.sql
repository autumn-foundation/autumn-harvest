ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS runbook_url TEXT,
    ADD COLUMN IF NOT EXISTS severity TEXT;

ALTER TABLE harvest_dead_letters
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS severity TEXT;
