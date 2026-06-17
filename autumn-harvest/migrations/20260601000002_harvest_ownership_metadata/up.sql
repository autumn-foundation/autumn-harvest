ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS runbook_url TEXT,
    ADD COLUMN IF NOT EXISTS severity TEXT;

ALTER TABLE harvest_dead_letters
    ADD COLUMN IF NOT EXISTS owner TEXT,
    ADD COLUMN IF NOT EXISTS severity TEXT;

CREATE INDEX IF NOT EXISTS idx_harvest_we_owner_created ON harvest_workflow_executions (owner, created_at DESC) WHERE owner IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_harvest_we_severity_created ON harvest_workflow_executions (severity, created_at DESC) WHERE severity IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_harvest_dl_owner ON harvest_dead_letters (owner) WHERE owner IS NOT NULL;
