DROP INDEX IF EXISTS idx_harvest_executions_sla_deadline;
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS sla_breached_at,
    DROP COLUMN IF EXISTS sla_breached,
    DROP COLUMN IF EXISTS sla_deadline_at,
    DROP COLUMN IF EXISTS sla;
