DROP INDEX IF EXISTS idx_harvest_ext_activity_name;
DROP INDEX IF EXISTS idx_harvest_ext_workflow_exec;
DROP INDEX IF EXISTS idx_harvest_ext_state_updated;

ALTER TABLE harvest_external_tasks
    DROP COLUMN IF EXISTS updated_at;
