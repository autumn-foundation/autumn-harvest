DROP INDEX IF EXISTS idx_harvest_wfx_start_source;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS started_by;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS start_source_ref;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS start_source;
