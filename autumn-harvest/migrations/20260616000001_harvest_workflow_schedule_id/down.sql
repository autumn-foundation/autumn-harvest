DROP INDEX IF EXISTS idx_harvest_wfx_schedule_carryover;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS scheduled_for;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS schedule_id;
