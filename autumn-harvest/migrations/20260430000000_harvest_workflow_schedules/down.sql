-- Reverse the workflow-schedule migration.
DROP INDEX IF EXISTS idx_harvest_schedules_workflow_name;
ALTER TABLE harvest_schedules DROP CONSTRAINT IF EXISTS harvest_schedules_kind_check;
ALTER TABLE harvest_schedules DROP COLUMN IF EXISTS workflow_input;
ALTER TABLE harvest_schedules DROP COLUMN IF EXISTS workflow_name;
ALTER TABLE harvest_schedules ALTER COLUMN dag_name SET NOT NULL;
