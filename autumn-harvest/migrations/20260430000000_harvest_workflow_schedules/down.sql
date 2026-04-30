-- Reverse the workflow-schedule migration.
-- Rows where workflow_name IS NOT NULL have dag_name = NULL, which would
-- violate the restored NOT NULL constraint; delete them first.
DELETE FROM harvest_schedules WHERE workflow_name IS NOT NULL;
ALTER TABLE harvest_schedules DROP CONSTRAINT IF EXISTS harvest_schedules_workflow_name_unique;
ALTER TABLE harvest_schedules DROP CONSTRAINT IF EXISTS harvest_schedules_kind_check;
ALTER TABLE harvest_schedules DROP COLUMN IF EXISTS queue_name;
ALTER TABLE harvest_schedules DROP COLUMN IF EXISTS workflow_input;
ALTER TABLE harvest_schedules DROP COLUMN IF EXISTS workflow_name;
ALTER TABLE harvest_schedules ALTER COLUMN dag_name SET NOT NULL;
