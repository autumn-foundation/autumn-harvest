-- Revert issue #534: per-schedule run history origin column.
DROP INDEX IF EXISTS idx_harvest_wfx_schedule_runs;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS origin;
