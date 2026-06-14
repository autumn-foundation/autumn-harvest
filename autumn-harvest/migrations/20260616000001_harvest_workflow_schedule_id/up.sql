-- issue #488: last-completion-result carryover for incremental scheduled jobs
--
-- schedule_id: links each workflow execution back to the harvest_schedules row
-- that fired it. NULL for manually-started (non-scheduled) executions.
-- Used by start_or_load_workflow_execution to resolve the most-recent COMPLETED
-- output for the same schedule (shard-local) and freeze it into the WorkflowStarted
-- event at fire time, so replay is deterministic and never re-queries.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS schedule_id UUID NULL;

-- Partial index for the carryover lookup:
--   SELECT output FROM harvest_workflow_executions
--   WHERE schedule_id = $1 AND state = 'COMPLETED' AND completed_at IS NOT NULL
--   ORDER BY completed_at DESC LIMIT 1;
--
-- Also serves the last_error query (adds state IN filter, same sort key).
-- Per-schedule cardinality is small; one index covers both queries. Both queries
-- additionally filter completed_at IS NOT NULL, so excluding still-running rows
-- (completed_at NULL) from the index keeps it compact without losing coverage.
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_schedule_carryover
    ON harvest_workflow_executions (schedule_id, completed_at DESC)
    WHERE schedule_id IS NOT NULL AND completed_at IS NOT NULL;
