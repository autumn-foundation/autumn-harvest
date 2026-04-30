-- Migration: per-workflow cron schedules (issue #91)
-- Strictly additive: existing DAG schedule rows are unaffected.

-- Allow dag_name to be NULL so workflow-only rows can omit it.
ALTER TABLE harvest_schedules ALTER COLUMN dag_name DROP NOT NULL;

-- New columns for workflow-only schedules.
ALTER TABLE harvest_schedules ADD COLUMN workflow_name  TEXT;
ALTER TABLE harvest_schedules ADD COLUMN workflow_input JSONB;

-- Exactly one of dag_name or workflow_name must be non-NULL per row.
-- NOT VALID avoids a full-table scan at migration time; VALIDATE CONSTRAINT
-- serialises against concurrent writes on just the new rows, not the whole table.
ALTER TABLE harvest_schedules
    ADD CONSTRAINT harvest_schedules_kind_check
    CHECK (
        (dag_name IS NOT NULL AND workflow_name IS NULL) OR
        (dag_name IS NULL    AND workflow_name IS NOT NULL)
    ) NOT VALID;

ALTER TABLE harvest_schedules
    VALIDATE CONSTRAINT harvest_schedules_kind_check;

-- Index for efficient scheduler tick queries on workflow schedules.
CREATE INDEX idx_harvest_schedules_workflow_name
    ON harvest_schedules (workflow_name)
    WHERE workflow_name IS NOT NULL;
