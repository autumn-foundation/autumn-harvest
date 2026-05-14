-- Allow unified DAG schedule rows to carry both the DAG marker and the
-- workflow schedule name. Classic DAG rows still use only dag_name, and
-- workflow-only rows still use only workflow_name.
ALTER TABLE harvest_schedules
    DROP CONSTRAINT IF EXISTS harvest_schedules_kind_check;

ALTER TABLE harvest_schedules
    ADD CONSTRAINT harvest_schedules_kind_check
    CHECK (
        dag_name IS NOT NULL OR
        workflow_name IS NOT NULL
    ) NOT VALID;

ALTER TABLE harvest_schedules
    VALIDATE CONSTRAINT harvest_schedules_kind_check;
