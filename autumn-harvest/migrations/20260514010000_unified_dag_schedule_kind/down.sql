-- Restore the pre-unified invariant for downgrade targets that do not know how
-- to interpret rows carrying both dag_name and workflow_name.
ALTER TABLE harvest_schedules
    DROP CONSTRAINT IF EXISTS harvest_schedules_kind_check;

UPDATE harvest_schedules
SET dag_name = NULL
WHERE dag_name IS NOT NULL
  AND workflow_name IS NOT NULL;

ALTER TABLE harvest_schedules
    ADD CONSTRAINT harvest_schedules_kind_check
    CHECK (
        (dag_name IS NOT NULL AND workflow_name IS NULL) OR
        (dag_name IS NULL    AND workflow_name IS NOT NULL)
    ) NOT VALID;

ALTER TABLE harvest_schedules
    VALIDATE CONSTRAINT harvest_schedules_kind_check;
