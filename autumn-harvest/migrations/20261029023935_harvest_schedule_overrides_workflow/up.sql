-- Workflow schedule overrides (issue #2386, epic #2384).
--
-- `WorkerBuilder::workflow_schedule` declares a schedule in code. The
-- scheduler converges its row to that code value on every tick, so an edit
-- through `PATCH /schedules/{id}` lasted one tick. This migration lets the
-- override table hold a value for a code-declared workflow schedule too.
--
-- A `workflow` row is keyed by its workflow name. It uses the six shared
-- columns for the fields that a DAG override also sets. `workflow_fields`
-- holds the other fields of the #771 patch as one JSON object:
--
--   * a key that is absent inherits the code value;
--   * a key with a JSON null sets the field to NULL;
--   * a key with a value sets that value.
--
-- A DAG row never uses `workflow_fields`, so the check keeps it empty there.
--
-- The table is small and only the API and the scheduler pass read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_schedule_overrides
    DROP CONSTRAINT harvest_schedule_overrides_kind_check;

ALTER TABLE harvest_schedule_overrides
    ADD CONSTRAINT harvest_schedule_overrides_kind_check
    CHECK (kind IN ('dag', 'workflow'));

ALTER TABLE harvest_schedule_overrides
    ADD COLUMN workflow_fields JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE harvest_schedule_overrides
    ADD CONSTRAINT harvest_schedule_overrides_workflow_fields_check
    CHECK (
        jsonb_typeof(workflow_fields) = 'object'
        AND (kind = 'workflow' OR workflow_fields = '{}'::jsonb)
    );

COMMENT ON COLUMN harvest_schedule_overrides.workflow_fields IS
    'Issue #2386: the workflow-only override fields. An absent key inherits the code value; a JSON null sets NULL.';
