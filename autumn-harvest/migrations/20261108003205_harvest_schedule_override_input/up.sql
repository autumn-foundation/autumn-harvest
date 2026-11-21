-- The input override of a code-declared workflow schedule.
--
-- The input of a scheduled run is a payload. This column holds the operator
-- input in the stored form of `harvest_schedules.workflow_input`: written
-- through the payload codecs. A NULL inherits the code input.
--
-- The codec rotation sweep pages by a UUID key and matches it in its
-- compare-and-swap. The primary key is `(kind, name)`, so `id` gives the
-- sweep its key. The volatile default rewrites the table. The table holds one
-- row per overridden schedule, so the rewrite is short.
--
-- A DAG override has no input. A DAG run takes its `run_conf`, so the check
-- keeps the column NULL on a DAG row. A reshard carry then never moves an
-- input.
--
-- The table is small. The API, the scheduler pass, the reshard carry and the
-- sweep use it. The lock timeout still bounds the wait for its exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_schedule_overrides
    ADD COLUMN id UUID NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE harvest_schedule_overrides
    ADD CONSTRAINT harvest_schedule_overrides_id_key UNIQUE (id);

ALTER TABLE harvest_schedule_overrides
    ADD COLUMN workflow_input JSONB;

ALTER TABLE harvest_schedule_overrides
    ADD CONSTRAINT harvest_schedule_overrides_workflow_input_check
    CHECK (kind = 'workflow' OR workflow_input IS NULL);

COMMENT ON COLUMN harvest_schedule_overrides.id IS
    'The key of the codec rotation sweep. The primary key stays (kind, name).';

COMMENT ON COLUMN harvest_schedule_overrides.workflow_input IS
    'The override input of a code-declared workflow schedule, in the payload codec form. NULL inherits the code input.';
