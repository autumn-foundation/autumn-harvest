-- Revert the input override of a code-declared workflow schedule.
--
-- A row that sets only the input would hold no override after the revert.
-- The revert deletes it, so the listing shows no empty override. Each
-- schedule then takes its code input at the next tick.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_schedule_overrides
WHERE workflow_input IS NOT NULL
  AND schedule_expr IS NULL
  AND catchup IS NULL
  AND max_active_runs IS NULL
  AND jitter_secs IS NULL
  AND overlap_policy IS NULL
  AND buffer_all_max IS NULL
  AND workflow_fields = '{}'::jsonb;

ALTER TABLE harvest_schedule_overrides
    DROP CONSTRAINT harvest_schedule_overrides_workflow_input_check;

ALTER TABLE harvest_schedule_overrides
    DROP COLUMN workflow_input;

ALTER TABLE harvest_schedule_overrides
    DROP CONSTRAINT harvest_schedule_overrides_id_key;

ALTER TABLE harvest_schedule_overrides
    DROP COLUMN id;
