-- Revert issue #2386. A `workflow` override cannot exist under the old check,
-- so the down migration deletes those rows first.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_schedule_overrides WHERE kind = 'workflow';

ALTER TABLE harvest_schedule_overrides
    DROP CONSTRAINT harvest_schedule_overrides_workflow_fields_check;

ALTER TABLE harvest_schedule_overrides
    DROP COLUMN workflow_fields;

ALTER TABLE harvest_schedule_overrides
    DROP CONSTRAINT harvest_schedule_overrides_kind_check;

ALTER TABLE harvest_schedule_overrides
    ADD CONSTRAINT harvest_schedule_overrides_kind_check
    CHECK (kind IN ('dag'));
