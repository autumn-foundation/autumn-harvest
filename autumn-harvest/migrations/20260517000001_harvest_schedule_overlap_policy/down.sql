-- Revert schedule overlap policy columns (issue #241)

ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS overlap_policy,
    DROP COLUMN IF EXISTS buffered_runs,
    DROP COLUMN IF EXISTS buffer_all_max;
