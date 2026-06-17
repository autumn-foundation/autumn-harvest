-- Revert bounded schedule run columns (issue #478).
DROP INDEX IF EXISTS idx_harvest_schedules_exhausted;
ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS exhausted_reason,
    DROP COLUMN IF EXISTS exhausted_at,
    DROP COLUMN IF EXISTS runs_started,
    DROP COLUMN IF EXISTS max_runs,
    DROP COLUMN IF EXISTS end_at;
