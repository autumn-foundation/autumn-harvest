-- Revert bounded schedule catchup window columns (issue #484).
DROP INDEX IF EXISTS idx_harvest_schedules_catchup_dropped;

ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS last_catchup_at,
    DROP COLUMN IF EXISTS last_catchup_dropped,
    DROP COLUMN IF EXISTS catchup_window_secs,
    DROP COLUMN IF EXISTS catchup_policy;
