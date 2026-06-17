ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS auto_paused_at,
    DROP COLUMN IF EXISTS consecutive_failure_count,
    DROP COLUMN IF EXISTS consecutive_failure_limit;
