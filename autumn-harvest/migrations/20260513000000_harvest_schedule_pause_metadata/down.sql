ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS paused_at,
    DROP COLUMN IF EXISTS paused_by,
    DROP COLUMN IF EXISTS pause_reason;
