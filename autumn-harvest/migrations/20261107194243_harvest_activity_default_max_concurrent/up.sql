-- Override the max_concurrent cap of an activity type at runtime.
--
-- `ActivityInfo::max_concurrent` caps the running tasks of one concurrency
-- key across the fleet. This column holds the cap that an operator sets at
-- runtime. A NULL inherits the code value, as for the other columns. The key
-- stays deploy-only.
--
-- Each task row stores its cap. A change of the override rewrites the cap of
-- the live rows of the activity, so a pending task takes the new cap too.
-- The row check now accepts a row that sets the cap alone.
--
-- The table is small and only the API and the cache refresh read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_activity_default_overrides
    ADD COLUMN max_concurrent INTEGER
    CONSTRAINT harvest_activity_default_overrides_max_concurrent_check
    CHECK (max_concurrent > 0);

ALTER TABLE harvest_activity_default_overrides
    DROP CONSTRAINT harvest_activity_default_overrides_check;

ALTER TABLE harvest_activity_default_overrides
    ADD CONSTRAINT harvest_activity_default_overrides_check
    CHECK (
        retry_max_attempts IS NOT NULL
        OR retry_initial_interval_ms IS NOT NULL
        OR retry_max_interval_ms IS NOT NULL
        OR start_to_close_ms IS NOT NULL
        OR heartbeat_timeout_ms IS NOT NULL
        OR schedule_to_start_ms IS NOT NULL
        OR schedule_to_close_ms IS NOT NULL
        OR max_concurrent IS NOT NULL
    );

COMMENT ON COLUMN harvest_activity_default_overrides.max_concurrent IS
    'The operator override of the max_concurrent cap. NULL inherits the code value.';
