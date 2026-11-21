-- Revert the max_concurrent override of an activity type.
--
-- A row that sets only the cap would fail the old row check. Such a row
-- holds no other override, so the revert deletes it. The task rows keep the
-- cap that the last rewrite stored. Each one drains under that cap.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_activity_default_overrides
WHERE retry_max_attempts IS NULL
  AND retry_initial_interval_ms IS NULL
  AND retry_max_interval_ms IS NULL
  AND start_to_close_ms IS NULL
  AND heartbeat_timeout_ms IS NULL
  AND schedule_to_start_ms IS NULL
  AND schedule_to_close_ms IS NULL;

ALTER TABLE harvest_activity_default_overrides
    DROP CONSTRAINT harvest_activity_default_overrides_check;

ALTER TABLE harvest_activity_default_overrides
    DROP COLUMN max_concurrent;

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
    );
