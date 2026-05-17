-- Schedule jitter (issue #240)
--
-- Adds a deterministic spread window to harvest_schedules.  The scheduler
-- shifts the effective fire time forward by a seahash-derived offset in
-- [0, jitter_secs) so that many schedules on the same cron expression do
-- not all enqueue workflow starts at the same millisecond.
--
-- DEFAULT 0 means existing rows behave identically to today.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS jitter_secs BIGINT NOT NULL DEFAULT 0;
