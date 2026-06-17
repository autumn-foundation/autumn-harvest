-- Auto-pause schedules after N consecutive run failures (issue #360).
--
-- Adds three columns to harvest_schedules:
--   consecutive_failure_limit  — operator-configured threshold; NULL = disabled
--   consecutive_failure_count  — running tally maintained by the worker path
--   auto_paused_at             — set when count reaches the limit; NULL = not auto-paused
--
-- All columns default to NULL / 0 so existing rows preserve today's
-- always-fire behaviour without any backfill.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS consecutive_failure_limit INT NULL DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS consecutive_failure_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS auto_paused_at TIMESTAMPTZ NULL;
