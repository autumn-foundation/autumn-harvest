-- Bounded schedule catchup window (issue #484).
--
-- Adds four columns to harvest_schedules:
--
--   catchup_policy      — discriminator: 'skip_all' | 'most_recent' | 'window' | 'unbounded'
--                         NULL means fall back to the existing `catchup` bool (zero backfill).
--   catchup_window_secs — window duration in seconds, used only when catchup_policy = 'window'.
--                         NULL for all other policies.
--   last_catchup_dropped — number of missed slots that were dropped (not fired) on the most
--                          recent scheduler recovery tick. 0 when no recovery has occurred or
--                          the policy is SkipAll/Unbounded (which don't enumerate drops).
--   last_catchup_at     — timestamp of the most recent recovery tick that produced drops.
--                         NULL when last_catchup_dropped = 0.
--
-- Backward compatibility: catchup_policy IS NULL means the existing `catchup` bool drives
-- behaviour, so existing rows require no backfill and all deployments without this column
-- behave identically to today.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS catchup_policy       TEXT        NULL,
    ADD COLUMN IF NOT EXISTS catchup_window_secs  BIGINT      NULL,
    ADD COLUMN IF NOT EXISTS last_catchup_dropped INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_catchup_at      TIMESTAMPTZ NULL;

-- Partial index: fast lookup of schedules that recently dropped catchup fires.
-- Operators use this to quickly find schedules affected by a downtime event.
CREATE INDEX IF NOT EXISTS idx_harvest_schedules_catchup_dropped
    ON harvest_schedules (last_catchup_at)
    WHERE last_catchup_at IS NOT NULL;
