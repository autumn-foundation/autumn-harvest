-- Schedule pause metadata (issue #229)
--
-- Adds operator-facing pause audit columns to harvest_schedules:
--   paused_at   - when the schedule was paused (NULL when active)
--   paused_by   - actor identity that issued the pause (NULL when active)
--   pause_reason - free-text reason recorded at pause time (NULL when active / no reason given)
--
-- These columns are cleared (set to NULL) on resume so the row always
-- reflects only the *current* pause state. Historical pause/resume
-- events are preserved in harvest_audit_log.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS paused_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paused_by     TEXT,
    ADD COLUMN IF NOT EXISTS pause_reason  TEXT;
