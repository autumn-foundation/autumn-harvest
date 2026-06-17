-- Schedule overlap policy (issue #241)
--
-- Adds three new columns to harvest_schedules:
--   overlap_policy  - what happens when a new firing collides with an in-flight run
--   buffered_runs   - durable storage for buffered fire times (BufferOne / BufferAll)
--   buffer_all_max  - cap on the number of buffered slots under BufferAll
--
-- All DEFAULT values preserve today's behaviour for existing rows.
-- DEFAULT 'skip' means existing schedules continue to silently skip firings
-- when max_active_runs is reached, identical to pre-migration behaviour.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS overlap_policy TEXT NOT NULL DEFAULT 'skip',
    ADD COLUMN IF NOT EXISTS buffered_runs JSONB NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS buffer_all_max INT NOT NULL DEFAULT 100;
