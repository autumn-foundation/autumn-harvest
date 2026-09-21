-- Revert: drop the timer-provenance column (issue #1402).
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS timer_fires_at;
