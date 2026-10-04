-- Revert: drop the handler-start marker (issue #1809).
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS timed_out_started_at;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS handler_started_attempt;
