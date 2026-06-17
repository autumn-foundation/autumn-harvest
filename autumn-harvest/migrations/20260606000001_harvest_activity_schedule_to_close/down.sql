DROP INDEX IF EXISTS harvest_task_queue_schedule_to_close_idx;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS schedule_to_close_at;
