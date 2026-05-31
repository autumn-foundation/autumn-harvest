DROP INDEX IF EXISTS harvest_task_queue_running_worker_idx;

ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS crash_strikes;
