DROP INDEX IF EXISTS harvest_task_queue_concurrency_key_running;
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS concurrency_cap;
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS concurrency_key;
