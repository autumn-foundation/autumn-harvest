DROP INDEX IF EXISTS idx_harvest_tq_activity_id;

ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS activity_id;
