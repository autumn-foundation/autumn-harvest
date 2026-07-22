ALTER TABLE harvest_task_queue DROP COLUMN created_at;

DROP INDEX IF EXISTS idx_harvest_we_canary_order;
