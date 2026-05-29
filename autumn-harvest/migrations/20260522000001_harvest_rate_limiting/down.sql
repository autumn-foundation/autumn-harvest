DROP INDEX IF EXISTS idx_harvest_task_queue_rate_limit_key;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS rate_limit_key;
DROP TABLE IF EXISTS harvest_rate_limit_buckets;
