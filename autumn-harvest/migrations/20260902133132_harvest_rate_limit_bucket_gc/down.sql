DROP INDEX IF EXISTS idx_harvest_task_queue_rate_limit_key_live;

ALTER TABLE harvest_rate_limit_buckets
    DROP COLUMN IF EXISTS baseline_set_at,
    DROP COLUMN IF EXISTS last_registered_at;
