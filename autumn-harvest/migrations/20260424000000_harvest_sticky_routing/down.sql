DROP INDEX IF EXISTS idx_harvest_tq_sticky_poll;

ALTER TABLE harvest_task_queue
    DROP CONSTRAINT IF EXISTS harvest_tq_sticky_pair_ck;

ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS sticky_timeout,
    DROP COLUMN IF EXISTS sticky_until,
    DROP COLUMN IF EXISTS sticky_worker_id;
