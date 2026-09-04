-- The untested candidate fix docs/performance.md names and leaves open.
CREATE INDEX IF NOT EXISTS idx_harvest_tq_concurrency_running
    ON harvest_task_queue (concurrency_key, task_type)
    WHERE state = 'RUNNING' AND worker_id IS NOT NULL;
