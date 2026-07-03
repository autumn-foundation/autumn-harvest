-- Revert issue #601 CI hardening: durable wake-request flag.
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS wake_requested;
