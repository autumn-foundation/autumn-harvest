-- Revert issue #804: bounded capability-miss redelivery counter.
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS capability_misses;
