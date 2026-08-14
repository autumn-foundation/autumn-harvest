-- Revert issue #804: bounded capability-miss redelivery counter.
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS capability_misses;

-- Revert issue #804 (review round 6): the distinct-misser set.
ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS capability_miss_workers;
