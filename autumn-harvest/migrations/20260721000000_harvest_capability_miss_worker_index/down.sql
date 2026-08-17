-- Revert issue #804 (review round 39): drop the capability-miss lookup index.
DROP INDEX IF EXISTS idx_harvest_tq_capability_miss_workers;
