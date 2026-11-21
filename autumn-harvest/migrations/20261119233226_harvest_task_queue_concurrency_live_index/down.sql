-- Drop the live keyed index of the concurrency gauges.
--
-- After this revert, `queue::concurrency_key_stats` reads the whole task
-- queue again.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_tq_concurrency_live;
