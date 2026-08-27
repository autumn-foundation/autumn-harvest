-- Drop the index BEFORE the table: it lives on `harvest_task_queue`, which this
-- migration does not own, so dropping `harvest_activity_pauses` alone would
-- leave it behind. A rollback that leaves an index on a hot, high-write table
-- has not restored the previous schema -- the deployment keeps paying its
-- storage and per-INSERT maintenance cost for a feature that is no longer there.
DROP INDEX IF EXISTS idx_harvest_tq_activity_pause;

DROP TABLE IF EXISTS harvest_activity_pauses;
