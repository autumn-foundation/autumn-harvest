-- Revert issue #2424. Drop the three indexes.
--
-- After this revert, the canary rows, the rebalance timer reads and the
-- parent-side child lookups read their whole table again.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2424 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_we_canary_completed;

-- lock-safety: allow blocking-index #2424 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_timers_exec;

DROP INDEX IF EXISTS idx_harvest_cross_shard_children_parent;
