-- Revert: drop the cross-shard child placement outbox (issue #956).
DROP INDEX IF EXISTS idx_harvest_cross_shard_children_inflight;
DROP INDEX IF EXISTS idx_harvest_cross_shard_children_actionable;
DROP TABLE IF EXISTS harvest_cross_shard_children;
