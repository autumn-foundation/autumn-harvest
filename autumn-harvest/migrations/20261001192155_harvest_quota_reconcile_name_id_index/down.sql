-- Revert: drop the name-leading candidate index (issue #1631).
-- The old `(id)` index was never dropped, so nothing else to restore.
DROP INDEX IF EXISTS idx_harvest_we_quota_reconcile_name_id;
