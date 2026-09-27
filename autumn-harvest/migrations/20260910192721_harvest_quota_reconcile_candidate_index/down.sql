-- Revert: drop the quota_reconcile candidate-scan index (issue #1226).
DROP INDEX IF EXISTS idx_harvest_we_quota_reconcile_candidates;
