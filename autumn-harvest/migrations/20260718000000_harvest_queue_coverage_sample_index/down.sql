-- Revert: drop the queue-coverage LATERAL-sample covering index (issue #774).
DROP INDEX IF EXISTS idx_harvest_tq_coverage_sample;
