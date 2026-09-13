-- Cover `occurred_at` in the export-seq index (issue #1271).
--
-- `export_lag_seconds` reads the oldest `occurred_at` within a bounded
-- window of the lowest-sequence pending rows. The old index,
-- `(export_seq) WHERE export_seq IS NOT NULL`, orders that window but does
-- not carry `occurred_at`. Every row in the window then costs a heap fetch.
--
-- Adding `occurred_at` as a second key column keeps the same leading column
-- and the same partial predicate. So every existing use of this index still
-- applies: `ORDER BY export_seq LIMIT n` in the claim scan, and `MIN(seq)`
-- in the redrive lookup. The bounded lag scan additionally becomes
-- index-only -- Postgres never touches the heap for `occurred_at`.
--
-- Recreated, not altered in place: Postgres has no `ALTER INDEX ... ADD
-- COLUMN`. On a live deployment with a large audit table, prefer the
-- concurrent form outside Diesel's migration transaction:
--   DROP INDEX CONCURRENTLY IF EXISTS harvest_audit_log_export_seq_idx;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_audit_log_export_seq_idx
--       ON harvest_audit_log (export_seq, occurred_at)
--       WHERE export_seq IS NOT NULL;
DROP INDEX IF EXISTS harvest_audit_log_export_seq_idx;
CREATE INDEX IF NOT EXISTS harvest_audit_log_export_seq_idx
    ON harvest_audit_log (export_seq, occurred_at)
    WHERE export_seq IS NOT NULL;
