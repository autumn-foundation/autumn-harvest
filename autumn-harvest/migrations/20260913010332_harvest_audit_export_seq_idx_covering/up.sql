-- Cover `occurred_at` in the export-seq index (issue #1271).
--
-- `export_lag_seconds` reads the oldest `occurred_at` within a bounded
-- window of the lowest-sequence pending rows. The old index,
-- `(export_seq) WHERE export_seq IS NOT NULL`, orders that window but does
-- not carry `occurred_at`. A page outside the visibility map still costs a
-- heap fetch per row; carrying `occurred_at` in the index lets a page
-- already in the visibility map skip that fetch.
--
-- Adding `occurred_at` as a second key column keeps the same leading column
-- and the same partial predicate. So every existing use of this index still
-- applies: `ORDER BY export_seq LIMIT n` in the claim scan, and `MIN(seq)`
-- in the redrive lookup.
--
-- Recreated, not altered in place: Postgres has no `ALTER INDEX ... ADD
-- COLUMN`. A plain `DROP INDEX` + `CREATE INDEX` takes `SHARE` on
-- `harvest_audit_log` for the build's duration. On a live deployment with a
-- large audit table, pre-build the covering shape out-of-band first, outside
-- any transaction (`CONCURRENTLY` cannot run inside Diesel's migration
-- transaction):
--   DROP INDEX CONCURRENTLY IF EXISTS harvest_audit_log_export_seq_idx;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_audit_log_export_seq_idx
--       ON harvest_audit_log (export_seq, occurred_at)
--       WHERE export_seq IS NOT NULL;
--
-- The block below then finds the covering shape already in place and skips
-- the rebuild, so this migration's own statement is a no-op. An unconditional
-- `DROP INDEX` before an `IF NOT EXISTS` `CREATE INDEX` cannot offer that: it
-- would remove the pre-built index first and rebuild it non-concurrently
-- regardless, defeating the whole point of pre-building it.
--
-- `indnkeyatts` (the key-column count) is enough to tell the two possible
-- shapes apart here, unlike a general same-named-index check: this index
-- name is created by exactly one other migration in this codebase
-- (`20260728000000_harvest_audit_export`, the single-column shape), so a
-- count of 1 always means "old shape" and 2 always means "already this
-- migration's shape". No third shape is possible.
DO $$
DECLARE
    key_count integer;
    is_valid boolean;
BEGIN
    SELECT pg_index.indnkeyatts, pg_index.indisvalid
      INTO key_count, is_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'harvest_audit_log_export_seq_idx'
      AND pg_index.indrelid = 'harvest_audit_log'::regclass;

    IF key_count IS NULL THEN
        -- No index by this name yet -- a fresh database. Build the covering
        -- shape directly.
        CREATE INDEX harvest_audit_log_export_seq_idx
            ON harvest_audit_log (export_seq, occurred_at)
            WHERE export_seq IS NOT NULL;
    ELSIF key_count = 2 AND is_valid THEN
        -- Already the covering shape: an operator's CONCURRENTLY pre-build,
        -- or a re-run of this migration. Nothing to do.
        NULL;
    ELSE
        -- The old single-column shape, or an INVALID leftover from a
        -- cancelled CONCURRENTLY build. Replace it. Non-concurrent: see the
        -- pre-build recipe above for a large live table.
        DROP INDEX harvest_audit_log_export_seq_idx;
        CREATE INDEX harvest_audit_log_export_seq_idx
            ON harvest_audit_log (export_seq, occurred_at)
            WHERE export_seq IS NOT NULL;
    END IF;
END $$;
