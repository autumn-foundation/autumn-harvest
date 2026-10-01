-- Build the claim-scan index on demand (issue #1667).
--
-- With no sink configured, `export_seq` stays NULL on every row. The partial
-- index `harvest_audit_log_unexported_idx` then matches the whole audit table.
-- It costs every audit insert and storage, and it never serves a read.
--
-- The exporter now builds the index when it first runs. See
-- `audit_export::ensure_unexported_index`.
--
-- A cursor row proves that export ran on this database. Keep the index there,
-- because the exporter needs it and a rebuild would repeat the cost.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM harvest_audit_export_cursor) THEN
        DROP INDEX IF EXISTS harvest_audit_log_unexported_idx;
    END IF;
END
$$;
