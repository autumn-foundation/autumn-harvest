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
-- because the exporter needs it and a rebuild would repeat the cost. A worker
-- role without table ownership could not rebuild it at all.
--
-- DROP INDEX needs an ACCESS EXCLUSIVE lock on `harvest_audit_log`. A held
-- lock must fail the migration fast, so audit traffic does not queue behind
-- it. A failed migration retries. The 5 s bound is the one every other
-- lock-taking migration in this tree uses (`SET LOCAL lock_timeout = '5s'`).
-- It is set with `set_config(..., true)` inside the block, because the block
-- also runs as a standalone statement in the test fixture, where a bare
-- `SET LOCAL` would have no transaction to bind to.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM harvest_audit_export_cursor) THEN
        PERFORM set_config('lock_timeout', '5s', true);
        DROP INDEX IF EXISTS harvest_audit_log_unexported_idx;
    END IF;
END
$$;
