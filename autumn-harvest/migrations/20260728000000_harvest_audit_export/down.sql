-- Revert the off-box audit-export schema (issue #953).
--
-- Drops the cursor table, both partial indexes, and the `export_seq` column.
-- Audit rows themselves are untouched: this migration only ever added
-- bookkeeping alongside them.

DROP TABLE IF EXISTS harvest_audit_export_cursor;
DROP INDEX IF EXISTS harvest_audit_log_export_seq_idx;
DROP INDEX IF EXISTS harvest_audit_log_unexported_idx;
ALTER TABLE harvest_audit_log DROP COLUMN IF EXISTS export_seq;
