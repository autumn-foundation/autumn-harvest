DROP TABLE IF EXISTS harvest_audit_export_cursor;
DROP INDEX IF EXISTS harvest_audit_log_export_seq_idx;
DROP INDEX IF EXISTS harvest_audit_log_unexported_idx;
ALTER TABLE harvest_audit_log DROP COLUMN IF EXISTS export_seq;
