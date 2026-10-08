-- Revert the export-seq index to its pre-#1271, single-column shape.
DROP INDEX IF EXISTS harvest_audit_log_export_seq_idx;
CREATE INDEX IF NOT EXISTS harvest_audit_log_export_seq_idx
    ON harvest_audit_log (export_seq)
    WHERE export_seq IS NOT NULL;
