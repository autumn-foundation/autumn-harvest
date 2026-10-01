-- Restore the index that migration 20260728000000 created (issue #1667).
CREATE INDEX IF NOT EXISTS harvest_audit_log_unexported_idx
    ON harvest_audit_log (occurred_at, id)
    WHERE export_seq IS NULL;
