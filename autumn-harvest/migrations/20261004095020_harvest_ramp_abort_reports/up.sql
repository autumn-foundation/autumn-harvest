-- Report ledger for the build ramp guard (issue #1814).
--
-- Many replicas can run the guard, and each one can decide that it must
-- report the same abort. One row per aborted ramp, keyed by its `ramp_id`,
-- makes the report exactly-once. The guard inserts the row with ON CONFLICT
-- DO NOTHING in the same transaction as the audit row. Only the guard whose
-- insert is new writes the audit row and counts the metric. A guard whose
-- insert finds the row only marks the abort markers as reported.
--
-- The table lives on the database of the audit pool, next to
-- `harvest_audit_log`. It holds one small row per auto-aborted ramp.
CREATE TABLE IF NOT EXISTS harvest_ramp_abort_reports (
    ramp_id     UUID PRIMARY KEY,
    queue_name  TEXT NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
