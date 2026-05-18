-- Idempotency key for signals (issue #244 — SignalWithStart).
--
-- Adds a nullable `idempotency_key` column to `harvest_signals` so that the
-- new `signal_with_start` primitive can dedupe webhook retries within a
-- workflow execution. Two signal rows with the same
-- (workflow_exec_id, idempotency_key) are rejected at insert time by the
-- partial unique index; the `NULL` case is unconstrained, preserving the
-- existing send_signal behaviour where no key is supplied.

ALTER TABLE harvest_signals
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

-- Partial unique index: only enforce uniqueness when an idempotency key is
-- present. NULL keys remain unconstrained so the column is fully optional.
CREATE UNIQUE INDEX IF NOT EXISTS uq_harvest_signals_idem
    ON harvest_signals (workflow_exec_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
