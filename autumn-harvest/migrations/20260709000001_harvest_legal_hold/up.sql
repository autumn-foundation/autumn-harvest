-- Per-execution legal hold (issue #747).
--
-- Exempts a specific workflow execution's `harvest_events` history from
-- retention deletion (and from PII erasure, issue #495) until the hold is
-- released. Additive nullable columns only — no new WorkflowEvent variant,
-- no replay impact, shard-local (a hold is set on the execution's own shard,
-- routed via ExecutionId::shard()).
--
-- Active-hold predicate (single source of truth, see `legal_hold_active`):
--   legal_hold_set_at IS NOT NULL
--   AND (legal_hold_until IS NULL OR legal_hold_until > now)
--
-- Columns:
--   - legal_hold_set_at: wall-clock the hold was placed; NULL = no hold.
--   - legal_hold_until:  optional auto-expiry; NULL = indefinite hold.
--     A hold whose `until` has elapsed is treated as inactive (eligible for
--     retention/erasure again) without any explicit release.
--   - legal_hold_reason: operator-supplied justification (audit trail).
--   - legal_hold_actor:  the principal who placed the hold (audit trail).

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS legal_hold_until  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL,
    ADD COLUMN IF NOT EXISTS legal_hold_actor  TEXT NULL;

-- Discovery index for GET /workflows?legal_hold=true and the retention-skip
-- gate. Held rows are rare, so the partial index stays tiny.
CREATE INDEX IF NOT EXISTS idx_harvest_executions_legal_hold
    ON harvest_workflow_executions (legal_hold_set_at)
    WHERE legal_hold_set_at IS NOT NULL;
