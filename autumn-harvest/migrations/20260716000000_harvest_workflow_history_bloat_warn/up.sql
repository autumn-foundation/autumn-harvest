-- Add the operator early-warning guard column for workflow history bloat
-- (issue #704).
--
-- history_bloat_warned_at is a nullable exactly-once guard, mirroring the
-- sla_breached_at / nd_blocked_at pattern: it is stamped by a guarded
-- `UPDATE ... WHERE history_bloat_warned_at IS NULL` the first (and only the
-- first) time a still-RUNNING execution's recorded history crosses
-- `history_bloat_warn_fraction * event_hard_cap`. NULL means "not yet
-- warned" -- the correct default for every newly-started execution, and
-- also the correct default for a continue-as-new successor / reset fork
-- (each is a fresh row tracking its own history size from zero, so it is
-- naturally eligible to warn again).
--
-- No new insert-site changes are required anywhere in the engine: the
-- column is never set at INSERT time (it always starts NULL via the
-- Postgres default for an unlisted nullable column) and is only ever
-- mutated later, by the guarded UPDATE described above.
--
-- Deliberately no index: this column is never scanned/filtered in a WHERE
-- clause (unlike nd_blocked_at, which backs `GET /workflows?nd_blocked=true`)
-- -- it is read and written only by exact-row (primary key) operations, so a
-- partial index would add write cost with no read benefit. An index can be
-- added later if an operator-facing "list warned executions" filter is ever
-- requested.

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS history_bloat_warned_at TIMESTAMPTZ NULL;
