-- Admission gate primitive (issue #377).
--
-- An admission gate halts new workflow starts fleet-wide or for a scoped
-- subset of work (workflow_name, queue, shard_id, or owner) while letting
-- in-flight executions drain naturally. Gates persist here and survive
-- plugin restart; the plugin reloads active gates before starting its
-- worker pool so there is no admission window between boot and re-apply.
--
-- Multiple active gates are evaluated as OR (any match → blocked).
-- Gates may carry an optional expiry; expired gates are ignored by the
-- check logic but are retained for the audit trail. Lifted (deleted)
-- gates are soft-deleted via lifted_at so the audit trail is complete.

CREATE TABLE harvest_admission_gates (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_kind      TEXT        NOT NULL,       -- 'fleet' | 'workflow_name' | 'queue' | 'shard_id' | 'owner'
    scope_value     TEXT,                       -- NULL for 'fleet'; the specific value for all others
    reason          TEXT        NOT NULL,
    message         TEXT,
    created_by      TEXT        NOT NULL DEFAULT 'anonymous',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,                -- NULL = no expiry; auto-cleared at/after this time
    lifted_at       TIMESTAMPTZ,                -- NULL = still active; set on DELETE /admin/gates/{id}
    lifted_by       TEXT
);

-- Fast index for the admission check: only active (not lifted, not expired)
-- gates need to be scanned. Partial index keeps the hot read-path tiny.
CREATE INDEX harvest_admission_gates_active_idx
    ON harvest_admission_gates (scope_kind, scope_value)
    WHERE lifted_at IS NULL;
