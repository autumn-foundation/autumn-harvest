-- Audit log for management API mutations (issue #158).
--
-- One row per high-impact operator action. Records who did what, when, and
-- whether it succeeded — without storing raw workflow payloads (no PII).
--
-- Excluded paths (never produce rows): health checks, list/get routes,
-- worker heartbeats, activity heartbeats, read-only UI page loads, and
-- worker fleet reads. See `autumn_harvest::audit::EXCLUDED_ROUTES` for the
-- full exclusion list.
--
-- Audit retention is independent from workflow-history retention (default: 90
-- days). See `HarvestApiState::set_audit_retention_days`.

CREATE TABLE IF NOT EXISTS harvest_audit_log (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    occurred_at      TIMESTAMPTZ NOT NULL    DEFAULT NOW(),
    -- Operator identity. Defaults to 'anonymous' when no actor extractor is
    -- configured; embedders should inject one for production deployments.
    actor            TEXT        NOT NULL    DEFAULT 'anonymous',
    -- Dot-separated operation name, e.g. "workflow.start", "schedule.delete".
    operation        TEXT        NOT NULL,
    -- High-level resource kind: "workflow", "schedule", "dead_letter", etc.
    target_type      TEXT        NOT NULL,
    -- Resource identifier when known (exec_id, schedule UUID, DLQ entry id).
    target_id        TEXT,
    -- The API path template or CLI command that produced this record.
    route_or_command TEXT        NOT NULL,
    -- Optional correlation token supplied by the caller.
    request_id       TEXT,
    -- Idempotency key from the request when present.
    idempotency_key  TEXT,
    -- Final outcome of the operation.
    status           TEXT        NOT NULL    CHECK (status IN ('succeeded', 'failed')),
    -- Short human-readable reason when status = 'failed'. Never contains raw
    -- workflow inputs, outputs, or signal payloads.
    error_summary    TEXT,
    -- Shard that processed the request, when applicable.
    shard_id         INT,
    -- Call origin: 'api' (direct HTTP), 'cli' (harvest-cli), 'ui' (Vantage).
    source           TEXT        NOT NULL    DEFAULT 'api'
                                             CHECK (source IN ('api', 'cli', 'ui'))
);

-- Primary read pattern: recency-ordered full-table scan with optional filters.
CREATE INDEX IF NOT EXISTS harvest_audit_occurred_at_idx
    ON harvest_audit_log (occurred_at DESC);

-- Incident review: "who did what" queries keyed by actor.
CREATE INDEX IF NOT EXISTS harvest_audit_actor_occurred_at_idx
    ON harvest_audit_log (actor, occurred_at DESC);

-- Incident review: "what happened to resource X" queries.
CREATE INDEX IF NOT EXISTS harvest_audit_target_occurred_at_idx
    ON harvest_audit_log (target_type, target_id, occurred_at DESC)
    WHERE target_id IS NOT NULL;

-- Compliance filter: "show me all schedule.delete operations" style.
CREATE INDEX IF NOT EXISTS harvest_audit_operation_occurred_at_idx
    ON harvest_audit_log (operation, occurred_at DESC);
