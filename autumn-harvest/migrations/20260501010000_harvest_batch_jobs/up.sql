-- Batch operations registry (issue #102).
--
-- One row per submitted batch job (cancel / terminate / signal). The row IS
-- the durable job state — survives plugin restart, resumes from
-- `completed + failed` cursor. The executor task polls open rows
-- (status IN ('Pending','Running')) on a background tick.
--
-- `harvest_batch_jobs` lives on every shard. The executor walks
-- `iter_shards()` and applies the filter per-shard; per-shard progress is
-- rolled up into the same row via incremental counter updates so the
-- response shape stays single-job for the operator.
--
-- No foreign keys: a batch job operates on a *filter* over the workflow
-- table, not specific rows, and survives the targeted workflows being
-- garbage-collected.
CREATE TABLE IF NOT EXISTS harvest_batch_jobs (
    id                  UUID PRIMARY KEY,
    -- Wire-format string: 'Cancel' | 'Terminate' | 'Signal'.
    action              TEXT        NOT NULL,
    -- Captured filter shape so the executor can rebuild it on resume.
    -- Mirrors the GET /workflows query contract: state / workflow_name /
    -- search_attrs.
    filter              JSONB       NOT NULL,
    -- Set when action = 'Signal'; NULL otherwise.
    signal_name         TEXT,
    signal_payload      JSONB,
    -- Lifecycle: 'Pending' | 'Running' | 'Completed' | 'Failed'.
    status              TEXT        NOT NULL DEFAULT 'Pending',
    -- Total target count, recorded once when the executor first claims the
    -- job and counts the filter result set across shards.
    total               BIGINT      NOT NULL DEFAULT 0,
    completed           BIGINT      NOT NULL DEFAULT 0,
    failed              BIGINT      NOT NULL DEFAULT 0,
    -- Per-target failures appended as `[{"execution_id": ..., "reason": ...}]`.
    errors              JSONB       NOT NULL DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    -- Operator-supplied retry token. UNIQUE so resubmitting the same key
    -- short-circuits to the existing row instead of starting a duplicate.
    idempotency_key     TEXT,
    -- Optional caller identity for audit ("incident commander handle, CI job
    -- run, ..."). The management API surface is unauthenticated by
    -- convention; richer RBAC is its own slice.
    created_by          TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS harvest_batch_jobs_idempotency_key_idx
    ON harvest_batch_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Open-job scan executed every executor tick.
CREATE INDEX IF NOT EXISTS harvest_batch_jobs_status_idx
    ON harvest_batch_jobs (status);

-- Created-at ordered list for the management list endpoint.
CREATE INDEX IF NOT EXISTS harvest_batch_jobs_created_at_idx
    ON harvest_batch_jobs (created_at DESC);
