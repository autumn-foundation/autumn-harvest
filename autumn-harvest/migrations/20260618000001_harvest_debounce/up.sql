-- Pending-start record table for debounced workflow starts (issue #499).
--
-- One row per (workflow_name, debounce_key). Each qualifying start request
-- upserts this row: the fire deadline is extended (trailing-edge), the
-- pending_count incremented, and the stored input overwritten (last-input-wins).
-- A background scanner fires each due row by calling start_or_load_workflow_execution
-- then deletes the record — all inside one transaction for crash safety.
--
-- Scoping: debounce coordination is shard-local (consistent with per-key
-- concurrency, issue #247). Cross-shard global debounce is out of scope.

CREATE TABLE IF NOT EXISTS harvest_debounce (
    id                 UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workflow_name      TEXT        NOT NULL,
    debounce_key       TEXT        NOT NULL,
    workflow_id        TEXT        NOT NULL,
    queue_name         TEXT        NOT NULL DEFAULT 'default',
    -- Most recent qualifying request's input (last-input-wins on each upsert).
    last_input         JSONB       NOT NULL DEFAULT 'null',
    -- Serialised start options: reuse_policy, execution_timeout_secs, memo,
    -- search_attrs, sla_secs, context_headers. Stored as opaque JSONB so the
    -- schema stays stable even as StartWorkflowParams grows.
    start_options      JSONB       NOT NULL DEFAULT '{}',
    -- Absolute UTC fire deadline. Recomputed on each upsert as
    -- LEAST(NOW() + window, max_fire_at). The scanner fires when this <= NOW().
    effective_fire_at  TIMESTAMPTZ NOT NULL,
    -- Absolute cap: first_seen + max_wait. Set once at insert time; never updated.
    max_fire_at        TIMESTAMPTZ NOT NULL,
    -- Monotonically-increasing count of qualifying start requests seen so far.
    pending_count      INT         NOT NULL DEFAULT 1,
    -- Shard this record was routed to (for operator visibility).
    shard_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT harvest_debounce_unique_key UNIQUE (workflow_name, debounce_key)
);

-- Scanner probe index: fire rows whose effective_fire_at has elapsed.
CREATE INDEX IF NOT EXISTS idx_harvest_debounce_fire_at
    ON harvest_debounce (effective_fire_at)
    WHERE effective_fire_at IS NOT NULL;
