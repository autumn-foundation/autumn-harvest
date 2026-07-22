-- Pending-start record table for event-batched workflow starts (issue #518).
--
-- One row per (workflow_name, batch_key). Each qualifying start request
-- appends its payload to buffered_payloads. When jsonb_array_length(buffered_payloads)
-- reaches max_size, exactly one workflow execution is started.
-- A background scanner flushes any due row (fire_at <= NOW()) by starting the execution
-- and deleting the row.

CREATE TABLE IF NOT EXISTS harvest_event_batches (
    id                 UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workflow_name      TEXT        NOT NULL,
    batch_key          TEXT        NOT NULL,
    workflow_id        TEXT        NOT NULL,
    queue_name         TEXT        NOT NULL DEFAULT 'default',
    -- JSONB array containing the buffered payloads in arrival order.
    buffered_payloads  JSONB       NOT NULL DEFAULT '[]',
    -- Serialised start options from the first request: reuse_policy, execution_timeout_secs,
    -- memo, search_attrs, sla_secs, context_headers, priority, etc.
    start_options      JSONB       NOT NULL DEFAULT '{}',
    -- Absolute UTC deadline when this batch must flush: first_seen + max_wait.
    fire_at            TIMESTAMPTZ NOT NULL,
    -- The target max size for size-triggered flush.
    max_size           INT         NOT NULL,
    -- Shard this record was routed to.
    shard_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT harvest_event_batches_unique_key UNIQUE (workflow_name, batch_key)
);

-- Scanner probe index: fire rows whose fire_at has elapsed, filtered by shard.
CREATE INDEX IF NOT EXISTS idx_harvest_event_batches_shard_fire_at
    ON harvest_event_batches (shard_id, fire_at);
