-- Durable pending-start records for workflow-start throttling (issue #607).
--
-- One row per DEFERRED start (contrast debounce, which collapses a key's burst
-- into a single row): a start that could not reserve a token is written here
-- BEFORE any WorkflowStarted event is appended, then admitted later by a
-- background scanner as tokens refill. This keeps the systemic cost (worker
-- slots, event rows, warm cache) out of the picture until a start can proceed.
--
-- The token bucket itself reuses the existing harvest_rate_limit_buckets table
-- (keyed "start-throttle:{workflow_name}:{throttle_key}") — no new bucket table.
--
-- Scoping: throttle coordination is shard-local (bucket + rows on the producer's
-- own shard), consistent with per-key concurrency (issue #247) and the activity
-- limiter. Cross-shard global rate coordination is out of scope.

CREATE TABLE IF NOT EXISTS harvest_start_throttle (
    id            UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workflow_name TEXT        NOT NULL,
    -- Resolved throttle key ("" for an unkeyed / global-per-workflow throttle).
    throttle_key  TEXT        NOT NULL,
    -- Token-bucket key in harvest_rate_limit_buckets.
    bucket_key    TEXT        NOT NULL,
    workflow_id   TEXT        NOT NULL,
    queue_name    TEXT        NOT NULL DEFAULT 'default',
    -- The deferred start's input payload.
    input         JSONB       NOT NULL DEFAULT 'null',
    -- Serialised start options (DebounceStartOptions), opaque JSONB so the
    -- schema stays stable as StartWorkflowParams grows.
    start_options JSONB       NOT NULL DEFAULT '{}',
    -- FIFO drain order.
    deferred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- schedule_to_start stale-out deadline (NULL = deferred indefinitely). A row
    -- past this is timed out (dropped) by the scanner rather than run stale.
    expires_at    TIMESTAMPTZ NULL,
    -- Shard this record was routed to (for operator visibility).
    shard_id      INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- Intentionally NO unique constraint: many rows per key is what makes this a
    -- paced queue rather than a collapse (contrast harvest_debounce).
);

-- Scanner drains oldest-first (FIFO).
CREATE INDEX IF NOT EXISTS idx_harvest_start_throttle_drain
    ON harvest_start_throttle (deferred_at ASC);

-- Fast-path FIFO backlog-existence probe at admission time.
CREATE INDEX IF NOT EXISTS idx_harvest_start_throttle_bucket
    ON harvest_start_throttle (bucket_key);
