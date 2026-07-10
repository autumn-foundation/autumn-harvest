-- Tiered / summary retention (issue #752).
--
-- When the history-retention janitor hard-deletes a terminal execution, it may
-- optionally demote it into a compact, queryable summary row instead of losing
-- all trace of the run. The summary is written in the SAME shard-local
-- transaction as the execution-row delete (no window where both are absent, no
-- orphan if the delete rolls back). Summaries have their own retention horizon
-- (`SummaryPolicy`) and a second GC pass; `Unbounded` keeps them forever.
--
-- No new WorkflowEvent variant, no replay impact — this is a retention-time
-- projection of terminal state only.
--
-- Columns:
--   - execution_id:  PK, the original execution's UUID.
--   - workflow_name/workflow_id/state: identity + terminal outcome.
--   - started_at/completed_at: run window; duration_ms is the convenience delta.
--   - shard_id:       the execution's own shard (summaries are shard-local).
--   - search_attrs:   copied verbatim for filtering (GIN indexed).
--   - result/error:   OPT-IN captured payload, bounded by a byte cap. Oversized
--                     or offloaded payloads become a typed "_harvest_omitted"
--                     marker (valid JSON, never silent truncation). NULL when
--                     payload capture is disabled.
--   - summarized_at:  when this summary row was written.

CREATE TABLE IF NOT EXISTS harvest_execution_summaries (
    execution_id   UUID PRIMARY KEY,
    workflow_name  TEXT NOT NULL,
    workflow_id    TEXT NOT NULL,
    state          TEXT NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ NOT NULL,
    duration_ms    BIGINT NULL,
    shard_id       INTEGER NOT NULL,
    search_attrs   JSONB NULL,
    result         JSONB NULL,
    error          TEXT NULL,
    summarized_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- GC scan + keyset pagination cursor (completed_at, execution_id).
CREATE INDEX IF NOT EXISTS idx_harvest_execution_summaries_gc
    ON harvest_execution_summaries (completed_at, execution_id);

-- Filter-by-name, newest-first listing.
CREATE INDEX IF NOT EXISTS idx_harvest_execution_summaries_name
    ON harvest_execution_summaries (workflow_name, completed_at DESC);

-- Search-attribute containment filtering.
CREATE INDEX IF NOT EXISTS idx_harvest_execution_summaries_search
    ON harvest_execution_summaries USING GIN (search_attrs);
