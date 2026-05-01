-- Worker fleet registry.
--
-- One row per worker process. Workers upsert this row on startup and heartbeat
-- every heartbeat_interval (default 5s). Stale workers (no heartbeat for
-- 2 × interval = 10s) are surfaced by the management API as `unhealthy` but
-- are NOT auto-deleted here — operators want churn visibility.
--
-- `harvest_workers` lives on every shard. The management API merges across
-- shards via iter_shards() to present a unified fleet view.
--
-- No foreign keys to other harvest tables: worker liveness is operational
-- metadata, not workflow history. The append-only event invariant is untouched.
CREATE TABLE IF NOT EXISTS harvest_workers (
    worker_id           TEXT PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- JSON array of queue names this worker polls, e.g. ["default","email-workers"]
    queues              JSONB       NOT NULL DEFAULT '[]',
    -- JSON array of shard IDs this worker is assigned to, e.g. [0]
    shard_assignments   JSONB       NOT NULL DEFAULT '[]',
    -- max_concurrent_workflows + max_concurrent_activities
    max_concurrency     INT4        NOT NULL,
    -- currently executing tasks (updated on every heartbeat)
    in_flight_count     INT4        NOT NULL DEFAULT 0,
    -- hostname or $HOSTNAME env var of the process
    host                TEXT        NOT NULL,
    -- optional crate version string for observability
    version             TEXT,
    -- lifecycle: Active | Draining | Stopped
    status              TEXT        NOT NULL DEFAULT 'Active'
);

-- Fast lookup by lifecycle status (list-by-status is the most common query).
CREATE INDEX IF NOT EXISTS harvest_workers_status_idx
    ON harvest_workers (status);

-- Index for stale-worker detection: scan rows with old last_heartbeat_at.
CREATE INDEX IF NOT EXISTS harvest_workers_last_heartbeat_at_idx
    ON harvest_workers (last_heartbeat_at);
