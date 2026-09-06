-- Minimal stand-in for harvest_task_queue, columns/indexes limited to what
-- the concurrency gate + ORDER BY/LIMIT pushdown question needs. See the
-- pre-registration's "Conditions" section for what was cut and why.
CREATE TABLE harvest_task_queue (
    id               BIGSERIAL PRIMARY KEY,
    queue_name       TEXT NOT NULL,
    task_type        TEXT NOT NULL,
    state            TEXT NOT NULL,
    priority         INT NOT NULL DEFAULT 0,
    scheduled_at     TIMESTAMPTZ NOT NULL,
    concurrency_key  TEXT,
    concurrency_cap  INT,
    worker_id        TEXT,
    last_heartbeat_at TIMESTAMPTZ
);

-- Mirrors autumn-harvest/migrations/20260409000000_harvest_initial/up.sql
CREATE INDEX idx_harvest_tq_poll ON harvest_task_queue
    (queue_name, state, priority DESC, scheduled_at)
    WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_running ON harvest_task_queue
    (state, last_heartbeat_at)
    WHERE state = 'RUNNING';
