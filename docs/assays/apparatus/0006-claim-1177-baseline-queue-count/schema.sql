-- Minimal stand-in for harvest_task_queue, columns/indexes limited to what
-- the concurrency gate + ORDER BY/LIMIT pushdown question needs. See the
-- pre-registration's "Conditions" section for what was cut and why.
--
-- NOTE (post-review, Codex, round 4): ledger #3/#4's own copy of this file
-- (unmodified here otherwise) has no DROP, so `run_assay.sh` documented as
-- "createdb, then run" fails outright with `psql`'s `ON_ERROR_STOP` on any
-- second invocation against the same database -- and since `run_assay.sh`
-- now `tee`s to `results/run.log` (a separate post-review fix), that
-- failed rerun would also truncate the previously archived log before
-- erroring. Added here so this apparatus's own reproduce instructions are
-- actually idempotent; not backported to #3/#4's archived copies.
DROP TABLE IF EXISTS harvest_task_queue;
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
