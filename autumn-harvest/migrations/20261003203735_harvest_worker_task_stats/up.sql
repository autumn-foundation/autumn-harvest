-- Per-worker task stats for gray-failure detection (issue #1815).
--
-- Each worker writes one row on its liveness heartbeat. The row holds a
-- snapshot of the worker's rolling task window. Peers and `status_summary`
-- compare these rows against the fleet median to flag an outlier.
--
-- Worker health state only: no `WorkflowEvent` variant, no change to
-- `harvest_events`, no replay impact. The FK drops a row with its worker.
CREATE TABLE IF NOT EXISTS harvest_worker_task_stats (
    worker_id       TEXT        PRIMARY KEY
                                REFERENCES harvest_workers (worker_id) ON DELETE CASCADE,
    window_tasks    INT4        NOT NULL CHECK (window_tasks >= 0),
    window_failures INT4        NOT NULL CHECK (window_failures >= 0),
    p99_latency_ms  INT8        NULL CHECK (p99_latency_ms >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
