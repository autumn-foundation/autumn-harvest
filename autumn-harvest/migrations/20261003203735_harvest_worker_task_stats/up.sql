-- Per-worker task stats for gray-failure detection (issue #1815).
--
-- Each worker writes one row on its liveness heartbeat. The row holds a
-- snapshot of the worker's rolling task window. Peers and `status_summary`
-- compare these rows against the fleet median to flag an outlier.
--
-- Worker health state only: no `WorkflowEvent` variant, no change to
-- `harvest_events`, no replay impact. The FK drops a row with its worker.
-- Most worker rows are never deleted, so the outlier tick also prunes rows
-- by `updated_at`. The index keeps that prune and the freshness filter cheap.
--
-- `updated_at` comes from this shard's clock, so it orders rows of one shard
-- only. `snapshot_seq` comes from the worker. It orders one worker's rows
-- across shards whose clocks differ.
--
-- `cohort` is the worker's queues, queue weights, build id and labels, as the
-- worker computes them. A worker is compared only with its cohort. The heartbeat reads only
-- its own cohort, so the second index keeps that read small.
CREATE TABLE IF NOT EXISTS harvest_worker_task_stats (
    worker_id       TEXT        PRIMARY KEY
                                REFERENCES harvest_workers (worker_id) ON DELETE CASCADE,
    window_tasks    INT4        NOT NULL CHECK (window_tasks >= 0),
    window_failures INT4        NOT NULL CHECK (window_failures >= 0),
    p99_latency_ms  INT8        NULL CHECK (p99_latency_ms >= 0),
    snapshot_seq    INT8        NOT NULL DEFAULT 0,
    cohort          TEXT        NOT NULL DEFAULT '',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS harvest_worker_task_stats_updated_at_idx
    ON harvest_worker_task_stats (updated_at);

CREATE INDEX IF NOT EXISTS harvest_worker_task_stats_cohort_idx
    ON harvest_worker_task_stats (cohort);
