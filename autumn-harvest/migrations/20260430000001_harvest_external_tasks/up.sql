-- harvest_external_tasks: lookup table for async/external activity completion.
--
-- The `token` column is the externally-visible, single-use completion handle
-- embedded in the ActivityAwaitingExternal event. The management API resolves
-- (exec_id, activity_id) from a token in O(log n) via the unique index below,
-- without scanning the full event history.
--
-- State machine: PENDING → COMPLETED | FAILED | TIMED_OUT

CREATE TABLE harvest_external_tasks (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token                 UUID NOT NULL,
    workflow_exec_id      UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    activity_id           UUID NOT NULL,
    name                  TEXT NOT NULL,
    queue                 TEXT NOT NULL,
    state                 TEXT NOT NULL DEFAULT 'PENDING'
                              CHECK (state IN ('PENDING', 'COMPLETED', 'FAILED', 'TIMED_OUT')),
    schedule_to_close_at  TIMESTAMPTZ NOT NULL,
    schedule_to_close_secs BIGINT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Primary access pattern: management API resolves token → exec_id in O(log n)
CREATE UNIQUE INDEX idx_harvest_ext_token ON harvest_external_tasks (token);

-- Secondary: timeout scanner finds expired pending tasks
CREATE INDEX idx_harvest_ext_timeout ON harvest_external_tasks (schedule_to_close_at)
    WHERE state = 'PENDING';
