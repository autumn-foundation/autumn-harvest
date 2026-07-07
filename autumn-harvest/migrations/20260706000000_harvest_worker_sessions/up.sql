-- Worker sessions (issue #606): co-locate an activity pipeline on one worker.
--
-- Adds advertised/in-use session capacity columns to harvest_workers,
-- a hard-pin session_id column on harvest_task_queue, and a new
-- harvest_sessions table tracking each session's lifecycle.

-- 1. Workers advertise session capacity and report current usage.
ALTER TABLE harvest_workers
    ADD COLUMN max_concurrent_sessions INT4 NOT NULL DEFAULT 0,
    ADD COLUMN in_use_sessions INT4 NOT NULL DEFAULT 0;

-- 2. Task queue rows record which session (if any) they belong to. When set,
--    the claim query hard-pins the row to sticky_worker_id -- unlike ordinary
--    sticky routing, it never fails over to another worker even after the
--    sticky lease expires, since the session's local state only exists on
--    that one worker.
ALTER TABLE harvest_task_queue
    ADD COLUMN session_id UUID;

CREATE INDEX IF NOT EXISTS harvest_task_queue_session_id_pending
    ON harvest_task_queue (session_id)
    WHERE state = 'PENDING' AND session_id IS NOT NULL;

-- 3. Worker sessions: one row per open/closed session.
CREATE TABLE IF NOT EXISTS harvest_sessions (
    id               UUID        PRIMARY KEY,
    workflow_exec_id UUID        NOT NULL
        REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    host_worker_id   TEXT        NOT NULL,
    queue_name       TEXT        NOT NULL,
    state            TEXT        NOT NULL DEFAULT 'ACTIVE',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMPTZ NOT NULL,
    broken_at        TIMESTAMPTZ,
    broken_reason    TEXT,
    completed_at     TIMESTAMPTZ
);

-- Broken-session scanner: find ACTIVE sessions whose lease has expired.
CREATE INDEX IF NOT EXISTS harvest_sessions_active_expires_at
    ON harvest_sessions (expires_at)
    WHERE state = 'ACTIVE';

-- Broken-session scanner: find ACTIVE sessions hosted by a given worker (to
-- reconcile against harvest_workers heartbeat/status).
CREATE INDEX IF NOT EXISTS harvest_sessions_active_host_worker_id
    ON harvest_sessions (host_worker_id)
    WHERE state = 'ACTIVE';

CREATE INDEX IF NOT EXISTS harvest_sessions_workflow_exec_id
    ON harvest_sessions (workflow_exec_id);
