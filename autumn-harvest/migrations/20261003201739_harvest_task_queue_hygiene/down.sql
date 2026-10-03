-- Revert task-queue table hygiene (issue #1811).
SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS harvest_task_queue_session_id_pending
    ON harvest_task_queue (session_id)
    WHERE state = 'PENDING' AND session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_harvest_task_queue_rate_limit_key
    ON harvest_task_queue (rate_limit_key)
    WHERE state = 'PENDING' AND rate_limit_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_harvest_tq_running
    ON harvest_task_queue (state, last_heartbeat_at)
    WHERE state = 'RUNNING';
DROP INDEX IF EXISTS idx_harvest_tq_running_started;

DROP INDEX IF EXISTS idx_harvest_tq_terminal_completed_at;

ALTER TABLE harvest_task_queue RESET (
    fillfactor,
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor,
    autovacuum_vacuum_cost_limit
);
