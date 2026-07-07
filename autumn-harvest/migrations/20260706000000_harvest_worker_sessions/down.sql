-- Rollback: worker sessions (issue #606)

DROP TABLE IF EXISTS harvest_sessions;

DROP INDEX IF EXISTS harvest_task_queue_session_id_pending;

ALTER TABLE harvest_task_queue
    DROP COLUMN IF EXISTS session_id;

ALTER TABLE harvest_workers
    DROP COLUMN IF EXISTS in_use_sessions,
    DROP COLUMN IF EXISTS max_concurrent_sessions;
