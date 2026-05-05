-- Persist the latest activity heartbeat payload as a retry checkpoint.
-- Last-write-wins; this is operational task state, not workflow history.
ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS heartbeat_details JSONB;
