ALTER TABLE harvest_task_queue
    ADD COLUMN activity_id UUID;

CREATE INDEX idx_harvest_tq_activity_id
    ON harvest_task_queue (workflow_exec_id, activity_id)
    WHERE activity_id IS NOT NULL;
