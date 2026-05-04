ALTER TABLE harvest_external_tasks
    DROP CONSTRAINT IF EXISTS harvest_external_tasks_state_check;

ALTER TABLE harvest_external_tasks
    ADD CONSTRAINT harvest_external_tasks_state_check
        CHECK (state IN ('PENDING','COMPLETED','FAILED','TIMED_OUT'));

ALTER TABLE harvest_task_queue
    DROP CONSTRAINT IF EXISTS harvest_task_queue_state_check;

ALTER TABLE harvest_task_queue
    ADD CONSTRAINT harvest_task_queue_state_check
        CHECK (state IN ('PENDING','RUNNING','COMPLETED','FAILED'));

DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state <> 'CONTINUED_AS_NEW';

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_state_check;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_state_check
        CHECK (state IN (
            'RUNNING',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW'
        ));
