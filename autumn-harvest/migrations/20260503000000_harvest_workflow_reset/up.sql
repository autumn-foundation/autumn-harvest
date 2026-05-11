-- Workflow reset forks seal the source execution and continue from a new
-- execution row with the same logical (workflow_name, workflow_id). The sealed
-- source must not occupy the active uniqueness slot.

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
            'CONTINUED_AS_NEW',
            'TERMINATED'
        ));

DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED');

ALTER TABLE harvest_task_queue
    DROP CONSTRAINT IF EXISTS harvest_task_queue_state_check;

ALTER TABLE harvest_task_queue
    ADD CONSTRAINT harvest_task_queue_state_check
        CHECK (state IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED'));

ALTER TABLE harvest_external_tasks
    DROP CONSTRAINT IF EXISTS harvest_external_tasks_state_check;

ALTER TABLE harvest_external_tasks
    ADD CONSTRAINT harvest_external_tasks_state_check
        CHECK (state IN ('PENDING','COMPLETED','FAILED','TIMED_OUT','CANCELLED'));
