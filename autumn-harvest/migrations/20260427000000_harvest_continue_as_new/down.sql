DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_workflow_name_workflow_id_key
        UNIQUE (workflow_name, workflow_id);

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_state_check;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_state_check
        CHECK (state IN (
            'RUNNING',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT'
        ));
