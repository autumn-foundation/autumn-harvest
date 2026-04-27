DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

-- Pre-continue-as-new Harvest cannot represent either the
-- `CONTINUED_AS_NEW` state or multiple rows sharing one logical
-- `(workflow_name, workflow_id)`. Preserve those sealed historical runs by
-- rewriting them into ordinary terminal rows with a synthetic workflow_id
-- derived from their execution id, leaving the latest run on the original
-- logical key.
UPDATE harvest_workflow_executions
SET workflow_id = workflow_id || '::continued-as-new:' || id::TEXT,
    state = 'COMPLETED'
WHERE state = 'CONTINUED_AS_NEW';

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
