-- Continue-as-new lets a long-running workflow restart itself with a fresh
-- event history while preserving its logical (workflow_name, workflow_id)
-- identity. The previous run is sealed in the new terminal state
-- 'CONTINUED_AS_NEW' and the next run inserts a new row that reuses the same
-- (workflow_name, workflow_id). To keep `start_or_load_workflow_execution`
-- idempotent without colliding with the now-multi-row chain, the hard
-- UNIQUE(workflow_name, workflow_id) is replaced with a partial unique index
-- that only enforces uniqueness on rows that have not yet been continued.

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

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_workflow_name_workflow_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS
    harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state <> 'CONTINUED_AS_NEW';
