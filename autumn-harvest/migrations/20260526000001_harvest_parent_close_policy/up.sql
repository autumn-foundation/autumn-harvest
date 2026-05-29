-- Issue #347: parent-close policy for detached child workflow spawns.
--
-- NULL means the child was spawned via the await path (existing behaviour).
-- Non-NULL values ('abandon', 'request_cancel', 'terminate') mean the child was
-- spawned detached; the value determines the cascade action the executor takes
-- when the parent reaches a terminal state.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN parent_close_policy VARCHAR(50) NULL;

-- Partial index to efficiently find running detached children during cascade.
CREATE INDEX harvest_wf_exec_detached_running
    ON harvest_workflow_executions (parent_id, parent_close_policy)
    WHERE parent_close_policy IS NOT NULL AND state = 'RUNNING';
