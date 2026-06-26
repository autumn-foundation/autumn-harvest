ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS workflow_attempt,
    DROP COLUMN IF EXISTS workflow_retry_policy,
    DROP COLUMN IF EXISTS retry_of_exec_id;

ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS retry_policy;
