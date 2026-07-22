-- Add workflow-level retry policy support (issue #523).
-- workflow_attempt: attempt number of this execution in its retry chain (1 = first attempt).
-- workflow_retry_policy: frozen effective retry policy for this execution (JSONB, NULL = no auto-retry).
-- retry_of_exec_id: FK to the previous failed execution in the retry chain (NULL for attempt 1).
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS workflow_attempt       INT          NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS workflow_retry_policy  JSONB        NULL,
    ADD COLUMN IF NOT EXISTS retry_of_exec_id       UUID         NULL;

-- schedule-default retry policy (issue #523).
ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS retry_policy           JSONB        NULL;
