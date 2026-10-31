SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_completion_trigger_outbox DROP COLUMN IF EXISTS tenant;

ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS tenant;

ALTER TABLE harvest_api_tokens DROP COLUMN IF EXISTS tenant;
