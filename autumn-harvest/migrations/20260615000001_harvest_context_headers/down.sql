ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS context_headers;
ALTER TABLE harvest_task_queue DROP COLUMN IF EXISTS context_headers;
