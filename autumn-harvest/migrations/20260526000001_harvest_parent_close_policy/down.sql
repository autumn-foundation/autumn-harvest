DROP INDEX IF EXISTS harvest_wf_exec_detached_running;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS parent_close_policy;
