-- Reverse issue #701 continue-as-new chain columns.
DROP INDEX IF EXISTS idx_harvest_wfx_continued_from;
DROP INDEX IF EXISTS idx_harvest_wfx_first_exec_id;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS first_exec_id;
ALTER TABLE harvest_workflow_executions DROP COLUMN IF EXISTS continued_from_exec_id;
