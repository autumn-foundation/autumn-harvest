-- Revert: drop the operator-mutable triage note column (issue #759).
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS triage_note;
