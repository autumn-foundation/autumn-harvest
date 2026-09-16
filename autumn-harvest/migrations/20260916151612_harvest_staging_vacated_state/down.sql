-- Revert: drop the staging-vacate restore marker (issue #1317 review).
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS staging_vacated_state;
