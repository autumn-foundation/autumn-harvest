-- Revert: drop the history-bloat early-warning guard column (issue #704).
ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS history_bloat_warned_at;
