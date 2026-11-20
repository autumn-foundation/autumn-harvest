-- Revert the workflow task index.
--
-- A wake then reads every task row of its run through
-- `idx_harvest_tq_workflow` again.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2713 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_tq_workflow_task;
