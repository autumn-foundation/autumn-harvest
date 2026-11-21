-- Revert issue #2643. Drop the open-timer index.
--
-- `20261110194400` restores `idx_harvest_timers_exec` on `workflow_exec_id`
-- when it is reverted. That revert runs first. So the open-timer lookups read
-- every timer of the run again after this revert.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2643 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_timers_exec_fired;
