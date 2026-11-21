-- Revert the second step of issue #2643. Restore the old
-- `idx_harvest_timers_exec` on `workflow_exec_id`.
--
-- The new index gets back its own name first. `20261110194313` drops it when
-- it is reverted too.
SET LOCAL lock_timeout = '5s';

ALTER INDEX IF EXISTS idx_harvest_timers_exec RENAME TO idx_harvest_timers_exec_fired;

-- lock-safety: allow blocking-index #2643 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_timers_exec
    ON harvest_timers (workflow_exec_id);
