-- Revert: drop the observed-terminal marker and restore the narrower
-- active-uniqueness index (issue #1317).
--
-- Safe to drop: it is reconciler bookkeeping only. After a rollback a
-- rebalanced business key stays blocked from a fresh start once again,
-- the pre-existing behavior this migration fixed.
DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED');

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS migrated_run_terminal_at;
