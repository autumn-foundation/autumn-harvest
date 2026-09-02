-- Reverse of 20260902131705_harvest_shard_rebalancing (issue #964).
--
-- Only safe while no execution is in `MIGRATING`/`MIGRATED`: rolling the CHECK
-- constraint back over a sealed row would fail, and dropping the forwarding
-- columns would destroy the pointer an already-migrated id resolves through.
DROP INDEX IF EXISTS idx_harvest_we_migrated_forward;
DROP INDEX IF EXISTS idx_harvest_shard_migrations_unsettled;
DROP TABLE IF EXISTS harvest_shard_migrations;

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_we_migrated_forward_check;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS migrated_at;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS migrated_to_shard;

-- The active-uniqueness index was never modified by `up.sql` (see the note
-- there on why a migration must NOT release the business-key slot), so there is
-- nothing to restore.

ALTER TABLE harvest_workflow_executions
    DROP CONSTRAINT IF EXISTS harvest_workflow_executions_state_check;

ALTER TABLE harvest_workflow_executions
    ADD CONSTRAINT harvest_workflow_executions_state_check
        CHECK (state IN (
            'RUNNING',
            'PAUSED',
            'COMPLETED',
            'FAILED',
            'CANCELLED',
            'TIMED_OUT',
            'CONTINUED_AS_NEW',
            'TERMINATED'
        ));
