-- Add the observed-terminal marker for a rebalanced seal (issue #1317).
--
-- A MIGRATED row is an active conflict forever, by design: the run is
-- still live, just on another shard. Nothing propagates the live copy's
-- terminal completion back to this row. A start of the same business key
-- then attaches to a dead seal forever, even long after the real run
-- finishes.
--
-- This column lets a reconciler record that fact once, without touching
-- `state`. Retention and erasure key off `state = 'MIGRATED'` to protect
-- the forwarding pointer; changing `state` would defeat that protection.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS migrated_run_terminal_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN harvest_workflow_executions.migrated_run_terminal_at IS
    'Wall-clock a reconciler observed this seal''s live copy as terminal '
    '(issue #1317). NULL until observed. Non-NULL releases the row from '
    'active-conflict classification without changing state, so retention '
    'and erasure keep treating it as a MIGRATED seal.';

-- Widen the active-uniqueness partial index to also release an
-- observed-terminal seal (issue #1317).
--
-- `replace_execution` vacates this index by setting state to
-- CONTINUED_AS_NEW. That path must never run against a MIGRATED row: the
-- pointer's protection from retention and erasure depends on state staying
-- exactly `MIGRATED`. This index change is the OTHER way to vacate the
-- slot, so a fresh start of the same business key never needs to touch the
-- seal's state at all.
DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED')
      AND migrated_run_terminal_at IS NULL;
