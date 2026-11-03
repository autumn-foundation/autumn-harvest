-- Override the chain cap of a workflow type at runtime (issue #2406, epic #2384).
--
-- `WorkflowInfo::chain_execution_timeout` (#617) caps a whole continue-as-new
-- chain. It is declared in code. This column holds the value that an operator
-- sets at runtime. A NULL inherits the code value, as for the other columns.
--
-- A chain that starts reads the override. A chain in flight keeps its stored
-- `chain_deadline_at`, because each successor copies it.
--
-- The row check now accepts a row that sets the chain cap alone.
--
-- The table is small and only the API and the cache refresh read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_run_default_overrides
    ADD COLUMN chain_execution_timeout_secs BIGINT
    CHECK (chain_execution_timeout_secs > 0);

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_check;

ALTER TABLE harvest_run_default_overrides
    ADD CONSTRAINT harvest_run_default_overrides_check
    CHECK (
        sla_secs IS NOT NULL
        OR execution_timeout_secs IS NOT NULL
        OR chain_execution_timeout_secs IS NOT NULL
    );

COMMENT ON COLUMN harvest_run_default_overrides.chain_execution_timeout_secs IS
    'Issue #2406: the operator override of chain_execution_timeout. NULL inherits the code value.';
