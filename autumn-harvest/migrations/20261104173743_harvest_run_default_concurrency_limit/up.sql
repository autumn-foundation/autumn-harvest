-- Override the concurrency limit of a workflow type at runtime (issue #2407,
-- epic #2384).
--
-- `WorkflowInfo::concurrency` caps the runs that share a key. Its `limit` is
-- declared in code. This column holds the limit that an operator sets at
-- runtime. A NULL inherits the code value, as for the other columns. The key
-- and `on_conflict` stay deploy-only.
--
-- A start stores its limit on its task row, so a change applies to the next
-- start. The row check now accepts a row that sets the limit alone.
--
-- The table is small and only the API and the cache refresh read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_run_default_overrides
    ADD COLUMN concurrency_limit INTEGER
    CHECK (concurrency_limit > 0);

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_check;

ALTER TABLE harvest_run_default_overrides
    ADD CONSTRAINT harvest_run_default_overrides_check
    CHECK (
        sla_secs IS NOT NULL
        OR execution_timeout_secs IS NOT NULL
        OR chain_execution_timeout_secs IS NOT NULL
        OR concurrency_limit IS NOT NULL
    );

COMMENT ON COLUMN harvest_run_default_overrides.concurrency_limit IS
    'Issue #2407: the operator override of the concurrency limit. NULL inherits the code value.';
