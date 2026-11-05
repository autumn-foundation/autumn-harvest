-- Override the quota limits of a workflow type at runtime (issue #2408,
-- epic #2384).
--
-- `WorkflowInfo::quota` declares three per-tenant admission caps in code.
-- These columns hold the caps that an operator sets at runtime. A NULL
-- inherits the code value, as for the other columns. `key_expr` stays
-- deploy-only, because it decides which runs share a tenant.
--
-- Each admission reads the cap again, and no row stores it. So a change
-- applies to the next admission, and a run in flight is never ended. The
-- row check now accepts a row that sets a quota cap alone.
--
-- The table is small and only the API and the cache refresh read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_run_default_overrides
    ADD COLUMN quota_max_active_executions BIGINT
        CHECK (quota_max_active_executions > 0),
    ADD COLUMN quota_max_history_bytes BIGINT
        CHECK (quota_max_history_bytes > 0),
    ADD COLUMN quota_max_dead_letters BIGINT
        CHECK (quota_max_dead_letters > 0);

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_check;

ALTER TABLE harvest_run_default_overrides
    ADD CONSTRAINT harvest_run_default_overrides_check
    CHECK (
        sla_secs IS NOT NULL
        OR execution_timeout_secs IS NOT NULL
        OR chain_execution_timeout_secs IS NOT NULL
        OR concurrency_limit IS NOT NULL
        OR quota_max_active_executions IS NOT NULL
        OR quota_max_history_bytes IS NOT NULL
        OR quota_max_dead_letters IS NOT NULL
    );

COMMENT ON COLUMN harvest_run_default_overrides.quota_max_active_executions IS
    'Issue #2408: the operator override of the quota max_active_executions. NULL inherits the code value.';
COMMENT ON COLUMN harvest_run_default_overrides.quota_max_history_bytes IS
    'Issue #2408: the operator override of the quota max_history_bytes. NULL inherits the code value.';
COMMENT ON COLUMN harvest_run_default_overrides.quota_max_dead_letters IS
    'Issue #2408: the operator override of the quota max_dead_letters. NULL inherits the code value.';
