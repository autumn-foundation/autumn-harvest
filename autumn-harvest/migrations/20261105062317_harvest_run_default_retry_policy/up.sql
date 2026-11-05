-- Override the workflow retry policy of a type at runtime (issue #2409,
-- epic #2384).
--
-- `WorkflowInfo::retry_policy` declares the policy in code. Each start
-- stores it on the execution row. These columns hold three fields that an
-- operator sets at runtime. A NULL inherits the stored value, as for the
-- other columns. The backoff coefficient, the jitter and the non-retryable
-- error list stay deploy-only.
--
-- The failure path applies the override over the stored policy of the
-- failed run. So a change applies to the next failure of any run of the
-- type. A schedule policy wins. An attempt that already started keeps its
-- fire time and its stored policy. The row check now accepts a row that
-- sets a retry field alone.
--
-- The table is small and only the API and the cache refresh read it. The
-- lock timeout still bounds the wait for its brief exclusive lock.
SET LOCAL lock_timeout = '5s';

-- Each bound matches the decode of the engine. One row that the engine
-- cannot read would fail the load of every override. An interval stops at
-- 365 days (31536000000 ms), the start-delay limit of a retry.
ALTER TABLE harvest_run_default_overrides
    ADD COLUMN retry_max_attempts INTEGER
        CHECK (retry_max_attempts >= 1),
    ADD COLUMN retry_initial_interval_ms BIGINT
        CHECK (retry_initial_interval_ms BETWEEN 1 AND 31536000000),
    ADD COLUMN retry_max_interval_ms BIGINT
        CHECK (retry_max_interval_ms BETWEEN 1 AND 31536000000),
    ADD CONSTRAINT harvest_run_default_overrides_retry_interval_check
        CHECK (retry_max_interval_ms >= retry_initial_interval_ms);

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
        OR retry_max_attempts IS NOT NULL
        OR retry_initial_interval_ms IS NOT NULL
        OR retry_max_interval_ms IS NOT NULL
    );

COMMENT ON COLUMN harvest_run_default_overrides.retry_max_attempts IS
    'Issue #2409: the operator override of the workflow retry max_attempts. NULL inherits the stored value.';
COMMENT ON COLUMN harvest_run_default_overrides.retry_initial_interval_ms IS
    'Issue #2409: the operator override of the workflow retry initial_interval, in milliseconds. NULL inherits the stored value.';
COMMENT ON COLUMN harvest_run_default_overrides.retry_max_interval_ms IS
    'Issue #2409: the operator override of the workflow retry max_interval, in milliseconds. NULL inherits the stored value.';
