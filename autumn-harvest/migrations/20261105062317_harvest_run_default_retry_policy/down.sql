-- Revert issue #2409.
--
-- A row that sets only retry fields would fail the old row check. Such a
-- row holds no other override, so the revert deletes it.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_run_default_overrides
WHERE sla_secs IS NULL
  AND execution_timeout_secs IS NULL
  AND chain_execution_timeout_secs IS NULL
  AND concurrency_limit IS NULL
  AND quota_max_active_executions IS NULL
  AND quota_max_history_bytes IS NULL
  AND quota_max_dead_letters IS NULL;

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_check;

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_retry_interval_check,
    DROP COLUMN retry_max_attempts,
    DROP COLUMN retry_initial_interval_ms,
    DROP COLUMN retry_max_interval_ms;

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
