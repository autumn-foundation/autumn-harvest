-- Revert issue #2406.
--
-- A row that sets only the chain cap would fail the old row check. Such a row
-- holds no other override, so the revert deletes it.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_run_default_overrides
WHERE sla_secs IS NULL AND execution_timeout_secs IS NULL;

ALTER TABLE harvest_run_default_overrides
    DROP CONSTRAINT harvest_run_default_overrides_check;

ALTER TABLE harvest_run_default_overrides
    DROP COLUMN chain_execution_timeout_secs;

ALTER TABLE harvest_run_default_overrides
    ADD CONSTRAINT harvest_run_default_overrides_check
    CHECK (sla_secs IS NOT NULL OR execution_timeout_secs IS NOT NULL);
