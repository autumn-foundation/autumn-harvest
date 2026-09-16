-- Revert: drop the observed-terminal marker and restore the narrower
-- active-uniqueness index (issue #1317).
--
-- Safe to drop ONLY while no reconciled seal has a live replacement (issue
-- #1317 review). Once a seal is released and a fresh same-key run is
-- admitted, both rows satisfy the narrower predicate below, and creating a
-- UNIQUE index over them fails on the duplicate pair. Detect that case
-- first and refuse with a clear message, rather than an opaque constraint
-- violation, since resolving the duplicate is an operator decision this
-- migration cannot make for them.
DO $$
DECLARE
    dup_count integer;
BEGIN
    SELECT count(*) INTO dup_count FROM (
        SELECT workflow_name, workflow_id
          FROM harvest_workflow_executions
         WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED')
         GROUP BY workflow_name, workflow_id
        HAVING count(*) > 1
    ) duplicated_keys;
    IF dup_count > 0 THEN
        RAISE EXCEPTION
            'cannot roll back 20260915231809_harvest_migrated_seal_terminal_at: % '
            'business key(s) have both a reconciled MIGRATED seal and an active '
            'replacement row. The pre-issue-#1317 unique index cannot represent '
            'both. Resolve or manually reseal the duplicates before retrying.',
            dup_count;
    END IF;
END $$;

DROP INDEX IF EXISTS harvest_we_workflow_name_workflow_id_active_key;

CREATE UNIQUE INDEX harvest_we_workflow_name_workflow_id_active_key
    ON harvest_workflow_executions (workflow_name, workflow_id)
    WHERE state NOT IN ('CONTINUED_AS_NEW', 'TERMINATED');

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS migrated_run_terminal_at;
