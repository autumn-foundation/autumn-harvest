-- Revert: drop the vacating-migration link (issue #1596 review, comment_id
-- 4055601106).
--
-- Safe only while no row is mid-staging-vacate with no other record of
-- which migration owns it. While staging_vacated_by is non-NULL,
-- activate_target and the abort-restore path both rely on it to finalize
-- or restore the paired staging_vacated_state without guessing. Dropping
-- it under an in-flight vacate would strand that row exactly like
-- dropping staging_vacated_state itself would (see
-- 20260916151612_harvest_staging_vacated_state/down.sql). Refuse instead,
-- for the same reason: resolving the in-flight migration is an operator
-- decision this migration cannot make for them.
--
-- Locked in ACCESS EXCLUSIVE mode before the check runs, for the same
-- TOCTOU reason as the sibling migration's down.sql: a plain read takes
-- only ACCESS SHARE, which stays compatible with a concurrent stage_copy
-- row update, so a vacate could commit after the count reads zero but
-- before the ALTER TABLE below takes its own exclusive lock.
LOCK TABLE harvest_workflow_executions IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    vacated_count integer;
BEGIN
    SELECT count(*) INTO vacated_count
      FROM harvest_workflow_executions
     WHERE staging_vacated_by IS NOT NULL;
    IF vacated_count > 0 THEN
        RAISE EXCEPTION
            'cannot roll back 20260920014641_harvest_staging_vacated_by: % '
            'row(s) are mid-staging vacates whose owning migration is '
            'tracked only by this column. Let the in-flight migration(s) '
            'finish or abort first, then retry.',
            vacated_count;
    END IF;
END $$;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS staging_vacated_by;
