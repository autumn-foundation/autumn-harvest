-- Revert: drop the staging-vacate restore marker (issue #1317 review).
--
-- Safe to drop ONLY while no migration is mid-staging with a vacated row in
-- flight (issue #1317 review, P2). While `staging_vacated_state` is
-- non-NULL, it is the ONLY surviving record of what that row's state was
-- before staging sealed it `CONTINUED_AS_NEW`. Dropping the column then
-- would strand that row: an abort could still delete the staged
-- `MIGRATING` copy, but could no longer restore the row it vacated,
-- permanently changing its result semantics. Refuse with a clear message
-- instead, since resolving the in-flight migration (let it finish, or
-- abort it) is an operator decision this migration cannot make for them.
DO $$
DECLARE
    vacated_count integer;
BEGIN
    SELECT count(*) INTO vacated_count
      FROM harvest_workflow_executions
     WHERE staging_vacated_state IS NOT NULL;
    IF vacated_count > 0 THEN
        RAISE EXCEPTION
            'cannot roll back 20260916151612_harvest_staging_vacated_state: % '
            'row(s) are mid-staging vacates with no other record of their '
            'prior state. Let the in-flight migration(s) finish or abort '
            'first, then retry.',
            vacated_count;
    END IF;
END $$;

ALTER TABLE harvest_workflow_executions
    DROP COLUMN IF EXISTS staging_vacated_state;
