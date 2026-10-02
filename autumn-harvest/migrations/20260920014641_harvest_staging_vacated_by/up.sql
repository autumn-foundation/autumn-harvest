-- Record precisely WHICH migration attempt vacated a row, so
-- activate_target can finalize its own vacate marker without guessing
-- (issue #1596 review, comment_id 4055601106).
--
-- An earlier fix (comment_id 4055454415) recorded the vacated row's id on
-- the SOURCE shard's harvest_shard_migrations record, in a write AFTER the
-- target transaction that performed the vacate had already committed.
-- That is a cross-database write with no atomicity guarantee: if it fails
-- and stage_copy is retried, the retry's own vacate-detection query finds
-- nothing left to vacate (the row is already CONTINUED_AS_NEW from the
-- first attempt), so it silently overwrites the link with NULL. The
-- vacate itself is real and durable on the target; only the SOURCE's
-- record of its own id was lost.
--
-- This column fixes that by moving the link onto the SAME row, in the
-- SAME statement, as the vacate itself: no cross-database write, no
-- retry race. stage_copy sets it in the identical UPDATE that flips the
-- row to CONTINUED_AS_NEW and stamps staging_vacated_state.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS staging_vacated_by UUID NULL;

COMMENT ON COLUMN harvest_workflow_executions.staging_vacated_by IS
    'The execution id of the migration whose staging vacated this row '
    '(issue #1596 review). Non-NULL exactly when staging_vacated_state '
    'is. Lets activate_target finalize this row''s marker with a direct '
    'match on the vacating migration''s own execution id -- no '
    'cross-database write and no retry race.';
