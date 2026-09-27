-- Make a shard-rebalance staging vacate reversible (issue #1317 review, P1).
--
-- `stage_copy` can seal an UNRELATED terminal row for the same business key
-- on the target, to free the key's active-uniqueness slot for the copy
-- being staged. If the migration later aborts, that seal must be undone --
-- otherwise a FAILED/CANCELLED/TIMED_OUT/COMPLETED run would read as
-- CONTINUED_AS_NEW forever, even though nothing actually continued it.
--
-- This column carries the vacated row's own prior state, so the abort path
-- can restore it exactly, without a separate lookup table. NULL on every
-- row outside that narrow staging window.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS staging_vacated_state TEXT NULL;

COMMENT ON COLUMN harvest_workflow_executions.staging_vacated_state IS
    'The state a shard-rebalance staging vacate sealed over (issue #1317 '
    'review). Non-NULL only while the migration that vacated this row is '
    'still in flight. An abort restores state to this value and clears '
    'it; a successful cutover just clears it, leaving the seal in place.';
