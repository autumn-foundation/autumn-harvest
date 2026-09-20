-- Record precisely which row `stage_copy` vacated, so `activate_target` can
-- finalize its marker without guessing (issue #1596 review, follow-up to
-- finding 2, comment_id 4055454415).
--
-- `staging_vacated_state` alone identifies WHAT to restore a vacated row to,
-- not WHICH migration attempt owns that vacate. `activate_target`'s
-- zero-rows-activated branch (a force-terminated target) inferred ownership
-- from uniqueness: "clear a marker only when it is the sole one under this
-- business key." That heuristic fails on a retried call. The first
-- invocation can clear the original marker but then fail before the
-- source-side `DONE` write. A different, later migration can then vacate a
-- new row under the same business key before the retry runs. The retry sees
-- that newer marker as the sole one and wrongly clears it, even though it
-- belongs to the newer migration -- so that migration's own abort finds no
-- marker to restore from if it needs one.
--
-- This column closes that gap: it is set in the SAME target transaction
-- that vacates a row, so it names exactly the row THIS migration vacated,
-- if any, with no inference needed.
ALTER TABLE harvest_shard_migrations
    ADD COLUMN IF NOT EXISTS vacated_execution_id UUID NULL;

COMMENT ON COLUMN harvest_shard_migrations.vacated_execution_id IS
    'The id of the unrelated same-key row this migration''s stage_copy '
    'vacated on the target, if any (issue #1596 review). NULL when staging '
    'vacated nothing. Lets activate_target finalize that row''s '
    'staging_vacated_state marker by id instead of inferring ownership '
    'from business-key uniqueness, which a retried call can get wrong.';
