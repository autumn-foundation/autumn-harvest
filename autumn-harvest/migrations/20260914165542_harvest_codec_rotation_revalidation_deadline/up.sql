-- Split the codec-rotation cursor's revalidation deadline off `updated_at`
-- (issue #1258).
--
-- `updated_at` had two jobs: last-write time, and the throttle deadline for
-- `claim_completed_cursor_revalidation`. Both jobs write it, so they fight.
-- An ordinary batch write bumps `updated_at` on any tick with new rows. On a
-- shard that gets at least one row every five minutes -- an ordinary traffic
-- pattern, not an edge case -- the deadline check never sees an `updated_at`
-- old enough to fire. Revalidation starves, and a row committed below the
-- cursor is never found again.
--
-- `next_revalidation_at` is the deadline now. Only two writers touch it:
-- `write_cursor`, when a pass newly completes (arms it), and
-- `claim_completed_cursor_revalidation`, when the deadline is due (rearms
-- it). An ordinary write to an already-completed cursor leaves it alone, so
-- it survives the exact traffic pattern that used to reset it.
ALTER TABLE harvest_codec_rotation_cursor
    ADD COLUMN IF NOT EXISTS next_revalidation_at TIMESTAMPTZ NULL;

-- Backfill: a stored row already marked complete needs a deadline, or it
-- would never revalidate under the new column until its next full pass.
-- `NOW()` makes it due immediately, which is the safe direction -- it costs
-- one extra census on the next tick, not a missed one.
UPDATE harvest_codec_rotation_cursor
SET next_revalidation_at = NOW()
WHERE completed_at IS NOT NULL
  AND next_revalidation_at IS NULL;

COMMENT ON COLUMN harvest_codec_rotation_cursor.updated_at IS
    'Last time this row was written (issue #1258). Operator-facing cursor '
    'liveness. NOT the revalidation deadline -- see next_revalidation_at.';

COMMENT ON COLUMN harvest_codec_rotation_cursor.next_revalidation_at IS
    'Deadline for the next completed-cursor re-census (issue #1258). NULL '
    'while a pass has not completed. Armed on completion and rearmed by the '
    'claim; an ordinary advancing write leaves it alone.';
