-- Revert: drop the revalidation-deadline column (issue #1258).
--
-- Safe to drop: it is sweep throttle bookkeeping. After a rollback the
-- throttle reads `updated_at` again, with the overloaded behavior this
-- migration fixed.
ALTER TABLE harvest_codec_rotation_cursor
    DROP COLUMN IF EXISTS next_revalidation_at;
