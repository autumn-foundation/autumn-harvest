-- Revert: drop the payload-codec re-encryption sweep cursor (issue #948).
--
-- Safe to drop: the table holds only resume bookkeeping. Dropping it makes the
-- next sweep restart from the beginning of the shard, which is a no-op for
-- every row already carrying the active key id (the sweep is idempotent).
DROP TABLE IF EXISTS harvest_codec_rotation_cursor;
