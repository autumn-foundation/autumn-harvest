SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_cross_shard_children DROP COLUMN IF EXISTS cancel_reason;
