-- Reverse of 20260906164501_shard_migration_legal_hold_guard (issue #1317).
ALTER TABLE harvest_shard_migrations
    DROP COLUMN IF EXISTS verified_legal_hold_set_at,
    DROP COLUMN IF EXISTS legal_hold_verified;
