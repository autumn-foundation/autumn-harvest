-- Reverse of 20260906164501_shard_migration_legal_hold_guard (issue #1317).
DROP INDEX IF EXISTS idx_harvest_shard_migrations_unsettled;
CREATE INDEX IF NOT EXISTS idx_harvest_shard_migrations_unsettled
    ON harvest_shard_migrations (created_at)
    WHERE phase NOT IN ('DONE', 'ABORTED');

DROP TRIGGER IF EXISTS harvest_shard_migrations_reset_hold_verification_trigger
    ON harvest_shard_migrations;
DROP FUNCTION IF EXISTS harvest_shard_migrations_reset_hold_verification();

ALTER TABLE harvest_shard_migrations
    DROP COLUMN IF EXISTS verified_legal_hold_set_at,
    DROP COLUMN IF EXISTS legal_hold_verified;
