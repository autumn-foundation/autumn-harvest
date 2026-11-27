-- The fire_at index for the unsharded event-batch scanner (Ledger pass).
--
-- Without a sharded pool, `fire_due_on_conn` fires one due batch per
-- transaction. Each read filters on `fire_at <= $1` and orders on `fire_at`.
-- The scanner index is `(shard_id, fire_at)`, and that read has no `shard_id`
-- predicate. So the index cannot give rows in `fire_at` order. Each read
-- fetched every due row and sorted them to return one.
--
-- This index makes the read one index probe. The query text does not
-- change. The sharded read keeps the `(shard_id, fire_at)` index.
-- `docs/performance-event-batch-fire-scan.md` has the numbers.
--
-- The plain build takes `SHARE` on `harvest_event_batches` for the build.
-- That blocks batch admissions and fires. Reads continue. On a large table,
-- build the index first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_event_batches_fire_at
--       ON harvest_event_batches (fire_at);
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
      INTO existing_def, existing_valid
      FROM pg_index i
     WHERE i.indexrelid = to_regclass('idx_harvest_event_batches_fire_at');
    IF existing_def IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_index i
         WHERE i.indexrelid = to_regclass('idx_harvest_event_batches_fire_at')
           AND i.indrelid = 'harvest_event_batches'::regclass
    ) THEN
        RAISE EXCEPTION
            'idx_harvest_event_batches_fire_at already exists on another table -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF existing_def IS NULL THEN
        CREATE INDEX idx_harvest_event_batches_fire_at
            ON harvest_event_batches (fire_at);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (fire_at)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_event_batches_fire_at already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_event_batches_fire_at already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
