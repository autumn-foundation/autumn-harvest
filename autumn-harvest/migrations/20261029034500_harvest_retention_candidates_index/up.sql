-- Issue #2395. The retention candidate scan reads its index again.
--
-- The candidate scan selects six terminal states. `TERMINATED` is one of
-- them. `idx_harvest_we_retention_scan` lists only the other five. Postgres
-- uses a partial index only when the query predicate implies the index
-- predicate. Six states do not imply five, so every retention tick read the
-- whole executions table, on every shard.
--
-- This migration builds `idx_harvest_we_retention_candidates` with all six
-- states, then drops the old index. Six states imply each subset of them. So
-- the new index also serves each query that the old index served.
--
-- `MIGRATED` stays out. A sealed source row is not purgeable (issue #964).
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on
-- `harvest_workflow_executions`. That blocks every state change until the
-- build ends. The index is partial, but the build still reads every row.
-- `DROP INDEX` takes `ACCESS EXCLUSIVE` for a short time.
--
-- On a live, large deployment, build the new index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_retention_candidates
--         ON harvest_workflow_executions (completed_at, id)
--         WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT',
--                         'CONTINUED_AS_NEW','TERMINATED')
--           AND completed_at IS NOT NULL;
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_retention_scan;
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid. It rejects a same-named
-- index with another definition, and an INVALID index from a failed
-- out-of-band build. Drop that one with `DROP INDEX CONCURRENTLY` and build
-- it again.
--
-- `lock_timeout` bounds the wait for each lock (issue #1810).
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_index_oid oid;
    existing_def text;
    existing_valid boolean;
BEGIN
    SELECT pg_class.oid, pg_get_indexdef(pg_class.oid), pg_index.indisvalid
      INTO existing_index_oid, existing_def, existing_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_we_retention_candidates'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2395 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_we_retention_candidates
            ON harvest_workflow_executions (completed_at, id)
            WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT',
                            'CONTINUED_AS_NEW','TERMINATED')
              AND completed_at IS NOT NULL;
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (completed_at, id) WHERE ((state = ANY (ARRAY[''COMPLETED''::text, ''FAILED''::text, ''CANCELLED''::text, ''TIMED_OUT''::text, ''CONTINUED_AS_NEW''::text, ''TERMINATED''::text])) AND (completed_at IS NOT NULL))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_retention_candidates already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_retention_candidates already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;

-- lock-safety: allow blocking-index #2395 operators drop it CONCURRENTLY first, see the header
DROP INDEX IF EXISTS idx_harvest_we_retention_scan;
