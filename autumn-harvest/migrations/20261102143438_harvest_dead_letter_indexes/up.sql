-- Issue #2401. Three indexes on `harvest_dead_letters`.
--
-- No index held `failed_at`, `original_task_id` or `queue_name`. So each of
-- these statements read the whole DLQ:
--
-- 1. `idx_harvest_dl_failed_at` serves the newest-first DLQ list and its UI
--    page. A backward scan reads one page of rows. It also serves the
--    newest-age probe of the plugin status summary, which reads one row.
-- 2. `idx_harvest_dl_original_task` serves the delete of a callback redrive.
--    A bulk callback redrive sends that delete once per row.
-- 3. `idx_harvest_dl_queue_failed` serves each filter on `queue_name`: the
--    counts and reads of a redrive, a bulk replay, a bulk discard and an
--    aggregate. It also gives the oldest-first order of a queue read.
--
-- Each index is full. The redrive delete binds `task_type`, so a generic
-- plan cannot use a partial index on `task_type = 'CALLBACK'`.
--
-- `error ILIKE` still reads the whole table. A btree index cannot serve a
-- pattern match.
--
-- Each dead-letter insert adds one entry to each index. A row arrives only
-- on a terminal failure, so the cost is small.
--
-- ## Build cost and the zero-downtime recipe
--
-- `harvest_dead_letters` is not a hot table. But `CREATE INDEX` (not
-- `CONCURRENTLY`) takes `SHARE` on it. That blocks each dead-letter insert
-- until the build ends. An insert runs in the transaction that fails a task,
-- so a long build stalls each terminal task failure. The DLQ is largest
-- during an incident. Size the build window from its row count.
--
-- On a live, large deployment, build the indexes out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_dl_failed_at
--         ON harvest_dead_letters (failed_at);
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_dl_original_task
--         ON harvest_dead_letters (original_task_id);
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_dl_queue_failed
--         ON harvest_dead_letters (queue_name, failed_at);
--
-- Then run this migration. Each guard below accepts an index that already
-- exists with the expected definition and is valid. It rejects a same-named
-- index with another definition, and an INVALID index from a failed
-- out-of-band build. Drop that one with `DROP INDEX CONCURRENTLY` and build
-- it again.
--
-- `lock_timeout` bounds the wait for `SHARE` (issue #1810).
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
    WHERE pg_class.relname = 'idx_harvest_dl_failed_at'
      AND pg_index.indrelid = 'harvest_dead_letters'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_dl_failed_at
            ON harvest_dead_letters (failed_at);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (failed_at)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_dl_failed_at already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_dl_failed_at already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;

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
    WHERE pg_class.relname = 'idx_harvest_dl_original_task'
      AND pg_index.indrelid = 'harvest_dead_letters'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_dl_original_task
            ON harvest_dead_letters (original_task_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (original_task_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_dl_original_task already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_dl_original_task already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;

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
    WHERE pg_class.relname = 'idx_harvest_dl_queue_failed'
      AND pg_index.indrelid = 'harvest_dead_letters'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_dl_queue_failed
            ON harvest_dead_letters (queue_name, failed_at);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (queue_name, failed_at)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_dl_queue_failed already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_dl_queue_failed already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
