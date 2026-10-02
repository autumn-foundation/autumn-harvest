-- Issue #1795. The timeout checker walks live task rows in creation order,
-- one bounded page per refill. A sweep reads only the rows created at or
-- before its start. No existing index orders live rows that way.
--
-- The key is `created_at`, with the Unix epoch for rows enqueued before
-- migration `20260619000000`, which left their `created_at` NULL. The
-- timeout code uses the same expression, so Postgres can match the index.
-- `id` breaks ties, so the walk is a strict keyset.
--
-- With this index, the page's creation bound and keyset bound are both index
-- conditions. A refill reads at most one page of index entries, and rows
-- created after the sweep started are never visited. One sweep reads each
-- live row once, so a large backlog costs linear work.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_task_queue`.
-- That blocks every enqueue, claim and completion until the build ends. The
-- index is partial, but the build still reads every row in the table,
-- terminal rows included. Size the build window from the total row count.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_live_created
--         ON harvest_task_queue
--         ((COALESCE(created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')), id)
--         WHERE state IN ('PENDING', 'RUNNING');
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid.
--
-- The guard rejects two states. A same-named index with a different
-- definition means this migration never installed the intended index, and
-- down.sql would drop an unrelated one. A matching but INVALID index means an
-- out-of-band CONCURRENTLY build did not finish. Drop it with
-- `DROP INDEX CONCURRENTLY` and build it again.
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
    WHERE pg_class.relname = 'idx_harvest_tq_live_created'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_tq_live_created
            ON harvest_task_queue
            ((COALESCE(created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')), id)
            WHERE state IN ('PENDING', 'RUNNING');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (COALESCE(created_at, ''1970-01-01 00:00:00+00''::timestamp with time zone), id) WHERE (state = ANY (ARRAY[''PENDING''::text, ''RUNNING''::text]))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_live_created already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_live_created already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
