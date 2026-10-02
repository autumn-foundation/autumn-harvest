-- Issue #1795. The timeout checker walks live task rows in `id` order, one
-- bounded page per refill. No existing index orders live rows by `id`. The
-- primary key also holds terminal rows, so a walk on it is not bounded.
--
-- With this index, a refill reads at most one page of live rows. One sweep
-- reads each live row once, so a large backlog costs linear work.
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
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_live_id
--         ON harvest_task_queue (id) WHERE state IN ('PENDING', 'RUNNING');
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
    WHERE pg_class.relname = 'idx_harvest_tq_live_id'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_tq_live_id
            ON harvest_task_queue (id) WHERE state IN ('PENDING', 'RUNNING');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (id) WHERE (state = ANY (ARRAY[''PENDING''::text, ''RUNNING''::text]))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_live_id already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_live_id already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
