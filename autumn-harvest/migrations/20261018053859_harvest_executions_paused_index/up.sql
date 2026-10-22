-- Issue #2212. The claim skips a workflow task whose run is PAUSED.
--
-- The gate probes `harvest_workflow_executions` for `state = 'PAUSED'`. No
-- index covered that state, so each candidate cost a primary-key probe. When
-- more than 32 paused-run tasks sit at the head of a queue, the claim falls
-- back to a scan that reads every PAUSED row. This index holds only PAUSED
-- runs, so both paths read a handful of pages. A row enters it on pause and
-- leaves it on resume.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on
-- `harvest_workflow_executions`. That blocks every state change until the
-- build ends. The index is partial, but the build still reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_paused
--         ON harvest_workflow_executions (id)
--         WHERE state = 'PAUSED';
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid.
--
-- The guard rejects two states. A same-named index with a different
-- definition means this migration never installed the intended index, and
-- down.sql would drop an unrelated one. A matching but INVALID index means an
-- out-of-band CONCURRENTLY build did not finish. Drop it with
-- `DROP INDEX CONCURRENTLY` and build it again.
--
-- `lock_timeout` bounds the wait for `SHARE` (issue #1810). A long
-- transaction on the table then fails this migration after 5 s, instead of
-- queueing every later write behind it. Run the migration again.
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
    WHERE pg_class.relname = 'idx_harvest_we_paused'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2212 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_we_paused
            ON harvest_workflow_executions (id)
            WHERE state = 'PAUSED';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX [^ ]+ ON (ONLY )?[^ ]+ ', '') <>
          'USING btree (id) WHERE (state = ''PAUSED''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_paused already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_paused already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
