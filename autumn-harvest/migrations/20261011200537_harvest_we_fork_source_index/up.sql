-- Issue #2000. A partial index on the fork source reference of each fork.
--
-- A fork row stores its source in `start_source_ref` with
-- `start_source = 'fork'`. Shard rebalance walks the lineage of every live
-- fork for each candidate it observes (`shard_rebalance`, the `LiveFork`
-- blocker). Without this index, each observation scans every execution to
-- find the forks. With it, the walk reads only fork rows. Retention can use
-- the same index to find the forks of a source (issue #2122).
--
-- The index is partial, so it holds one entry per fork. A deployment with no
-- forks pays almost nothing.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on
-- `harvest_workflow_executions`. That blocks every start and state change
-- until the build ends. The build still reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_fork_source
--         ON harvest_workflow_executions (start_source_ref)
--         WHERE start_source = 'fork';
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
    WHERE pg_class.relname = 'idx_harvest_we_fork_source'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2000 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_we_fork_source
            ON harvest_workflow_executions (start_source_ref)
            WHERE start_source = 'fork';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX [^ ]+ ON (ONLY )?[^ ]+ ', '') <>
          'USING btree (start_source_ref) WHERE (start_source = ''fork''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_fork_source already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_fork_source already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
