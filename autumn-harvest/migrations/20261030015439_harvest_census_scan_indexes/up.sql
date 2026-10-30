-- Issue #2424. Three indexes for statements that read a whole table.
--
-- 1. `idx_harvest_we_canary_completed` serves the canary rows of
--    `GET /admin/canary`. It holds canary runs only, newest first. The read
--    stops after its row cap. The query names the prefix as a literal, so a
--    generic plan can use this partial index too.
-- 2. `idx_harvest_timers_exec` serves the timer copy and the timer delete of a
--    shard rebalance. It also serves each `ON DELETE CASCADE` from an
--    execution, and the worker's timer lookups by execution.
-- 3. `idx_harvest_cross_shard_children_parent` serves the parent-side lookups.
--    Migration `20260901130054` left this index out on purpose. At that time
--    no lookup went by parent. The rebalance quiescence check and the PII
--    erasure now do. The index costs one entry for each row version. Rows of
--    this table are few, because a row goes away when its child is done.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on its table. That blocks
-- every write to the table until the build ends. The canary index is partial,
-- but its build still reads every execution row. Size the build window from
-- the row counts of `harvest_workflow_executions` and `harvest_timers`.
--
-- On a live, large deployment, build the indexes out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_canary_completed
--         ON harvest_workflow_executions (completed_at DESC NULLS LAST)
--         WHERE starts_with(workflow_name, '__harvest_canary_probe');
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_timers_exec
--         ON harvest_timers (workflow_exec_id);
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_cross_shard_children_parent
--         ON harvest_cross_shard_children (parent_exec_id);
--
-- Then run this migration. Each guard below accepts an index that already
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
    WHERE pg_class.relname = 'idx_harvest_we_canary_completed'
      AND pg_index.indrelid = 'harvest_workflow_executions'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2424 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_we_canary_completed
            ON harvest_workflow_executions (completed_at DESC NULLS LAST)
            WHERE starts_with(workflow_name, '__harvest_canary_probe');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (completed_at DESC NULLS LAST) WHERE starts_with(workflow_name, ''__harvest_canary_probe''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_we_canary_completed already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_we_canary_completed already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
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
    WHERE pg_class.relname = 'idx_harvest_timers_exec'
      AND pg_index.indrelid = 'harvest_timers'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2424 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_timers_exec
            ON harvest_timers (workflow_exec_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_exec_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
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
    WHERE pg_class.relname = 'idx_harvest_cross_shard_children_parent'
      AND pg_index.indrelid = 'harvest_cross_shard_children'::regclass;

    IF existing_index_oid IS NULL THEN
        CREATE INDEX idx_harvest_cross_shard_children_parent
            ON harvest_cross_shard_children (parent_exec_id);
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (parent_exec_id)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_cross_shard_children_parent already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_cross_shard_children_parent already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
