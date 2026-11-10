-- Issue #2643. The open-timer lookups of a run read only its open timers.
--
-- Each workflow decision reads the open timers of its run. The reads filter
-- on `workflow_exec_id` and `fired`. A fired timer row stays until retention
-- deletes its run. `idx_harvest_timers_exec` holds `workflow_exec_id` only.
-- So each read fetches every timer that the run has fired, and then drops
-- each fired row in a filter. The cost grows with the length of the run.
--
-- This migration builds `idx_harvest_timers_exec_fired` on
-- `(workflow_exec_id, fired)`. A read then reaches the open timers of the run
-- directly. Diesel binds `fired` as a parameter, so a partial index on
-- `NOT fired` does not serve a generic plan. A plain key column does.
--
-- The leading column is `workflow_exec_id`. So the new index also serves each
-- read that the old index served: the rebalance timer copy and delete, and
-- each `ON DELETE CASCADE` from an execution. `20261110194400` drops the old
-- index in its own transaction. It then gives the new index the old name.
--
-- `docs/performance-timer-pending-exec-lookup.md` records the measurement.
--
-- ## Write cost
--
-- An update of `fired` is not HOT already, because the predicate of
-- `idx_harvest_timers_pending` names `fired`. So that update already adds an
-- entry to each index. Between the two migrations, the table has one more
-- index than before. After the second, it has the same number of indexes.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_timers`. That
-- blocks every timer write until the build ends. The build reads every row.
--
-- On a live, large deployment, build the new index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_timers_exec_fired
--         ON harvest_timers (workflow_exec_id, fired);
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid. It also accepts the end
-- state of both migrations: `idx_harvest_timers_exec` with the new
-- definition. It rejects a same-named index with another definition, and an
-- INVALID index from a failed out-of-band build. Drop that one with
-- `DROP INDEX CONCURRENTLY` and build it again.
--
-- `lock_timeout` bounds the wait for `SHARE` (issue #1810).
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    existing_index_oid oid;
    existing_def text;
    existing_valid boolean;
    swapped_def text;
BEGIN
    SELECT pg_class.oid, pg_get_indexdef(pg_class.oid), pg_index.indisvalid
      INTO existing_index_oid, existing_def, existing_valid
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_timers_exec_fired'
      AND pg_index.indrelid = 'harvest_timers'::regclass;

    SELECT regexp_replace(pg_get_indexdef(pg_class.oid), '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '')
      INTO swapped_def
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_timers_exec'
      AND pg_index.indrelid = 'harvest_timers'::regclass
      AND pg_index.indisvalid;

    IF existing_index_oid IS NULL THEN
        IF swapped_def IS DISTINCT FROM 'USING btree (workflow_exec_id, fired)' THEN
            -- lock-safety: allow blocking-index #2643 operators prebuild it CONCURRENTLY, see the header
            CREATE INDEX idx_harvest_timers_exec_fired
                ON harvest_timers (workflow_exec_id, fired);
        END IF;
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_exec_id, fired)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec_fired already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec_fired already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
