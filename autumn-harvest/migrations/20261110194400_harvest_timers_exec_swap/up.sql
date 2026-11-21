-- Issue #2643, second step. Drop the old `idx_harvest_timers_exec` and give
-- `idx_harvest_timers_exec_fired` its name.
--
-- `20261110194313` builds `idx_harvest_timers_exec_fired` on
-- `(workflow_exec_id, fired)`. Its leading column is the only column of the
-- old index. So the new index serves each read that the old index served,
-- and the old index now only costs writes.
--
-- The rename keeps the name that the rebalance plan gate, the Ledger plans
-- and the upgrade guide cite. `ALTER INDEX ... RENAME` takes
-- `SHARE UPDATE EXCLUSIVE` on the index only. Reads and writes continue.
--
-- ## Why the drop is its own migration
--
-- The build in `20261110194313` holds `SHARE` on `harvest_timers` until its
-- commit. A drop in the same transaction asks for `ACCESS EXCLUSIVE` on the
-- same table. A step transaction that waits behind the build can then close a
-- deadlock with it, and the build rolls back. Issue #2395 hit that case.
-- Here the drop holds no earlier lock in its transaction.
--
-- ## Lock cost and the zero-downtime recipe
--
-- `DROP INDEX` (not `CONCURRENTLY`) takes `ACCESS EXCLUSIVE` on
-- `harvest_timers` for a short time. While it waits, each later read and
-- write of the table queues behind it. `lock_timeout` bounds that wait (issue
-- #1810). When it fires, run the migration again.
--
-- On a live, large deployment, swap the indexes out of band first:
--
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_timers_exec;
--     ALTER INDEX idx_harvest_timers_exec_fired RENAME TO idx_harvest_timers_exec;
--
-- Then this migration finds the new definition under the old name and takes
-- no lock. The guard drops `idx_harvest_timers_exec` only when it still has
-- the old definition. It renames only when `idx_harvest_timers_exec_fired`
-- exists. Any other state fails the migration with its definitions.
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    old_def text;
    new_def text;
BEGIN
    SELECT regexp_replace(pg_get_indexdef(pg_class.oid), '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '')
      INTO old_def
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_timers_exec'
      AND pg_index.indrelid = 'harvest_timers'::regclass;

    SELECT regexp_replace(pg_get_indexdef(pg_class.oid), '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '')
      INTO new_def
    FROM pg_class
    JOIN pg_index ON pg_index.indexrelid = pg_class.oid
    WHERE pg_class.relname = 'idx_harvest_timers_exec_fired'
      AND pg_index.indrelid = 'harvest_timers'::regclass
      AND pg_index.indisvalid;

    IF old_def = 'USING btree (workflow_exec_id, fired)' AND new_def IS NULL THEN
        RETURN;
    END IF;
    IF new_def IS DISTINCT FROM 'USING btree (workflow_exec_id, fired)' THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec_fired is missing, INVALID or has an unexpected definition -- run 20261110194313 first: %',
            new_def;
    END IF;
    IF old_def IS NOT NULL AND old_def <> 'USING btree (workflow_exec_id)' THEN
        RAISE EXCEPTION
            'idx_harvest_timers_exec has an unexpected definition -- rename or drop it before retrying this migration: %',
            old_def;
    END IF;

    -- lock-safety: allow blocking-index #2643 operators drop it CONCURRENTLY first, see the header
    DROP INDEX IF EXISTS idx_harvest_timers_exec;
    ALTER INDEX idx_harvest_timers_exec_fired RENAME TO idx_harvest_timers_exec;
END $$;
