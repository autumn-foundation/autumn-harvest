-- The concurrency gauges read only the live rows that hold a key and a cap.
--
-- `queue::concurrency_key_stats` feeds `harvest.concurrency.in_flight` and
-- `harvest.concurrency.deferred`. It counts the `PENDING` and `RUNNING` rows
-- with a concurrency key and a cap. `harvest_task_queue_concurrency_key_running`
-- holds `RUNNING` rows only. No index held the `PENDING` rows with a key. So
-- each call read the whole task queue.
--
-- This migration builds `idx_harvest_tq_concurrency_live`. Its predicate
-- holds only the live rows with a key and a cap. Its key columns are the
-- `GROUP BY` columns of the gauge read. These statements use it:
--
-- 1. `queue::concurrency_key_stats`, the gauge read.
-- 2. `queue::concurrency_key_workflows`, the admin read. It has the same
--    row filter.
-- 3. The `concurrency_pending_keys` CTE of the full claim statement. Only a
--    fallback claim runs it.
--
-- The claim cap re-check counts `RUNNING` rows and tests no cap. So it cannot
-- use this index, and it keeps `harvest_task_queue_concurrency_key_running`.
--
-- `docs/performance-concurrency-live-index.md` records the read and the
-- write cost.
--
-- ## Write cost
--
-- A row with no key or no cap never enters the index. A keyed row adds one
-- entry when it is enqueued and one when it is claimed. Its completion adds
-- none, because a finished row is not live.
--
-- Each indexed column is in another index already, except `concurrency_cap`.
-- Each write that changes `state` is not HOT already. An operator cap rewrite
-- changes `concurrency_cap` only. That update is not HOT after this
-- migration.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_task_queue`.
-- That blocks each enqueue, claim and completion until the build ends. The
-- build reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_concurrency_live
--         ON harvest_task_queue (concurrency_key, task_type)
--         WHERE concurrency_key IS NOT NULL
--           AND concurrency_cap IS NOT NULL
--           AND state IN ('PENDING', 'RUNNING');
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid. It rejects a same-named
-- index with another definition, and an INVALID index from a failed
-- out-of-band build. Drop that one with `DROP INDEX CONCURRENTLY` and build
-- it again.
--
-- `lock_timeout` bounds the wait for `SHARE`.
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
    WHERE pg_class.relname = 'idx_harvest_tq_concurrency_live'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2678 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_tq_concurrency_live
            ON harvest_task_queue (concurrency_key, task_type)
            WHERE concurrency_key IS NOT NULL
              AND concurrency_cap IS NOT NULL
              AND state IN ('PENDING', 'RUNNING');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (concurrency_key, task_type) WHERE ((concurrency_key IS NOT NULL) AND (concurrency_cap IS NOT NULL) AND (state = ANY (ARRAY[''PENDING''::text, ''RUNNING''::text])))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_concurrency_live already exists with an unexpected definition. Rename or drop it, then run this migration again: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_concurrency_live already exists but is INVALID. Drop it with DROP INDEX CONCURRENTLY, build it again out of band, then run this migration again';
    END IF;
END $$;
