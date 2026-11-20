-- A wake reads the workflow task row of a run directly.
--
-- Each run has one workflow task row. A wake, the owner key read and a shard
-- move find that row by `workflow_exec_id` and `task_type = 'workflow'`.
-- `idx_harvest_tq_workflow` holds every task row of the run. A run with many
-- activities keeps each terminal activity row until retention deletes the
-- run. So each wake read every task row of a large run.
--
-- This migration builds `idx_harvest_tq_workflow_task` on `workflow_exec_id`,
-- partial on `task_type = 'workflow'`. It holds one row for each run. The
-- statements keep `task_type = 'workflow'` as a plain test, so the planner
-- can prove the predicate. They hide their `state` and `worker_id` tests, so
-- no live partial index can win. `docs/performance-claim-release-guard.md`
-- records the measurement.
--
-- ## Write cost
--
-- An update of a workflow task row that is not HOT adds one entry to this
-- index. On a 1M-row fixture, that is about 40 bytes of WAL for each update,
-- about 3% more. An activity row is not in the index, so it costs nothing.
-- The index is about 31 bytes for each run.
--
-- ## Build cost and the zero-downtime recipe
--
-- `CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_task_queue`.
-- That blocks every enqueue, claim and completion until the build ends. The
-- index is partial, but the build reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_workflow_task
--         ON harvest_task_queue (workflow_exec_id)
--         WHERE task_type = 'workflow';
--
-- Then run this migration. The guard below accepts an index that already
-- exists with the expected definition and is valid. It rejects a same-named
-- index with another definition, and an INVALID index from a failed
-- out-of-band build. Drop that one with `DROP INDEX CONCURRENTLY` and build it
-- again.
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
    WHERE pg_class.relname = 'idx_harvest_tq_workflow_task'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2713 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_tq_workflow_task
            ON harvest_task_queue (workflow_exec_id)
            WHERE task_type = 'workflow';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (workflow_exec_id) WHERE (task_type = ''workflow''::text)'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_workflow_task already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_workflow_task already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
