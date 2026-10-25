-- Issue #2308. The owner decider takes workflow tasks in claim order.
--
-- `idx_harvest_tq_owner_order` serves the ordered scan of the decider. It
-- reads the first rows of each owned partition by priority, then by due
-- time, for new starts and for continuations apart. The claim uses the same
-- two heads (`idx_harvest_tq_claim_seek`). So the decider merges at most a
-- few rows per partition and head, never a whole backlog.
--
-- `idx_harvest_tq_owner_scan` stays. It gives the next due time of a
-- partition with one probe.
--
-- The index holds pending workflow rows only: one entry per run. It adds one
-- index write to each workflow-task insert, state change and reschedule.
--
-- ## Build cost and the zero-downtime recipe
--
-- A plain `CREATE INDEX` takes `SHARE` on `harvest_task_queue`. That blocks
-- each claim and each task write until the build ends. On a live, large
-- deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_owner_order
--         ON harvest_task_queue
--         (decision_partition, (new_start AND attempt = 0), priority DESC, scheduled_at)
--         WHERE state = 'PENDING' AND task_type = 'workflow';
--
-- Then run this migration. The guard accepts a valid index with the expected
-- definition. It rejects a same-named index with another definition, and an
-- INVALID index that a failed concurrent build left. Drop that one with
-- `DROP INDEX CONCURRENTLY` and build it again.
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
    WHERE pg_class.relname = 'idx_harvest_tq_owner_order'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2308 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_tq_owner_order
            ON harvest_task_queue
            (decision_partition, (new_start AND attempt = 0), priority DESC, scheduled_at)
            WHERE state = 'PENDING' AND task_type = 'workflow';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX [^ ]+ ON (ONLY )?[^ ]+ ', '') <>
          'USING btree (decision_partition, ((new_start AND (attempt = 0))), priority DESC, scheduled_at) WHERE ((state = ''PENDING''::text) AND (task_type = ''workflow''::text))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_owner_order already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_owner_order already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
