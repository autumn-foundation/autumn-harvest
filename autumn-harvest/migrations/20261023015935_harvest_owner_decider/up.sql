-- Issue #2255. Stage 1b of the decision owner: the owner decider.
--
-- Two objects:
--
-- 1. `idx_harvest_tq_owner_scan`. The owner decider scans the pending
--    workflow rows of its partitions by due time. It finds rows that no wake
--    reached: a lost NOTIFY, a due timer, or the backlog of a partition that
--    it just acquired. The index holds pending workflow rows only. A run has
--    one workflow-task row, so the index holds at most one entry per run.
--
-- 2. `harvest_decision_fence`. It fences the DR generation row and the owner
--    row in one statement. It takes the generation row `FOR SHARE` first,
--    then the owner row `FOR KEY SHARE`, as ADR 0009 decision 6 orders them.
--    Issue #2248 measured the two separate statements at +17.8% of the
--    persist. The lock wait is at most 250 ms on each row. A miss returns
--    `out_locked = false`, and the caller fails closed.
--
-- ## Build cost and the zero-downtime recipe
--
-- A plain `CREATE INDEX` takes `SHARE` on `harvest_task_queue`. That blocks
-- each claim and each task write until the build ends. The index is partial,
-- but the build still reads every row.
--
-- On a live, large deployment, build the index out of band first:
--
--     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_owner_scan
--         ON harvest_task_queue (scheduled_at)
--         WHERE state = 'PENDING' AND task_type = 'workflow';
--
-- Then run this migration. The guard below accepts an index that exists
-- with the expected definition and is valid. It rejects a same-named index
-- with another definition, and an INVALID index that a failed concurrent
-- build left. Drop that one with `DROP INDEX CONCURRENTLY` and build it
-- again.
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
    WHERE pg_class.relname = 'idx_harvest_tq_owner_scan'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2255 operators prebuild it CONCURRENTLY, see the header
        CREATE INDEX idx_harvest_tq_owner_scan
            ON harvest_task_queue (scheduled_at)
            WHERE state = 'PENDING' AND task_type = 'workflow';
    ELSIF regexp_replace(existing_def, '^CREATE INDEX [^ ]+ ON (ONLY )?[^ ]+ ', '') <>
          'USING btree (scheduled_at) WHERE ((state = ''PENDING''::text) AND (task_type = ''workflow''::text))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_owner_scan already exists with an unexpected definition -- resolve the name collision (rename or drop the existing index) before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_owner_scan already exists with the expected definition but is INVALID -- DROP INDEX CONCURRENTLY and retry the out-of-band build before retrying this migration';
    END IF;
END $$;

-- The handler catches lock_not_available by its SQLSTATE. A missing
-- generation row returns a NULL generation, and the caller fails closed with
-- ShardFenced. A missing owner row returns a NULL epoch, and the caller fails
-- closed with OwnershipLost.
CREATE OR REPLACE FUNCTION harvest_decision_fence(
    p_shard INTEGER,
    p_partition INTEGER,
    p_idle TEXT
)
RETURNS TABLE (
    out_generation BIGINT,
    out_holder TEXT,
    out_epoch BIGINT,
    out_locked BOOLEAN
)
LANGUAGE plpgsql
SET lock_timeout = '250ms'
AS $$
DECLARE
    v_generation BIGINT;
    v_holder TEXT;
    v_epoch BIGINT;
BEGIN
    PERFORM set_config('idle_in_transaction_session_timeout', p_idle, true);
    SELECT g.generation INTO v_generation
      FROM harvest_shard_generation g
     WHERE g.shard_id = p_shard
       FOR SHARE OF g;
    SELECT o.holder, o.epoch INTO v_holder, v_epoch
      FROM harvest_decision_owner o
     WHERE o.partition = p_partition
       FOR KEY SHARE OF o;
    RETURN QUERY SELECT v_generation, v_holder, v_epoch, TRUE;
EXCEPTION WHEN lock_not_available THEN
    RETURN QUERY SELECT NULL::BIGINT, NULL::TEXT, NULL::BIGINT, FALSE;
END;
$$;
