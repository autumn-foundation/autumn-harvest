-- Session-keyed seek index for harvest_task_queue (issue #2069).
--
-- `enforce_broken_sessions` seeks member tasks by `session_id` twice per
-- candidate session. One seek probes for a RUNNING member. The other loads
-- the PENDING and RUNNING members. Migration 20261003201739 dropped
-- harvest_task_queue_session_id_pending, because its `state = 'PENDING'`
-- predicate could never serve either seek. No index has served them since.
-- Each seek reads every live row that another index returns, then discards
-- all but one or two.
--
-- This index holds only session-pinned rows in an active state. A row leaves
-- it when its task turns terminal. Both seeks filter `state` to PENDING or
-- RUNNING, so the planner can prove the predicate for either one.
--
-- HOT updates are not affected. `session_id` never changes after enqueue,
-- and `state` is already in the predicate of other indexes on this table.
--
-- The build takes a SHARE lock on harvest_task_queue, which blocks writes
-- until it ends. On a live deployment, build it first with the concurrent
-- form. The concurrent form cannot run inside Diesel's migration
-- transaction:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_session_active
--       ON harvest_task_queue (session_id)
--       WHERE session_id IS NOT NULL AND state IN ('PENDING', 'RUNNING');
-- The block below accepts a prebuilt index only if its definition matches
-- and it is valid. It raises an error otherwise.
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
    WHERE pg_class.relname = 'idx_harvest_tq_session_active'
      AND pg_index.indrelid = 'harvest_task_queue'::regclass;

    IF existing_index_oid IS NULL THEN
        -- lock-safety: allow blocking-index #2069 operators prebuild it CONCURRENTLY
        CREATE INDEX idx_harvest_tq_session_active
            ON harvest_task_queue (session_id)
            WHERE session_id IS NOT NULL AND state IN ('PENDING', 'RUNNING');
    ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <>
          'USING btree (session_id) WHERE ((session_id IS NOT NULL) AND (state = ANY (ARRAY[''PENDING''::text, ''RUNNING''::text])))'
    THEN
        RAISE EXCEPTION
            'idx_harvest_tq_session_active already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
            existing_def;
    ELSIF NOT existing_valid THEN
        RAISE EXCEPTION
            'idx_harvest_tq_session_active already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration';
    END IF;
END $$;
