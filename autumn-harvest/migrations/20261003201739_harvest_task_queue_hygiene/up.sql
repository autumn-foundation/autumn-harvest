-- Task-queue table hygiene (issue #1811).
--
-- `harvest_task_queue` has constant `state` churn. This migration tunes
-- autovacuum and fillfactor for it, adds the indexes that the terminal-task
-- janitor reads, and removes indexes that no query can use.
--
-- Config and index state only: no `WorkflowEvent` variant, no write to
-- `harvest_events`, no replay impact.
--
-- A held lock fails the migration after 5 s, so traffic does not queue behind
-- it. The operator then clears the blocker and re-runs the migration.
SET LOCAL lock_timeout = '5s';

-- On a large live table, run these statements first, one at a time, and not
-- in a transaction. Check that each new index is valid
-- (`pg_index.indisvalid`) before you drop the old RUNNING index. The
-- migration then finds the new indexes, builds nothing, and drops nothing
-- that is already gone:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_terminal_completed_at
--       ON harvest_task_queue (completed_at, id)
--       WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED');
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_running_started
--       ON harvest_task_queue (started_at)
--       WHERE state = 'RUNNING';
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_dl_workflow_exec_id
--       ON harvest_dead_letters (workflow_exec_id)
--       WHERE workflow_exec_id IS NOT NULL;
--   DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_tq_running;
--   DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_task_queue_rate_limit_key;
--   DROP INDEX CONCURRENTLY IF EXISTS harvest_task_queue_session_id_pending;
--
-- Without that, a plain CREATE INDEX holds SHARE on the table for the whole
-- build, and a DROP INDEX holds ACCESS EXCLUSIVE. Both block enqueue, claim
-- and completion while they run.

-- 1. Autovacuum and fillfactor.
--
-- The default vacuum threshold is 20% of the table. On a large queue, dead
-- tuples then build up for a long time between passes. `SKIP LOCKED` claims
-- slow down as they scan past them. 2% starts vacuum 10 times sooner and
-- analyze 5 times sooner. A cost limit of 2000 lets each pass finish faster.
--
-- A fillfactor of 80 keeps free space on each heap page. An update that
-- changes no indexed column can then stay on its page as a HOT update. A
-- heartbeat changes only `last_heartbeat_at` and `heartbeat_details`, so it
-- can be HOT after step 3. A state change is never HOT, because `state` is in
-- index predicates. The fillfactor applies to pages written from now on.
--
-- These settings take a SHARE UPDATE EXCLUSIVE lock. Reads and writes go on.
ALTER TABLE harvest_task_queue SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit = 2000
);

-- 2. New indexes.
--
-- `idx_harvest_tq_terminal_completed_at`: the janitor reads terminal rows in
-- `(completed_at, id)` order and deletes them in batches. Without it, each
-- batch scans the whole table. Only terminal rows are in it.
--
-- `idx_harvest_tq_running_started`: see step 3.
--
-- `idx_harvest_dl_workflow_exec_id`: the janitor keeps a terminal workflow
-- row while a dead letter exists for its execution. The retention purge also
-- deletes dead letters by this column.
--
-- Each index is built only when it is missing. An index that an operator
-- built ahead of time must be valid and have the expected definition. A
-- failed CONCURRENTLY build leaves an INVALID index that the planner never
-- uses, so the migration stops instead of accepting it.
DO $$
DECLARE
    spec text[];
    existing_def text;
    existing_valid boolean;
BEGIN
    FOREACH spec SLICE 1 IN ARRAY ARRAY[
        ARRAY[
            'idx_harvest_tq_terminal_completed_at',
            'harvest_task_queue',
            'USING btree (completed_at, id) WHERE (state = ANY (ARRAY[''COMPLETED''::text, ''FAILED''::text, ''CANCELLED''::text]))',
            'CREATE INDEX idx_harvest_tq_terminal_completed_at ON harvest_task_queue (completed_at, id) WHERE state IN (''COMPLETED'', ''FAILED'', ''CANCELLED'')'
        ],
        ARRAY[
            'idx_harvest_tq_running_started',
            'harvest_task_queue',
            'USING btree (started_at) WHERE (state = ''RUNNING''::text)',
            'CREATE INDEX idx_harvest_tq_running_started ON harvest_task_queue (started_at) WHERE state = ''RUNNING'''
        ],
        ARRAY[
            'idx_harvest_dl_workflow_exec_id',
            'harvest_dead_letters',
            'USING btree (workflow_exec_id) WHERE (workflow_exec_id IS NOT NULL)',
            'CREATE INDEX idx_harvest_dl_workflow_exec_id ON harvest_dead_letters (workflow_exec_id) WHERE workflow_exec_id IS NOT NULL'
        ]
    ]
    LOOP
        SELECT pg_get_indexdef(pg_class.oid), pg_index.indisvalid
          INTO existing_def, existing_valid
        FROM pg_class
        JOIN pg_index ON pg_index.indexrelid = pg_class.oid
        WHERE pg_class.relname = spec[1]
          AND pg_index.indrelid = spec[2]::regclass;

        IF existing_def IS NULL THEN
            EXECUTE spec[4];
        ELSIF regexp_replace(existing_def, '^CREATE INDEX \S+ ON (ONLY )?\S+ ', '') <> spec[3] THEN
            RAISE EXCEPTION
                '% already exists with an unexpected definition -- rename or drop it before retrying this migration: %',
                spec[1], existing_def;
        ELSIF NOT existing_valid THEN
            RAISE EXCEPTION
                '% already exists but is INVALID -- DROP INDEX CONCURRENTLY it and retry the out-of-band build before retrying this migration',
                spec[1];
        END IF;
    END LOOP;
END $$;

-- 3. Replace the RUNNING index so heartbeats can be HOT.
--
-- `idx_harvest_tq_running` is `(state, last_heartbeat_at) WHERE state =
-- 'RUNNING'`. No query seeks on `last_heartbeat_at`: the heartbeat-timeout
-- scan reads `COALESCE(last_heartbeat_at, started_at)`, which no existing
-- index matches. The key column only makes every heartbeat a non-HOT update.
-- `idx_harvest_tq_running_started` has the same predicate, so it serves the
-- same RUNNING-row scans. A heartbeat does not change `started_at`. Step 2
-- has proved the new index valid, so this drop never leaves the RUNNING scans
-- without an index.
DROP INDEX IF EXISTS idx_harvest_tq_running;

-- 4. Drop two indexes that no query can use.
--
-- `idx_harvest_task_queue_rate_limit_key` is `(rate_limit_key) WHERE state =
-- 'PENDING' AND rate_limit_key IS NOT NULL`.
-- `idx_harvest_task_queue_rate_limit_key_live` has the same key and a wider
-- predicate, so it serves every query the first one could.
--
-- `harvest_task_queue_session_id_pending` is `(session_id) WHERE state =
-- 'PENDING' AND session_id IS NOT NULL`. The three session queries filter
-- `state = 'RUNNING'` or `state = ANY('{PENDING,RUNNING}')`. None implies the
-- predicate, so the planner can never pick the index.
DROP INDEX IF EXISTS idx_harvest_task_queue_rate_limit_key;
DROP INDEX IF EXISTS harvest_task_queue_session_id_pending;
