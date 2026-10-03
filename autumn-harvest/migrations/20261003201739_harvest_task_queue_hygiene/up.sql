-- Task-queue table hygiene (issue #1811).
--
-- `harvest_task_queue` has constant `state` churn. This migration tunes
-- autovacuum and fillfactor for it, adds the index that the terminal-task
-- janitor reads, and removes indexes that no query can use.
--
-- Config and index state only: no `WorkflowEvent` variant, no write to
-- `harvest_events`, no replay impact.
--
-- A held lock fails the migration fast, so traffic does not queue behind it.
-- The migration then retries.
SET LOCAL lock_timeout = '5s';

-- 1. Autovacuum and fillfactor.
--
-- The default vacuum threshold is 20% of the table. On a large queue, dead
-- tuples then build up for a long time between passes. `SKIP LOCKED` claims
-- slow down as they scan past them. 2% starts vacuum and analyze ten times
-- sooner. A cost limit of 2000 lets each pass finish faster.
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

-- 2. The terminal-task janitor's index.
--
-- The janitor reads terminal rows in `(completed_at, id)` order and deletes
-- them in batches. Without this index, each batch scans the whole table. Only
-- terminal rows are in it, so a live row costs nothing here.
--
-- On a large live table, build it first with the concurrent form. A plain
-- CREATE INDEX holds SHARE on the table for the whole build and blocks every
-- enqueue, claim and completion. The statement below is then a no-op:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_terminal_completed_at
--       ON harvest_task_queue (completed_at, id)
--       WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED');
CREATE INDEX IF NOT EXISTS idx_harvest_tq_terminal_completed_at
    ON harvest_task_queue (completed_at, id)
    WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED');

-- 3. Replace the RUNNING index so heartbeats can be HOT.
--
-- `idx_harvest_tq_running` has `last_heartbeat_at` as a key column. No query
-- seeks on it: the heartbeat-timeout scan reads
-- `COALESCE(last_heartbeat_at, started_at)`, which no index can match. The
-- key column only makes every heartbeat a non-HOT update. The new index keeps
-- the same RUNNING-row set for the timeout scans. A heartbeat does not change
-- `started_at`.
--
-- Concurrent form for a large live table:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_running_started
--       ON harvest_task_queue (started_at)
--       WHERE state = 'RUNNING';
--   DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_tq_running;
CREATE INDEX IF NOT EXISTS idx_harvest_tq_running_started
    ON harvest_task_queue (started_at)
    WHERE state = 'RUNNING';
DROP INDEX IF EXISTS idx_harvest_tq_running;

-- 4. Drop two indexes that no query can use.
--
-- `idx_harvest_task_queue_rate_limit_key` is `(rate_limit_key) WHERE state =
-- 'PENDING'`. `idx_harvest_task_queue_rate_limit_key_live` has the same key
-- and a wider predicate, so it serves every query the first one could.
--
-- `harvest_task_queue_session_id_pending` is `(session_id) WHERE state =
-- 'PENDING'`. Both session queries filter `state = 'RUNNING'` or
-- `state IN ('PENDING', 'RUNNING')`. Neither implies the predicate, so the
-- planner can never pick the index.
DROP INDEX IF EXISTS idx_harvest_task_queue_rate_limit_key;
DROP INDEX IF EXISTS harvest_task_queue_session_id_pending;
