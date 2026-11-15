-- Drop `idx_harvest_events_exec` (#2684).
--
-- It has the key of the constraint `harvest_events_workflow_exec_id_event_id_key`:
-- the same columns, operator classes, collation and options. That index
-- serves each read it served. Each event insert wrote both.
--
-- The partitioned layout keeps the constraint as
-- `(workflow_exec_id, event_id, cohort)`. It still leads with the same two
-- columns, so the per-execution probes of each partition keep an index.
--
-- ## Lock cost
--
-- A plain `DROP INDEX` takes `ACCESS EXCLUSIVE` on `harvest_events` for a short
-- time. While it waits, each later read and write of the table queues behind
-- it. `lock_timeout` bounds that wait. When it fires, run the migration again.
--
-- On a large flat-layout deployment, drop the index out of band first:
--
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_events_exec;
--
-- The partitioned layout cannot drop it `CONCURRENTLY`, because it sits on the
-- partitioned parent. There the migration takes the short lock.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2684 operators drop it CONCURRENTLY first on the flat layout, see the header
DROP INDEX IF EXISTS idx_harvest_events_exec;
