-- Post-review (Codex, round 3) correction check: the report and
-- batch_claim.sql's comment cited ledger #4's own recheck.sql cost as
-- "~1 buffer at both 256 and 5,000 key cardinality" -- true only for the
-- *idle* (0 RUNNING) scenario. Ledger #4's own archived
-- hot_256-recheck.explain.txt / hot_5000-recheck.explain.txt both show 34
-- buffers, not 1. Re-measured directly against this apparatus's own
-- schema/seed (identical to ledger #4's) rather than only re-citing: same
-- query, same 2,000-RUNNING-row fixture, at both 256 and 5,000 distinct
-- keys.
\set backlog 10000
\set queues 4
\set running_rows 2000
\set keys 256
\i seed.sql
\set probe_key 'bench-ck-0'
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT COUNT(*) FROM harvest_task_queue recheck
WHERE recheck.concurrency_key = :'probe_key'
  AND recheck.task_type = 'activity'
  AND recheck.state = 'RUNNING'
  AND recheck.worker_id IS NOT NULL;

\set keys 5000
\i seed.sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT COUNT(*) FROM harvest_task_queue recheck
WHERE recheck.concurrency_key = :'probe_key'
  AND recheck.task_type = 'activity'
  AND recheck.state = 'RUNNING'
  AND recheck.worker_id IS NOT NULL;
