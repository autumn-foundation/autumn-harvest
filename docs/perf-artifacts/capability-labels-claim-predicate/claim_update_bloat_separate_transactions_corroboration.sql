-- Follow-up corroboration for docs/performance-capability-labels.md's
-- "Write-side cost" section, addressing a review finding on PR #1192
-- (against commit eb621e2, on claim_update_bloat_corroboration.sql):
--
--   "the single set-based UPDATE is a confound: all 10,000 old tuple
--   versions remain visible until this one statement/transaction finishes,
--   so PostgreSQL cannot vacuum or reuse any of their space during the
--   pass. The measured claim drain invokes claim_task() 10,000 times as
--   separate statements, allowing pruning/autovacuum and reuse between
--   committed claims; therefore the reported 250/344-page growth and the
--   derived +37.6% are a bulk-transaction worst case, not a direct
--   measurement of the production mechanism as documented."
--
-- The finding is correct on the mechanism: within one uncommitted
-- transaction, a dead tuple's xmax belongs to a transaction that has not
-- yet committed, so neither opportunistic HOT-pruning-on-access nor
-- autovacuum can reclaim that space for reuse by a *later* row processed
-- in the same statement -- reclaimability requires the killing transaction
-- to have committed and be below every live snapshot's OldestXmin. A loop
-- of 10,000 separately-committed UPDATEs (the real claim_task() pattern)
-- opens a reclaim window between each commit that the single bulk UPDATE
-- structurally cannot have.
--
-- This script isolates exactly that variable: it claims each row with its
-- own committed transaction (via a PL/pgSQL PROCEDURE looping with an
-- explicit COMMIT per iteration -- DO blocks cannot COMMIT, only
-- procedures invoked with CALL can, since Postgres 11), while leaving the
-- claim-shaped SET clause, row count, and queue/state seeding identical to
-- claim_update_bloat_corroboration.sql, so the *only* thing that changes
-- between the two scripts is the transaction boundary the finding is about.
--
-- Run ONLY against a disposable scratch database with autumn-harvest's
-- Diesel migrations applied -- NEVER a real development, staging, or
-- production database. It TRUNCATEs and reseeds harvest_task_queue twice
-- in place, and leaves behind a throwaway PROCEDURE this script itself
-- drops at the end.
--
--   createdb -h localhost -U postgres harvest_perf_scratch
--   DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
--   (cd autumn-harvest && diesel migration run)
--   psql "$DATABASE_URL" -f docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_separate_transactions_corroboration.sql
--
-- pg_relation_size is a pure catalog/storage read, not an EXPLAIN estimate
-- -- admissible evidence under this repo's performance-measurement
-- discipline (docs/performance.md). No manual VACUUM runs between the
-- pre- and post-loop measurements in either state -- this deliberately
-- lets ordinary autovacuum activity (if it fires during the loop's real
-- wall-clock runtime) participate exactly as it would in production,
-- which is the whole point of testing the separate-transaction pattern.
\set ON_ERROR_STOP on
\timing off

CREATE OR REPLACE PROCEDURE claim_loop_committed(n INT) AS $body$
DECLARE
  i INT;
BEGIN
  FOR i IN 1..n LOOP
    UPDATE harvest_task_queue
    SET state = 'RUNNING',
        worker_id = 'sim-claim-worker',
        started_at = NOW()
    WHERE id = (
      SELECT id FROM harvest_task_queue
      WHERE state = 'PENDING'
      ORDER BY scheduled_at ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    );
    COMMIT;
  END LOOP;
END;
$body$ LANGUAGE plpgsql;

-- State A: no-capabilities. Fresh INSERT, then 10,000 separately-committed
-- claim-shaped UPDATEs, each touching one row, none touching
-- required_capabilities (it is NULL throughout).
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'no-caps-before-claim-separate-txns' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;

CALL claim_loop_committed(10000);
SELECT 'no-caps-after-claim-separate-txns' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;

-- State B: capability-labels. Fresh INSERT with required_capabilities
-- populated at birth, then the identical separately-committed claim loop
-- -- still never touching required_capabilities, but each new tuple
-- version must still carry its ~70 extra bytes forward.
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at, required_capabilities)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second',
       '[{"Exact":{"key":"region","value":"us-east"}}]'::jsonb
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'caps-before-claim-separate-txns' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;

CALL claim_loop_committed(10000);
SELECT 'caps-after-claim-separate-txns' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;

-- cleanup: leave the scratch DB empty again and drop the throwaway helper.
TRUNCATE harvest_task_queue RESTART IDENTITY;
DROP PROCEDURE claim_loop_committed(INT);
