-- Second corroboration for docs/performance-schedule-to-close.md's
-- "Write-side cost" section (Codex review, PR #1339: the loop-based result
-- was originally reported without a committed reproducer).
--
-- claim_update_bloat_corroboration.sql applies ONE bulk `UPDATE ... WHERE
-- state = 'PENDING'` touching all 10,000 rows in a single statement. This
-- script instead drains the table one row at a time via 10,000 individual
-- `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` pairs inside a PL/pgSQL
-- loop -- the same per-row claim shape `queue::claim_task_query()` uses in
-- production, as opposed to a single bulk statement. Both are run, on
-- otherwise-identical fresh 10,000-row fixtures, to check whether the
-- claim-shaped access pattern changes the heap-page-growth result the bulk
-- UPDATE measures. It does not (see docs/performance-schedule-to-close.md
-- for the comparison); this script exists so that agreement is reproducible
-- and auditable, not merely asserted.
--
-- Run ONLY against a disposable scratch database with autumn-harvest's
-- Diesel migrations applied -- NEVER a real development, staging, or
-- production database. It TRUNCATEs and reseeds harvest_task_queue twice
-- in place. `psql` runs each statement in its own autocommit transaction,
-- so if a later statement fails, the TRUNCATEs that already ran are NOT
-- rolled back: pointed at a shared database, this irreversibly deletes its
-- queued tasks.
--
--   createdb -h localhost -U postgres harvest_perf_scratch
--   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
--   (cd autumn-harvest && diesel migration run)
--   psql "$DATABASE_URL" -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.sql
--
-- pg_relation_size is a pure catalog/storage read, not an EXPLAIN estimate --
-- admissible evidence under this repo's performance-measurement discipline
-- (docs/performance.md). No VACUUM runs between the pre- and post-drain
-- measurements in either state, matching the real window between a claim
-- and whenever autovacuum next runs in production. Each `DO` block runs as
-- one implicit transaction (psql's default autocommit-per-statement), so
-- the 10,000 individual UPDATEs inside it are not 10,000 separate commits --
-- unlike a real claim workload's drain, which commits each claim
-- independently. That difference is the same one
-- docs/performance-schedule-to-close.md's "Write-side cost" section notes
-- for why this script reproduces cleanly where the real, separately-committed
-- 10,001-call drain does not: no ~15-minute window exists here for
-- autovacuum to run partway through.
\set ON_ERROR_STOP on
\timing off

-- State A: no-schedule-to-close. Fresh INSERT, then a claim-shaped drain:
-- 10,000 individual SELECT ... FOR UPDATE SKIP LOCKED + UPDATE pairs, never
-- touching schedule_to_close_at (it is NULL throughout).
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'no-stc-before-drain' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages \gset no_stc_before_

DO $$
DECLARE
  rid uuid;
BEGIN
  FOR i IN 1..10000 LOOP
    SELECT id INTO rid FROM harvest_task_queue
      WHERE state = 'PENDING' LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN rid IS NULL;
    UPDATE harvest_task_queue
      SET state = 'RUNNING', worker_id = 'sim-claim-worker', started_at = NOW()
      WHERE id = rid;
  END LOOP;
END $$;
SELECT 'no-stc-after-drain' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages \gset no_stc_after_

-- State B: schedule-to-close. Fresh INSERT with schedule_to_close_at
-- populated at birth (one hour in the future -- far enough out that it
-- never elapses during this script), then the identical claim-shaped
-- per-row drain -- still never touching schedule_to_close_at, but each new
-- tuple version must still carry its 8 extra bytes forward.
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at, schedule_to_close_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second',
       NOW() + INTERVAL '1 hour'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'stc-before-drain' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages \gset stc_before_

DO $$
DECLARE
  rid uuid;
BEGIN
  FOR i IN 1..10000 LOOP
    SELECT id INTO rid FROM harvest_task_queue
      WHERE state = 'PENDING' LIMIT 1 FOR UPDATE SKIP LOCKED;
    EXIT WHEN rid IS NULL;
    UPDATE harvest_task_queue
      SET state = 'RUNNING', worker_id = 'sim-claim-worker', started_at = NOW()
      WHERE id = rid;
  END LOOP;
END $$;
SELECT 'stc-after-drain' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages \gset stc_after_

-- Derived summary -- computed and printed by the script itself, so
-- redirecting this script's output to the committed .txt artifact captures
-- the analysis, not just the four raw before/after measurements above.
SELECT
  (:no_stc_after_heap_pages - :no_stc_before_heap_pages) AS no_stc_delta_pages,
  (:stc_after_heap_pages - :stc_before_heap_pages) AS stc_delta_pages,
  ((:stc_after_heap_pages - :stc_before_heap_pages)
     - (:no_stc_after_heap_pages - :no_stc_before_heap_pages)) AS extra_pages,
  round(
    100.0 * ((:stc_after_heap_pages - :stc_before_heap_pages)
              - (:no_stc_after_heap_pages - :no_stc_before_heap_pages))
    / (:no_stc_after_heap_pages - :no_stc_before_heap_pages),
    1
  ) AS extra_pct_more_growth_from_schedule_to_close;

-- cleanup: leave the scratch DB empty again.
TRUNCATE harvest_task_queue RESTART IDENTITY;
