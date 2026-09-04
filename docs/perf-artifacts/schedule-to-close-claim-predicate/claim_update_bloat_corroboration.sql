-- Corroboration for docs/performance-schedule-to-close.md's "Write-side
-- cost" section, mirroring
-- capability-labels-claim-predicate/claim_update_bloat_corroboration.sql
-- in method, with one addition: this script snapshots BOTH the heap
-- (`harvest_task_queue`) AND the partial index
-- (`harvest_task_queue_schedule_to_close_idx`, migration 20260606000001)
-- separately, rather than the heap alone. Codex review on PR #1339 (P2)
-- correctly flagged that an earlier revision of this script (heap-only)
-- could not support the doc's claim that it corroborates a *combined*
-- row-width-plus-index-write effect: `pg_relation_size('harvest_task_queue')`
-- excludes every index relation by definition, so a heap-only snapshot is
-- evidence for the row-width component alone, never for the index-write
-- component the doc's "Plan" section separately derives from
-- EXPLAIN-reported `dirtied`/`written` deltas. This revision measures both
-- components directly and independently, rather than asserting the index
-- component from EXPLAIN evidence alone.
--
-- Every UPDATE to a task row -- including the claim UPDATE in
-- claim_task_query()'s `claimed` CTE, which never touches
-- `schedule_to_close_at` itself -- still creates a brand-new MVCC tuple
-- version that carries the full, unchanged column value forward, and (for
-- schedule-to-close rows only, since they are the only ones ever a member
-- of the partial index) still needs a new index entry pointing at that new
-- tuple. This measures both costs directly, by isolating the heap-page AND
-- index-page growth caused by ONE UPDATE pass simulating a claim (SET
-- state, worker_id, started_at -- never schedule_to_close_at) applied to an
-- otherwise-clean, freshly-INSERTed 10,000-row table in each data state.
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
--   psql "$DATABASE_URL" -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.sql
--
-- pg_relation_size is a pure catalog/storage read, not an EXPLAIN estimate --
-- admissible evidence under this repo's performance-measurement discipline
-- (docs/performance.md). No VACUUM runs between the pre- and post-UPDATE
-- measurements in either state, matching the real window between a claim
-- and whenever autovacuum next runs in production.
\set ON_ERROR_STOP on
\timing off

-- State A: no-schedule-to-close. Fresh INSERT, then a claim-shaped UPDATE
-- that never touches schedule_to_close_at (it is NULL throughout, so these
-- rows are never members of harvest_task_queue_schedule_to_close_idx).
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'no-stc-before-claim' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages
  \gset no_stc_before_

UPDATE harvest_task_queue
SET state = 'RUNNING',
    worker_id = 'sim-claim-worker',
    started_at = NOW()
WHERE state = 'PENDING';
SELECT 'no-stc-after-claim' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages
  \gset no_stc_after_

-- State B: schedule-to-close. Fresh INSERT with schedule_to_close_at
-- populated at birth (100 years in the future -- far enough out that it
-- never elapses no matter how long this script takes to run; matches the
-- Rust harness's seeded value exactly -- see the note on
-- `SCHEDULE_TO_CLOSE_SQL` in claim_budget_tests.rs for why 'infinity' was
-- tried and rejected there), then the identical claim-shaped UPDATE --
-- still never touching schedule_to_close_at, but each new tuple version
-- must still carry its 8 extra heap bytes AND a new
-- harvest_task_queue_schedule_to_close_idx entry forward.
TRUNCATE harvest_task_queue RESTART IDENTITY;
INSERT INTO harvest_task_queue
  (queue_name, task_type, activity_name, activity_id, input, state,
   priority, max_attempts, scheduled_at, schedule_to_close_at)
SELECT 'q-' || (i % 4), 'activity', 'bench_activity', gen_random_uuid(),
       '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second',
       NOW() + INTERVAL '100 years'
FROM generate_series(0, 9999) AS s(i);
VACUUM ANALYZE harvest_task_queue;
SELECT 'stc-before-claim' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages
  \gset stc_before_

UPDATE harvest_task_queue
SET state = 'RUNNING',
    worker_id = 'sim-claim-worker',
    started_at = NOW()
WHERE state = 'PENDING';
SELECT 'stc-after-claim' AS label,
       pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages;
SELECT pg_relation_size('harvest_task_queue') / 8192 AS heap_pages,
       pg_relation_size('harvest_task_queue_schedule_to_close_idx') / 8192 AS idx_pages
  \gset stc_after_

-- Derived summary -- computed and printed by the script itself, so
-- redirecting this script's output to the committed .txt artifact captures
-- the analysis, not just the eight raw before/after measurements above.
-- Heap and index growth are reported SEPARATELY, never summed into one
-- percentage, so neither component's evidence can misrepresent the other's.
SELECT
  (:no_stc_after_heap_pages - :no_stc_before_heap_pages) AS no_stc_heap_delta_pages,
  (:stc_after_heap_pages - :stc_before_heap_pages) AS stc_heap_delta_pages,
  ((:stc_after_heap_pages - :stc_before_heap_pages)
     - (:no_stc_after_heap_pages - :no_stc_before_heap_pages)) AS extra_heap_pages,
  round(
    100.0 * ((:stc_after_heap_pages - :stc_before_heap_pages)
              - (:no_stc_after_heap_pages - :no_stc_before_heap_pages))
    / (:no_stc_after_heap_pages - :no_stc_before_heap_pages),
    1
  ) AS extra_heap_pct_more_growth_from_schedule_to_close;

SELECT
  (:no_stc_after_idx_pages - :no_stc_before_idx_pages) AS no_stc_idx_delta_pages,
  (:stc_after_idx_pages - :stc_before_idx_pages) AS stc_idx_delta_pages;

-- cleanup: leave the scratch DB empty again.
TRUNCATE harvest_task_queue RESTART IDENTITY;
