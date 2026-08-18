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
-- between the two scripts is the transaction boundary the finding is
-- about.
--
-- Because the separate-transaction pattern's growth depends on
-- autovacuum/opportunistic-pruning timing (unlike the single-transaction
-- script, which has no window for either to act at all), the seed+claim
-- cycle below is repeated N_RUNS times inside ONE procedure invocation,
-- and a final query derives the min/max/mean directly from the per-run
-- results table -- so one `psql -f` invocation reproduces both the
-- individual runs and the aggregate range/mean cited in the doc, with no
-- separate manual repetition or hand-computed statistics step required
-- (a review finding on PR #1192 caught that gap: the previous version of
-- this script measured one run per invocation and the multi-run range
-- committed in this artifact was computed by hand from five separate
-- invocations, which the documented single-command reproduction procedure
-- could not itself regenerate).
--
-- Run ONLY against a disposable scratch database with autumn-harvest's
-- Diesel migrations applied -- NEVER a real development, staging, or
-- production database. It TRUNCATEs and reseeds harvest_task_queue
-- 2 * N_RUNS times in place, and leaves behind a throwaway PROCEDURE and
-- TEMP TABLE this script itself drops at the end.
--
--   createdb -h localhost -U postgres harvest_perf_scratch
--   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
--   (cd autumn-harvest && diesel migration run)
--   psql "$DATABASE_URL" -f docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_separate_transactions_corroboration.sql
--
-- pg_relation_size is a pure catalog/storage read, not an EXPLAIN estimate
-- -- admissible evidence under this repo's performance-measurement
-- discipline (docs/performance.md). No VACUUM runs between the pre- and
-- post-loop measurements in either state -- PostgreSQL disallows VACUUM
-- inside any function or procedure body under any circumstances ("VACUUM
-- cannot be executed from a function"), so the loop below cannot invoke it
-- even if it wanted to. This was checked to not compromise the "before"
-- measurement: running VACUUM ANALYZE immediately after an equivalent
-- fresh bulk INSERT (before any claims) leaves pg_relation_size unchanged,
-- because a freshly-inserted table has no dead tuples for VACUUM to act
-- on. Its absence also deliberately lets ordinary autovacuum activity (if
-- it fires during the loop's real wall-clock runtime) participate exactly
-- as it would in production, which is the whole point of testing the
-- separate-transaction pattern.
\set ON_ERROR_STOP on
\timing off

CREATE TEMP TABLE claim_bloat_runs (
  run_no INT,
  variant TEXT,
  before_pages INT,
  after_pages INT
);

CREATE OR REPLACE PROCEDURE run_separate_txn_claim_bloat(n_runs INT, n_rows INT) AS $body$
DECLARE
  r INT;
  i INT;
  before_pages INT;
  after_pages INT;
BEGIN
  FOR r IN 1..n_runs LOOP
    -- Variant A: no-capabilities. Fresh INSERT, then n_rows
    -- separately-committed claim-shaped UPDATEs, none touching
    -- required_capabilities (it is NULL throughout).
    TRUNCATE harvest_task_queue RESTART IDENTITY;
    INSERT INTO harvest_task_queue
      (queue_name, task_type, activity_name, activity_id, input, state,
       priority, max_attempts, scheduled_at)
    SELECT 'q-' || (gs % 4), 'activity', 'bench_activity', gen_random_uuid(),
           '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second'
    FROM generate_series(0, n_rows - 1) AS s(gs);
    -- No VACUUM here: PostgreSQL disallows VACUUM inside any function or
    -- procedure body under any circumstances ("VACUUM cannot be executed
    -- from a function"), so it cannot run inside this loop at all -- and it
    -- would not change this measurement anyway. Verified directly: running
    -- VACUUM ANALYZE immediately after an equivalent fresh bulk INSERT left
    -- pg_relation_size unchanged (213 pages before and after), because a
    -- freshly-inserted table has zero dead tuples for VACUUM to reclaim or
    -- truncate -- its only page-count-changing effect (truncating
    -- all-empty trailing pages) has nothing to act on yet.
    SELECT (pg_relation_size('harvest_task_queue') / 8192)::INT
      INTO before_pages;

    FOR i IN 1..n_rows LOOP
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

    SELECT (pg_relation_size('harvest_task_queue') / 8192)::INT
      INTO after_pages;
    INSERT INTO claim_bloat_runs VALUES (r, 'no-caps', before_pages, after_pages);
    COMMIT;

    -- Variant B: capability-labels. Fresh INSERT with required_capabilities
    -- populated at birth, then the identical separately-committed claim
    -- loop -- still never touching required_capabilities, but each new
    -- tuple version must still carry its ~70 extra bytes forward.
    TRUNCATE harvest_task_queue RESTART IDENTITY;
    INSERT INTO harvest_task_queue
      (queue_name, task_type, activity_name, activity_id, input, state,
       priority, max_attempts, scheduled_at, required_capabilities)
    SELECT 'q-' || (gs % 4), 'activity', 'bench_activity', gen_random_uuid(),
           '{}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '1 second',
           '[{"Exact":{"key":"region","value":"us-east"}}]'::jsonb
    FROM generate_series(0, n_rows - 1) AS s(gs);
    -- See the no-caps variant above for why VACUUM is omitted here too.
    SELECT (pg_relation_size('harvest_task_queue') / 8192)::INT
      INTO before_pages;

    FOR i IN 1..n_rows LOOP
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

    SELECT (pg_relation_size('harvest_task_queue') / 8192)::INT
      INTO after_pages;
    INSERT INTO claim_bloat_runs VALUES (r, 'capability-labels', before_pages, after_pages);
    COMMIT;
  END LOOP;
END;
$body$ LANGUAGE plpgsql;

CALL run_separate_txn_claim_bloat(5, 10000);

-- Per-run detail, as captured.
SELECT run_no, variant, before_pages, after_pages,
       (after_pages - before_pages) AS delta_pages
FROM claim_bloat_runs
ORDER BY run_no, variant;

-- Derived summary -- computed and printed by the script itself (from the
-- per-run table above), so redirecting this script's output to the
-- committed .txt artifact captures the full analysis, including the
-- multi-run range/mean, not just the raw per-run page counts.
WITH per_run AS (
  SELECT run_no,
         MAX(delta_pages) FILTER (WHERE variant = 'no-caps') AS no_caps_delta,
         MAX(delta_pages) FILTER (WHERE variant = 'capability-labels') AS caps_delta
  FROM (
    SELECT run_no, variant, (after_pages - before_pages) AS delta_pages
    FROM claim_bloat_runs
  ) t
  GROUP BY run_no
),
per_run_pct AS (
  SELECT run_no, no_caps_delta, caps_delta,
         100.0 * (caps_delta - no_caps_delta) / no_caps_delta AS extra_pct
  FROM per_run
)
SELECT
  COUNT(*) AS runs,
  MIN(no_caps_delta) AS no_caps_delta_min,
  MAX(no_caps_delta) AS no_caps_delta_max,
  MIN(caps_delta) AS caps_delta_min,
  MAX(caps_delta) AS caps_delta_max,
  round(MIN(extra_pct), 1) AS extra_pct_min,
  round(MAX(extra_pct), 1) AS extra_pct_max,
  round(AVG(extra_pct), 1) AS extra_pct_mean
FROM per_run_pct;

-- cleanup: leave the scratch DB empty again and drop the throwaway helpers.
TRUNCATE harvest_task_queue RESTART IDENTITY;
DROP PROCEDURE run_separate_txn_claim_bloat(INT, INT);
DROP TABLE claim_bloat_runs;
