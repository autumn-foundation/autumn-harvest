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
-- A follow-up review finding on PR #1192 (against an earlier revision of
-- this script) correctly caught that this script's doc asserted, in prose,
-- that this five-run, 100,000-claim reproduction reflects opportunistic
-- HOT pruning alone with zero background autovacuum reclamation, without
-- the script itself ever sampling `pg_stat_user_tables.autovacuum_count`,
-- so a regenerated artifact had no way to verify or falsify it. (A later
-- review round further caught that "reflects this condition" is not the
-- same claim as "is therefore a guaranteed floor on the true effect" --
-- see the doc's Write-side cost section for how that distinction is now
-- drawn.)
--
-- A THIRD review round on PR #1192 correctly caught a race the
-- before/after autovacuum_count comparison alone cannot close:
-- pg_stat_user_tables.autovacuum_count is only incremented when an
-- autovacuum RUN on this relation COMPLETES, but VACUUM reclaims space
-- into the free space map (FSM) incrementally, page by page, as it scans
-- -- well before that completion. A worker that STARTED vacuuming
-- harvest_task_queue mid-loop and had not yet finished (hence not yet
-- incremented the counter) by the time the "after" SELECT runs could
-- therefore have already contributed reclaimed pages to the measured
-- after_pages figure while autovacuum_count_before still equals
-- autovacuum_count_after -- silently invalidating the opportunistic-
-- pruning-only classification the equal-counter check alone was being
-- used to prove.
--
-- Rather than try to detect that race after the fact (e.g. polling
-- pg_stat_progress_vacuum, itself timing-fragile -- a worker that starts
-- AND finishes between two polls is invisible to it), this script now
-- closes it by construction: the `ALTER TABLE harvest_task_queue SET
-- (autovacuum_enabled = false)` below disables autovacuum for this one
-- table for the duration of the measurement, so no autovacuum run against
-- it -- completed, in progress, or not yet started -- can occur at all
-- while the loop runs, regardless of timing. (A prior-run autovacuum
-- worker already mid-scan on this table at the instant that ALTER TABLE
-- executes is not itself a further gap: the very next statement inside
-- the loop's PROCEDURE is a TRUNCATE, which requires an
-- AccessExclusiveLock that conflicts with autovacuum's lock --
-- PostgreSQL autovacuum workers self-cancel rather than block a
-- conflicting lock request, so any such worker is force-terminated
-- before the first claim UPDATE in the loop ever runs.) The two
-- `SELECT`s immediately before and after the `CALL` below are retained as
-- a secondary sanity check only (e.g. against a manual `VACUUM` issued by
-- some other session sharing this scratch database), not as the primary
-- proof: with autovacuum disabled on the table, they are expected -- and
-- required -- to always read equal.
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
-- on. Its absence, combined with the autovacuum-disable above, keeps this
-- measurement scoped to opportunistic pruning alone: with no VACUUM of
-- any kind able to touch this table while the loop runs, every byte of
-- space reclaimed here comes solely from the opportunistic HOT pruning
-- triggered in-line by the claim loop's own commits.
\set ON_ERROR_STOP on
\timing off

-- Disable autovacuum on this one table for the duration of the
-- measurement below (reset at cleanup, near the end of this script). This
-- is what makes the opportunistic-pruning-only condition hold by
-- construction rather than by observation of the autovacuum_count SELECTs
-- further down -- see the header comment above for the review finding
-- this closes.
ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false);

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

-- autovacuum_count on harvest_task_queue immediately BEFORE the five-run
-- loop below. With autovacuum disabled on this table (the ALTER TABLE ...
-- SET (autovacuum_enabled = false) above), the opportunistic-pruning-only
-- condition described in docs/performance-capability-labels.md's
-- Write-side cost section holds by construction, regardless of what this
-- counter reads -- no autovacuum run against this table, completed or
-- still in progress, can occur while autovacuum is disabled on it. This
-- SELECT and the matching "after" query (following the CALL, below) are
-- retained only as a secondary sanity check (e.g. against a manual
-- VACUUM issued by some other session sharing this scratch database, or
-- against a bug in the isolation itself), not as the primary proof. A
-- nonzero value here is not itself a problem: the counter is cumulative,
-- and a scratch database reused across multiple runs of this script (or
-- any other prior activity) can leave this baseline above zero. With
-- autovacuum disabled, this and the "after" value are expected -- and
-- required -- to read equal.
SELECT 'autovacuum_count_before' AS label, autovacuum_count
FROM pg_stat_user_tables
WHERE relname = 'harvest_task_queue';

CALL run_separate_txn_claim_bloat(5, 10000);

-- autovacuum_count on harvest_task_queue immediately AFTER the five-run
-- loop. TRUNCATE does not reset this counter (it resets the table's
-- storage/relfilenode, not the cumulative pg_stat_user_tables row for the
-- relation's OID), so this value is directly comparable to the "before"
-- value above. With autovacuum disabled on this table for the duration of
-- the loop (see the ALTER TABLE above), this is expected -- and required
-- -- to equal autovacuum_count_before: every byte of space reclaimed
-- above comes solely from opportunistic HOT pruning triggered in-line by
-- the claim loop's own commits, by construction, not merely by
-- observation. If this value is instead GREATER than
-- autovacuum_count_before, autovacuum somehow ran against this table
-- despite being disabled on it -- an unexpected condition worth
-- investigating on its own, since it means the isolation this script
-- relies on failed for this run, not that the +8.3% figure includes
-- background reclamation as a valid alternate measurement.
SELECT 'autovacuum_count_after' AS label, autovacuum_count
FROM pg_stat_user_tables
WHERE relname = 'harvest_task_queue';

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

-- cleanup: leave the scratch DB empty again, drop the throwaway helpers,
-- and re-enable autovacuum on harvest_task_queue so any other script run
-- against this same scratch database afterward (e.g.
-- claim_update_bloat_corroboration.sql, or another run of this script)
-- is not left with autovacuum silently disabled on the table.
TRUNCATE harvest_task_queue RESTART IDENTITY;
ALTER TABLE harvest_task_queue RESET (autovacuum_enabled);
DROP PROCEDURE run_separate_txn_claim_bloat(INT, INT);
DROP TABLE claim_bloat_runs;
