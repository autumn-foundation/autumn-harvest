# ⛏️ Prospect pre-registration: does a partial-index rewrite fix the concurrency gate's high-cardinality cost? (assay ledger #3)

**Committed:** 2026-09-04T09:07:37Z, before any apparatus was built or measurement taken.
This document is the contract; the report that follows it is graded against
these lines, not against whatever the numbers turn out to be.

## 🎯 Question

`docs/performance.md` ("Known limitation: cost scales with distinct
concurrency-key cardinality", lines 780-855) documents that the committed
concurrency-key claim-gate fix (issue #247) degrades badly outside its tested
range: at 5,000 distinct concurrency keys (vs. the committed 256-key
fixture), a hot-contention claim costs **1,600ms**, worse than the pre-fix
baseline (1,573.3ms) it was meant to improve on. The doc names one
untested candidate fix and explicitly leaves it open: a correlated subquery
in the same shape as the `claimed` CTE's authoritative recheck (which already
scans the base table, not a CTE, and is cheap because it runs once per claim)
— "needs a new supporting partial index (e.g. on `(concurrency_key,
task_type) WHERE state = 'RUNNING' AND worker_id IS NOT NULL)` ... Adding an
index is outside what this PR changes unilaterally; see the review discussion
on this PR for the concrete proposal and open question."

**Falsifiable question:** does replacing the candidate-side gate's correlated
subquery against `concurrency_running_counts` (a `MATERIALIZED` CTE — no
index, linear `CTE Scan` per candidate row) with a correlated subquery
against `harvest_task_queue` itself, backed by a new partial index
`ON harvest_task_queue (concurrency_key, task_type) WHERE state = 'RUNNING'
AND worker_id IS NOT NULL`, simultaneously:

1. keep the current fix's idle-case near-zero cost (the committed fixture
   shows ~1-4 buffers at a 10,000-row/256-key idle backlog), and
2. fix the high-cardinality hot-contention blowup (1,600ms at 5,000
   keys/2,000 RUNNING rows), and
3. not regress the already-good 256-key hot-contention case

— the same three-way bar the doc's own three rejected rewrites (`LEFT JOIN`,
`LEFT JOIN LATERAL`, and the join-hints variants) all failed, each for a
different one of these three properties.

## 👤 Decision this feeds

Whether to land the partial-index + base-table-correlated-subquery rewrite as
a follow-up fix to the concurrency-key claim gate (issue #247 follow-up),
closing the "known limitation" section of `docs/performance.md` and lifting
its caveat that deployments with concurrency-key cardinality in the
thousands are unsupported by the fix's evidence.

**Decider:** whoever owns the queue/claim-path performance work — the same
reviewer thread `docs/performance.md:849-851` points at ("see the review
discussion on this PR for the concrete proposal and open question"). This
assay does not decide; it produces the missing numbers that discussion was
waiting on.

## ⚖️ Success / kill criteria (numeric, set now)

All measured with `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`
against one from-scratch apparatus, same machine, same run, control and
candidate built from the identical fixture generator so the comparison is
same-harness (not a cross-machine citation of the existing published
numbers, which are used only as a sanity cross-check).

Three lines, all three must clear for a **pursue** verdict; any one miss is
**kill**:

- **L1 — idle case must not regress.** 10,000-row backlog, 4 queues, 256
  distinct concurrency keys, zero RUNNING rows: candidate's total buffer
  count for `claim_task_query()`'s concurrency-gate-bearing plan must stay
  **≤ 50 buffers**. (Control/committed fix measures ~1-4; the three rejected
  `LEFT JOIN`-family rewrites landed at 230-916 buffers by defeating the
  `ORDER BY … LIMIT` pushdown through `idx_harvest_tq_poll` — 50 is a
  generous line that still clearly separates "keeps the pushdown" from
  "loses it.")
- **L2 — high-cardinality hot-contention must clear a 10x margin.** 10,000-row
  backlog, 4 queues, 5,000 distinct concurrency keys, 2,000 RUNNING rows
  spread across those same keys (mirrors
  `claim_budget_tests.rs`'s `HOT_CONTENTION_ROWS` shape exactly): candidate's
  wall-clock **must be ≤ 160ms** (10x faster than the documented 1,600ms).
- **L3 — 256-key hot-contention must not regress.** Same 2,000-RUNNING-row
  shape at the committed 256-key cardinality: candidate's wall-clock must be
  **≤ 2x** whatever the control measures on this apparatus in the same run
  (a fresh, same-harness number, not the old published one, since this
  assay's control run is what L3 is graded against).

## ⏱️ Conditions

- Postgres 16 (matches the crate's tested target), local, default
  `postgres.conf`, `ANALYZE` run after every bulk seed (as
  `claim_bench_support.rs` does) so the planner works from live statistics.
- Fixture shapes ported by value from `claim_bench_support.rs::seed_backlog`
  (concurrency key format `'bench-ck-' || (i % KEY_CARDINALITY)`,
  `NON_BLOCKING_CAP`) and `claim_budget_tests.rs`'s hot-contention seed
  (2,000 RUNNING rows, `worker_id` set, spread round-robin across the
  backlog's own distinct keys) — reused rather than reinvented, since a
  fixture-shape mismatch is exactly what cost assay #2 a review round.
- Apparatus query is a **minimal** stand-in for `candidate`'s WHERE/ORDER BY:
  only the columns and clauses that bear on the concurrency gate and the
  `idx_harvest_tq_poll`-driven ordering are included (`queue_name`, `state`,
  `scheduled_at`, `priority`, `concurrency_key`, `task_type`,
  `concurrency_cap`, `worker_id`, `id`). Every other gate in the real query
  (build routing, sticky pin, rate limit, capability labels, workflow-pause,
  activity-pause) is cut, not faked with a stub predicate — it is orthogonal
  to the question and would only add planner noise. This is recorded on the
  stubs list, not hidden.

## 🧨 Riskiest assumption, attacked first

That a correlated subquery against the base table, filtered by
`WHERE state = 'RUNNING' AND worker_id IS NOT NULL`, actually gets planned as
an index probe against the new partial index (not a scan) **and** that using
a correlated subquery rather than a JOIN — the same shape property that made
the *original pre-fix* predicate get the `LIMIT` pushdown the doc says JOINs
categorically cannot get in this planner — still gets that pushdown once the
subquery's target is an indexed base-table lookup instead of an unindexed
CTE. If this fails, none of the rest of the apparatus matters, so it is
measured first, before the 5,000-key run.

## 🎛️ Control

The current committed fix's exact query shape — the `concurrency_running_counts`
`MATERIALIZED` CTE + correlated subquery against it — run on the identical
apparatus, fixtures, and machine as the candidate, in the same session. The
already-published `docs/performance.md` numbers are cited only as a
cross-machine sanity check, not substituted for this run.

## 📦 Containment

Local, non-production Postgres 16 (`pg_lsclusters` showed it stopped;
started for this assay as a local dev service, not shared infra), one
scratch database created and dropped within the assay, throwaway SQL
apparatus archived under `docs/assays/apparatus/0003-concurrency-gate-cardinality-index/`
after the run. No migration is added to `autumn-harvest/migrations/`; no
crate code changes. The prototype does not merge regardless of verdict.

## 💵 Budget / time box

$0 spend (local only). Time box: same session, target under 3 hours from this
commit to verdict. No extension without an explicit re-charter.

## 🔍 Prior art already checked

- `docs/performance.md:780-855` — names the exact candidate fix and states it
  is untested ("Adding an index is outside what this PR changes
  unilaterally").
- `docs/assays/README.md` (#1, #2) — both prior Prospect assays are the
  unrelated Redis question; this is not a re-dig.
- Repo search for the proposed index
  (`(concurrency_key, task_type) WHERE state = 'RUNNING' AND worker_id IS NOT
  NULL`) and for any prior benchmark of it: not present in
  `autumn-harvest/migrations/` (only `idx_harvest_tq_poll`,
  `idx_harvest_tq_running` on `(state, last_heartbeat_at) WHERE state =
  'RUNNING'`, `idx_harvest_tq_workflow`, `idx_harvest_tq_sticky_poll` exist),
  not present in `docs/assays/apparatus/`. No existing measurement to close
  this from reading alone — apparatus is warranted.
