# ⛏️ Prospect: does a partial index fix the concurrency gate's high-cardinality cost? (kill: 20,137 vs 50-buffer idle line, ledger #3)

> Status: **measured.** The pre-registration
> (`docs/rnd/2026-09-04-concurrency-gate-cardinality-index-preregistration.md`,
> commit `d6976b2`) was committed before the apparatus was built or run;
> nothing in it has been edited since. This report is the Apparatus, Assay,
> Verdict and Reproduce sections appended afterward, with the actual numbers.

## 🎯 Question

`docs/performance.md` ("Known limitation: cost scales with distinct
concurrency-key cardinality", lines 780-855) documents that the committed
concurrency-key claim-gate fix (issue #247) degrades outside its tested
256-key fixture: at 5,000 distinct concurrency keys, a hot-contention claim
measured **1,600ms**, worse than the pre-fix baseline (1,573.3ms) it was
meant to improve on. The doc names one untested candidate fix and leaves it
explicitly open: replace the correlated subquery against
`concurrency_running_counts` (a `MATERIALIZED` CTE — no index, linear `CTE
Scan` per candidate row) with a correlated subquery against
`harvest_task_queue` itself, in the same shape as the `claimed` CTE's
authoritative recheck, backed by a new partial index `ON harvest_task_queue
(concurrency_key, task_type) WHERE state = 'RUNNING' AND worker_id IS NOT
NULL`. *"Adding an index is outside what this PR changes unilaterally; see
the review discussion on this PR for the concrete proposal and open
question."*

**Falsifiable question:** does that rewrite simultaneously (1) keep the
current fix's near-zero idle-case cost, (2) fix the high-cardinality
hot-contention blowup, and (3) not regress the already-good 256-key
hot-contention case — the same three-way bar the doc's three already-
rejected rewrites (`LEFT JOIN`, `LEFT JOIN LATERAL`, and the join-hint
variants) each failed on a different one of these three properties?

**Decision this feeds:** whether to land the partial-index rewrite as a
follow-up fix to the concurrency-key claim gate, closing the "known
limitation" section and lifting its caveat that thousands-of-keys
deployments are unsupported by the fix's evidence. **Decider:** whoever owns
the queue/claim-path performance work — the reviewer thread
`docs/performance.md:849-851` points at.

## ⚖️ Pre-registration

Committed in full at
[`docs/rnd/2026-09-04-concurrency-gate-cardinality-index-preregistration.md`](../rnd/2026-09-04-concurrency-gate-cardinality-index-preregistration.md)
(`d6976b2`). Summary of the three lines, all three required for **pursue**,
any one miss is **kill**:

- **L1 (idle, must not regress):** 10,000-row backlog, 4 queues, 256 keys,
  zero RUNNING rows — candidate's total buffer count ≤ **50 buffers**.
- **L2 (high cardinality, must clear 10x):** same shape at 5,000 keys, 2,000
  RUNNING rows spread across those keys — candidate wall-clock ≤ **160ms**
  (10x the documented 1,600ms).
- **L3 (256-key hot-contention, must not regress):** same 2,000-RUNNING-row
  shape at 256 keys — candidate wall-clock ≤ **2x** this apparatus's own
  same-run control measurement.

Riskiest assumption, registered to be attacked first: that a correlated
subquery against an *indexed* base table both (a) gets planned as an index
probe, and (b) still lets the planner push `ORDER BY … LIMIT` through
`idx_harvest_tq_poll` the way a correlated subquery against the (unindexed)
CTE currently does not need to.

## 🔍 Prior art

- `docs/performance.md:780-855` names the exact candidate fix and states it
  is untested.
- `docs/assays/README.md` #1/#2 are the unrelated Redis question — not a
  re-dig.
- No existing index matching `(concurrency_key, task_type) WHERE state =
  'RUNNING' AND worker_id IS NOT NULL` in `autumn-harvest/migrations/`, and
  no prior benchmark of it under `docs/assays/apparatus/`. Apparatus was
  warranted.

## 🧪 Apparatus

`docs/assays/0003-concurrency-gate-cardinality-index/` (archived): a minimal
Postgres 16 schema (`schema.sql`) carrying only the columns and the one index
(`idx_harvest_tq_poll`) that bear on the question, a fixture generator
(`seed.sql`) ported by value from `claim_bench_support.rs::seed_backlog`
(PENDING backlog, key format, `NON_BLOCKING_CAP`) and
`claim_budget_tests.rs`'s `HOT_CONTENTION_ROWS` seed (2,000 RUNNING rows
round-robin across the backlog's own distinct keys), and two query variants
(`control.sql` — the committed fix's shape; `candidate.sql` — the proposed
rewrite, gated behind `candidate_index.sql`'s new partial index). Run via
`run_assay.sh` against a local, non-production Postgres 16 instance.

**Stubs list (what was faked/cut, on purpose):**

- `worker_info`, `paused_queues`, `paused_activities`, build-routing,
  workflow-pause, rate-limit, and capability-label clauses — all cut
  entirely, not stubbed, since they are orthogonal to the concurrency gate
  and would only add planner noise the pre-registration's own "Conditions"
  section calls out.
- No `claimed` CTE / `UPDATE` — only the `candidate` selection is measured
  (`SELECT … LIMIT 1 FOR UPDATE SKIP LOCKED`, no write), since that is the
  subtree `docs/performance.md`'s own analysis attributes the cost to.
- Single scenario family (10,000-row backlog, 4 queues) — the pre-registered
  scope; not swept across 1,000/100,000 the way `docs/performance.md`'s
  committed table is, since L1/L2/L3 are registered at this one depth only.

## 📊 Assay

All eight runs (`docs/assays/apparatus/0003-concurrency-gate-cardinality-index/results/`),
`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`, same machine,
same apparatus, same session:

| scenario | keys | running | variant | execution time | top-level buffers |
|:--|--:|--:|:--|--:|--:|
| idle_256 | 256 | 0 | control | 8.168 ms | 270 |
| idle_256 | 256 | 0 | **candidate** | 14.944 ms | **20,137** |
| hot_256 | 256 | 2,000 | control | 162.128 ms | 858 |
| hot_256 | 256 | 2,000 | **candidate** | 45.799 ms | 98,670 |
| hot_5000 | 5,000 | 2,000 | control | 1,184.364 ms | 882 |
| hot_5000 | 5,000 | 2,000 | **candidate** | 24.396 ms | 24,558 |
| idle_5000 | 5,000 | 0 | control | 7.911 ms | 278 |
| idle_5000 | 5,000 | 0 | candidate (not gated) | 18.077 ms | 20,141 |

**Riskiest assumption, checked first:** half confirmed, half refuted. (a) The
new partial index *is* used efficiently per call — every candidate run shows
`Index Only Scan using idx_harvest_tq_concurrency_running`, cost ~8 per
probe, `Heap Fetches: 0` in the idle case. (b) The `ORDER BY … LIMIT`
pushdown through `idx_harvest_tq_poll` does **not** survive the rewrite: in
every candidate run, the plan is `Limit → LockRows → Sort → {Seq,Index}
Scan`, with the scan's `actual rows=10000 loops=1` — the full backlog is
read and explicitly sorted before `LIMIT 1` applies, for every scenario,
including the two idle ones where nothing downstream should have needed
more than a handful of rows. Postgres's planner does not treat "correlated
subquery against an indexed base table" as cheap enough per call to justify
choosing the ordered-scan-with-early-stop plan the way it apparently does (in
this apparatus) when the subquery targets a small `MATERIALIZED` CTE
instead — even though the per-call *cost estimate* (8.14) is comparable to
the CTE-form's genuinely-tiny per-call cost when that CTE is empty.

**Against the lines:**

- **L1 — FAIL.** Candidate's idle_256 buffer count is **20,137**, against a
  ≤50 line — 403x over. This is not close, and it is not a cardinality
  artifact: idle_5000 (5,000 keys, still zero RUNNING rows) shows the
  identical 20,141 buffers, confirming the cost comes from re-evaluating the
  per-row index probe 10,000 times regardless of how many keys exist, not
  from key cardinality. This is the mechanism the pre-registration flagged
  as the riskiest assumption, and it is the one that broke.
- **L2 — PASS, decisively.** Candidate's hot_5000 wall-clock is **24.396ms**,
  against a ≤160ms line — comfortably inside, and a **48.6x** speedup over
  this apparatus's own same-run control (1,184.364ms), well past the 10x
  bar. This is the number the doc's open question was actually asking about,
  and the partial index does fix it.
- **L3 — PASS.** Candidate's hot_256 wall-clock is **45.799ms** against a
  ≤324.256ms line (2x control's 162.128ms) — not just inside the line, faster
  than control outright.

**Control comparison, always:** every candidate number above is read against
a control measured on the identical apparatus, same session — not against
the older cross-machine numbers in `docs/performance.md` (those are cited
only as the source of the L1/L2 pre-set lines, per the pre-registration).

**Worst case:** the apparatus's own control run does not reproduce
`docs/performance.md`'s published "~1-4 buffers" idle-case figure for the
committed fix either (this apparatus measures 270-278) — the full production
query's extra joins/columns and the doc's own larger backlog sweep (up to
100,000 rows) evidently matter to whether Postgres chooses the early-stop
plan at all, a detail this minimal apparatus does not resolve either way.
That gap does not change today's verdict — L1's line was set as an absolute
threshold precisely because a same-apparatus recalibration wasn't available
going in — but it does mean this apparatus cannot independently corroborate
the doc's own control number, only the relative candidate-vs-line and
candidate-vs-control comparisons it was built to make.

## 🏁 Verdict

**Kill**, on L1: 20,137 buffers vs. a 50-buffer line. Per the pre-registered
rule, one miss among three is a kill regardless of the other two clearing
by wide margins.

The partial-index + base-table-correlated-subquery rewrite **does** solve
the specific problem `docs/performance.md` measured (high-cardinality
hot-contention: 48.6x faster than control, well past the 10x bar) and does
so **without** the 256-key hot-contention regression the doc's three
previously-rejected `LEFT JOIN`-family rewrites all shared. But it trades
that fix for a new, more universal one: it loses the cheap idle-case
short-circuit *unconditionally* — the regression shows up at zero RUNNING
rows regardless of key cardinality (256 or 5,000), which is the common,
steady-state case this table is in "empty in steady state" language
elsewhere in the same doc describes for comparable predicates. A fix that
is fast exactly when the system is under contention and slow exactly when it
is idle is not the shape the open question was hoping to close.

This is evidence, not a proof that no fix exists: it kills *this specific*
formulation (correlated subquery against `harvest_task_queue` filtered by
the new partial index), the one the doc's review thread proposed and left
open. It does not test other shapes (e.g., a `LATERAL` join with hints
specifically defeating the planner's Sort-before-Limit choice, or a
recheck-only strategy that defers the expensive gate to the already-cheap
`claimed` CTE and accepts more `pg_try_advisory_xact_lock` contention at the
gate itself) — those remain open, un-re-chartered pits.

## 🔬 Reproduce

```
sudo -u postgres createdb prospect_assay3   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0003-concurrency-gate-cardinality-index
PGHOST=/var/run/postgresql PGDATABASE=prospect_assay3 ./run_assay.sh
grep "Execution Time" results/*.explain.txt
```

`schema.sql`, `seed.sql`, `control.sql`, `candidate_index.sql`, and
`candidate.sql` are archived alongside `run_assay.sh` in this directory,
along with the full `results/*.explain.txt` output this report's table is
drawn from. No migration was added to `autumn-harvest/migrations/`; no
crate code changed. The prototype does not merge.
