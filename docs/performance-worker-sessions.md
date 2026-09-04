# Worker-sessions claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Known limitations" section named worker sessions
(issue #606), alongside `schedule_to_close` (#378) and sticky routing (#235),
as claim-path predicates left unmeasured by the earlier passes: "cheap inline
column tests, against columns the seed leaves null." PR #1339 closed that gap
for `schedule_to_close`, confirming it cheap (+3.6% to +5.7% buffers, well
under the impact floor), and narrowed the remaining unmeasured set to worker
sessions and sticky routing. This page is the worker-sessions measurement.

The result is **not** the same as `schedule_to_close`'s: this predicate has a
real, moderate-to-large buffer cost -- **+32.9%** at the 10,000-row headline
depth on a single cold claim, corroborated by a real 10,001-call
production-shaped drain at **+22.1%** (same direction, same order of
magnitude -- see [Measurement](#measurement)). The mechanism is the same one
`docs/performance-capability-labels.md` documents: row-width growth from
populating previously-`NULL` columns, compounded by the MVCC cost of
`queue::enqueue()`'s real two-statement write (see
[Workload](#workload)) -- not a plan inefficiency. There is no query-shape
fix, because the `WHERE` clause already evaluates the predicate as a plain
inline test on a row the scan reads regardless. Per the acceptable-outcomes
rule ("optimization PR, findings issue, negative result -- all are successful
runs"), this ships as a measured, reproducible finding with no code change to
`queue.rs`.

## Harness corrections (four review rounds)

This page's numbers moved twice before landing here, each time in response to
a Codex review finding on PR #1358 that caught a real methodological gap.
Recorded in full because the corrections themselves are the reproducible
part of this pass, not just its conclusion:

1. **Wrong row shape.** The first cut wrote `session_id`/`sticky_worker_id`/
   `sticky_until` directly in the seed `INSERT` and omitted `sticky_timeout`
   entirely. `queue::enqueue()` never writes it that way: `NewTaskQueueItem`
   hardcodes all three sticky columns to `NULL` at `INSERT` time regardless
   of `EnqueueParams`; a separate `UPDATE` sets them, only when
   `sticky_worker_id`/`sticky_timeout` are set on the params -- verified
   directly against `worker.rs`'s session-member dispatch, which sets
   `session_id` + `sticky_worker_id` + `sticky_timeout`
   (`SESSION_MEMBER_STICKY_TIMEOUT`, 24h) together. Fixed by reproducing the
   real `INSERT`-then-`UPDATE` column shape.
2. **Wrong transaction granularity.** The round-1 fix still ran that
   `INSERT`-then-`UPDATE` as one bulk `INSERT` of N rows followed by one bulk
   `UPDATE` covering all of them. Postgres's default heap fillfactor (100)
   packs a bulk `INSERT`'s pages full before any row is widened, so the
   following bulk `UPDATE` finds no room on the same page for any row's new
   tuple version and is forced onto a fresh page for every single row --
   roughly doubling the table's physical size, and not what an interleaved,
   per-task production write produces. Fixed with a server-side PL/pgSQL
   procedure that loops per row -- `INSERT`, `UPDATE` by id, `COMMIT` --
   reproducing the real interleaved commit pattern with one network round
   trip per depth (the loop runs entirely server-side) instead of N.
3. **Untracked nested statements.** `pg_stat_statements.track` defaults to
   `top`, so the `INSERT`/`UPDATE` executed *inside* the procedure from fix 2
   were invisible to `pg_stat_statements` -- only the top-level `CALL` would
   have been recorded, leaving the write-cost table with nothing to read.
   Fixed with `SET pg_stat_statements.track = 'all'` for the seeding session.
4. **Unfair write-cost control.** Fix 2 made `worker-session`'s seed N
   separately-committed `INSERT`+`UPDATE` pairs while `no-session` stayed a
   single bulk `INSERT` (`db::seed()`'s convenience for populating a backlog
   to query against) -- comparing the two would measure statement-granularity
   overhead, not the predicate's cost. Fixed with a second procedure
   (`harvest_bench_seed_plain_rows`, `INSERT`-then-`COMMIT` per row, no
   `UPDATE`) so both labels' write-cost figures come from the same per-row
   lifecycle, differing only in the sticky/session columns.

The originally published figures (+21.9% single-call, +19.0% aggregate,
+28.3% write-side, then +124.8%/+7.2%/+171.7% after fix 1 alone) are all
superseded by this page.

A fifth review round raised a further, unresolved point about the seeding
transaction boundary -- see [Known limitations](#known-limitations); it did
not trigger another harness rewrite.

## Workload

`claim_task_query()`'s `candidate` CTE gates every row with a plain inline
test: `session_id IS NULL OR sticky_worker_id = $1` -- no subquery, no join.
A real worker-session row always carries `session_id`, `sticky_worker_id`,
`sticky_until`, and `sticky_timeout` non-`NULL` together, written via the
two-statement lifecycle established above -- unlike `schedule_to_close_at`, a
single column set once at `INSERT` time in isolation.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
seeds `claim_bench_support`'s standard 4-queue backlog at the published
`BACKLOG_SWEEP` depths (1,000 / 10,000 / 100,000 rows) in two data states,
identical in every other column:

- **`no-session`** -- `session_id`, `sticky_worker_id`, `sticky_until`, and
  `sticky_timeout` all `NULL`. Seeded per row via
  `harvest_bench_seed_plain_rows` (`INSERT`, `COMMIT`, repeat) for the
  write-cost measurement; via `db::seed()`'s single bulk `INSERT` for the
  `EXPLAIN` sweep (a fair simplification there -- with no follow-up `UPDATE`
  ever run against these rows, batching the `INSERT` does not create the
  dead-tuple asymmetry that motivated fix 2 above).
- **`worker-session`** -- seeded per row via `harvest_bench_seed_worker_session_rows`
  (`INSERT` carrying `session_id` only, `UPDATE` setting `sticky_worker_id`
  to the claiming worker's own id / `sticky_until` to `NOW() + 24h` /
  `sticky_timeout` to `24h`, `COMMIT`, repeat). Setting `sticky_worker_id =
  $1` makes both the session predicate *and* the pre-existing
  ordinary-sticky predicate directly above it
  (`sticky_worker_id IS NULL OR sticky_worker_id = $1 OR ...`) evaluate
  `TRUE` for every row, so the claimable row count is **identical** between
  the two labels -- the same match-not-exclude isolation technique the
  capability-labels capture uses.

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each depth (a single **cold** claim,
against a table nothing has touched since the seed), and for a full
`pg_stat_statements` drain of the real `queue::claim_task()` async function
against the 10,000-row/4-queue headline scenario, claiming every row one call
at a time (10,001 calls: 10,000 successful claims plus the terminal empty
poll, each call its own committed transaction, matching production).

## Plan

At every depth the two plans are structurally identical -- same `Seq Scan` on
`harvest_task_queue`, same join order, same CTE structure -- and differ only
in buffer counts. At backlog=10,000:

```text
no-session:      Seq Scan on harvest_task_queue  Buffers: shared hit=244  (actual rows=10000 loops=1)
worker-session:   Seq Scan on harvest_task_queue  Buffers: shared hit=334  (actual rows=10000 loops=1)
```

The `Seq Scan` node's own delta (244 -> 334, +90) **exactly matches** the
whole query's total delta at this depth (274 -> 364, +90) -- the entire cost
is inside the scan reading physically more pages, nothing leaks into any
other node. This is the same signature
`docs/performance-capability-labels.md`'s Plan section documents for its own
predicate, and it rules out a plan-shape explanation the same way:
`session_id IS NULL OR sticky_worker_id = $1` is a `Filter:` clause evaluated
row-by-row during the scan itself, not a separate `SubPlan`/`InitPlan` node.

Plan shape is stable across all three depths -- both states choose a plain
`Seq Scan` at 1,000, 10,000, and 100,000 rows, with no crossover to an
`Index Scan`.

## Measurement

### Buffer deltas across backlog depth (single cold claim)

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-session` vs `worker-session` (artifacts:
`docs/perf-artifacts/worker-session-claim-predicate/{no-session,worker-session}-claim-backlog-{depth}.explain.txt`):

| backlog | no-session buffers | worker-session buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 63 | +10 | +18.9% |
| 10,000 (headline) | 274 | 364 | +90 | **+32.9%** |
| 100,000 | 2,473 | 3,367 | +894 | +36.2% |

The delta grows with backlog size, consistent with a per-row storage effect
that compounds with the number of rows touched.

### Corroboration: `pg_stat_statements` over the real claim-drain

The `EXPLAIN` numbers above are single **cold** snapshots -- one claim against
a table nothing has touched since the seed committed. To confirm the effect
under the actual claim workload, the harness also drives the real async
`queue::claim_task(...)` function 10,001 times against the 10,000-row/4-queue
headline scenario at each data state and snapshots `pg_stat_statements`
afterward (artifacts:
`docs/perf-artifacts/worker-session-claim-predicate/{no-session,worker-session}-pg_stat_statements.txt`):

| state | calls | `shared_blks_hit` | avg per call |
|---|---:|---:|---:|
| no-session | 10,001 | 4,998,048 | 499.75 |
| worker-session | 10,001 | 6,101,864 | 610.13 |

Aggregate delta: **+22.1%** -- the same direction and the same order of
magnitude as the single-cold-claim finding (+32.9%), the "corroborated by a
buffer/row-count change in the same direction" bar this persona's rules set.
Unlike the round-1-fix-only capture (which showed a 17x divergence between
these two figures, +124.8% vs +7.2%, driven by the bulk-transaction
seeding artifact fix 2 above removed), the per-row-committed fixture gives
two numbers that agree with each other the way the capability-labels and
`schedule_to_close` pages' own single-call/aggregate pairs do.

## Equivalence

Both drains claim exactly 10,000 of 10,000 seeded rows
(`claimed == seeded.claimable_rows` asserted for both labels inside the
test, both against the ground-truth seeded count, not just against each
other), and `claim_row.calls == claimed + 1` is asserted for the terminal
empty poll in each state. The worker-session claim path returns the same
claim behavior as the unpinned path -- the cost measured here is pure
overhead on an otherwise identical result set.

## Write-side cost

No schema or index change is proposed, so there is no *new* write
amplification to weigh -- but the same `pg_stat_statements` capture that
produced the read-side numbers also captured the per-row seed writes for
each label, both going through the same `INSERT`-then-`COMMIT` (per row)
lifecycle (artifacts: same `pg_stat_statements.txt` files referenced above):

| state | statement(s) | calls | `shared_blks_hit` |
|---|---|---:|---:|
| no-session | `INSERT` only | 10,000 | 137,976 |
| worker-session | `INSERT` + `UPDATE` | 10,000 each | 158,259 + 235,626 = 393,885 |

Delta: **+185.5%** to write the identical row count through the real
two-statement, per-row-committed lifecycle. This is the largest percentage on
this page, and the mechanism is direct: `worker-session` issues twice as many
statements per row (an `INSERT` plus an `UPDATE`, each its own round trip and
its own WAL record) as `no-session`'s single `INSERT`, and the `UPDATE`
itself creates a second MVCC tuple version for every row.

This is a one-time cost in production, paid once at enqueue time --
`sticky_worker_id`/`sticky_until`/`sticky_timeout` are not columns any later
claim, completion, or retry path rewrites, unlike capability-labels' finding
of write cost recurring on every subsequent `UPDATE`.

## Why no fix is proposed

The measured cost is heap-page growth -- both from wider stored columns and
from the second MVCC tuple version `queue::enqueue()`'s real two-statement
write produces -- evaluated by a `Seq Scan` that already reads every
candidate row regardless of `session_id`/`sticky_worker_id`. Not a plan
inefficiency SQL can route around:

- The predicate itself is a plain `Filter:` boolean test with no `SubPlan` or
  `InitPlan` to rewrite -- confirmed directly in the captured `EXPLAIN`
  output, which shows the entire buffer delta landing inside the `Seq Scan`
  node itself at every depth (see [Plan](#plan) above).
- The two-statement write is what issue #606's hard-pin design requires:
  session membership (`session_id`) and the hard sticky pin
  (`sticky_worker_id`/`sticky_until`/`sticky_timeout`) are resolved at
  different points in `worker.rs`'s dispatch flow (a member activity's host
  worker is not known until the session's acquire step resolves it), so a
  single combined `INSERT` is not how the feature's control flow produces
  this row. Changing that write path is a feature-level design question, not
  a query-shape fix, and is out of scope for a measurement pass.
- There is no `MATERIALIZED`/index/rewrite angle on the *read* side: the
  columns are read directly off the already-scanned row, the same as the
  pre-existing ordinary-sticky-routing predicate immediately above this one
  in the query.

**Scope of this conclusion.** As with the capability-labels page, every
measurement here is I/O-scoped (`EXPLAIN (..., TIMING OFF)`, buffers/rows
only) -- this pass did not separately measure CPU cost.

## Known limitations

- **This measurement does not isolate worker sessions (#606) from ordinary
  sticky routing (#235).** A worker-session row necessarily also sets
  `sticky_worker_id`/`sticky_until`/`sticky_timeout`, which ordinary sticky
  routing (#235, still itself unmeasured on its own) sets independently via
  the same mechanism. The `worker-session` label's cost therefore includes
  whatever ordinary sticky routing alone would cost plus whatever
  `session_id` alone adds on top -- this page cannot and does not decompose
  the two. A session-tagged row with no sticky pin cannot occur in
  production (the two are always written together), so this is not
  resolvable from this capture alone.
- **The seeding fixture assumes one activity enqueued per transaction; a
  real fan-out from one workflow decision does not.** A review finding
  (round 5) correctly caught that `worker.rs::persist_scheduled_activities`
  wraps its ENTIRE `for params in &enqueued { queue::enqueue(conn,
  params).await? }` loop -- every scheduled activity from one decision,
  worker-session or not -- inside a single `conn.transaction(...)`. This
  page's per-row-committed procedure instead commits after every individual
  row, which understates the physical bloat a decision that schedules
  several session-member activities at once would produce: none of a
  same-transaction batch's superseded tuple versions can be pruned until the
  whole batch commits, where this page's fixture opens a reclaim window
  after every single row.
  This page does not re-measure with a batched-transaction fixture. The
  round-1-fix figures (+124.8% single-call, +171.7% write-side, from a
  bulk `INSERT`-of-N followed by a bulk `UPDATE`-of-N, both auto-committed
  separately) are **not** a substitute for that measurement either -- a
  single bulk statement and N individually-executed statements sharing one
  transaction are two more distinct scenarios, not the same one, and no
  scenario matching "N individual `INSERT`/`UPDATE` statement pairs sharing
  one transaction" has been captured. What can be said without a new capture:
  the true cost for a multi-activity-per-decision fan-out lies **at or above**
  this page's per-row-committed figures (+32.9% single-call / +22.1%
  aggregate / +185.5% write-side), since batching can only remove reclaim
  opportunities this fixture has, never add ones it lacks. How far above
  depends on typical fan-out size (activities per decision), which this pass
  does not have data on and did not attempt to estimate. Pinning down a
  precise number needs a dedicated capture with a batched-transaction seeding
  procedure at a stated, deliberately-chosen fan-out size -- left as future
  work rather than chased through a further round of this pass, per this
  persona's own floor on when to stop iterating.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths via server-side
  per-row seeding procedures matching `queue::enqueue()`'s real write
  lifecycle, captures `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING
  OFF)` for each, drains the real 10,000-row headline scenario through
  `queue::claim_task()` at both states while snapshotting
  `pg_stat_statements`, and asserts claim-count equivalence against ground
  truth as a correctness check.
- `docs/perf-artifacts/worker-session-claim-predicate/` -- the committed
  `EXPLAIN` captures, `pg_stat_statements` snapshots, and a
  `fixture-summary.txt` for both data states at all three depths.
- `autumn-harvest/scripts/worker_session_claim_perf_repro.sh` -- a
  reproduction script that re-runs the capture test.
- This doc.

`queue::claim_task_query()` is unmodified.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/worker_session_claim_perf_repro.sh
```

or, with only a reachable Docker daemon and no external Postgres:

```bash
./autumn-harvest/scripts/worker_session_claim_perf_repro.sh
```

Both regenerate the `EXPLAIN` captures, `pg_stat_statements` snapshots, and
`fixture-summary.txt` under
`docs/perf-artifacts/worker-session-claim-predicate/` from scratch. The
`pg_stat_statements` capture requires the extension loaded via
`shared_preload_libraries` on the target server (`ALTER SYSTEM SET
shared_preload_libraries = 'pg_stat_statements'` plus a restart, if not
already configured) -- without it the capture fails loudly with
`pg_stat_statements must be loaded via shared_preload_libraries` rather than
silently producing a partial artifact set. The write-cost table additionally
needs `pg_stat_statements.track = 'all'` (the harness sets this itself, for
its own seeding session only, so no server-level configuration is required
beyond the extension being loaded).
