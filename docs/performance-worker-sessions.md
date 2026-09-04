# Worker-sessions claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Known limitations" section named worker sessions
(issue #606), alongside `schedule_to_close` (#378) and sticky routing (#235),
as claim-path predicates left unmeasured by the earlier passes: "cheap inline
column tests, against columns the seed leaves null." PR #1339 closed that gap
for `schedule_to_close`, confirming it cheap (+3.6% to +5.7% buffers, well
under the impact floor), and narrowed the remaining unmeasured set to worker
sessions and sticky routing. This page is the worker-sessions measurement.

The result is **not** the same as `schedule_to_close`'s: this predicate has a
large, real buffer cost -- **+124.8%** at the 10,000-row headline depth on a
single cold claim, though a real 10,001-call production-shaped drain shows a
much smaller **+7.2%** aggregate effect (see [Why the two numbers
diverge](#why-the-two-numbers-diverge) below for the reclaim mechanism this
page believes explains the gap). The mechanism is the same one
`docs/performance-capability-labels.md` documents: row-width growth from
populating previously-`NULL` columns compounded by MVCC bloat from
`queue::enqueue()`'s real two-step write (see
[Workload](#workload)) -- not a plan inefficiency. There is no query-shape
fix, because the `WHERE` clause already evaluates the predicate as a plain
inline test on a row the scan reads regardless. Per the acceptable-outcomes
rule ("optimization PR, findings issue, negative result -- all are successful
runs"), this ships as a measured, reproducible finding with no code change to
`queue.rs`.

## Harness correction (review finding on PR #1358)

The first cut of this capture seeded the `worker-session` state by writing
`session_id`, `sticky_worker_id`, and `sticky_until` **directly in the seed
`INSERT`**, and omitted `sticky_timeout` entirely. A Codex review finding on
PR #1358 caught that this is not how `queue::enqueue()` writes these columns
in production: `NewTaskQueueItem` **hardcodes**
`sticky_worker_id`/`sticky_until`/`sticky_timeout` to `NULL` at `INSERT` time
regardless of `EnqueueParams` (`queue.rs`, the `enqueue()` function); only
`session_id` is written at `INSERT` time. The three sticky columns are set by
a **separate `UPDATE`** immediately after, run only when `sticky_worker_id`
and `sticky_timeout` are both set:

```sql
UPDATE harvest_task_queue
   SET sticky_worker_id = $2, sticky_until = NOW() + $3, sticky_timeout = $3
 WHERE id = $1
```

Verified directly against the real caller: `worker.rs`'s session-member
dispatch (the code that actually enqueues a worker-session activity) sets
`params.session_id = Some(session_id)`, `params.sticky_worker_id =
Some(host_worker_id)`, and `params.sticky_timeout =
Some(SESSION_MEMBER_STICKY_TIMEOUT)` (24 hours) together -- exercising exactly
this INSERT-then-UPDATE path.

The original capture's shortcut therefore measured a **narrower, less bloated
row than production ever produces**: it omitted `sticky_timeout` (an
`INTERVAL`, further widening the row) and, more importantly, skipped the
MVCC cost of the second write entirely. An `UPDATE` gives a row a brand-new
tuple version while the old one stays physically resident in the heap until
vacuumed or opportunistically pruned -- exactly the pitfall
`docs/performance-capability-labels.md`'s own harness note describes for a
different predicate, except there it was an artifact to avoid (capability
labels really are written once, at `INSERT` time, in production) and here
it is the **real, unavoidable production write shape** (worker sessions
really are written via `INSERT` then `UPDATE`, every time).

**Fix:** the harness now reproduces the same two-step write `queue::enqueue()`
performs -- one `INSERT` carrying only `session_id`, then one `UPDATE`
setting `sticky_worker_id`/`sticky_until`/`sticky_timeout` (24 hours, matching
`SESSION_MEMBER_STICKY_TIMEOUT`) -- rather than writing all four columns in a
single `INSERT`. All figures on this page are from the corrected harness. The
originally published figures (+21.9% single-call, +19.0% aggregate, +28.3%
write-side) undercounted the real cost and are superseded by this page.

## Workload

`claim_task_query()`'s `candidate` CTE gates every row with a plain inline
test: `session_id IS NULL OR sticky_worker_id = $1` -- no subquery, no join.
As established above, a real worker-session row always carries `session_id`,
`sticky_worker_id`, `sticky_until`, and `sticky_timeout` non-`NULL` together,
written via the two-step lifecycle -- unlike `schedule_to_close_at`, a single
column set once at `INSERT` time in isolation.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
seeds `claim_bench_support`'s standard 4-queue backlog at the published
`BACKLOG_SWEEP` depths (1,000 / 10,000 / 100,000 rows) in two data states,
identical in every other column:

- **`no-session`** -- `session_id`, `sticky_worker_id`, `sticky_until`, and
  `sticky_timeout` all `NULL` (today's default seed shape, and every other
  `ClaimGate` scenario's shape too). Seeded with one set-based `INSERT`.
- **`worker-session`** -- seeded via the real two-step lifecycle: one
  `INSERT` carrying `session_id` only, then one `UPDATE` setting
  `sticky_worker_id` to the claiming worker's own id, `sticky_until` to
  `NOW() + 24 hours`, and `sticky_timeout` to `24 hours`. Setting
  `sticky_worker_id = $1` makes both the session predicate *and* the
  pre-existing ordinary-sticky predicate directly above it
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
worker-session:   Seq Scan on harvest_task_queue  Buffers: shared hit=586  (actual rows=10000 loops=1)
```

The `Seq Scan` node's own delta (244 -> 586, +342) **exactly matches** the
whole query's total delta at this depth (274 -> 616, +342) -- the entire cost
is inside the scan reading physically more pages, nothing leaks into any
other node. This is the same signature
`docs/performance-capability-labels.md`'s Plan section documents for its own
predicate, and it rules out a plan-shape explanation the same way:
`session_id IS NULL OR sticky_worker_id = $1` is a `Filter:` clause evaluated
row-by-row during the scan itself, not a separate `SubPlan`/`InitPlan` node.

Unlike the first (flawed) capture, **the plan shape is now stable across all
three depths** -- both states choose a plain `Seq Scan` at 1,000, 10,000, and
100,000 rows, with no crossover to an `Index Scan` at the largest depth. The
extra physical bloat from the two-step write pushes both states past whatever
threshold the planner was flip-flopping around in the original (narrower)
capture.

## Measurement

### Buffer deltas across backlog depth (single cold claim)

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-session` vs `worker-session` (artifacts:
`docs/perf-artifacts/worker-session-claim-predicate/{no-session,worker-session}-claim-backlog-{depth}.explain.txt`):

| backlog | no-session buffers | worker-session buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 89 | +36 | +67.9% |
| 10,000 (headline) | 274 | 616 | +342 | **+124.8%** |
| 100,000 | 2,473 | 5,892 | +3,419 | +138.3% |

The delta percentage grows with backlog size (+67.9% -> +124.8% -> +138.3%),
consistent with a per-row storage effect that compounds with the number of
rows touched, not a fixed per-query overhead that would amortize away at
scale. This clears the impact floor's "≥20% buffer reduction" bar in
magnitude (as a *cost*, not a reduction -- see [Why no fix is
proposed](#why-no-fix-is-proposed) for why that does not make it fixable).

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
| no-session | 10,001 | 5,288,273 | 528.77 |
| worker-session | 10,001 | 5,671,354 | 567.08 |

Aggregate delta: **+7.24%** -- real, same direction as the single-cold-claim
finding, but a much smaller magnitude (roughly a seventeenth of +124.8%, not
the "within a few points" agreement the capability-labels and
`schedule_to_close` pages report for their own single-call/aggregate pairs).
This page does not treat the two numbers as corroborating each other in the
"order of magnitude" sense this persona's own rules require for wall-clock
evidence; they agree only in *direction*. See below for why.

### Why the two numbers diverge

Both `no-session` and `worker-session` grow their **average** per-call cost
well above their **cold, first-call** `EXPLAIN` figure over the course of the
drain (no-session: 274 cold vs. 528.77 average, +93%) -- expected, because
`claim_task()`'s own claiming `UPDATE` (`SET state = 'RUNNING', worker_id =
..., started_at = ..., attempt = attempt + 1, wake_requested = ...`) gives
every claimed row a new tuple version, so the physical table only ever grows
across a drain that claims every row once.

`worker-session`'s average (567.08) is instead **lower** than its own cold
figure (616) -- the opposite direction. The mechanism this page believes
explains it: the `worker-session` table starts the drain already carrying,
for every row, a **dead** tuple version (the first-`INSERT` row, superseded by
the seed's own hard-pin `UPDATE`) that is eligible for reclaim from the moment
the seeding transaction commits -- no reader holds a snapshot old enough to
need it. `no-session` carries no equivalent debt; its rows have exactly one
tuple version when the drain starts. As each of the 10,000 separately
committed claim calls writes to a page, Postgres's opportunistic pruning can
reclaim that pre-existing dead space and let the claiming `UPDATE`'s new
tuple reuse it rather than extend the table -- a reclaim opportunity
`no-session` does not have, since it has no comparable backlog of dead
tuples to draw down. Net effect: `worker-session`'s per-call cost trends
down across the drain as its seed-time debt gets paid off, `no-session`'s
trends up as its own claim-induced debt accumulates, and the two curves
converge far closer than the cold snapshot alone would suggest.

**This mechanism is offered as the most coherent explanation consistent with
the numbers, not as an independently proven one.** It was not confirmed by an
interval `pg_stat_user_tables.n_dead_tup`/`pg_relation_size` sampling across
the drain (the kind of direct instrumentation
`docs/performance-capability-labels.md`'s write-side section uses to nail
down a similar claim) -- doing so is future work, not part of this pass. What
*is* directly evidenced is the same class of effect
`docs/performance-capability-labels.md`'s write-side section documents for a
different predicate: a bulk, single-transaction write pattern (the cold
`EXPLAIN`'s snapshot, taken immediately after the seed's two uncommitted-then-
rolled-back statements inside the `EXPLAIN ANALYZE` transaction) overstates
cost relative to a sequence of separately committed writes (the real drain),
because only the latter gets an opportunistic reclaim window between
commits. That page's own separate-vs-bulk-transaction figures differ by
roughly 4.5x in the same direction this page's two figures differ (17x) --
different queries, different magnitudes, same underlying asymmetry.

**Which number to use depends on the question.** A worker issuing isolated,
infrequent session claims against an otherwise-idle table experiences
something closer to the cold, **+124.8%** figure. A fleet running a steady
stream of session claims, where most reads land on pages other claims have
already touched and partially reclaimed, experiences something closer to the
**+7.2%** aggregate figure. Neither is "the" answer; both are reported rather
than picking one.

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
produced the read-side numbers also captured the headline-scenario seed
writes for each label (artifacts: same `pg_stat_statements.txt` files
referenced above):

| state | statement(s) | rows | `shared_blks_hit` |
|---|---|---:|---:|
| no-session | one `INSERT` | 10,000 | 136,712 |
| worker-session | `INSERT` + `UPDATE` | 10,000 | 156,493 + 214,918 = 371,411 |

Delta: **+171.7%** to write the identical row count through the real
two-statement lifecycle -- substantially larger than this page's original
(incorrect) single-`INSERT` estimate of +28.3%, because it now includes the
follow-up `UPDATE`'s own cost (rewriting every row into a second tuple
version) rather than assuming the row arrives fully-formed in one write.

This is measured **immediately after the seed**, before any claim has run and
before any opportunistic reclaim (discussed above) has had a chance to occur
-- it is the *un-reclaimed*, worst-case snapshot of the write cost, the same
caveat the read-side aggregate section applies in the other direction. Unlike
capability-labels' finding of write cost recurring on *every* subsequent
claim/completion/retry `UPDATE`, this two-step cost is paid once per task, at
enqueue time -- `sticky_worker_id`/`sticky_until`/`sticky_timeout` are not
columns any later claim, completion, or retry path rewrites.

## Why no fix is proposed

The measured cost is heap-page growth -- both from wider stored columns and
from the second MVCC tuple version `queue::enqueue()`'s real two-step write
produces -- evaluated by a `Seq Scan` that already reads every candidate row
regardless of `session_id`/`sticky_worker_id`. Not a plan inefficiency SQL
can route around:

- The predicate itself is a plain `Filter:` boolean test with no `SubPlan` or
  `InitPlan` to rewrite -- confirmed directly in the captured `EXPLAIN`
  output, which shows the entire buffer delta landing inside the `Seq Scan`
  node itself at every depth (see [Plan](#plan) above).
- The two-step write is what issue #606's hard-pin design requires: session
  membership (`session_id`) and the hard sticky pin
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
  the same `with_sticky`-triggered `UPDATE` path. The `worker-session`
  label's cost therefore includes whatever ordinary sticky routing alone
  would cost plus whatever `session_id` alone adds on top -- this page
  cannot and does not decompose the two. A session-tagged row with no sticky
  pin cannot occur in production (the two are always written together), so
  this is not resolvable from this capture alone.
- **The reclaim mechanism in [Why the two numbers
  diverge](#why-the-two-numbers-diverge) is a plausible explanation, not a
  directly instrumented one.** See that section's own caveat.
- **The write-side figure is an un-reclaimed, immediately-post-seed
  snapshot**, not a steady-state production number -- see
  [Write-side cost](#write-side-cost).

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths via the real
  `queue::enqueue()`-shaped two-step write, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each,
  drains the real 10,000-row headline scenario through `queue::claim_task()`
  at both states while snapshotting `pg_stat_statements`, and asserts
  claim-count equivalence against ground truth as a correctness check.
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
silently producing a partial artifact set.
