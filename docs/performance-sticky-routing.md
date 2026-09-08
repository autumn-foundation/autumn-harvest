# Sticky-routing claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Any residual predicate defeats sort-elision" section
names three claim-path predicates that independently defeat sort-elision
regardless of the value tested against them: `schedule_to_close` (#378),
worker sessions (#606), and ordinary sticky routing (#235). The first two
already had a seed-variant measurement isolating their own marginal buffer
cost, holding the already-collapsed plan shape fixed (PR #1339 and PR #1358).
Sticky routing was the one left "still unmeasured" -- this page closes that
gap.

`session_id` stays `NULL` on both labels here, unlike the worker-sessions
capture. That isolates ordinary sticky routing (a bare `with_sticky()` pin,
issue #235) from the worker-sessions predicate that sits immediately below
it in the same `candidate` CTE and always sets `sticky_worker_id` too.

The headline result is the same shape as worker sessions': a real,
moderate buffer cost at ordinary backlog depths -- **+18.9%** at 1,000 rows,
**+32.9%** at the 10,000-row headline depth, corroborated by a real
10,001-call production-shaped drain at **+21.5%** (same direction, same
order of magnitude -- see [Measurement](#measurement)). The mechanism is the
same one `docs/performance-worker-sessions.md` documents: row-width growth
from populating previously-`NULL` columns, compounded by the MVCC cost of
`queue::enqueue()`'s real two-statement write -- not a plan inefficiency.
There is no query-shape fix, because the `WHERE` clause already evaluates
the predicate as a plain inline test on a row the scan reads regardless.

At the largest tested depth (100,000 rows) the buffer count **reverses
sign** -- sticky-routing reads *fewer* buffers than no-sticky, not more.
This is not the same predicate-evaluation cost the smaller depths measure:
it is a genuine plan-shape crossover, reported in full under
[The 100,000-row crossover](#the-100000-row-crossover) rather than folded
into the headline number.

## Workload

`claim_task_query()`'s `candidate` CTE gates every row with a plain inline
test: `sticky_worker_id IS NULL OR sticky_worker_id = $1 OR sticky_until IS
NULL OR sticky_until <= NOW()` -- no subquery, no join. A real ordinary
sticky pin (`EnqueueParams::with_sticky`) always carries `sticky_worker_id`,
`sticky_until`, and `sticky_timeout` non-`NULL` together, written via
`queue::enqueue()`'s real two-statement lifecycle: one `INSERT` (all three
sticky columns `NULL`, per `NewTaskQueueItem`'s hardcoded shape), followed
immediately by an `UPDATE` that sets them (`queue.rs`'s `sticky` block).
`session_id` is untouched by either statement when only `with_sticky()` is
used.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_sticky_routing_claim_evidence`
seeds `claim_bench_support`'s standard 4-queue backlog at the published
`BACKLOG_SWEEP` depths (1,000 / 10,000 / 100,000 rows) in two data states,
identical in every other column:

- **`no-sticky`** -- `sticky_worker_id`, `sticky_until`, and `sticky_timeout`
  all `NULL`. Seeded per row via `harvest_bench_seed_plain_rows` (`INSERT`,
  `COMMIT`, repeat) for the write-cost measurement; via `db::seed()`'s
  single bulk `INSERT` for the `EXPLAIN` sweep, for the same reason the
  worker-sessions page gives: with no follow-up `UPDATE` ever run against
  these rows, batching the `INSERT` creates no dead-tuple asymmetry.
- **`sticky-routing`** -- seeded per row via
  `harvest_bench_seed_sticky_routing_rows` (`INSERT` with no sticky columns
  and no `session_id`, `UPDATE` setting `sticky_worker_id` to the claiming
  worker's own id / `sticky_until` to `NOW() + 24h` / `sticky_timeout` to
  `24h`, `COMMIT`, repeat) -- the same interleaved, per-row-committed
  procedure shape `zz_capture_worker_session_claim_evidence` established,
  minus the `session_id` column. Setting `sticky_worker_id = $1` with a
  future `sticky_until` makes the predicate evaluate `TRUE` for every row,
  so the claimable row count is **identical** between the two labels (see
  [Equivalence](#equivalence)).

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each depth (a single **first claim**
against a table nothing has *claimed* from since the seed -- every committed
artifact reports `shared hit`, never `read`, confirming the table's pages
are already resident in `shared_buffers`), and for a full
`pg_stat_statements` drain of the real `queue::claim_task()` async function
against the 10,000-row/4-queue headline scenario, claiming every row one
call at a time (10,001 calls: 10,000 successful claims plus the terminal
empty poll, each its own committed transaction, matching production).

## Plan

At 1,000 and 10,000 rows the two plans are structurally identical -- same
`Seq Scan` on `harvest_task_queue`, same in-memory `quicksort`, same CTE
structure -- and differ only in buffer counts:

```text
backlog=10,000:
no-sticky:       Seq Scan on harvest_task_queue  Buffers: shared hit=253  (actual rows=10000 loops=1)
sticky-routing:  Seq Scan on harvest_task_queue  Buffers: shared hit=343  (actual rows=10000 loops=1)
```

The `Seq Scan` node's own delta (253 -> 343, +90) accounts for the whole
query's delta at this depth exactly (274 -> 364, +90) -- the entire cost is
inside the scan reading physically more pages, the same signature
`docs/performance-worker-sessions.md` documents. This rules out a plan-shape
explanation at these two depths: `sticky_worker_id IS NULL OR ...` is a
`Filter:` clause evaluated row-by-row during the scan, not a separate
`SubPlan`/`InitPlan`.

At 100,000 rows this stops being true -- see below.

## Measurement

### Buffer deltas across backlog depth (single first claim, cache-warm)

`EXPLAIN (ANALYZE, BUFFERS, ...)` **shared** buffers (`shared hit` + `shared
read`) for `claim_task_query()`'s whole-query root node, `no-sticky` vs
`sticky-routing` (artifacts:
`docs/perf-artifacts/sticky-routing-claim-predicate/{no-sticky,sticky-routing}-claim-backlog-{depth}.explain.txt`):

| backlog | no-sticky shared buffers | sticky-routing shared buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 63 | +10 | +18.9% |
| 10,000 (headline) | 274 | 364 | +90 | **+32.9%** |
| 100,000 | 9,878 | 3,367 | -6,511 | **-65.9%** (plan-shape crossover, see below) |

The 1,000/10,000 delta grows with backlog size, consistent with a per-row
storage effect that compounds with the number of rows touched -- the same
signature `docs/performance-worker-sessions.md` reports for the analogous
worker-session predicate (that page's own deltas: +20.8% / +40.9%). The
100,000-row row is **not** a continuation of that trend; see the next
section before citing it as one.

### The 100,000-row crossover

At 100,000 rows both labels' sort still spills to disk (`Sort Method:
external merge Disk: 15280kB`, `temp read=495 written=1914` -- identical in
both artifacts, confirming issue #1177's finding that this residual
predicate defeats sort-elision regardless of the value tested against it,
corroborated again here). What changes is the base access path the planner
picks *underneath* that sort:

```text
no-sticky (100,000 rows):
  Index Scan using idx_harvest_tq_poll on harvest_task_queue
    (cost=0.29..1555627.18 rows=68360 width=192) (actual rows=100000 loops=1)
    Buffers: shared hit=9845

sticky-routing (100,000 rows):
  Seq Scan on harvest_task_queue
    (cost=0.00..2264834.00 rows=99990 width=197) (actual rows=100000 loops=1)
    Buffers: shared hit=3334
```

Both scans return the true row count, 100,000, but the planner's own
row-count *estimate* diverges sharply between the two queries: 68,360 for
`no-sticky`'s `sticky_worker_id IS NULL` branch (a 31.6% underestimate) vs.
99,990 for `sticky-routing`'s `sticky_worker_id = 'deadbeef-...'` branch (a
near-exact estimate). `no-sticky`'s underestimate is what makes
`idx_harvest_tq_poll` look cheap enough to prefer over a `Seq Scan` at this
depth; the estimate for `sticky-routing` gives the optimizer no such reason,
so it picks the plain `Seq Scan` (3,334 shared-hit buffers vs. the index
path's 9,845). Whichever access path costs less at a given depth is what
each query gets independently -- the two labels are not choosing between
the same two options at the same cost, because the residual predicate
changes the cardinality estimate feeding that choice.

This is a real, `EXPLAIN`-documented plan-shape difference, not
measurement noise -- but it is observed at exactly **one** of the three
published `BACKLOG_SWEEP` depths, not the "≥3 data sizes" this persona's own
rules require before treating a plan-shape change as a general finding
rather than a fixture-specific coincidence. At 1,000 and 10,000 rows both
labels choose `Seq Scan` (see [Plan](#plan)); the crossover to
`idx_harvest_tq_poll` for `no-sticky` happens somewhere between 10,000 and
100,000 rows, and this pass does not narrow that further. Reported here in
full because it directly contradicts the 1,000/10,000 trend if skimmed
without this section, not because it is a claim about sticky routing's cost
at production scale.

### Corroboration: `pg_stat_statements` over the real claim-drain

The `EXPLAIN` numbers above are single **first-claim** snapshots against a
cache-warm table. To confirm the 10,000-row effect under the actual claim
workload, the harness also drives the real async `queue::claim_task(...)`
function 10,001 times against the 10,000-row/4-queue headline scenario at
each data state and snapshots `pg_stat_statements` afterward (artifacts:
`docs/perf-artifacts/sticky-routing-claim-predicate/{no-sticky,sticky-routing}-pg_stat_statements.txt`).
As in the worker-sessions capture, this is the main `claim_task_query()`
statement's own count -- it does not fold in the queue-pause/activity-pause
rechecks `queue::claim_task()` also issues per claim:

| state | calls | main query `shared_blks_hit` |
|---|---:|---:|
| no-sticky | 10,001 | 5,079,670 |
| sticky-routing | 10,001 | 6,170,940 |

Aggregate delta: **+21.5%** -- the same direction and order of magnitude as
the single-first-claim 10,000-row finding (+32.9%), and smaller than
worker-sessions' combined predicate delta (+29.0% aggregate) in the same
direction a subset predicate should be: ordinary sticky routing alone is one
of the two columns worker-sessions' `session_id` + `sticky_worker_id` pair
sets together.

## Equivalence

Both drains claim exactly 10,000 of 10,000 seeded rows (`claimed ==
seeded.claimable_rows` asserted for both labels inside the test, both
against the ground-truth seeded count, not just against each other), and
`claim_row.calls == claimed + 1` is asserted for the terminal empty poll in
each state (`fixture-summary.txt` records `claimed=10000` for both). The
sticky-routing claim path returns the same claim behavior as the unpinned
path -- the cost measured here is pure overhead on an otherwise identical
result set.

## Write-side cost

No schema or index change is proposed, so there is no *new* write
amplification to weigh -- but the same `pg_stat_statements` capture that
produced the read-side numbers also captured the per-row seed writes for
each label, both going through the same per-row-committed lifecycle
(artifacts: same `pg_stat_statements.txt` files referenced above):

| state | statement(s) | calls | `shared_blks_hit` |
|---|---|---:|---:|
| no-sticky | `INSERT` only | 10,000 | 137,982 |
| sticky-routing | `INSERT` + `UPDATE` | 10,000 each | 138,492 + 218,658 = 357,150 |

Delta: **+158.8%** to write the identical row count through the real
two-statement, per-row-committed lifecycle. As with worker sessions, three
mechanisms compose this and this pass does not attribute the delta across
them individually: a second statement per row, a second MVCC tuple version
from the `UPDATE`, and an entry added to the partial index
`idx_harvest_tq_sticky_poll` (`WHERE state = 'PENDING' AND sticky_worker_id
IS NOT NULL`) that the `no-sticky` control never touches. This figure is
buffer-only (`shared_blks_hit`), not WAL volume, and does not include the
extra client/server round trip production pays for the separate `INSERT`
and `UPDATE` -- see `docs/performance-worker-sessions.md`'s write-side
section for the full reasoning behind each of those scope limits, which
applies identically here.

The wider row's rewrite cost is not one-time: PostgreSQL creates a new
tuple version on every subsequent `UPDATE` regardless of which columns that
`UPDATE` touches, so a sticky-pinned row's extra width recurs on the
claiming `UPDATE` inside `claim_task_query()`'s own `claimed` CTE (already
folded into the [+21.5% aggregate figure](#corroboration-pg_stat_statements-over-the-real-claim-drain)
above), and on any later heartbeat or retry `UPDATE` this pass does not
measure.

## Why no fix is proposed

The measured cost at ordinary backlog depths is heap-page and index-page
growth -- from wider stored columns, from the second MVCC tuple version
`queue::enqueue()`'s real two-statement write produces, and from the
partial-index entry that write adds -- evaluated by a scan that already
reads every candidate row regardless of `sticky_worker_id`. Not a plan
inefficiency SQL can route around:

- The predicate itself is a plain `Filter:` boolean test with no `SubPlan`
  or `InitPlan` to rewrite -- confirmed directly in the captured `EXPLAIN`
  output at 1,000 and 10,000 rows, where the entire buffer delta lands
  inside the scan node itself (see [Plan](#plan)).
- `docs/performance-worker-sessions.md` already surfaces the real
  optimization candidate here -- collapsing `queue::enqueue()`'s
  `INSERT`-then-`UPDATE` sticky write into a single `INSERT` that computes
  `sticky_until` inline -- and explains why it is out of scope for a
  single-predicate measurement pass: `queue::enqueue()` is shared by every
  sticky-pin caller, ordinary routing included, so changing its write path
  is shared write-plumbing work, not a query-shape fix. This page does not
  repeat that analysis; it applies unchanged.
- The 100,000-row crossover ([above](#the-100000-row-crossover)) is a
  statistics/cardinality-estimate effect on the *existing* plan choice, not
  a defect a query rewrite fixes -- and it is observed at one depth, not the
  three this persona's own rules require before treating a plan-shape
  change as actionable.

**Scope of this conclusion.** As with the worker-sessions and
capability-labels pages, every measurement here is I/O-scoped (`EXPLAIN
(..., TIMING OFF)`, buffers/rows only) -- this pass did not separately
measure CPU cost.

## Known limitations

- **The 100,000-row plan-shape crossover is reported at one depth, not
  three.** See [The 100,000-row crossover](#the-100000-row-crossover). Where
  the crossover actually sits between 10,000 and 100,000 rows is not
  narrowed by this pass.
- **The seeding fixture assumes one activity enqueued per transaction; a
  real fan-out from one workflow decision does not.**
  `worker.rs::persist_scheduled_activities` wraps its entire per-decision
  enqueue loop in one `conn.transaction(...)`, unlike this page's
  per-row-committed procedure. `docs/performance-worker-sessions.md`'s
  identical limitation, and its correction on what direction batching would
  move the write-side figure (unknown, not "at or above"), applies here
  unchanged -- this page does not re-run that analysis independently.
- **The write-side `+158.8%` figure is not decomposed across its
  contributing mechanisms** (statement count, MVCC tuple version, partial
  index maintenance), for the same reason `docs/performance-worker-sessions.md`
  does not decompose its own `+188.2%` figure: isolating each share needs a
  capture that varies one mechanism at a time, which this pass did not run.
- **Seeded rows leave `workflow_exec_id` `NULL`; every real activity task
  carries a real one.** This is the harness-wide simplification
  `docs/performance-worker-sessions.md`'s own "Known limitations" documents
  in full for every published Ledger claim-path measurement in this repo,
  this page's fixture included -- not something this pass introduces or
  re-derives.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_sticky_routing_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths via a
  server-side per-row seeding procedure matching `queue::enqueue()`'s real
  write lifecycle, captures `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
  TIMING OFF)` for each, drains the real 10,000-row headline scenario
  through `queue::claim_task()` at both states while snapshotting
  `pg_stat_statements`, and asserts claim-count equivalence against ground
  truth as a correctness check.
- `docs/perf-artifacts/sticky-routing-claim-predicate/` -- the committed
  `EXPLAIN` captures, `pg_stat_statements` snapshots, and a
  `fixture-summary.txt` for both data states at all three depths.
- `autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh` -- a
  reproduction script that re-runs the capture test.
- This doc.

`queue::claim_task_query()` is unmodified.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh
```

or, with only a reachable Docker daemon and no external Postgres:

```bash
./autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh
```

Both regenerate the `EXPLAIN` captures, `pg_stat_statements` snapshots, and
`fixture-summary.txt` under
`docs/perf-artifacts/sticky-routing-claim-predicate/` from scratch. See
`docs/performance-worker-sessions.md`'s Reproduce section for the exact
`pg_stat_statements`/`shared_preload_libraries` and role-privilege
preconditions (`pg_stat_statements_reset(...)` `EXECUTE`, `SET` on
`pg_stat_statements.track`) -- identical here, since this capture reuses the
same harness.
