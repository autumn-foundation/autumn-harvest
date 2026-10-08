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
moderate buffer cost that **grows monotonically across every published
backlog depth** -- **+18.9%** at 1,000 rows, **+32.9%** at the 10,000-row
headline depth, **+36.2%** at 100,000 rows, corroborated by a real
10,001-call production-shaped drain at **+18.3%** (same direction, same
order of magnitude -- see [Measurement](#measurement)). The mechanism is the
same one `docs/performance-worker-sessions.md` documents: row-width growth
from populating previously-`NULL` columns, compounded by the MVCC cost of
`queue::enqueue()`'s real two-statement write -- not a plan inefficiency.
There is no query-shape fix, because the `WHERE` clause already evaluates
the predicate as a plain inline test on a row the scan reads regardless.

**An earlier revision of this page reported a plan-shape crossover at
100,000 rows instead** (sticky-routing reading *fewer* buffers than
no-sticky). Codex review on this page's own PR traced that to a seeding
confound, not to `sticky_worker_id`: the two labels' primary-key and
`activity_id` B-trees held independently-random values, and page-split
noise from those OTHER indexes was large enough to flip which access path
the planner preferred at that depth. See
[Harness correction](#harness-correction-eliminating-a-seeding-confound)
for the fix and the corrected capture, which shows both labels choosing the
identical `Seq Scan` plan at every depth, including 100,000 rows.

## Harness correction: eliminating a seeding confound

The first committed capture seeded `no-sticky` and `sticky-routing` each
with their own, independently-random `id`/`activity_id` UUIDs (`no-sticky`
via `db::seed()`/`harvest_bench_seed_plain_rows`'s own `gen_random_uuid()`
calls, `sticky-routing` via a since-removed `harvest_bench_seed_sticky_routing_rows`
procedure that generated its own). Every claim is a non-HOT `UPDATE` that
touches every applicable index on `harvest_task_queue` -- the primary key
and `idx_harvest_tq_activity_id` unconditionally, not just the
sticky-routing partial index. Codex review on this page's own PR pointed
out that independently-random keys in those OTHER indexes' B-trees could
add page-split/traversal noise of a similar size to the effect this page
attributes to `sticky_worker_id` -- the exact confound
`docs/performance-schedule-to-close.md` documents fixing for its own
predicate on PR #1339. The first committed capture's 100,000-row plan-shape
"crossover", described above, turned out to be a symptom of exactly this:
reproduced with matched keys, both labels choose the identical `Seq Scan`
at that depth.

The fix follows `docs/performance-schedule-to-close.md`'s own two-part
correction:

1. **Snapshot the `no-sticky` control's exact seeded values.**
   `snapshot_seed_for_sticky_routing` copies `id`, `activity_id`, and every
   other seeded column out of `harvest_task_queue` into a plain (not
   `TEMP`) table, numbered by `ROW_NUMBER() OVER (ORDER BY ctid)` --
   physical insertion order, not the random primary key -- for the same
   reason schedule_to_close's fix orders by `ctid`: ordering by `id` would
   scramble the re-insert into a different physical order than the control
   used.
2. **Re-seed `sticky-routing` from that snapshot, not from fresh random
   values.** `reseed_from_sticky_routing_snapshot` truncates
   `harvest_task_queue` and calls a PL/pgSQL procedure
   (`harvest_bench_reseed_sticky_from_snapshot`) that reads the snapshot in
   `i` order and, per row, `INSERT`s it with its *exact* `id`/`activity_id`
   from the control, then `UPDATE`s the sticky columns, then `COMMIT`s --
   preserving both the exact indexed values and the interleaved
   `INSERT`-then-`UPDATE`-then-`COMMIT` physical layout the original
   procedure established.

Unlike schedule_to_close's fix, this one must also survive the real-drain
loop's cross-connection boundary: the per-depth `EXPLAIN` sweep seeds both
labels on one shared connection, but the stat-snapshot loop opens a
**fresh** `db::connect()` per label. A plain table, snapshotted at the end
of the `no-sticky` label's own iteration, is visible to the `sticky-routing`
label's later connection against the same database; a `TEMP` table would
not have survived the boundary, the same problem schedule_to_close's second
fix round addressed.

The impact was real, not cosmetic: the 100,000-row `EXPLAIN` delta moved
from **-65.9%** (a false plan-shape crossover) to **+36.2%** (the same
monotonic-growth trend as the smaller depths), and the real-drain aggregate
moved from +21.5% to +18.3%. Both remaining deltas are corroborating, not
confounded -- see [Measurement](#measurement).

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
  `harvest_bench_reseed_sticky_from_snapshot`, reusing the `no-sticky`
  control's exact `id`/`activity_id` values in their original physical
  insertion order (see
  [Harness correction](#harness-correction-eliminating-a-seeding-confound)):
  `INSERT` with no sticky columns and no `session_id`, `UPDATE` setting
  `sticky_worker_id` to the claiming worker's own id / `sticky_until` to
  `NOW() + 24h` / `sticky_timeout` to `24h`, `COMMIT`, repeat -- the same
  interleaved, per-row-committed procedure shape
  `zz_capture_worker_session_claim_evidence` established, minus the
  `session_id` column. Setting `sticky_worker_id = $1` with a future
  `sticky_until` makes the predicate evaluate `TRUE` for every row, so the
  claimable row count is **identical** between the two labels (see
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

At every published depth -- 1,000, 10,000, and 100,000 rows -- the two
plans are structurally identical: same `Seq Scan` on `harvest_task_queue`,
same join order, same CTE structure, and (at 100,000 rows) the same
external-merge disk sort -- differing only in buffer counts:

```text
backlog=10,000:
no-sticky:       Seq Scan on harvest_task_queue  Buffers: shared hit=244  (actual rows=10000 loops=1)
sticky-routing:  Seq Scan on harvest_task_queue  Buffers: shared hit=334  (actual rows=10000 loops=1)

backlog=100,000:
no-sticky:       Seq Scan on harvest_task_queue  Buffers: shared hit=2440  (actual rows=100000 loops=1)
sticky-routing:  Seq Scan on harvest_task_queue  Buffers: shared hit=3335  (actual rows=100000 loops=1)
```

The `Seq Scan` node's own delta accounts for the whole query's delta at
every depth almost exactly (10,000 rows: 244 -> 334, +90, against a
whole-query delta of 274 -> 364, +90; 100,000 rows: 2440 -> 3335, +895,
against a whole-query delta of 2473 -> 3368, +895) -- the entire cost is
inside the scan reading physically more pages, the same signature
`docs/performance-worker-sessions.md` documents. This rules out a
plan-shape explanation at any tested depth: `sticky_worker_id IS NULL OR
...` is a `Filter:` clause evaluated row-by-row during the scan, not a
separate `SubPlan`/`InitPlan`.

The planner's own row-count *estimate* for this scan is exact for both
labels at 100,000 rows (`rows=100000`, matching the actual count) -- unlike
an earlier, confounded revision of this capture, where independently-random
seed keys skewed `no-sticky`'s estimate to 68,360 against an actual
100,000 and flipped its preferred access path to an `Index Scan`. See
[Harness correction](#harness-correction-eliminating-a-seeding-confound).

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
| 100,000 | 2,473 | 3,368 | +895 | **+36.2%** |

The delta grows monotonically with backlog size across all three published
depths, consistent with a per-row storage effect that compounds with the
number of rows touched -- the same signature `docs/performance-worker-sessions.md`
reports for the analogous worker-session predicate (that page's own
deltas: +20.8% / +40.9% / +45.9%). At 100,000 rows both labels' sort still
spills to disk (`Sort Method: external merge Disk: 15280kB`, `temp
read=495 written=1914` -- identical in both artifacts, confirming issue
#1177's finding that this residual predicate defeats sort-elision
regardless of the value tested against it, corroborated again here), but
the access path underneath that sort is the identical `Seq Scan` for both
labels -- see [Plan](#plan). An earlier, confounded revision of this
capture reported a plan-shape crossover at this depth instead; see
[Harness correction](#harness-correction-eliminating-a-seeding-confound)
for why that was an artifact of the seeding fixture, not of
`sticky_worker_id`.

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
| no-sticky | 10,001 | 5,201,282 |
| sticky-routing | 10,001 | 6,150,622 |

Aggregate delta: **+18.3%** -- the same direction and order of magnitude as
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
| sticky-routing | `INSERT` + `UPDATE` | 10,000 each | 138,486 + 218,701 = 357,187 |

Delta: **+158.9%** to write the identical row count through the real
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
folded into the [+18.3% aggregate figure](#corroboration-pg_stat_statements-over-the-real-claim-drain)
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
  output at every tested depth, where the entire buffer delta lands
  inside the scan node itself (see [Plan](#plan)).
- `docs/performance-worker-sessions.md` already surfaces the real
  optimization candidate here -- collapsing `queue::enqueue()`'s
  `INSERT`-then-`UPDATE` sticky write into a single `INSERT` that computes
  `sticky_until` inline -- and explains why it is out of scope for a
  single-predicate measurement pass: `queue::enqueue()` is shared by every
  sticky-pin caller, ordinary routing included, so changing its write path
  is shared write-plumbing work, not a query-shape fix. This page does not
  repeat that analysis; it applies unchanged.
- Plan shape is stable across all three tested depths -- both labels choose
  a plain `Seq Scan` at 1,000, 10,000, and 100,000 rows, with no crossover
  to an `Index Scan` once the seeding confound is eliminated (see
  [Harness correction](#harness-correction-eliminating-a-seeding-confound)).

**Scope of this conclusion.** As with the worker-sessions and
capability-labels pages, every measurement here is I/O-scoped (`EXPLAIN
(..., TIMING OFF)`, buffers/rows only) -- this pass did not separately
measure CPU cost.

## Known limitations

- **The seeding fixture assumes one activity enqueued per transaction; a
  real fan-out from one workflow decision does not.**
  `worker.rs::persist_scheduled_activities` wraps its entire per-decision
  enqueue loop in one `conn.transaction(...)`, unlike this page's
  per-row-committed procedure. `docs/performance-worker-sessions.md`'s
  identical limitation, and its correction on what direction batching would
  move the write-side figure (unknown, not "at or above"), applies here
  unchanged -- this page does not re-run that analysis independently.
- **The write-side `+158.9%` figure is not decomposed across its
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
  truth as a correctness check. `snapshot_seed_for_sticky_routing` /
  `reseed_from_sticky_routing_snapshot` (plus the server-side
  `harvest_bench_reseed_sticky_from_snapshot` procedure) reuse the
  `no-sticky` control's exact `id`/`activity_id` values for the
  `sticky-routing` label -- see
  [Harness correction](#harness-correction-eliminating-a-seeding-confound).
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
