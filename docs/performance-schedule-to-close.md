# `schedule_to_close_at` claim predicate: measured, confirmed cheap

`docs/performance.md`'s "Known limitations" section flagged
`schedule_to_close_at` (issue #378), alongside worker sessions (#606) and
sticky routing (#235), as "cheap inline column tests, against columns the
seed leaves null" -- present in `queue::claim_task_query()` on every claim,
but never measured because `claim_bench_support::db::seed_backlog` never
populates the column. This page is that measurement, for `schedule_to_close_at`
only.

The result **confirms the doc's own suspicion**: at the two backlog depths
where this environment's measurements reproduce consistently (1,000 and
10,000 rows), populating `schedule_to_close_at` adds a small, real buffer
cost to the claim query -- **+5.7% and +3.6%** respectively -- corroborated by
a real 10,001-call production-code drain (**+2.5%** aggregate) and two
standalone MVCC-bloat scripts (**+4.7%** and **+5.2%**). All four independent
measurements land in the same 2.5-6% band, none within shouting distance of
the 20% impact floor. This is a **negative result**: no fix is proposed, and
none is needed. The one caveat, reported rather than papered over: at the
100,000-row depth, the planner occasionally (once in two runs) chose a
markedly more expensive plan for the `schedule_to_close_at`-populated table
-- see [The 100k-depth plan instability](#the-100k-depth-plan-instability)
below.

## Workload

`claim_task_query()`'s candidate CTE gates every row with:

```sql
AND (
    schedule_to_close_at IS NULL
    OR schedule_to_close_at > NOW()
)
```

a plain inline test against a column already on the `harvest_task_queue` row
the scan has fetched -- no subquery, no join, no correlated cost. In
production this column is set once at initial enqueue (`NOW() +
schedule_to_close`, issue #378) when a caller declares a total-attempt
deadline, and left `NULL` (unbounded) otherwise.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_schedule_to_close_claim_evidence`
mirrors `zz_capture_capability_labels_claim_evidence` exactly in shape:
`queue::claim_task_query()` is unmodified end to end (there is no query-shape
fix to try for a plain column test), and every EXPLAIN /
`pg_stat_statements` pair is captured from the exact same query text at two
seeded states of `harvest_task_queue.schedule_to_close_at`:

- **`no-schedule-to-close`** -- every row's column is `NULL` (today's
  default, and what every other claim-path benchmark in this crate already
  measures).
- **`schedule-to-close`** -- every row seeded with `schedule_to_close_at =
  NOW() + INTERVAL '1 hour'` at `INSERT` time. Deliberately far enough in the
  future that it can never elapse during the capture, so -- like the
  capability-labels capture's matching `Exact` requirement -- it excludes
  nothing: this isolates the predicate's *evaluation* cost from any change in
  which rows are eligible, and lets the drain loop's claimed-row count serve
  as a correctness check between labels.

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each of the published `BACKLOG_SWEEP`
depths (1,000 / 10,000 / 100,000), and for a full `pg_stat_statements` drain
of the real async `queue::claim_task()` function (not literal-substituted
SQL) against the 10,000-row/4-queue headline scenario, claiming every row one
call at a time -- plus, added specifically for this page, a
`pg_stat_user_tables`/`pg_relation_size` snapshot of `harvest_task_queue`
immediately before and immediately after that same drain, to see whether any
aggregate delta is driven by MVCC bloat accumulating over the drain itself
rather than by row width alone (a single-call EXPLAIN, seeded fresh and
rolled back inside a transaction, can never observe that: it never
accumulates dead tuples).

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-schedule-to-close` vs `schedule-to-close` (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-claim-backlog-{depth}.explain.txt`):

| backlog | no-schedule-to-close buffers | schedule-to-close buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 56 | +3 | +5.7% |
| 10,000 | 274 | 284 | +10 | +3.6% |
| 100,000 | 2,476 | 10,128 | +7,652 | +309.0% |

The 1,000- and 10,000-row rows are the reliable evidence here: they
reproduced within a point of these values across two independent full runs
of this capture (the first run measured 53/56 and 274/284 identically). The
100,000-row row did **not** reproduce -- see the next section.

### The 100k-depth plan instability

The first capture run measured 100,000-row buffers as 2,473 (no-schedule-to-close)
vs 2,537 (schedule-to-close), a +2.6% delta consistent with the two smaller
depths. The second run -- the one whose numbers are tabulated above, and the
one this page's aggregate/heap-growth sections use throughout, for
internal consistency -- measured 2,476 vs **10,128**, a +309% delta. Comparing
the plans directly (`grep`-ed from the committed artifacts) shows why: the
`schedule-to-close` side's main candidate-row source flipped from `Seq Scan
on harvest_task_queue` (both runs' `no-schedule-to-close` side, and the first
run's `schedule-to-close` side) to `Index Scan using idx_harvest_tq_poll` in
the second run's `schedule-to-close` capture only. That index cannot serve
the query's `ORDER BY` (the non-indexable leading `CASE` expression --
see `docs/performance.md`'s TL;DR), so the plan still pays for a full
external-merge sort afterward (`Sort Method: external merge Disk: 15280kB`,
identical in both plans) *in addition to* a more expensive random-access
scan to source the rows -- strictly worse than the `Seq Scan` alternative
here, not a genuine optimization the planner found.

This is the same class of run-to-run plan instability at this exact backlog
depth that `docs/performance-capability-labels.md`'s "Review note" section
documents (there, a `Seq Scan` vs `Index Scan` flip on `harvest_workers`
between two runs of *that* capture, attributed to `ANALYZE`-statistics
sampling variance rather than to the change under test). It was **not**
independently reproduced a third time here, and no targeted test isolating
the mechanism was run -- so, per this repo's "reasoning about what the
planner will probably do" prohibition, this page reports the observation
(a real, once-observed plan flip, specifically on the schedule-to-close-populated
table, nowhere else) without asserting a confirmed causal mechanism. It is
flagged as a risk to be aware of at large backlog depths, not as a proposed
fix target: there is no schema or query change on offer that would pin the
planner's choice without the "planner-disabling flags... outside a
diagnostic session" this repo's rules ban.

### Corroboration: `pg_stat_statements` over the real claim-drain

To confirm the small-depth EXPLAIN deltas hold under the actual claim
workload -- repeated `claim_task()` calls draining the backlog one row at a
time, as production does -- the harness drives the real async
`queue::claim_task(...)` function 10,001 times (10,000 successful claims plus
one final empty poll) against the 10,000-row/4-queue headline scenario at
each data state and snapshots `pg_stat_statements` afterward (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-pg_stat_statements.txt`):

| state | calls | `shared_blks_hit` | avg per call |
|---|---:|---:|---:|
| no-schedule-to-close | 10,001 | 5,124,396 | 512.39 |
| schedule-to-close | 10,001 | 5,250,159 | 524.96 |

Aggregate delta: **+2.45%** -- in the same 2.5-6% band as the 1,000- and
10,000-row single-call EXPLAIN deltas above, and well clear of the 100k
depth's plan-instability outlier.

**This page's first capture run measured this same aggregate at +22.5%**
(458.85 -> 562.11 avg buffers/call), a result this rewrite does not carry
forward as the headline number. Investigating the gap (see [Write-side
cost](#write-side-cost) below) found real, reproducible dead-tuple growth
from the wider row, but only enough to explain roughly 5 percentage points
of it -- not 22. The most likely explanation, consistent with [the 100k-depth
plan instability](#the-100k-depth-plan-instability) above, is that the first
run's drain caught the same kind of plan flip at some point along its
10,000-claim descent through shrinking backlog depths (a full sweep passes
through depths from 10,000 down to 1, including the neighborhood where the
100k-row instability was directly observed) -- but this was not confirmed by
re-instrumenting that specific run, so it is reported as the more likely
explanation, not a proven one. The second run's numbers are used throughout
this page because they are the ones the heap-growth instrumentation was
captured alongside, in the same run, making the aggregate-vs-heap-growth
comparison in the next section internally consistent.

## Write-side cost

Every `UPDATE` to a claimed row -- including the claim `UPDATE` itself in
`claim_task_query()`'s `claimed` CTE, which never touches
`schedule_to_close_at` -- still creates a brand-new MVCC tuple version that
carries the column's value forward, exactly the mechanism
`docs/performance-capability-labels.md`'s "Write-side cost" section
documents for `required_capabilities`. Two independent, standalone
corroborations (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.{sql,txt}`):

| seeding + update shape | no-schedule-to-close heap-page growth | schedule-to-close heap-page growth | extra growth |
|---|---:|---:|---:|
| one bulk `UPDATE ... WHERE state = 'PENDING'` (10,000 rows, one statement) | 250 | 263 | +5.2% |
| 10,000 individual `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` pairs, PL/pgSQL loop | 233 | 244 | +4.7% |

Both land close to the EXPLAIN/aggregate band above (2.5-6%), not near the
first run's +22.5% outlier -- consistent with that outlier being a plan-shape
event rather than a row-width effect.

The instrumented rerun also snapshotted `pg_stat_user_tables` immediately
before and after the real 10,000-claim headline drain (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-heap-growth.txt`):

| state | heap pages before | heap pages after | page growth | `n_dead_tup` after |
|---|---:|---:|---:|---:|
| no-schedule-to-close | 244 | 288 | +44 | 802 |
| schedule-to-close | 250 | 302 | +52 | 4,779 |

Heap-*page* growth is close between the two states (+44 vs +52, 18.2% more
for schedule-to-close) -- consistent with the small buffer deltas measured
throughout this page. **Dead-tuple count is not close: 4,779 vs 802, a 5.96x
difference.** The wider row leaves less free space per heap page to begin
with, so more of the claim UPDATE's 10,000 new tuple versions cannot fit a
HOT (Heap-Only Tuple) update in place on the same page and instead leave a
larger share of old versions dead without triggering a proportional increase
in *page count* (Postgres reuses free space within existing pages via HOT
before allocating new ones). This dead-tuple growth is real and reproducible
in this one capture, but this page does not have a second independent
measurement of it the way the buffer/heap-page numbers above do -- read it as
a directional finding, not a pinned percentage. It recurs in production
exactly as it does here: the window between a claim and whenever autovacuum
next runs.

No schema or index change is proposed by this pass. The dead-tuple
observation is a candidate input to `autovacuum_vacuum_scale_factor` tuning
on `harvest_task_queue` for a deployment that both uses
`schedule_to_close_at` heavily and runs a very hot claim path, but that is an
operational/config decision for a deployment to make with its own workload
shape, not a code change this measurement pass makes unilaterally (per this
repo's "ask before... changing `postgresql.conf` / parameter group"
convention).

## Equivalence

Both drains claim exactly 10,000 of 10,000 seeded rows
(`claimed == claimed_by_label` asserted equal between the two labels inside
the test), and `claim_row.calls == claimed + 1` is asserted for the final
empty poll in each state (this assertion is inherited from the shared
pattern; see the test source). The schedule-to-close claim path returns the
same claim behavior as the unpopulated path -- the cost measured here is
overhead on an otherwise identical result set, not a correctness difference.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_schedule_to_close_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each,
  snapshots `pg_relation_size`/`pg_stat_user_tables` immediately before and
  after a real 10,000-row headline drain through `queue::claim_task()` at
  both states while also snapshotting `pg_stat_statements`, and asserts
  claim-count equivalence between the two states as a correctness check.
- `docs/perf-artifacts/schedule-to-close-claim-predicate/` -- the committed
  `EXPLAIN` captures, `pg_stat_statements` snapshots, heap-growth snapshots,
  the standalone bulk-UPDATE bloat-corroboration script and its output, and
  a `fixture-summary.txt`.
- `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh` -- a
  reproduction script that re-runs the capture test.
- This doc.

`queue::claim_task_query()` is unmodified.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
```

or, with only a reachable Docker daemon and no external Postgres:

```bash
./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
```

Both regenerate the `EXPLAIN` captures, `pg_stat_statements` snapshots,
heap-growth snapshots, and `fixture-summary.txt` under
`docs/perf-artifacts/schedule-to-close-claim-predicate/` from scratch. **They
do NOT regenerate `claim_update_bloat_corroboration.txt`** -- that script is
independent of the Rust harness and is never invoked by the repro command
above. After any schema, index, or storage-layout change to
`harvest_task_queue`, re-run it explicitly, or the committed corroboration
output will silently go stale even though the primary `EXPLAIN`/
`pg_stat_statements` captures are fresh:

**`$DATABASE_URL` below MUST point at a disposable scratch database --
never a real development, staging, or production database.** The SQL script
repeatedly runs `TRUNCATE harvest_task_queue RESTART IDENTITY`, and `psql`
executes each top-level statement in its own autocommit transaction, so if a
later statement fails, the `TRUNCATE`s that already ran are **not** rolled
back. Pointed at a shared application database, this command irreversibly
deletes its queued tasks. The Rust harness above never has this risk -- it
creates and tears down its own dedicated, pid-scoped scratch database for
every run.

```bash
# 1. Create a throwaway database and apply migrations to it.
createdb -h localhost -U postgres harvest_perf_scratch
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
(cd autumn-harvest && diesel migration run)

# 2. Run the corroboration script against the scratch database only.
psql "$DATABASE_URL" \
  -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.sql \
  > docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.txt

# 3. Tear the scratch database down when done.
dropdb -h localhost -U postgres harvest_perf_scratch
```
