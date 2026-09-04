# Worker-sessions claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Known limitations" section named worker sessions
(issue #606), alongside `schedule_to_close` (#378) and sticky routing (#235),
as claim-path predicates left unmeasured by the earlier passes: "cheap inline
column tests, against columns the seed leaves null." PR #1339 closed that gap
for `schedule_to_close`, confirming it cheap (+3.6% to +5.7% buffers, well
under the impact floor), and narrowed the remaining unmeasured set to worker
sessions and sticky routing. This page is the worker-sessions measurement.

The result is **not** the same as `schedule_to_close`'s: this predicate has a
real, non-trivial buffer cost (+21.9% at the 10,000-row headline depth,
corroborated by an 18,000-call real-drain aggregate at +19.0%) -- closer in
size to the capability-labels finding (+34.3%) than to `schedule_to_close`'s
(+3.6%). The mechanism is the same one `docs/performance-capability-labels.md`
documents: row-width growth from populating previously-`NULL` columns, not a
plan inefficiency. There is no query-shape fix, because the `WHERE` clause
already evaluates the predicate as a plain inline test on a row the scan reads
regardless. Per the acceptable-outcomes rule ("optimization PR, findings
issue, negative result -- all are successful runs"), this ships as a measured,
reproducible finding with no code change to `queue.rs`.

## Workload

`claim_task_query()`'s `candidate` CTE gates every row with a plain inline
test: `session_id IS NULL OR sticky_worker_id = $1` -- no subquery, no join.
Issue #606's worker-sessions feature (`queue::EnqueueParams::with_session_id`)
always sets this column together with ordinary sticky routing's
`sticky_worker_id`/`sticky_until` pair (`queue.rs`'s `with_session_id` doc
comment: "the claim query hard-pins this row to `sticky_worker_id`"), so a
real worker-session row always carries all three columns non-`NULL` at once --
unlike `schedule_to_close_at`, which is a single added column in isolation.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
seeds `claim_bench_support`'s standard 4-queue backlog at the published
`BACKLOG_SWEEP` depths (1,000 / 10,000 / 100,000 rows) in two data states,
identical in every other column:

- **`no-session`** -- `session_id` and `sticky_worker_id` both `NULL`
  (today's default seed shape, and every other `ClaimGate` scenario's shape
  too).
- **`worker-session`** -- every row seeded with `sticky_worker_id` set to the
  claiming worker's own id (`{bench-prefix}-worker-0`), `sticky_until` one
  hour in the future, and a fresh `session_id`. Setting `sticky_worker_id =
  $1` makes both the session predicate *and* the pre-existing ordinary-sticky
  predicate directly above it (`sticky_worker_id IS NULL OR sticky_worker_id
  = $1 OR ...`) evaluate `TRUE` for every row, so the claimable row count is
  **identical** between the two labels -- the same match-not-exclude
  isolation technique the capability-labels capture uses.

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each depth, and for a full
`pg_stat_statements` drain of the real `queue::claim_task()` async function
against the 10,000-row/4-queue headline scenario, claiming every row one call
at a time (10,001 calls: 10,000 successful claims plus the terminal empty
poll).

Rows are re-seeded via `TRUNCATE` + a fresh `INSERT ... SELECT` carrying the
three columns populated from birth, not by `UPDATE`-ing already-seeded rows --
the same MVCC-bloat pitfall the capability-labels capture found and fixed in
its own harness (an `UPDATE` leaves dead `NULL`-column tuple versions
resident in the heap, inflating the measured page count with an artifact that
never occurs in production, where these columns are set once at enqueue
time).

## Plan

At backlog=10,000 the two plans are structurally identical -- same `Seq Scan`
on `harvest_task_queue`, same join order, same CTE structure -- and differ
only in buffer counts:

```text
no-session:      Seq Scan on harvest_task_queue  Buffers: shared hit=244  (actual rows=10000 loops=1)
worker-session:   Seq Scan on harvest_task_queue  Buffers: shared hit=304  (actual rows=10000 loops=1)
```

The `Seq Scan` node's own delta (244 -> 304, +60) **exactly matches** the
whole query's total delta at this depth (274 -> 334, +60) -- the entire cost
is inside the scan reading wider tuples off the heap, nothing leaks into any
other node (the CTE structure, join order, and every other subplan's
`Buffers:` figure are unchanged between the two captures). This is the same
signature `docs/performance-capability-labels.md`'s Plan section documents
for its own predicate, and it rules out a plan-shape explanation the same way:
`session_id IS NULL OR sticky_worker_id = $1` is a `Filter:` clause evaluated
row-by-row during the scan itself, not a separate `SubPlan`/`InitPlan` node
with its own cost to isolate.

### The 100,000-row depth is not comparable

At 100,000 rows the two captures took **different plan shapes** --
`no-session` chose `Index Scan using idx_harvest_tq_poll` (9,878 total
buffers), `worker-session` chose a plain `Seq Scan` (3,066 total buffers) --
so the raw delta at this depth (-69.6%) does not isolate the predicate's cost;
it isolates two different access strategies for the *same* underlying table.
This is the identical instability `docs/performance-schedule-to-close.md`
documents at the same depth for a different predicate ("the planner chose a
markedly more expensive plan... in 2 of 3 runs") and the same mechanism
`docs/perf-artifacts/queue-pause-claim-anti-join/`'s 100k row discusses (the
planner switching between an ordered index scan and a raw sequential scan
once the table is large enough that reading it sequentially beats walking the
index). No causal mechanism for *why* this particular run picked one shape
over the other is claimed here -- consistent with this persona's rule against
reasoning about what the planner "will probably do." The 1,000- and
10,000-row depths are the reliable, population- and plan-shape-matched
comparison; the 100,000-row row is reported for completeness and flagged, not
trusted.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-session` vs `worker-session` (artifacts:
`docs/perf-artifacts/worker-session-claim-predicate/{no-session,worker-session}-claim-backlog-{depth}.explain.txt`):

| backlog | no-session buffers | worker-session buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 56 | 59 | +3 | +5.4% |
| 10,000 (headline) | 274 | 334 | +60 | **+21.9%** |
| 100,000 ⚠ | 9,878 | 3,066 | -6,812 | not comparable -- plan-shape change, see above |

### Corroboration: `pg_stat_statements` over the real claim-drain

The `EXPLAIN` numbers above are single-call snapshots. To confirm the effect
holds under the actual claim workload, the harness also drives the real async
`queue::claim_task(...)` function 10,001 times against the 10,000-row/4-queue
headline scenario at each data state and snapshots `pg_stat_statements`
afterward (artifacts:
`docs/perf-artifacts/worker-session-claim-predicate/{no-session,worker-session}-pg_stat_statements.txt`):

| state | calls | `shared_blks_hit` | avg per call |
|---|---:|---:|---:|
| no-session | 10,001 | 5,136,725 | 513.62 |
| worker-session | 10,001 | 6,110,253 | 610.96 |

Aggregate delta: **+18.95%** -- within about three points of the single-call
`EXPLAIN` delta at the same depth (+21.9%), the same "consistent in direction
and order of magnitude" bar the capability-labels page sets, not exact
numeric agreement.

### A row-width gradient across three measured predicates

Read alongside the two prior passes, the three buffer-cost figures at the
10,000-row headline depth line up with how many bytes of previously-`NULL`
column each predicate's feature populates:

| predicate | columns populated | approx. added bytes/row | buffer delta @ 10k |
|:--|:--|--:|--:|
| `schedule_to_close` (#378) | one `TIMESTAMPTZ` | ~8 | +3.6% |
| worker sessions (#606) | `TEXT` + `TIMESTAMPTZ` + `UUID` | ~47 | **+21.9%** |
| capability labels (#382) | one JSONB payload | ~70 | +34.3% |

This is offered as a consistency check across this persona's own prior
findings, not as a validated linear model -- the three measurements differ in
column type, seed shape, and capture run, and no regression or controlled
byte-count sweep was run to confirm proportionality. What it does establish is
that worker sessions' cost is not an anomaly: it sits where the row-width
mechanism predicts relative to the other two already-published points, which
is why the same "no fix identified" conclusion applies.

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
amplification to weigh from this pass -- but the same `pg_stat_statements`
capture that produced the read-side numbers above also captured the
headline-scenario seed `INSERT` for each label, since the harness seeds via a
single set-based `INSERT ... SELECT FROM generate_series` (artifacts: same
`pg_stat_statements.txt` files referenced above):

| state | rows | `shared_blks_hit` |
|---|---:|---:|
| no-session `INSERT` | 10,000 | 136,706 |
| worker-session `INSERT` | 10,000 | 175,338 |

Delta: **+28.3%** more buffers to write the identical row count -- consistent
in direction and rough magnitude with the read-side deltas above, and
expected from the same row-width mechanism: a wider row costs more to write
as well as to scan. This column data is written once at enqueue time in
production (`with_session_id`/`with_sticky` are set at `EnqueueParams`
construction, never mutated afterward), so -- unlike the capability-labels
page's finding about *retroactive* `UPDATE` cost recurring on every claim --
this write cost is paid once per task, not on every subsequent state
transition, since `sticky_worker_id`/`sticky_until`/`session_id` are not
columns any claim, completion, or retry path rewrites.

## Why no fix is proposed

The measured cost is heap-page growth from three wider stored columns,
evaluated by a `Seq Scan` that already reads every candidate row regardless
of `session_id`/`sticky_worker_id` -- not a plan inefficiency SQL can route
around:

- The predicate itself is a plain `Filter:` boolean test with no `SubPlan` or
  `InitPlan` to rewrite -- confirmed directly in the captured `EXPLAIN`
  output, which shows the entire buffer delta landing inside the `Seq Scan`
  node itself (see [Plan](#plan) above).
- There is no `MATERIALIZED`/index/rewrite angle: the columns are read
  directly off the already-scanned row, the same as the pre-existing
  ordinary-sticky-routing predicate immediately above this one in the query
  (`sticky_worker_id IS NULL OR sticky_worker_id = $1 OR ...`), which this
  page does not separately re-measure (see [Known limitations](#known-limitations)
  below).
- Storing `sticky_worker_id`/`session_id` is what issue #606's hard-pin
  design requires -- there is no way to hard-pin a task to a worker session
  without recording which worker and which session on the row itself.

**Scope of this conclusion.** As with the capability-labels page, every
measurement here is I/O-scoped (`EXPLAIN (..., TIMING OFF)`, buffers/rows only)
-- this pass did not separately measure CPU cost, which for a plain inline
boolean test on already-fetched columns is expected to be negligible relative
to the JSONB-parsing cost capability-labels measured, but that expectation was
not independently confirmed.

## Known limitations

- **This measurement does not isolate worker sessions (#606) from ordinary
  sticky routing (#235).** A worker-session row necessarily also sets
  `sticky_worker_id`/`sticky_until`, which ordinary sticky routing (#235,
  still itself unmeasured on its own) sets independently. The `worker-session`
  label's cost therefore includes whatever ordinary sticky routing alone
  would cost plus whatever `session_id` alone adds on top -- this page cannot
  and does not decompose the two. A future pass isolating `session_id` alone
  (`sticky_worker_id`/`sticky_until` left `NULL`) against a sticky-routing-only
  state would be needed to separate them, and is not done here because a
  session-tagged row with no sticky pin cannot occur in production (the two
  are always written together by `with_session_id`).
- **The 100,000-row depth is unusable**, per [above](#the-100000-row-depth-is-not-comparable).
- **The row-width gradient table is a consistency check, not a model.** See
  the caveat directly under that table.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_worker_session_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each, drains
  the real 10,000-row headline scenario through `queue::claim_task()` at both
  states while snapshotting `pg_stat_statements`, and asserts claim-count
  equivalence against ground truth as a correctness check.
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
