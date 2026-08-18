# Capability-labels claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Known limitations" section flagged the capability-labels
claim predicate (issue #382) as the one piece of `queue::claim_task_query()`
left unmeasured by the earlier claim-path passes: "the one whose real cost is
least predictable from the query text, and the most defensible next scenario
to add. The seed leaves `required_capabilities` null." This page is that
measurement.

The result is a **negative finding, not an optimization**: the predicate has a
real, non-trivial buffer cost (+34-37% on the claim query, corroborated three
independent ways below), but the cost is heap-page growth from a wider stored
column, not a plan inefficiency -- there is no query rewrite, index, or
`MATERIALIZED` hint that removes it without changing what issue #382 stores.
Per the acceptable-outcomes rule ("optimization PR, findings issue, negative
result -- all are successful runs; do not open a PR to demonstrate activity"),
this ships as a measured, reproducible finding: a harness, committed baseline
artifacts, and this writeup, with no code change to `queue.rs`.

## Workload

`claim_task_query()`'s `FOR UPDATE SKIP LOCKED` claim CTE joins each
candidate `harvest_task_queue` row against the claiming worker's
`harvest_workers.labels` via a correlated `jsonb_array_elements(t.required_capabilities)`
SubPlan, evaluated once per candidate row, whenever `required_capabilities`
is non-null. In production this column is populated once at task-schedule
time by any workflow using label-based worker routing (issue #382) and never
mutated afterward.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_capability_labels_claim_evidence`
seeds `claim_bench_support`'s standard 4-queue backlog at the published
`BACKLOG_SWEEP` depths (1,000 / 10,000 / 100,000 rows) in two data states,
identical in every other column:

- **`no-capabilities`** -- `required_capabilities` left `NULL` (today's
  default, and what every other claim-path benchmark in this crate already
  measures).
- **`capability-labels`** -- every row seeded with
  `required_capabilities = '[{"Exact":{"key":"region","value":"us-east"}}]'::jsonb`,
  and all 64 seeded workers given `labels = '{"region":"us-east"}'::jsonb` so
  every row is claimable (a worst case for wasted claim work would be
  *unsatisfiable* requirements; this measures the predicate's cost when it is
  doing useful, matching work, which is the common case).

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each depth, and for a full
`pg_stat_statements` drain of the real `queue::claim_task()` async function
(not literal-substituted SQL) against the 10,000-row/4-queue headline
scenario, claiming every row one call at a time.

## Plan

At backlog=10,000, the two plans are identical in shape (same join order, same
CTE structure) and differ only in per-node buffer counts:

- The `Seq Scan on harvest_task_queue` node itself grows from 244 to 338
  buffers (delta 94) -- almost exactly the query's whole top-level delta at
  this depth (94 of 94; see Measurement below). This is the scan reading the
  wider `required_capabilities`-bearing tuples off the heap, not any
  correlated subplan doing extra I/O.
- `SubPlan 10` (the correlated `jsonb_array_elements(t.required_capabilities)`
  requirement check) reports `(never executed)` under `no-capabilities` --
  the planner short-circuits it entirely when the column is `NULL` -- and
  `(actual rows=0 loops=10000)` under `capability-labels`: it genuinely runs
  once per candidate row when the column is populated. Its own direct cost is
  small, because it operates on JSONB data the outer scan already pulled into
  memory; it adds essentially zero *extra* buffer reads beyond what the wider
  scan already paid for.
- The nested `InitPlan 6`/`InitPlan 7` (the worker-label `Exact`-branch
  lookups against `harvest_workers`) show `loops=1` in both states, despite
  being lexically nested inside the row-driven `SubPlan 10` -- they are
  uncorrelated to the outer row and Postgres computes them once, cached, and
  reuses the result across all 10,000 rows. `InitPlan 8`/`InitPlan 9` (the
  `In`-branch lookups) show `(never executed)` in both states, since the
  sole seeded `Exact` requirement resolves the requirement's `OR` first.

The mechanism is therefore **row-width growth**, not subplan or InitPlan
inefficiency: a wider `required_capabilities` JSONB payload means fewer rows
fit per heap page, so the same 10,000-row/4-queue scan touches more pages.
Nothing about the join, the CTE structure, or the InitPlan caching behavior
changes between the two states.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-capabilities` vs `capability-labels`, at each `BACKLOG_SWEEP` depth
(artifacts: `docs/perf-artifacts/capability-labels-claim-predicate/{no-capabilities,capability-labels}-claim-backlog-{depth}.explain.txt`):

| backlog | no-capabilities buffers | capability-labels buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 66 | +13 | +24.5% |
| 10,000 | 274 | 368 | +94 | +34.3% |
| 100,000 | 2,473 | 3,376 | +903 | +36.5% |

The delta *percentage* grows and stabilizes with backlog size (+24.5% ->
+34.3% -> +36.5%) rather than shrinking, which is consistent with a per-row
storage-width effect rather than a fixed per-query overhead that would
amortize away at scale.

### Corroboration 1: `pg_relation_size` / `pg_column_size` (isolating the row-width effect directly)

To confirm the buffer delta really is row-width growth and not some artifact
of the EXPLAIN capture itself, the effect was measured directly via
`pg_relation_size('harvest_task_queue')` (heap pages) and
`pg_column_size(...)` (per-row bytes), comparing three seeding strategies for
10,000 rows against a plain, migrated `harvest_task_queue` table (artifacts:
`docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.{sql,txt}`):

| seeding strategy | heap pages | delta vs. fresh no-caps |
|---|---:|---:|
| fresh `INSERT` with `required_capabilities` left `NULL` | 213 | -- |
| fresh `INSERT` with `required_capabilities` populated at birth | 304 | +42.7% |
| fresh `INSERT` (no caps) **then `UPDATE ... SET required_capabilities`** | 516 | +142.3% |

The third row is a methodology bug this pass found and fixed in its own
harness, documented in full under [Harness note](#harness-note-an-mvcc-methodology-bug-this-pass-found-and-fixed-in-itself)
below -- it is reproduced here (rather than only asserted) to confirm the
MVCC dead-tuple bloat it documents is real: seeding via `UPDATE` after the
fact costs roughly 3.3x the heap-page growth (+142.3% vs. +42.7%) that
seeding the column at `INSERT` time costs, for identical final column
contents.

This table's absolute page counts are not expected to match the EXPLAIN
artifacts' buffer counts one-for-one -- `pg_relation_size` measures only the
isolated `harvest_task_queue` heap, while the EXPLAIN totals cover the whole
claim query (the `harvest_workers`/`harvest_queue_pauses`/`harvest_activity_pauses`
joins, the CTE machinery, and the claim `UPDATE`'s own write cost). What
matters is that the two independent methods agree in **direction and order
of magnitude**: a clean, isolated row-width comparison here shows **+42.7%**
heap growth, and the EXPLAIN-measured buffer delta at the same 10,000-row
depth shows **+34.3%** -- both large, both positive, both consistent with the
plan-node evidence in [Plan](#plan) that this is a row-width effect, not a
plan-shape difference (the two plans are structurally identical; only the
per-node buffer counts differ).

The per-row byte accounting confirms the mechanism further: average row size
with capabilities populated is 237 bytes vs. 165 bytes without (delta 72
bytes), and `avg(pg_column_size(required_capabilities))` over the populated
rows is 70 bytes -- accounting for essentially the entire per-row width
increase.

### Corroboration 2: `pg_stat_statements` over the real claim-drain

The `EXPLAIN` numbers above are single-call snapshots against a static
backlog. To confirm the effect holds under the actual claim workload --
repeated `claim_task()` calls draining the backlog one row at a time, as
production does -- the harness also drives the real async
`queue::claim_task(...)` function 10,001 times (10,000 successful claims plus
one final empty poll) against the 10,000-row/4-queue headline scenario at
each data state and snapshots `pg_stat_statements` afterward (artifacts:
`docs/perf-artifacts/capability-labels-claim-predicate/{no-capabilities,capability-labels}-pg_stat_statements.txt`):

| state | calls | `shared_blks_hit` | avg per call |
|---|---:|---:|---:|
| no-capabilities | 10,001 | 4,722,036 | 472.16 |
| capability-labels | 10,001 | 6,485,642 | 648.50 |

Aggregate delta: **+37.35%**, in the same range as the single-call `EXPLAIN`
delta (+34.3%) at the same depth and the isolated-table `pg_relation_size`
delta (+42.7%) above. Three independent measurement methods -- a single-call
EXPLAIN, a direct heap-page-count comparison on the isolated table, and an
aggregate `pg_stat_statements` snapshot over a 10,001-call production-code
drain -- all land in the same ~34-43% band and none contradicts the others in
direction or order of magnitude. That consistency, not exact numeric
agreement across methods measuring different things, is the evidence bar this
finding clears.

## Equivalence

Both drains claim exactly 10,000 of 10,000 seeded rows (`claimed ==
claimed_by_label` asserted equal between the two labels inside the test), and
`claim_row.calls == claimed + 1` is asserted for the final empty poll in each
state. The capability-labels claim path returns the same claim behavior as
the unlabeled path -- the cost measured here is pure overhead on an otherwise
identical result set, not a correctness difference.

## Write-side cost

No schema or index change is proposed by this pass, so there is no write
amplification to weigh: the 70-byte-per-row JSONB storage cost measured above
is inherent to issue #382's existing design (storing capability requirements
inline on each task row) and is paid once, at task-schedule `INSERT` time,
by every deployment that opts into label-based routing. Deployments that
never populate `required_capabilities` pay nothing -- the `no-capabilities`
baseline in this doc *is* every existing claim-path benchmark in this crate.

## Why no fix is proposed

The measured cost is heap-page growth from a wider stored column, evaluated
by a `Seq Scan` that already reads every candidate row regardless of
`required_capabilities` -- it is not a plan inefficiency SQL can route
around:

- The correlated `jsonb_array_elements` SubPlan itself contributes
  negligible *direct* buffer cost (it reads JSONB already resident in the
  scan's in-memory tuple); rewriting it would not touch the dominant cost,
  which is the Seq Scan's own page count.
- The nested worker-label InitPlans are already cached at `loops=1` /
  `never executed` in both data states -- there is no `MATERIALIZED` /
  `NOT MATERIALIZED` CTE hint to reach for, because the CTE's own evaluation
  strategy is not where the cost lives; the InitPlan-caching behavior is
  identical between states and was directly confirmed in the captured
  `EXPLAIN` output rather than assumed.
- TOAST compression only engages for values well above the ~70-byte payload
  measured here (Postgres's default TOAST threshold is roughly 2 KB), so
  there is no free compression angle at this payload size without changing
  what issue #382 stores -- and a storage-representation change is a schema
  change, which is on this persona's "ask before" list rather than something
  to ship unilaterally off a measurement pass.
- A partial or expression index keyed on `required_capabilities` would not
  help the claim query's dominant cost either: the `Seq Scan` reads
  `PENDING`, `scheduled_at <= now()` rows across all requested queues
  regardless of the JSONB column's presence, so the candidate set an index
  could narrow is unrelated to what grew.

The honest conclusion is that this is real, unavoidable production cost of
storing per-task capability requirements as designed, not a bug in
`claim_task_query()`.

## Harness note: an MVCC methodology bug this pass found and fixed in itself

The first version of this evidence-capture test seeded the
`capability-labels` state by `UPDATE`-ing already-seeded rows in place
(`UPDATE harvest_task_queue SET required_capabilities = ...`) rather than
inserting fresh rows with the column populated from birth. That is
methodologically wrong: Postgres MVCC gives an `UPDATE` a brand-new tuple
version, and the old (`NULL`-caps) version stays physically resident in the
heap until `VACUUM` runs -- which the test never triggers. That inflated the
measured heap-page count with dead-tuple bloat that has nothing to do with
the predicate's real cost and never occurs in production, where
`required_capabilities` is set once at task-schedule `INSERT` time and never
retroactively mutated.

This was caught before any doc page or PR was written, via the same
"corroborated by a buffer/row-count change in the same direction" discipline
this persona applies to every target query, applied to the harness's own
measurement: a direct `pg_relation_size`/`pg_column_size` check against a
seeded database showed the `UPDATE`-based seeding strategy cost roughly 3.3x
the heap-page growth a clean fresh-INSERT comparison produced for the exact
same final column contents (row 3 vs. row 2 of the table in [Corroboration
1](#corroboration-1-pg_relation_size--pg_column_size-isolating-the-row-width-effect-directly)
above, +142.3% vs. +42.7%) -- too large a gap to be storage-width growth
alone. The fix: seed the `capability-labels` state via `TRUNCATE` + a fresh
`INSERT ... SELECT` carrying `required_capabilities` populated inline,
matching `claim_bench_support::seed_backlog()`'s exact column shape for
every other column. The corrected numbers in this doc are from that fixed
harness, re-verified end to end (`cargo fmt`, `cargo clippy -p
autumn-harvest --features db,testing --test integration -- -D warnings`, and
a full successful run against a real local Postgres 16) after the fix.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_capability_labels_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each, drains
  the real 10,000-row headline scenario through `queue::claim_task()` at both
  states while snapshotting `pg_stat_statements`, and asserts claim-count
  equivalence between the two states as a correctness check.
- `docs/perf-artifacts/capability-labels-claim-predicate/` -- the committed
  `EXPLAIN` captures, `pg_stat_statements` snapshots, the standalone
  `pg_relation_size`/`pg_column_size` corroboration script and its output,
  and a `fixture-summary.txt` for both data states at all three depths.
- `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh` -- a
  reproduction script that re-runs the capture test.
- This doc.

`queue::claim_task_query()` is unmodified.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/capability_labels_claim_perf_repro.sh
```

or, with only a reachable Docker daemon and no external Postgres:

```bash
./autumn-harvest/scripts/capability_labels_claim_perf_repro.sh
```

Both regenerate every artifact under
`docs/perf-artifacts/capability-labels-claim-predicate/` from scratch, except
the `pg_relation_size_corroboration.{sql,txt}` pair, which is independent of
the Rust harness -- regenerate it against any migrated database with:

```bash
psql "$DATABASE_URL" \
  -f docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.sql \
  > docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.txt
```
