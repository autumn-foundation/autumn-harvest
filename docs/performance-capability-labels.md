# Capability-labels claim predicate: measured, no query-shape fix identified

`docs/performance.md`'s "Known limitations" section flagged the capability-labels
claim predicate (issue #382) as the one piece of `queue::claim_task_query()`
left unmeasured by the earlier claim-path passes: "the one whose real cost is
least predictable from the query text, and the most defensible next scenario
to add. The seed leaves `required_capabilities` null." This page is that
measurement.

The result is a **negative finding, not an optimization**: the predicate has a
real, non-trivial buffer cost on the claim query (roughly +30-37% across the
tested backlog depths, corroborated three independent ways below), driven by
heap-page growth from a wider stored column, not a plan inefficiency -- there
is no query rewrite, index, or `MATERIALIZED` hint that removes it without
changing what issue #382 stores. That cost also recurs on the write side: it
is not paid once at task-schedule time, but again on every subsequent
`UPDATE` to a claimed row (the claim itself, completion or failure, and any
retry), since Postgres MVCC copies the wider column value forward into each
new tuple version -- measured directly under [Write-side
cost](#write-side-cost) below. Per the acceptable-outcomes rule
("optimization PR, findings issue, negative result -- all are successful
runs; do not open a PR to demonstrate activity"), this ships as a measured,
reproducible finding: a harness, committed baseline artifacts, and this
writeup, with no code change to `queue.rs`.

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
  and the single claiming worker (`{bench-prefix}-worker-0`, the worker id
  the harness actually drives `claim_task()` as) given
  `labels = '{"region":"us-east"}'::jsonb` so every row is claimable (a
  worst case for wasted claim work would be *unsatisfiable* requirements;
  this measures the predicate's cost when it is doing useful, matching
  work, which is the common case). Only that one worker's labels matter
  here: `claim_task_query()`'s `worker_info` CTE resolves labels via
  `SELECT labels FROM harvest_workers WHERE worker_id = $1` -- a
  single-row, worker-id-keyed lookup on the claiming worker's own id, not a
  scan or join over the other 63 seeded (but never-claiming, and here
  unlabeled) workers -- so what labels those other workers carry has no
  effect on this query's plan or measured buffer cost.

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each depth, and for a full
`pg_stat_statements` drain of the real `queue::claim_task()` async function
(not literal-substituted SQL) against the 10,000-row/4-queue headline
scenario, claiming every row one call at a time.

## Plan

At backlog=10,000, the two plans are identical in shape (same join order, same
CTE structure, and -- since a harness fix described in [Review
note](#review-note-an-analyze-asymmetry-the-harness-carried-caught-by-review)
below -- the same `Seq Scan on harvest_workers` access path for all four
aliased `worker_info`-CTE lookups) and differ only in per-node buffer counts:

- The `Seq Scan on harvest_task_queue` node reports 244 -> 338 total buffers
  (delta 94) -- almost exactly the query's whole top-level delta at this
  depth (94 of 94; see Measurement below). That 338 is a **cumulative**
  subtree total, not the scan's own exclusive read cost: it already includes
  the nested `SubPlan 10`/`InitPlan`s discussed next, since Postgres reports
  `EXPLAIN` buffer usage inclusively at every ancestor node. Of the 94-buffer
  delta, 90 buffers are the scan reading the wider
  `required_capabilities`-bearing tuples directly off the heap, and 4 are
  `SubPlan 10`'s own cumulative contribution -- the exact split is derived
  below, once `SubPlan 10`'s buffer figure has been introduced.
- `SubPlan 10` (the correlated `jsonb_array_elements(t.required_capabilities)`
  requirement check) reports `(never executed)` under `no-capabilities` --
  the planner short-circuits it entirely when the column is `NULL` -- and
  `(actual rows=0 loops=10000)` under `capability-labels`: it genuinely runs
  once per candidate row when the column is populated. Its own direct
  *buffer* cost is small -- `Buffers: shared hit=4` on the `capability-labels`
  side -- because it operates on JSONB data the outer scan already pulled
  into memory; it adds essentially zero *extra* buffer reads beyond what the
  wider scan already paid for. (This is a buffer-scoped statement; see the
  CPU-cost caveat under [Why no fix is
  proposed](#why-no-fix-is-proposed) below.)
- The nested `InitPlan 6`/`InitPlan 7` (the worker-label `Exact`-branch
  lookups against `harvest_workers`) show `(never executed)` under
  `no-capabilities` -- inherited directly from `SubPlan 10` itself never
  executing when the column is `NULL` -- and `(actual rows=1 loops=1)`,
  `Buffers: shared hit=2` each, under `capability-labels`: despite being
  lexically nested inside the row-driven `SubPlan 10` (which runs
  `loops=10000` times there), Postgres recognizes they are uncorrelated to
  the outer row and computes them once, caching the result and reusing it
  across all 10,000 `SubPlan` invocations. `InitPlan 8`/`InitPlan 9` (the
  `In`-branch lookups) show `(never executed)` in *both* states: under
  `no-capabilities` because the whole `SubPlan` never runs, and under
  `capability-labels` because the sole seeded `Exact` requirement resolves
  the requirement's `OR` first, short-circuiting evaluation of the `In`
  branch.

The mechanism is therefore **row-width growth**, not subplan or InitPlan
inefficiency: a wider `required_capabilities` JSONB payload means fewer rows
fit per heap page, so the same 10,000-row/4-queue scan touches more pages.
Nothing about the join, the CTE structure, or the InitPlan caching behavior
changes between the two states -- and the whole label-matching machinery
accounts for only 4 of the 94-buffer total delta at this depth, under 5% of
it. `SubPlan 10`'s own reported `Buffers: shared hit=4` is *already*
cumulative over its subtree -- Postgres reports buffer usage inclusively at
every ancestor node in an `EXPLAIN` tree, the same convention it uses for
"Actual Total Time" -- so that figure already counts `InitPlan 6`/`7`'s
`shared hit=2` each; it is not 4 *in addition to* their 2+2. Subtracting
`SubPlan 10`'s total (4) from the `Seq Scan`'s own total (338) leaves 334 --
i.e. the scan's own, exclusive heap-read cost grows 244 -> 334 (delta 90),
not 244 -> 338 (delta 94). That isolated 334-page figure exactly matches the
334-heap-page figure independently derived via a `TRUNCATE`+re-INSERT
`pg_relation_size` measurement on this identical 10,000-row/4-queue shape
(see the code comment on the seeding helper in `claim_budget_tests.rs`),
which corroborates that the label-matching machinery contributes only 4
buffers here, not 8.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-capabilities` vs `capability-labels`, at each `BACKLOG_SWEEP` depth
(artifacts: `docs/perf-artifacts/capability-labels-claim-predicate/{no-capabilities,capability-labels}-claim-backlog-{depth}.explain.txt`):

| backlog | no-capabilities buffers | capability-labels buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 69 | +16 | +30.2% |
| 10,000 | 274 | 368 | +94 | +34.3% |
| 100,000 | 2,473 | 3,371 | +898 | +36.3% |

The delta *percentage* grows with backlog size (+30.2% -> +34.3% -> +36.3%)
rather than shrinking, which is consistent with a per-row storage-width
effect rather than a fixed per-query overhead that would amortize away at
scale.

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
| no-capabilities | 10,001 | 5,092,628 | 509.21 |
| capability-labels | 10,001 | 7,464,424 | 746.37 |

Aggregate delta: **+46.57%**, in the same range as the single-call `EXPLAIN`
delta (+34.3%) at the same depth and the isolated-table `pg_relation_size`
delta (+42.7%) above. Three independent measurement methods -- a single-call
EXPLAIN, a direct heap-page-count comparison on the isolated table, and an
aggregate `pg_stat_statements` snapshot over a 10,001-call production-code
drain -- all land in the same ~34-47% band and none contradicts the others in
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
amplification from *this* pass to weigh -- but issue #382's existing design
carries a recurring write cost worth stating precisely, corrected here from
an earlier draft of this page (flagged by review on PR #1192): the wider
`required_capabilities` column is populated once, at task-schedule `INSERT`
time, but its cost is **not paid only once**. Every subsequent `UPDATE` to
that row -- the claim `UPDATE` in `claim_task_query()`'s `claimed` CTE, and
any later terminal-state transition or retry requeue -- creates a brand-new
MVCC tuple version that carries the full, unchanged column value forward,
whether or not that particular `UPDATE` ever touches `required_capabilities`
itself.

This was measured directly, in two shapes -- the second added after a
review round on PR #1192 caught a confound in the first (see below). Both
apply a claim-shaped `UPDATE` (setting `state`, `worker_id`, `started_at` --
never `required_capabilities`) 10,000 times to a freshly seeded, freshly
`VACUUM`ed 10,000-row table, and differ only in how those 10,000 claims are
committed.

The **production-representative** shape commits each claim in its own
transaction, matching `claim_task()`'s real one-call-per-commit pattern.
Across five runs it costs 50-55 heap pages of growth when the rows carry no
capability requirements, and 57-60 pages when they do (artifacts:
`docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_separate_transactions_corroboration.{sql,txt}`)
-- **roughly +9% to +16% more heap growth from the identical claim
operation** (mean ~+14% across the five runs), purely from carrying the
wider payload forward into the new tuple version.

A second, deliberately **non**-representative shape applies all 10,000
claims as a single set-based `UPDATE` inside one transaction (artifacts:
`docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_corroboration.{sql,txt}`)
and costs 250 and 344 pages respectively -- **+37.6%**. Review correctly
flagged this bulk-transaction figure as a confound with the production
mechanism: within one uncommitted transaction, a tuple version killed
earlier in that same transaction cannot be reclaimed by a later row
processed in it -- its `xmax` belongs to a transaction that has not yet
committed, so it is categorically unprunable, by either opportunistic HOT
pruning or autovacuum, until that transaction commits -- whereas a loop of
separately-committed claims (the real production pattern) opens a reclaim
window between every commit that opportunistic pruning can use. The
bulk-transaction figure still measures something real -- it is the worst
case a batch operation touching many rows inside one explicit transaction
(a mass backfill, a migration script, an admin tool) would see -- but it is
not representative of the per-claim production path this design actually
adds cost to, roughly overstating that cost by 2.4-4.1x (37.6% divided by
the 9.1-15.7% range measured above). The
separate-transaction measurement above is this section's headline figure;
the bulk-transaction one is included only as an explicitly-scoped upper
bound.

This bloat is **not fully transient**: `VACUUM` (autovacuum, in production)
marks the superseded tuple versions' space as reusable for future writes to
this same table, but it does not compact live rows together or shrink the
relation's on-disk size -- ordinary `VACUUM` only truncates pages that are
entirely empty at the table's physical tail, which a continuously-churning
task queue essentially never has. Some of the heap-page growth measured
above is reclaimed the moment a later write happens to land on a page
holding one of these now-dead tuples -- exactly the mechanism that makes
the separate-transaction figure smaller than the bulk-transaction one -- but
growth that is *not* reclaimed that way persists past any number of
ordinary vacuum cycles, not just in a window before the next vacuum runs:
it recurs on every state transition a task row goes through (claim, then
completion or failure, and again on every retry), and the resulting extra
pages stay part of the table's physical footprint -- inflating every
*future* `Seq Scan`'s buffer cost too, not just the write path that created
them -- until either later writes happen to land in and reuse that specific
freed space, or a full table rewrite (`VACUUM FULL`, `CLUSTER`,
`pg_repack`) runs. Deployments that never populate `required_capabilities`
pay none of this -- the `no-capabilities` baseline in this doc *is* every
existing claim-path benchmark in this crate.

## Why no fix is proposed

The measured cost is heap-page growth from a wider stored column, evaluated
by a `Seq Scan` that already reads every candidate row regardless of
`required_capabilities` -- it is not a plan inefficiency SQL can route
around:

- The correlated `jsonb_array_elements` `SubPlan 10` itself measures
  `Buffers: shared hit=4` at backlog=10,000 -- and that figure is already
  cumulative over its subtree (Postgres reports buffer usage inclusively at
  every ancestor `EXPLAIN` node, not per-node-exclusively), so it already
  counts its two executed `InitPlan`s (6 and 7, `shared hit=2` each) rather
  than adding to them. The whole label-matching machinery therefore
  accounts for only 4 of the 94-buffer total delta at this depth, under 5%
  of it -- corroborated by `338 - 4 = 334`, matching an independently
  measured 334-heap-page `pg_relation_size` figure for this identical
  10,000-row/4-queue shape. Rewriting the `SubPlan` would not touch the
  dominant *I/O* cost, which is the `Seq Scan`'s own page count.
- The nested worker-label `InitPlan`s are already resolved as cheaply as
  they can be in each state: `InitPlan 6`/`7` are `(never executed)` under
  `no-capabilities` (the whole `SubPlan` never runs) and cached at
  `loops=1` under `capability-labels` (uncorrelated to the outer row,
  computed once despite 10,000 `SubPlan` invocations); `InitPlan 8`/`9` are
  `(never executed)` in both states. There is no `MATERIALIZED` /
  `NOT MATERIALIZED` CTE hint to reach for, because the CTE's own evaluation
  strategy is not where the cost lives; this caching behavior was directly
  confirmed in the captured `EXPLAIN` output rather than assumed.
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

**Scope of this conclusion.** Every measurement on this page is I/O-scoped
(`EXPLAIN (..., TIMING OFF)`, buffers/rows/plan-shape only, per this
persona's rule against wall-clock timing as primary evidence), so "no fix
is proposed" above is an *I/O*-cost conclusion, not a *CPU* one -- this pass
did not separately measure the CPU cost of parsing and comparing JSONB once
per candidate row across `SubPlan 10`'s 10,000 loop iterations (flagged by
review on PR #1192; a `TIMING ON` capture was considered and rejected as
evidence, since single-shot execution-time deltas on a workload this small
are exactly the kind of noisy, non-buffer signal this persona's evidentiary
rules exist to exclude from a conclusion -- the review comment itself
observed the one-shot totals are noisy enough to show the
`capability-labels` plan as *faster* in the committed 10k artifacts).
Whatever that CPU cost is, it is inherent to the feature: any query shape
checking "does this row's capability requirement match the claiming
worker's labels" has to parse and compare JSONB once per populated-column
candidate row, so it is not a rewritable query-shape inefficiency -- it is
the unavoidable cost of the check running at all, same in kind as the I/O
cost measured above.

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

## Review note: an `ANALYZE` asymmetry the harness carried, caught by review

The evidence-capture test seeded `harvest_task_queue` identically for both
labels but ran `ANALYZE harvest_workers` only on the `capability-labels`
side, since only that branch mutates the claiming worker's `labels` column
and has to re-analyze after doing so. The `no-capabilities` side ran against
whatever `harvest_workers` statistics happened to survive from a previous
iteration, or none at all.

For a ~64-row table, the planner's choice between an `Index Scan` and a
`Seq Scan` for the `worker_info` CTE's single-row equality lookup is
genuinely statistics-sensitive rather than obviously favoring one shape --
and the committed backlog=10,000 artifacts confirmed it mattered in
practice: the first capture showed `no-capabilities` using an `Index Scan
using harvest_workers_pkey` while `capability-labels` used a `Seq Scan` for
the identical logical lookup, an unrelated access-path difference leaking
into what is meant to be a pure `required_capabilities`-population
comparison. This was flagged by review on PR #1192, not caught by this
pass's own pre-publication checks.

Fix: `ANALYZE harvest_workers` runs once, symmetrically, before either
label's capture in both the backlog-sweep `EXPLAIN` loop and the
`pg_stat_statements` stat-snapshot loop, so both labels start from
identical `harvest_workers` statistics regardless of iteration order or
prior state. Both now use a `Seq Scan on harvest_workers` for all four
aliased `worker_info`-CTE lookups at every depth (verified directly against
the regenerated artifacts, not assumed). The fix's effect on the headline
numbers was modest -- it corrected an access-path discrepancy on a small,
unrelated table, not the row-width mechanism this page is about -- and the
figures throughout this page are from the corrected harness.

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
  `pg_relation_size`/`pg_column_size` row-width corroboration script and its
  output, the standalone claim-time-`UPDATE` heap-growth corroboration
  script and its output, and a `fixture-summary.txt` for both data states at
  all three depths.
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

Both regenerate the `EXPLAIN` captures, `pg_stat_statements` snapshots, and
`fixture-summary.txt` under
`docs/perf-artifacts/capability-labels-claim-predicate/` from scratch.
**They do NOT regenerate the three standalone SQL corroboration outputs**
(`pg_relation_size_corroboration.txt`,
`claim_update_bloat_corroboration.txt`, and
`claim_update_bloat_separate_transactions_corroboration.txt`) -- those
scripts are independent of the Rust harness and are never invoked by either
repro command above. After any schema, index, or storage-layout change to
`harvest_task_queue` or `harvest_workers`, re-run these three commands
explicitly as well, or the committed corroboration `.txt` outputs will
silently go stale (reflecting the old layout) even though the primary
`EXPLAIN`/`pg_stat_statements` captures are fresh. Note that
`claim_update_bloat_separate_transactions_corroboration.sql` commits 10,000
separate transactions and its own dead-tuple reclamation therefore depends
on autovacuum/opportunistic-pruning timing, so its exact page counts vary
slightly run to run (see the range noted in its `.txt` artifact and in the
[Write-side cost](#write-side-cost) section) -- this is expected, not a
reproduction failure.

**`$DATABASE_URL` below MUST point at a disposable scratch database --
never a real development, staging, or production database.** All three SQL
scripts repeatedly run `TRUNCATE harvest_task_queue RESTART IDENTITY`, and
`psql` executes each top-level statement in its own autocommit transaction,
so if a later statement in a script fails, the `TRUNCATE`s that already ran
are **not** rolled back. Pointed at a shared application database, these
commands irreversibly delete its queued tasks. The Rust harness above never
has this risk -- it creates and tears down its own dedicated, pid-scoped
scratch database (`harvest_claim_bench_<pid>_<token>_<seq>`, see
`claim_bench_support.rs`) for every run. Do the equivalent by hand before
invoking any of the three scripts directly:

```bash
# 1. Create a throwaway database and apply migrations to it -- do this
#    ONCE, then reuse the same scratch database for all three scripts below.
createdb -h localhost -U postgres harvest_perf_scratch
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
(cd autumn-harvest && diesel migration run)

# 2. Run the corroboration scripts against the scratch database only.
psql "$DATABASE_URL" \
  -f docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.sql \
  > docs/perf-artifacts/capability-labels-claim-predicate/pg_relation_size_corroboration.txt

psql "$DATABASE_URL" \
  -f docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_corroboration.sql \
  > docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_corroboration.txt

psql "$DATABASE_URL" \
  -f docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_separate_transactions_corroboration.sql \
  > docs/perf-artifacts/capability-labels-claim-predicate/claim_update_bloat_separate_transactions_corroboration.txt

# 3. Tear the scratch database down when done.
dropdb -h localhost -U postgres harvest_perf_scratch
```
