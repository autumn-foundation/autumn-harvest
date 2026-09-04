# `schedule_to_close_at` claim predicate: measured, confirmed cheap at the row level; a scale-dependent plan risk found alongside

`docs/performance.md`'s "Known limitations" section flagged
`schedule_to_close_at` (issue #378), alongside worker sessions (#606) and
sticky routing (#235), as "cheap inline column tests, against columns the
seed leaves null" -- present in `queue::claim_task_query()` on every claim,
but never measured because `claim_bench_support::db::seed_backlog` never
populates the column. This page is that measurement, for `schedule_to_close_at`
only.

The result **confirms the doc's own suspicion at the row level**: at the two
backlog depths where this environment's measurements reproduced identically
across four independent capture runs (1,000 and 10,000 rows), populating
`schedule_to_close_at` adds a small, real, stable buffer cost to the claim
query -- **+5.7% and +3.6%** respectively -- corroborated by two standalone
MVCC-bloat scripts, one bulk and one per-row (**+5.2%** both, twice). None of
this comes close to the 20% impact floor; no fix is proposed or needed for
the row-level cost.

**Two things this page found alongside that are not as clean, and are
reported as such rather than smoothed into a single headline number:**

1. At the 100,000-row depth, the planner chose a markedly more expensive
   plan for the `schedule_to_close_at`-populated table in **two of four**
   capture runs -- not a rare fluke, a roughly coin-flip outcome in this
   environment, and it was never once observed on the `no-schedule-to-close`
   side. See [The 100k-depth plan
   instability](#the-100k-depth-plan-instability).
2. The real-drain `pg_stat_statements` aggregate and the
   `pg_stat_user_tables` dead-tuple counts varied far more between runs than
   the 1,000-/10,000-row `EXPLAIN` numbers did -- including a 6x swing in
   dead-tuple count for the *same* `no-schedule-to-close` label between two
   runs. See [Corroboration](#corroboration-pg_stat_statements-over-the-real-claim-drain)
   and [Write-side cost](#write-side-cost). This page does not have a
   reliable pinned percentage for either measurement and says so rather than
   reporting whichever run's number looked cleanest.

**A note on why this took four data-bearing runs, not one.** Review on this
page's own PR (#1339) found two real bugs in the harness after runs 1-3 had
already produced the numbers below: the `pg_stat_statements` drain never
`ANALYZE`d `harvest_workers` between labels (risking stale-statistics
contamination of the very comparison this capture exists to make -- the
same class of bug `docs/performance-capability-labels.md` had already found
and fixed once), and the seeded `schedule_to_close_at` deadline
(`NOW() + INTERVAL '1 hour'`) could in principle expire mid-drain on a slow
enough run. Fixing the deadline took two attempts:
`'infinity'::timestamptz` looked like the obvious fix and was tried first,
but it broke the real claim path outright -- `queue::claim_task()`'s
`RETURNING` clause hands the claimed row back to Diesel, which cannot
deserialize Postgres's `infinity` into a `chrono::DateTime<Utc>`, so every
claim on the `schedule-to-close` side of that attempt's drain panicked.
That attempt produced no usable data for either the aggregate or the
heap-growth tables below and is not counted as a run in them; it is the
reason `NOW() + INTERVAL '100 years'` is the value actually shipped (see
[Workload](#workload) for the full story). Run 4, with both the `ANALYZE`
fix and the 100-year deadline, is the first fully clean run and is the
**committed** one -- but runs 1-3's numbers are still reported in the
tables below rather than discarded, since the row-level `EXPLAIN` numbers
neither bug could have touched reproduced identically across all four
regardless.

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
  NOW() + INTERVAL '100 years'` at `INSERT` time. Far enough out that it can
  never elapse no matter how long a capture run takes. This went through two
  wrong iterations before landing here, both caught by review rather than
  shipped silently:
  - `NOW() + INTERVAL '1 hour'` (the original value): Codex review on PR
    #1339 (P2) correctly flagged this as unsafe, since the drain below has
    no overall wall-clock bound and already took ~15 minutes end to end in
    this pass's own environment -- a slower machine or remote database could
    plausibly exceed an hour.
  - `'infinity'::timestamptz` (the first fix attempted): a valid Postgres
    value that compares later than every finite timestamp, so it fixes the
    wall-clock problem above -- but it broke the real claim path outright.
    `queue::claim_task()`'s `claimed` CTE `RETURNING`s the full claimed row,
    including `schedule_to_close_at`, for Diesel to deserialize into a
    `chrono::DateTime<Utc>`; Chrono has no `infinity` sentinel, so every
    claim in the `schedule-to-close` drain panicked with "Tried to
    deserialize a timestamp that is too large for Chrono" the moment this
    was actually run. 100 years is comfortably inside
    `chrono::DateTime<Utc>`'s representable range (roughly to the year
    262,000) while still far exceeding any realistic drain duration.

  Like the capability-labels capture's matching `Exact` requirement, the
  seeded deadline excludes nothing: this isolates the predicate's
  *evaluation* cost from any change in which rows are eligible, and lets the
  drain loop's claimed-row count serve as a correctness check between labels.

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

This capture was run **four data-bearing times** end to end during this
pass, plus one failed attempt that is not counted as a run (see below):

1. The first run, seeding `schedule_to_close_at` as
   `NOW() + INTERVAL '1 hour'` and missing an `ANALYZE harvest_workers` call
   the `pg_stat_statements` drain needed.
2. A rerun after a clippy fix that only hoisted item declarations -- no
   functional change -- same seed value, same missing `ANALYZE`.
3. A third rerun, same seed value, same missing `ANALYZE`. This was the
   state committed when PR #1339 was opened for review.
4. *(Not counted as a run.)* Review on that PR found both bugs above.
   Fixing the `ANALYZE` gap was straightforward; fixing the deadline took
   two attempts. `'infinity'::timestamptz` looked like the obvious fix for
   "can this ever expire mid-drain?" and was tried first, but it broke the
   real claim path outright: `queue::claim_task()`'s `RETURNING` clause
   hands the claimed row back to Diesel, which cannot deserialize
   Postgres's `infinity` into a `chrono::DateTime<Utc>`, so every claim on
   the `schedule-to-close` side of this attempt's drain panicked. It
   produced no usable aggregate or heap-growth data and is excluded from
   every table below.
5. The fourth data-bearing run, with both the `ANALYZE` fix and
   `schedule_to_close_at` seeded as `NOW() + INTERVAL '100 years'` instead.
   The first fully clean run, and the **committed** one.

Where a number reproduced across all four data-bearing runs, this page says
so and treats it as reliable; where it didn't, all four observed values are
reported rather than one being silently picked as canonical.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-schedule-to-close` vs `schedule-to-close` (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-claim-backlog-{depth}.explain.txt`,
run 4 / committed):

| backlog | no-schedule-to-close buffers | schedule-to-close buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 56 | +3 | +5.7% |
| 10,000 | 274 | 284 | +10 | +3.6% |
| 100,000 | 2,473 | 2,537 | +64 | +2.6% |

The 1,000- and 10,000-row rows reproduced to the exact buffer count across
**all four** independent data-bearing runs of this capture (53/56 and
274/284, every time) -- this is the reliable evidence on this page. The
100,000-row row did **not** reproduce consistently -- see the next section,
which is where its real finding lives.

### The 100k-depth plan instability

The 100,000-row depth's `no-schedule-to-close` buffer count was stable
across all four runs (2,473, 2,476, 2,473, 2,473). The `schedule-to-close`
side was not:

| run | no-schedule-to-close | schedule-to-close | delta % | plan for `schedule-to-close` |
|---:|---:|---:|---:|---|
| 1 | 2,473 | 2,537 | +2.6% | `Seq Scan` |
| 2 | 2,476 | 10,128 | +309.0% | `Index Scan using idx_harvest_tq_poll` |
| 3 | 2,473 | 10,125 | +309.4% | `Index Scan using idx_harvest_tq_poll` |
| 4 (committed) | 2,473 | 2,537 | +2.6% | `Seq Scan` |

**Two of four runs show the expensive plan, an even split** -- not a rare
fluke, but not the majority outcome either; call it a coin flip in this
environment rather than "the common case", which an earlier draft of this
page overstated from a 2-of-3 sample. What held across all four: the flip
never once appeared on the `no-schedule-to-close` side. Comparing the plans
directly (`grep`-ed from the committed artifacts) shows the mechanism: in
runs 2 and 3, the `schedule-to-close` side's main candidate-row source used
`Index Scan using idx_harvest_tq_poll` instead of `Seq Scan on
harvest_task_queue` (the plan every `no-schedule-to-close` capture used, in
all four runs, and the plan runs 1 and 4's `schedule-to-close` captures also
used). That index cannot serve the query's `ORDER BY` (the non-indexable
leading `CASE` expression -- see `docs/performance.md`'s TL;DR), so the plan
still pays for a full external-merge sort afterward (`Sort Method: external
merge Disk: 15280kB`, identical across all captures at this depth) *in
addition to* a more expensive random-access scan to source the rows --
strictly worse than the `Seq Scan` alternative here, not a genuine
optimization the planner found.

This is the same class of run-to-run plan instability at this exact backlog
depth that `docs/performance-capability-labels.md`'s "Review note" section
documents (there, a `Seq Scan` vs `Index Scan` flip on `harvest_workers`
between two runs of *that* capture, attributed to `ANALYZE`-statistics
sampling variance rather than to the change under test). Here, though, the
flip correlates with which *label* is captured (2 of 2 times it appeared, it
was on the `schedule-to-close` side, never on `no-schedule-to-close`, across
four runs), which is suggestive of a real interaction between the wider row
and the planner's cost/selectivity estimate at this row count rather than
pure chance -- but no targeted test isolating that mechanism was run, and a
2-vs-4 sample cannot distinguish "the wider row measurably raises the odds
of this plan" from "an unrelated shared factor across this environment's
runs happened to correlate." Per this repo's "reasoning about what the
planner will probably do" prohibition, this page reports the observation
and its correlation honestly without asserting a confirmed causal mechanism.
It is flagged as a **risk to be aware of at large backlog depths for
deployments that populate `schedule_to_close_at`**, not as a proposed fix
target: there is no schema or query change on offer that would pin the
planner's choice without the "planner-disabling flags... outside a
diagnostic session" this repo's rules ban, and extended statistics or a
planner hint would be a schema/config change outside this pass's scope
(this repo's "ask before" list).

### Corroboration: `pg_stat_statements` over the real claim-drain

To check whether the small-depth `EXPLAIN` deltas hold under the actual
claim workload -- repeated `claim_task()` calls draining the backlog one row
at a time, as production does -- the harness drives the real async
`queue::claim_task(...)` function 10,001 times (10,000 successful claims plus
one final empty poll) against the 10,000-row/4-queue headline scenario at
each data state and snapshots `pg_stat_statements` afterward (artifacts,
run 4 / committed:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-pg_stat_statements.txt`):

| run | no-schedule-to-close avg/call | schedule-to-close avg/call | delta % |
|---:|---:|---:|---:|
| 1 | 458.85 | 562.11 | +22.5% |
| 2 | 512.39 | 524.96 | +2.5% |
| 3 | 532.91 | 560.18 | +5.1% |
| 4 (committed) | 510.50 | 539.05 | +5.6% |

**This did not reproduce to a stable number the way the 1,000-/10,000-row
`EXPLAIN` deltas did.** All four runs are positive (`schedule-to-close`
never came out cheaper), and three of the four cluster in a 2.5-5.6% band --
consistent with the row-level `EXPLAIN` evidence -- but run 1 is a clear
outlier at +22.5%, roughly 4-9x the other three. Given the 100k-depth
finding above -- a plan flip that specifically favors `schedule-to-close`
and adds a large buffer cost when it fires, observed in exactly the runs
(2 and 3) that are *not* outliers here, which rules out "the same flip
explains both" as a clean story -- no confirmed mechanism is claimed for
run 1's outlier value. The drain loop does not capture a plan for every one
of its 10,001 calls, only the aggregate `pg_stat_statements` counters, so
there is no per-call plan trace to check any hypothesis against directly.
Read the aggregate delta as "positive and real, typically mid-single-digit
percent, with an observed but unexplained potential to run several times
higher" rather than as a single citable percentage.

## Write-side cost

Every `UPDATE` to a claimed row -- including the claim `UPDATE` itself in
`claim_task_query()`'s `claimed` CTE, which never touches
`schedule_to_close_at` -- still creates a brand-new MVCC tuple version that
carries the column's value forward, exactly the mechanism
`docs/performance-capability-labels.md`'s "Write-side cost" section
documents for `required_capabilities`. Two independent, standalone,
single-transaction corroborations (which is what makes these two reproduce
cleanly where the live 10,001-call drain below does not -- neither leaves a
~15-minute window for autovacuum to run partway through, since neither
commits until the whole simulated drain finishes): artifacts
`docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.{sql,txt}`
and
`docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.{sql,txt}`:

| seeding + update shape | no-schedule-to-close heap-page growth | schedule-to-close heap-page growth | extra growth |
|---|---:|---:|---:|
| one bulk `UPDATE ... WHERE state = 'PENDING'` (10,000 rows, one statement) | 250 | 263 | +5.2% |
| 10,000 individual `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` pairs, PL/pgSQL loop (still one transaction end to end) | 250 | 263 | +5.2% |

The two access shapes land on the **identical** result: within a single
transaction (no commit boundaries in between), whether the 10,000 rows are
touched by one bulk statement or by 10,000 individual per-row statements
does not change the heap-page-growth outcome. Both are close to the
`EXPLAIN` band above (2.5-6%).

The instrumented captures also snapshotted `pg_stat_user_tables` immediately
before and after the real 10,000-claim headline drain -- a ~15-minute window
in this environment, long enough for autovacuum to run unpredictably partway
through. This instrumentation was added partway through run 2 and exists in
every run from there on, including the failed `'infinity'` attempt's
`no-schedule-to-close` half (it panicked on the `schedule-to-close` side
before that side's after-drain snapshot; artifacts, all runs' final state
committed for run 4):

| run | no-schedule-to-close `n_dead_tup` | schedule-to-close `n_dead_tup` | ratio | heap-page growth (no-stc / stc) |
|---:|---:|---:|---:|---|
| 2 | 802 | 4,779 | 5.96x | +44 / +52 (+18.2%) |
| 3 | 4,855 | 5,131 | 1.06x | +49 / +52 (+6.1%) |
| *(failed `infinity` attempt)* | 4,789 | -- (panicked) | n/a | +49 / -- |
| 4 (committed) | 4,753 | 4,961 | 1.04x | +49 / +52 (+6.1%) |

**This does not support a pinned dead-tuple ratio, but the picture is
clearer with four observations than it was with two.** Of the four
`no-schedule-to-close` dead-tuple counts, three cluster tightly around
4,750-4,855 (run 3, the failed attempt, and run 4) and one -- run 2's 802 --
is the outlier, not the other way around (an earlier draft of this page,
written with only runs 2 and 3 in hand, could not tell which side was
anomalous and guessed wrong). `schedule-to-close`'s two available
observations (run 3's 5,131 and run 4's 4,961) are close to each other and
close to the `no-schedule-to-close` cluster, giving dead-tuple *ratios* of
1.06x and 1.04x on the two runs with complete data -- essentially no
meaningfully higher bloat from the wider column, not the 5.96x run 2's
outlying `no-schedule-to-close` value had suggested. The most plausible
explanation for run 2's low figure is that autovacuum happened to run
partway through that one drain and not the other three -- entirely outside
this harness's control, since nothing in the test triggers or waits for it.
**Heap-page growth**, unlike the raw dead-tuple count, was consistently
modest and stable across every run that measured it (+44 to +49 pages for
`no-schedule-to-close`, +52 for `schedule-to-close` every time, landing at
either +6.1% or +18.2% "extra growth" depending on which
`no-schedule-to-close` figure it is compared against) -- more consistent
with the small, stable row-width effect measured elsewhere on this page
than the noisier dead-tuple counts are.

No schema, index, or autovacuum-configuration change is proposed by this
pass. The variance itself is the finding worth recording: a live queue's
dead-tuple count at any given moment is not something this measurement
approach can pin down without controlling for autovacuum, and a future pass
that wants a reliable dead-tuple number for this table should disable
autovacuum for the duration of its own measurement window explicitly (not
done here, since disabling autovacuum is itself something this repo's rules
require flagging findings about rather than doing silently inside a
benchmark) or take many more than three samples.

## Equivalence

All drains claim exactly 10,000 of 10,000 seeded rows
(`claimed == claimed_by_label` asserted equal between the two labels inside
the test), and `claim_row.calls == claimed + 1` is asserted for the final
empty poll in each state (this assertion is inherited from the shared
pattern; see the test source). The schedule-to-close claim path returns the
same claim behavior as the unpopulated path in every run -- the cost (and its
variance) measured here is overhead on an otherwise identical result set, not
a correctness difference.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_schedule_to_close_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each,
  `ANALYZE`s `harvest_workers` before either label's stat-snapshot drain
  (added in response to Codex review on PR #1339 -- see [the note
  above](#workload) on why the drain needs it), snapshots
  `pg_relation_size`/`pg_stat_user_tables` immediately before and after a
  real 10,000-row headline drain through `queue::claim_task()` at both
  states while also snapshotting `pg_stat_statements`, and asserts
  claim-count equivalence between the two states as a correctness check.
- `docs/perf-artifacts/schedule-to-close-claim-predicate/` -- the committed
  (run 4) `EXPLAIN` captures, `pg_stat_statements` snapshots, heap-growth
  snapshots, the two standalone bloat-corroboration scripts (bulk `UPDATE`
  and per-row PL/pgSQL loop, both seeding `NOW() + INTERVAL '100 years'`)
  and their output, and a `fixture-summary.txt`.
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
`docs/perf-artifacts/schedule-to-close-claim-predicate/` from scratch.
**Expect the 100,000-row depth and the aggregate/heap-growth numbers to vary
between runs** -- see [The 100k-depth plan
instability](#the-100k-depth-plan-instability) and [Write-side
cost](#write-side-cost) above; this is expected, not a reproduction failure.
The 1,000- and 10,000-row `EXPLAIN` buffer counts should reproduce exactly.

**They do NOT regenerate `claim_update_bloat_corroboration.txt` or
`claim_update_bloat_loop_corroboration.txt`** -- both scripts are
independent of the Rust harness and neither is invoked by the repro command
above. After any schema, index, or storage-layout change to
`harvest_task_queue`, re-run both explicitly, or the committed corroboration
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

# 2. Run both corroboration scripts against the scratch database only.
psql "$DATABASE_URL" \
  -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.sql \
  > docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.txt

psql "$DATABASE_URL" \
  -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.sql \
  > docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.txt

# 3. Tear the scratch database down when done.
dropdb -h localhost -U postgres harvest_perf_scratch
```
