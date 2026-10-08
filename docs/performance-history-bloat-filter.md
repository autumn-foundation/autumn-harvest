# The `min_history_events` / `history_bloat_min_events` threshold filter

Three query builders in `autumn-harvest-plugin/src/api.rs` — `load_workflows`,
`load_stalled_workflows`, and `load_history_bloat_workflows` — answer the same
question against `harvest_events`: *"does this execution's recorded history
have at least N events?"* All three had independently hand-copied the same
answer: `(SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = id) >=
N`. That is the most expensive way to ask a `>=` question Postgres has —
unbounded, and worst on exactly the rows the filter exists to find.

> **This is a reference measurement, not an SLO.** It was taken on one machine
> with one Postgres configuration (below). Reproduce it on your own hardware
> before designing against it — the harness is in the repo precisely so you
> can.

## TL;DR

* **`COUNT(*) >= N` cannot early-exit.** Postgres must count every matching
  row before it can answer a threshold question, so a workflow with 448,175
  recorded events pays to have all 448,175 counted merely to confirm `>=
  10,000`. Measured: **3,976 total buffers**
  (`pg_stat_statements.total_buffers`, one call) — the access path Postgres
  picks for the unbounded scan is sensitive to table statistics (see the
  callout below), but the *lack of an early exit* is not; it holds
  regardless of which plan shape answers the query.
* **`EXISTS (... ORDER BY event_id OFFSET N-1 LIMIT 1)` is boolean-equivalent**
  to `COUNT(*) >= N` for every `N` (verified exhaustively — see
  [Correctness](#correctness--result-equivalence)) but only ever reads
  `min(actual_count, N)` rows. Measured on the identical execution: **94
  total buffers** — a **97.64% reduction** — via an Index Only Scan with
  `Heap Fetches: 0`, capped at exactly `N` rows regardless of how large the
  workflow's true history is.
* **A freshly-`VACUUM`'d table is not optional evidence hygiene here — it can
  change which plan shape Postgres picks, not just how expensive that plan
  is.** An un-set visibility map doesn't only inflate `Heap Fetches` within a
  fixed plan (the effect this page originally described); it can also make
  the planner's *cost estimate* for an Index Only Scan look bad enough that
  it falls back to a full scan instead. See [Why `ORDER BY` is
  load-bearing](#why-order-by-is-load-bearing) for the measured before/after.
* **No regression for the common case.** An ordinary, un-bloated execution
  (5 recorded events) reads the same 5 buffers either way — the filter this
  page is about only matters once a history is actually large.
* Three independent call sites duplicated the unbounded predicate; the fix is
  one shared helper (`apply_min_history_events_filter`), so all three get the
  bounded form and can never drift back out of sync with each other.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers` (128MB) |
| Harness | `autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh` |
| Artifacts | `docs/perf-artifacts/history-bloat-filter/` (committed, this page's source) |

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL (mirrors
`claim_bench_support.rs`'s convention): a fresh, uniquely-named database is
created, migrated with every `up.sql` in timestamp order, seeded, measured,
and dropped. The role it names must be able to `CREATE DATABASE`/`DROP
DATABASE`. Takes roughly 2-3 minutes — seeding ~5.3M event rows dominates the
runtime.

The fixture is production-shaped, not a toy: 5,000 ordinary `order_flow`
executions with 5-79 events each (a healthy fleet), plus 15 long-running
`poll_loop` executions with 50,000-449,999 events each — the exact "one
workflow accumulated an unbounded history" shape issue #704's
`history_bloat_min_events` filter exists to surface — **5,345,908 events
across 5,015 executions** in total (`fixture-summary.txt`). Both cohorts'
event counts are derived deterministically from `md5(workflow_id)`
(normalized to a non-negative `bigint` before the range modulus — see the
harness script's comment on that expression for why the naive signed-`int`
form silently zeroed out ~46% of rows in an earlier revision of this harness,
caught in code review before this page's numbers were finalized), so
re-running the harness reproduces the same per-execution counts (448,175
events for the largest bloated execution, 5 for `healthy-1`, every time —
verified across three independent harness runs, byte-identical down to the
`fixture_total_events`/`fixture_total_executions`/per-execution counts).

**The harness `VACUUM ANALYZE`s both tables immediately before every capture**
(added in PR #1173 review, [discussion_r3787646193][pr-1173-r3787646193],
after an earlier run of this same script — before that fix — produced
artifacts whose visibility map hadn't been fully set, which changed more
than just the buffer count: it changed which *plan shape* Postgres chose for
two of the five queries below. Comparing artifacts from a `VACUUM`'d run
against an un-vacuumed one is comparing different experiments, not noise —
see [Why `ORDER BY` is load-bearing](#why-order-by-is-load-bearing) for the
concrete before/after. With the fix in place, the bounded form's buffer
count carries only the small amount of run-to-run variance expected from
`gen_random_uuid()`-driven physical page layout differing slightly between
separately-seeded databases: 93 and 94 total buffers across two independent
`VACUUM`'d runs against an identical 448,175-event history — noise on the
order of a single buffer, not a plan-shape flip. The numbers on this page are
from the more recent of those two runs.

## Profile — this predicate dominates the query it lives in

`GET /workflows?min_history_events=N` and `GET
/workflows?history_bloat_min_events=N` exist for exactly one reason: to
evaluate this predicate. There is no ambient "total workload" to compare
against for a freshly-added, single-purpose filter — the predicate's cost
*is* the query's cost once the filter is in use. What the fixture's
`pg_stat_statements` snapshot shows directly is that dominance: of the two
statements captured, the `COUNT(*)` form alone accounts for 3,976 of the
4,070 total buffers touched across both — **97.69% of everything the
fixture recorded for this filter**. That is the concrete, measured sense in
which this predicate is not a marginal contributor: for the endpoints that
use it, it *is* the query.

## The problem

```sql
(SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = id) >= 10000
```

Postgres has no way to know a `COUNT(*)` has already reached a threshold
without finishing the count — there is no early exit, and that is true no
matter which access path answers the query. Against the 448,175-event
execution in the fixture, this reads
[**3,976 total buffers**](../perf-artifacts/history-bloat-filter/before-count-star-bloated.explain.txt)
via a Parallel Index Only Scan (2 workers) against `idx_harvest_events_exec_last`
— an index that exists for an unrelated endpoint (issue #486's
`no_progress_minutes` stall filter), not this one, but which the planner is
free to use for any equality predicate on its leading column. Every one of
those 149,392+ index entries the two workers walk still has to be visited,
one at a time, to be counted; only the fact that none of them needed a heap
fetch (`Heap Fetches: 0`, confirmed by `VACUUM`-set visibility-map bits) kept
the buffer count this low. **The access path here is not the durable part of
this measurement** — see [Why `ORDER BY` is
load-bearing](#why-order-by-is-load-bearing) for a fixture state in which
this same query fell back to a full Seq Scan over the whole 5.3M-row table
instead. What *is* durable, and is the actual point of this section, is that
no access path lets `COUNT(*)` stop early: the query still has to visit
every one of the 448,175 matching rows to answer a yes/no question about
whether there are at least 10,000 of them. For the 5-event healthy execution
the same form reads
[5 buffers](../perf-artifacts/history-bloat-filter/before-count-star-healthy.explain.txt)
via an Index Only Scan — cheap, because there is almost nothing to count.
The predicate's cost is entirely a function of how *large* the history is,
which is backwards for a filter whose whole purpose is to find large
histories.

## The fix

```sql
EXISTS (
    SELECT 1 FROM harvest_events
    WHERE workflow_exec_id = id
    ORDER BY event_id
    OFFSET 9999  -- min_events - 1
    LIMIT 1
)
```

`OFFSET N-1 LIMIT 1` finds the N-th row and stops — Postgres now has an early
exit, because "does a row exist at this offset" is answerable the moment that
one row is produced, not after every row is counted. Against the identical
448,175-event execution this reads
[**94 total buffers**](../perf-artifacts/history-bloat-filter/after-bounded-exists-bloated.explain.txt)
(both `pg_stat_statements.total_buffers` and the individual `EXPLAIN
(ANALYZE, BUFFERS)` capture agree exactly) — a **97.64% reduction** — via an
Index Only Scan against `idx_harvest_events_exec (workflow_exec_id,
event_id)`, reading exactly 10,000 rows (`Heap Fetches: 0`, `actual
rows=10000`) regardless of the other 438,175 events that exist beyond the
threshold. For the healthy execution both forms read
[the same 5 buffers](../perf-artifacts/history-bloat-filter/after-bounded-exists-healthy.explain.txt)
— no regression for the common, un-bloated case.

### Why `ORDER BY` is load-bearing

**This claim needs a more careful, two-part answer than a single buffer count
can give — an earlier version of this page measured a 228x regression here
and that number, on its own, turned out to be a property of the *statistics
state* the measurement happened to catch, not an inherent property of the
query.** Both halves are real and are documented below; neither one alone is
the whole story.

Dropping the `ORDER BY` from the fix's query:

```sql
EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = id OFFSET 9999 LIMIT 1)
```

**Against this page's current, freshly-`VACUUM`'d reference fixture, dropping
`ORDER BY` costs almost nothing.** The no-`ORDER BY` form reads
[**93 total buffers**](../perf-artifacts/history-bloat-filter/after-bounded-exists-no-order-by-bloated.explain.txt)
against the identical 448,175-event execution — statistically indistinguishable
from (and, in this particular capture, one buffer *cheaper* than) the
`ORDER BY`'d form's 94, via an Index Only Scan with `Heap Fetches: 0`. This
was verified two ways: the fixture capture above, and a direct counterfactual
— `BEGIN; DROP INDEX idx_harvest_events_exec_last, idx_harvest_events_history_page;
<run the no-`ORDER BY` query>; ROLLBACK;` against a separate persistent
fixture database, leaving *only* `idx_harvest_events_exec (workflow_exec_id,
event_id)` — the one index this predicate was actually written against — in
place. Even then, Postgres still chose an Index Only Scan against that index
without needing an `ORDER BY` at all (94 total buffers, `Heap Fetches: 0`).
So the collapse of the 228x gap is not merely borrowed from an incidental
index that happens to exist for an unrelated endpoint (see [Known
limitations](#known-limitations--out-of-scope)); it holds against the
intended index alone, once the table's statistics — specifically, the
visibility map — are fresh.

**But the original 228x measurement was not fabricated or a measurement bug —
it is a real, reproducible outcome of the same visibility-map mechanism
described above, just at the opposite extreme.** An earlier revision of this
harness ran its captures against a table whose `VACUUM` had not yet caught
up. Under that condition, the no-`ORDER BY` query fell back to a plain Seq
Scan reading 2,956,744 non-matching rows before finding its 10,000th match —
88,618 total buffers, 228x worse than the `ORDER BY`'d form. This is the
second, more consequential way an un-set visibility map affects the planner,
beyond inflating `Heap Fetches` within a plan already chosen (the effect
documented in [The fix](#the-fix)): it can change the estimated *cost* of an
Index Only Scan enough that the planner rejects that access path entirely
and falls back to a full scan instead — a genuinely different plan shape, not
just a more expensive version of the same one. Real production tables spend
real time in exactly this state between writes and the next autovacuum pass.

**`ORDER BY event_id` therefore stays in the code as free insurance, not as a
buffer-count optimization for this fixture's steady state.** It gives the
planner an explicit sort target that makes the intended composite index — the
same one `store::load_history` already relies on for the primary
history-load path — the correct choice *regardless of whether the visibility
map happens to be fresh*, closing off the 228x failure mode above without
costing anything when the map already is fresh (93 vs. 94 buffers is not a
tradeoff worth reasoning about). Removing it would trade a documented,
reproducible worst case for an unmeasurable bet on autovacuum timing.

## Correctness — result equivalence

`EXISTS (... ORDER BY event_id OFFSET N-1 LIMIT 1)` is boolean-equivalent to
`COUNT(*) >= N` for every non-negative `N`: the N-th row (0-indexed offset
`N-1`) exists if and only if at least `N` rows exist. This was verified two
ways.

**Interactively**, against four sample executions (two healthy, two
bloated) at six threshold values each — `0`, `1`, `actual_count - 1`,
`actual_count` (the exact boundary), `actual_count + 1` (the adjacent
off-by-one), and a value far past `actual_count` — all 24 cases produced
identical `true`/`false` results between the old and new forms. This
equivalence is a boolean-logic identity independent of what any particular
execution's event count happens to be, so it holds regardless of fixture
shape. `N == 0` is handled as an explicit early return in
`apply_min_history_events_filter` (leaving the query unfiltered, matching
`COUNT(*) >= 0`'s always-true behavior) rather than computing `OFFSET -1`,
which Postgres rejects.

**In the committed test suite**
(`autumn-harvest-plugin/tests/history_bloat_integration.rs`), two dedicated
regression tests —
`min_history_events_exact_boundary_and_off_by_one_are_precise` and
`history_bloat_min_events_exact_boundary_and_off_by_one_are_precise` — pin
the exact-count-inclusion and adjacent-off-by-one-exclusion boundaries for
both call sites, at both the smallest non-zero threshold (`min_events=1`,
exercising `OFFSET 0`) and an arbitrary interior one (`min_events=7`,
exercising `OFFSET 6`). These tests seed their own small, self-contained
fixtures inline (not the harness's `md5`-derived generator), so they are
unaffected by the harness bug described above. They sit alongside the
pre-existing 17-test suite covering AC1/AC2/AC7 for both filters
(live/terminal exclusion, sorting, `history_event_count` reporting,
invalid-value rejection, cross-shard merging, and composition with
`state`/pagination/`failure_cause`/`min_history_events`/`no_progress_minutes`)
— all 19 tests pass against the bounded form with zero behavioral change
beyond the buffer-cost fix itself.

```bash
cargo test -p autumn-harvest-plugin --test history_bloat_integration
```

## Verification against the real production entry point

The measurements above (TL;DR, Profile, The fix) all isolate the WHERE-clause
predicate as a **standalone scalar query** — `SELECT (SELECT COUNT(*) ...
WHERE workflow_exec_id = '<one hardcoded execution id>') >= N`. Postgres
plans that as an `InitPlan`: the subquery has no outer-row reference at all
(no `FROM harvest_workflow_executions` in the query), so it is evaluated
*once* and the boolean result reused. The real production code embeds the
identical predicate as `WHERE ... EXISTS (SELECT 1 FROM harvest_events WHERE
workflow_exec_id = harvest_workflow_executions.id ORDER BY event_id OFFSET
N-1 LIMIT 1)` — correlated to the outer row, and non-hoistable into a
semi-join because of the `ORDER BY ... OFFSET ... LIMIT` inside. Postgres
must plan that as a genuine `SubPlan`, re-executed once per outer candidate
row that survives the rest of the `WHERE` clause — a structurally different
execution shape the single-row synthetic benchmark never exercises (PR #1173
review, [discussion_r3786205954][pr-1173-r3786205954]).

To answer that directly, `autumn-harvest-plugin/tests/real_query_probe.rs`
drives `GET /workflows?min_history_events=10000` and `GET
/workflows?history_bloat_min_events=10000` through the actual
`harvest_api_router` (`tower::ServiceExt::oneshot`, no network port bound,
but a real `axum::Router` handling a real `Request` through the real
`load_workflows`/`load_history_bloat_workflows` handlers) against a
persistent copy of the identical 5.3M-row / 5,015-execution fixture used
above, comparing the pre-fix commit (`dd47715`, unbounded `COUNT(*)`,
checked out into a separate `git worktree`) against the current bounded-`EXISTS`
code.

**A methodological correction found along the way, reported for the same
reason the harness bug above is:** the first attempt at this comparison ran
the two configurations back-to-back against the same physical database with
no explicit `VACUUM` between them, and produced a result that flatly
contradicted the single-row hypothesis (the bounded form reading *more*
total buffers than the unbounded form). `pg_stat_statements.total_buffers`
(`shared_blks_hit + shared_blks_read`) is a count of buffer *accesses*, not
wall-clock time, and is not affected by OS-page-cache or `shared_buffers`
warmth — but it *is* affected by the Postgres **visibility map**: an Index
Only Scan on a page whose visibility-map bit isn't set must still fetch the
heap page to check tuple visibility (`Heap Fetches` > 0 in `EXPLAIN`), and
that bit is set by `VACUUM` (or autovacuum, on its own schedule) rather than
being an inherent property of the query plan. The two configurations were
measured with autovacuum having had different amounts of time to run since
the last state change, which changed each side's heap-fetch count by enough
to reverse the apparent direction of the result — a real, deterministic,
buffer-counted effect, just not the effect being measured. Running `VACUUM
ANALYZE harvest_workflow_executions; VACUUM ANALYZE harvest_events;`
immediately before *each* measurement (so both sides start from an
identical, fully-vacuumed visibility-map state) and re-running in both
measurement orders reproduced identical, order-independent numbers:

| Query param | Endpoint handler | Before (unbounded `COUNT(*)`) | After (bounded `EXISTS`) | Reduction |
|---|---|---:|---:|---:|
| `min_history_events=10000` | `load_workflows` | 79,350 total buffers | 27,642 total buffers | **65.16%** |
| `history_bloat_min_events=10000` | `load_history_bloat_workflows` | 128,145 total buffers | 76,438 total buffers | **40.35%** |

Both clear the ≥20% impact floor. Both were reproduced twice in reversed
measurement order with identical results (`docs/perf-artifacts/history-bloat-filter/real-endpoint-before-unfixed.txt`,
`real-endpoint-after-fixed.txt`).

These percentages are meaningfully smaller than the 97.64% headline number
above — expected, not contradictory: the single-row number is the *best
case* for the one specific 448,175-event execution the WHERE-clause fix
helps the most, while the real-endpoint number is the *aggregate* cost
across the full realistic candidate population the endpoint actually scans
(5,000 small/healthy executions plus 15 bloated ones). Isolating the
WHERE-clause predicate alone as a genuine correlated `SubPlan` — the same
`EXISTS`/`COUNT(*)` forms above, embedded against the full outer table with
no other endpoint-specific filters or columns — makes this explicit:

```sql
-- both wrapped in `SELECT count(*) FROM (... ) sub` to force full
-- materialization rather than stopping at the first match
SELECT id FROM harvest_workflow_executions
WHERE (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= 10000;  -- before: 73,918 total buffers
SELECT id FROM harvest_workflow_executions
WHERE EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id ORDER BY event_id OFFSET 9999 LIMIT 1);  -- after: 22,443 total buffers (69.64% reduction)
```

(`docs/perf-artifacts/history-bloat-filter/correlated-before-count-star.explain.txt`,
`correlated-after-bounded-exists.explain.txt` — both are now **generated by
the harness script itself**, `history_bloat_perf_repro.sh`'s `capture
"correlated-before-count-star"`/`capture "correlated-after-bounded-exists"`
calls, not hand-typed `psql` commands, so rerunning the single reproduction
command below regenerates every artifact this doc cites from one code path;
see [discussion_r3787605008][pr-1173-r3787605008].) Notably, the **absolute**
buffer savings (51,475 buffers here) sits within 0.5% of both
`real_query_probe.rs` real-endpoint measurements below (51,707 and 51,708
buffers) — the same underlying WHERE-clause fix contributing the same fixed
savings regardless of what other filters/columns each endpoint layers on
top; the *percentage* differs only because `history_bloat_min_events` has a
higher base cost (it carries its own mandatory non-terminal-state filter and
an additional `history_event_count` projection) than `min_history_events`
does.

**This correlated pair's buffer count is noisier, run to run, than the
single-row isolated queries above are — and honestly reporting that noise
matters more here than picking one favorite number.** Across five
independent measurements taken during this investigation (a mix of fresh
`history_bloat_perf_repro.sh` invocations and repeated `EXPLAIN` captures
against one already-seeded, already-`VACUUM`'d database), the **before**
side ranged **66,278–74,461 total buffers** (a ~12% spread) while the
**after** side ranged **22,165–22,754** (a ~2.7% spread) — the bounded
`EXISTS` form is consistently far more stable than the unbounded `COUNT(*)`
form. This is expected, not a red flag: the correlated `COUNT(*)` plan is a
`Seq Scan` over all 5,015 outer rows, re-invoking its `SubPlan` once per row
with no early exit, so its buffer count is sensitive to exactly which pages
happen to be cache-resident at capture time; the bounded `EXISTS` form's
early exit makes it far less exposed to that variance. Every one of the five
measurements cleared **66%+ reduction**, well past the ≥20% impact floor,
and `gen_random_uuid()`-driven physical page-layout differences between
separately-seeded databases account for the same ±1-2% noise band already
noted for the single-row measurement above.

```bash
# Reproduce the isolated correlated-plan artifacts (ephemeral DB, ~2-3 min):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh

# Reproduce the real-endpoint numbers above (persistent fixture DB, see the
# harness script for the exact seeding SQL, then VACUUM ANALYZE it):
REAL_QUERY_PERF_DB_URL=postgres://postgres:postgres@127.0.0.1:5432/<fixture-db> \
  cargo test -p autumn-harvest-plugin --test real_query_probe -- --ignored --nocapture
```

[pr-1173-r3786205954]: https://github.com/autumn-foundation/autumn-harvest/pull/1173#discussion_r3786205954
[pr-1173-r3787605008]: https://github.com/autumn-foundation/autumn-harvest/pull/1173#discussion_r3787605008
[pr-1173-r3787646193]: https://github.com/autumn-foundation/autumn-harvest/pull/1173#discussion_r3787646193

## Write cost

None. This is a read-only query rewrite — no schema change, no migration, no
index added or dropped. `apply_min_history_events_filter` is called from
existing `SELECT` paths only.

## Reproduce

```bash
# 1. The correctness suite (Docker required — testcontainers-backed).
cargo test -p autumn-harvest-plugin --test history_bloat_integration

# 2. The buffer-cost measurement (regenerates the committed artifacts under
#    docs/perf-artifacts/history-bloat-filter/). ~2-3 minutes.
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh
```

## Known limitations / out of scope

* **`load_history_bloat_workflows`'s `SELECT`/`ORDER BY` pair still computes
  an exact `COUNT(*)`** for every candidate that survives the bounded `WHERE`
  filter — this is inherent, not an oversight: the endpoint's AC2 requires
  reporting each row's real current `history_event_count` for ranking, and
  there is no threshold to bound an exact-count request against. Only the
  `WHERE`-clause threshold check (evaluated on every row, most of which are
  healthy and never reach `SELECT`) was the unbounded cost worth fixing; the
  exact count is paid only by the small number of rows that already cleared
  the bounded filter.
* **Three overlapping indexes exist on `harvest_events (workflow_exec_id,
  ...)`** — `idx_harvest_events_exec (workflow_exec_id, event_id)`,
  `idx_harvest_events_exec_last (workflow_exec_id, timestamp DESC)`, and
  `idx_harvest_events_history_page (workflow_exec_id, id)` — added across
  three separate migrations for three separate access patterns. The first and
  third look structurally close enough to warrant a human look at whether one
  is now redundant; this page does not do that (dropping any index requires
  explicit operator sign-off, and doing so safely needs live production
  `pg_stat_user_indexes` usage data this synthetic fixture cannot provide,
  not a code-level judgment call). Flagged here as a candidate for a
  dedicated follow-up, not touched by this fix. **This overlap is not
  hypothetical**: `idx_harvest_events_exec_last`, added for an unrelated
  endpoint (issue #486), is the index the planner actually picks for the
  unbounded `COUNT(*)` measured in [The problem](#the-problem), even though
  this page's fix targets a different index (`idx_harvest_events_exec`)
  entirely. A future migration that
  drops or narrows `idx_harvest_events_exec_last` believing it serves only
  the `no_progress_minutes` filter would be a correctness-neutral but
  performance-relevant change to the unbounded `COUNT(*)` form's access path
  documented on this page — worth a mention in that migration's own PR, not
  a reason to avoid the cleanup.

## See also

* [`docs/performance.md`](performance.md) — the `claim_task` latency
  investigation (issue #786), the sibling "measured, not fixed" writeup this
  page follows in structure.
* Issue #704 — the `history_bloat_min_events` early-warning discovery filter
  this page's second call site belongs to.
* Issue #493 — the general-purpose `min_history_events` filter this page's
  first call site belongs to.
