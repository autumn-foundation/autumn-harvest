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
  10,000`. Measured: **158,593 total buffers**, a Parallel Seq Scan over the
  whole 5.3M-row table (`pg_stat_statements.total_buffers`, one call).
* **`EXISTS (... ORDER BY event_id OFFSET N-1 LIMIT 1)` is boolean-equivalent**
  to `COUNT(*) >= N` for every `N` (verified exhaustively — see
  [Correctness](#correctness--result-equivalence)) but only ever reads
  `min(actual_count, N)` rows. Measured on the identical execution: **388
  total buffers** — a **99.76% reduction** — via an Index Only Scan capped at
  exactly `N` rows regardless of how large the workflow's true history is.
* **The `ORDER BY` is load-bearing, not decorative.** Drop it and the same
  bounded `OFFSET`/`LIMIT` shape still beats the unbounded form (88,618
  buffers vs. 158,593), but the planner falls back to a Seq Scan instead of
  using the existing `(workflow_exec_id, event_id)` index — **228x** worse
  than the indexed form.
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
verified across two independent harness runs, byte-identical down to the
`fixture_total_events`/`fixture_total_executions`/per-execution counts). The
buffer-cost measurements below carry a small amount of run-to-run noise from
physical page layout (the second verification run measured 387 total buffers
for the bounded form against an identical 448,175-event history, vs. 388 in
the committed artifacts) — negligible against a 99%+ reduction, and called
out here rather than silently rounded away.

## Profile — this predicate dominates the query it lives in

`GET /workflows?min_history_events=N` and `GET
/workflows?history_bloat_min_events=N` exist for exactly one reason: to
evaluate this predicate. There is no ambient "total workload" to compare
against for a freshly-added, single-purpose filter — the predicate's cost
*is* the query's cost once the filter is in use. What the fixture's
`pg_stat_statements` snapshot shows directly is that dominance: of the two
statements captured, the `COUNT(*)` form alone accounts for 158,593 of the
158,981 total buffers touched across both — **99.76% of everything the
fixture recorded for this filter**. That is the concrete, measured sense in
which this predicate is not a marginal contributor: for the endpoints that
use it, it *is* the query.

## The problem

```sql
(SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = id) >= 10000
```

Postgres has no way to know a `COUNT(*)` has already reached a threshold
without finishing the count — there is no early exit. Against the
448,175-event execution in the fixture, this reads
[**158,593 total buffers**](../perf-artifacts/history-bloat-filter/before-count-star-bloated.explain.txt)
via a Parallel Seq Scan (2 workers) over the entire `harvest_events` table
(5,345,908 rows across both cohorts at seed time): Postgres estimated a
high-selectivity equality predicate (`workflow_exec_id = '...'`) cheaper to
satisfy by streaming past the ~4.9M non-matching rows than by walking the
existing `(workflow_exec_id, event_id)` index. For the 5-event healthy
execution the same form reads
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
[**388 total buffers**](../perf-artifacts/history-bloat-filter/after-bounded-exists-bloated.explain.txt)
(both `pg_stat_statements.total_buffers` and the individual `EXPLAIN
(ANALYZE, BUFFERS)` capture agree exactly) — a **99.76% reduction** — via an
Index Only Scan against `idx_harvest_events_exec (workflow_exec_id,
event_id)`, reading exactly 10,000 rows (`Heap Fetches: 10000`, `actual
rows=10000`) regardless of the other 438,175 events that exist beyond the
threshold. For the healthy execution both forms read
[the same 5 buffers](../perf-artifacts/history-bloat-filter/after-bounded-exists-healthy.explain.txt)
— no regression for the common, un-bloated case.

### Why `ORDER BY` is load-bearing

The bound alone (`OFFSET`/`LIMIT`) is not sufficient — without an explicit
sort target, Postgres has no reason to prefer walking the index in
`event_id` order. Dropping the `ORDER BY` from the identical query:

```sql
EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = id OFFSET 9999 LIMIT 1)
```

reads
[**88,618 total buffers**](../perf-artifacts/history-bloat-filter/after-bounded-exists-no-order-by-bloated.explain.txt)
against the same execution — still better than the unbounded `COUNT(*)`
(the `OFFSET`/`LIMIT` early-exit still applies once 10,000 matching rows are
found), but via a plain Seq Scan reading 2,956,744 non-matching rows first,
**228x** worse than the `ORDER BY`'d form. The `ORDER BY event_id` clause
gives the planner a sort target that makes the existing composite index — the
same one `store::load_history` already relies on for the primary
history-load path — the obviously cheaper choice, and it turns the query into
an Index Only Scan capped at exactly the threshold.

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

These percentages are meaningfully smaller than the 99.76% headline number
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
WHERE (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= 10000;  -- before: 74,457 total buffers
SELECT id FROM harvest_workflow_executions
WHERE EXISTS (SELECT 1 FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id ORDER BY event_id OFFSET 9999 LIMIT 1);  -- after: 22,165 total buffers (70.23% reduction)
```

(`docs/perf-artifacts/history-bloat-filter/correlated-before-count-star.explain.txt`,
`correlated-after-bounded-exists.explain.txt` — both are now **generated by
the harness script itself**, `history_bloat_perf_repro.sh`'s `capture
"correlated-before-count-star"`/`capture "correlated-after-bounded-exists"`
calls, not hand-typed `psql` commands, so rerunning the single reproduction
command below regenerates every artifact this doc cites from one code path;
see [discussion_r3787605008][pr-1173-r3787605008].) Notably, the **absolute**
buffer savings is within ~1.1% across all three real-scan measurements
(51,707–52,292 buffers) — the same underlying WHERE-clause fix contributing
the same fixed savings regardless of what other filters/columns each
endpoint layers on top; the *percentage* differs only because
`history_bloat_min_events` has a higher base cost (it carries its own
mandatory non-terminal-state filter and an additional `history_event_count`
projection) than `min_history_events` does. The small remaining
variance between this isolated-query measurement and the two
`real_query_probe.rs` real-endpoint measurements above is expected, not a
methodology gap: each was captured against a separately-seeded database (the
event *counts* are deterministic — `md5(workflow_id)`-derived, confirmed
identical across every seeding run — but `gen_random_uuid()` primary keys
mean the physical heap/index page packing differs slightly run to run, the
same ±1-2% buffer-count noise band already noted for the single-row
measurement above).

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
  dedicated follow-up, not touched by this fix.

## See also

* [`docs/performance.md`](performance.md) — the `claim_task` latency
  investigation (issue #786), the sibling "measured, not fixed" writeup this
  page follows in structure.
* Issue #704 — the `history_bloat_min_events` early-warning discovery filter
  this page's second call site belongs to.
* Issue #493 — the general-purpose `min_history_events` filter this page's
  first call site belongs to.
