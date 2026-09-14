# `build_routing::all_build_reachability` — one pass per table, not one per build

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration. Reproduce it on your own hardware
> before designing against it — the harness is in the repo precisely so you
> can.

## 🎯 Workload

`all_build_reachability` backs the Vantage UI Builds page and its
`GET /admin/builds` counterpart (issue #171's rolling-deploy build routing).
An operator opens either one to answer "which old builds have no more work
and can be retired": for every build id the fleet has ever seen, it reports
open executions, pending tasks, and active/stale worker counts.

Before this page's fix, `all_build_reachability` first collected the
distinct build ids present across `harvest_workflow_executions`,
`harvest_task_queue`, and `harvest_workers`, then looped over that list and
called `build_reachability` once per id. That helper issues one statement
combining four correlated subqueries, each filtered on `WHERE ... = $1` for
that one build. A fleet with a real deploy history easily accumulates dozens
of distinct build ids — every build that still has so much as one open
execution or queued task stays in the catalog — so this page's cost scales
with **build count**, not with fleet size, and every operator visit to the
Builds page repeats the full scan set once per build.

`harvest_workers` carries no index on `build_id` at all (only
`harvest_workflow_executions.assigned_build_id` and
`harvest_task_queue.required_build_id` are indexed, both partial and added
by the same migration). So each loop iteration's two worker subqueries
(`active_workers`, `stale_workers`) were a full sequential scan of the
entire worker fleet, repeated once per build.

The harness is
`autumn-harvest/tests/integration/build_reachability_fanout_perf.rs`,
seeding a deterministic, production-shaped fixture — 50 distinct build ids,
60,000 workflow executions, 30,000 queued tasks, and 3,000 workers — into a
real Postgres 16 with `pg_stat_statements` preloaded. Reproduce with:

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh
```

## 📈 Profile

`pg_stat_statements`, scoped to statements naming `build_id` (both
strategies' own bind literal in this filter, so nothing else on the fixture
database is swept in), reset immediately before each strategy runs:

| strategy | distinct statement shapes | total `calls` | total buffers (`shared_blks_hit + read`) |
|---|--:|--:|--:|
| before (loop `build_reachability`, N=50) | 2 (catalog scan + the per-build combined query) | 51 | 30,249 |
| after (`all_build_reachability`, N=50) | 4 (catalog scan + 3 grouped queries) | 4 | 2,799 |

Calls: **51 → 4** (-92.2%). Buffers: **30,249 → 2,799** (-90.7%).

The per-build combined query alone accounts for 50 of the 51 "before" calls
and 28,300 of the 30,249 "before" buffers — one call for every distinct
build id the catalog scan returned, each combining a scan of all three
source tables (an average of 566 buffers per call). The one-time catalog
scan (1,949 buffers) is identical in both captures, so it is not part of
the delta above — it runs exactly once either way. The "after" side
replaces the 50 per-build calls with exactly **three**: one
`GROUP BY assigned_build_id` pass over `harvest_workflow_executions` (395
buffers total), one `GROUP BY required_build_id` pass over
`harvest_task_queue` (401 buffers total), and one `GROUP BY build_id` pass
over `harvest_workers` (54 buffers total) with conditional aggregation
standing in for the two separate `active`/`stale` subqueries. Each source
table is now scanned exactly once for *all* 50 builds combined, instead of
once per build — 850 buffers total for every build in the fixture, versus
28,300 for the same 50 builds under the old loop.

Full snapshots: [`before.pg_stat_statements.txt`](perf-artifacts/build-reachability-fanout/before.pg_stat_statements.txt),
[`after.pg_stat_statements.txt`](perf-artifacts/build-reachability-fanout/after.pg_stat_statements.txt).

## 🧭 Plan

No index changes and no migration. The fix is confined to
`build_routing::all_build_reachability`'s own SQL: the per-build correlated
subqueries become three `GROUP BY` aggregations, each already well-served by
a single sequential (or, for the two indexed columns, index) scan of its
table — Postgres does not need a new index to compute a `GROUP BY` over
every row it already has to visit once. `build_reachability` (the per-build
helper other callers use directly) is untouched.

## 💡 Hypothesis

Round trips and full-table scans of `harvest_workers` both scale with the
number of *distinct builds* under the old loop, not with the size of the
fleet or its work backlog. Replacing the loop with one grouped pass per
source table converts that from `O(builds)` round trips and scans to `O(1)`
— exactly 3, independent of how many builds the catalog names.

## 🔧 Change

`autumn-harvest/src/build_routing.rs`: `all_build_reachability` no longer
calls `build_reachability` in a loop. It runs three new query-text constants
— `all_build_open_executions_query`, `all_build_pending_tasks_query`,
`all_build_worker_counts_query` — once each, folds each result set into a
`HashMap<String, _>` keyed on build id, and merges the three maps against
the same catalog list the old code already computed
(`all_build_ids_query`, extracted unchanged). A build id present in the
catalog but absent from a given map defaults to zero for that map's
counters, matching the old per-build query's `COUNT(*) = 0` behavior on a
build with no matching rows in that table.

## 📏 Measurement

`before.result-rows.txt` and `after.result-rows.txt` list every one of the
50 builds' four counters and `safe_to_retire` flag, sorted by build id. The
evidence-capture test asserts they are identical before publishing either
file — see Equivalence below. Both were produced against the same
deterministic fixture in the same test run, so the row counts, states, and
worker heartbeat ages behind each build id are exactly the same input on
both sides; only the querying strategy differs.

## ✅ Equivalence

Two independent checks, both in
`build_reachability_fanout_perf.rs`:

1. **`all_build_reachability_agrees_with_the_per_build_helper`** — fast,
   always-run. Seeds a small fixture covering a build present in only one of
   the three source tables, then asserts the batched function's counters
   for every build match `build_reachability`'s counters for that same
   build, field by field.
2. **`zz_capture_build_reachability_fanout_evidence`** — the evidence
   capture test itself asserts, before writing any artifact, that looping
   `build_reachability` (the "before" reproduction) and calling the real,
   shipped `all_build_reachability` (the "after" measurement) return
   byte-identical sorted result sets across all 50 builds in the production
   fixture.

## ✍️ Write cost

None. `all_build_reachability` is a read-only report; nothing it touches
performs a write.

## 🔁 Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh
```
