# The retention pass planned five statements per candidate

The retention candidate loop visits each expired execution once. Five of
its statements ran unnamed, so Postgres parsed and planned each of them
on every call. This page measures that cost and removes it with the
statement cache mark of [issue #2651](performance-hot-path-statement-cache.md).

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the bench and its load on it. The buffer and plan
> counts reproduce. The wall-clock numbers are one sample each.

## 🎯 Workload

The workload is one pass of `retention_reclaim_bench`, the unpartitioned
arm, at the small scale. It drives `RetentionRuntime::run_now` through the
public API.

* The seed has 20,000 executions with 10 events each, over 20 daily
  cohorts. Half of them expire, so the pass visits 10,000 candidates.
* A concurrent load appends events and claims tasks during the pass.
* PostgreSQL 16 with `pg_stat_statements` preloaded and
  `pg_stat_statements.track_planning = on`. Default `shared_buffers`.
* Base: `trunk-dev` at `1c692d2`. Measured on 2026-11-13.

The plan checks use a second fixture,
[`fixture.sql`](perf-artifacts/retention-pass-statement-cache/fixture.sql),
at 100k, 300k and 1M executions. Its shape follows the issue #2668
fixture: 96% terminal, 3.5% RUNNING, 0.5% PAUSED, 50 skewed workflow
names. It adds the lineage that the retention gate reads: continue-as-new
chains, children of a few parents, forks and legal holds.

## 📈 Profile

Each statement of the candidate loop runs once or twice per candidate.
The census filters on that call count. The seed and the load have other
counts.

| Share of the bench database, seed and DDL excluded | buffers | calls |
|:--|--:|--:|
| the candidate loop | 54.0% | 15.0% |
| the planning of the five statements below | 41.2% | 6.9% |

The five statements had `plans = calls`:

| Statement | Function | calls | plans | buffers |
|:--|:--|--:|--:|--:|
| open-fork lineage CTE | `fork::is_pinned_by_recorded_fork` | 20,000 | 20,000 | 1,900,293 |
| legal-hold read `FOR UPDATE` | `retention::delete_candidate_execution` | 10,000 | 10,000 | 301,256 |
| chain-link count | `retention::should_skip_candidate` | 10,000 | 10,000 | 293,176 |
| active-child count | `retention::should_skip_candidate` | 10,000 | 10,000 | 280,085 |
| child lineage null-out | `retention::delete_candidate_execution` | 10,000 | 10,000 | 280,000 |

With `track_planning = off`, the same five statements read 172,646
buffers. So planning was about 2.88M of their 3.05M buffers.

Wait-event samples at 200 ms during the pass agree. The fork CTE and the
chain-link count were the most-sampled active statements of the loop, on
CPU. Their execution reads 3 buffers per call.

## 💡 Hypothesis

Diesel sends a statement unnamed when it reports that caching is unsafe.
Three of the five are `diesel::sql_query`. The legal-hold read holds two
`sql::<T>()` fragments. The null-out is a DSL `UPDATE`. Each text is
constant, so each can stay prepared.

The planning cost is high on this table. `harvest_workflow_executions`
has 39 indexes, and the planner reads the catalog entry of each index
that a statement could use. The recursive fork CTE names the table three
times, so it read 92 buffers per plan.

## 🔧 Change

Each of the five statements carries `CachePreparedDsl::cache_prepared`.
The legal-hold read uses `.limit(1).get_result`, the same SQL that
`first` sends. No statement text changes. No bind changes. There is no
migration.

The three rules of [`cached_stmt`](../autumn-harvest/src/cached_stmt.rs)
hold for each statement:

1. **Constant text.** Each text is a constant, or a DSL statement of a
   fixed shape. The null-out binds a fixed array of six states.
2. **Named columns.** Each statement names its result columns.
3. **An acceptable generic plan.** Each partial-index predicate is a
   literal in the text, or follows from a bound equality. See the plan
   check below.

## 🧭 Plan

[`plans.sql`](perf-artifacts/retention-pass-statement-cache/plans.sql) runs
each statement as a prepared statement under `force_custom_plan` and
`force_generic_plan`. Its bind values are the worst cases of the fixture.

At 100k, 300k and 1M executions, the generic plan of each statement uses
the same nodes and the same indexes as the custom plan:

| Statement | Access path, custom and generic |
|:--|:--|
| active-child count | Bitmap Index Scan on `idx_harvest_we_parent_id` |
| open-fork lineage CTE | Index Scan on `idx_harvest_we_fork_source`, then a Recursive Union |
| chain-link count | Index Scan on `idx_harvest_wfx_workflow_identity` |
| legal-hold read | Index Scan on `harvest_workflow_executions_pkey`, LockRows |
| child lineage null-out | Bitmap Index Scan on `idx_harvest_we_parent_id` |

The full `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` output is in
`baseline-plans-<size>.txt`.

## 📊 Measurement

The same harness, the same seed and the same session, with
`track_planning = on` in both runs. Tool: `pg_stat_statements`.

| Statement | plans before | plans after | buffers before | buffers after | change |
|:--|--:|--:|--:|--:|--:|
| open-fork lineage CTE | 20,000 | 9 | 1,900,293 | 61,158 | −96.8% |
| legal-hold read `FOR UPDATE` | 10,000 | 9 | 301,256 | 43,075 | −85.7% |
| chain-link count | 10,000 | 8 | 293,176 | 39,231 | −86.6% |
| active-child count | 10,000 | 8 | 280,085 | 20,328 | −92.7% |
| child lineage null-out | 10,000 | 8 | 280,000 | 20,212 | −92.8% |
| **the five** | **60,000** | **42** | **3,054,810** | **184,004** | **−94.0%** |

| The candidate loop | Before | After |
|:--|--:|--:|
| client statements per candidate | 13 | 13 |
| plans | 60,046 | 88 |
| buffers | 3,781,048 | 916,459 (−75.8%) |
| `temp_blks_written` | 0 | 0 |
| WAL bytes | 13,163,565 | 13,169,309 |
| planning time | 23.4 s | 0.02 s |
| pass total, as the bench reports it | 64.65 s | 22.52 s |

The statement count does not change. Each statement still runs once or
twice per candidate. The planning time and the pass total are wall-clock
numbers. Each moves by more than 2 times, in the same direction as the
buffer count.

## ✅ Equivalence

No statement text or bind changes, so only the plan can change.
[`equivalence.sql`](perf-artifacts/retention-pass-statement-cache/equivalence.sql)
runs the four reads over every execution of the 100k fixture, under the
custom and the generic plan. The result sets are identical, 100,000 rows
each
([`equivalence-100000.txt`](perf-artifacts/retention-pass-statement-cache/equivalence-100000.txt)).

The rows cover a run with no child, 621 runs with an active child, 10 runs
with an open fork, 13,688 runs with a chain link, and 97 held runs. A
second chain-link probe sets the cutoff to the `completed_at` of the run
itself, the boundary of the `>=` predicate.

The bench collects the same rows in both runs: 100,000 events of 10,000
executions. The retention, fork and legal-hold integration tests pass
unchanged.

## 💸 Write cost

No index. The WAL of the loop is the same within 0.05%. Each cached text
costs each pooled connection one prepared statement.

## What stays

* **The loop is still one transaction per candidate.** Issue #1270
  records why: archival, the legal-hold re-check under `FOR UPDATE` and
  the summary demotion visit each execution. A batched loop needs its
  own design.
* **The dependency probes of `should_skip_candidate` still run once per
  candidate.** One statement for the batch would move the check earlier.
  Only the fork check runs again under the row lock. The other probes do
  not, so a batch would widen their window.
* **The recursive step of the fork CTE reads every fork row.** Its hash
  join scans `idx_harvest_wfx_start_source` once per level that finds a
  fork. It reads 147 buffers at 100k executions with 193 forks. It runs
  only for a run that has a fork.

## 🔬 Reproduce

```sh
cargo bench -p autumn-harvest --features db --bench retention_reclaim_bench --no-run
docs/perf-artifacts/retention-pass-statement-cache/census.sh after \
  target/release/deps/retention_reclaim_bench-<hash> \
  docs/perf-artifacts/retention-pass-statement-cache
```

`census.sh` needs `pg_stat_statements` in `shared_preload_libraries` and
the extension in the `postgres` database. The plan and equivalence checks
run on any migrated database:

```sh
psql -v n=1000000 -f fixture.sql
psql -f plans.sql
psql -f equivalence.sql
```
