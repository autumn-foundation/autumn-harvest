# Usage-report activity-attempt lookback indexed (issue #596)

`usage::usage_sql()` -- the query behind `GET /admin/usage` -- resolves each
activity terminal event's owning `ActivityStarted` attempt with a `LEFT JOIN
LATERAL` correlated subquery. The earlier
`20260702000000_harvest_usage_report_indexes` migration indexed every other
CTE in this query but not this one. This page measures that gap and fixes it.

## 🎯 Workload

`GET /admin/usage` (issue #596): an operator-facing chargeback/consumption
report, aggregating `harvest_workflow_executions` + `harvest_events` over a
caller-supplied `[from, to]` window, grouped by `workflow_name` or a
`search_attrs` key. It is the single query this endpoint issues -- there is
no other statement in the request path -- so it is the entire workload being
profiled here, not a slice of a larger one.

`tests/integration/usage_report_activity_lookback_tests.rs::zz_capture_usage_report_activity_lookback_evidence`
seeds a production-shaped fixture: 40,000 `harvest_workflow_executions` rows
across 30 workflow names spread over the last 80 days, with a **skewed**
per-execution activity count -- 85% get 1-5 activities (the overwhelming
majority of real workflows), 14% get 5-25, and a 1% "batch/DAG-run" tail gets
50-300 -- plus a 10% chance per activity of a second (retry) `ActivityStarted`
attempt. This produced ~562,000 `harvest_events` rows in the committed run
(workflow-terminal events, and 2-3 events per activity). Reproduce with:

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh
```

Two review-round corrections to the fixture generator, both real and both
fixed before the numbers below were captured: the fan-out and
activity-outcome `CASE` expressions each originally called `random()`
independently per `WHEN` branch, so an execution/activity that missed the
first threshold reached the tail bucket only on a SECOND independent draw
clearing the second threshold too -- silently shrinking the intended 1%
"batch" tail to ~0.15% (Codex review, PR #1381). The fix draws one `random()`
value per row from the same flat `SELECT` list as the row source and reuses
it for every threshold in the `CASE`; the initial fix attempt used `CROSS
JOIN LATERAL (SELECT random() AS r)` instead, which -- being uncorrelated --
Postgres hoists and evaluates exactly once for the entire statement rather
than once per row (verified directly: every row landed in the same bucket).
Both were caught before publication; the measurements below reflect the
correct, verified 85%/14%/1% and 90%/8%/2% distributions.

## 📈 Profile

`GET /admin/usage` issues exactly one statement per shard
(`usage::load_usage_grouped`), so this is not a case of ranking many
statements by share of a larger workload -- the query under test **is** 100%
of this endpoint's database cost. Read directly from `EXPLAIN (ANALYZE,
BUFFERS)`: within that one statement, the `activity_metrics` CTE's `LEFT JOIN
LATERAL` accounts for the large majority of total buffers (before the fix:
~3.90M of ~4.63M total, ~84%), because it runs once per row of
`activity_events` -- every `ActivityStarted`/`ActivityCompleted`/
`ActivityFailed`/`ActivityTimedOut` event in the report window,
`loops=522,374` in this run -- while every other CTE (`execution_starts`,
`terminal_counts`, `reset_terminated_execs`) is a single indexed scan over
`harvest_workflow_executions`/`harvest_events` already covered by the 2026-07
migration.

## 🧭 Plan

Before (full plan in
[`docs/perf-artifacts/usage-report-activity-lookback/before.explain.txt`](perf-artifacts/usage-report-activity-lookback/before.explain.txt)):

```text
->  Aggregate  (actual rows=1 loops=522374)
      Output: max(e2."timestamp")
      Buffers: shared hit=3896701 read=4525 written=2319
      ->  Bitmap Heap Scan on public.harvest_events e2  (actual rows=1 loops=522374)
            Output: e2."timestamp"
            Recheck Cond: ((e2.workflow_exec_id = e.workflow_exec_id) AND (e2."timestamp" <= e."timestamp"))
            Filter: ((e2.event_type = 'ActivityStarted'::text) AND ((e2.event_data #>> '{data,activity_id}'::text[]) = (e.event_data #>> '{data,activity_id}'::text[])))
            Rows Removed by Filter: 63
            Heap Blocks: exact=2130234
            Buffers: shared hit=3896701 read=4525 written=2319
            ->  Bitmap Index Scan on idx_harvest_events_exec_last  (actual rows=64 loops=522374)
                  Index Cond: ((e2.workflow_exec_id = e.workflow_exec_id) AND (e2."timestamp" <= e."timestamp"))
```

After (full plan in
[`docs/perf-artifacts/usage-report-activity-lookback/after.explain.txt`](perf-artifacts/usage-report-activity-lookback/after.explain.txt)):

```text
->  Result  (actual rows=1 loops=522374)
      Output: $3
      Buffers: shared hit=2084271 read=5225 written=30
      InitPlan 1 (returns $3)
        ->  Limit  (actual rows=1 loops=522374)
              Output: e2."timestamp"
              ->  Index Scan Backward using idx_harvest_events_activity_started_lookup on public.harvest_events e2  (actual rows=1 loops=522374)
                    Output: e2."timestamp"
                    Index Cond: ((e2.workflow_exec_id = e_2.workflow_exec_id) AND ((e2.event_data #>> '{data,activity_id}'::text[]) = (e_2.event_data #>> '{data,activity_id}'::text[])) AND (e2."timestamp" IS NOT NULL) AND (e2."timestamp" <= e_2."timestamp"))
```

The changed node: Postgres's own `MAX()`-via-index-descent transform replaces
a `Bitmap Heap Scan` (recheck on `workflow_exec_id` + `timestamp` alone,
`event_type`/`activity_id` resolved by a post-scan `Filter` that discarded an
average of 63 sibling rows per loop -- `Rows Removed by Filter: 63`,
`Heap Blocks: exact=2,130,234` total) with an `Index Scan Backward` + `Limit 1`
against the new index, whose leading columns already pin
`workflow_exec_id`/`activity_id` exactly and whose trailing `timestamp` column
makes `MAX(...) WHERE timestamp <= $bound` answerable by walking the index
backward from the bound instead of aggregating over every candidate row.

## 💡 Hypothesis

The subquery is:

```sql
LEFT JOIN LATERAL (
    SELECT MAX(e2.timestamp) AS last_started_at
    FROM harvest_events e2
    WHERE e2.workflow_exec_id = ae.workflow_exec_id
      AND e2.event_type = 'ActivityStarted'
      AND e2.event_data #>> '{data,activity_id}' = ae.activity_id
      AND e2.timestamp <= ae.timestamp
) s ON true
```

The only index naming `workflow_exec_id` at all before this fix is the
initial migration's `idx_harvest_events_exec (workflow_exec_id, event_id)`
(plus `idx_harvest_events_exec_last`, `(workflow_exec_id, timestamp)`-shaped,
added for the stalled-workflow scanner) -- neither carries `event_type` or the
JSON-extracted `activity_id`, so neither can serve an equality lookup on
either. The planner's only option was `idx_harvest_events_exec_last`'s
`(workflow_exec_id, timestamp)` prefix, recheck on those two columns, then
filter out every event that is not this specific activity's `ActivityStarted`
-- i.e., every OTHER event belonging to the same execution (all event types,
any activity), once per activity terminal event in the report window. The
cost is therefore structural and scales with a workflow's own activity
fan-out, not with the report's selectivity: the 1% "batch" tail this fixture
seeds (50-300 activities per execution) pays this once per activity, and it
is invisible in a small-fixture test precisely because a typical 1-5-activity
workflow has few sibling events to filter out.

## 🔧 Change

Migration `20260905181020_harvest_usage_activity_lookback_index`:

```sql
CREATE INDEX IF NOT EXISTS idx_harvest_events_activity_started_lookup
    ON harvest_events (workflow_exec_id, (event_data #>> '{data,activity_id}'), timestamp)
    WHERE event_type = 'ActivityStarted';
```

Partial (`WHERE event_type = 'ActivityStarted'`) and keyed exactly to the
subquery's three predicates in order, so only `ActivityStarted` rows -- the
only event type this lookup ever targets -- pay for it; no other event type's
write path is touched. No query-text change: `usage_sql()` is byte-identical
before and after, only the schema gains a supporting index.

`CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events` for the
build's duration, blocking every append, claim and completion touching this
table -- the same trade-off `20260702000000_harvest_usage_report_indexes`
made and documented. Measured build time on this fixture's ~273,500
`ActivityStarted` rows (out of ~562,000 total `harvest_events` rows):
**~329 ms**. For a live, already-large deployment (including one that has
opted into the partitioned `harvest_events` layout -- see the migration's own
comment for the partition-aware recipe), build it out-of-band first with
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` (cannot run inside Diesel's
migration transaction) and this migration's own statement becomes a safe
no-op via `IF NOT EXISTS`; `CONCURRENTLY` can leave an `INVALID` index behind
on failure, so check `pg_index.indisvalid` for this index's oid before
relying on it if you take that path.

Rollback (`DROP INDEX IF EXISTS idx_harvest_events_activity_started_lookup;`)
measured at **~3 ms** on the same fixture.

## 📊 Measurement

One cold run per form (`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING
OFF)` plus one plain execution), same 40,000-execution fixture, before and
after captured **in the same test run against the same seeded fixture** --
the candidate index is dropped unconditionally before the "before" capture
(so this stays reproducible even after the migration ships and
`setup_bench_db` runs it ahead of time), then created for the "after"
capture. Full artifacts under
[`docs/perf-artifacts/usage-report-activity-lookback/`](perf-artifacts/usage-report-activity-lookback/).

Numbers below are the plain (non-`EXPLAIN`) execution's `pg_stat_statements`
row -- the real production query cost, not `EXPLAIN`'s own instrumentation
overhead:

| | `shared_blks_hit` | `shared_blks_read` | **Total buffers** | `temp_blks_written` |
|:--|--:|--:|--:|--:|
| Before | 4,594,831 | 35,316 | **4,630,147** | 0 |
| After | 2,098,162 | 10,786 | **2,108,948** | 0 |
| **Δ** | | | **-2,521,199 (-54.45%)** | 0 |

(The `hit`/`read` split above is the plain execution's row, taken right after
the `EXPLAIN`-wrapped run of the same query already warmed the cache -- it
varies run-to-run with cache state, as the evidence rules note. The
**total** is the stable, gated number and is read directly from the
committed `pg_stat_statements` snapshot files, not derived.)

Tool: `pg_stat_statements` (`shared_blks_hit + shared_blks_read`), captured
via `pg_stat_statements_reset()` immediately before each form's execution.
Unlike the history-ceiling scanner's evidence capture, this came back
non-empty on the first attempt once the harness explicitly ran `CREATE
EXTENSION IF NOT EXISTS pg_stat_statements` against the **bench** database --
`setup_bench_db` provisions a fresh throwaway database per run, and the
extension (unlike the underlying preloaded module) must be created in each
database that wants to query its own view into it.

**-54.45% clears the impact floor** (`>=20%` reduction in total buffers) with
nearly 2.7x margin. No temp blocks in either form -- no spill.

Rows read: `Heap Blocks: exact=2,130,234` before (the LATERAL's Bitmap Heap
Scan alone), replaced by a `Limit 1` per loop after -- one heap fetch per
activity terminal event instead of an average of 64 candidate rows
(`Rows Removed by Filter: 63`, plus the one that matched) examined per loop.

Statement count: unaffected -- this was never an N+1 across requests, one
statement per shard before and after.

WAL bytes: see [💸 Write cost](#-write-cost) below -- this is a read-path fix
with a real, measured write-path cost, not a pure `SELECT` rewrite.

## ✅ Equivalence

Both forms run against the **identical** fixture in the **identical** test
run, each producing its sorted grouped result set (one row per
`workflow_name`, all nine reported columns), asserted equal:

```
equivalence confirmed: before and after agree on all 30 groups
```

Full result-row dumps are committed as
[`before.result-rows.txt`](perf-artifacts/usage-report-activity-lookback/before.result-rows.txt) /
[`after.result-rows.txt`](perf-artifacts/usage-report-activity-lookback/after.result-rows.txt)
and are byte-identical.

Edge cases, exercised explicitly by the always-run
`usage_report_activity_lookback_index_does_not_change_the_result_set` (not
just implied by the production-shaped fixture matching by chance):

* **Retry resolution.** A workflow with two `ActivityStarted` attempts
  sharing one `activity_id`, terminal event after the second: asserted that
  `activity_compute_seconds` is measured from the **second** (later) start,
  not the first -- exactly the `MAX(timestamp) WHERE timestamp <= terminal.timestamp`
  predicate this index now serves directly. Same result with and without the
  index.
* **No matching start (external activity).** An `ActivityTimedOut` with no
  `ActivityStarted` at all: asserted it does **not** count toward
  `activity_executions_failed` (the module doc's documented exclusion,
  implemented via `s.last_started_at IS NOT NULL`). Same result with and
  without the index.

This is not a case with `ORDER BY`/`LIMIT`-tie nondeterminism to worry about
for the LATERAL itself: `MAX()` over a `<=` bound is well-defined regardless
of how many rows satisfy it, and grouping is by `workflow_name`/`search_attr`
value, not by any column this index touches. Isolation/visibility: unchanged
-- the fix touches only the read-only report query; no transaction boundary
in `load_usage_grouped` is affected.

## 💸 Write cost

Measured on the same fixture (~562,000 `harvest_events` rows, ~273,500 of
them `ActivityStarted`):

* **Index size:** 23 MB.
* **Build WAL (one-time):** 21,481,472 bytes (~20 MB), proportional to the
  `ActivityStarted` backlog at build time.
* **Ongoing insert WAL:** inserting a batch of 10,000 `ActivityStarted` rows
  produced 10,378,888 bytes of WAL with the index present vs. 7,737,416 bytes
  without it -- **+34.1% WAL** on `ActivityStarted` inserts specifically. No
  other event type's insert path is touched (the index is partial on
  `event_type = 'ActivityStarted'`), and `ActivityStarted` is one of four
  event types written per activity attempt, not the majority of
  `harvest_events` traffic.

`idx_scan` confirmation: the `after` `EXPLAIN` plan shows
`idx_harvest_events_activity_started_lookup` used directly (`Index Scan
Backward using idx_harvest_events_activity_started_lookup`), not merely
present-but-unused.

## 🔬 Reproduce

```bash
# Fast, always-run correctness test (no seeding, runs against a migrated
# HARVEST_TEST_DATABASE_URL or the container fallback):
cargo test -p autumn-harvest --features db,testing --test integration -- \
  usage_report_activity_lookback_tests::usage_report_activity_lookback_index_does_not_change_the_result_set

# Full evidence capture (seeds ~560K+ rows; roughly 20-30 seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh
```

## See also

* `autumn-harvest/src/usage.rs` -- `usage_sql()`, the query this page
  measures.
* `tests/integration/usage_report_activity_lookback_tests.rs` -- the harness
  and evidence-capture test.
* `autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh` --
  regenerates the committed artifacts from a clean checkout.
* `docs/perf-artifacts/usage-report-activity-lookback/` -- committed
  before/after `EXPLAIN` and result-set evidence.
* `docs/performance-history-ceiling.md` -- the sibling Ledger writeup this
  page's structure and evidence-capture pattern follow.
