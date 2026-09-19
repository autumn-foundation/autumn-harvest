# `GET /admin/status` — the stalled-workflows anti-join

`status_summary::count_stalled_candidates` (`autumn-harvest-plugin/src/status_summary.rs`)
counts stalled candidates for the `stalled_workflows` subsystem of
`GET /admin/status` (issue #679). Issue #1643 profiled it and found its
`harvest_events` anti-join responsible for 86.9% of a request's buffers on a
3,000-active-execution fixture: `NOT EXISTS (... timestamp >= NOW() - N
minutes)` is correlated on a range condition, so Postgres cannot hash it —
it runs as a `Nested Loop Anti Join`, one B-tree probe per `RUNNING`/
`SUSPENDED` row.

Issue #1643 explicitly declined to propose a fix without first
characterizing it against two workload shapes, because the obvious rewrite
— a `MATERIALIZED` CTE precomputing "execution ids with a recent event" —
scans `harvest_events` by timestamp fleet-wide instead of once per active
execution. Whether that is cheaper depends on write volume relative to
active-execution count, which is deployment-specific.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it.

## TL;DR

* **The fix**: `count_stalled_candidates_query()` now reads
  `recent_event_execs`, a `MATERIALIZED` CTE built once per call, then
  anti-joins it against active executions by plain equality — a `Hash Anti
  Join`, not a `Nested Loop Anti Join`. A new index,
  `idx_harvest_events_recent_by_timestamp (timestamp, workflow_exec_id)`,
  supports the CTE's timestamp-range scan (neither existing `harvest_events`
  index leads with `timestamp`).
* **Characterized against two regimes**, both with the same 3,000
  active executions (2,970 healthy, 30 true positives) and the same
  50,000-row terminal-execution dead weight, differing only in how many
  recent events each healthy execution emits:

  | regime | events per healthy execution | recent-window rows | before (buffers) | after (buffers) | change |
  |:--|--:|--:|--:|--:|--:|
  | execution-heavy (issue's own fixture shape) | 1 | ~2,970 | 12,175 | 267 | **-97.8%** |
  | event-write-heavy | 100 | ~297,000 | 12,175 | 5,724 | **-53.0%** |

  The rewrite is a large win in the execution-heavy regime (the common
  case: most deployments have far more active executions than any one of
  them emits events per minute) and remains a smaller but real win even at
  100x the per-execution event-write rate. It is not a net loss in either
  measured regime — the concern issue #1643 raised did not materialize at
  either tested scale, though a fleet with an even higher event-write rate
  relative to active-execution count could still cross over (see
  [Where this could stop being a win](#where-this-could-stop-being-a-win)).
* **Result-equivalence**: the true-positive/healthy split (30 stalled, 0 of
  the 2,970 healthy rows) is asserted identical in both regimes and in a
  small always-run fixture
  (`count_stalled_candidates_matches_seeded_true_positives_small_fixture`).
  Boundary and multiplicity edge cases (event just inside/outside the
  window, an old event alongside a fresh one) are covered by
  `status_summary_localpg.rs::stalled_workflows_recent_event_boundary_and_multiplicity`.
  The pre-existing `GET /admin/status` and `GET /workflows?no_progress_minutes=N`
  integration suites pass unmodified against the fixed query.
* **Scope**: only `count_stalled_candidates` was rewritten.
  `api::load_stalled_workflows` (`GET /workflows?no_progress_minutes=N`)
  carries the identical anti-join shape but is a Diesel boxed query with
  spliced raw-SQL filters, not a hand-rolled string — retrofitting a CTE
  there needs to keep composing with its ~15 other optional filters and is
  left for a follow-up, as issue #1643 itself scoped only the
  `/admin/status` call site.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest-plugin/tests/status_summary_stalled_perf.rs` |
| Artifacts | `docs/perf-artifacts/status-summary-stalled-cte/` (committed, this page's source) |

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_VARIANT=after \
  cargo test -p autumn-harvest-plugin --test status_summary_stalled_perf -- \
  --ignored --exact zz_capture_status_summary_stalled_perf_evidence --nocapture
```

`HARVEST_TEST_DATABASE_URL` is an **admin** URL: a fresh, uniquely-named
database is created off it per regime, migrated via
`autumn_harvest::test_init_sql()`, seeded, and measured.
`pg_stat_statements` must be preloaded via `shared_preload_libraries =
'pg_stat_statements'`; without a target that has it preloaded, use Docker/
testcontainers instead (the harness falls back automatically when
`HARVEST_TEST_DATABASE_URL` is unset).

The "before" row was captured by checking out `count_stalled_candidates`'s
pre-#1643 SQL and temporarily removing the new index, then running the
identical harness — the harness itself only calls the public HTTP entry
point and reads `pg_stat_statements`, so it is unchanged between the two
runs; only the code and schema behind the endpoint moved. The pre-fix
query's own `EXPLAIN (ANALYZE, BUFFERS)` plan (`Nested Loop Anti Join`,
`loops=3000`) is reproduced in issue #1643 itself and is not re-captured
here.

## Profile: execution-heavy regime

`EXPLAIN (ANALYZE, BUFFERS)` after the fix
(`docs/perf-artifacts/status-summary-stalled-cte/after-execution-heavy-explain.txt`):

```
Aggregate (actual rows=1 loops=1)
  Buffers: shared hit=267
  CTE recent_event_execs
    -> HashAggregate (actual rows=2970 loops=1)
          Buffers: shared hit=62
          -> Bitmap Heap Scan on harvest_events (actual rows=2970 loops=1)
                Recheck Cond: (timestamp >= (now() - '01:00:00'::interval))
                Buffers: shared hit=62
                -> Bitmap Index Scan on idx_harvest_events_recent_by_timestamp
                      Buffers: shared hit=24
  -> Limit (actual rows=30 loops=1)
        Buffers: shared hit=267
        -> Hash Anti Join (actual rows=30 loops=1)
              Hash Cond: (e.id = r.workflow_exec_id)
              Buffers: shared hit=267
              -> Index Scan using idx_harvest_we_non_terminal_wf_name on
                 harvest_workflow_executions e (actual rows=3000 loops=1)
                    Buffers: shared hit=205
                    [OR-block SubPlans, unchanged, ~132 buffers total]
              -> Hash (actual rows=2970 loops=1)
                    -> CTE Scan on recent_event_execs r (actual rows=2970 loops=1)
                          Buffers: shared hit=62
```

The anti-join is now a `Hash Anti Join`: `recent_event_execs` is built once
(`loops=1`), hashed once, and probed once per candidate row at O(1)
amortized cost — replacing the `Nested Loop Anti Join`'s `loops=3000`. The
OR-block subplans are untouched (still hashed SubPlans, ~132 of 267
buffers) — the rewrite touches only the "no recent event" check.

## Profile: event-write-heavy regime

Same active-execution count, but each of the 2,970 healthy executions emits
100 recent events instead of 1 (~297,000 rows in the window, 66% of the
447,030-row `harvest_events` table). The planner correctly switches
strategy
(`docs/perf-artifacts/status-summary-stalled-cte/after-event-write-heavy-explain.txt`):

```
CTE recent_event_execs
    -> HashAggregate (actual rows=2970 loops=1)
          Buffers: shared hit=3797 read=1722
          -> Gather (Workers Launched: 2)
                -> Parallel Seq Scan on harvest_events (actual rows=99000 loops=3)
                      Filter: (timestamp >= (now() - '01:00:00'::interval))
                      Rows Removed by Filter: 50010
```

At 66% selectivity a sequential scan beats the index — Postgres runs it in
parallel and still lands at 5,724 total buffers, 2.1x cheaper than the
pre-fix per-row probe's 12,175. This is the planner working as intended,
not a missing index: an index cannot make a majority-selectivity range scan
cheaper than reading the table.

## Where this could stop being a win

Both regimes hold active-execution count constant (3,000) and vary only
per-execution event-write rate. The two data points suggest the crossover
is roughly where recent-window rows approach ~200x the active-execution
count (interpolating: ~267 buffers at ~1x, ~5,724 buffers at ~100x, versus
a constant ~12,175 pre-fix) — a fleet with active executions in the
thousands but *extreme* per-execution write rates (tens of thousands of
events per minute per execution) could tip the balance back. This is an
approximation from two measured points, not an exhaustively characterized
curve. A deployment with a workload shape far outside both regimes measured
here should re-run this harness against its own fixture shape before
relying on this rewrite being a win.

## Equivalence

Both regimes return `stalled_count = 30` — every seeded true positive, none
of the 2,970 healthy executions — asserted directly in
`capture_regime`'s evidence-capture path. Boundary conditions (an event
just inside/just outside the 60-minute default window) and multiplicity
(an old event coexisting with a fresh one on the same execution) are
covered by `status_summary_localpg.rs`'s
`stalled_workflows_recent_event_boundary_and_multiplicity`. The pre-existing
`admin_status_localpg_end_to_end` and `status_summary_integration.rs` /
`stalled_workflow_tests.rs` suites pass unmodified.

## Write cost

One new index, `idx_harvest_events_recent_by_timestamp (timestamp,
workflow_exec_id)`, on `harvest_events` — the highest-write-volume table in
the system. This adds one B-tree insert per event write. No existing index
was removed.
