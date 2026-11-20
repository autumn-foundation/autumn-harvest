# DESIGN-2713: Keep by-id task row statements on the primary key

## Problem

Many statements find one `harvest_task_queue` row by `id` and guard it on
`state` and `worker_id`. A partial index matches each guard:

- `harvest_task_queue_running_worker_idx (worker_id) WHERE state = 'RUNNING'`
- `idx_harvest_tq_running_started (started_at) WHERE state = 'RUNNING'`
- `idx_harvest_tq_live_created (...) WHERE state IN ('PENDING', 'RUNNING')`
- `harvest_task_queue_concurrency_key_running (concurrency_key) WHERE state = 'RUNNING' ...`
- the `state = 'PENDING'` indexes, such as `idx_harvest_tq_poll`

Each path estimates one row. The planner then picks the smaller index, not
the primary key. That index path reads one heap tuple for each entry of the
partial index that matches the guard. Then it filters on `id`. A cached
generic plan keeps the choice for the life of the connection.

## Facts (white hat)

Fixture: a migrated database, claim settings, `force_generic_plan`.

| Fixture | Guard | Plan before | Buffers before | Plan after | Buffers after |
|---|---|---|--:|---|--:|
| 1M rows, 20 RUNNING per worker | `state` + `worker_id` | `running_worker_idx` | 24 | `pkey` | 6 |
| 100k rows, 20 RUNNING per worker | `state` + `worker_id` | `running_worker_idx` | 24 | `pkey` | 6 |
| 1M rows, stats from an idle table | `state` + `worker_id` | `live_created` | 201 | `pkey` | 6 |
| 1M rows, stats from an idle table | `state` only | `live_created` | 200 | `pkey` | 5 |
| 100k rows, stats from an idle table | `state IN (...)` | `live_created` | — | `pkey` | — |

The 100k rows of the last row show the plan choice only.

The idle-table row is new evidence. Autovacuum often analyzes the queue
while no task runs. The partial indexes then have `reltuples = 0`, and every
guard form wins over the primary key. When the load returns, the cached
plan scans every live row.

## Options (green hat, brainstorming)

1. Wrap the guard in `CASE WHEN <guard> THEN true END`. The planner cannot
   match a `CASE` to an index column or to a partial index predicate.
2. Change `running_worker_idx` to `(worker_id, id)`. This fixes the grouped
   case only. The idle-table case still picks `live_created`. It also needs
   a concurrent index build on a hot table.
3. Use `force_custom_plan`. The issue measured a custom plan that still
   picked `running_worker_idx` at 200 rows per worker.
4. Add extended statistics. The estimates are already one row. The tie is
   in index cost, not in row count.
5. Use `pg_hint_plan`. The engine cannot require an extension.
6. Use other expression tricks, such as `state || '' = 'RUNNING'`. They
   work, but each guard needs its own trick. One `CASE` form is clearer.

Decision: option 1, through one helper for raw SQL and one for Diesel.

## Reverse brainstorming: how can the fix fail?

| Way to fail | Prevention |
|---|---|
| `CASE` gives NULL, not false, so a result changes. | Use the form only in `WHERE`. `WHERE` drops NULL and false the same way. A truth-table test runs both forms on every input. |
| A concurrent writer changes the row, and the recheck of the new row version skips the `CASE`. | Postgres rechecks every qual of an `UPDATE`. A concurrency test blocks a recheck behind a completion and a reclaim. |
| The form goes on a scan that needs the partial index. | Use the form only with `id = $1`. A scan by worker keeps the plain guard. |
| A select-list use of `claim_held` changes. | The select list keeps the plain predicate. Only the `WHERE` form changes. |
| A new by-id statement uses the plain guard. | Two unit tests walk the engine source and fail on a plain by-id guard. |
| A shape test or splice anchor reads the old text. | Update the shape tests. The claim splice anchors are in the claim CTE, which this change does not touch. |

## Feelings (red hat)

The `CASE` form looks like a mistake to a new reader. A named helper with a
short reason makes the intent clear.

## Risks (black hat)

- Semantics. The truth-table test and the concurrency test cover it.
- Coverage. The plan test checks each raw statement and each Diesel guard
  shape. A source walk checks each Diesel call site.
- Other Postgres versions. A `CASE` is opaque to index matching on every
  supported version.

## Benefits (yellow hat)

- A recheck costs about 6 buffers for any slot count.
- A cached generic plan cannot keep a slot-count-dependent index path.
- A plan from idle-table statistics cannot scan every live row.

## Scope (blue hat)

In scope: each per-task statement that finds one task row by `id` and
guards on `state` or `worker_id`.

- The claim releases: queue pause, unconditional, activity pause, workflow
  pause and the concurrency cap.
- The park statements (the "activity start CTE" of the issue).
- The claim fence `claim_held` in a `WHERE` clause: completion, heartbeat,
  handler start, fenced fail and requeue writes, and the claim locks.
- The other by-id releases, requeues, defers and fails in `queue.rs`,
  `poison_pill.rs` and `worker.rs`, and the pending-hint read.

Out of scope:

- The by-id claim. It is a variant of the claim statement, which has its
  own plan guards. Issue #2751 measures it.

- Scans by worker, by execution or by key. They need their partial index.
- The SQLite backend. Its planner has no generic plan cache.
- Select-list uses of `claim_held`. They do not filter rows.
- Statements on `harvest_workflow_executions`. That table is not in the
  measured family.

## Plan

1. RED. A DB test builds both fixtures. For each statement in scope, it
   asserts that the generic plan reads `harvest_task_queue` only through
   `harvest_task_queue_pkey`. Shape tests assert the `CASE` form.
2. GREEN. Add `row_guard_sql!` for raw SQL and `row_guard` for Diesel.
   Apply them to each statement in scope.
3. Proof. A truth-table test runs the plain and the `CASE` form on every
   input. A concurrency test runs a recheck behind a completion, a reclaim
   and a pause.
4. Measure. Record buffers before and after on the 1M-row fixture, and an
   `e2e_bench` throughput run, in `docs/performance-claim-release-guard.md`.
5. REFACTOR. Run the gates.

## Acceptance criteria

1. The three claim-release rechecks read `harvest_task_queue` through its
   primary key at 5, 20, 50 and 200 RUNNING rows per worker, under the
   generic and the custom plan.
2. The park statements read it through its primary key.
3. The fenced completion, heartbeat and requeue writes read it through its
   primary key.
4. Each statement in scope keeps its primary key plan when the statistics
   come from an idle table.
5. The shape tests in `queue_pause.rs` and `queue.rs` pin the new form.
6. A test proves that the new form keeps the same rows, on every input and
   under a concurrent pause, completion and reclaim.
7. A Ledger page records the before and after cost, end to end and on the
   fixture.
