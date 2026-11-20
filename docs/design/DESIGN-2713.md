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
| The form goes on a scan that needs the partial index. | Use the form only with a key: `id = $1`, or a by-execution key (see the addendum). A scan by worker keeps the plain guard. |
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

- Scans by worker. They need their partial index. The addendum adds the
  by-execution statements.
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

## Addendum: by-execution statements

The first change kept the by-id statements on the primary key. The issue
also names "the completion and re-pend UPDATEs". The re-pend statements
find task rows by `workflow_exec_id`, not by `id`. They have the same
defect, through `idx_harvest_tq_workflow`.

### Facts (white hat)

Fixture: 1,000,000 rows, 4 rows for each run. Generic plan. Buffers are
`shared hit + read` of the top plan node. "Hot run" is one run with 50,000
terminal activity rows and 2 live rows. The idle case analyzes terminal
rows only. Then 40,000 live rows arrive with no new analyze. "Before" is the
plain form with no new index. "After" is the final form of this change.

| Statement | 5 per worker | Hot run | Idle statistics |
|---|--:|--:|--:|
| wake re-pend | 21 → 4 | 1,343 → 4 | 1,411 → 4 |
| wake fallback | 509 → 9 | 509 → 9 | 1,416 → 9 |
| due-row read of a wake | 4 → 4 | 1,326 → 4 | 1,231 → 4 |
| owner key read | 12 → 4 | 51 → 4 | 1,232 → 4 |
| parked-row read of a migration | 24 → 8 | 1,346 → 8 | 1,414 → 8 |
| fail or cancel of open tasks | 64 → 71 | 1,354 → 1,358 | 1,462 → 66 |
| drain of a failed or timed-out run | 69 → 72 | 1,359 → 1,359 | 1,466 → 66 |
| in-flight count | 5 → 4 | 1,327 → 1,327 | 1,411 → 5 |
| pending-row delete of a cancel | 4 → 4 | 1,326 → 1,326 | 18 → 4 |
| fork loser cancel | 2 → 2 | 2 → 2 | 1,412 → 2 |
| deadline shift of a resume | 3 → 5 | 38 → 1,418 | 1,502 → 154 |
| session liveness check | 2 → 4 | 2 → 1,326 | 1,411 → 4 |

The fallback has the issue's own defect. At 5 `RUNNING` rows per worker,
it scans all of `running_worker_idx` with `worker_id IS NOT NULL`. That
reads every `RUNNING` row in the table, not only those of one worker.

With idle statistics, each plain statement scans a partial index of live
rows. That reads every live row in the table. A wake runs the re-pend on
each activity completion, signal and child result.

A write figure includes the rows that it changes.

### Review finding: a run with many rows

A run keeps each terminal activity row until retention deletes the run.
With only the `CASE` form, each wake statement read every row of a hot run
through `idx_harvest_tq_workflow`. The plain form could combine that index
with a small live partial index in a bitmap.

Each run has one workflow task row. A new partial index,
`idx_harvest_tq_workflow_task (workflow_exec_id) WHERE task_type =
'workflow'`, holds that row only. The wake statements, the owner key read
and the migration statements keep `task_type = 'workflow'` as a plain test.
The planner proves the index predicate from it. They hide only `state` and
`worker_id`. Each one then reads one row for any run size.

The index adds about 40 bytes of WAL to an update of a workflow task row
that is not HOT, about 3% more. An activity row adds none. The index holds
about 31 bytes for each run.

Two statements cost more on a hot run, and the trade is accepted:

- The session check reads each row of its run. Its plain form can read
  every live row of the table after an idle analyze. A run bounds the new
  cost. The whole table bounds the old one.
- The deadline shift runs once for each resume. It reads each row of the
  run, as the drains do.

The fail, cancel and drain writes and the in-flight counts read each row of
the run in both forms. They run once at the end of a run.

### Review finding: the timeout batch re-read

`timeout::find_timed_out_tasks_batch` and `timeout::lapsed_moves` test a
scan predicate on a batch of ids. The predicate tests `state` before the
`id = ANY($1)` key, so the walk did not see it. After an idle analyze, a
batch of 100 ids read 1,414 buffers through `idx_harvest_tq_live_created`.

The batch now loads its ids through the primary key in a materialized CTE.
The predicate then tests those rows only. A batch of 100 ids reads about
380 buffers on every fixture. The plain form read 22 buffers when only 500
rows ran, so the fence costs more when the live set is very small.

### Options (green hat, brainstorming)

1. Hide the guard with `row_guard_sql!` and `row_guard`, as for the by-id
   statements.
2. Add `(workflow_exec_id, state)` to each partial index. That needs a
   concurrent build of several indexes on a hot table.
3. Make the execution index partial on the live states. A wake also reads
   terminal rows, and the idle case still picks `live_created`.
4. Accept the cost. The idle case reads every live row on each wake.
5. Add a partial index on the workflow task row of each run. A wake then
   reads one row for any run size.

Decision: option 1 for each statement, and option 5 for the statements that
read the workflow task row only.

### Reverse brainstorming: how can this change fail?

| Way to fail | Prevention |
|---|---|
| The guard hides a key column, such as `session_id` or `activity_id`. | Wrap only `state` and `worker_id` terms. Keep each key test plain. A `session_id` test then has no usable index, so the execution index serves it. |
| The guard hides `task_type = 'workflow'`, so the workflow task index cannot serve. | Keep that test plain. The plan test names the workflow task index as the key index. |
| An `OR` guard is split, and part of it stays plain. | Wrap the whole parenthesized `OR` as one top-level `AND` term. |
| A guard in a CTE wraps the `EXISTS` of the CTE. | Wrap the guard only. The `EXISTS` term stays a separate top-level term. |
| A wake fallback misses a park, so a wake is lost. | A concurrency test runs the fallback behind a park in both forms. |
| A new keyed statement uses a plain guard. | The source walks read every engine file and each top-level term of a keyed `WHERE` clause. They also find a `concat!` that drops `row_guard_sql!`. |
| A fixture stops provoking the defect, so the plan test proves nothing. | The plan test also plans each plain form. It fails when no plain form leaves its key index. |
| The plan test expects the primary key for a by-execution statement. | Each statement names the index that it must read. |

### Feelings (red hat)

A second family makes the rule broader. One rule for both families is
easier to keep than a list of exceptions.

### Risks (black hat)

- Semantics. The truth table, the differential run and the concurrency test
  cover the new statements.
- A by-execution statement with no other index. Each one keys on
  `workflow_exec_id`, and `idx_harvest_tq_workflow` covers every row.
- The fork cancel keys on `activity_id` too. It must keep
  `idx_harvest_tq_activity_id`.
- The new index adds a build on a hot table. The migration guards the
  build, and its header gives the `CONCURRENTLY` recipe.

### Benefits (yellow hat)

- A wake costs a few buffers for any queue size, any run size and any
  statistics.
- A timeout batch costs a fixed amount for each id.

### Scope (blue hat)

In scope: each statement that finds task rows by a key and guards them on
`state` or `worker_id`.

- `queue`: the wake re-pend, the wake fallback, the due-row read of a wake,
  the owner key read, the fail and cancel of open tasks, and the session
  liveness check.
- `execution`: the deadline shift of a resume and the pending-row delete
  of a cancel.
- The drains of `poison_pill`, `worker` and `timeout`.
- The fork loser cancel, the reset count and the retention count.
- `shard_rebalance`: the parked-row read, the seal cancel and the signal
  re-pend.
- `timeout`: the batch and probe re-reads of a scan.

The plan test checks the seal cancel through the source walk only. Its
statement also runs the quiescence check, a join that is out of scope.

Out of scope: a join on `workflow_exec_id`, such as the quiescence check,
and a scan that excludes executions. Neither one keys on a parameter. The
deadline shift of `harvest_external_tasks` is on another table.

### Plan

1. RED. Add the by-execution statements to `guarded_statements`, each with
   its key index. Seed the plan test with executions. The plan test then
   fails for each statement that leaves its key index. Widen the source
   walks to every engine file and to by-execution keys.
2. GREEN. Wrap each guard. Extract the inline wake read to a `const fn`,
   and the two inline migration statements to constants.
3. Proof. Add the by-execution raw statements to the differential run. Add
   a fallback-behind-a-park case and a re-pend case to the concurrency test.
4. Review. Add the workflow task index and the timeout batch fence. Harden
   the walks and add a negative control to the plan test.
5. Measure. Record the fixture figures on the performance page.
6. REFACTOR. Run the gates.

### Acceptance criteria

8. Each by-execution statement in scope reads its key index at 5 to 200
   `RUNNING` rows per worker and after an idle analyze, under the generic
   and the custom plan.
9. Each raw by-execution statement keeps the same rows in both forms, and
   the wake fallback and re-pend keep them beside a concurrent park.
10. A source walk over every engine file fails on a plain by-id or
    by-execution guard.
11. The performance page records the before and after cost of the
    by-execution statements.
12. A wake, the owner key read and the migration read cost the same for a
    run with 50,000 rows as for a run with 4.
13. A timeout batch re-read reads its ids through the primary key.
14. An `e2e_bench` run records the before and after cost of the change.
