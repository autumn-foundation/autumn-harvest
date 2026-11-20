# Task row guards stay on their key index

Issue #2713 refines the result of issue #2689. The claim-release rechecks
find one `harvest_task_queue` row by `id`, and guard it on `state` and
`worker_id`. When the generic plan chooses
`harvest_task_queue_running_worker_idx`, each recheck reads every `RUNNING`
row of its worker. Its cost then grows with the slot count of the worker.

> **This is a reference measurement, not an SLO.** It comes from one
> machine with one Postgres configuration. The plan test in the repository
> reproduces the plan choice on any machine.

## 🎯 Workload

- A migrated database on Postgres 16.15, with `fsync = off`.
- 1,000,000 rows in `harvest_task_queue`. 5, 20, 50 or 200 rows for each
  of 100 workers are `RUNNING`. Of the rest, 2% are `PENDING`, and the
  others are terminal. Then `VACUUM ANALYZE`.
- The idle case analyzes 1,000,000 terminal rows. Then 40,000 live rows
  arrive with no new analyze. Autovacuum often analyzes a queue while no
  task runs, so this is the statistics state after an idle period.
- Each statement runs as a prepared statement under the claim settings and
  `plan_cache_mode = force_generic_plan`. Each figure is warm. Buffers are
  `shared hit + read` of the top plan node.
- A write pays more the first time that it touches a row in a run. So each
  write figure is a first write.

## 💡 Mechanism

A partial index matches each guard form:

| Guard | Partial index |
|---|---|
| `state = 'RUNNING' AND worker_id = $2` | `harvest_task_queue_running_worker_idx` |
| `state = 'RUNNING'` | `idx_harvest_tq_running_started`, `idx_harvest_tq_live_created` |
| `state IN ('PENDING', 'RUNNING')` | `idx_harvest_tq_live_created` |
| `state = 'PENDING'` | `idx_harvest_tq_poll`, `idx_harvest_tq_claim_seek` and others |
| `concurrency_key IS NOT NULL AND state = 'RUNNING'` | `harvest_task_queue_concurrency_key_running` |

Each path estimates one row. The partial index is small and shallow, so its
estimated cost is lower than that of the key index. The partial index
path reads one heap tuple for each index entry that matches the guard. Then
it filters on the key. When the statistics come from an idle table, each
live partial index looks empty, and every guard form leaves its key index.

## 🔧 Fix

`row_guard_sql!` and `row_guard` wrap the guard in
`CASE WHEN <guard> THEN true END`. The planner cannot use a `CASE` as an
index condition. It also cannot prove a partial index predicate from it.
So the only access path left is the key index. For a by-id statement,
that is the primary key.

`WHERE` drops a NULL row as it drops a false row, so each statement keeps
the same rows. `row_guard_plan_db_tests` proves it three ways:

- A truth table over every SQL truth value of the guard and of `id = $1`.
- A differential run of each raw statement in both forms, over every mix of
  state, worker, kind, pause, cap and strike count.
- A concurrent completion, reclaim, heartbeat and pause beside a release.
  Postgres checks every condition again on the new row version, the `CASE`
  included.

The fix covers each per-task statement that finds task rows by `id`:

- the pause rechecks: `queue_pause::release_claim_if_queue_paused`,
  `activity_pause::release_claim_if_activity_paused` and
  `execution::release_claim_if_workflow_paused`
- `queue_pause::release_claim` and `queue::release_claim_if_over_cap`
- the park statements of `queue::park_workflow_task_inner`
- `queue::fence`, so each fenced write: completion, heartbeat, handler
  start, fenced fail and requeue
- the claim locks, the capability-miss releases, reads and resets, the
  suspended-claim release, `queue::pending_hint_rows_query` and
  `poison_pill::requeue_orphan`
- the other by-id releases, requeues, defers and fails in `queue.rs` and
  `worker.rs`

Two unit tests walk the engine source. A by-id statement with a plain guard
fails them.

The by-id claim (`queue::claim_task_by_id_query`) is not in scope. It is a
variant of the claim statement, which has its own plan guards.
[`performance-claim-by-id-guard.md`](performance-claim-by-id-guard.md)
measures and fixes it.

The Ledger profiles of `queue::release_claim_via`,
`queue::park_workflow_task_inner`, `queue::complete_task_inner` and
`queue::write_heartbeat` point at this page. Each pause recheck runs
through `release_claim_via`. The by-execution section below names the
other functions whose profiles point here.

## 📈 Before and after, 1M-row fixture

Buffers per call. "Fence" is the fenced heartbeat write.

| RUNNING per worker | queue pause | activity pause | workflow pause | park | fence |
|--:|--:|--:|--:|--:|--:|
| 5 | 8 → 6 | 8 → 6 | 7 → 5 | 6 → 6 | 15 → 13 |
| 20 | 24 → 6 | 24 → 6 | 23 → 5 | 7 → 6 | 31 → 13 |
| 50 | 54 → 6 | 54 → 6 | 53 → 5 | 6 → 6 | 61 → 13 |
| 200 | 6 → 6 | 6 → 6 | 5 → 5 | 6 → 6 | 13 → 13 |
| idle statistics | 201 → 6 | 201 → 6 | 200 → 5 | 200 → 5 | 208 → 13 |

Before the fix, 5 to 50 rows per worker chose `running_worker_idx`, and the
cost grew with the slot count. At 200 rows per worker the tie flipped to the
primary key, so both forms cost the same. With idle statistics every
statement chose `idx_harvest_tq_live_created`. After the fix every
statement reads the primary key at every size, under the generic and the
custom plan.

## 📈 End to end

`e2e_bench`, `throughput` scenario, 1 shard, 1,200 completions, five
alternating pairs of runs. Each figure is the median of the five runs, from
`pg_stat_statements`. The bench was a test-profile build, so this page
publishes no throughput figure.

| Statement | Buffers before | Buffers after | Per call |
|---|--:|--:|--:|
| queue pause recheck | 66,542 | 50,622 | 6.6 → 5.0 |
| activity pause recheck | 23,818 | 21,420 | 5.5 → 5.0 |
| workflow pause recheck | 33,224 | 22,841 | 5.8 → 4.0 |
| the three rechecks | 125,680 | 94,886 | 6.2 → 4.7 |
| park statement | 95,565 | 90,980 | 22.1 → 21.1 |
| completion | 97,535 | 97,339 | 16.9 → 16.9 |
| all statements | 2,724,712 | 2,712,282 | 9.2 → 9.2 |

The rechecks read 24.5% fewer buffers. The bench table is small, so its
partial indexes hold few rows, and the gain is smaller than on the fixture.
Issue #2713 measured 21.8 buffers per recheck on another machine. There the
bench chose `idx_harvest_tq_running_started`. The total stays within run
noise.

## 🧭 By-execution statements

A wake, a drain and a cancel find task rows by `workflow_exec_id`, not by
`id`. Their guard has the same defect. Each one now hides its `state` and
`worker_id` tests with the same `CASE` form.

Each run has one workflow task row. The new index
`idx_harvest_tq_workflow_task (workflow_exec_id) WHERE task_type =
'workflow'` holds that row only. The statements that read only that row
keep `task_type = 'workflow'` as a plain test, so the planner can prove the
predicate. Each one then reads one row for any run size. The other
statements read `idx_harvest_tq_workflow`, which holds every row of a run.

The fixture has 1,000,000 rows and 4 rows for each run. "Hot run" is one
run with 50,000 terminal activity rows and 2 live rows. The idle case
analyzes terminal rows only. Then 40,000 live rows arrive with no new
analyze. Each figure is a generic plan, warm, in `shared hit + read`
buffers. "Before" is the plain form with no new index.

| Statement | 5 per worker | Hot run | Idle statistics |
|---|--:|--:|--:|
| `queue::primary_repend_workflow_task` | 21 → 4 | 1,343 → 4 | 1,411 → 4 |
| `queue::wake_workflow_task`, fallback | 509 → 9 | 509 → 9 | 1,416 → 9 |
| `queue::wake_workflow_task`, due-row read | 4 → 4 | 1,326 → 4 | 1,231 → 4 |
| `queue::owner_keys` | 12 → 4 | 51 → 4 | 1,232 → 4 |
| parked-row read of a migration | 24 → 8 | 1,346 → 8 | 1,414 → 8 |
| `queue::fail_open_tasks_for_execution` | 64 → 71 | 1,354 → 1,358 | 1,462 → 66 |
| drain of a failed or timed-out run | 69 → 72 | 1,359 → 1,359 | 1,466 → 66 |
| in-flight count of retention and reset | 5 → 4 | 1,327 → 1,327 | 1,411 → 5 |
| pending-row delete of a cancel | 4 → 4 | 1,326 → 1,326 | 18 → 4 |
| fork loser cancel | 2 → 2 | 2 → 2 | 1,412 → 2 |
| `execution::shift_schedule_to_close_for_resume` | 3 → 5 | 38 → 1,418 | 1,502 → 154 |
| `queue::session_has_other_live_tasks` | 2 → 4 | 2 → 1,326 | 1,411 → 4 |

At 5 rows per worker, the plain fallback scans all of
`running_worker_idx`, so it reads every `RUNNING` row in the table. With
idle statistics, each plain statement scans a partial index of live rows.
A write figure includes the rows that it changes.
`queue::cancel_open_tasks_for_execution` is the same statement as
`queue::fail_open_tasks_for_execution` with another `SET`.

Two statements cost more on a hot run. The session check now reads each
row of its run. Its plain form read every live row of the table after an
idle analyze. The deadline shift runs once for each resume. The fail,
cancel and drain writes and the in-flight counts read each row of the run
in both forms. They run once at the end of a run.

The pending-row delete of a cancel binds `task_type` as a Diesel
parameter. A generic plan cannot prove a partial index predicate from a
parameter, so it keeps `idx_harvest_tq_workflow`.

The workflow task index adds about 40 bytes of WAL to an update of a
workflow task row that is not HOT, about 3% more. An activity row adds
none. The index holds about 31 bytes for each run.

The in-scope statements:

- `queue`: the three wake statements, `owner_keys`, the fail and cancel of
  open tasks, and `session_has_other_live_tasks`
- `execution`: the deadline shift of a resume, and the pending-row delete
  of a cancel
- the drains of `poison_pill`, `worker` and `timeout`
- the fork loser cancel, and the in-flight counts of `reset` and
  `retention`
- `shard_rebalance`: the parked-row read, the seal cancel and the signal
  re-pend

The plan test checks each raw statement except the seal cancel, and a
sample of each Diesel shape. The seal cancel also runs the quiescence
check, a join that is out of scope, so only the source walk checks it. The
source walks read every engine file and each top-level term of a keyed
`WHERE` clause.

The Ledger profiles of `queue::primary_repend_workflow_task`,
`queue::owner_keys`, `queue::fail_open_tasks_for_execution`,
`queue::cancel_open_tasks_for_execution`,
`queue::session_has_other_live_tasks`,
`execution::shift_schedule_to_close_for_resume`,
`timeout::find_timed_out_tasks_batch` and `timeout::lapsed_moves` point at
this page.

## 📈 End to end, by-execution change

`e2e_bench`, `throughput` scenario, 1 shard, five alternating pairs of
runs. "Before" is the merge base with no new index. Each figure is the mean
of the five runs, from `pg_stat_statements`. The bench was a test-profile
build, so this page publishes no throughput figure.

| Statement | Buffers per call | WAL bytes |
|---|--:|--:|
| due-row read of a wake | 22.21 → 3.76 | 39,844 → 22,661 |
| `queue::primary_repend_workflow_task` | 27.02 → 26.78 | 4,080,804 → 4,460,541 |
| park statement | 20.97 → 22.86 | 2,108,600 → 2,443,335 |
| claim statement | 121.42 → 120.51 | 6,900,803 → 7,335,287 |
| all statements | 8.82 → 8.58 | 47,298,023 → 48,715,632 |

The due-row read reads 83% fewer buffers. All statements read 2.5% fewer
buffers, and each run varies by about 3%. WAL grows by 3.0%. Each update of
a workflow task row adds an entry to the new index: the re-pend, the park
and the claim. The bench queue is small, so it shows the write cost of the
index and little of the read gain on a large run.

## ⏱️ Timeout batch re-read

`timeout::find_timed_out_tasks_batch` and `timeout::lapsed_moves` test a
scan predicate on a batch of ids. The predicate tests `state` before its
`id = ANY($1)` key. After an idle analyze, the plain form scanned
`idx_harvest_tq_live_created`.

The batch now loads its ids through the primary key in a materialized CTE.
The predicate then tests those rows only. A batch of 100 ids, heartbeat
predicate, generic plan:

| Fixture | Plain | Fence |
|---|--:|--:|
| idle statistics | 1,414 | 395 |
| 500 `RUNNING` rows | 22 | 378 |
| 5,000 `RUNNING` rows | 384 | 378 |
| 20,000 `RUNNING` rows | 387 | 381 |

The fence costs the same on every fixture. It costs more only when very
few rows run.

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --test integration row_guard_plan_db_tests
```

The test seeds 100,000 rows, which is enough for the same plan choice. On
the parent commit it fails, and it names each statement that leaves its
key index.
