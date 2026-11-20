# By-id task row guards stay on the primary key

Issue #2713 refines the negative result of issue #2689. The claim-release
rechecks find one `harvest_task_queue` row by `id`, and guard it on `state`
and `worker_id`. When the generic plan chooses
`harvest_task_queue_running_worker_idx`, each recheck reads every `RUNNING`
row of its worker. Its cost then grows with the slot count of the worker.

> **This is a reference measurement, not an SLO.** It comes from one
> machine with one Postgres configuration. The plan test in the repository
> reproduces the plan choice on any machine.

## 🎯 Workload

- A migrated database on Postgres 16.15, with `fsync = off`.
- 1,000,000 rows in `harvest_task_queue`. R rows are `RUNNING`, spread over
  100 workers. The rest are terminal, with 2% `PENDING`. Then
  `VACUUM ANALYZE`.
- The idle case analyzes 1,000,000 terminal rows. Then 40,000 live rows
  arrive with no new analyze. Autovacuum often analyzes a queue while no
  task runs, so this is the statistics state after an idle period.
- Each statement runs as a prepared statement under the claim settings and
  `plan_cache_mode = force_generic_plan`. The second of two runs is the
  warm figure. Buffers are `shared hit + read` of the top plan node.

## 💡 Mechanism

A partial index matches each guard form:

| Guard | Partial index |
|---|---|
| `state = 'RUNNING' AND worker_id = $2` | `harvest_task_queue_running_worker_idx` |
| `state = 'RUNNING'` | `idx_harvest_tq_running_started`, `idx_harvest_tq_live_created` |
| `state IN ('PENDING', 'RUNNING')` | `idx_harvest_tq_live_created` |
| `concurrency_key IS NOT NULL AND state = 'RUNNING'` | `harvest_task_queue_concurrency_key_running` |

Each path estimates one row. The planner compares index cost only. A small
partial index costs less than one more btree level of the primary key. The
partial index path reads one heap tuple for each row of the index, then
filters on `id`. When the statistics come from an idle table, each live
partial index looks empty, and every guard form leaves the primary key.

## 🔧 Fix

`row_guard_sql!` and `row_guard` wrap the guard in
`CASE WHEN <guard> THEN true END`. The planner cannot use a `CASE` as an
index condition. It also cannot prove a partial index predicate from it.
So the only access path left is `id = $1` on the primary key.

`WHERE` drops a NULL row as it drops a false row, so each statement keeps
the same rows. `row_guard_plan_db_tests` proves it three ways:

- A truth table over every SQL truth value of the guard and of `id = $1`.
- A differential run of each raw statement in both forms, over every mix of
  state, worker, kind, pause, cap and strike count.
- A concurrent completion, reclaim, heartbeat and pause beside a release.
  Postgres checks every condition again on the new row version, the `CASE`
  included.

The fix covers each per-task statement that finds one task row by `id`:

- `queue_pause::release_claim_if_queue_paused` and `release_claim`
- `activity_pause::release_claim_if_activity_paused`
- `execution::release_claim_if_workflow_paused`
- `queue::release_claim_if_over_cap`
- the park statements of `queue::park_workflow_task`
- `queue::fence`, so each fenced write: completion, heartbeat, handler
  start, fenced fail and requeue
- the claim locks, the capability-miss releases, the suspended-claim release
  and `poison_pill::requeue_orphan`

The Ledger profiles of `queue::release_claim_via`,
`queue::park_workflow_task_inner` and `queue::complete_task_inner` point at
this page. Each pause recheck runs through `release_claim_via`.

## 📈 Before and after, 1M-row fixture

Buffers per call. "Fence" is the fenced heartbeat write.

| RUNNING per worker | queue pause | activity pause | workflow pause | park | fence |
|--:|--:|--:|--:|--:|--:|
| 5 | 8 → 6 | 8 → 6 | 7 → 5 | 6 → 6 | 12 → 7 |
| 20 | 24 → 8 | 24 → 6 | 23 → 5 | 7 → 6 | 30 → 7 |
| 50 | 54 → 6 | 54 → 6 | 53 → 5 | 6 → 6 | 58 → 7 |
| 200 | 6 → 6 | 6 → 6 | 5 → 5 | 6 → 6 | 10 → 7 |
| idle statistics | 201 → 6 | 201 → 6 | 200 → 5 | 200 → 5 | 205 → 7 |

Before the fix, 5 to 50 rows per worker chose `running_worker_idx`, and the
cost grew with the slot count. At 200 rows per worker the tie flipped to the
primary key. With idle statistics every statement chose
`idx_harvest_tq_live_created`. After the fix every statement reads the
primary key at every size, under the generic and the custom plan.

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --test integration row_guard_plan_db_tests
```

The test seeds 100,000 rows, which is enough for the same plan choice. On
the parent commit it fails, and it names each statement that leaves the
primary key.
