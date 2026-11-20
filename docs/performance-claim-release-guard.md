# By-id task row guards stay on the primary key

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
estimated cost is lower than that of the primary key. The partial index
path reads one heap tuple for each index entry that matches the guard. Then
it filters on `id`. When the statistics come from an idle table, each live
partial index looks empty, and every guard form leaves the primary key.

## 🔧 Fix

`row_guard_sql!` and `row_guard` wrap the guard in
`CASE WHEN <guard> THEN true END`. The planner cannot use a `CASE` as an
index condition. It also cannot prove a partial index predicate from it.
So the only access path left is the primary key.

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
variant of the claim statement, which has its own plan guards. Issue #2751
measures it.

The Ledger profiles of `queue::release_claim_via`,
`queue::park_workflow_task_inner`, `queue::complete_task_inner` and
`queue::write_heartbeat` point at this page. Each pause recheck runs
through `release_claim_via`.

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

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --test integration row_guard_plan_db_tests
```

The test seeds 100,000 rows, which is enough for the same plan choice. On
the parent commit it fails, and it names each statement that leaves the
primary key.
