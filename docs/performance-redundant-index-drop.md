# Redundant index drop on the hot tables (issue #2684)

An audit read all 73 indexes of `harvest_task_queue`,
`harvest_workflow_executions` and `harvest_events`. Five could go. Each
drop survived two adversarial checks: one looked for a query, plan, order
or uniqueness use that needs the index, the other for the partition layout,
migrations, tests and docs.

> **This is a reference measurement, not an SLO.** It comes from one 4-core
> box with the database, the worker and the load generator on it.

## TL;DR

* **`idx_harvest_events_exec` duplicated a constraint.** It had the key of
  `harvest_events_workflow_exec_id_event_id_key`, so each event insert wrote
  two identical trees. Four indexes of `harvest_workflow_executions` were
  covered by another index or matched no query.
* **WAL per run falls 4.1%,** from 51.16 MB to 49.08 MB, in two runs of each
  build.
* **An event insert costs 7.1% less** in the census: 158.0 µs to 146.8 µs.
  Postgres execution time falls 1.5% in total.
* **Throughput does not move.** Six alternating pairs give a median ratio of
  1.002, faster in 3 of 6. The saving is per write, and its size grows with
  the tables, not with throughput.
* **`harvest_task_queue` keeps all 20 indexes.** The one proposed merge there
  was refuted.

## Bench usage before the drop

One run of the `throughput` cell
([`idx_stats.txt`](perf-artifacts/redundant-index-drop/idx_stats.txt),
[`tbl_stats.txt`](perf-artifacts/redundant-index-drop/tbl_stats.txt)):

| Table | Writes | HOT share | Indexes | Unused in the bench |
|:--|:--|--:|--:|--:|
| `harvest_task_queue` | 26,889 updates | 15% | 20 | 12 |
| `harvest_events` | 14,795 inserts | n/a | 14 | 10 |
| `harvest_workflow_executions` | 1,341 updates | 0% | 39 | 30 |

A zero in this table does not make an index droppable. Admin endpoints,
scanners and retention read through most of them. The audit dropped only an
index that another index covers, or that no query can use.

## The five

| Index | Why it goes |
|:--|:--|
| `idx_harvest_events_exec` | Same key, operator classes, collation and options as the `(workflow_exec_id, event_id)` constraint. The partitioned layout keeps the constraint with `cohort` third, so per-execution probes keep an index. |
| `idx_harvest_we_state` | Same predicate and rows as `idx_harvest_we_canary_order`. |
| `idx_harvest_we_shard` | Prefix of `idx_harvest_we_shard_started`. |
| `idx_harvest_we_quota_reconcile_candidates` | Its query was replaced in `20261001192155`. |
| `harvest_wf_exec_detached_running` | Its readers select `state IN ('RUNNING','PAUSED')`, which does not imply its `state = 'RUNNING'` predicate. |

## Kept after the check

* `idx_harvest_events_activity_type_ts`. The usage report needs it for a
  narrow window when activity events are a minority.
* `idx_harvest_tq_activity_id` and `idx_harvest_tq_workflow`. A merged
  unconditional index would make the cancel by activity id scan workflow
  rows too.

## Measurements

| | Base | Drop |
|:--|--:|--:|
| WAL per run | 51.16 MB | 49.08 MB |
| `INSERT INTO harvest_events` | 158.0 µs | 146.8 µs |
| `INSERT INTO harvest_workflow_executions` | 341.7 µs | 324.1 µs |
| Postgres execution time | 20,129 ms | 19,833 ms |
| throughput, median of 6 pairs | | ratio 1.002 |

Raw output: [`perf-artifacts/redundant-index-drop/`](perf-artifacts/redundant-index-drop/).
The pairs ran with
[`claim-window-split/paired.sh`](perf-artifacts/claim-window-split/paired.sh).
