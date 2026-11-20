# One-row claims stay on a full index

Issue #2751 follows issue #2713. The by-id claim finds one
`harvest_task_queue` row by `id`. Its `candidate` and
`concurrency_pending_keys` scans also test `state = 'PENDING'`. After an
idle analyze, the plan read a live-row partial index instead of the primary
key. The cost then grew with the live backlog. The owner take, which claims
the workflow task of one execution, had the same defect.

> **This is a reference measurement, not an SLO.** It comes from one
> machine with one Postgres configuration. The plan test in the repository
> reproduces the plan choice on any machine.

## 🎯 Workload

- A migrated database on Postgres 16.15, with autovacuum off on the queue.
- 1,000,000 terminal rows, then `VACUUM ANALYZE`. These are the idle
  statistics.
- Then, with no new analyze: 1,000 `RUNNING` rows, 100,000 due `PENDING`
  activity rows, and 20,000 executions, each with one due `PENDING`
  workflow row. All rows are in one queue.
- The fresh case runs `VACUUM ANALYZE` again after the live rows arrive.
- Each statement is prepared and runs once under the claim settings. Then
  `EXPLAIN (ANALYZE, BUFFERS)` runs it again in a rolled-back transaction.
  Buffers are `shared hit + read` of the top plan node.
- The by-id claim names the last `PENDING` activity row. The owner take
  names the execution of the last workflow row. Owner mode is on for the
  take.

## 💡 Mechanism

The by-id claim is the full-scan claim with an id test spliced into two
scans. Both scans keep `queue_name = ANY($2) AND state = 'PENDING'`. Each
partial index on `PENDING` rows matches that test. With idle statistics
each such index looks empty, so its estimated cost is lower than that of
the primary key. The generic plan read `idx_harvest_tq_live_created`. The
custom plan read `idx_harvest_tq_claim_seek`. Each path reads the live
entries of the index and then filters on `id`.

The owner take tests `workflow_exec_id` and `task_type = 'workflow'`. So
`idx_harvest_tq_owner_order` also matched it.

## 🔧 Fix

`splice_one_row_predicates` adds the id test of a one-row claim. It also
wraps the state test of both scans with `row_guard_sql!`. The planner cannot
prove a partial index predicate from the `CASE`. So the by-id claim reads
the primary key, and the owner take reads `idx_harvest_tq_workflow_task`.
That index holds the one workflow task row of each run. See
`docs/performance-claim-release-guard.md`.

The window claim keeps the plain state test. It needs its partial indexes.

The Ledger profiles of `queue::claim_task_by_id_with_owner` and
`queue::claim_workflow_task_of_execution` point at this page. Each by-id
claim runs through `claim_task_by_id_with_owner`.

`row_guard_plan_db_tests` checks the plan of each variant in four plan
modes, at 5 to 200 `RUNNING` rows per worker and after an idle analyze. Its
differential test runs each variant in both forms over every seeded row,
and the effects match.

## 📈 Before and after, 1M-row fixture

Buffers per call, generic plan / custom plan.

| Statement | Idle, before | Idle, after | Fresh, before | Fresh, after |
|---|--:|--:|--:|--:|
| by-id claim | 622 / 3,077 | 26 / 28 | 26 / 28 | 26 / 28 |
| by-id claim, fenced | 622 / 3,077 | 26 / 28 | 26 / 28 | 26 / 28 |
| by-id claim, owned | 622 / 3,077 | 26 / 29 | 26 / 28 | 26 / 28 |
| by-id claim, owned and fenced | 622 / 3,077 | 26 / 28 | 26 / 28 | 26 / 28 |
| owner take | 3,610 / 561 | 33 / 35 | 33 / 35 | 33 / 35 |
| owner take, fenced | 3,610 / 566 | 33 / 35 | 33 / 35 | 33 / 35 |

With fresh statistics the plan already read the primary key, so the fix
changes nothing there. With idle statistics the fix removes the
backlog-sized scan.

## ⚠️ Residual cost

A row with a concurrency key also runs `concurrency_running_counts` and the
cap `recheck`. Both count `RUNNING` rows by key. After an idle analyze each
one read `idx_harvest_tq_live_created`, about 3,700 buffers on this
fixture. With 1,024 keys on the pending rows, the by-id claim read 8,629 /
13,782 buffers before the fix and 7,439 / 7,441 after. Every claim form has
these two scans. They read by key, so `row_guard_sql!` cannot help them.
They need their own fix.

The owner take read each row of its run through `idx_harvest_tq_workflow`.
A run with many activity rows cost one heap read for each row. The
workflow task index of migration `20261120145312` removes that cost. The
owner take now reads one row for any run size. The table above predates
that index.

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --test integration row_guard_plan_db_tests
```

The test seeds 100,000 rows, which is enough for the same plan choice. On
the parent commit it fails, and it names each variant that leaves its full
index.
