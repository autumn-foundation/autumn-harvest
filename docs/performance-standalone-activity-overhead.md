# One-step workflow overhead against a bare activity

Harvest has no standalone-activity start path. To run one durable job, a
user wraps one activity in a one-step workflow. Issue #1987 asks what that
wrapper costs. This page measures it. [ADR 0006](adr/0006-standalone-activity.md)
records the decision that follows from it.

> **This is a reference measurement, not an SLO.** It comes from one
> machine with one Postgres configuration. Reproduce it on your own
> hardware before you design against it.

## TL;DR

* A one-step workflow with a regular activity (arm A) writes **5.67x** the
  rows and **5.78x** the WAL of the bare floor (arm C).
* A one-step workflow with a local activity (arm B) is the cheapest
  existing pattern. It still writes **3.33x** the rows and **3.39x** the
  WAL of the floor.
* `DESIGN-1987.md` fixed a 2.0x line before the first measurement. Arm B is
  above it on both figures. Verdict: **Build a standalone-activity API.**

## Arms

| Arm | Shape | Events | Task rows | Claims |
|---|---|--:|--:|--:|
| A | One-step workflow, regular activity | 5 | 2 | 3 |
| B | One-step workflow, local activity | 4 | 1 | 1 |
| C | Bare floor: one task row, no workflow | 0 | 1 | 1 |

The counts are per job. The harness asserts them exactly for every job.

* **Arm A** writes `WorkflowStarted`, `ActivityScheduled`,
  `ActivityStarted`, `ActivityCompleted` and `WorkflowCompleted`. The
  workflow task row is claimed twice: once to schedule the activity and
  once to complete the run. The activity row is claimed once.
* **Arm B** writes `WorkflowStarted`, `LocalActivityScheduled`,
  `LocalActivityCompleted` and `WorkflowCompleted`. The activity runs
  inside the one workflow task, so there is no second row and no second
  claim.
* **Arm C** calls `queue::enqueue` with no execution, then
  `queue::claim_task` and `queue::complete_claimed_task`. No worker path
  runs such a row: `process_activity_task` fails a task with no
  execution. Arm C is the least that a durable, retried job can cost on
  the Harvest queue. It is a floor, not a design.

All arms run the same handler body on the same input.

## Cost per job

| Arm | Rows written | WAL bytes | Statement calls | Calls net of idle |
|---|--:|--:|--:|--:|
| A | 17.00 | 13453 | 264.34 | 197.75 |
| B | 10.00 | 7879 | 145.56 | 105.39 |
| C | 3.00 | 2327 | 10.00 | 10.00 |

| Ratio | Rows written | WAL bytes |
|---|--:|--:|
| A / C | 5.67x | 5.78x |
| B / C | 3.33x | 3.39x |

Where the rows go, per job:

| Arm | `harvest_events` | `harvest_task_queue` | `harvest_workflow_executions` |
|---|---|---|---|
| A | 5 inserts | 2 inserts, 8 updates | 1 insert, 1 update |
| B | 4 inserts | 1 insert, 3 updates | 1 insert, 1 update |
| C | none | 1 insert, 2 updates | none |

* **Rows written** and **WAL bytes** are the deciders. Both are exact or
  near exact. Three repetitions gave the same rows. WAL spread was under
  0.6% (A 13382-13453, B 7873-7890, C 2327).
* **Statement calls** are context only. A live worker polls, so calls
  depend on how long the run takes. "Calls net of idle" removes the idle
  worker's call rate over the same window. Across three repetitions it
  was 191.59-203.91 for arm A and 105.39-114.03 for arm B. Arm C runs no
  worker, so its two columns are equal.
* **Latency is not measured.** Each arm enqueues all its jobs and then
  drains them, so a start-to-completion time measures the queue, not the
  job. The house standard also does not admit wall-clock on a shared-vCPU
  host.

## Method

* Each arm gets a fresh, migrated database. Each arm runs
  `JOBS_PER_ARM` (50) jobs.
* Arms A and B start every job with `start_or_load_workflow_execution`,
  then a live `Worker` drains them. The worker uses the test defaults,
  with two changes. Scanner election is off, and the worker heartbeat is
  10 minutes. Both write rows on a timer. They are fleet upkeep, not job
  cost.
* **Rows written** is the change in `n_tup_ins + n_tup_upd + n_tup_del`
  over all tables, from `pg_stat_user_tables`.
* **WAL bytes** is the sum of WAL records that touch this database's
  `public` tables and indexes, from `pg_walinspect`. Full-page images are
  left out, because they depend on checkpoint timing. The cluster WAL
  position alone is not usable. It also holds autovacuum, catalog upkeep
  and the other databases.
* **Statement calls** come from `pg_stat_statements`. Harness queries carry
  a `/* harness */` tag and are left out.
* **The idle control** runs a worker with no work for the longest arm
  window. It wrote 0 rows and 0 WAL bytes in every repetition, which shows
  that the deciders hold no worker noise.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features testing --test integration -- \
  --ignored --exact \
  standalone_activity_overhead_perf::zz_capture_standalone_activity_overhead_evidence \
  --nocapture
```

`HARVEST_TEST_DATABASE_URL` is an admin URL for a superuser role. The
server must preload `pg_stat_statements`. With the variable unset, the
harness starts a `postgres:16` container with that setting.

| | |
|:--|:--|
| Machine | Linux, 4 logical CPUs |
| Postgres | 16.15 (Ubuntu), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest/tests/integration/standalone_activity_overhead_perf.rs` |
| Raw output | [`perf-artifacts/standalone-activity-overhead/`](perf-artifacts/standalone-activity-overhead/) (`capture.txt` is the run above; `repeat-1.txt` and `repeat-2.txt` are the other two) |

The structural tests `arm_a_structural_counts`, `arm_b_structural_counts`
and `arm_c_structural_counts` run in CI. They assert the event, task-row
and claim counts above. The docs guard `standalone_activity_docs` checks
that this page quotes those counts, that the ratio table matches the
cost table, that the cost table is the captured one, and that the ADR
states the verdict that the line gives.

## Limits

* **The floor flatters a build.** A real standalone-activity API must
  also store a job record that a client can read. It may also write a
  start event. Each such row moves a built API up from 3 rows. At 4
  rows per job, arm B is still 2.50x.
* **One host, one Postgres.** Rows and events do not depend on the host.
  WAL bytes depend on the Postgres version and the row layout.
* **Decision boundaries are off.** With `decision_boundaries` on, each
  decision also appends a `DecisionCommitted` event. Arms A and B then
  write more events. Arm C has no decision, so the gap gets wider.
