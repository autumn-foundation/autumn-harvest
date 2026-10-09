# One-step workflow overhead against a bare activity

Harvest has no standalone-activity start path. To run one durable job, a
user wraps one activity in a one-step workflow. Issue #1987 asks what that
wrapper costs. This page measures it.
[ADR 0007](adr/0007-standalone-activity.md) records the decision that
follows from it.

> **This is a reference measurement, not an SLO.** It comes from one
> machine with one Postgres configuration. Reproduce it on your own
> hardware before you design against it.

## TL;DR

* A one-step workflow with a regular activity (arm A) writes **5.67x** the
  rows and **5.77x** the WAL of a bare task row (arm C).
* A one-step workflow with a local activity (arm B) is the cheapest
  existing pattern. It writes **3.33x** the rows and **3.39x** the WAL of
  arm C.
* Arm C is a floor that no real API reaches. Arm D adds what a real
  standalone-activity API must also write: a job record and the
  handler-start marker. Arm B writes **1.67x** the rows and **1.56x** the
  WAL of arm D.
* `DESIGN-1987.md` §0.6 fixed a 2.0x line against arm D before arm D ran.
  Verdict: **Document the one-step-workflow pattern.**
* The verdict covers jobs that fit a local activity. A job that needs a
  regular activity uses arm A, which is **2.83x** the rows and **2.65x**
  the WAL of arm D. That gap is above the line. §0.6 did not name arm A,
  so the gap is recorded, not decided. See [Scope](#scope-of-the-verdict).
* The first line, §0.4, compared against arm C. It could not fail, and it
  gave **Build a standalone-activity API**. That verdict stays on record.

## Arms

| Arm | Shape | Events | Task rows | Claims |
|---|---|--:|--:|--:|
| A | One-step workflow, regular activity | 5 | 2 | 3 |
| B | One-step workflow, local activity | 4 | 1 | 1 |
| C | Bare floor: one task row, no workflow | 0 | 1 | 1 |
| D | Realistic floor: task row, job record, handler-start marker | 0 | 1 | 1 |

The counts are per job. The harness reads them for every job and asserts
that each job equals these constants.

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
  runs such a row: `process_activity_task` fails a task with no execution.
* **Arm D** is arm C plus a job record and the handler-start marker. The
  record is a `harvest_workflow_executions` row. It is inserted with the
  task row in one transaction and set to `COMPLETED` with the output when
  the task completes. The marker is the fenced update that timeout retries
  need ([ADR 0005](adr/0005-activity-timeout-retry-and-open-circuit.md)).
  Arm D writes no events.

All arms run the same handler body on the same input.

## Cost per job

| Arm | Rows written | WAL bytes | Statement calls | Calls net of idle |
|---|--:|--:|--:|--:|
| A | 17.00 | 13429 | 145.94 | 114.80 |
| B | 10.00 | 7890 | 83.86 | 63.75 |
| C | 3.00 | 2326 | 10.00 | 10.00 |
| D | 6.00 | 5073 | 18.00 | 18.00 |

| Ratio | Rows written | WAL bytes |
|---|--:|--:|
| A / C | 5.67x | 5.77x |
| B / C | 3.33x | 3.39x |
| B / D | 1.67x | 1.56x |
| A / D | 2.83x | 2.65x |

Where the rows go, per job:

| Arm | `harvest_events` | `harvest_task_queue` | `harvest_workflow_executions` |
|---|---|---|---|
| A | 5 inserts | 2 inserts, 8 updates | 1 insert, 1 update |
| B | 4 inserts | 1 insert, 3 updates | 1 insert, 1 update |
| C | none | 1 insert, 2 updates | none |
| D | none | 1 insert, 3 updates | 1 insert, 1 update |

* **Rows written** and **WAL bytes** are the deciders. Rows are exact.
  Three repetitions gave the same rows. WAL varied by less than 1%
  (A 13429-13562, B 7890-7918, C 2326, D 5072-5073).
* Arm B's 3 task-row updates are the claim, the completion and a reset of
  the capability-miss counters (`reset_capability_misses_after_inline_progress`).
  The reset runs even when the counters are already 0. A guard on it would
  take arm B to 9 rows, which is 1.50x arm D.
* **Statement calls** are context only. A live worker polls, so its calls
  depend on how long the run takes. "Calls net of idle" removes the idle
  worker's call rate over the same window. Across three repetitions it
  was 101.30-114.80 for arm A and 59.88-67.18 for arm B. Arms C and D run
  no worker, so their two columns are equal. Their calls do not include
  the worker's claim and dispatch path.
* **The harness does not measure latency.** Each arm enqueues all its
  jobs and then drains them, so a start-to-completion time measures the
  queue, not the job. `activity_enqueue_batch_perf.rs` also does not admit
  wall-clock on a shared-vCPU host.

## Method

* Each arm gets a fresh, migrated database. Each arm runs
  `JOBS_PER_ARM` (50) jobs. The harness drops the database after the arm.
* Arms A and B start every job with `start_or_load_workflow_execution`,
  then a live `Worker` drains them. The worker uses the test defaults,
  with two changes. Scanner election is off, and the worker heartbeat is
  10 minutes. Both write rows on a timer. They are fleet upkeep, not job
  cost.
* After the last job completes, the harness stops the worker and waits
  until every worker connection closes. A backend publishes its table
  counters when it exits, so the counters are complete.
* A stopping worker updates its own `harvest_workers` row twice. The idle
  control pays exactly those writes: 2 rows and 762 WAL bytes. The
  capture subtracts them from arms A and B.
* **Rows written** is the change in `n_tup_ins + n_tup_upd + n_tup_del`
  over all tables, from `pg_stat_user_tables`.
* **WAL bytes** is the sum of WAL records that touch this database's
  `public` tables and indexes, from `pg_walinspect`. The harness leaves
  out full-page images, because they depend on checkpoint timing. Records
  with no block reference, such as a commit, also drop out. The cluster
  WAL position alone is not usable: it also holds autovacuum, catalog
  upkeep and other databases.
* **Statement calls** come from `pg_stat_statements`. Harness queries carry
  a `/* harness */` tag, and the capture drops them.
* **Arms C and D** run through the `queue` API with no worker. Arm D makes
  the handler-start and record-completion writes in raw SQL. They write the
  same rows as the worker, with fewer columns set.

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
harness starts a `postgres:16` container with `pg_stat_statements`
preloaded and `fsync=off`.

| | |
|:--|:--|
| Machine | Linux, 4 logical CPUs |
| Postgres | 16.15 (Ubuntu), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest/tests/integration/standalone_activity_overhead_perf.rs` |
| Raw output | [`perf-artifacts/standalone-activity-overhead/`](perf-artifacts/standalone-activity-overhead/): `capture.txt` is the run above; `repeat-1.txt` and `repeat-2.txt` are the other two |

CI runs the structural tests, one per arm. They need only a plain
database. `standalone_activity_docs` ties this page to those tests, to the
capture and to the ADR.

## Scope of the verdict

Arm B is the decider, because §0.6 named the cheapest existing pattern. A
local activity cannot heartbeat, cannot use another queue and has a
start-to-close cap. A job that needs any of these uses arm A.

Against arm D, arm A is 2.83x on rows and 2.65x on WAL. The fixed cost of
arm A is 11 more rows than arm D per job. For a job that runs for minutes,
that cost is small next to the job itself. It matters for short jobs at a
high rate that must run on another queue. No measurement here covers that
class. The ADR lists it as an open question.

## Limits

* **Arm D is a model, not a design.** A real API may write more than arm D:
  a start event, an idempotency-key row or a dead-letter row. Each extra
  row lowers the B / D ratio further. A real API could also write less, if
  the task row alone held the job record. The janitor deletes terminal
  activity rows after 7 days by default, so a task row alone cannot hold a
  record for longer.
* **One host, one Postgres.** Rows and events do not depend on the host.
  WAL bytes depend on the Postgres version and the row layout.
* **Decision boundaries are off.** With
  `HarvestBuilder::record_decision_boundaries(true)`, each decision also
  appends a `DecisionCommitted` event. Arms A and B then write more
  events. Arms C and D have no decision.
