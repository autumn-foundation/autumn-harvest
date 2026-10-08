# ADR 0006: Standalone durable activity

## Status

Accepted (issue #1987).

## Context

- Temporal made Standalone Activities GA on 15 September 2026
  ([Temporal changelog](https://temporal.io/change-log)). A standalone
  activity is a retried, timed-out, durable job with no workflow around it.
- Harvest has no such start path. A user wraps one activity in a one-step
  workflow. That costs a workflow task and its history.
- The task queue allows a row with no execution. No worker path runs one:
  `process_activity_task` fails it. The Redis crate's `RedisTaskQueue`
  claims and completes tasks, but it runs no handler.
- Issue #1987 asked for a measurement first, then a decision: build a
  standalone-activity API, or document the one-step pattern.

The measurement is in
[One-step workflow overhead against a bare activity](../performance-standalone-activity-overhead.md).
Per job:

| Arm | Shape | Rows written | WAL bytes |
|---|---|--:|--:|
| A | One-step workflow, regular activity | 17.00 | 13434 |
| B | One-step workflow, local activity | 10.00 | 7884 |
| C | Bare floor: one task row | 3.00 | 2326 |
| D | Realistic floor: task row, job record, handler-start marker | 6.00 | 5073 |

`DESIGN-1987.md` fixed two lines, each before its floor ran. Both use
**2.0x**: a new start path is worth its surface only if it at least halves
the write cost of the cheapest existing pattern.

- **§0.4, arm B against arm C.** B is 3.33x on rows and 3.39x on WAL. The
  line gives **Build a standalone-activity API**. Review showed that this
  line could not fail. Code reading already put arm B at 6 inserts or
  more, against 3 rows for arm C. Arm C also leaves out writes that any
  real API needs.
- **§0.6, arm B against arm D.** B is 1.67x on rows and 1.55x on WAL. This
  line decides.

## Decision

**Document the one-step-workflow pattern.** Do not build a
standalone-activity start path now.

A standalone API would cut the local-activity pattern from 10 rows per job
to about 6. That is 1.67x, less than the 2.0x line asks for. The one-step
pattern also keeps every workflow feature with no new code: idempotent
start, result fetch, cancel, retention, search attributes, the UI and the
management API.

The [activities guide](../getting-started/activities.md#run-one-durable-job)
shows the pattern and its local-activity fast path.

## Consequences

- A user writes a small workflow for each job type. For a short
  in-process job, the local-activity form costs 10 rows and 1 claim per
  job. A job that needs heartbeats, another queue or a long timeout uses
  a regular activity and costs 17 rows and 3 claims.
- Arm B's reset of the capability-miss counters runs even when they are
  already 0. A guard on that update is a cheap engine fast path. It takes
  arm B from 10 rows to 9.
- Open questions, if a future measurement reopens a build:
  - **Job record.** The janitor deletes terminal activity task rows after
    7 days by default. A task row alone cannot hold a job record for
    longer.
  - **Idempotency.** A caller-chosen job id needs a unique index on a hot
    table. That adds index WAL to every task insert.
  - **Dead letters.** The DLQ queries join through
    `harvest_workflow_executions`. A dead letter with no execution drops
    out of them.
