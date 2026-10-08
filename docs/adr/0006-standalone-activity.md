# ADR 0006: Standalone durable activity

## Status

Accepted (issue #1987).

## Context

- Temporal made Standalone Activities GA on 15 September 2026. A
  standalone activity is a retried, timed-out, durable job with no
  workflow around it.
- Harvest has no such start path. A user wraps one activity in a one-step
  workflow. That costs a workflow task and its history.
- The task queue allows a row with no execution. No worker path runs one:
  `process_activity_task` fails it. The Redis crate's `RedisTaskQueue`
  claims and completes tasks, but it runs no handler.
- Issue #1987 asked for a measurement first, then a decision: build a
  standalone-activity API, or document the one-step pattern.
- `DESIGN-1987.md` fixed the decision rule before the first measurement.
  Compare arm B, the cheapest one-step pattern, against arm C, the bare
  floor, on rows written and WAL bytes per job. At or below **2.0x** on
  both, document the pattern. Above 2.0x on either, build.

The measurement is in
[One-step workflow overhead against a bare activity](../performance-standalone-activity-overhead.md).
Per job:

| Arm | Shape | Rows written | WAL bytes | Against C |
|---|---|--:|--:|---|
| A | One-step workflow, regular activity | 17.00 | 13453 | 5.67x rows, 5.78x WAL |
| B | One-step workflow, local activity | 10.00 | 7879 | 3.33x rows, 3.39x WAL |
| C | Bare floor: one task row | 3.00 | 2327 | 1.00x |

## Decision

**Build a standalone-activity API**, as a follow-up issue under epic
#1969. Arm B is above the 2.0x line on both figures.

The follow-up starts from these constraints:

1. **Reuse the task row as the job record.** A task row already holds the
   input, output, state, attempt, retry policy and timeouts. Arm C shows
   what that costs. A separate record table adds rows to every job.
2. **Teach the worker to run a task row with no execution.** It must
   keep the claim fence, the retry policy, the timeouts, heartbeats and
   the dead-letter path that a workflow activity gets.
3. **Write no `harvest_events` rows.** There is no history to replay. The
   append-only invariant is not touched.
4. **Give the job a caller-chosen id.** A second start with the same id
   returns the first job. This is the job's idempotency.
5. **Expose start, result, cancel and list** through the client, the
   management API, the CLI and the UI.

Until the API ships, a user runs one durable job as a one-step workflow.
The [activities guide](../getting-started/activities.md#run-one-durable-job)
shows the pattern and its local-activity fast path.

## Consequences

- A standalone job will cost about 3 to 4 rows instead of 10 to 17. A
  job-only workload gets a higher write ceiling on the same database.
- A new start path is a new surface. Retention, visibility, quotas,
  authorization and the UI must each learn about jobs with no workflow.
  The follow-up must budget for that work.
- A standalone job has no history, so replay, reset and the time-travel
  debugger do not apply to it.
- The floor flatters a build. A real API may write a start event or a
  client-readable record. At 4 rows per job, arm B is still 2.50x the
  build, so the verdict holds.
- The one-step pattern stays valid. It keeps every workflow feature: a
  history, signals, search attributes and child workflows.
