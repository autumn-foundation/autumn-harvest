# Claim order under overload (issue #1824)

A worker claims tasks from a queue in a fixed order. Under overload, two
rules keep in-flight work moving:

1. Continuations of running workflows go before new starts.
2. A task whose run deadline has passed does not run.

Both rules are on by default. They need no configuration.

## Order

A claim sorts eligible `PENDING` rows by these keys:

1. Sticky affinity to the claiming worker.
2. `priority`, plus the optional ageing boost (`priority_aging_secs`).
3. The claim-order due time, oldest first.

The claim-order due time is `scheduled_at`, with one exception. A **new
start** sorts as if it were due 30 seconds later
(`queue::NEW_START_HANDICAP_SECS`).

A new start is the first workflow task of a freshly admitted run. The
workflow start path marks that row (`harvest_task_queue.new_start`). The
row stops being a new start at its first claim.

Every other task is a continuation:

- an activity task;
- a workflow task woken by an activity result, a signal, a timer or a child;
- the first task of a child, a continue-as-new, a reset fork, a workflow
  retry or a DLQ redrive.

This is the same split that the admission gate and
[load shedding](load-shedding.md) use. They act on fresh admissions only.

### What this guarantees

- At equal priority, a continuation that is due goes before a new start
  that is less than 30 seconds old.
- A new start never waits more than 30 seconds behind continuations that
  arrived after it. After that, plain FIFO order applies.
- An explicit priority always wins. A `High` new start goes before a
  `Normal` continuation.

### Limits

- The handicap is a constant. No setting changes it.
- The dispatch channel (issue #1312) orders its hints by priority and due
  time only. A worker that claims through dispatch references follows that
  order.
- The SQLite and Redis-queue backends do not use this order.

## Run deadline

A run can have a deadline: `deadline_at` from `execution_timeout`, or
`chain_deadline_at` from `chain_execution_timeout`. The timeout scanner
times out an expired run once per `poll_interval`. Before issue #1824, a
task of that run could still be claimed and run in that gap.

Now every claim checks the run of the claimed task. When the run is
`RUNNING` and either deadline has passed, the claim does not return the
task. It sets the task to `FAILED` with this error:

```text
deadline_exceeded: the run deadline passed before the task was claimed
```

The claim then tries the next task, up to 8 times per call. The scanner
still times out the run on its next tick. It changes only `PENDING` and
`RUNNING` rows, so the task keeps its `deadline_exceeded` error.

A `PAUSED` run is not checked. A resume moves its deadline forward.

### How to find these tasks

```sql
SELECT id, workflow_exec_id, task_type, completed_at
FROM harvest_task_queue
WHERE state = 'FAILED' AND error LIKE 'deadline_exceeded:%';
```

The worker also logs one `info` line per such task, with the task id, type
and queue.

## Related

- [`load-shedding.md`](load-shedding.md) — refuses new starts on an old
  backlog.
- [`admission-gate-producers.md`](admission-gate-producers.md) — the manual
  admission gate.
- [`upgrading/0.7.0.md`](../upgrading/0.7.0.md#15-continuations-are-claimed-before-new-starts--behavior-change)
  — the upgrade note.
