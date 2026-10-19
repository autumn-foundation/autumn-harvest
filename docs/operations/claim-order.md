# Claim order under overload (issue #1824)

A worker claims tasks from a queue in a fixed order. Under overload, two
rules keep in-flight work moving:

1. Continuations of running workflows go before new starts.
2. A task whose run deadline has passed does not run.

Both rules are on by default. They need no configuration.

## Order

A claim sorts eligible `PENDING` rows by these keys:

1. Sticky affinity to the claiming worker.
2. The effective priority: `priority`, plus the optional ageing boost
   (`priority_aging_secs`).
3. The claim-order due time, oldest first.

A worker with [fairness keys](../fairness-keys.md) on adds one key between 2
and 3: the fairness lag of the row's key (issue #1976). Within one fairness
key the order above does not change.

The claim-order due time is `scheduled_at`, with one exception. A **new
start** sorts as if it were due 30 seconds later
(`queue::NEW_START_HANDICAP_SECS`).

A new start is the first workflow task of a freshly admitted run. The
workflow start path marks that row (`harvest_task_queue.new_start`). Every
start through that path is a new start, except a workflow retry. That
includes API, batch, schedule, trigger, debounce, throttle and typed-client
starts. The row stops being a new start at its first claim that is not
given back.

Every other task is a continuation:

- an activity task;
- a workflow task woken by an activity result, a signal, a timer or a child;
- the first task of a child, a continue-as-new, a reset fork, a workflow
  retry or a DLQ redrive.

The admission gate and [load shedding](load-shedding.md) use a similar
split, but they also exempt some internal producers. Their split is
therefore not identical.

### What this guarantees

- At equal effective priority, a continuation that is due goes before a new
  start that is less than 30 seconds old.
- A new start never waits more than 30 seconds behind continuations that
  arrived after it. After that, plain FIFO order applies.
- An explicit priority always wins. A `High` new start goes before a
  `Normal` continuation.
- Priority ageing reads `scheduled_at`, not the handicap. With
  `priority_aging_secs` below 30, ageing can lift an aged new start one
  level, above a fresh continuation.

### Limits

- The handicap is a constant. No setting changes it.
- The dispatch channel (issue #1312) orders its hints by `priority` and
  `scheduled_at`. It ignores the handicap.
- The SQLite and Redis-queue backends do not use this order.
- The order term costs about 18 ms per claim at a 20k-row backlog. See
  [`performance.md`](../performance.md#the-continuation-band-and-the-expired-run-gate-issue-1824).

## Run deadline

A run can have a deadline: `deadline_at` from `execution_timeout`, or
`chain_deadline_at` from `chain_execution_timeout`. The timeout scanner
times out an expired run once per `poll_interval`. Before issue #1824, a
task of that run could still be claimed and run in that gap.

Now every claim skips a task of a `RUNNING` run that is past either
deadline. The task stays `PENDING`. The claim spends no attempt, rate-limit
token or concurrency slot on it, and takes the next eligible task instead.

A claim can wait on a rate-limit bucket lock. It reads its clock after
that wait and checks the run again, so a deadline that passes during the
wait still stops the claim.

The check skips a `PAUSED` run. A resume moves its deadline forward.

The scanner then times out the run. It records `WorkflowExecutionTimedOut`
in the history and sets the run to `TIMED_OUT`. It fails each open task of
the run with an error that starts with `deadline_exceeded`:

```text
deadline_exceeded: timeout: WorkflowExecution for <workflow>
deadline_exceeded: timeout: WorkflowChain for <workflow>
```

### How to find these tasks

```sql
SELECT id, workflow_exec_id, task_type, completed_at
FROM harvest_task_queue
WHERE state = 'FAILED' AND error LIKE 'deadline_exceeded:%';
```

The retention janitor deletes finished task rows after 7 days by default.

## Related

- [`load-shedding.md`](load-shedding.md) — refuses new starts on an old
  backlog.
- [`admission-gate-producers.md`](admission-gate-producers.md) — the manual
  admission gate.
- [`upgrading/0.7.0.md`](../upgrading/0.7.0.md#15-continuations-are-claimed-before-new-starts--behavior-change)
  — the upgrade note.
