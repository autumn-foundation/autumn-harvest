## Feature — SQLite: cancellable timers, workflow mutex, local activities, execution timeout and workflow retry (issue #2029)

Five primitives sealed a SQLite run `FAILED` with `UnsupportedFeature`, or
panicked at registration. Code that used any of them could not move from
Postgres. Each one now works on SQLite and records the Postgres events, so
its history replays on the core `WorkflowReplayer`.

**What shipped**

- **Cancellable timers (#768).** `ArmTimer { for_await: false }` appends
  `TimerStarted`. `ArmTimer { for_await: true }` inserts the `harvest_timers`
  row once, so the deadline counts from the first await. `CancelTimer`
  deletes the unfired row and appends `TimerCancelled`. A terminal batch
  records the events and arms no row. `store::pending_timer_arms` now
  subtracts `TimerCancelled`, so a cancelled id can arm a `ctx.timer`.
- **Workflow mutex (#691).** New tables `harvest_mutex_locks`,
  `harvest_mutex_waiters` and `harvest_mutex_seq`. A grant goes to the first
  run in the FIFO queue of a free key. It mints a new `lock_seq` and appends
  `MutexGranted`. A waiting run returns the new `RunState::WaitingMutex(key)`.
  `lock_seq` fences `ReleaseMutex`. Every terminal state setter releases
  the run's locks and queue places. A release that frees a lock is progress,
  so `run_until_idle` drives the waiter. An acquire must be the only
  awaitable in its batch, as on Postgres. This backend also rejects a timer
  await beside an acquire, because a timer that fires first breaks the replay
  of the grant. A run that stops waiting gives up its queue place.
- **Local activities.** `register_activity` accepts `local = true`. A local
  activity that declares `schedule_to_close` panics at registration, as
  Postgres rejects it. The cycle appends `LocalActivityScheduled`. The body
  runs after the commit, with the local `ActivityContext`. Each attempt
  appends `LocalActivityFailed` or `LocalActivityCompleted`. A terminal
  failure adds `LocalActivityExhausted`. The per-attempt budget is
  `start_to_close`, capped at 60 seconds. Under Tokio a body past it is
  dropped, as on Postgres. A closure body is checked after it returns.
  A retry backoff parks the run in `local_retry_at`, and the drive does not
  sleep. A batch with a durable awaitable ends the run `FAILED`, as on
  Postgres (issue #950). This backend also rejects a second local activity
  in one batch. A typed `retry_after` hint wins over the policy backoff.
- **Execution timeout (#243).** A start stores `execution_timeout_ms` and
  `deadline_at`. Each cycle checks the deadline first, and again after the body runs. A late run gets
  `WorkflowExecutionTimedOut`, ends `TIMED_OUT` with the Postgres error text,
  and loses its open rows and locks. The drive returns the new
  `RunState::TimedOut(error)`. `ctx.deadline()` reports the same deadline,
  because `WorkflowStarted` carries the run's start instant. A signal, an
  update, a cancel or a terminate after the deadline seals the run first and
  gets `WorkflowNotRunning`. A signal by workflow id does the same. A
  signal-with-start replaces the overdue run with a new one.
- **Workflow retry (#523).** A start freezes the policy and
  `workflow_attempt = 1`. An author failure that the policy allows starts a
  retry in the failure transaction. The retry gets the same input, the next
  attempt, its own exec id as its `workflow_id`, and every signal of the
  failed run, undelivered. The failed run gets `WorkflowRetryScheduled`. The
  retry waits for its backoff in `start_at`, and a cancel ends it at once.
  `SqliteRuntime::successor` now returns a retry too. The jitter seeds are the
  Postgres ones.
- **Schema.** `harvest_executions` gains `execution_timeout_ms`,
  `deadline_at`, `start_at`, `workflow_attempt`, `retry_policy_json`,
  `retry_of_exec_id` and `local_retry_at`. `schema::migrate` adds them to an old file.

**Core.** `ActivityContext::new_embedded_local` is a new hidden constructor
for an embedded local-activity context. `WorkflowReplayer::replay_from_events`
and `replay_from_snapshot` now replay a history that ends in
`WorkflowExecutionTimedOut` as an
in-flight run up to the seal. The engine seals such a run while the body is
parked, so no command follows the last recorded event. A divergence before
the seal still reports.

**Invariants.** No new `WorkflowEvent` variant. `harvest_events` stays
append-only. The SQLite schema change is additive.

**Tests.** `autumn-harvest-sqlite/tests/integration/` gains
`cancellable_timers.rs`, `workflow_mutex.rs`, `local_activities.rs`,
`execution_timeout.rs` and `workflow_retry.rs`. Each runs a workflow that
was rejected before, and replays its history on `WorkflowReplayer`. Unit
tests pin the mutex FIFO and fence, the column migration, and the replayer
seal rule.
