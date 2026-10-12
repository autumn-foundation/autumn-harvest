## Feature — SQLite: cancel and terminate a run, with cooperative cancellation (issue #2021)

`SqliteRuntime` had no way to stop a live run. The only path was a
`TerminateIfRunning` restart of the same workflow id. Embedded apps, such as
the agent daemon, could not stop a stuck run.

**What shipped**

- `SqliteRuntime::cancel(exec, reason)` is cooperative. It stores the
  request in `harvest_executions.cancel_reason` and appends no event. The
  next drive replays the body with `ctx.cancellation_reason()` set, so the
  body can run cleanup activities. The run seals `CANCELLED` when the body
  returns `Ok` or `Err`. It also seals when the body blocks on a signal or a
  timer with no activity task left, because a parked await cannot see the
  request. A pending activity task, such as a retry in backoff, still runs.
  A repeat cancel keeps the first reason.
- `SqliteRuntime::terminate(exec, reason)` seals the run `TERMINATED` in one
  transaction. The body does not run again.
- Both seals append the core `WorkflowCancelled { reason }` event as the last
  event, as Postgres does. They delete pending tasks, unfired timers and
  staged signals.
- An unknown id returns `ExecutionNotFound`. A terminal run returns
  `WorkflowNotRunning`. A blank reason gets the Postgres default text.
- `RunState` gains `Cancelled(reason)` and `Terminated(reason)`. A drive of
  a sealed run returns them.
- `AllowDuplicateFailedOnly` replaces a `CANCELLED` prior, as on Postgres.
- The agent daemon retires a cancelled or terminated session.

**Design.** The core matcher does not skip `WorkflowCancelled`. A cleanup
command recorded after that event therefore diverges on replay. The request
is stored outside the event log until the seal. The core gains one additive
seam, `WorkflowExecuteSpanMeta::cancellation_reason`, which sets
`ctx.cancellation_reason()` for one decision. A recorded `WorkflowCancelled`
wins. Postgres passes `None`, so its behavior does not change. See
`DESIGN-2021.md`.

**Breaking changes.** `WorkflowExecuteSpanMeta` has a new public field.
`RunState` has two new variants, so an exhaustive `match` needs new arms.
The `WorkflowNotRunning` message now ends "cannot accept the request".

**Invariants.** No new `WorkflowEvent` variant. No Postgres migration. The
SQLite schema adds one nullable column through the additive `migrate` step.
`harvest_events` stays append-only.

**Known limit.** A terminated history replays on `WorkflowReplayer` only when
the body checks `is_cancelled()` before its next new command. A Postgres
history of the same shape has the same limit.

**Tests.** `autumn-harvest-sqlite/tests/integration/cancel_terminate.rs` has
20 tests: cleanup and reason, a reopen between request and seal, a parked
signal, a parked timer, a pending retry, idempotence, blank reasons, an `Err`
return, the fleet driver, row cleanup, unknown and terminal targets, both
replays and the reuse matrix. Core unit tests pin the seam and the precedence
of a recorded event. A schema unit test pins the migration.
