## Feature — SQLite: cancel and terminate a run, with cooperative cancellation (issue #2021)

`SqliteRuntime` had no way to stop a live run. The only path was a
`TerminateIfRunning` restart of the same workflow id. Embedded apps, such as
the agent daemon, could not stop a stuck run.

**What shipped**

- `SqliteRuntime::cancel(exec, reason)` is cooperative. It stores the
  request in `harvest_executions.cancel_reason` and appends no event. The
  next drive replays the body with `ctx.cancellation_reason()` set, so the
  body can run cleanup activities.
- A cancel always ends the run `CANCELLED`. The run ends when the body
  returns `Ok` or `Err`, or continues as new. No successor starts. It also ends when the body blocks on a signal or a
  timer with no activity task left, because a parked await cannot see the
  request. It also ends when the cycle diverges or deadlocks: the driver
  discards that cycle. It also ends once the run adds 1,000 events after the
  request, so a body that ignores the request and keeps working cannot run
  forever. `harvest_executions.cancel_from_seq` stores the history length at
  the request.
- A cleanup that reaches a history cap ends the run `CANCELLED`, not
  `FAILED`.
- A pending activity task, such as a retry in backoff, still runs. A repeat
  cancel keeps the first reason. The request survives a reopen.
- `SqliteRuntime::terminate(exec, reason)` ends the run `TERMINATED` in one
  transaction. The body does not run again.
- Both paths append the core `WorkflowCancelled { reason }` event as the last
  event, as Postgres does. They delete pending tasks, unfired timers and
  staged signals.
- An unknown id returns `ExecutionNotFound`. A terminal run returns
  `WorkflowNotRunning`. Postgres treats a repeat call on a terminal run as a
  no-op. A blank reason gets the Postgres default text.
- `RunState` gains `Cancelled(reason)` and `Terminated(reason)`. The drive
  that ends the run, and any later drive, returns them.
- `AllowDuplicateFailedOnly` replaces a `CANCELLED` prior, as on Postgres.
- The agent daemon retires a cancelled or terminated session.

**Design.** The core matcher does not skip `WorkflowCancelled`. A cleanup
command recorded after that event therefore diverges on replay. The request
is stored outside the event log until the run ends. The core gains one seam,
`WorkflowExecuteSpanMeta::cancellation_reason`. It sets
`ctx.cancellation_reason()` on each decision cycle while the request is
pending. A recorded `WorkflowCancelled` wins. Every other caller passes
`None`, so Postgres behavior does not change. See `DESIGN-2021.md`.

**Breaking changes.** `WorkflowExecuteSpanMeta` has a new public field.
`RunState` has two new variants, so an exhaustive `match` needs new arms.
The `WorkflowNotRunning` message now ends "cannot accept the request".

**Invariants.** No new `WorkflowEvent` variant. No Postgres migration. The
SQLite schema adds two nullable columns through the additive `migrate` step.
`harvest_events` stays append-only.

**Known limits.**

- The body sees the request from the start of its replay. A check before
  recorded commands, such as a check at the top of a loop, diverges. The run
  then ends without cleanup.
- A cleanup that waits on a timer or a signal ends the run at that wait.
- The output of a body that returns `Ok` after a request is not kept.
- A cancelled or terminated history replays on `WorkflowReplayer` only when
  the body checks `is_cancelled()` before its next new command. A Postgres
  history of the same shape has the same limit.

**Tests.** `autumn-harvest-sqlite/tests/integration/cancel_terminate.rs`
covers:

- cleanup with the reason, and a reopen between request and end;
- a body parked on a signal, on a timer, and behind a pending retry;
- a check before recorded steps and before a recorded timer;
- a deadlock and a panic under a pending request;
- a busy body that ignores the request and keeps running activities;
- a cleanup that reaches the history cap, and a final or suspending batch
  past the cap or the budget;
- a side effect of the returning cycle, and a second drive of a cancelled run;
- a signal consumed after a request, and a cancel that wins over
  `continue_as_new`;
- idempotence, blank and padded reasons, an `Err` return and the fleet driver;
- row cleanup, unknown and terminal targets, and each reuse policy over a
  cancelled prior;
- replay of a cancel history, a terminate history and a parked cancel.

Core unit tests pin the seam and the precedence of a recorded event. A schema
unit test pins the migration.
