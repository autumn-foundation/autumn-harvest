## Programming model — Cancellation scopes (issue #1984)

**What shipped.**

- `ctx.cancellation_scope()` returns a `CancellationScope`. `scope.run(body)`
  tracks each activity, durable timer and awaited child workflow that the
  body starts. `scope.cancel()` cancels all of them as a unit. The run future
  then returns `HarvestError::Cancelled` and drops the body.
- `ctx.non_cancellable(body)` runs `body` to completion when the workflow is
  cancelled while it runs. The engine defers the cancel until the last open
  block closes, then cancels the run in the same decision transaction.

**Determinism.** The cancel decision is a recorded `cancel_scope:{seq}`
marker. It holds the members and the history length that the cancelling
cycle saw. On replay, the scope polls its body with the matcher cut at that
length, so a completion that the live cycle did not see cannot change the
outcome. The body's commands from the cancelling cycle never reach the
worker, so an operation cannot start and leak in the cycle that cancels it.

**Engine notes.**

- One new `WorkflowEvent` variant: `WorkflowCancelRequested { reason }`.
  Replay skips it. Only a run that uses `ctx.non_cancellable` gets it, so
  N-1 workers see it only for code that needs N.
- `WorkflowCommand::CancelRaceLosers` gets a `reason` field
  (`LoserCancelReason`). The scope reuses the race teardown. Synthetic
  terminals name the reason.
- `CancelledWorkflowExecution` gets `deferred`. A deferred cancel returns the
  live state, usually `RUNNING`.
- `TerminateIfRunning`, signal-with-start replace and latest-wins supersede
  use the new `cancel_workflow_execution_collect_now`. They are not deferred.
- No migration.

**Limits.** A non-cancellable block cannot nest inside a cancellable scope
(`HarvestError::Config`). A cancel that arrives when no block is open is
still terminal at once. Local activities, external activities and detached
children in a scope are not cancelled.

**Tests.** Unit tests per member kind, a same-cycle withdraw test, replay
tests with and without a concurrent member completion, shield marker tests,
an in-process harness test, and a database suite
(`cancellation_scope_tests`) for the worker teardown, the deferred cancel and
a clean replay of the recorded history. Design record: `DESIGN-1984.md`.
