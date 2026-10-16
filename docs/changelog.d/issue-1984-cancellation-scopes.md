## Programming model — Cancellation scopes (issue #1984)

**What shipped.**

- `ctx.cancellation_scope()` returns a `CancellationScope`. `scope.run(body)`
  tracks each activity, durable timer and awaited child workflow that the
  body starts. `scope.cancel()` cancels all of them as a unit. The run future
  then returns `HarvestError::Cancelled` and drops the body.
- `ctx.non_cancellable(body)` runs `body` to completion when the workflow is
  cancelled while it runs. The engine defers the cancel until no block is
  open, then cancels the run in the same decision transaction.

**Determinism.** The cancel decision is a recorded `cancel_scope:{seq}`
marker. It holds the open members and the history length that the
cancelling cycle saw. On replay, the scope polls its body with the matcher
cut at that length. A completion that the live cycle did not see therefore
cannot change the outcome. The body's commands from the cancelling cycle
never reach the worker, so an operation cannot start and leak in that cycle.
A value or version gate that the body recorded in that cycle stays in the
marker, so replay returns the same value. A replayed decision that differs
from the captured one, or comes in a different order, reports
non-determinism. A withdrawn timer command restores the timer state.

**Deferred cancel.**

- A cancel that finds an open block on a `RUNNING` run records
  `WorkflowCancelRequested` once. The run keeps running.
- The first suspended cycle with no open block cancels the run. This also
  holds when the block closes in an inline local-activity loop.
- A cycle that would fail or continue as new is cancelled instead. A cycle
  that completes keeps its result.
- A paused run is not deferred, and a run with a pending deferred cancel
  cannot be paused. A reset drops a copied request. The
  same-shard parent-close cascade defers like the cross-shard path.
- Scheduler `CancelOther` counts a deferred cancel as no freed slot.

**API changes.**

- New `WorkflowEvent::WorkflowCancelRequested { reason }`. Replay skips it.
  Only a run that calls `ctx.non_cancellable` gets it.
- `WorkflowCommand::CancelRaceLosers` gets a `reason` field
  (`LoserCancelReason`, non-exhaustive). The scope reuses the race teardown.
- `CancelledWorkflowExecution` gets `deferred`. The REST cancel response and
  the OpenAPI document get `deferred` too.
- The two new fields break a downstream struct literal or an exhaustive
  pattern on these types.
- `TerminateIfRunning`, signal-with-start replace and latest-wins supersede
  use the new `cancel_workflow_execution_collect_now`. They are not deferred.
- No migration.

**Limits.** A non-cancellable block cannot nest inside a cancellable scope.
A scope cannot acquire a durable mutex, or signal, cancel or await an
external workflow. A scope cannot create a session, spawn a detached child or run an admitted update. Such an update fails at once, and its result survives the cancel. A cancel that finds no open block is
still terminal at once. A scope does not cancel local activities or external
activities. On SQLite, a cooperative cancel request is hidden from
`is_cancelled()` inside a block, so the block's cleanup runs. While the
history holds an open block, the request is hidden from the whole replay.
SQLite does not seal a run that parks inside an open block. It still ends such a run
`CANCELLED` when the body returns.

**Tests.**

- Unit tests cover each member kind, live and on replay.
- Unit tests cover the same-cycle withdraw, a concurrent member completion,
  nested scopes, a late external result and an unreadable marker.
- The in-process harness test runs a scope cancel and `replay_check`.
- The database suite `cancellation_scope_tests` covers the worker teardown,
  the deferred cancel, the inline and continue-as-new paths, a paused run
  and a replay of each recorded history.

Design record: `DESIGN-1984.md`.
