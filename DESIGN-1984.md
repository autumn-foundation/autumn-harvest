# Design — Issue #1984: cancellation scopes

A workflow cannot cancel a group of in-flight operations as a unit. It also
cannot shield a cleanup block from a workflow cancel. This change adds both.

- `ctx.cancellation_scope()` returns a `CancellationScope`. `scope.run(fut)`
  tracks the activities, timers and child workflows that `fut` starts.
  `scope.cancel()` cancels all of them together.
- `ctx.non_cancellable(fut)` runs `fut` to completion when the workflow is
  cancelled while `fut` runs.

**One new `WorkflowEvent` variant: `WorkflowCancelRequested`. No migration.
No route change.**

---

## 0. Planning record

### 0.1 Facts that constrain the design

| # | Fact | Source |
|---|------|--------|
| F1 | `CancelRaceLosers { activities, children, timers }` already cancels a set of operations in the decision transaction. | `context.rs`, `worker.rs::apply_race_loser_cancellations` |
| F2 | A workflow cancel is terminal. It writes `WorkflowCancelled`, sets `CANCELLED` and fails every open task in one transaction. | `execution.rs::cancel_workflow_execution_collect` |
| F3 | A command after `WorkflowCancelled` diverges, because that event is never consumed. | `saga_tests.rs::known_limitation_durable_compensation_after_recorded_cancel_fails_and_is_counted` |
| F4 | Each cold cycle replays the handler from the start. Resident mode resumes a parked future, but only for one awaited command and no `CancelRaceLosers`. | `executor.rs`, `resident.rs` |
| F5 | `apply_race_loser_cancellations` runs before the new activity, timer and child rows are inserted. A start and a cancel of the same operation in one batch would leak the operation. | `worker.rs::persist_mixed_suspension_batch` |
| F6 | Events from other writers (a completion, a signal) can land between the history load and the commit of a cycle. | `notify_awaited_parent_of_child_terminal`, signal delivery |

### 0.2 Brainstorm — how can a scope cancel replay deterministically?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Each member future races the scope cancel signal. A cancelled member returns `Cancelled`. | Rejected. On replay a member resolves from its synthetic terminal before the cancel is reached. Code after it then runs, which did not run live. |
| B2 | `scope.run` drops the body when the scope is cancelled and returns `Cancelled`. | **Adopted.** Nothing in the body runs after the cancel, live or on replay. |
| B3 | Record only the cancel marker. On replay, let members resolve from history. | Rejected. Same defect as B1. |
| B4 | Record the history length that the live cycle saw (`horizon`) in the marker. On replay, the matcher sees only that prefix while it polls the cancelled body. | **Adopted** with B2. The body sees exactly the history that it saw live (F6). |
| B5 | A new `CancelScope` command. | Rejected. `CancelRaceLosers` (F1) does the same work. It gets a `reason` field. |

### 0.3 Brainstorm — how can a cleanup block outlive a workflow cancel?

| # | Idea | Verdict |
|---|------|---------|
| C1 | Make every workflow cancel a request that the workflow handles. | Rejected for this slice. It changes F2 for every workflow, every caller and every test. |
| C2 | Defer a cancel while a non-cancellable block is open. Complete it when the last block closes. | **Adopted.** Only a workflow that uses the new API sees new behavior. |
| C3 | Store the pending cancel in a new column. | Rejected. A migration for a rare state. History holds it instead. |
| C4 | Reuse `MarkerRecorded` for the pending cancel. | Rejected. A marker is positional. An event that the cancel path appends at any position must be transparent to replay. A new variant states that clearly. |

### 0.4 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Start and cancel an operation in one cycle. The worker inserts it after the cancel (F5) and it runs. | At a live cancel, the scope removes the commands that its body issued in this cycle. They never reach the worker. Replay sees the same, because of the horizon (B4). |
| R2 | A member completes during the cancel cycle (F6). Replay resolves it and runs more body code. | The horizon hides every event that the live cycle did not see. |
| R3 | A member's synthetic terminal stays unconsumed and blocks the next command. | On replay the cancel marker names its members. The scope consumes their start, heartbeat and terminal events. |
| R4 | A sibling's event sits between the cursor and the cancel marker. | The marker is found with the tolerant scan that `ctx.race` uses for its winner marker. |
| R5 | A non-cancellable block is dropped before it closes. The cancel then waits for ever. | `Drop` records the close marker, except when the cycle suspends. This is the `MutexGuard` rule. A terminate is never deferred. |
| R6 | A non-cancellable block nests inside a cancellable scope. The scope drops the body and the block never closes. | Nesting in that order returns `HarvestError::Config`. The other order is allowed. |
| R7 | A start policy that replaces the old run, or a latest-wins supersede, waits on a deferred cancel. The new run then overlaps the old one. | `TerminateIfRunning`, signal-with-start replace and supersede use the non-deferring cancel. |
| R8 | An N-1 worker reads `WorkflowCancelRequested` and fails to decode. | Only a run that uses `non_cancellable` gets the event. That code needs N. The upgrade guide lists it under known limits. |
| R9 | The workflow sees the pending cancel through `is_cancelled()`. A cancel-checking primitive then fails on replay but not live. | `is_cancelled()` still reads only `WorkflowCancelled`. The pending cancel is invisible to workflow code. |

### 0.5 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | F1–F6. `ctx.race` already proves the marker plus `CancelRaceLosers` pattern. The tolerant marker scan exists. |
| Red | "Cancel the group, get `Cancelled` back" is easy to explain. Silent body work after a cancel would feel wrong. |
| Black | The horizon is new matcher state. A shield defers an operator cancel, so the operator sees `RUNNING` after a cancel. A shield cannot nest in a scope. Cleanup after a cancel that arrives when no block is open is still not possible (F3). |
| Yellow | No new command. One new event, only for runs that opt in. The race teardown is reused, so the worker work is small. |
| Green | B1–B5 and C1–C4. |
| Blue | Red phase: unit tests per member kind, a replay test, a live-cancel test, and a database test for the shield. Green phase: the scope, the shield, the matcher horizon, the deferred cancel. Refactor phase: docs, changelog, review. |

---

## 1. Design

### 1.1 `CancellationScope`

`ctx.cancellation_scope()` takes the next scope sequence number. `run(fut)`
polls `fut` with the scope pushed on a context stack. `push_command` tags each
command with the cancellable scopes above the nearest shield. Activity, timer
and child ids become scope members.

`cancel()` sets a flag and wakes the run future. The run future then acts at
its next poll:

1. It polls the body once more. If the body completes, `run` returns `Ok`.
2. **Live.** It removes the commands its body issued in this cycle
   (`ReleaseMutex` stays). It records the marker `cancel_scope:{seq}` with the
   reason, the horizon and the member ids. It pushes
   `CancelRaceLosers { reason: ScopeCancelled, .. }` for the open members.
   It returns
   `Err(Cancelled)` and drops the body.
3. **Replay.** At the first poll the scope finds its marker ahead. Then it
   polls the body with the matcher cut at the horizon, and it holds the body's
   commands. At the cancel it takes the marker and consumes the member events.
   It returns `Err(Cancelled)`.

A `cancel()` after `run` completes does nothing. A `cancel()` before the
first poll returns `Err(Cancelled)` after one poll of the body.

### 1.2 `non_cancellable`

The shield records `non_cancellable_open:{seq}` before its first poll of the
body and `non_cancellable_close:{seq}` when the body completes.

`cancel_workflow_execution_collect` counts open shields in history. If one is
open and the run is `RUNNING`, it appends `WorkflowCancelRequested { reason }`
once, keeps the state and the tasks, and returns `deferred = true`. A paused
run cannot close its block, so its cancel is not deferred.

The worker reads the request from the history that the cycle replayed:

- A cycle that suspends completes the cancel when no block is open. This
  covers a block that closed in an inline local-activity loop.
- A cycle that fails or continues as new is cancelled instead, so a retry or
  a successor cannot drop the cancel.
- A cycle that completes keeps its result.

The same-shard parent-close cascade defers in the same way as the
cross-shard path. A reset drops a request that it copies from the source.

### 1.3 Worker

`CancelRaceLosers` gets `reason`. The synthetic `ActivityFailed` and the child
cancel reason name it. No other worker path changes.

### 1.4 Limits

- A shield cannot nest inside a cancellable scope. A scope cannot acquire a
  durable mutex: a dropped acquire would block the key's waiter queue.
- A cancel that arrives when no shield is open is still terminal at once.
  A shield is open from the commit of the cycle that enters it. A cancel
  that commits first discards that cycle, the shield's work included.
- Local activities, external activities and detached children in a scope
  are not cancelled. The scope still drops the body, and replay consumes a
  late external result.
