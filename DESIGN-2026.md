# Design — Issue #2026: SQLite child workflows, and signal or cancel between runs

Issue #2026 asks the SQLite backend to run composition. A workflow must start
a child, await another run, and signal or cancel another run. Before this
change, each of these commands sealed the caller `FAILED` as an unsupported
feature.

**One additive schema step. No new `WorkflowEvent` variant. One new
`RunState` variant, `WaitingWorkflow`.** Every event this change writes
already exists in the core. Each event has the shape that the Postgres worker
writes.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The core context emits five commands: `StartChildWorkflow`,
  `SpawnDetachedChildWorkflow`, `AwaitExternalWorkflow`,
  `SignalExternalWorkflow` and `RequestCancelExternalWorkflow`. The SQLite
  runtime rejected all five by name.
- An in-progress child re-emits `StartChildWorkflow` with the recorded
  `child_id` on each replay. The worker must not start it again.
- Postgres starts a child in the parent's decision transaction. The child row
  holds `parent_id`. `workflow_id` is the child id as a string.
- An awaited child has no parent-close policy. Its terminal reaches the
  parent as `ChildWorkflowCompleted` or `ChildWorkflowFailed`. A typed failure
  keeps `error_type`, `details` and `non_retryable`.
- A cancelled child reaches the parent as `ChildWorkflowFailed` with the text
  `child workflow cancelled: <reason>`.
- A detached child carries a policy. When the parent closes, `request_cancel`
  cancels the child with reason `parent closed`, and `terminate` fails it with
  `ParentClosed`. The parent then records `ChildWorkflowCascadeApplied`.
  `abandon` does nothing.
- External signal, cancel and await each write a `*Requested` event and then
  a terminal event. Postgres resolves a same-shard target in one transaction.
- An external cancel is a hard seal. The target gets `WorkflowCancelled`
  with reason `cancelled by external request`. It ends `CANCELLED`.
- The cancel API of issue #2021 does not exist yet. This change adds the
  internal cancel step that #2021 can expose later.
- `ctx.info().parent_execution_id` is set for a child on Postgres.

### 0.2 Brainstorm — how can one process run composition?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A child is an execution row plus a parent link. Start it in the parent's cycle transaction. | **Adopted.** The issue proposes it, and it matches Postgres. |
| B2 | Write the child's result into the parent's history in the child's terminal transaction. | **Adopted.** The parent then sees the result on its next cycle. No outbox is needed. |
| B3 | Poll the child's state from the parent on each cycle instead. | Rejected for children. It writes the result at a later point than Postgres. It is used for await, where Postgres also polls. |
| B4 | Let `run_until_blocked(parent)` drive a running child or await target in the same call. | **Adopted.** A child is local work, not an external input. The simple case then completes in one call. |
| B5 | Return the child's own block reason from `run_until_blocked(parent)`. | Rejected. A caller would send a signal to the wrong run. A new `WaitingWorkflow(exec)` state names the run to look at. |
| B6 | Resolve an external target in the caller's transaction. | **Adopted.** One database holds every run, so the answer is final at once. |
| B7 | Keep the Postgres five-second grace window for an unknown target. | Rejected. One file is authoritative. An unknown target fails at once with `target_unknown`. |
| B8 | Add a public `cancel` method now. | Deferred to issue #2021, which owns its contract. |
| B9 | Apply the parent-close policy on every terminal path through one helper. | **Adopted.** A missed path would leave a child running or a parent parked. |
| B10 | Keep the signal idempotency key. | **Adopted.** A new nullable column holds it. No command field is dropped. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Start a child twice on a re-drive. | Skip a `child_id` that history already records or that has a row. A test drives a parent many cycles and counts one child. |
| R2 | A terminal path forgets to tell the parent. The parent parks forever. | One `close_execution` helper runs on every terminal path. Tests cover complete, typed fail, unsupported seal and cancel. |
| R3 | A child result lands in a sealed parent. | The helper writes only to a `RUNNING` parent. |
| R4 | A cancel leaves task, timer or signal rows behind. | The cancel removes them, as the unsupported seal does. A test counts the rows. |
| R5 | Two runs await each other, and the driver spins. | The driver keeps a stack and refuses a run that is already on it. It returns `WaitingWorkflow`. |
| R6 | A history does not replay on Postgres. | Each test replays every history on the core `WorkflowReplayer`. |
| R7 | An old database file lacks the new columns. | The additive `migrate` step adds them. A test opens an old-shape file. |
| R8 | A child error hides behind the parent's drive. | A sealed child is terminal, and the parent records its typed failure. Any other child error returns from the drive. |
| R9 | A new `RunState` variant breaks an exhaustive match. | The crate is pre-1.0. The changelog names the variant. The in-tree daemon match is updated. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Every event shape comes from the Postgres
  worker. No new event variant is needed.
- **Red (feeling).** Composition is the main blocker for porting real code.
  Users expect `run_until_blocked(parent)` to finish a parent whose child
  needs no input.
- **Black (risk).** Parked parents (R2), double starts (R1), orphan rows
  (R4), mutual awaits (R5), and a public enum change (R9).
- **Yellow (value).** Larger workflows port unchanged. The agent daemon can
  fan work out to children. Issue #2021 gets its cancel step for free.
- **Green (options).** The stack driver (B4) and the immediate answer for an
  unknown target (B7). Await polls on each cycle (B3), which keeps the target
  untouched.
- **Blue (process).** Red phase: integration tests for each criterion, plus
  the rewrite of tests that used these commands as the unsupported example.
  Green phase: schema, start, delivery, cascade, external commands, driver.
  Refactor: one close helper, short STE docs, review by several agents.

---

## 1. Design

### 1.1 Schema

`harvest_executions` gains `parent_exec_id` and `parent_close_policy`. A row
with a parent and no policy is an awaited child. A row with a policy is a
detached child. `harvest_signals` gains `idempotency_key`. The `migrate` step
adds each column and its index to an old file.

### 1.2 Start

`StartChildWorkflow` appends `ChildWorkflowStarted` to the parent. It inserts
the child row and the child's `WorkflowStarted` in the same transaction.
`SpawnDetachedChildWorkflow` appends `ChildWorkflowSpawnedDetached` and does
the same, with the policy on the row. An unregistered child workflow returns
`UnknownWorkflow`, writes nothing, and leaves the parent `RUNNING`.

### 1.3 Close

`close_execution` runs in the terminal transaction of every run:

1. An awaited child writes its result to a `RUNNING` parent.
2. Each running detached child gets its parent-close policy.
3. The parent records `ChildWorkflowCascadeApplied` for each cascaded child.

### 1.4 External commands

| Command | Target state | Caller events |
|---|---|---|
| Signal | `RUNNING` | `ExternalSignalRequested`, `ExternalSignalDelivered` |
| Signal | terminal | `ExternalSignalFailed` `target_terminal`, or `not_running` by id |
| Signal | unknown | `ExternalSignalFailed` `target_unknown` |
| Cancel | `RUNNING` | `ExternalCancelRequested`, `ExternalCancelDelivered` |
| Cancel | terminal | `ExternalCancelDelivered` |
| Cancel | unknown | `ExternalCancelFailed` `target_unknown` |
| Await | `RUNNING` | `ExternalAwaitRequested`, then the run waits |
| Await | terminal | `ExternalAwaitResolved` or `ExternalAwaitFailed` |
| Await | unknown | `ExternalAwaitFailed` `target_unknown` |

### 1.5 Drive

A parked run that waits on a running child or await target returns
`WaitingWorkflow(target)`. `run_until_blocked` then drives the target in the
same call. The fleet drivers treat `WaitingWorkflow` as blocked.

### 1.6 Terminal cycle

A failing cycle that drops a fresh `StartChildWorkflow` records the abandoned
pair, as Postgres does (issue #952). A detached spawn in a terminal cycle
starts its child before the terminal event. External commands in a terminal
cycle write nothing, as on Postgres.
