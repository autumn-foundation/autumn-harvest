# Design — Issue #2035: run admitted declarative updates

The HTTP edge admits an update, but no engine code runs its handler. A
caller that waits for completion gets `504`. This change makes the worker
run each pending update in the decision cycle and record the result.

**No new `WorkflowEvent` variant. No schema change.** The worker already
persists `RecordUpdateResult` as `UpdateCompleted` or `UpdateFailed`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Before this change, `admit_update` appended `UpdateAdmitted`, woke the
  workflow task, and polled history for `UpdateCompleted` or `UpdateFailed`.
- `build_task_context` registers each declarative handler. Nothing ran it.
- Only `execute_admitted_update` runs a handler, and no engine code called
  it. `drain_admitted_updates` lists updates with no result. No engine code
  called it either.
- The woken task replayed the body. The body parked again. The update stayed
  admitted.
- The worker already persists each `RecordUpdateResult`. It does so on the
  suspend path, before a terminal event, and in each inline re-drive. It also
  emits the update metrics.
- Replay skips the three update events when it matches commands. An update
  event can sit anywhere in history and never causes a divergence.
- A handler runs on its own context. It cannot read a body local variable.
- A resident resume declines when its delta holds `UpdateAdmitted`. A cycle
  with a `RecordUpdateResult` never stays resident.
- The replay gates share `drive_cycle` with the worker.
- The determinism guide says an update that the engine has not run when the
  run ends is orphaned. The caller gets `409 update_orphaned`.
- `wake_workflow_task` wakes a parked row. It skips a `PENDING` timer row.
- The whole decision cycle runs under the workflow-task budget. The budget is
  the larger of `workflow_task_timeout` and the local-activity cap. A cycle
  that exceeds it is cancelled, and each cancellation is a poison-pill strike.

### 0.2 Brainstorm — where can a pending update run?

| # | Idea | Verdict |
|---|------|---------|
| B1 | The edge runs the handler at admission. | Rejected. The edge has no body, so a handler that the body registers does not exist there. Two edges can run one update. |
| B2 | The worker runs pending updates before it polls the body. | Rejected. A body-registered handler does not exist yet, so the update fails as "not found". |
| B3 | The worker runs pending updates after the body parks or returns, in the same cycle. | **Adopted.** Declarative and body-registered handlers both exist. The result joins the cycle commands. |
| B4 | Run each update when the replay position passes its event. | Rejected. The `match_history` hook is synchronous. A handler is async. |
| B5 | Add `ctx.wait_for_update()` for the body. | Rejected. A declarative update needs no body code. |
| B6 | Give each update its own task row. | Rejected. It needs a migration. The wake path exists. |
| B7 | Dispatch in every executor path. | Rejected. A replay gate must not run handler side effects. |
| B8 | Dispatch only for a context that the worker marks. | **Adopted.** One flag, set at one call site. |
| B9 | Bound each handler and record a timeout as `UpdateFailed`. | **Adopted.** A hung handler cannot block the task. One failure stops a repeat. |
| B10 | Run updates on terminal outcomes too. | **Adopted.** An update that precedes the final event in history gets its answer. |
| B11 | Run every pending update in one cycle. | Rejected. Many slow handlers can exceed the task budget and cost poison-pill strikes. |
| B12 | Cap the count per cycle. | Rejected. A count says nothing about time. |
| B13 | Start a handler only while a full handler limit is left in half of the remaining budget. Wake the task for the rest. | **Adopted.** The cycle persists inside the budget. The first update always runs, so each cycle makes progress. |
| B16 | Stop every handler at half of the budget that is left. | **Adopted.** A slow body cannot push the first update past the cut-off. The persist keeps the other half. |
| B17 | Run every update when the task timeout is off. | Rejected. A burst of slow handlers could hold the task and a connection for a long time. A fixed 30 second window ends the batch. |
| B14 | Fail the updates that the window leaves. | Rejected. A slow handler would fail healthy updates. A later cycle can run them. |
| B15 | Store the panic text in the result. | Rejected. History is plaintext and the caller reads it. The log keeps the text. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A replay gate runs a handler. | The flag is off by default. A test drives a cycle without it. |
| R2 | A re-drive runs a handler twice. | The worker keeps each result in `history_events`. Tests replay a history with the result and drive a local-activity cycle. |
| R3 | The body calls `execute_admitted_update` for the same id, so the handler runs twice. | The dispatch skips an id that the batch already records. A test covers it. |
| R4 | A handler panic kills the worker task. | The dispatch catches the panic and records `UpdateFailed`. |
| R5 | A handler never finishes and blocks the task. | A 5 second limit records `UpdateFailed`. A paused-clock test covers it. |
| R6 | A discarded cycle runs a handler. Its result is lost, so the next cycle runs the handler again. | Dispatch skips non-deterministic, panicked and deadlocked cycles. |
| R7 | A result lands after the terminal event. | The terminal path persists update results first. A worker test covers it. |
| R8 | Several updates run out of order. | The dispatch follows history order. Tests cover it. |
| R9 | An unknown update name loops. | The dispatch records the "not found" failure once. |
| R10 | `continue_as_new` reorders the batch. | The dispatch adds results after the cycle outcome. A test covers it. |
| R11 | A warm resume skips an update. | The resident path declines on `UpdateAdmitted`. |
| R12 | The cycle is discarded after the handler ran. | Not preventable. Section 2 lists the paths. |
| R13 | Many slow handlers exceed the task budget. The run gets poison-pill strikes and fails. | The dispatch window stops the batch. The worker wakes the task for the rest. Unit tests and a worker test cover it. |
| R14 | The window never lets a handler start, and the wake repeats forever. | The first update of a decision always runs. Each woken decision resolves at least one update. |
| R16 | A slow body leaves under 5 seconds, and the first handler passes the cut-off. | The handler limit is half of what is left. A late handler records `UpdateFailed`, and the run lives. |
| R15 | A panic message with internal detail reaches history and the HTTP caller. | The result text is fixed. The log keeps the detail. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The worker persists update results. The
  executor never produced them for a declarative update.
- **Red (feelings).** Running a handler after the body feels late. It is
  safe: replay returns the recorded result and update events never shift a
  command match.
- **Black (risks).** Section 0.3. Delivery is at least once. A value that a
  handler writes to a captured variable does not return on replay. A burst
  of slow updates needs the window.
- **Yellow (benefits).** `wait=completed` and the MCP update tools work. The
  fix reuses the worker persist path, the metrics and the resident rules.
- **Green (ideas).** A body API to await an update. A wake for a task that
  waits on a timer. A builder setting for the handler limit. The update id
  as a dedupe key for the handler.
- **Blue (process).** Red tests, then green code, then refactor. Then a
  review from several angles.

---

## 1. Change

### 1.1 Where an update runs

An admitted update runs **after the body, at the end of the same cycle, in
history order**. This order is safe for replay:

- Replay never runs the handler. `execute_admitted_update` returns the
  recorded result.
- Update events are transparent to command matching. The result event can
  sit after the events that the cycle loaded.
- A declarative handler has its own context. It cannot change a body command.

### 1.2 Mechanism

1. `WorkflowContext::with_update_dispatch` turns dispatch on and sets the end
   of the task budget. Only the worker cold path calls it.
2. `update::dispatch_pending_updates` lists the updates with no result
   through `WorkflowContext::pending_updates`. It skips an id that the batch
   already records. It runs each handler through
   `WorkflowContext::run_update_handler` and adds a `RecordUpdateResult` to
   the batch.
3. `drive_cycle` calls it after `classify_cycle`. A suspended, completed,
   continued-as-new or failed outcome dispatches. A divergent, panicked or
   deadlocked cycle does not, because the worker discards its commands.
4. Each handler runs under `UPDATE_HANDLER_TIMEOUT` (5 seconds). A panic or a
   timeout becomes `UpdateFailed`. The panic text goes to the log only.
5. The dispatch window is half of the budget that is left at the end of the
   body. A handler starts only while a full handler limit is left in the
   window. The first update of a decision always runs. A re-drive of the same
   decision runs no update past the window. Every handler stops at half of the
   budget that is left. A task with no budget gets a fixed 30 second window.
6. When updates remain, `DriveResult::updates_deferred` is `true`. The worker
   then calls `wake_workflow_task`. The claimed row records the wake, and the
   park that ends the cycle re-pends the task.

---

## 2. Known limits

- Delivery is at least once. The handler runs before the worker persists the
  result. Any path that discards the cycle runs the handler again: a crash, a
  deadlock retry, a pause that lands in the window, a lost claim, and a
  capability-miss release. The handler has no dedupe key. Write a handler that
  tolerates a repeat.
- A handler that times out stops at its next await point. The effects that it
  already made stay, and the result is `UpdateFailed`.
- Replay does not run the handler again. A value that the handler writes to a
  captured variable does not return in the body.
- A handler cannot await a durable command, such as an activity or a timer.
  It times out and records `UpdateFailed`.
- A handler stops at half of the budget that is left. A slow body can leave
  less than 5 seconds. The handler then records `UpdateFailed`, and the run
  lives.
- A terminal cycle that the window cuts short leaves updates pending. The
  workflow ends, so those updates are orphaned.
- A missing handler records `UpdateFailed` for good. This happens on a worker
  of an older build, and when the body registers the handler after a later
  await.
- An admit does not wake a task that waits on a timer. The update waits for
  the timer or for another event. A retry of the call is a new update.
- The result event lands after the events that the cycle loaded.
  `all_handlers_finished()` reads history up to the replay position. It sees
  the result once the body passes it.
- The worker writes the update result before the other events of the cycle.
  A body that reads `replay_position()` or `info().history_event_count` in
  such a cycle can read another value on replay. Do not branch on them.
- A handler result has no size cap of its own, as for a body-driven update.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| An update sent with `wait=completed` to a workflow that waits on a signal returns the handler output. | `a_declarative_update_runs_once_while_the_workflow_waits` and `mcp_update_tools_run_the_handler_synchronously` |
| The two ignored tests in `mcp_tools_integration` pass. | `mcp_update_tools_run_the_handler_synchronously` and `update_follows_the_continue_as_new_chain_to_the_live_successor` |

Other tests cover these cases:

- Unit tests in `update.rs`: a suspended cycle, a recorded result, a context
  without dispatch, a handler error, panic and timeout, an unknown name,
  history order, terminal and continue-as-new cycles, a divergent cycle, a
  panicking body, a body-driven update and a body-registered handler.
- Unit tests in `update.rs`: results of one cycle fed into the next cycle, and
  the dispatch window with a closed, an open and a paused clock.
- `declarative_update_tests` on Postgres: a finishing cycle, a failing and an
  unknown update, a local-activity cycle, and updates that a closed window
  leaves for later cycles.
