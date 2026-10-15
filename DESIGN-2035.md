# Design — Issue #2035: run admitted declarative updates

The HTTP edge admits an update, but no engine code runs its handler. A
caller that waits for completion gets `504`. This change makes the worker
run each admitted update once, in the decision cycle, and record the result.

**No new `WorkflowEvent` variant. No schema change.** The worker already
persists `RecordUpdateResult` as `UpdateCompleted` or `UpdateFailed`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `admit_update` appends `UpdateAdmitted`, wakes the workflow task, and polls
  history for `UpdateCompleted` or `UpdateFailed`.
- `build_task_context` registers each declarative handler. Nothing runs it.
- Only `execute_admitted_update` runs a handler, and no engine code calls it.
  `drain_admitted_updates` lists updates with no result. No engine code calls
  it either.
- The woken task replays the body. The body parks again. The update stays
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

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A replay gate runs a handler. | The flag is off by default. A test drives a cycle without it. |
| R2 | A re-drive runs a handler twice. | The worker keeps each result in `history_events`. A test replays a history with the result. |
| R3 | The body calls `execute_admitted_update` for the same id, so the handler runs twice. | The dispatch skips an id that the batch already records. A test covers it. |
| R4 | A handler panic kills the worker task. | The dispatch catches the panic and records `UpdateFailed`. |
| R5 | A handler never finishes and blocks the task. | A 5 second limit records `UpdateFailed`. A paused-clock test covers it. |
| R6 | A discarded cycle runs a handler. Its result is lost, so the next cycle runs the handler again. | Dispatch skips non-deterministic, panicked and deadlocked cycles. |
| R7 | A result lands after the terminal event. | The terminal path persists update results first. A test covers it. |
| R8 | Several updates run out of order. | The dispatch follows history order. A test covers it. |
| R9 | An unknown update name loops. | The dispatch records the "not found" failure once. |
| R10 | `continue_as_new` reorders the batch. | The dispatch adds results after the cycle outcome. A test covers it. |
| R11 | A warm resume skips an update. | The resident path declines on `UpdateAdmitted`. |
| R12 | A crash after the handler and before the persist runs the handler again. | Not preventable. Section 2 records it. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The worker persists update results. The
  executor never produces them for a declarative update.
- **Red (feelings).** Running a handler after the body feels late. It is
  safe: replay returns the recorded result and update events never shift a
  command match.
- **Black (risks).** Section 0.3. Delivery is at least once. State that a
  handler writes does not return on replay.
- **Yellow (benefits).** `wait=completed` and the MCP update tools work. The
  fix reuses the worker persist path, the metrics and the resident rules.
- **Green (ideas).** A body API to await an update. A wake for a task that
  waits on a timer. A builder setting for the handler limit.
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

1. `WorkflowContext::with_update_dispatch` turns dispatch on. Only the
   worker cold path calls it.
2. `WorkflowContext::run_pending_updates` lists the updates with no result.
   It skips an id that the batch already records. It runs each handler and
   adds a `RecordUpdateResult` to the batch.
3. `drive_cycle` calls it after `classify_cycle` for a suspended, completed,
   continued-as-new or failed outcome. A failed outcome with a divergence or a
   panic keeps no commands, so it does not dispatch.
4. Each handler runs under `UPDATE_HANDLER_TIMEOUT` (5 seconds). A panic or a
   timeout becomes `UpdateFailed`.

---

## 2. Known limits

- Delivery is at least once. A crash after the handler and before the persist
  runs the handler again. Write a handler that tolerates a repeat.
- Replay does not run the handler again. State that the handler writes to a
  captured variable does not return in the body.
- A handler cannot await a durable command, such as an activity or a timer.
  It times out and records `UpdateFailed`.
- One cycle runs every pending update, one after another.
- An admit does not wake a task that waits on a timer. The update waits for
  the timer or for another event.
- The result event lands after the events that the cycle loaded.
  `all_handlers_finished()` reads history up to the replay position. It sees
  the result once the body passes it.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| An update sent with `wait=completed` to a workflow that waits on a signal returns the handler output. | `mcp_update_tools_run_the_handler_synchronously` |
| The two ignored tests in `mcp_tools_integration` pass. | `update_follows_the_continue_as_new_chain_to_the_live_successor` |
