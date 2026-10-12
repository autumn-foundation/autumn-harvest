# Design — Issue #2025: workflow updates on SQLite

A workflow that registers an update handler cannot receive updates on the
SQLite backend. This change adds `SqliteRuntime::update`. It also persists a
`RecordUpdateResult` command instead of sealing the run `FAILED`.

**No new `WorkflowEvent` variant. No schema change.** The backend writes the
core `UpdateAdmitted`, `UpdateCompleted` and `UpdateFailed` events.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The drive passes no declarative update handlers to the core
  (`runtime.rs`, the `&[]` argument). `RecordUpdateResult` falls to the
  catch-all rejection, so the run ends `FAILED` as `UnsupportedFeature`.
- On Postgres, the edge runs the validator before it admits the update. A
  rejection writes no event. The admit appends `UpdateAdmitted`.
- The core runs a handler only through
  `WorkflowContext::execute_admitted_update`. It returns a recorded result
  on replay. In live mode it runs the handler and pushes
  `RecordUpdateResult`.
- The Postgres worker turns each `RecordUpdateResult` into `UpdateCompleted`
  or `UpdateFailed`. It does this on the suspend path and before a terminal
  event.
- `WorkflowTestEnv::update` (issue #1991) runs an update with no database.
  It replays the history into a new context, validates, admits, runs the
  handler, and records the result. It does not drive the run.
- Replay treats the three update events as transparent. The main command
  match skips them, so an update never makes a replay diverge.
- The SQLite runtime holds the only write handle. No other writer can change
  the run between a read and a write in one call.

### 0.2 Brainstorm — how can SQLite run an update?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Stage the update in a table. The next decision cycle admits it and runs the handler. | Rejected. The core cycle never calls `execute_admitted_update` on its own. A validator must also run before any write. |
| B2 | Replay the history into a new context, as a query does. Validate, then run the handler on that context. Write `UpdateAdmitted` and the result in one transaction. | **Adopted.** It reuses the core replay driver and the `WorkflowTestEnv` precedent. A crash leaves no half-admitted update. |
| B3 | Write `UpdateAdmitted` first, then run the handler, then write the result. | Rejected. A crash or a blocked handler leaves an admitted update with no result. |
| B4 | Persist `RecordUpdateResult` in the decision cycle, on the suspend path and before a terminal event. | **Adopted.** It matches the Postgres worker. A workflow body that calls `execute_admitted_update` no longer fails. |
| B5 | Accept declarative `#[update]` handlers through `register_update`. | **Adopted.** The drive and the replay context get the same handlers. The arg schema and the validator both run. |
| B6 | Drive the run after the update. | Rejected. The caller drives, as for `send_signal`. The update returns as soon as its result is durable. |
| B7 | Cap the update input size. | Rejected. Postgres sets no update input cap. A new cap would make the backends differ. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A rejected update writes an event. | Validation runs before the transaction opens. A test compares the history before and after. |
| R2 | A crash after admission leaves an update with no result. | One transaction writes both events. |
| R3 | A handler awaits a durable command, so the call hangs forever. | The handler is polled until it stops making progress. Then the call returns `UpdateBlocked` and writes nothing. |
| R4 | A handler panic takes down the caller. | The poll catches the panic. The update records `UpdateFailed`, as `WorkflowTestEnv` does. |
| R5 | An update reaches a terminal run. | The state check returns `WorkflowNotRunning` first. |
| R6 | The new events break replay on the core. | Tests replay each new history on `WorkflowReplayer`. |
| R7 | A declarative handler of another workflow answers the update. | Handlers register by workflow name. A test sends a foreign name. |
| R8 | A workflow panic during the state rebuild takes down the caller. | The core replay driver contains it. The call returns `WorkflowPanicked`. |
| R9 | A body-driven `RecordUpdateResult` still seals the run. | Tests cover the suspend path and the terminal path. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The core already owns every update
  primitive. The backend lacks an ingress and a persist step.
- **Red (feelings).** A handler that runs outside the decision cycle feels
  odd. It is the same model that `WorkflowTestEnv` uses, and replay ignores
  update events.
- **Black (risks).** Section 0.3. A handler runs once, at admission. Replay
  does not run it again, so state it writes is not rebuilt in the body. This
  is also true on Postgres. Section 2 records it.
- **Yellow (benefits).** Request-response updates work on the edge backend.
  The history is byte-compatible with Postgres.
- **Green (ideas).** Update-with-start. An update that waits for a later
  decision cycle.
- **Blue (process).** Red tests, then green code, then refactor. Then a
  review from several angles.

---

## 1. Change

### 1.1 `SqliteRuntime::update(exec, name, args)`

1. Read the state. An unknown id is `ExecutionNotFound`. A terminal run is
   `WorkflowNotRunning`.
2. For a declarative handler, check the arg schema. A violation is
   `UpdateRejected`.
3. Replay the history into a new context with the core query driver. The
   body registers its imperative handlers. The runtime registers the
   declarative handlers of the workflow.
4. Run `validate_update`. An unknown name is `UpdateHandlerNotFound`. A
   validator rejection is `UpdateRejected`. Steps 1 to 4 write nothing.
5. Run the handler through `execute_admitted_update`. Poll it until it is
   ready or stops making progress.
6. In one transaction, append `UpdateAdmitted` and the event that the
   `RecordUpdateResult` command gives.
7. Return the output, or `UpdateFailed` with the handler error.

### 1.2 `RecordUpdateResult` in the decision cycle

`apply_commands` and the terminal drain append `UpdateCompleted` or
`UpdateFailed`, in command order. One helper builds the event for all three
sites.

### 1.3 `register_update(&UpdateHandlerInfo)`

The runtime keeps the declarative handlers by workflow name. The drive and
the update replay both register them.

---

## 2. Known limits

- The handler runs once, at admission. Replay returns the recorded result
  and does not run the handler again. State that the handler writes is not
  rebuilt in the body on a later cycle. Postgres has the same limit.
- A handler that awaits a durable command, such as an activity or a timer,
  returns `UpdateBlocked`. No event is written.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| An accepted update returns its result and is in history. | `accepted_update_returns_its_result_and_is_in_history` |
| A rejected update (validator fails) writes no event. | `rejected_update_writes_no_event` |
| The history replays on the core `WorkflowReplayer`. | `update_history_replays_on_the_core_replayer` |

Other tests in `tests/integration/updates.rs` cover a handler error, a panic,
a blocked handler, an unknown name, a terminal run, declarative handlers, a
reopen, and a body-driven `RecordUpdateResult`.
