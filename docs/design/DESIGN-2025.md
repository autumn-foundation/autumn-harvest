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
| B7 | Cap the update input size. | Rejected. Postgres sets no update cap. Only the HTTP route has the implicit 2 MiB body limit, and this backend is in-process like the typed client. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A rejected update writes an event. | Validation runs before the transaction opens. A test compares the history before and after. |
| R2 | A crash after admission leaves an update with no result. | One transaction writes both events. |
| R3 | A handler awaits a durable command, so the call hangs forever. | The handler runs under a 5 second limit. An alarm thread wakes the task at the deadline on any async runtime. The call returns `UpdateTimedOut` and writes nothing. |
| R10 | A handler awaits a timer, I/O or an async lock, and the call gives up on the first `Pending`. | The call awaits the handler until it finishes or the limit ends. |
| R11 | A validator panic reaches the caller. | The call catches it and returns `UpdateRejected`. No event is written. |
| R12 | A changed build replays the history to another state, and the update runs against it. | The call checks the replay for drift and returns `NonDeterministic`. No event is written. |
| R4 | A handler panic takes down the caller. | The poll catches the panic. The update records `UpdateFailed`, as `WorkflowTestEnv` does. |
| R5 | An update reaches a terminal run. | The state check returns `WorkflowNotRunning` first. |
| R6 | The new events break replay on the core. | Tests replay each new history on `WorkflowReplayer`. |
| R7 | A declarative handler of another workflow answers the update. | Handlers register by workflow name. A test sends a foreign name. |
| R8 | A workflow panic during the state rebuild takes down the caller. | The core replay driver contains it. The call returns `WorkflowPanicked`. |
| R9 | A body-driven `RecordUpdateResult` still seals the run. | Tests cover the suspend path and the terminal path. |
| R13 | A body that mints a new update id on each cycle makes the drive spin. | A recorded result is not progress, so each drive appends one result, as on Postgres. |
| R14 | Updates on a run that no one drives grow history past the caps (#2023). | The update transaction checks the caps with its two events. At a cap it rolls back and fails the run, as a drive does. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The core already owns every update
  primitive. The backend lacks an ingress and a persist step.
- **Red (feelings).** A handler that runs outside the decision cycle feels
  odd. It is the same model that `WorkflowTestEnv` uses, and replay ignores
  update events.
- **Black (risks).** Section 0.3. A handler runs once, at admission, on a
  separate replay. State that it writes never reaches the body. On Postgres
  the body runs the handler in the live cycle, so the body sees its writes
  until a replay. Section 2 records it.
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
5. Run the handler through `execute_admitted_update`. Await it until it
   finishes or its 5 second limit ends.
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
  rebuilt in the body on a later cycle. The handler also runs on a separate
  replay, so the body never sees its writes. On Postgres the body runs the
  handler in the live cycle, so the body sees its writes until a replay.
- The rebuild runs the body past the history frontier, as a query does. A
  handler can read a side effect, such as `new_uuid`, that the next drive
  mints again with another value.
- A handler that awaits a durable command, such as an activity or a timer,
  returns `UpdateTimedOut` after 5 seconds. No event is written.
- A workflow body that wakes itself forever makes the rebuild run for 5
  seconds, then return `TaskFailed`. A drive of that run fails too.
- Update input and output have no size cap of their own, as on the
  in-process Postgres path. The history caps still bound them.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| An accepted update returns its result and is in history. | `accepted_update_returns_its_result_and_is_in_history` |
| A rejected update (validator fails) writes no event. | `rejected_update_writes_no_event` |
| The history replays on the core `WorkflowReplayer`. | `update_history_replays_on_the_core_replayer` |

Other tests in `tests/integration/updates.rs` cover these cases:

- A handler error, a handler panic and a validator panic.
- A handler that a later wake finishes, and handlers that time out.
- An unknown name, a terminal run, a sealed run and an unregistered workflow.
- Declarative handlers and their arg schema.
- A reopen, and an injected write failure that proves the two events commit
  together.
- An update before the first drive and during a pending timer.
- A changed build that drifts from history.
- A body-driven `RecordUpdateResult`, on both the suspend path and the
  terminal path.
- Parity with `WorkflowTestEnv` for the same steps.
