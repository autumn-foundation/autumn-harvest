# Design — Issue #2021: SQLite cancel and terminate

`SqliteRuntime` gets `cancel(exec, reason)` and `terminate(exec, reason)`.
Cancel is cooperative. Terminate seals the run at once.

**No new `WorkflowEvent` variant.** Both paths record the core
`WorkflowCancelled` event, as Postgres does. The SQLite schema gets two
nullable columns, `harvest_executions.cancel_reason` and `cancel_from_seq`. The core gets one new
seam, `WorkflowExecuteSpanMeta::cancellation_reason`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Postgres `cancel` appends `WorkflowCancelled` and seals `CANCELLED` in one
  transaction. Postgres `terminate` appends the same event and seals
  `TERMINATED`. Neither runs the body again.
- `WorkflowContext` reads `cancellation_reason()` from any
  `WorkflowCancelled` event in the history. No other input sets it.
- The core `HistoryMatcher` does not skip `WorkflowCancelled`. A command
  after that event in history diverges. `docs/shipped-work.md` (issue #801,
  PR #973) records this limitation.
- An experiment on the core `WorkflowReplayer` gave these results:

  | History | Body | Result |
  |---------|------|--------|
  | `step, cleanup, WorkflowCancelled` | checks `is_cancelled()`, runs `cleanup` | `ReplaySucceeded` |
  | `step, WorkflowCancelled, cleanup` | same | ND: `ActivityScheduled(cleanup)` vs `WorkflowCancelled` |
  | `step, WorkflowCancelled` | waits on a signal, no check | ND: `SignalReceived(go)` vs `WorkflowCancelled` |

- The SQLite driver replays the body from the start on each cycle. A
  suspended body future is dropped at the end of the cycle. The driver
  therefore cannot resolve a parked await with `Err(Cancelled)`.
- `run_workflow_with_state_history_policy_and_caps` builds the context from
  `WorkflowExecuteSpanMeta`. The SQLite driver already uses that struct as
  its seam for the business `workflow_id`.

### 0.2 Brainstorm — how can the body see a cancel request?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Append `WorkflowCancelled` at request time. | Rejected. A cleanup command after it diverges (0.1, row 2). |
| B2 | Make the matcher skip `WorkflowCancelled`. | Rejected. It changes Postgres replay. Issue #801 rejected it for that reason. |
| B3 | Inject a virtual `WorkflowCancelled` into the replayed history only. | Rejected. The cursor stops on it, so a new command still diverges. |
| B4 | Store the request in a column. Pass the reason to the context through `WorkflowExecuteSpanMeta`. Append `WorkflowCancelled` once, as the terminal event. | **Adopted.** The stored history has the replayable shape (0.1, row 1). Postgres passes `None`, so its behavior does not change. |
| B5 | Add a parameter to the executor entry point. | Rejected. That signature already has 20 positional parameters and many callers. |
| B6 | Seal on the request and never run the body, as Postgres does. | Rejected. The issue asks that the body observe the cancel and run cleanup activities. |

### 0.3 Brainstorm — when does a cancel-requested run end?

| # | Idea | Verdict |
|---|------|---------|
| E1 | When the body returns `Ok` or `Err`. | **Adopted.** The body has done its cleanup. |
| E2 | When the body blocks on a signal or a timer and no activity is left to run. | **Adopted.** A parked await cannot observe the cancel (0.1). Without this rule a stuck run never ends. |
| E3 | Wait while an activity task is still pending, for example a retry in backoff. | **Adopted.** That work can still resolve the await. The body then sees the cancel at its next check. |
| E4 | Fire pending timers early to wake the body. | Rejected. A false `TimerFired` makes the history lie. |
| E6 | When the run has added 1,000 events since the request. | **Adopted after the Codex review.** A body that ignores the request and keeps running activities makes progress on every cycle, so E2 never applies. The bound is durable: `cancel_from_seq` stores the history length at the request. |
| E5 | When the cycle diverges or deadlocks. | **Adopted after review.** The body sees the request from the start of its replay. A check before recorded commands therefore diverges on every drive. The driver discards that cycle and ends the run. |

### 0.4 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | The cancel event sits before the cleanup events, so the history does not replay. | The event is appended only at the seal, as the last event. A test replays the cancel history on `WorkflowReplayer`. |
| R2 | A crash between request and seal loses the request. | The request is a committed column. A test reopens the file and drives the run. |
| R3 | A sealed run keeps a pending task, timer or signal row. | Both seals delete them in the seal transaction. Tests count the rows. |
| R4 | A second `cancel` overwrites the first reason. | A repeat cancel keeps the first reason and appends nothing. A test pins it. |
| R5 | The seam changes Postgres replay. | Postgres passes `None`. A recorded `WorkflowCancelled` still wins over the seam. A unit test pins both. |
| R6 | A non-determinism or panic during cleanup seals the run. | A panic keeps its strike budget. Non-determinism and a deadlock end the run `CANCELLED` (rule E5), because a pending cancel must end the run. |
| R7 | A body that ignores the cancel runs forever. | It seals as soon as it blocks (E2). `terminate` is the hard stop. |
| R8 | `AllowDuplicateFailedOnly` cannot restart a cancelled run. | A `CANCELLED` prior is replaced like a `FAILED` one, as in the Postgres matrix. |
| R9 | A check at the top of a loop replays as cancelled before recorded steps, so every drive diverges and the run never ends. | Rule E5 ends the run. A test cancels a loop body and a body that checks before a recorded timer. |

### 0.5 Six thinking hats

- **White (facts).** Section 0.1. Four acceptance criteria. Postgres seals
  at once and reuses `WorkflowCancelled` for both paths.
- **Red (feel).** "Cooperative" must not mean "maybe never". A cancel must
  always end the run in a bounded number of drives.
- **Black (risks).** A core API change. A schema change. A parked body that
  cannot observe the cancel. Sections 0.3 and 0.4 cover each risk.
- **Yellow (value).** The agent daemon and CLIs can stop a stuck run.
  Cleanup activities run. Both histories replay on the core replayer.
- **Green (options).** Section 0.2. Follow-ups: record the request position
  in history, so a check sees the request only after that point. Wake a
  parked await through the resident path (issue #1798).
- **Blue (process).** Red tests first, then the core seam, then the SQLite
  paths, then docs. Comment hygiene and clippy gate the push.

---

## 1. API

```rust
impl SqliteRuntime {
    pub fn cancel(&mut self, exec: ExecutionId, reason: &str) -> SqliteResult<()>;
    pub fn terminate(&mut self, exec: ExecutionId, reason: &str) -> SqliteResult<()>;
}

pub enum RunState {
    // ...existing variants...
    Cancelled(String),
    Terminated(String),
}
```

- An unknown id returns `ExecutionNotFound`. A terminal run returns
  `WorkflowNotRunning`. Postgres treats a repeat call on a terminal run as a
  no-op, but the acceptance criteria ask for the error.
- A blank reason becomes the Postgres default text.
- `outcome()` reports `Terminated("CANCELLED")` or `Terminated("TERMINATED")`.

## 2. Cancel

1. `cancel` writes `cancel_reason` on a `RUNNING` row. It appends no event.
   A repeat call keeps the first reason.
2. Each drive reads `cancel_reason` and passes it through
   `WorkflowExecuteSpanMeta::cancellation_reason`. The body sees
   `cancellation_reason()` from the start of the replay.
3. Cleanup commands persist and run as usual.
4. The run ends per section 0.3. A body that returned keeps its terminal
   bookkeeping. A diverged or deadlocked cycle is discarded. The seal then
   appends `WorkflowCancelled { reason }`, sets `CANCELLED` with
   `error = reason`, and deletes pending tasks, unfired timers and staged
   signals. The drive returns `RunState::Cancelled(reason)`.

Known limits:

- A check before recorded commands ends the run without cleanup (E5).
- A cleanup that waits on a timer or a signal ends the run at that wait (E2).
- The output of a body that returns `Ok` after a request is not kept (E1).

## 3. Terminate

One transaction appends `WorkflowCancelled { reason }`, sets `TERMINATED`
with `error = reason`, and deletes pending tasks, unfired timers and staged
signals. The body does not run again. A later drive returns
`RunState::Terminated(reason)`.

## 4. Replay

- A cancel history ends `..., cleanup events, WorkflowCancelled`. It replays
  on `WorkflowReplayer` (0.1, row 1). A cancel that ends a parked body with
  no check has the terminate limit below.
- A terminate history ends `..., WorkflowCancelled`. It replays when the
  body checks `is_cancelled()` before it issues a new command. A body parked
  on a signal does not replay (0.1, row 3). The same limit applies to a
  Postgres history of the same shape.

## 5. Tests

`autumn-harvest-sqlite/tests/integration/cancel_terminate.rs` covers each
criterion, the crash window, idempotence, row cleanup and the reuse matrix.
A core unit test pins the seam.
