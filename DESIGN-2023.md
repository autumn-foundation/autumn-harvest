# Design — Issue #2023: SQLite `continue_as_new` and the default history caps

A long-lived workflow on SQLite cannot bound its history. `continue_as_new`
fails the run, and the #1804 caps are not enforced. Each decision reloads and
replays the whole history, so a loop workflow gets slower without limit.

**No migration. No new `WorkflowEvent` variant. No Postgres change.** One new
`RunState` variant, one new public type, two new setters.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `drive_one_cycle_inner` maps `WorkflowOutcome::ContinuedAsNew` to
  `Unsupported("ContinueAsNew")`. The run then seals `FAILED` (#1834).
- The reuse-policy path already seals a prior run `CONTINUED_AS_NEW`
  (`cancel_and_seal_prior`). The reuse lookup skips sealed rows, so the
  key passes to the next run.
- The Postgres worker writes one transaction: the `WorkflowContinuedAsNew`
  event, the seal, the successor row, its `WorkflowStarted`, and a move of
  the undelivered signals. Delivered signals stay on the old run.
- The executor returns the cycle's bookkeeping commands as `pending` with
  a `ContinuedAsNew` outcome. Postgres drops a dispatch in that drain.
- Postgres counts `history + this decision's events` against the event
  cap. A `ContinuedAsNew` outcome is exempt. The byte cap uses the bytes
  stored when the decision starts.
- Postgres writes `WorkflowFailed { error: <DeadLetterReason JSON> }`.
  `DeadLetterReason` lives in the `db`-only `dlq` module, so this crate
  cannot use it.
- `WorkflowHistoryPolicy::decision_boundaries` is read by the Postgres
  worker only. The executor reads the soft threshold.

### 0.2 Brainstorm — how can SQLite bound a history?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Seal, start the successor and move signals in one transaction, as Postgres does. | **Adopted.** One commit, so a crash leaves both runs or neither. |
| B2 | Let `run_until_blocked` follow the chain and drive the successor. | Rejected. The caller then gets a state for an id it never saw. |
| B3 | Return a new `RunState::ContinuedAsNew(successor)`. | **Adopted.** The caller learns the successor id. A re-drive of the sealed run returns it again. |
| B4 | Support a cross-type continuation (`continue_as_new_as_type`). | Deferred. It needs per-type cap checks. It is rejected by name, as other non-goals are. |
| B5 | Check the event cap before the handler runs. | Rejected. One decision can overshoot the cap. |
| B6 | Check the event cap inside the decision transaction, after its appends and before commit. | **Adopted.** The count is exact, with no estimate of each command. |
| B7 | Sum the byte length while the history loads. | **Adopted.** No second scan. |
| B8 | Take a whole `WorkflowHistoryPolicy` setter. | Rejected. Its `decision_boundaries` flag would have no effect here. Two cap setters say exactly what applies. |
| B9 | Mirror `DeadLetterReason` as a local type, pinned to the same JSON. | **Adopted.** A test pins the bytes on each side. |

### 0.3 Reverse brainstorm — how can this go wrong?

| # | Failure | Guard |
|---|---------|-------|
| R1 | A signal staged before the hand-off is lost. | Move every undelivered row to the successor in the seal transaction. |
| R2 | A task or timer of the old run runs after the seal. | Delete its pending tasks and unfired timers in the same transaction. |
| R3 | A re-drive of the old run starts a second successor. | The terminal short-circuit runs first. It reads the successor from the last event. |
| R4 | A dispatch in the continue-as-new drain seals the run `UnsupportedFeature`. | Drop dispatch commands in that drain, as Postgres does. Keep markers and side effects. |
| R5 | A cap failure leaves an orphan task, timer or signal. | The cap seal shares the cleanup of the unsupported seal. |
| R6 | A reuse-policy seal is reported as a continuation. | Only a last `WorkflowContinuedAsNew` event names a successor. |
| R7 | The cap fails a run whose cycle is a replay divergence. | The divergence, panic and failed-task gates return first, as on Postgres. |
| R8 | A test needs 50,000 events. | `set_history_event_hard_cap` lowers the cap. |
| R9 | `claude-agent-daemon` matches `RunState` exhaustively. | Add the arm: retire the old run, keep the successor live. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The defaults are 50,000 events and
  50 MiB. Each SQLite decision is O(history).
- **Red (feeling).** A user who saw "the SQLite backend does not enforce
  the caps" expects parity. A silent new failure mode must be loud and
  typed.
- **Black (risk).** A new `RunState` variant breaks an exhaustive match.
  The crate is pre-1.0, and the changelog says so. Byte measures differ:
  Postgres uses `pg_column_size`, SQLite the stored JSON length.
- **Yellow (value).** Loop workflows (agent sessions, pollers) become
  bounded. Replay stays O(one run). A runaway loop fails instead of
  slowing forever.
- **Green (options).** B2, B4 and a history-bloat warning are follow-ups.
- **Blue (process).** Red tests first, then the smallest green change,
  then a refactor that shares the seal path. Then review and the AC check.

---

## 1. Design

### 1.1 Continue-as-new

On `ContinuedAsNew { input, new_workflow_type: None }`, one transaction:

1. Append the drain's bookkeeping events. Drop its dispatches.
2. Append `WorkflowContinuedAsNew { new_exec_id, input }` to the old run.
3. Delete the old run's pending tasks and unfired timers.
4. Seal the old run `CONTINUED_AS_NEW`.
5. Insert the successor: same `workflow_name`, same `workflow_id`.
6. Append its `WorkflowStarted { input }`.
7. Move the old run's undelivered signals to the successor.

The drive returns `RunState::ContinuedAsNew(successor)`. A cross-type
continuation seals `FAILED` with feature `ContinueAsNew.new_workflow_type`.

### 1.2 History caps

`run_workflow_*` gets the configured caps. After the handler, and only for
a `Completed`, an author `Failed` or a `Suspended` outcome:

- Event cap: the durable count after this decision's appends, before its
  terminal event, reaches the cap.
- Byte cap: the stored JSON bytes at decision start reach the cap.

A breach rolls the decision back. A new transaction appends
`WorkflowFailed` with `error` = the reason JSON, `error_type` = the reason
tag and `non_retryable = true`. It sets the run `FAILED` and removes its
pending tasks, unfired timers and staged signals.

## 2. Test plan (red first)

| Test | AC |
|------|----|
| A loop workflow continues as new twice and completes in the third run. | 1 |
| A signal staged before the hand-off is consumed by the successor. | 2 |
| A run past a lowered event cap fails with `HistoryCapExceeded`. | 3 |
| A run past a lowered byte cap fails with `HistoryBytesCapExceeded`. | 3 |
| A continuing run is exempt from the cap. | 3 |
| Both histories replay on `WorkflowReplayer`. | 4 |
| A re-drive of the sealed run returns the same successor. | R3 |
| A cross-type continuation fails by name. | B4 |
| The reason JSON is byte-identical to `DeadLetterReason`. | B9 |
