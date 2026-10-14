# Design — Issue #2028: SQLite signal by workflow id, idempotency keys, signal-with-start

On SQLite, `send_signal` takes only an `ExecutionId`. A caller with a
business `workflow_id` must find the run. A retried delivery stages the
signal twice. No call can start a run or signal it in one step.

This change adds three calls to `SqliteRuntime`:

- `send_signal_idempotent(exec, name, payload, key)`
- `send_signal_by_workflow_id(workflow_name, workflow_id, name, payload, key)`
- `signal_with_start(SignalWithStart { .. })`

**One additive migration: `harvest_signals.idempotency_key` and a partial
unique index. No new `WorkflowEvent` variant.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `send_signal_as_of` stages a row in `harvest_signals`. It checks that the
  run exists and is not terminal, then the payload cap.
- A promise signal (issue #1985) dedupes by name through
  `store::has_signal`. The core `settlement_idempotency_key` gives the key
  rules on every path.
- Postgres keys a signal on `(workflow_exec_id, idempotency_key)` with a
  partial unique index. The insert runs before the state check, so a keyed
  retry to a terminal run returns `Ok(false)`, not an error.
- Postgres `signal_with_start` and `resolve_and_signal_by_workflow_id` also
  dedupe across the whole `(workflow_name, workflow_id)` chain. A promise
  key dedupes per execution only.
- Postgres resolves a business id active-first, else the newest terminal
  run. A terminal run is `NotRunning`.
- The Postgres signal-with-start matrix differs from the plain start: under
  `AllowDuplicate` and `AllowDuplicateFailedOnly`, a terminal prior gets a
  fresh run, so the signal can land.
- SQLite deletes undelivered signal rows when it seals a run (cancel,
  terminate, failure, reuse-policy replace). A deleted keyed row cannot
  dedupe a later retry.
- A continue-as-new moves undelivered rows to the successor. The successor
  keeps the `workflow_id`.
- The single writer holds the only write handle. One transaction is atomic
  against every other write.

### 0.2 Brainstorm — how can SQLite meet the three criteria?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add `idempotency_key` to `harvest_signals` with a partial unique index on `(exec_id, idempotency_key)`. | **Adopted.** It matches Postgres. The database enforces it. |
| B2 | Dedupe in memory. | Rejected. A restart loses it. |
| B3 | Probe the key, then insert. Rely on the index as the backstop. | **Adopted.** The probe gives the result before the state check, as on Postgres. |
| B4 | `INSERT OR IGNORE` only. | Rejected. The cap and state checks must run between the probe and the insert. |
| B5 | Dedupe across the `(workflow_name, workflow_id)` chain for by-id and signal-with-start. | **Adopted.** A retry after a continue-as-new or a fresh start must not stage again. |
| B6 | Keep keyed undelivered rows when a run seals. | **Adopted.** They are the dedupe record. Unkeyed rows are still deleted. |
| B7 | Share one start matrix between `start_workflow_*` and `signal_with_start`. Use a mode flag for the terminal-prior escalation. | **Adopted.** One code path, two documented tables. |
| B8 | Return a typed outcome: `SignalWithStartOutcome { exec_id, started_fresh, signal_delivered }`. | **Adopted.** It matches the Postgres outcome. |
| B9 | By-id returns `SignalDelivery { exec_id, delivered }`. | **Adopted.** The caller learns the run it reached. |
| B10 | Back-fill the key of old promise rows with the name. Drop `has_signal`. | **Adopted.** One dedupe rule for every signal. |

### 0.3 Reverse brainstorm — how can a duplicate get through?

| # | Failure | Guard |
|---|---------|-------|
| R1 | A retry arrives before the first row is delivered. | The probe finds the row. The unique index rejects a second insert. |
| R2 | A retry arrives after the drive delivers the row. | Delivered rows stay. The probe ignores `delivered`. |
| R3 | A retry arrives after the run completes. | The probe runs before the terminal check. It returns `Ok(false)`. |
| R4 | A retry arrives after a cancel or terminate deleted the row. | Keyed rows are not deleted (B6). |
| R5 | A signal-with-start retry arrives after the first run completes. The matrix starts a fresh run and stages again. | The chain probe (B5) runs before the start decision. |
| R6 | A by-id retry arrives after a continue-as-new. It resolves the successor. | The chain probe finds the row on the predecessor. |
| R7 | A failure between the start and the stage leaves a run with no signal. | One transaction. An error drops it uncommitted. |
| R8 | An empty key collides across unrelated signals. | An empty key is no key, as on Postgres. |
| R9 | A normal signal takes a `harvest.promise:` key and blocks the real settlement. | `settlement_idempotency_key` rejects it as `InvalidSignal`. |
| R10 | An old file has duplicate promise rows. The unique index build fails on open. | The back-fill uses `UPDATE OR IGNORE`. The index is built first. |
| R11 | A blank `workflow_id` never matches its chain, so keyed retries start new runs. | Signal-with-start and by-id reject a blank id as `BlankWorkflowId`. |
| R12 | By-id signals a terminal run while a live run exists for the key. | Resolution is active-first. |

### 0.4 Six thinking hats

- **White (facts).** See 0.1. Postgres owns the contract. SQLite has one
  writer, so a transaction gives the atomicity that Postgres gets from
  locks.
- **Red (feeling).** A webhook author wants one call that is safe to
  retry: `rt.signal_with_start(..)` with a key.
- **Black (risks).** B6 leaves rows that no drive reads. They are small and
  bounded by the caller's keys. The migration adds an index on open. Two
  new error variants grow the enum. A `COMPLETED` prior that
  signal-with-start replaces seals `CONTINUED_AS_NEW`, as the plain-start
  replace does, so `outcome()` no longer returns its output.
- **Yellow (benefits).** Parity with Postgres for webhook ingress. The
  database, not the caller, enforces the key. The promise path loses its
  special case.
- **Green (alternatives).** A key table separate from `harvest_signals`.
  It doubles the writes and needs its own retention. Rejected.
- **Blue (process).** Red: failing tests against stubs. Green: the
  migration, the store helpers and the three calls. Refactor: share the
  start matrix and the keyed stage. Then docs, the changelog fragment, the
  comment-hygiene audit, clippy and a multi-angle review.

---

## 1. Behaviour

### 1.1 Keyed stage (shared)

`stage_keyed(exec, name, payload, key)`, inside the caller's transaction:

1. A key that already exists for `exec` returns `Ok(false)`.
2. A terminal run is `WorkflowNotRunning`.
3. An oversized payload is `PayloadTooLarge`.
4. Insert the row with the key. Return `Ok(true)`.

An empty key is no key. `settlement_idempotency_key` runs first on every
path: a promise uses its name as the key, and other signals cannot use a
promise key.

### 1.2 `send_signal_idempotent`

An unknown id is `ExecutionNotFound`. Then the keyed stage. `send_signal`
calls it with no key.

### 1.3 `send_signal_by_workflow_id`

1. A blank `workflow_id` is `BlankWorkflowId`.
2. A key already used in the `(workflow_name, workflow_id)` chain returns
   `SignalDelivery { delivered: false }` for the run that holds it. A
   promise key skips this step.
3. Resolve the key: the live run, else the newest run. None is
   `WorkflowIdNotFound`.
4. The keyed stage on that run. A terminal run is `WorkflowNotRunning`.

### 1.4 `signal_with_start`

One transaction:

1. Checks before the transaction: a registered workflow, a non-blank id,
   the input cap and the key rules.
2. The chain probe, as in 1.3 step 2. A hit returns
   `started_fresh: false, signal_delivered: false`.
3. The start matrix below.
4. The keyed stage on the run that step 3 gives.

| Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
|---|---|---|---|---|
| none | start + signal | start + signal | start + signal | start + signal |
| `RUNNING` | signal | `AlreadyExists` | signal | cancel + start + signal |
| `COMPLETED` / `FAILED` / `CANCELLED` | start + signal | `AlreadyExists` | start + signal | start + signal |
| `TERMINATED` / `CONTINUED_AS_NEW` | start + signal | start + signal | start + signal | start + signal |

"Start" over a prior seals the prior `CONTINUED_AS_NEW`, as the plain-start
replace does. The next drive appends the signal before the body runs
(issue #2020).

## 2. Test plan

Unit tests: `schema.rs` (migration, back-fill) and `queue.rs` (a seal keeps
keyed rows). Integration tests:
`autumn-harvest-sqlite/tests/integration/signal_with_start.rs`.

| Test | AC or risk |
|------|------|
| A duplicate keyed signal stages once, and the run sees it once. | AC1, R1 |
| A keyed retry after delivery stages nothing. | AC1, R2 |
| A keyed retry after completion is `Ok(false)`. | AC1, R3 |
| A keyed retry after a terminate is `Ok(false)`. Unkeyed rows go. | AC1, R4 |
| Distinct keys and unkeyed signals each stage. An empty key is no key. | R8 |
| A key is scoped to its execution. | — |
| A reserved promise key on a normal signal is `InvalidSignal`. | R9 |
| The database rejects a duplicate key. | AC1, R1 |
| Signal-with-start starts a run, and the run receives the signal. | AC2 |
| Signal-with-start signals a live run and starts nothing. | AC2 |
| An injected stage failure rolls back the start and the prior's cancel. | AC2, R7 |
| A keyed signal-with-start retry stages once, also after completion. | AC1, AC2, R5 |
| The matrix: terminal prior, `RejectDuplicate`, `TerminateIfRunning`, `AllowDuplicateFailedOnly`. | AC2 |
| A blank id, an unknown workflow, and an oversized payload start nothing. | AC2, R11 |
| A repeated promise settlement through signal-with-start stages once. | R9 |
| By-id reaches the live run. | AC3 |
| By-id rejects a completed, a cancelled and a terminated run. | AC3 |
| By-id with an unknown id or name is `WorkflowIdNotFound`. | AC3 |
| By-id after a continue-as-new reaches the successor. A keyed retry stages nothing. | AC3, R6, R12 |
