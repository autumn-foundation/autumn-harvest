# Design — Issue #2029: SQLite timers, mutex, local activities, timeout and retry

The SQLite backend rejected five primitives. Each rejection sealed the run
`FAILED` or panicked at registration. This change supports all five:

1. Cancellable timers (`ctx.start_timer`, `TimerHandle`, issue #768).
2. The workflow mutex (`ctx.mutex(key).acquire()`, issue #691).
3. Local activities (`#[activity(local = true)]`).
4. Workflow `execution_timeout` (issue #243).
5. Workflow-level `retry_policy` (issue #523).

Each primitive records the same core events as Postgres. No new
`WorkflowEvent` variant is added.

---

## 0. Planning record

### 0.1 Facts found before the plan

- **Timers.** `start_timer` pushes `ArmTimer { for_await: false }`. Postgres
  appends `TimerStarted` and inserts no row. A parked `await_fire` pushes
  `ArmTimer { for_await: true }` on every cycle. Postgres inserts a row once
  and keeps its `fires_at` on later cycles. `CancelTimer` appends
  `TimerCancelled` and deletes the unfired row. The fire path is the same as
  for `ctx.timer`.
- **Mutex.** `acquire` pushes `AcquireMutex` until history holds
  `MutexGranted { key, lock_seq, acquired_at }`. The matcher is positional.
  Postgres requires the acquire to be the only suspending command in its
  batch. `MutexGuard` drop pushes `ReleaseMutex { key, lock_seq }` on every
  replay, so a release must be fenced and idempotent. A terminal run releases
  every lock it holds and leaves every queue it waits in.
- **Local activities.** The context pushes `RunLocalActivity`. Postgres
  appends `LocalActivityScheduled`, runs the body inline, and appends
  `LocalActivityFailed` per failed attempt. It ends with
  `LocalActivityCompleted`, or with `LocalActivityFailed` and
  `LocalActivityExhausted`. A local activity cannot share a batch with a
  durable awaitable (issue #950).
- **Timeout.** Postgres stores `deadline_at` at start. A sweeper appends
  `WorkflowExecutionTimedOut { deadline, timed_out_at }` and seals the run
  `TIMED_OUT`. The error is `timeout: WorkflowExecution for <name>`.
- **Retry.** On an author failure, Postgres appends `WorkflowFailed` and seals
  the run `FAILED`. It then starts a successor and appends
  `WorkflowRetryScheduled { retry_exec_id, attempt, fire_at }`. The
  successor's `workflow_id` is its own exec id. Non-determinism, a handler
  panic, an engine error, a timeout and a cancel never retry.
- **SQLite today.** `apply_commands` and the terminal drain reject all four
  commands. `register_workflow` panics on `execution_timeout` and
  `retry_policy`. `register_activity` panics on `local = true`.

### 0.2 Brainstorm — how can each primitive work on one writer?

| # | Idea | Verdict |
|---|------|---------|
| T1 | Apply `ArmTimer` and `CancelTimer` in the cycle transaction, in command order. A fresh arm appends `TimerStarted`. An await arm inserts a row only when no unfired row exists. | **Adopted.** It matches the Postgres event order and keeps the first deadline. |
| T2 | Insert the row at the fresh arm. | Rejected. An unawaited timer could then fire while the body waits on something else. |
| M1 | Two tables: a lock row per key and a FIFO waiter queue. A grant appends `MutexGranted` in the cycle transaction. | **Adopted.** One writer makes the check and the grant atomic. |
| M2 | Leases and a reclaim scanner, as on Postgres. | Rejected. A holder that crashes resumes on reopen with its lock. The terminal sweep frees every other lock. |
| M3 | Wake a waiter with an event on its history. | Rejected. Postgres appends no wake event. The fleet poll drives every run, so a waiter re-drives on the next pass. |
| L1 | Append `LocalActivityScheduled` in the cycle transaction. Run the body after the commit. Append each attempt result in its own transaction. | **Adopted.** It is the Postgres order, and a crash resumes from the recorded attempts. |
| L2 | Run a local activity as a regular task. | Rejected. The history would then hold `ActivityScheduled`, which does not replay on Postgres. |
| D1 | Store `deadline_at` at start. The drive checks it at the start of each cycle. | **Adopted.** The issue proposes it. The fleet poll reaches a blocked run too. |
| D2 | A background timer thread. | Rejected. The backend has no threads and no push wake. |
| R1 | In the failure transaction, seal the run `FAILED` and insert the successor with a `start_at`. The drive does not run it before that time. | **Adopted.** It is one atomic step, as on Postgres. |
| R2 | Retry in place under the same exec id. | Rejected. The failed history must stay terminal to replay. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| H1 | Each replay re-emits the await arm and resets `fire_at`, so the timer never fires. | The insert runs only when no unfired row exists. A test pins the deadline over several drives. |
| H2 | A cancelled arm stays "pending", so a later `ctx.timer` on the same id never arms. | `pending_timer_arms` subtracts `TimerCancelled`. |
| H3 | A replayed `ReleaseMutex` frees a lock that a later grant owns. | The release deletes only the row with the same holder and `lock_seq`. |
| H4 | A holder ends without a release, so its waiters wait forever. | Every terminal state setter releases the run's locks and waiter rows in the same transaction. |
| H5 | A release does not count as progress, so `run_until_idle` stops before the waiter runs. | A release that deletes a row reports progress. |
| H6 | A crash after `LocalActivityScheduled` runs the body twice. | At-least-once, as on Postgres. The context passes the recorded failures, so the loop resumes at the next attempt. |
| H7 | A failing local activity sleeps on a runtime without a timer. | The backoff sleep uses Tokio when a Tokio runtime is present. Otherwise it blocks the thread. |
| H8 | A timed-out run keeps a timer, a task or a lock. | The timeout seal deletes the open rows and releases the locks. |
| H9 | A retry loops forever. | `workflow_attempt` is frozen on the row and grows by one per retry. The successor stops at `max_attempts`. |
| H10 | A retry runs before its backoff. | The successor row holds `start_at`. The drive returns `WaitingTimer` until then. |
| H11 | An old database file lacks the new columns. | `schema::migrate` adds each column. Each one is nullable or has a default. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Three acceptance criteria: a test per
  primitive that completes, a replay of each history on the core
  `WorkflowReplayer`, and `docs/sqlite-backend.md` §11 without the shipped
  items.
- **Red (feel).** The mutex and the retry change who drives what. They must
  stay simple to reason about on one writer.
- **Black (risks).** Section 0.3. Five features in one change make a large
  diff. Each feature is a separate section of code and a separate test file.
- **Yellow (value).** Code that uses any of these primitives can now move
  from Postgres to SQLite without change.
- **Green (options).** Section 0.2. Follow-ups: per-attempt cancellation of a
  local activity, and a lease for a holder that never resumes.
- **Blue (process).** Red tests first. Then one primitive at a time. Then
  docs. Comment hygiene, clippy and the full crate suite gate the push.

---

## 1. Cancellable timers

`apply_commands` handles the commands in batch order:

- `ArmTimer { for_await: false }` appends `TimerStarted`. It inserts no row.
- `ArmTimer { for_await: true }` inserts a `harvest_timers` row at
  `now + duration` when no unfired row exists. It appends no event.
- `CancelTimer` appends `TimerCancelled` and deletes the unfired row.

A due row fires through the `ctx.timer` path. A terminal batch records a
fresh arm and a cancel, and ignores an await arm.

## 2. Workflow mutex

Three tables hold the state: `harvest_mutex_locks`, `harvest_mutex_waiters`
and the `harvest_mutex_seq` counter.

- `AcquireMutex` enqueues the run. When the key is free and the run is first
  in the queue, it mints a `lock_seq`, inserts the lock row, removes the
  waiter row and appends `MutexGranted`. Otherwise the run waits, and the
  drive returns `RunState::WaitingMutex(key)`.
- The acquire must be the only suspending command in its batch, as on
  Postgres. Another awaitable in the batch seals the run `Unsupported`.
- `ReleaseMutex` deletes the lock row that matches the run and `lock_seq`.
- Each terminal state setter releases the run's locks and deletes its waiter
  rows.

## 3. Local activities

`register_activity` accepts `local = true`. A local activity that declares
`schedule_to_close` panics at registration, because Postgres rejects it too.

1. The cycle transaction appends `LocalActivityScheduled` at the command's
   position, unless the context reports it as already scheduled.
2. After the commit, the drive runs the attempts. It starts after the
   recorded failures.
3. Each attempt appends `LocalActivityCompleted`, or `LocalActivityFailed`.
   A terminal failure also appends `LocalActivityExhausted`.
4. The per-attempt budget is `start_to_close`, capped at 60 seconds, as on
   Postgres. It is checked after the body returns.

A batch with more than one local activity, or with a durable awaitable,
seals the run `Unsupported` (issue #950).

## 4. Execution timeout

`start` stores `execution_timeout_ms` and `deadline_at`. A continue-as-new
or a retry successor re-anchors the deadline at its own start. Each cycle
first compares `now` with `deadline_at`. A run past its deadline gets
`WorkflowExecutionTimedOut`, ends `TIMED_OUT`, and loses its open rows and
locks. The drive returns `RunState::TimedOut(error)`. The context gets the
deadline, so `ctx.deadline()` reports it.

## 5. Workflow retry

`start` freezes the policy and `workflow_attempt = 1` on the row. An author
failure retries when the policy allows it. One transaction then:

1. appends `WorkflowFailed` and seals the run `FAILED`;
2. inserts the successor with `workflow_attempt + 1`, the same input and
   policy, and `start_at = now + backoff`;
3. moves every signal row to the successor as undelivered, as on Postgres;
4. appends `WorkflowRetryScheduled`.

The drive returns `RunState::Failed`. `SqliteRuntime::successor` returns the
retry run. The fleet poll drives it once `start_at` passes.

## 6. Tests

One test file per primitive under `autumn-harvest-sqlite/tests/integration/`.
Each test runs a workflow that sealed `UnsupportedFeature` or panicked
before. Each asserts that the run now completes, and replays its history on
`WorkflowReplayer`.
