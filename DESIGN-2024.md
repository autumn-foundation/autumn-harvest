# Design — Issue #2024: SQLite runs the `#[activity]` body with an `ActivityContext`

On SQLite the `#[activity]` async body never runs. The author registers a
second, synchronous closure, and that closure runs instead. This change runs
the macro's own handler on the drive's async runtime, with a real
`ActivityContext`.

**No `WorkflowEvent` change. No Postgres migration.** The SQLite schema gains
two nullable columns on `harvest_tasks`. The core gains one hidden constructor.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `ActivitySpec.body` is a `Box<dyn Fn(Value) -> Result<Value, String>>`.
  `worker::drain_ready` calls it inline, inside `catch_unwind`.
- `drain_ready` is synchronous. Its only caller, `drive_one_cycle_inner`, is
  `async`. So the drive already runs on the caller's Tokio runtime.
- `ActivityInfo::handler` is an `ActivityHandlerFn`. It returns a boxed
  `Send` future that borrows the `ActivityContext`.
- Every `ActivityContext` constructor is `pub(crate)` or gated on
  `testing`. The SQLite crate builds the core without `testing`, so it
  cannot build a context today.
- Without a heartbeat sink, `ctx.heartbeat(..)` returns `Config` and
  `ctx.heartbeat_details()` returns `Config`. A portable activity that
  checkpoints would fail on SQLite.
- The Postgres worker fills `info()` from the task row: `attempt` is
  1-based, `queue_name` is the row's queue, `task_id` is the row's UUID,
  and `deadline` is the earlier of `started_at + start_to_close` and
  `schedule_to_close_at`.
- Postgres stores the previous attempt's error as
  `parse_error_payload_full(&error).message`. `previous_failure()` reads it.
- SQLite `harvest_tasks.task_id` is already a v4 UUID string. The `queue`
  column already holds the command's queue.
- SQLite enforces `start_to_close` after the body returns, and records it as
  terminal. Postgres retries it per policy. That gap is out of scope here.

### 0.2 Brainstorm — how can SQLite run the real body?

| # | Idea | Verdict |
|---|------|---------|
| B1 | `register_activity(&info)` stores `info.handler` and awaits it in the drain. | **Adopted.** It is the issue's proposed direction. |
| B2 | Keep the closure form under a new name, `register_activity_with_body(&info, body)`. | **Adopted.** About 50 call sites use it. They keep the audited defaults and change one name. |
| B3 | Keep `register_activity_raw` and `ActivitySpec::new` unchanged. | **Adopted.** The issue asks for it. |
| B4 | Add a hidden core constructor, `ActivityContext::new_embedded`. | **Adopted.** It is the smallest public surface. It sets the idempotency key from the identity, so the two never disagree. |
| B5 | Enable the `testing` feature from the SQLite crate and use `new_test_*`. | Rejected. It ships test code in production builds. |
| B6 | Persist heartbeats. Drive the body and a bounded channel in one `poll_fn`. Write the latest payload to the task row. | **Adopted.** No extra task, no `select!`, and a retry or a reopen reads the checkpoint back. |
| B7 | Spawn the body on a Tokio task. | Rejected. The body borrows the context, and a spawn needs `'static`. |
| B8 | Store the previous attempt's error on the task row as `last_error`. | **Adopted.** The audit table has no activity id, so two same-name activities would mix. |
| B9 | Honor `heartbeat_timeout` now. | Deferred. It needs a preemptive cancel and a retry-per-policy path. The SQLite timeout model treats `start_to_close` as terminal, so both belong in one follow-up. Registration still rejects it, by name. |
| B10 | Cancel the body on a drive drop. | Rejected. The token never fires on SQLite. `is_cancelled()` stays `false`, which is true. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A handler panics in construction or in a poll, and the task stays `RUNNING`. | Both are inside `catch_unwind`. A panic becomes the typed `HandlerPanic` envelope, as before. Test. |
| R2 | A heartbeat write fails and kills a good attempt. | The write logs a warning and the attempt goes on. Postgres does the same. |
| R3 | The last heartbeat before an `Err` is lost, so the retry restarts from zero. | The `poll_fn` drains the channel after each poll, also on the final one. Test. |
| R4 | A body that heartbeats in a tight loop fills the channel and deadlocks. | The drain frees capacity in the same poll. The sender's waker re-polls the body. |
| R5 | `info().attempt` is off by one against Postgres. | SQLite counts consumed attempts. The context reports `attempt + 1`. Test on attempt 1 and 3. |
| R6 | The drive future stops being `Send`. | Everything held across the await is `Send`. Clippy `future_not_send` gates it. |
| R7 | An old database lacks the new columns and every claim fails. | `schema::migrate` adds them, additive and idempotent. Test. |
| R8 | The rename breaks downstream callers silently. | It is a compile error, not a behavior change. The changelog names the new method. |
| R9 | The same fn gives different `info()` on the two backends. | One shared fixture runs on both. A core test runs it on Postgres. A SQLite test runs it here. Both assert the same JSON. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The AC is three bullets: one fn on both
  backends, `info()` parity, and raw-closure tests still pass.
- **Red (feelings).** A rename of `register_activity` feels risky. It is
  still the right name for the default path. Authors expect the info-based
  call to run their code.
- **Black (risks).** Section 0.3. The largest risk is a hung or panicking
  body. The panic path is covered. A hung body hangs the drive, exactly as a
  hung synchronous closure does today.
- **Yellow (benefits).** One implementation per activity. Retry-aware code
  (`attempt()`, `previous_failure()`, `is_last_attempt()`) works. Checkpoints
  survive a retry and a crash. Idempotency keys match Postgres for the same
  `activity_id`.
- **Green (alternatives).** B5, B7 and B9. A future `with_state` on the
  runtime could fill `ctx.state::<T>()`. It is not needed for the AC.
- **Blue (process).** Red: tests that do not compile or fail. Green: core
  constructor, then the SQLite path. Refactor: docs, examples, call sites.
  Then a review from several angles, then the AC evidence.

---

## 1. Design

### 1.1 Core

`ActivityContext::new_embedded(state, identity, heartbeat_tx,
heartbeat_details)` is public and `#[doc(hidden)]`. It is not gated. It
attaches the heartbeat sink and seeds the checkpoint. It also sets the
idempotency key from `identity.activity_id`.

### 1.2 SQLite registration

| Method | Body | Audit |
|--------|------|-------|
| `register_activity(&info)` | `info.handler`, awaited with a context | yes |
| `register_activity_with_body(&info, closure)` | the closure | yes |
| `register_activity_raw(name, spec)` | `ActivitySpec::new` closure | no |

### 1.3 The drain

`drain_ready` is now `async`. For a handler body it builds the context,
then awaits one `poll_fn`. Each poll runs the body under `catch_unwind`,
then drains the heartbeat channel and writes the latest payload. A closure
body runs inline, as before.

### 1.4 `info()` values

| Field | SQLite value | Postgres value |
|-------|--------------|----------------|
| `execution_id` | the run's id | same |
| `workflow_id` | `harvest_executions.workflow_id` | same |
| `workflow_type` | the workflow name | same |
| `activity_type` | the task's activity name | same |
| `activity_id` | the scheduled `ActivityExecId` | same |
| `attempt` | consumed attempts + 1 | same |
| `max_attempts` | the frozen cap | same |
| `is_local` | `false` | same |
| `queue_name` | the task's queue | same |
| `task_id` | the task row's UUID | same |
| `deadline()` | min(claim + `start_to_close`, `schedule_to_close_at`) | same rule |

### 1.5 Schema

`harvest_tasks.heartbeat_details TEXT` and `harvest_tasks.last_error TEXT`.
Both are nullable and added by `schema::migrate`.

---

## 2. Tests

- `tests/integration/activity_context.rs` (SQLite): the body runs; `info()`
  values; attempt and `previous_failure` on a retry; a heartbeat checkpoint
  survives a retry and a reopen; a panic is contained; `deadline()`; no
  cancellation.
- `tests/integration/portable_activity.rs` (SQLite) and
  `autumn-harvest/tests/integration/sqlite_portable_activity_tests.rs`
  (Postgres) run one shared fixture and assert the same report.
- `examples/portable_activity.rs` shows the fn on SQLite.
- Every existing raw-closure test passes unchanged.
