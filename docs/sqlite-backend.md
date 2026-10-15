# The SQLite backend (edge / local-first)

`autumn-harvest-sqlite` is an **embedded, single-writer** persistence backend
for the `autumn-harvest` durable-workflow engine. It gives you the full
deterministic replay engine — activities, durable timers, signals, crash
recovery — inside your own binary, backed by one SQLite file, with **no server
to run and no Docker**.

This guide is task-oriented: it walks through building a small order-processing
runtime from an empty project to a crash-recoverable, signal-driven workflow. The
canonical API/contract reference is the crate-level rustdoc
(`cargo doc --open -p autumn-harvest-sqlite`); this guide shows how to use it.

For *why* this is a separate crate rather than a `StorageBackend` trait in core
— and which capabilities the single-writer model deliberately gives up — see the
R&D decision record [`docs/rnd/sqlite-feasibility.md`](rnd/sqlite-feasibility.md).

## Contents

1. [What it is, and when to reach for it](#1-what-it-is-and-when-to-reach-for-it)
2. [Adding the dependency](#2-adding-the-dependency)
3. [Defining a workflow and an activity](#3-defining-a-workflow-and-an-activity)
4. [Opening a runtime — file vs in-memory](#4-opening-a-runtime--file-vs-in-memory)
5. [Registering workflows and activities](#5-registering-workflows-and-activities)
6. [Starting a workflow](#6-starting-a-workflow)
7. [The drive model](#7-the-drive-model)
8. [Signals, queries and updates](#8-signals-queries-and-updates)
9. [Durability and crash recovery](#9-durability-and-crash-recovery)
10. [The single-writer / single-server contract](#10-the-single-writer--single-server-contract)
11. [v0.1 non-goals and follow-ups](#11-v01-non-goals-and-follow-ups)
12. [Runnable examples](#12-runnable-examples)

---

## 1. What it is, and when to reach for it

The hard, valuable part of `autumn-harvest` — the deterministic replay engine
(`run_workflow`, `WorkflowEvent`, the history matcher, `WorkflowContext`) — is
backend-neutral. The SQLite backend reuses that core **wholesale** and
reimplements only *persistence*: the event store, the task queue + durable
timers, and the worker pass, on embedded SQLite via `rusqlite` (`bundled`). It
depends on `autumn-harvest` with `default-features = false`, so no
Diesel/Postgres is pulled in.

A history written here is byte-identical, per event, to one the Postgres backend
would write, so it replays unchanged on the core `WorkflowReplayer`.

**Reach for the SQLite backend when:**

- You want a durable workflow engine **embedded in your binary** — edge devices,
  desktop/CLI apps, local-first tools — with nothing external to deploy.
- You run a **single server** (one writer process, one database file).
- You want a full runtime in **tests and demos** with zero setup (in-memory).

**Reach for the Postgres core ([`autumn-harvest`](../autumn-harvest)) instead
when** you need distributed / multi-worker execution, multi-server crash
recovery, high throughput, `LISTEN`/`NOTIFY` push wake-ups, schedules, DAGs, the
management API, worker sessions, retention, or sharding.

| | SQLite backend | Postgres core |
| --- | --- | --- |
| Deployment | Embedded, no server | Postgres server(s) |
| Writers | **One** (single process) | Many (a fleet) |
| Wake-ups | Polling (you drive it) | `LISTEN`/`NOTIFY` push |
| Crash recovery | Single-process reopen | Multi-server heartbeat/reclaim |
| Throughput | Modest, single-writer | High, horizontally scaled |
| Extras (schedules, DAGs, mgmt API, sharding) | v0.1 non-goals | Yes |

---

## 2. Adding the dependency

```toml
[dependencies]
autumn-harvest = { version = "0.7", default-features = false }
autumn-harvest-sqlite = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# The drivers are async; use any runtime. tokio is the simplest.
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread"] }
```

You do **not** need the `autumn-harvest` `db` feature — the SQLite backend uses
the backend-neutral core only. `rusqlite` is pulled in with the `bundled`
feature, so there is no system SQLite dependency.

---

## 3. Defining a workflow and an activity

Workflows and activities are written with the ordinary `#[workflow]` /
`#[activity]` macros from the core prelude — the SQLite backend does not change
the authoring surface.

```rust
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order { id: String, item: String, qty: u32 }

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_inventory_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("order {} confirmed — {reservation}", order.id))
}

#[activity]
async fn reserve_inventory(ctx: &ActivityContext, order: Order) -> Result<String, String> {
    // Stable across retries and re-runs: safe to send to a downstream API.
    let key = ctx.idempotency_key().map_err(|e| e.to_string())?;
    Ok(format!("reserved {}x {} ({key})", order.qty, order.item))
}
```

> **The `#[activity]` body runs (issue #2024).** `register_activity(&info)` runs
> the async body itself, on the drive's async runtime, with a real
> `ActivityContext`. The same fn runs unchanged on this backend and on Postgres.
> `ctx.info()` reports the same values as the Postgres worker for one process.
> They are the run, the activity, the 1-based `attempt`, `max_attempts`, the
> task queue and the task row id. `ctx.heartbeat(..)` stores the latest payload on the task
> row, so a retry or a reopen resumes from it with `ctx.heartbeat_details()`.
> `ctx.previous_failure()` is the previous attempt's error. The cancellation
> token never fires here.
>
> Some values differ from Postgres by design:
>
> - With no declared `retry`, this backend runs one attempt, so
>   `max_attempts()` is `1`. Postgres applies its engine default.
> - A re-run after a crash, or after a dropped drive, keeps its `attempt`
>   number. Postgres counts the lost attempt.
> - `ctx.state::<T>()` is always `None`, `ctx.headers()` is empty, and
>   `ctx.metrics()` records nothing.
> - `heartbeat_timeout` is rejected at registration. No scanner times out a
>   silent attempt here.

---

## 4. Opening a runtime — file vs in-memory

```rust
use autumn_harvest_sqlite::SqliteRuntime;

// Durable, restart-safe: reopening the same path resumes in-flight runs.
let mut rt = SqliteRuntime::open("workflows.db")?;

// Ephemeral: a fresh, empty, private database each call. Great for tests/demos.
let mut rt = SqliteRuntime::open_in_memory()?;
```

- Use **`open(path)`** for anything that must survive a process restart. Opening
  applies the schema idempotently and reclaims any task stranded `RUNNING` by a
  previous crash (see [§9](#9-durability-and-crash-recovery)). It first takes the
  single-writer lock, so a second open of a held file fails with
  `SqliteError::DatabaseLocked` (see [§10](#10-the-single-writer--single-server-contract)).
- Use **`open_in_memory()`** for tests, demos, and throwaway runs. Each call is a
  brand-new database and is **not** reopen-safe — dropping the runtime discards
  all state.

Both return `SqliteResult<SqliteRuntime>` and are synchronous (only the *drivers*
are async).

---

## 5. Registering workflows and activities

Registrations live in process memory, not the database, so **every runtime
(including a reopened one) must re-register** its handlers.

```rust
rt.register_workflow(&process_order_info());
rt.register_activity(&reserve_inventory_info());
```

- **`register_workflow(&WorkflowInfo)`** retains only the workflow's `name` +
  `handler`. If the `#[workflow(...)]` attributes declare an
  execution/admission-affecting feature this v0.1 backend cannot honor
  (`execution_timeout`, a workflow-level `retry_policy`, `concurrency`,
  `debounce`, `batch`, `throttle`, a raised `max_input_bytes`), registration
  **panics at setup time** naming the feature — never a silently-dropped setting.
- **`register_activity(&ActivityInfo)`** is the default. It runs the
  `#[activity]` body with an `ActivityContext`. It honors the
  declared `#[activity(...)]` defaults it can (`schedule_to_close`, the full
  `retry` policy — `max_attempts` + backoff + non-retryable classification —
  `start_to_close`, `queue`) and **panics** for one it cannot
  (`heartbeat_timeout`, `schedule_to_start`, `circuit_breaker`, `rate_limit_*`,
  `max_concurrent`, `is_local`, …). This keeps a declared setting from taking
  silently-wrong effect.
- **`register_activity_with_body(&ActivityInfo, body)`** runs the same audit,
  but runs a synchronous `Fn(Value) -> Result<Value, String>` closure in place of
  the `#[activity]` body. The closure gets no `ActivityContext`. Use it to stub an
  activity in a test, or to capture process state that the context cannot carry:

```rust
rt.register_activity_with_body(&reserve_inventory_info(), |input| {
    let order: Order = serde_json::from_value(input).map_err(|e| e.to_string())?;
    Ok(serde_json::json!(format!("stub: {}", order.id)))
});
```

- **`register_activity_raw(name, ActivitySpec::new(max_attempts, body))`** is the
  escape hatch — a hand-made body that carries no `ActivityInfo` and runs no
  audit. Use it when you don't have (or don't want) the macro companion:

```rust
use autumn_harvest_sqlite::ActivitySpec;

rt.register_activity_raw(
    "reserve_inventory",
    ActivitySpec::new(3 /* max_attempts */, |input| {
        Ok(serde_json::json!(format!("reserved {input}")))
    }),
);
```

---

## 6. Starting a workflow

```rust
let exec = rt.start_workflow("process_order", serde_json::json!({
    "id": "ORD-1001", "item": "widget", "qty": 3
}))?;
```

`start_workflow(name, input)` appends the `WorkflowStarted` event and returns the
run's `ExecutionId` immediately — **nothing has executed yet**; you drive it in
step 7.

`start_workflow_with_id(name, workflow_id, input)` lets you supply a business
`workflow_id` (what `ctx.info().workflow_id` reports — idempotency-key material,
cross-backend-identical).

> **Idempotent starts by default (issue #1068).** `start_workflow_with_id`
> applies the `AllowDuplicate` reuse policy. A non-blank `workflow_id` that
> matches an existing, non-sealed execution for the same `workflow_name`
> **attaches** to it. The call returns the SAME `ExecutionId`. No second run
> starts. **The new `input` is discarded** — a duplicate delivery, e.g. a
> retried webhook, does not repeat side effects.
>
> A blank or whitespace-only `workflow_id` has no reuse key. It always
> creates a fresh, distinct run.
>
> For a different policy — reject the duplicate, replace a failed run, or
> terminate and restart — use `start_workflow_with_reuse_policy`. It applies
> the full `WorkflowIdReusePolicy` matrix and returns a `StartOutcome`, which
> reports whether the call attached to a prior run or started a fresh one.
> See the rustdoc on both methods for the full matrix
> (`cargo doc --open -p autumn-harvest-sqlite`).

An oversized start input (over the 2 MiB default cap) is rejected with
`SqliteError::PayloadTooLarge` before anything is persisted, matching the core.

---

## 7. The drive model

SQLite has no push notification, so instead of the Postgres worker's
`LISTEN`/`NOTIFY` loop you **drive** the runtime. A drive is a polling loop: drain
all ready tasks, re-run the workflow (replay recorded history, then execute new
commands), persist the resulting events, and repeat until the run is terminal or
blocked on external input. All three drivers are `async`.

| Driver | Scope | Stops when | Returns |
| --- | --- | --- | --- |
| `poll_once().await` | Every running execution, **one cycle each** | after a single pass | `bool` — did anything make progress? |
| `run_until_blocked(exec).await` | **One** execution | that run is terminal or blocked | `RunState` |
| `run_until_idle().await` | **Every** execution, repeatedly | the whole fleet quiesces | `()` |

```rust
use autumn_harvest_sqlite::{RunState, ExecutionOutcome};

// Drive a single run to its next stopping point:
match rt.run_until_blocked(exec).await? {
    RunState::Completed(output) => println!("done: {output}"),
    RunState::Failed(err)       => println!("failed: {err}"),
    RunState::WaitingSignal(name) => println!("blocked on signal `{name}`"),
    RunState::WaitingTimer      => println!("blocked on a durable timer"),
    RunState::Cancelled(reason) => println!("cancelled: {reason}"),
    RunState::Terminated(reason) => println!("terminated: {reason}"),
    RunState::ContinuedAsNew(next) => println!("continued as new; drive {next}"),
    RunState::InProgress        => {} // not returned by run_until_blocked
}

// Or drive the whole fleet to quiescence, then read a stored outcome:
rt.run_until_idle().await?;
match rt.outcome(exec)? {
    ExecutionOutcome::Completed(output) => println!("{output}"),
    ExecutionOutcome::Failed(err)       => println!("{err}"),
    ExecutionOutcome::Running           => println!("still running"),
    ExecutionOutcome::Terminated(state) => println!("terminal ({state}) — superseded, cancelled or terminated"),
}
```

**Which to use:**

- **`run_until_blocked(exec)`** when you care about one specific run and want its
  `RunState` (including *why* it blocked). This is the natural choice right after
  a `start_workflow` or a `send_signal`.
- **`run_until_idle()`** to settle everything — the simplest "process all
  pending work now" call. Follow it with `outcome(exec)` for a pure read.
- **`poll_once()`** for a custom loop — e.g. a background tick where you decide
  the cadence and inspect the `bool` progress flag yourself.


`poll_once()`/`run_until_idle()` drive every execution in a pass even if an
earlier one errors — see [§11](#11-v01-non-goals-and-follow-ups) for what
happens to a rejected execution. `run_until_blocked(exec)` is the one
fail-fast driver, since it already targets a single execution.

`outcome(exec)`, `load_history(exec)`, and `activity_attempts(exec, name)` are
**pure reads** — they never advance a run.

Every call that takes an `exec` id returns `SqliteError::ExecutionNotFound(exec)`
when no execution has that id. This includes `run_until_blocked` and the list
reads. A typo or a stale id gives this one typed error. It never gives an empty
list or a generic `SQLite` error.

Timers use the real wall clock (read once per decision cycle), so a run blocked
on a `ctx.timer(...)` that is not yet due returns `WaitingTimer`; drive it again
after the deadline passes and it proceeds.

### Cancel and terminate

Stop a run with `cancel` (cooperative) or `terminate` (at once). Both take a
reason. A blank reason gets the Postgres default text.

```rust
// Cooperative: the body sees the reason on the next drive.
rt.cancel(exec, "user stopped the session")?;
match rt.run_until_blocked(exec).await? {
    RunState::Cancelled(reason) => println!("cancelled: {reason}"),
    other => println!("not ended yet: {other:?}"),
}

// At once: the body does not run again.
rt.terminate(other_exec, "stuck for an hour")?;
```

**Cancel.** `cancel` stores the request and appends no event. The next drive
replays the body with `ctx.cancellation_reason()` set. The body can check it
and run cleanup activities:

```rust
ctx.execute_activity_raw("step", input, "default").await.map_err(|e| e.to_string())?;
if let Some(reason) = ctx.cancellation_reason() {
    ctx.execute_activity_raw("cleanup", json!(reason), "default")
        .await
        .map_err(|e| e.to_string())?;
    return Ok(json!("cleaned up"));
}
ctx.wait_for_signal("next").await.map_err(|e| e.to_string())
```

The body sees the request from the start of its replay, not from the point
where the request arrived. Put the check after the last await that records
history, as above, just before a signal wait. A check that runs before
recorded commands, such as a check at the top of a loop, diverges on replay.

The run ends `CANCELLED` when one of these occurs:

- The body returns `Ok` or `Err`, or continues as new. The output of an `Ok`
  is not kept, and no successor starts.
- The body blocks on a signal or a timer, and no activity task is left to
  run. A parked await cannot see the request.
- The cycle diverges or deadlocks. The driver discards that cycle.
- The drive meets an error that repeats on every drive: an unregistered
  activity or workflow, or a stuck cycle.
- The run has added 1,000 events since the request. A body that ignores the
  request and keeps working cannot run forever. The budget leaves room for
  about 500 cleanup activities. The budget acts as one more event cap: a
  cycle whose events reach it, or reach a history cap, is discarded.

The seal drops each pending task. Each one first gets a non-retryable
`ActivityFailed`, so a body that awaits it still replays. The error type
is `UnregisteredActivity` for an unregistered activity, and `CancelSealed`
otherwise. These failures must fit the history caps, but not the cleanup
budget. Failures that reach a cap roll back, and the seal then adds only
`WorkflowCancelled`. `terminate` adds no failures, as on Postgres.

So every cancel ends the run. Cleanup can run activities, but it must not
wait on a timer or a signal: that wait ends the run. A pending activity task,
such as a retry in backoff, still runs, and the body sees the request when
that await resolves. A repeat `cancel` keeps the first reason. The request
survives a crash and a reopen. An update replays the body with the pending
request too, so its handlers see `cancellation_reason()`.

**Terminate.** `terminate` ends the run `TERMINATED` in one transaction. The
body does not run again, and `terminate` drops a pending cancel request.

**Both paths:**

- The run gets the core `WorkflowCancelled { reason }` event as its last
  event, as on Postgres. Cleanup events come before it.
- The pending tasks, unfired timers and staged signals are deleted. A signal
  with an idempotency key stays, so its key still dedupes (issue #2028).
- An unknown id returns `SqliteError::ExecutionNotFound`. A terminal run
  returns `SqliteError::WorkflowNotRunning`. Postgres treats a repeat call on
  a terminal run as a no-op, but this backend refuses it.
- `outcome(exec)` reports `ExecutionOutcome::Terminated("CANCELLED")` or
  `ExecutionOutcome::Terminated("TERMINATED")`. The drive returns the reason
  in `RunState::Cancelled` or `RunState::Terminated`.
- `AllowDuplicateFailedOnly` replaces a `CANCELLED` prior, as on Postgres. A
  `TERMINATED` prior frees its `workflow_id` for every policy.
- A cleanup or an `update` that reaches a history cap or the cleanup budget
  ends the run `CANCELLED`, not `FAILED`. An unsupported command or a used-up panic budget during cleanup
  ends the run `FAILED`, as without a cancel.

A cancelled or terminated history replays on the core `WorkflowReplayer` only
when the body checks `is_cancelled()` before its next new command. A body
parked on a signal with no check reports non-determinism. A Postgres history
of the same shape does the same.

### Long-lived runs: `continue_as_new` and the history caps (issue #2023)

Each decision reloads and replays the whole history. A loop workflow, such as
an agent session or a poller, must therefore bound its history. Call
`ctx.continue_as_new(input)`. One transaction then does all of this:

- It appends `WorkflowContinuedAsNew` and seals the run `CONTINUED_AS_NEW`.
- It starts a successor with the same `workflow_name` and `workflow_id`. The
  successor's `WorkflowStarted` carries `input`.
- It appends the staged signals that the hand-off decision replayed to the
  old run (issue #2020). Such a signal stays with the old run, read or not,
  as on Postgres. A backlog past the room the history caps leave was not
  replayed. It moves to the successor, so each signal arrives exactly once.
- It deletes the old run's pending tasks and unfired timers.

`run_until_blocked` returns `RunState::ContinuedAsNew(successor)`. A later
drive of the old run returns the same successor and starts no other run.
`run_until_idle` and `poll_once` drive the successor like any other run.
`start_workflow_with_id` with the same id attaches to the newest run. Send
later signals to the successor, because the old run is terminal.

`continue_as_new` bounds the replay cost of a run, not the database size. This
backend has no retention, so each sealed run keeps its history in the file.

`outcome()` of the old run is `ExecutionOutcome::Terminated("CONTINUED_AS_NEW")`.
`successor(old)` returns the successor id without a drive.
`ctx.should_continue_as_new()` uses the default threshold of 10,000 events. An
event cap at or below it logs a warning, because the cap then fails the run
before the advice turns true.

The #1804 hard caps apply, with the Postgres defaults:

| Cap | Default | Setter | Reason |
| --- | --- | --- | --- |
| Events per run | `50_000` | `set_history_event_hard_cap(Some(n))` | `HistoryCapExceeded { count, cap, workflow_type }` |
| Stored bytes per run | 50 MiB | `set_history_byte_hard_cap(Some(n))` | `HistoryBytesCapExceeded { bytes, cap, workflow_type }` |

`None` removes a cap. A decision that reaches a cap rolls back. The run then
fails, and the drive returns `RunState::Failed` with the reason JSON. The
`WorkflowFailed` event holds the same JSON in `error`. The event is
byte-identical to the one the Postgres worker writes. Parse the JSON as
`HistoryCapReason`. The seal removes the run's pending tasks, unfired timers
and unkeyed staged signals.

A continuing run is exempt, because its successor starts a new history. The
event count includes the decision's own appends. The byte count is the stored
event JSON length when the decision starts. Postgres measures
`pg_column_size` instead, so the two byte counts can differ.

---

## 8. Signals, queries and updates

Deliver a signal from outside with `send_signal`. Read it inside the workflow
with any signal API.

```rust
// Inside the workflow — block until the signal arrives:
let approved: bool = ctx.receive_signal("approve").await.map_err(|e| e.to_string())?;
// (or the untyped `ctx.wait_for_signal("approve")` returning a serde_json::Value)

// From outside — stage the signal, then drive the run forward:
rt.send_signal(exec, "approve", serde_json::json!(true))?;
rt.run_until_blocked(exec).await?;
```

Each drive appends every staged signal to history (as `SignalReceived`)
before the workflow runs, in arrival order. Postgres does the same at task
preparation. So every signal API works here:

- the blocking waits `wait_for_signal` / `receive_signal`;
- the non-blocking reads `drain_signals` / `try_receive_signal`;
- push handlers (`register_signal_handler`), also with `await_condition`;
- the signal-or-deadline waits `wait_for_signal_timeout` /
  `receive_signal_timeout`. They race the signal against a durable deadline
  timer, and arrival time decides the winner.

A push handler runs at the next durable call (a timer, an activity or a signal
read) after it registers. Put such a call between the registration and a read
of its state. A run parked on `await_condition` emits no command, so a drive
before the signal returns `SqliteError::Stuck`. `poll_once` and
`run_until_idle` report it as a per-execution error and still drive the other
runs. The run stays `RUNNING`. Drive it again after `send_signal`.

The history caps bound this ingest. A drive reads staged signals only up to
the room the caps leave. A signal flood thus fails the run at the cap, as on
Postgres, and never loads past it into memory.

A signal that the workflow never reads also reaches history. The strict core
`WorkflowReplayer` then reports `NonDeterminismDetected(EarlyCompletion)`, as
for a Postgres history. A signal that lost a deadline race is exempt. A file
from an earlier release can hold signals staged for a name that the run never
awaited. The first drive after the upgrade appends them to history.

`send_signal` validates the target at the boundary: an unknown execution is
`SqliteError::ExecutionNotFound` and a terminal one is
`SqliteError::WorkflowNotRunning`, so a wakeup no drive could ever consume is
rejected rather than silently lost.

### Keyed signals, signals by workflow id, signal-with-start (issue #2028)

A webhook can deliver the same event twice. Give the signal an idempotency
key, and the run gets it once:

```rust,ignore
// `true`: staged. `false`: this run already holds the key.
let staged = rt.send_signal_idempotent(exec, "approve", json!(true), Some("hook-42"))?;
```

- A partial unique index on `harvest_signals (exec_id, idempotency_key)`
  enforces the key.
- The key probe runs before the state check. A retry after the run ends
  returns `Ok(false)`, not an error, as on Postgres.
- A seal (cancel, terminate, failure, replace) keeps a keyed row, so its key
  still dedupes. Unkeyed rows are deleted, as before.
- The key is scoped to the run that holds the row. A continue-as-new moves an
  undelivered row to the successor. A retry to the old `exec` then returns
  `SqliteError::WorkflowNotRunning`. Use `send_signal_by_workflow_id` for a
  key that spans the chain.
- An empty key is no key, on every path. A normal signal cannot use a
  `harvest.promise:` key. That is `SqliteError::InvalidSignal`.

Signal a run by its business id:

```rust,ignore
let delivery = rt.send_signal_by_workflow_id("order", "order-7", "approve", json!(true), None)?;
// delivery.exec_id is the run that holds the signal.
```

- The call picks the `RUNNING` run of the `(workflow_name, workflow_id)`
  key. A continue-as-new successor keeps the key, so the call reaches it.
- With no running run, the newest run is terminal, and the call returns
  `SqliteError::WorkflowNotRunning`. With no run at all, it returns
  `SqliteError::WorkflowIdNotFound`. A blank id is
  `SqliteError::BlankWorkflowId`.
- The checks run in this order: the blank id, the key rules, the chain key
  probe, the run lookup, then the run state and the payload cap. Postgres
  reports these outcomes as `ByIdSignalOutcome` values. This backend returns
  errors.
- A key that the chain already holds returns `delivered: false`, with the
  `exec_id` of the run that holds it.

Start a run and signal it, or signal the run that exists:

```rust,ignore
let out = rt.signal_with_start(SignalWithStart {
    workflow_name: "order",
    workflow_id: "order-7",
    input: json!({ "sku": "A1" }),
    reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
    signal_name: "approve",
    signal_payload: json!(true),
    idempotency_key: Some("hook-42"),
})?;
// out.started_fresh, out.signal_delivered
```

One transaction holds the key probe, the start decision, any start or
replace, and the signal stage. An error rolls back all of them. The next
drive appends the signal before the body runs.

| Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
|---|---|---|---|---|
| none | start + signal | start + signal | start + signal | start + signal |
| `RUNNING` | signal | `AlreadyExists` | signal | cancel + start + signal |
| `COMPLETED` / `FAILED` / `CANCELLED` | start + signal | `AlreadyExists` | start + signal | start + signal |
| `TERMINATED` / `CONTINUED_AS_NEW` | start + signal | start + signal | start + signal | start + signal |

A terminal prior is the difference from the plain start matrix in
[§6](#6-starting-a-workflow). A signal cannot land on a terminal run, so
the call starts a fresh run, as on Postgres. The fresh run seals the prior
`CONTINUED_AS_NEW`.

- A `RUNNING` run with a pending cooperative cancel counts as `CANCELLED`.
  The next drive ends it, so a signal cannot act there. Postgres sets
  `CANCELLED` at once, so its matrix gives the same result.
- A replace seals every active run of the key, as `TerminateIfRunning` does.
- The input cap applies only when the call starts a run. An attach ignores
  the input, as on Postgres.
- The new calls stamp the arrival time from the wall clock, as `send_signal`
  does.
- A cancelled `RUNNING` prior gets `WorkflowCancelled` with the reason
  `"terminated by signal-with-start"`, the Postgres text.
- After a replace, `outcome(prior)` returns
  `ExecutionOutcome::Terminated("CONTINUED_AS_NEW")`, as for a plain-start
  replace. Postgres still reports the prior's own outcome. Read the result of
  a replaced `COMPLETED` prior from the last event of `load_history(prior)`.

Both `send_signal_by_workflow_id` and `signal_with_start` dedupe a key
across every run of the key, in any state. A retry after a continue-as-new,
or after the first run ends, stages nothing and starts nothing. A promise key
dedupes per run only, as on Postgres.

### Queries (issue #2022)

Read a run's state with `query`. It calls a handler that
`ctx.register_query` registered, and it writes nothing.

```rust
// Inside the workflow — expose state:
let stage = Arc::new(Mutex::new("started"));
let read = Arc::clone(&stage);
ctx.register_query("stage", move || serde_json::json!(*read.lock().unwrap()));

// From outside — read it:
let stage = rt.query(exec, "stage", serde_json::Value::Null).await?;
```

`query` loads the history and replays the run to its frontier. It then calls
the handler. It appends no event, changes no row, delivers no staged signal,
fires no timer and runs no activity body.

- A running run answers with the state at its frontier: the first command
  that the history does not record. The replay does not know a query that
  the workflow registers past that point.
- A running run with a pending `cancel` replays with that reason, so
  `ctx.cancellation_reason()` reads as it does on the next drive.
- A terminal run answers with the state at its recorded end.
- The call reads the rows immediately. The returned future borrows nothing,
  so it can go to `tokio::spawn`. A later `set_query_timeout` does not change
  it.

> The replay runs workflow code, as a drive does. Code past the last recorded
> event runs again in each query, a `ctx.side_effect` closure included. Keep
> that code free of external effects. Put an external effect in an activity.

The future resolves to one of these errors:

| Error | Cause |
|---|---|
| `ExecutionNotFound` | The execution id is unknown. |
| `UnknownWorkflow` | The run's workflow is not registered in this runtime. |
| `QueryNotFound` | The replay registered no handler with this name. |
| `QueryFailed` | The handler returned `Err`. |
| `QueryPanicked` | The handler panicked, or the workflow panicked during the replay of a running run. |
| `QueryTimedOut` | The replay passed the query timeout. The default is 5 s. Set it with `set_query_timeout`. The timeout holds only while the replay yields. |
| `NonDeterministic` | The workflow code changed, so the replay of a running run diverges. A drive returns the same error. |
| `QueryUnavailable` | A terminal run cannot give a stable answer. The workflow code changed, the replay panicked, or a seal for an unsupported feature or a history cap left side effects unrecorded. |
| Other variants | A row cannot be read. |

### Updates (issue #2025)

An update is a request with a result. Register the handler in the workflow
body, or register a declarative `#[update]` with `register_update`.

```rust
// Inside the workflow — register the handler before the first wait:
ctx.register_update_handler(
    "quote",
    |input| if input.as_i64() > Some(0) { Ok(()) } else { Err("must be positive".into()) },
    |input| async move { Ok(serde_json::json!({ "price": input })) },
);

// Or register a declarative `#[update(workflow = "...")]` handler:
// rt.register_update(&quote_info());

// From outside — run the update and get its result:
let result = rt.update(exec, "quote", serde_json::json!(5)).await?;
```

`update` replays the run to rebuild its handlers. The arg schema runs first. A
violation is `SqliteError::UpdateInputInvalid`. The validator runs next. A
rejection is `SqliteError::UpdateRejected`. Neither writes an event.

The handler then runs. One transaction appends `UpdateAdmitted` and then
`UpdateCompleted`. If the handler returns an error or panics, the transaction
appends `UpdateFailed` instead, and the call returns
`SqliteError::UpdateFailed`. These are the core update events, so the history
replays on the core `WorkflowReplayer`.

The update does not drive the run. Call `run_until_blocked` after it, as after
`send_signal`.

The history caps count the two update events. An update that would reach a
cap writes no update event. The run then fails at the cap, as a drive fails
it, and the call returns `SqliteError::UpdateHistoryCapReached`.

The handler runs once, on a separate replay of the run. **State that the
handler writes does not reach the workflow body.** Use an update to compute a
result from the workflow state, not to change that state. Replay returns the
recorded result and does not run the handler again.

The handler can await any future, within a 5 second limit. It cannot await a
durable command, such as an activity or a timer. Such a handler returns
`SqliteError::UpdateTimedOut` and writes no event.

### List, describe and search attributes (issue #2027)

List the runs in a file, newest first, one page at a time:

```rust
use autumn_harvest_sqlite::{ExecutionFilter, StartOptions};
use serde_json::json;

// Start a run with a memo and a first search attribute.
let mut first = serde_json::Map::new();
first.insert("tenant".into(), json!("acme"));
let started = rt.start_workflow_with_options(
    "order",
    json!({ "id": 7 }),
    StartOptions {
        workflow_id: Some("order-7".into()),
        memo: Some(json!({ "owner": "ops" })),
        search_attributes: first,
        ..StartOptions::default()
    },
)?;

// Inside a workflow, merge more attributes in:
// ctx.upsert_search_attrs([("stage".to_string(), Some(json!("paid")))])?;

let mut filter = ExecutionFilter::new()
    .workflow_name("order")
    .state("RUNNING")
    .search_attribute("tenant", json!("acme"))
    .limit(50);
loop {
    let page = rt.list_executions(&filter)?;
    for run in &page.executions {
        println!("{:?} {:?}", run.exec_id, run.state);
    }
    match page.next {
        Some(cursor) => filter = filter.before(cursor),
        None => break,
    }
}

// One run, with its input, output and error:
let run = rt.describe(started.exec_id)?;
```

- **Filters.** `workflow_name`, `exec_id`, `workflow_id` (exact),
  `workflow_id_prefix`, `state` (repeat it for several states),
  `started_after` (inclusive), `started_before` (exclusive) and
  `search_attribute`. Set filters combine with AND. A filter holds at most
  64 states and 64 search attributes. A larger one is
  `SqliteError::InvalidFilter`.
- **Times.** `started_at` is the `WorkflowStarted` time, to the microsecond.
  `closed_at` is the time of the first terminal write. A later reuse-policy
  seal does not move it.
- **Search attributes.** A filter tests equality. Numbers compare as numbers,
  so `1` matches `1.0`. A filter value must be a JSON string, number, boolean
  or `null`. Another value is `SqliteError::InvalidSearchAttribute`. So is an
  integer above `i64::MAX`: JSON1 reads it as a float, which cannot tell
  neighbouring values apart.
- **Pages.** A page holds `limit` rows: 100 by default, cut to the range 1 to
  1000. `next` is the cursor of the next page, or `None` at the end. The
  cursor is the table `rowid`. A `VACUUM` can renumber it, so do not keep a
  cursor across one.
- **No payloads in a list.** `list_executions` returns no input, output or
  error. `visit_executions(&filter, |run| ...)` passes the same rows with
  their payloads, one row at a time. The visit holds one read snapshot until
  it returns, so keep the closure short. A long one stops the WAL file from
  shrinking.
- **Damaged rows.** Each field is a `Stored` value: `Value`, `Null` or
  `Unreadable`. A value in the wrong storage class or text that is not
  UTF-8 is `Unreadable`. So is a memo or search-attribute value that does
  not parse, and search attributes that are not an object. One bad row does
  not fail the page.
- **A reader beside the writer.** `SqliteReader::open(path)` opens the file
  `READ_ONLY` and takes no writer lock. It has the same three reads. It
  reads a file that an older version wrote. A column that the file does not
  have reads as `Null`.
- **Upserts.** `ctx.upsert_search_attrs` merges its patch in the cycle
  transaction, on the suspend path and on the terminal path, as on
  Postgres. `None` removes a key. A cycle that rolls back keeps no patch.
- **Memo.** The start sets it. No command changes it later. A start that
  returns an existing run leaves its memo and its search attributes as they
  are.
- **Continue-as-new.** The successor keeps the memo and the search
  attributes.

Limits:

- A run written before issue #2027 has no `started_at`. A time filter does
  not match it.
- Search-attribute filters test equality only. There is no comparison,
  `IN` or `EXISTS` predicate.
- One state filter seeks on an index. Every other filter walks the table
  from the newest row. A filter that matches few rows in a large file walks
  far to fill a page. A run's later columns sit after its input, so the walk
  also reads past each large input.
- A start does not check search-attribute keys against the
  `upsert_search_attrs` rules, as on Postgres. Search attributes have no
  size cap of their own.
- The memo is stored as plain JSON. This backend has no payload codec.

---

## 9. Durability and crash recovery

All durable state lives in the database. Dropping a runtime and re-`open`-ing the
**same file** is a faithful crash/restart:

1. `open(path)` applies the schema and **reclaims any orphaned `RUNNING` task** —
   under the single-writer assumption, a `RUNNING` row at startup was claimed by
   a process that exited without finalizing, so it is flipped back to `PENDING`.
2. Re-register your handlers (registrations are in process memory, not the file).
3. Drive the run — it **resumes purely by deterministic replay** of the recorded
   history. Completed steps are replayed from history, not re-executed; only work
   scheduled after the last durable event runs fresh.

This makes activity execution **at-least-once**: a crash *after a body runs but
before its result commits* re-runs that body on resume. **Write activity bodies
to be idempotent.** `ctx.idempotency_key()` is stable across these re-runs. A
body that checkpoints with `ctx.heartbeat(..)` resumes from the last stored
payload through `ctx.heartbeat_details()`.

The [`examples/durability.rs`](../autumn-harvest-sqlite/examples/durability.rs)
example proves resume-vs-restart with side-effect counters: it runs a workflow to
a signal block, **drops the runtime**, reopens the same file, delivers the signal,
and finishes — showing the reserve body ran exactly once (session 1; replayed
from history in session 2) and the ship body ran exactly once (session 2 only).

---

## 10. The single-writer / single-server contract

This backend assumes **one writer process** against **one** SQLite file:

- **`BEGIN IMMEDIATE` replaces `SELECT … FOR UPDATE SKIP LOCKED`.** A task claim
  takes SQLite's database-level write lock up front, selects the oldest ready
  task, and flips it to `RUNNING`. Under the single-writer assumption this is
  exactly-once by construction — no two claimers race.
- **Polling replaces `LISTEN`/`NOTIFY`** (see [§7](#7-the-drive-model)).
- **Restart = drop the runtime + reopen the file** (see
  [§9](#9-durability-and-crash-recovery)).

The runtime opens the database with `journal_mode = WAL`, `synchronous = FULL`
(fsync on every commit — the crash tests depend on it), and a `busy_timeout` so
an occasional external *reader* (a monitoring/inspector connection) can coexist.

### The contract is enforced (issue #1834)

`SqliteRuntime::open` takes an exclusive OS lock on a sidecar file,
`<database>.lock`. A second runtime on the same file fails fast with
`SqliteError::DatabaseLocked`. This applies to another process, and to a
second runtime in the same process.

- The second open fails **before** it changes the file. It runs no pragma, no
  schema step and no orphan reclaim. It cannot steal a `RUNNING` task.
- The open retries the lock for about 100 ms before it fails. A just-dropped
  runtime can hold its lock for a moment, for example while another thread
  forks a child process.
- The kernel releases the lock when the holder exits, also on a crash. A
  restart never meets a stale lock.
- The lock path comes from the canonical database path. A symlink or a
  relative path to the same file maps to the same lock. A hard link or a bind
  mount gives a second path, and so a second lock. Open the database through
  one path.
- A read-only inspector connection still works. It never touches the lock.
- An in-memory database has no file, so its lock is an owner row inside the
  database (`harvest_memory_writer_lock`). Several URI forms share one such
  database, such as `cache=shared` or `vfs=memdb`. `SQLite` decides which
  connections reach it, so no URI alias can split the lock. A private
  `:memory:` database starts empty and never conflicts.

The lock file stays on disk after the runtime drops. **Do not delete it** while
a runtime runs: a second process could then lock a new file. On Unix the lock
file takes the database file's read and write bits. Any user who can open the
lock file can hold it, so this keeps that set to the users who reach the data.

**Do not** use this backend as a shared multi-server queue. The lock is an
advisory lock on the local file system. A network file system may not honour
it. The only residual crash window is *mid
activity body* — a crash while a body is executing leaves the task `RUNNING` and
is recovered (re-running the body, at-least-once) by the orphan reclaim on the
next `open`. A single-process design has no heartbeat-timeout reclaimer for a
*live* peer, by design; multi-writer / multi-server recovery is out of scope.

---

## 11. v0.1 non-goals and follow-ups

Out-of-subset workflow primitives are rejected **loudly, by name** — never
silently dropped. A workflow reaching one of these surfaces
`SqliteError::Unsupported` (or a setup-time panic at registration) naming the
specific command/feature. The run then ends `FAILED` (see below):

- **Child workflows** (`spawn_child_workflow`, `spawn_child_workflow_detached`).
- **External signals / cancels** (`signal_external_workflow`,
  `request_cancel_external_workflow`).
- **Local activities** (`execute_local_activity`) and **external / task-token
  activities**.
- A **continue-as-new to a different type** (`continue_as_new_as_type`).
  The token is `ContinueAsNew.new_workflow_type`. A same-type
  `continue_as_new` is supported (see [§7](#7-the-drive-model)).
- **Worker sessions** (`create_session`) and **cancellable durable timers**
  (`start_timer` / `TimerHandle::…` — use the fire-once `ctx.timer(...)`).

Search attributes are supported since issue #2027. See
[§8](#list-describe-and-search-attributes-issue-2027).

A rejected execution ends `FAILED` (issue #1834). The drive that meets the
feature rolls back the whole cycle. It then seals the run in a new
transaction, with a typed `WorkflowFailed` event:

| Field | Value |
|---|---|
| `error_type` | `"UnsupportedFeature"` (`UNSUPPORTED_FEATURE_ERROR_TYPE`) |
| `details` | `{"feature": "<stable token>", "message": "<full text>"}`. The token is a command or field name, such as `StartChildWorkflow` or `ScheduleActivity.session_id`. |
| `non_retryable` | `true` |
| `error` | The `SqliteError::Unsupported` message |

The seal also removes the run's pending tasks, unfired timers and unkeyed
staged signals. The cycle's own cleanup rolled back, and nothing can use them
now. A keyed signal stays, so its key still dedupes (issue #2028).

That drive still returns `SqliteError::Unsupported`, so the caller sees the
reason. A later drive returns `RunState::Failed` and does not run the handler
again. `outcome()` returns `ExecutionOutcome::Failed`.

Only an unsupported feature or a history cap seals a run. Other errors leave
the run `RUNNING`:
an unregistered workflow or activity, a replay divergence, a contained panic
under its budget, or a failed task. A fix in the same runtime can then resume
the run.

A failing execution does not block unrelated executions. `poll_once` still
drives the rest of the fleet in the same pass (issue #1530). `run_until_idle`
still converges the rest of the fleet in one call (issue #1555). Both report
the first execution error to the caller. A sealed execution is terminal, so
later passes skip it.

Backend-level non-goals: distributed / multi-writer workers, `LISTEN`/`NOTIFY`
push wake-ups, multi-server crash recovery, schedules, the management API,
retention, worker sessions, sharding, and DAGs. Rejection is deliberate, so a
partial, silently-wrong implementation never ships.

The `WorkflowIdReusePolicy` matrix (see [§6](#6-starting-a-workflow)) shipped
under issue #1068. It is no longer a non-goal.

Two benign bookkeeping commands are silently no-ops (they append no event and
gate no control flow): `ctx.set_current_details(...)` and a re-park
`WaitForActivity`.

---

## 12. Runnable examples

- **[`examples/quickstart.rs`](../autumn-harvest-sqlite/examples/quickstart.rs)**
  — a complete order-processing workflow: open in-memory → register a workflow +
  two activities → start → `run_until_idle` → print the terminal outcome.

  ```text
  cargo run -p autumn-harvest-sqlite --example quickstart
  ```

- **[`examples/durability.rs`](../autumn-harvest-sqlite/examples/durability.rs)**
  — crash recovery: drive to a signal block, drop the runtime, reopen the same
  file, deliver the signal, resume to completion — proving the run *resumed* (via
  replay) rather than *restarted*.

  ```text
  cargo run -p autumn-harvest-sqlite --example durability
  ```

- **[`examples/claude-agent-daemon/`](../examples/claude-agent-daemon/)** — a
  whole application on this backend: a local daemon that runs Claude agent
  sessions as durable workflows. It shows the drive loop
  ([§7](#7-the-drive-model)), signals with a deadline
  ([§8](#8-signals-queries-and-updates)), and crash recovery
  ([§9](#9-durability-and-crash-recovery)) in one place, and it runs with no API
  key against a scripted offline model.

  ```text
  cargo run -p claude-agent-daemon -- serve --workspace /tmp/agent-demo
  ```

For the full API/contract reference, run
`cargo doc --open -p autumn-harvest-sqlite`.
