# autumn-harvest-sqlite

An **embedded, single-writer SQLite** persistence backend for the
[`autumn-harvest`](../autumn-harvest) durable-workflow engine — for edge,
local-first, and single-server deployments where there is no database server to
run.

The valuable, hard part of `autumn-harvest` — the deterministic replay engine
(`run_workflow`, `WorkflowEvent`, the history matcher, `WorkflowContext`) — is
backend-neutral. This crate reuses that core **wholesale** and reimplements only
*persistence* (the event store, the task queue + durable timers, and the worker
pass) on embedded SQLite via `rusqlite` (`bundled`, so **no system SQLite and no
Docker are required**). It pulls in `autumn-harvest` with
`default-features = false`, so no Diesel/Postgres comes along.

A history written by this backend is byte-identical, per event, to one the
Postgres backend would write, so it replays unchanged on the core
`WorkflowReplayer`.

## When to use it

- **Edge / local-first / embedded** — the workflow engine ships *inside* your
  binary; there is no separate service to deploy or operate.
- **Single server** — one writer process against one SQLite file.
- **Development, tests, demos, CLIs** — an in-memory database gives you a full
  durable-workflow runtime with zero setup.

## When *not* to use it

Reach for the Postgres core ([`autumn-harvest`](../autumn-harvest)) instead when
you need:

- **Distributed / multi-worker** execution or **multi-server** crash recovery.
- **High throughput** — a single writer with `BEGIN IMMEDIATE` is not built for
  a fleet claiming thousands of tasks per second.
- **`LISTEN`/`NOTIFY` push wake-ups**, DAGs, the management API, worker
  sessions, or sharding — all v0.1 non-goals here (see below).

## Quickstart

```rust
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ExecutionOutcome, SqliteError, SqliteRuntime};

#[workflow]
async fn process_order(ctx: &WorkflowContext, order: serde_json::Value) -> Result<String, String> {
    let reservation: String = ctx
        .execute_activity(&reserve_inventory_info(), order)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("confirmed: {reservation}"))
}

// The `#[activity]` body runs with a real `ActivityContext` (issue #2024). The
// same fn runs unchanged on the Postgres backend.
#[activity]
async fn reserve_inventory(ctx: &ActivityContext, order: serde_json::Value) -> Result<String, String> {
    Ok(format!("reserved {order} on attempt {}", ctx.attempt()))
}

#[tokio::main]
async fn main() -> Result<(), SqliteError> {
    let mut rt = SqliteRuntime::open_in_memory()?;    // or open("workflows.db") for durability
    rt.register_workflow(&process_order_info());
    rt.register_activity(&reserve_inventory_info());

    let exec = rt.start_workflow("process_order", serde_json::json!({"item": "widget"}))?;
    rt.run_until_idle().await?;                        // drive the polling loop to quiescence

    if let ExecutionOutcome::Completed(output) = rt.outcome(exec)? {
        println!("{output}");
    }
    Ok(())
}
```

A complete, runnable version is in
[`examples/quickstart.rs`](examples/quickstart.rs).

## Key concepts

- **Single-writer contract (`BEGIN IMMEDIATE`).** One writer process owns one
  SQLite file. A task claim takes the database write lock up front and flips the
  oldest ready task to `RUNNING` — exactly-once by construction, replacing
  Postgres's `SELECT … FOR UPDATE SKIP LOCKED`. `open` enforces the contract
  (issue #1834): it locks `<database>.lock`, and a second runtime on the same
  file fails fast with `SqliteError::DatabaseLocked`. The lock dies with its
  process, so a crash leaves no stale lock.
- **Polling drive model.** SQLite has no push notification, so instead of
  `LISTEN`/`NOTIFY` you *drive* the runtime: `poll_once` / `run_until_blocked` /
  `run_until_idle` drain all ready work and re-run the workflow until every run
  is terminal or blocked on external input.
- **Queries (issue #2022).** `rt.query(exec, "name", args).await` calls a
  handler that `ctx.register_query` registered. It replays the stored history
  to the run's frontier and writes nothing. A terminal run answers with the
  state at its recorded end.
- **List and describe (issue #2027).** `list_executions(&filter)` pages runs
  newest first. It filters by workflow name, workflow id prefix, state, start
  time and search attributes. `describe(exec)` reads one run with its
  payloads. `SqliteReader::open(path)` gives the same reads without the
  writer. `ctx.upsert_search_attrs` and the memo of
  `start_workflow_with_options` are stored on the run.
- **Durability & crash recovery.** All state is in the database. Dropping the
  runtime and re-`open`-ing the same file is a faithful restart: any task left
  `RUNNING` by a crash is reclaimed and the workflow resumes purely by
  deterministic replay. Activity execution is therefore **at-least-once** — write
  activity bodies to be idempotent. See
  [`examples/durability.rs`](examples/durability.rs).
- **Updates (issue #2025).** `update(exec, name, input)` runs a registered
  update handler and returns its result. A validator rejection writes no event.
  An accepted update writes `UpdateAdmitted` and then `UpdateCompleted` or
  `UpdateFailed`, in one transaction. The handler runs on a separate replay,
  so state that it writes does not reach the workflow body.
- **Idempotent starts by default (issue #1068).** `start_workflow_with_id`
  applies the `AllowDuplicate` reuse policy. A duplicate `(workflow_name,
  workflow_id)` **attaches** to the existing, non-sealed run instead of
  starting a second one. The new call's input is discarded. Use
  `start_workflow_with_reuse_policy` for the full `WorkflowIdReusePolicy`
  matrix — reject, replace-if-failed, or terminate-and-restart.
- **Keyed signals and signal-with-start (issue #2028).**
  `send_signal_idempotent(exec, name, payload, key)` stages a signal once per
  key, so a retried webhook is safe. `send_signal_by_workflow_id` signals the
  running run of a `(workflow_name, workflow_id)` key and refuses a terminal
  one. `signal_with_start` starts a run and signals it, or signals the run
  that exists, in one transaction.
- **Cancel and terminate (issue #2021).** `cancel(exec, reason)` is
  cooperative. The next drive replays the body with
  `ctx.cancellation_reason()` set, so the body can run cleanup activities. The
  run then ends `CANCELLED`. `terminate(exec, reason)` ends the run
  `TERMINATED` at once. Both record the core `WorkflowCancelled` event, as
  Postgres does.
- **Long-lived runs (issue #2023).** `ctx.continue_as_new(input)` seals the
  run `CONTINUED_AS_NEW` and starts a successor with the same `workflow_id`, in
  one transaction. Replayed signals stay with the old run, as on Postgres. A
  backlog past the history caps moves to the successor.
  `run_until_blocked` returns `RunState::ContinuedAsNew(successor)`. The #1804
  history caps apply with the Postgres defaults: 50,000 events and 50 MiB per
  run. A run that reaches a cap fails. Its error is `HistoryCapReason` JSON. Change
  the caps with `set_history_event_hard_cap` and `set_history_byte_hard_cap`.
- **Timers, mutex, local activities, timeout and retry (issue #2029).**
  `ctx.start_timer` and `TimerHandle` arm, cancel and reset durable timers.
  `ctx.mutex(key).acquire()` grants a key in FIFO order, and a waiting run
  returns `RunState::WaitingMutex(key)`. `#[activity(local = true)]` runs
  inline after its decision commits. `#[workflow(execution_timeout = ..)]` ends a
  late run `TIMED_OUT`. `#[workflow(retry = ..)]` starts a retry run, which
  `successor(exec)` returns. Each records the Postgres events.
- **Schedules, retention and the append-only guard (issue #2031).**
  `upsert_schedule` stores a core `WorkflowSchedule`. Each `poll_once` pass
  fires the due slots. A reopen keeps every slot. `purge_terminal(older_than)`
  removes old terminal runs and keeps live ones. Two triggers keep
  `harvest_events` append-only, the analog of the Postgres guard of issue
  #1817. This backend has no sanctioned rewriter, so they refuse every
  `UPDATE` and an `INSERT OR REPLACE`.

## v0.1 non-goals

Out of scope for this backend (tracked as issue #1068 follow-ups):

- Distributed / multi-writer workers; multi-server crash recovery.
- `LISTEN`/`NOTIFY` push wake-ups.
- The management API, DAGs, worker sessions, sharding.
- Child workflows, external signals/cancels, and a continue-as-new to a
  different workflow type
  (`continue_as_new_as_type`) — a workflow reaching one of these is
  rejected **loudly, by name**, never silently dropped. The run then ends
  `FAILED` with `error_type = "UnsupportedFeature"` (issue #1834).

## Learn more

- **Docs guide:** [`docs/sqlite-backend.md`](../docs/sqlite-backend.md) — a
  task-oriented walkthrough of the whole surface.
- **Runnable examples:** [`examples/quickstart.rs`](examples/quickstart.rs) and
  [`examples/durability.rs`](examples/durability.rs). For a whole application on
  this backend, see
  [`examples/claude-agent-daemon/`](../examples/claude-agent-daemon/) — a local
  daemon that runs Claude agent sessions as durable workflows.
- **API reference:** `cargo doc --open -p autumn-harvest-sqlite` — the
  crate-level docs are the canonical design/contract document.
- **Why this crate exists (and why it is not a core trait):**
  [`docs/rnd/sqlite-feasibility.md`](../docs/rnd/sqlite-feasibility.md) — the
  R&D decision record behind the companion-crate shape, including the
  module-by-module Postgres-coupling audit and the capabilities the
  single-writer model gives up.
