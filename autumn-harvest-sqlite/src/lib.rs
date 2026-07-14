//! Embedded, single-writer **`SQLite`** backend for `autumn-harvest` — an
//! edge / local-first workflow runtime.
//!
//! The valuable, hard part of `autumn-harvest` — the deterministic replay engine
//! (`run_workflow`, [`WorkflowEvent`](autumn_harvest::WorkflowEvent), the history
//! matcher, `WorkflowContext`) — is already backend-neutral: it consumes plain
//! values (an `ExecutionId`, a `Vec<WorkflowEvent>`, a handler `fn`, a JSON
//! input) and needs no database connection or trait object. This crate reuses
//! that core **wholesale** and reimplements only *persistence* — the event store
//! ([`store`]), the task queue + durable timers ([`queue`]), and the worker pass
//! ([`worker`]) — on embedded `SQLite` via `rusqlite` (`bundled`, so no system
//! `SQLite` and no Docker are required). It depends on `autumn-harvest` with
//! `default-features = false`, so no Diesel/Postgres is pulled in.
//!
//! A history produced by this backend is byte-identical, **per event**, to one
//! the Postgres backend would write (both serialize `WorkflowEvent` in the same
//! adjacently-tagged `#[serde(tag = "type", content = "data")]` form), so it
//! replays unchanged on the core `WorkflowReplayer` — the property the
//! cross-backend tests assert.
//!
//! # The single-writer / single-server contract
//!
//! This backend assumes **one writer process** against **one** `SQLite` file:
//!
//! - **`BEGIN IMMEDIATE` replaces `SELECT … FOR UPDATE SKIP LOCKED`.** A task
//!   claim takes `SQLite`'s database-level write lock up front, selects the oldest
//!   ready task, and flips it to `RUNNING`. Under the single-writer assumption
//!   this is exactly-once by construction — no two claimers race.
//! - **Polling replaces `LISTEN`/`NOTIFY`.** `SQLite` has no push notification, so
//!   the driver drains all ready work, re-runs the workflow, and repeats until a
//!   run is terminal or blocked ([`SqliteRuntime::run_until_blocked`] /
//!   [`SqliteRuntime::run_until_idle`]).
//! - **Restart = drop the runtime + re-open the file.** All state is in the
//!   database. [`SqliteRuntime::open`] reclaims any task stranded `RUNNING` by a
//!   crash (there is one server, so a `RUNNING` row at startup is an orphan) and
//!   the workflow resumes purely by deterministic replay. This makes activity
//!   execution **at-least-once**: a crash *after a body runs but before its result
//!   commits* re-runs the body on resume — write activity bodies to be idempotent.
//! - **Durable timers use the real wall clock.** A timer's absolute deadline is
//!   stored at arm time (`now + duration`); on any pass, timers whose deadline has
//!   passed fire. There is no virtual clock to persist or reset, so timers are
//!   naturally monotonic across restarts. The public drivers use [`chrono::Utc::now`];
//!   the `*_as_of` variants inject an as-of time for deterministic, sleep-free tests.
//!
//! # Crash-model bound (accepted non-goal)
//!
//! The only residual crash window is **mid activity body** — a crash while a body
//! is executing leaves the task `RUNNING` and is recovered by the orphan reclaim
//! on the next [`open`](SqliteRuntime::open) (re-running the body, at-least-once).
//! A single-process design has no heartbeat-timeout / poison-pill reclaimer for a
//! *live* peer, by design; multi-writer / multi-server recovery is out of scope.
//!
//! # Usage sketch
//!
//! ```no_run
//! use autumn_harvest::prelude::*;
//! use autumn_harvest_sqlite::{ActivitySpec, SqliteRuntime};
//!
//! #[workflow]
//! async fn greet(ctx: &WorkflowContext, name: String) -> Result<String, String> {
//!     let greeting: String = ctx
//!         .execute_activity_raw("say_hello", serde_json::json!(name), "default")
//!         .await
//!         .map_err(|e| e.to_string())?
//!         .as_str()
//!         .unwrap_or_default()
//!         .to_string();
//!     Ok(greeting)
//! }
//!
//! # async fn run() -> Result<(), autumn_harvest_sqlite::SqliteError> {
//! let mut rt = SqliteRuntime::open("workflows.db")?;
//! rt.register_workflow(&greet_info());
//! rt.register_activity(
//!     "say_hello",
//!     ActivitySpec::new(1, |input| Ok(serde_json::json!(format!("hello {input}")))),
//! );
//!
//! let exec = rt.start_workflow("greet", serde_json::json!("world"))?;
//! rt.run_until_blocked(exec).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Non-goals (see issue #1068 follow-ups)
//!
//! Out of scope for this backend: distributed / multi-writer workers, `LISTEN`/
//! `NOTIFY` push wake-ups, multi-server crash recovery, schedules, the management
//! API, retention, worker sessions, sharding, and DAGs. In the supported
//! workflow subset, `continue_as_new`, child workflows, external signals/cancels,
//! local activities, updates, and search attributes are unsupported and surface
//! as [`SqliteError::Unsupported`]. Two benign bookkeeping commands are instead
//! silently **no-ops** (they append no `WorkflowEvent` and gate no control flow):
//! `ctx.set_current_details(...)` (the operator status breadcrumb, issue #593) and
//! a re-park `WaitForActivity`.
//!
//! **Signals are pull-only.** A staged signal is appended to history (as
//! `SignalReceived`) only when a workflow reaches a pull primitive that consumes
//! it. The supported pull surface is:
//! - the plain waits `ctx.wait_for_signal` / `ctx.receive_signal`; and
//! - the signal-or-deadline waits `ctx.wait_for_signal_timeout` /
//!   `ctx.receive_signal_timeout` (issue #476), which race the signal against a
//!   durable deadline timer — the signal wins if it arrives first, else the timer
//!   fires and the wait resolves to `None`. When the **signal** wins, the core
//!   emits a `CancelRaceLosers` bookkeeping command to durably delete the losing
//!   deadline timer; this backend handles it (deleting the `harvest_timers` row in
//!   the same cycle transaction — whether it rides a suspending batch or the
//!   terminal drain), so the run completes cleanly and no orphaned timer is left
//!   behind.
//!
//! The push-handler and non-blocking drain APIs — `register_signal_handler`
//! (#546), `drain_signals` / `try_receive_signal` (#775) — depend on the Postgres
//! task-preparation ingest that promotes *all* pending signals into history up
//! front, which this single-writer backend does not implement; a workflow that
//! relies on them will not see its signals here. Restrict to the pull primitives
//! on this backend; push-based signal *handlers* remain a follow-up.
//!
//! There is deliberately **no import/seed-a-foreign-history API**. The
//! cross-backend guarantee (a history written here replays on the core engine) is
//! export-shaped and needs none. A future edge → hub sync path that ingests an
//! in-flight history would have to rebuild the derived task/timer rows from any
//! open scheduled work (or reject such histories) so a run isn't wedged — that is
//! a follow-up.
//!
//! # Deterministic side effects
//!
//! `ctx.system_now()`, `ctx.new_uuid()`, `ctx.random_*()`, and `ctx.side_effect()`
//! record a frozen value as a `SideEffectRecorded` event so replay never re-mints
//! it. These events are persisted **atomically with the rest of the cycle** in
//! every case — whether they ride a **suspending** batch (a side effect *before*
//! an activity/timer/signal, via [`apply_commands`](crate::SqliteRuntime)) or are
//! emitted in the cycle that **completes/fails** the workflow with no further
//! suspension (e.g. `ctx.new_uuid()` right before `Ok(..)`): the terminal cycle
//! drains those pending bookkeeping commands and appends each one BEFORE the
//! terminal `WorkflowCompleted`/`WorkflowFailed` event, in the same transaction,
//! mirroring the Postgres worker's `persist_terminal_outcome_commands`. So a
//! deterministic primitive is **never** silently dropped, and a history written
//! here always replays byte-identically on the core `WorkflowReplayer`.

mod error;
mod queue;
mod runtime;
mod schema;
mod store;
mod worker;

pub use autumn_harvest::{ExecutionId, WorkflowEvent};

pub use crate::error::{SqliteError, SqliteResult};
pub use crate::runtime::{ActivityBody, ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
pub use crate::store::ActivityAttempt;
