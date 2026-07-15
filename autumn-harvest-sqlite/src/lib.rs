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
//! - **Durable timers use the real wall clock, at millisecond precision.** A
//!   timer's absolute deadline is stored at arm time (`now + duration`) as an
//!   epoch-MILLISECOND (issue #1069 P2), so a fractional-second arm never fires
//!   before its true deadline (a `duration = 1s` timer armed at `…000.900` fires
//!   at `…001.900`, not the floored `…001.001`). On any pass, timers whose deadline
//!   has passed fire. There is no virtual clock to persist or reset, so timers are
//!   naturally monotonic across restarts. The public drivers read
//!   [`chrono::Utc::now`] **once per decision cycle** (issue #1069 P2), so a timer
//!   armed after a real-time activity anchors to its true arm instant rather than
//!   the start of the driver call; the `*_as_of` variants inject a single as-of
//!   time for deterministic, sleep-free tests.
//!   Timers that share the same `fire_at` (e.g. `tokio::join!(ctx.timer("a", 1),
//!   ctx.timer("b", 1))`) fire in `(fire_at, arm_seq)` order, so their `TimerFired`
//!   events land in `TimerStarted`-append order — the core matcher requires it to
//!   replay (issue #1069 P2).
//! - **Declared/per-call retry policies are honored.** The typed
//!   `ctx.execute_activity(&info, ...)` / `execute_activity_with_opts` path carries
//!   the activity's resolved [`RetryPolicy`](autumn_harvest::policy::RetryPolicy) in
//!   the scheduling command's `retry_policy_override`; the backend persists its
//!   `max_attempts` on the task row and the worker uses it in preference to the
//!   registered [`ActivitySpec`] cap (the raw `execute_activity_raw` path carries no
//!   override and still falls back to the registered default) (issue #1069 P2).
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
//!   fires and the wait resolves to `None`. The winner is decided by **arrival
//!   time** (at millisecond precision, issue #1069 P2), not consumption order: the
//!   wake ingest interleaves a staged signal's `received_at` against the deadline
//!   timer's `fire_at` (mirroring the Postgres `merge_wake_events`), so a signal
//!   delivered *after* an expired deadline records its `TimerFired` FIRST and the
//!   timeout wins (the late signal stays recorded, observable to a later plain
//!   wait), while an on-time signal wins even when the run is only driven past the
//!   deadline. **Only the raced deadline timer of THIS wait** (the core's
//!   `__signal_timeout:{seq}:{name}` row) is reordered ahead of the signal (issue
//!   #1069 P2); an unrelated durable timer is never fired ahead of a consumed
//!   signal. When the **signal** wins, the
//!   core emits a `CancelRaceLosers` bookkeeping command to durably delete the
//!   losing deadline timer; this backend handles it (deleting the `harvest_timers`
//!   row in the same cycle transaction — whether it rides a suspending batch or the
//!   terminal drain), so the run completes cleanly and no orphaned timer is left
//!   behind.
//!
//! Signal ingress is validated at the boundary:
//! [`SqliteRuntime::send_signal`](crate::SqliteRuntime::send_signal) rejects a signal
//! aimed at an unknown execution ([`SqliteError::ExecutionNotFound`]) or a terminal
//! one ([`SqliteError::WorkflowNotRunning`]) instead of staging a row no live pull
//! primitive could ever consume — mirroring the Postgres engine's rejection of a
//! signal to an unknown/terminal target. A `RUNNING` execution stages (and later
//! consumes) the signal exactly as before.
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
//! # Robustness — healthy-run-killer containment (Codex #1069 / round 8)
//!
//! Failure modes that would otherwise WEDGE (or wrongly seal) a healthy run are
//! contained:
//!
//! - **A workflow-handler panic is bounded-retried, not sealed on the first
//!   strike.** A `#[workflow]` body that `panic!()`s (distinct from an activity
//!   body panic, above) is caught by the core at the dispatch boundary and
//!   surfaced as a contained panic. Rather than sealing the run `FAILED` on the
//!   first strike — permanently closing a transiently-panicking or hotfix-able run
//!   — the backend discards the panicked decision cycle (appends nothing, persists
//!   no bookkeeping command, state stays `RUNNING`) and surfaces
//!   [`SqliteError::WorkflowPanicked`] under a bounded consecutive-panic budget, so
//!   a fixed/rolled-back build can re-drive the UNCHANGED history to completion.
//!   Only after the budget is exhausted is the run sealed `FAILED` with the typed
//!   `HandlerPanic` error (as a normal [`RunState::Failed`]). A genuine author
//!   `Err(...)` still fails terminally on the first strike, unchanged. Mirrors the
//!   Postgres worker's issue #782 panic gate.
//!
//! - **Engine replay non-determinism is NON-TERMINAL.** If a workflow's code
//!   drifts from its recorded history (a bad deploy), the determinism core
//!   returns a non-determinism divergence. Rather than sealing the run `FAILED`
//!   and poisoning history with a terminal event from the bad build, the backend
//!   **discards** the divergent decision cycle (appends nothing, persists no
//!   bookkeeping command), leaves the execution `RUNNING` / resumable, and
//!   surfaces [`SqliteError::NonDeterministic`]. A fixed or rolled-back build then
//!   re-drives the UNCHANGED history to completion. This mirrors the Postgres
//!   engine's issue #603 semantics ("a workflow-task failure must never fail the
//!   workflow"); the full automatic block-with-backoff is a documented follow-up.
//!   A genuine author `Err(...)` still fails terminally, unchanged.
//! - **An unregistered activity leaves its task re-claimable.** A workflow that
//!   schedules an activity whose body is not registered has the just-claimed task
//!   RELEASED back to `PENDING` (not stranded `RUNNING`) before the
//!   [`SqliteError::UnregisteredActivity`] error is surfaced, so a later drain
//!   re-claims it once the body is registered in the SAME runtime — no DB reopen.
//! - **A panicking activity body is contained.** A body that `panic!()`s (rather
//!   than returning `Err`) is caught at the dispatch boundary and routed through
//!   the NORMAL retryable-failure path (record the attempt, requeue if attempts
//!   remain, else terminal `ActivityFailed`), so it never unwinds past the
//!   finalize and strands the task `RUNNING`. Mirrors the Postgres worker's
//!   handler-panic containment (issue #782). Write bodies to be idempotent
//!   regardless — a caught panic is a retry, and (as with any failure) the body
//!   may have run partially.
//! - **Mixed timer + activity batch.** A single decision cycle emitting BOTH a
//!   timer and an activity (`tokio::join!`) records its schedule events in COMMAND
//!   order and drives to completion + replays cleanly on the core
//!   `WorkflowReplayer` — **when the activity branch is polled first**
//!   (`join!(activity, timer)`). The reverse order (`join!(timer, activity)`) is a
//!   pre-existing limitation of the *core* determinism engine, NOT this backend:
//!   `HistoryMatcher::match_timer_strict` breaks its forward `TimerFired` scan at
//!   an unconsumed `ActivityScheduled`, so the core `WorkflowReplayer` rejects even
//!   an ideal timer-first history (`NonDeterminismDetected(EarlyCompletion)`) —
//!   shared with the Postgres engine. Put the activity branch first in a mixed
//!   `join!`.
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
