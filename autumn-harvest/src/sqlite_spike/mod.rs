//! `SQLite` edge/local-first feasibility spike (issue #966) — **throwaway R&D**.
//!
//! This module answers one question: *can the backend-neutral determinism core
//! (`run_workflow`, `WorkflowEvent`, deterministic replay) be driven by a
//! non-Postgres, embedded, single-writer persistence layer?* It reuses that core
//! **wholesale** and reimplements only persistence — the event store
//! ([`store`]), the task queue + durable timers ([`queue`]), and the worker pass
//! ([`worker`]) — on embedded `SQLite` via `rusqlite`.
//!
//! It is gated behind the `sqlite-spike` Cargo feature so the default build never
//! compiles it (or `rusqlite`). It is **not** part of the shipped engine and is
//! intentionally minimal (no sharding, no concurrency, no LISTEN/NOTIFY, no
//! cross-workflow features).
//!
//! # The decision loop
//!
//! [`SqliteRuntime::run_until_blocked`] polls a single execution to a terminal
//! state or an external-input block:
//!
//! 1. Load the accumulated history from `SQLite` and call
//!    [`run_workflow`](crate::run_workflow) once.
//! 2. On `Suspended`, persist each new command as event(s) and enqueue the
//!    corresponding work (an activity task, a durable timer, or apply a staged
//!    signal) — **the event append and its paired task/timer row insert commit
//!    in one `SQLite` transaction per decision cycle**, so a crash can never
//!    leave a scheduled-event without its queue/timer row — then run the
//!    [`worker`] pass to execute ready activities and fire due timers, appending
//!    each activity's post-body persistence (attempt audit + terminal event +
//!    task-state flip) and each timer's fire (`TimerFired` event + `fired = 1`
//!    flag) atomically in one transaction too.
//! 3. Repeat until the run reports `Completed`/`Failed` — the terminal event and
//!    the execution-state flip likewise commit in one transaction, so a crash
//!    can never re-run a run that already appended its terminal event — or until
//!    a full cycle makes no progress (blocked on a not-yet-due timer or an
//!    undelivered signal).
//!
//! **Every event append is committed together with its companion durable state
//! mutation** — the paired queue/timer row insert, the fired-timer flag, the
//! signal `delivered` flag, the terminal execution-state flip, and (at start) the
//! execution row — so no crash window can leave the two out of sync.
//!
//! A "crash" is modelled by dropping the runtime and its `SQLite` connection, then
//! opening a fresh [`SqliteRuntime`] on the same file: deterministic replay
//! reproduces the identical command stream from the durable history, so no
//! activity is re-executed and no work is lost. The virtual clock is itself
//! durable (persisted on every advance, restored on open), so a timer armed at a
//! non-zero logical time still fires after a restart.

// Throwaway R&D module: keep the owned-value public API ergonomic (callers pass
// owned `serde_json::Value`/`Vec` payloads straight through) and skip the
// per-function `# Errors` doc ceremony a shipped module would carry — every
// fallible method returns `SpikeError`, documented on the type.
#![allow(clippy::needless_pass_by_value, clippy::missing_errors_doc)]

mod queue;
mod schema;
mod store;
mod worker;

use std::collections::HashMap;

use rusqlite::Connection;
use serde_json::Value;

use crate::context::WorkflowCommand;
use crate::event::WorkflowEvent;
use crate::executor::WorkflowOutcome;
use crate::info::{WorkflowHandlerFn, WorkflowInfo};
use crate::run_workflow;
use crate::types::ExecutionId;

pub use store::ActivityAttempt;

/// The activity body a workflow's `execute_activity_raw(name, ...)` resolves to.
///
/// Synchronous and keyed by name — the reused, valuable part of the engine is
/// the workflow orchestration + replay, so activity execution is trivially
/// stubbed here (no `ActivityContext`, no I/O framework).
pub type ActivityBody = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// A registered activity: its body plus how many attempts the worker may make.
pub struct ActivitySpec {
    pub(crate) body: ActivityBody,
    pub(crate) max_attempts: u32,
}

impl ActivitySpec {
    /// Register an activity body that may be attempted up to `max_attempts`
    /// times (`1` = no retry).
    #[must_use]
    pub fn new(
        max_attempts: u32,
        body: impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            body: Box::new(body),
            max_attempts: max_attempts.max(1),
        }
    }
}

/// The status of an execution after a driver step.
#[derive(Debug)]
pub enum RunState {
    /// The workflow reached a terminal success with this output.
    Completed(Value),
    /// The workflow returned an error.
    Failed(String),
    /// Made durable progress this cycle; not yet terminal or blocked.
    InProgress,
    /// Blocked awaiting a named signal that has not been delivered.
    WaitingSignal(String),
    /// Blocked awaiting a durable timer that has not yet fired.
    WaitingTimer,
}

/// Errors surfaced by the spike runtime.
#[derive(Debug)]
pub enum SpikeError {
    /// A `rusqlite` / `SQLite` error.
    Sqlite(rusqlite::Error),
    /// A JSON (de)serialization error.
    Json(serde_json::Error),
    /// A workflow name with no registered handler.
    UnknownWorkflow(String),
    /// An activity name with no registered body.
    UnregisteredActivity(String),
    /// A workflow command outside the spike's supported subset.
    Unsupported(String),
    /// A stored value could not be parsed back into its type.
    Corrupt(String),
    /// The runtime made no progress and could not classify the block — should
    /// not happen for the supported primitives; surfaced instead of looping.
    Stuck,
}

impl SpikeError {
    fn corrupt(field: &str) -> Self {
        Self::Corrupt(field.to_string())
    }
    fn unregistered(name: &str) -> Self {
        Self::UnregisteredActivity(name.to_string())
    }
}

impl std::fmt::Display for SpikeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {e}"),
            Self::Json(e) => write!(f, "json error: {e}"),
            Self::UnknownWorkflow(n) => write!(f, "unknown workflow: {n}"),
            Self::UnregisteredActivity(n) => write!(f, "unregistered activity: {n}"),
            Self::Unsupported(n) => write!(f, "unsupported command for spike: {n}"),
            Self::Corrupt(field) => write!(f, "corrupt stored value: {field}"),
            Self::Stuck => write!(
                f,
                "runtime made no progress and could not classify the block"
            ),
        }
    }
}

impl std::error::Error for SpikeError {}

impl From<rusqlite::Error> for SpikeError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

impl From<serde_json::Error> for SpikeError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Maximum driver iterations before declaring a runaway (safety bound only —
/// the supported scenarios converge in a handful of cycles).
const MAX_ITERATIONS: usize = 10_000;

/// A single-writer, embedded workflow runtime backed by one `SQLite` file.
pub struct SqliteRuntime {
    conn: Connection,
    workflows: HashMap<String, WorkflowHandlerFn>,
    activities: HashMap<String, ActivitySpec>,
    /// Virtual clock (epoch seconds). Real wall-clock is deliberately avoided so
    /// tests can drive durable timers deterministically via [`Self::advance_time`].
    clock: i64,
}

impl SqliteRuntime {
    /// Open (creating if absent) the `SQLite` database at `path`, applying the
    /// schema idempotently.
    pub fn open(path: &str) -> Result<Self, SpikeError> {
        let conn = Connection::open(path)?;
        conn.execute_batch(schema::SCHEMA)?;
        // Restore the durable virtual clock so timers armed at a non-zero logical
        // time still fire after a restart. Timers store an *absolute* `fire_at`;
        // resetting the clock to 0 here would leave any timer armed after the
        // clock advanced permanently not-yet-due. Belt-and-braces: never regress
        // below the deadline of an already-FIRED timer (the clock provably reached
        // it), guarding against a lost clock write — the persisted value is the
        // primary source.
        let clock = store::load_clock(&conn)?.max(queue::max_fired_timer_deadline(&conn)?);
        Ok(Self {
            conn,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            clock,
        })
    }

    /// Register a workflow handler (by name) from its macro-generated
    /// [`WorkflowInfo`].
    pub fn register_workflow(&mut self, info: &WorkflowInfo) {
        self.workflows.insert(info.name.to_string(), info.handler);
    }

    /// Register an activity body under `name`.
    pub fn register_activity(&mut self, name: impl Into<String>, spec: ActivitySpec) {
        self.activities.insert(name.into(), spec);
    }

    /// Advance the virtual clock by `secs` seconds (drives durable timers) and
    /// **persist** the new value so a subsequent [`Self::open`] restores it. The
    /// durable write is what keeps a timer armed at a non-zero logical time firing
    /// across a restart. The new value is persisted *before* the in-memory clock
    /// is updated, so a failed write leaves the two consistent.
    pub fn advance_time(&mut self, secs: i64) -> Result<(), SpikeError> {
        let new = self.clock.saturating_add(secs);
        store::persist_clock(&self.conn, new)?;
        self.clock = new;
        Ok(())
    }

    /// Start a fresh workflow execution, appending its `WorkflowStarted` event.
    pub fn start_workflow(
        &mut self,
        workflow_name: &str,
        input: Value,
    ) -> Result<ExecutionId, SpikeError> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(SpikeError::UnknownWorkflow(workflow_name.to_string()));
        }
        let exec = ExecutionId::new();
        // Execution row + its `WorkflowStarted` event commit in ONE transaction,
        // so a crash between them can never leave a RUNNING execution with no
        // history (which would reload with an empty, start-marker-less log).
        let tx = self.conn.transaction()?;
        store::insert_execution(&tx, exec, workflow_name, &input)?;
        store::append_event(
            &tx,
            exec,
            &WorkflowEvent::workflow_started(input, chrono::Utc::now()),
        )?;
        tx.commit()?;
        Ok(exec)
    }

    /// Seed an execution from an externally-produced history (e.g. one a
    /// Postgres backend would have written). Used to prove a PG-shaped history
    /// drives the prototype's own reload path identically.
    ///
    /// **In-flight open work is materialized, not left dangling.** When the
    /// imported history carries an *open* (un-terminated) `ActivityScheduled` or
    /// `TimerStarted` — the source backend stopped with the activity/timer still
    /// in flight — the importer rebuilds the companion `spike_tasks`/`spike_timers`
    /// row a live decision cycle would have created. Without it, replay re-derives
    /// a `WaitForActivity`/timer wait, [`Self::apply_commands`] skips enqueueing
    /// (the schedule/start is already in history), and the worker has no durable
    /// row to drain — wedging the imported run at [`SpikeError::Stuck`]. This is
    /// the *import* twin of `apply_commands`'s own append+enqueue atomicity, closed
    /// so a cross-backend handoff of a *partially-complete* execution drains
    /// correctly rather than deadlocking.
    pub fn import_execution(
        &mut self,
        workflow_name: &str,
        input: Value,
        events: Vec<WorkflowEvent>,
    ) -> Result<ExecutionId, SpikeError> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(SpikeError::UnknownWorkflow(workflow_name.to_string()));
        }
        let exec = ExecutionId::new();
        // Copy the Copy clock before borrowing `self.conn` mutably for the tx —
        // open timers are re-armed relative to it (handoff semantic below).
        let clock = self.clock;
        // Execution row + the whole imported history + companion rows for any
        // in-flight open scheduled work commit in ONE transaction, so a crash
        // mid-import can never leave a partial (torn) history, nor a scheduled-work
        // event without its derived queue/timer row.
        let tx = self.conn.transaction()?;
        store::insert_execution(&tx, exec, workflow_name, &input)?;
        for event in &events {
            store::append_event(&tx, exec, event)?;
        }
        // Rebuild the derived state for any *open* scheduled work in the import.
        for event in &events {
            match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name,
                    input,
                    queue,
                } => {
                    // Open iff no terminal (`ActivityCompleted`/exhausted
                    // `ActivityFailed`) for this id. Materialize a fresh PENDING
                    // task the worker claims + runs. The source backend's in-flight
                    // attempt (its `ActivityStarted`, which the spike ignores) is
                    // lost on handoff, so re-running is the correct at-least-once
                    // recovery.
                    if !store::history_has_activity_terminal(&events, &activity_id.to_string()) {
                        queue::enqueue_activity(
                            &tx,
                            exec,
                            *activity_id,
                            name,
                            input,
                            queue,
                            clock,
                        )?;
                    }
                }
                WorkflowEvent::TimerStarted {
                    timer_id,
                    duration_secs,
                } => {
                    // Open iff no `TimerFired` for this id. Handoff semantic: a
                    // timer's *remaining* delay restarts from import time — the
                    // source backend's absolute deadline is not portable across
                    // backends (different clock epochs) — so re-arm fresh at
                    // `clock + duration_secs` (saturating, like `apply_commands`).
                    if !store::history_has_timer_fired(&events, &timer_id.to_string()) {
                        let fire_at = clock
                            .checked_add(i64::try_from(*duration_secs).unwrap_or(i64::MAX))
                            .unwrap_or(i64::MAX);
                        queue::enqueue_timer(&tx, exec, &timer_id.to_string(), fire_at)?;
                    }
                }
                _ => {}
            }
        }
        tx.commit()?;
        Ok(exec)
    }

    /// Deliver an inbound signal (staged until a `wait_for_signal` consumes it).
    pub fn deliver_signal(
        &mut self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
    ) -> Result<(), SpikeError> {
        store::stage_signal(&self.conn, exec, name, &payload)
    }

    /// The full ordered event history — the canonical, replayable log.
    pub fn load_history(&self, exec: ExecutionId) -> Result<Vec<WorkflowEvent>, SpikeError> {
        store::load_history(&self.conn, exec)
    }

    /// The per-attempt audit log for `activity_name` (retryable failures live
    /// here, not in the event log).
    pub fn activity_attempts(
        &self,
        exec: ExecutionId,
        activity_name: &str,
    ) -> Result<Vec<ActivityAttempt>, SpikeError> {
        store::load_attempts(&self.conn, exec, activity_name)
    }

    /// Introspection (tests): the `activity_id`s of every task-queue row (any
    /// state) for `exec`. Paired with [`Self::load_history`] this asserts the
    /// per-cycle append+enqueue atomicity — every `ActivityScheduled` event has a
    /// matching `spike_tasks` row, and a rolled-back batch leaves neither.
    pub fn queued_activity_ids(&self, exec: ExecutionId) -> Result<Vec<String>, SpikeError> {
        queue::all_task_activity_ids(&self.conn, exec)
    }

    /// Introspection (tests): the `timer_id`s of every armed (unfired) durable
    /// timer for `exec`. The timer half of the same append+arm atomicity
    /// invariant — every `TimerStarted` event has a matching `spike_timers` row.
    pub fn armed_timer_ids(&self, exec: ExecutionId) -> Result<Vec<String>, SpikeError> {
        queue::armed_timer_ids(&self.conn, exec)
    }

    /// Introspection (tests): the `timer_id`s of every *fired* durable timer for
    /// `exec`. Paired with [`Self::load_history`]'s `TimerFired` events this
    /// asserts timer-fire atomicity — the `TimerFired` append and its `fired = 1`
    /// flag commit together, so a reload never re-fires an already-fired timer
    /// (which would append a stray duplicate `TimerFired`).
    pub fn fired_timer_ids(&self, exec: ExecutionId) -> Result<Vec<String>, SpikeError> {
        queue::fired_timer_ids(&self.conn, exec)
    }

    /// Drive `exec` to a terminal state or an external-input block.
    pub async fn run_until_blocked(&mut self, exec: ExecutionId) -> Result<RunState, SpikeError> {
        for _ in 0..MAX_ITERATIONS {
            match self.drive_one_cycle(exec).await? {
                RunState::InProgress => {}
                terminal_or_blocked => return Ok(terminal_or_blocked),
            }
        }
        Err(SpikeError::Stuck)
    }

    /// Run exactly one decision cycle: replay/execute the workflow once, persist
    /// any resulting side effects, and run one worker pass. Returns the resulting
    /// [`RunState`] (`InProgress` when durable progress was made but the run is
    /// neither terminal nor blocked).
    pub async fn drive_one_cycle(&mut self, exec: ExecutionId) -> Result<RunState, SpikeError> {
        // Already terminal? Return the stored outcome without re-running.
        match store::execution_state(&self.conn, exec)?.as_str() {
            "COMPLETED" => {
                let out = store::execution_output(&self.conn, exec)?.unwrap_or(Value::Null);
                return Ok(RunState::Completed(out));
            }
            "FAILED" => {
                let err = store::execution_error(&self.conn, exec)?.unwrap_or_default();
                return Ok(RunState::Failed(err));
            }
            _ => {}
        }

        let history = store::load_history(&self.conn, exec)?;
        let input = store::execution_input(&self.conn, exec)?;
        let workflow_name = self.workflow_name_of(exec)?;
        let handler = *self
            .workflows
            .get(&workflow_name)
            .ok_or_else(|| SpikeError::UnknownWorkflow(workflow_name.clone()))?;

        // The reused, backend-neutral determinism core.
        let outcome = run_workflow(exec, history.clone(), handler, input).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                // Terminal event + execution-state flip in ONE transaction. A
                // crash between them would leave a `WorkflowCompleted` event in
                // history while the row stayed RUNNING — on reload the top-of-cycle
                // "already terminal?" check would miss it, re-run the workflow, and
                // append a SECOND `WorkflowCompleted` (duplicate terminal).
                let tx = self.conn.transaction()?;
                store::append_event(
                    &tx,
                    exec,
                    &WorkflowEvent::WorkflowCompleted {
                        output: output.clone(),
                    },
                )?;
                store::set_completed(&tx, exec, &output)?;
                tx.commit()?;
                Ok(RunState::Completed(output))
            }
            WorkflowOutcome::Failed { error, .. } => {
                // Terminal event + state flip in ONE transaction (same duplicate-
                // terminal hazard as the completed arm above).
                let tx = self.conn.transaction()?;
                store::append_event(&tx, exec, &WorkflowEvent::workflow_failed(error.clone()))?;
                store::set_failed(&tx, exec, &error)?;
                tx.commit()?;
                Ok(RunState::Failed(error))
            }
            WorkflowOutcome::ContinuedAsNew { .. } => {
                Err(SpikeError::Unsupported("ContinueAsNew".to_string()))
            }
            WorkflowOutcome::Suspended { commands } => {
                let applied = self.apply_commands(exec, &history, &commands)?;
                let drained =
                    worker::drain_ready(&mut self.conn, exec, self.clock, &self.activities)?;
                if applied || drained {
                    Ok(RunState::InProgress)
                } else {
                    self.classify_block(exec, &commands)
                }
            }
        }
    }

    /// Persist newly-requested side effects. Returns `true` if any new event was
    /// appended (i.e. progress was made this call).
    ///
    /// **Atomic per-decision-cycle persistence.** Every derived event append AND
    /// its paired task-queue / durable-timer row insert commit in **one** `SQLite`
    /// `BEGIN…COMMIT` transaction. This mirrors the Postgres engine, which
    /// persists an event and its task-queue row in a single transaction (see the
    /// [`worker`](crate::worker) persist paths). Without it, a crash *between*
    /// the event append and the row insert would leave history saying an
    /// activity/timer is scheduled while no `spike_tasks`/`spike_timers` row
    /// exists — on reload, replay re-derives a `WaitForActivity` whose wait
    /// branch enqueues nothing, wedging the run at [`SpikeError::Stuck`]. The
    /// single writer holds `SQLite`'s database write lock for the whole
    /// transaction, so the batch is simultaneously **serialized** (the
    /// SKIP-LOCKED substitute) and **atomic** (event + row commit together, or —
    /// if any command in the batch is unsupported and returns `Err`, dropping the
    /// uncommitted transaction — neither does).
    ///
    /// **No command is ever silently dropped.** The match is exhaustive (no `_`
    /// wildcard): each variant is either persisted (`ScheduleActivity`,
    /// `StartTimer`, `WaitForSignal`, and the bookkeeping `RecordSideEffect` /
    /// `RecordMarker`), a single documented no-op (`WaitForActivity`, whose work
    /// the worker pass drains), or an explicit `SpikeError::Unsupported` that rolls
    /// the batch back. In particular the deterministic side-effect commands
    /// (issue #384) are persisted as their `SideEffectRecorded` / `MarkerRecorded`
    /// events so replay recovers the recorded value at the same cursor position
    /// rather than diverging on the following scheduled-work event.
    // The exhaustive `WorkflowCommand` match (every variant enumerated so no
    // command is silently dropped) is intentionally long; the persist arms are
    // each short and linear.
    #[allow(clippy::too_many_lines)]
    fn apply_commands(
        &mut self,
        exec: ExecutionId,
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
    ) -> Result<bool, SpikeError> {
        // Copy the Copy clock before borrowing `self.conn` mutably for the tx.
        let clock = self.clock;
        let tx = self.conn.transaction()?;
        let mut produced = false;
        for cmd in commands {
            match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name,
                    input,
                    queue,
                    ..
                } => {
                    let id = activity_id.to_string();
                    if !store::history_has_activity_scheduled(history, &id) {
                        // Event append + task-queue row insert in the SAME tx.
                        store::append_event(
                            &tx,
                            exec,
                            &WorkflowEvent::ActivityScheduled {
                                activity_id: *activity_id,
                                name: name.clone(),
                                input: input.clone(),
                                queue: queue.clone(),
                            },
                        )?;
                        queue::enqueue_activity(
                            &tx,
                            exec,
                            *activity_id,
                            name,
                            input,
                            queue,
                            clock,
                        )?;
                        produced = true;
                    }
                }
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => {
                    let id = timer_id.to_string();
                    if !store::history_has_timer_started(history, &id) {
                        // Event append + durable-timer row insert in the SAME tx.
                        store::append_event(
                            &tx,
                            exec,
                            &WorkflowEvent::TimerStarted {
                                timer_id: timer_id.clone(),
                                duration_secs: *duration_secs,
                            },
                        )?;
                        // Checked add: a pathological duration must not panic —
                        // saturate to `i64::MAX` (an effectively-infinite deadline).
                        let fire_at = clock
                            .checked_add(i64::try_from(*duration_secs).unwrap_or(i64::MAX))
                            .unwrap_or(i64::MAX);
                        queue::enqueue_timer(&tx, exec, &id, fire_at)?;
                        produced = true;
                    }
                }
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    if let Some(payload) = store::take_pending_signal(&tx, exec, signal_name)? {
                        store::append_event(
                            &tx,
                            exec,
                            &WorkflowEvent::SignalReceived {
                                signal_name: signal_name.clone(),
                                payload,
                            },
                        )?;
                        produced = true;
                    }
                }
                // Deterministic side-effect capture (issue #384): the primitives
                // `system_now()`/`new_uuid()`/`random_*()`/`side_effect()` emit a
                // `RecordSideEffect` (often BEFORE a suspending activity/timer/
                // signal in the same batch). It MUST be persisted as a
                // `SideEffectRecorded` so the next replay returns the recorded value
                // at the same cursor position — dropping it makes the matcher find
                // the following `ActivityScheduled`/`TimerStarted` where it expects
                // the side effect, diverging replay (or re-minting a fresh value).
                // No idempotency guard is needed: the reused core emits this ONLY
                // when running live (past end of history) and replays it from
                // history thereafter, so a committed side-effect event is never
                // re-emitted. Appended in command order, so it precedes any
                // activity/timer scheduled later in the same batch.
                WorkflowCommand::RecordSideEffect { kind, name, value } => {
                    store::append_event(
                        &tx,
                        exec,
                        &WorkflowEvent::SideEffectRecorded {
                            kind: *kind,
                            name: name.clone(),
                            value: value.clone(),
                        },
                    )?;
                    produced = true;
                }
                // Version-gate / opaque markers (`ctx.version()` etc.). Same
                // contract as `RecordSideEffect`: persist as `MarkerRecorded` so
                // replay reads it at the recorded cursor position rather than
                // diverging on the following scheduled-work event.
                WorkflowCommand::RecordMarker { name, details } => {
                    store::append_event(
                        &tx,
                        exec,
                        &WorkflowEvent::MarkerRecorded {
                            name: name.clone(),
                            details: details.clone(),
                        },
                    )?;
                    produced = true;
                }
                // The ONLY genuine no-op: a re-park of an already-scheduled
                // activity. The worker pass (`drain_ready`) runs the already-
                // enqueued task to completion; there is nothing to persist here.
                WorkflowCommand::WaitForActivity { .. } => {}
                // Every remaining command is outside the spike's supported subset.
                // Enumerated EXPLICITLY (no `_` wildcard) so adding a new engine
                // `WorkflowCommand` variant fails the spike compile and forces an
                // explicit decision rather than silently dropping the command —
                // the strongest "never lose a command" guarantee. Returning `Err`
                // drops the un-committed `tx`, rolling back the whole batch
                // (both-or-neither). `Complete`/`Fail` normally surface via
                // `WorkflowOutcome` in `drive_one_cycle`, never reaching here.
                WorkflowCommand::StartChildWorkflow { .. }
                | WorkflowCommand::ScheduleExternalActivity { .. }
                | WorkflowCommand::Complete { .. }
                | WorkflowCommand::Fail { .. }
                | WorkflowCommand::ContinueAsNew { .. }
                | WorkflowCommand::RunLocalActivity { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
                | WorkflowCommand::SetCurrentDetails { .. }
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                | WorkflowCommand::SignalExternalWorkflow { .. }
                | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                | WorkflowCommand::CancelRaceLosers { .. }
                | WorkflowCommand::ArmTimer { .. }
                | WorkflowCommand::CancelTimer { .. } => {
                    return Err(SpikeError::Unsupported(command_name(cmd).to_string()));
                }
            }
        }
        tx.commit()?;
        Ok(produced)
    }

    /// Classify why a no-progress cycle is blocked.
    ///
    /// A no-progress `Suspended` cycle is only a legitimate *wait* for one of two
    /// reasons: an undelivered signal, or a not-yet-due durable timer. We check
    /// each against ground truth rather than defaulting: a `WaitForSignal` command
    /// names the awaited signal, and an armed-but-unfired row in `spike_timers` is
    /// the proof of a pending timer (`drain_ready` already fired every *due* timer
    /// this cycle, so any survivor is genuinely not yet due). If neither holds the
    /// run made no progress and cannot be classified — an unsupported primitive or
    /// a task stranded `RUNNING` by a crash mid-drain — which is exactly the
    /// [`SpikeError::Stuck`] condition, surfaced honestly instead of being
    /// mislabelled `WaitingTimer`.
    fn classify_block(
        &self,
        exec: ExecutionId,
        commands: &[WorkflowCommand],
    ) -> Result<RunState, SpikeError> {
        for cmd in commands {
            if let WorkflowCommand::WaitForSignal { signal_name, .. } = cmd {
                return Ok(RunState::WaitingSignal(signal_name.clone()));
            }
        }
        if queue::has_unfired_timer(&self.conn, exec)? {
            return Ok(RunState::WaitingTimer);
        }
        Err(SpikeError::Stuck)
    }

    fn workflow_name_of(&self, exec: ExecutionId) -> Result<String, SpikeError> {
        Ok(self.conn.query_row(
            "SELECT workflow_name FROM spike_executions WHERE exec_id = ?1",
            rusqlite::params![exec.to_string()],
            |row| row.get(0),
        )?)
    }
}

/// Human name for a command (for `Unsupported` error messages). Exhaustive (no
/// `_` wildcard) so a newly-added engine `WorkflowCommand` variant surfaces a
/// real name in the error rather than a silent `"Unknown"`.
const fn command_name(cmd: &WorkflowCommand) -> &'static str {
    match cmd {
        WorkflowCommand::ScheduleActivity { .. } => "ScheduleActivity",
        WorkflowCommand::WaitForActivity { .. } => "WaitForActivity",
        WorkflowCommand::StartTimer { .. } => "StartTimer",
        WorkflowCommand::StartChildWorkflow { .. } => "StartChildWorkflow",
        WorkflowCommand::RecordMarker { .. } => "RecordMarker",
        WorkflowCommand::RecordSideEffect { .. } => "RecordSideEffect",
        WorkflowCommand::ScheduleExternalActivity { .. } => "ScheduleExternalActivity",
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
        WorkflowCommand::RecordUpdateResult { .. } => "RecordUpdateResult",
        WorkflowCommand::UpsertSearchAttributes { .. } => "UpsertSearchAttributes",
        WorkflowCommand::SetCurrentDetails { .. } => "SetCurrentDetails",
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => "SpawnDetachedChildWorkflow",
        WorkflowCommand::SignalExternalWorkflow { .. } => "SignalExternalWorkflow",
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => "RequestCancelExternalWorkflow",
        WorkflowCommand::CancelRaceLosers { .. } => "CancelRaceLosers",
        WorkflowCommand::ArmTimer { .. } => "ArmTimer",
        WorkflowCommand::CancelTimer { .. } => "CancelTimer",
    }
}
