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
//!    signal), then run the [`worker`] pass to execute ready activities and fire
//!    due timers — appending their terminal events.
//! 3. Repeat until the run reports `Completed`/`Failed`, or until a full cycle
//!    makes no progress (blocked on a not-yet-due timer or an undelivered
//!    signal).
//!
//! A "crash" is modelled by dropping the runtime and its `SQLite` connection, then
//! opening a fresh [`SqliteRuntime`] on the same file: deterministic replay
//! reproduces the identical command stream from the durable history, so no
//! activity is re-executed and no work is lost.

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
        Ok(Self {
            conn,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            clock: 0,
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

    /// Advance the virtual clock by `secs` seconds (drives durable timers).
    pub const fn advance_time(&mut self, secs: i64) {
        self.clock += secs;
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
        store::insert_execution(&self.conn, exec, workflow_name, &input)?;
        store::append_event(
            &self.conn,
            exec,
            &WorkflowEvent::workflow_started(input, chrono::Utc::now()),
        )?;
        Ok(exec)
    }

    /// Seed an execution from an externally-produced history (e.g. one a
    /// Postgres backend would have written). Used to prove a PG-shaped history
    /// drives the prototype's own reload path identically.
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
        store::insert_execution(&self.conn, exec, workflow_name, &input)?;
        for event in &events {
            store::append_event(&self.conn, exec, event)?;
        }
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
                store::append_event(
                    &self.conn,
                    exec,
                    &WorkflowEvent::WorkflowCompleted {
                        output: output.clone(),
                    },
                )?;
                store::set_completed(&self.conn, exec, &output)?;
                Ok(RunState::Completed(output))
            }
            WorkflowOutcome::Failed { error, .. } => {
                store::append_event(
                    &self.conn,
                    exec,
                    &WorkflowEvent::workflow_failed(error.clone()),
                )?;
                store::set_failed(&self.conn, exec, &error)?;
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
                    Ok(Self::classify_block(&commands))
                }
            }
        }
    }

    /// Persist newly-requested side effects. Returns `true` if any new event was
    /// appended (i.e. progress was made this call).
    fn apply_commands(
        &self,
        exec: ExecutionId,
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
    ) -> Result<bool, SpikeError> {
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
                        store::append_event(
                            &self.conn,
                            exec,
                            &WorkflowEvent::ActivityScheduled {
                                activity_id: *activity_id,
                                name: name.clone(),
                                input: input.clone(),
                                queue: queue.clone(),
                            },
                        )?;
                        queue::enqueue_activity(
                            &self.conn,
                            exec,
                            *activity_id,
                            name,
                            input,
                            queue,
                            self.clock,
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
                        store::append_event(
                            &self.conn,
                            exec,
                            &WorkflowEvent::TimerStarted {
                                timer_id: timer_id.clone(),
                                duration_secs: *duration_secs,
                            },
                        )?;
                        let fire_at =
                            self.clock + i64::try_from(*duration_secs).unwrap_or(i64::MAX);
                        queue::enqueue_timer(&self.conn, exec, &id, fire_at)?;
                        produced = true;
                    }
                }
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    if let Some(payload) =
                        store::take_pending_signal(&self.conn, exec, signal_name)?
                    {
                        store::append_event(
                            &self.conn,
                            exec,
                            &WorkflowEvent::SignalReceived {
                                signal_name: signal_name.clone(),
                                payload,
                            },
                        )?;
                        produced = true;
                    }
                }
                // A re-park of an already-scheduled activity (`WaitForActivity`)
                // needs nothing persisted — the worker pass completes it — and
                // the spike scenarios never emit `RecordMarker`/`RecordSideEffect`
                // bookkeeping, so all three are harmless no-ops.
                WorkflowCommand::WaitForActivity { .. }
                | WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordSideEffect { .. } => {}
                other => return Err(SpikeError::Unsupported(command_name(other).to_string())),
            }
        }
        Ok(produced)
    }

    /// Classify why a no-progress cycle is blocked.
    fn classify_block(commands: &[WorkflowCommand]) -> RunState {
        for cmd in commands {
            if let WorkflowCommand::WaitForSignal { signal_name, .. } = cmd {
                return RunState::WaitingSignal(signal_name.clone());
            }
        }
        // The only other blocking primitive in scope is a not-yet-due timer.
        RunState::WaitingTimer
    }

    fn workflow_name_of(&self, exec: ExecutionId) -> Result<String, SpikeError> {
        Ok(self.conn.query_row(
            "SELECT workflow_name FROM spike_executions WHERE exec_id = ?1",
            rusqlite::params![exec.to_string()],
            |row| row.get(0),
        )?)
    }
}

/// Human name for an unsupported command (for error messages).
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
        _ => "Unknown",
    }
}
