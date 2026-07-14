//! The embedded, single-writer runtime: registration, start/signal ingress, the
//! decision loop, and the two atomic-persist sites (dispatch + terminal).

use std::collections::HashMap;
use std::path::Path;

use autumn_harvest::{
    ActivityExecId, ExecutionId, TimerId, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn,
    WorkflowInfo, WorkflowOutcome, run_workflow,
};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::{queue, schema, store, worker};

/// The activity body a workflow's `execute_activity_raw(name, ...)` resolves to.
///
/// Synchronous and keyed by name. The valuable, reused part of the engine is the
/// workflow orchestration + deterministic replay; activity execution is a simple
/// registered closure (no `ActivityContext`, no I/O framework).
pub type ActivityBody = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// A registered activity: its body plus how many attempts the worker may make.
pub struct ActivitySpec {
    // `ActivitySpec` is re-exported publicly; these fields are crate-internal
    // (read by the worker). `pub` would leak them onto the public type, so
    // `pub(crate)` is deliberate and NOT redundant despite the private module.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) body: ActivityBody,
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) max_attempts: u32,
}

impl ActivitySpec {
    /// Register an activity body that may be attempted up to `max_attempts` times
    /// (`1` = no retry; values below `1` are clamped to `1`).
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

/// The stored, non-driving outcome of an execution — a pure read accessor.
#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    /// Still non-terminal.
    Running,
    /// Completed with this output.
    Completed(Value),
    /// Failed with this error.
    Failed(String),
}

/// Maximum driver iterations before declaring a runaway (safety bound only — the
/// supported scenarios converge in a handful of cycles).
const MAX_ITERATIONS: usize = 10_000;

/// A single-writer, single-server, embedded workflow runtime backed by one `SQLite`
/// database.
///
/// All durable state lives in the database, so dropping a runtime and re-opening
/// the same file is a faithful crash/restart: [`open`](Self::open) reclaims any
/// task stranded `RUNNING` by the previous process (see the crash model in
/// [`worker`](crate::worker)) and the workflow resumes purely by deterministic
/// replay of the recorded history.
pub struct SqliteRuntime {
    conn: Connection,
    workflows: HashMap<String, WorkflowHandlerFn>,
    activities: HashMap<String, ActivitySpec>,
}

impl SqliteRuntime {
    /// Open (creating if absent) the `SQLite` database at `path`, applying the
    /// schema idempotently and reclaiming any orphaned `RUNNING` task from a
    /// previous process (crash recovery; makes activities at-least-once).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened, the schema
    /// cannot be applied, or the orphan reclaim fails.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Open a private, in-memory database (each call is a fresh, empty database —
    /// not reopen-safe; use [`open`](Self::open) with a file path for restart
    /// semantics).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the in-memory database cannot be
    /// created or the schema cannot be applied.
    pub fn open_in_memory() -> SqliteResult<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> SqliteResult<Self> {
        conn.execute_batch(schema::SCHEMA)?;
        // Single-server crash recovery: any task left `RUNNING` was claimed by a
        // process that exited without finalizing — flip it back to `PENDING` so
        // its body re-runs (at-least-once).
        queue::reclaim_orphaned_running(&conn)?;
        Ok(Self {
            conn,
            workflows: HashMap::new(),
            activities: HashMap::new(),
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

    /// Start a fresh workflow execution, appending its `WorkflowStarted` event.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::UnknownWorkflow`] if `workflow_name` has no
    /// registered handler, or [`SqliteError::Sqlite`]/[`SqliteError::Json`] on a
    /// persistence failure.
    pub fn start_workflow(
        &mut self,
        workflow_name: &str,
        input: Value,
    ) -> SqliteResult<ExecutionId> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(SqliteError::UnknownWorkflow(workflow_name.to_string()));
        }
        let exec = ExecutionId::new();
        let tx = self.conn.transaction()?;
        store::insert_execution(&tx, exec, workflow_name, &input)?;
        store::append_event(
            &tx,
            exec,
            &WorkflowEvent::workflow_started(input, Utc::now()),
        )?;
        tx.commit()?;
        Ok(exec)
    }

    /// Deliver an inbound signal (staged until a `wait_for_signal` consumes it).
    ///
    /// # Errors
    ///
    /// Returns a persistence error if the signal row cannot be written.
    // Owned-value ingress API (see `import_execution`): `payload` is serialized
    // into durable storage, borrowed at the leaf.
    #[allow(clippy::needless_pass_by_value)]
    pub fn send_signal(
        &mut self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
    ) -> SqliteResult<()> {
        store::stage_signal(&self.conn, exec, name, &payload)
    }

    /// The full ordered event history — the canonical, replayable log.
    ///
    /// # Errors
    ///
    /// Returns a persistence error if the history cannot be read or a stored
    /// event cannot be parsed.
    pub fn load_history(&self, exec: ExecutionId) -> SqliteResult<Vec<WorkflowEvent>> {
        store::load_history(&self.conn, exec)
    }

    /// The per-attempt audit log for `activity_name` (retryable failures live
    /// here, not in the replayable event log).
    ///
    /// # Errors
    ///
    /// Returns a persistence error if the audit rows cannot be read.
    pub fn activity_attempts(
        &self,
        exec: ExecutionId,
        activity_name: &str,
    ) -> SqliteResult<Vec<store::ActivityAttempt>> {
        store::load_attempts(&self.conn, exec, activity_name)
    }

    /// The stored, non-driving outcome of `exec` (a pure read; does not advance
    /// the workflow).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// persistence error.
    pub fn outcome(&self, exec: ExecutionId) -> SqliteResult<ExecutionOutcome> {
        if !store::execution_exists(&self.conn, exec)? {
            return Err(SqliteError::ExecutionNotFound(exec));
        }
        match store::execution_state(&self.conn, exec)?.as_str() {
            "COMPLETED" => Ok(ExecutionOutcome::Completed(
                store::execution_output(&self.conn, exec)?.unwrap_or(Value::Null),
            )),
            "FAILED" => Ok(ExecutionOutcome::Failed(
                store::execution_error(&self.conn, exec)?.unwrap_or_default(),
            )),
            _ => Ok(ExecutionOutcome::Running),
        }
    }

    // ── Drivers ──────────────────────────────────────────────────────────────

    /// Drive `exec` to a terminal state or an external-input block, using the
    /// real wall clock ([`Utc::now`]) to decide which timers are due.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Stuck`] if the run makes no progress and cannot be
    /// classified, [`SqliteError::Unsupported`] for an unsupported command, or a
    /// persistence error.
    pub async fn run_until_blocked(&mut self, exec: ExecutionId) -> SqliteResult<RunState> {
        self.run_until_blocked_as_of(exec, Utc::now()).await
    }

    /// Like [`run_until_blocked`](Self::run_until_blocked) but with an injected
    /// "as-of" time, so tests can arm a durable timer, restart, and fire it past
    /// its absolute deadline without sleeping. The runtime stores each timer's
    /// deadline as an absolute epoch second (`now + duration`) at arm time, so
    /// this is naturally monotonic across restarts — there is no virtual clock to
    /// persist or reset.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    pub async fn run_until_blocked_as_of(
        &mut self,
        exec: ExecutionId,
        now: DateTime<Utc>,
    ) -> SqliteResult<RunState> {
        let now = now.timestamp();
        for _ in 0..MAX_ITERATIONS {
            match self.drive_one_cycle(exec, now).await? {
                RunState::InProgress => {}
                terminal_or_blocked => return Ok(terminal_or_blocked),
            }
        }
        Err(SqliteError::Stuck(exec))
    }

    /// Drive every non-terminal execution one cycle, using the real wall clock.
    /// Returns `true` if any execution made durable progress this pass.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    pub async fn poll_once(&mut self) -> SqliteResult<bool> {
        self.poll_once_as_of(Utc::now()).await
    }

    /// Like [`poll_once`](Self::poll_once) but with an injected "as-of" time.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    pub async fn poll_once_as_of(&mut self, now: DateTime<Utc>) -> SqliteResult<bool> {
        let now = now.timestamp();
        let mut progress = false;
        for exec in store::running_executions(&self.conn)? {
            match self.drive_one_cycle(exec, now).await? {
                RunState::WaitingSignal(_) | RunState::WaitingTimer => {}
                _ => progress = true,
            }
        }
        Ok(progress)
    }

    /// Repeatedly [`poll_once`](Self::poll_once) until the fleet is quiescent (no
    /// execution makes progress — every remaining run is terminal or blocked on
    /// an external input).
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    pub async fn run_until_idle(&mut self) -> SqliteResult<()> {
        for _ in 0..MAX_ITERATIONS {
            if !self.poll_once().await? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Run exactly one decision cycle at logical time `now` (epoch seconds):
    /// replay/execute the workflow once, persist the resulting side effects, and
    /// run one worker pass.
    async fn drive_one_cycle(&mut self, exec: ExecutionId, now: i64) -> SqliteResult<RunState> {
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
        let workflow_name = store::workflow_name_of(&self.conn, exec)?;
        let handler = *self
            .workflows
            .get(&workflow_name)
            .ok_or_else(|| SqliteError::UnknownWorkflow(workflow_name.clone()))?;

        // The reused, backend-neutral determinism core.
        let outcome = run_workflow(exec, history.clone(), handler, input).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
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
                let tx = self.conn.transaction()?;
                store::append_event(&tx, exec, &WorkflowEvent::workflow_failed(error.clone()))?;
                store::set_failed(&tx, exec, &error)?;
                tx.commit()?;
                Ok(RunState::Failed(error))
            }
            WorkflowOutcome::ContinuedAsNew { .. } => {
                Err(SqliteError::Unsupported("ContinueAsNew".to_string()))
            }
            WorkflowOutcome::Suspended { commands } => {
                let applied = self.apply_commands(exec, &history, &commands, now)?;
                let drained = worker::drain_ready(&mut self.conn, exec, now, &self.activities)?;
                if applied || drained {
                    Ok(RunState::InProgress)
                } else {
                    self.classify_block(exec, &commands)
                }
            }
        }
    }

    /// Persist newly-requested side effects for one decision cycle.
    ///
    /// **Atomic per-decision-cycle persistence (AC6).** Every derived event append
    /// AND its paired task-queue / durable-timer row insert commit in **one**
    /// `SQLite` `BEGIN…COMMIT` transaction, mirroring the Postgres engine (which
    /// persists an event and its task-queue row in a single transaction). Without
    /// it, a crash *between* the event append and the row insert would leave
    /// history saying an activity/timer is scheduled while no queue/timer row
    /// exists — on reload, replay re-derives a wait whose branch enqueues nothing,
    /// wedging the run at [`SqliteError::Stuck`]. The single writer holds `SQLite`'s
    /// database write lock for the whole transaction, so the batch is
    /// simultaneously **serialized** (the `SKIP LOCKED` substitute) and **atomic**
    /// (event + row commit together, or — if any command in the batch is
    /// unsupported and returns `Err`, dropping the uncommitted transaction —
    /// neither does).
    fn apply_commands(
        &mut self,
        exec: ExecutionId,
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
        now: i64,
    ) -> SqliteResult<bool> {
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
                    if !store::history_has_activity_scheduled(history, &activity_id.to_string()) {
                        persist_scheduled_activity(
                            &tx,
                            exec,
                            *activity_id,
                            name,
                            input,
                            queue,
                            now,
                        )?;
                        produced = true;
                    }
                }
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => {
                    if !store::history_has_timer_started(history, &timer_id.to_string()) {
                        // Absolute deadline computed from `now` (real wall clock in
                        // the public API), so it stays valid across a restart —
                        // there is no virtual clock to reset. Checked add: a
                        // pathological duration saturates to an effectively-infinite
                        // deadline instead of panicking.
                        let fire_at = now
                            .checked_add(i64::try_from(*duration_secs).unwrap_or(i64::MAX))
                            .unwrap_or(i64::MAX);
                        persist_started_timer(&tx, exec, timer_id, *duration_secs, fire_at)?;
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
                // A re-park of an already-scheduled activity needs nothing
                // persisted — the worker pass completes it.
                WorkflowCommand::WaitForActivity { .. } => {}
                // Bookkeeping commands that carry an event MUST be persisted, even
                // when they ride in the same batch as a suspending command: a
                // dropped `SideEffectRecorded`/`MarkerRecorded` would make the next
                // replay re-mint a different value (e.g. `ctx.new_uuid()`) or
                // diverge. They join the SAME atomic cycle transaction. The core
                // only emits these at the live frontier (a replayed side effect is
                // matched, not re-emitted), so each is appended exactly once.
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
                // Dropping `tx` here (un-committed) rolls back the whole batch — an
                // unsupported command after a supported one leaves NEITHER the
                // supported command's event NOR its queue/timer row persisted.
                other => return Err(SqliteError::Unsupported(command_name(other).to_string())),
            }
        }
        tx.commit()?;
        Ok(produced)
    }

    /// Classify why a no-progress cycle is blocked.
    ///
    /// A no-progress `Suspended` cycle is a legitimate *wait* for one of two
    /// reasons: an undelivered signal, or a not-yet-due durable timer. Each is
    /// checked against ground truth: a `WaitForSignal` command names the awaited
    /// signal, and an armed-but-unfired row in `harvest_timers` proves a pending
    /// timer (`drain_ready` already fired every *due* timer this cycle, so any
    /// survivor is genuinely not yet due). If neither holds the run made no
    /// progress and cannot be classified — an unsupported primitive, or a task
    /// stranded `RUNNING` by a crash that reclaim did not clear — which is exactly
    /// [`SqliteError::Stuck`], surfaced honestly instead of mislabelled
    /// `WaitingTimer`.
    fn classify_block(
        &self,
        exec: ExecutionId,
        commands: &[WorkflowCommand],
    ) -> SqliteResult<RunState> {
        for cmd in commands {
            if let WorkflowCommand::WaitForSignal { signal_name, .. } = cmd {
                return Ok(RunState::WaitingSignal(signal_name.clone()));
            }
        }
        if queue::has_unfired_timer(&self.conn, exec)? {
            return Ok(RunState::WaitingTimer);
        }
        Err(SqliteError::Stuck(exec))
    }
}

/// Atomic dispatch pair (AC6): append `ActivityScheduled` **and** insert the
/// task-queue row on the same connection/transaction. Callers pass a
/// `rusqlite::Transaction` (via `&tx`) so a crash cannot land the event without
/// the row.
pub fn persist_scheduled_activity(
    conn: &Connection,
    exec: ExecutionId,
    activity_id: ActivityExecId,
    name: &str,
    input: &Value,
    queue_name: &str,
    run_at: i64,
) -> SqliteResult<()> {
    store::append_event(
        conn,
        exec,
        &WorkflowEvent::ActivityScheduled {
            activity_id,
            name: name.to_string(),
            input: input.clone(),
            queue: queue_name.to_string(),
        },
    )?;
    queue::enqueue_activity(conn, exec, activity_id, name, input, queue_name, run_at)?;
    Ok(())
}

/// Atomic dispatch pair (AC6): append `TimerStarted` **and** insert the
/// durable-timer row on the same connection/transaction.
pub fn persist_started_timer(
    conn: &Connection,
    exec: ExecutionId,
    timer_id: &TimerId,
    duration_secs: u64,
    fire_at: i64,
) -> SqliteResult<()> {
    store::append_event(
        conn,
        exec,
        &WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs,
        },
    )?;
    queue::enqueue_timer(conn, exec, &timer_id.to_string(), fire_at)?;
    Ok(())
}

/// Human name for an unsupported command (for error messages). The `_` arm keeps
/// this total against future [`WorkflowCommand`] variants.
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

#[cfg(test)]
mod tests {
    use autumn_harvest::{ActivityExecId, ExecutionId, TimerId};
    use rusqlite::Connection;

    use super::{persist_scheduled_activity, persist_started_timer};
    use crate::{queue, schema, store};

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    fn scheduled_count(conn: &Connection, exec: ExecutionId) -> usize {
        store::load_history(conn, exec)
            .unwrap()
            .iter()
            .filter(|e| matches!(e, autumn_harvest::WorkflowEvent::ActivityScheduled { .. }))
            .count()
    }

    fn task_row_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM harvest_tasks", [], |r| r.get(0))
            .unwrap()
    }

    // AC6: the dispatch pair (ActivityScheduled event + task-queue row) rolls
    // back together — never a scheduled event without its row (which would wedge
    // the run at Stuck on reload).
    #[test]
    fn schedule_activity_pair_rolls_back_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();

        {
            let tx = conn.transaction().unwrap();
            persist_scheduled_activity(
                &tx,
                exec,
                ActivityExecId::new(),
                "act",
                &serde_json::json!({}),
                "default",
                0,
            )
            .unwrap();
            // Drop `tx` WITHOUT commit → rollback.
        }

        assert_eq!(scheduled_count(&conn, exec), 0, "event must roll back");
        assert_eq!(task_row_count(&conn), 0, "task row must roll back");
    }

    // AC6: on commit BOTH land together.
    #[test]
    fn schedule_activity_pair_commits_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();

        {
            let tx = conn.transaction().unwrap();
            persist_scheduled_activity(
                &tx,
                exec,
                ActivityExecId::new(),
                "act",
                &serde_json::json!({}),
                "default",
                0,
            )
            .unwrap();
            tx.commit().unwrap();
        }

        assert_eq!(scheduled_count(&conn, exec), 1);
        assert_eq!(task_row_count(&conn), 1);
    }

    // AC6: the timer dispatch pair (TimerStarted event + timer row) is likewise
    // atomic.
    #[test]
    fn start_timer_pair_rolls_back_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();

        {
            let tx = conn.transaction().unwrap();
            persist_started_timer(&tx, exec, &TimerId::new("t1"), 60, 60).unwrap();
        }

        assert!(store::load_history(&conn, exec).unwrap().is_empty());
        assert!(!queue::has_unfired_timer(&conn, exec).unwrap());
    }
}
