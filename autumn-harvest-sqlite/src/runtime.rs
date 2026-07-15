//! The embedded, single-writer runtime: registration, start/signal ingress, the
//! decision loop, and the two atomic-persist sites (dispatch + terminal).

use std::collections::HashMap;
use std::path::Path;

use autumn_harvest::builder::{
    DEFAULT_MAX_ACTIVITY_INPUT_BYTES, DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
    DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
};
use autumn_harvest::context::{DEFAULT_CURRENT_DETAILS_CAP_BYTES, empty_shared_state};
use autumn_harvest::executor::run_workflow_with_state_history_policy_and_caps;
use autumn_harvest::{
    ActivityExecId, ExecutionId, NoOpMetrics, TimerId, WorkflowCommand, WorkflowEvent,
    WorkflowHandlerFn, WorkflowHistoryPolicy, WorkflowInfo, WorkflowOutcome,
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

/// Bounded consecutive-panic budget for a `#[workflow]` handler that panics
/// (issue #782 analog). A contained workflow-handler panic re-drives the
/// UNCHANGED history up to this many times before the run is sealed terminally
/// `FAILED` — buying time for a hotfix/rollback rather than permanently closing a
/// transiently-panicking or fixable run on the first strike. With `3`, strikes 1
/// and 2 are non-terminal (surfaced as [`SqliteError::WorkflowPanicked`]) and
/// strike 3 seals the run. Mirrors the Postgres worker's default
/// `workflow_panic_max_attempts`.
const WORKFLOW_PANIC_MAX_ATTEMPTS: u32 = 3;

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
    /// Consecutive contained-workflow-panic strike counter, keyed by execution
    /// (issue #782 analog). In-process (reset on restart, like the Postgres
    /// worker's strike map); incremented on each panicked decision cycle and
    /// cleared on any non-panic outcome, so only *consecutive* panics count
    /// against [`WORKFLOW_PANIC_MAX_ATTEMPTS`].
    workflow_panic_strikes: HashMap<ExecutionId, u32>,
    /// Wall-clock source for the public drivers (issue #1069 P2). Read **once per
    /// decision cycle** inside [`run_until_blocked`](Self::run_until_blocked) /
    /// [`poll_once`](Self::poll_once) so a timer armed in a later cycle (e.g. after
    /// a real-time activity) anchors its absolute deadline to its true arm time,
    /// not to the start of the outer call. Defaults to [`Utc::now`]; tests install
    /// a controllable clock via [`set_clock`](Self::set_clock). The `_as_of`
    /// drivers bypass it entirely (they take a caller-fixed timestamp — the
    /// deterministic-simulation seam).
    now_fn: NowFn,
}

/// A wall-clock source for the public drivers — see [`SqliteRuntime::set_clock`].
///
/// `Fn`-based (not a stored timestamp) precisely so it can be re-read on every
/// decision cycle; `Arc` + `Send`/`Sync` keeps [`SqliteRuntime`] cheaply cloneable
/// in bound and usable across the async drivers (the closure is invoked
/// synchronously, never held across an `.await`).
type NowFn = std::sync::Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

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
        // Durability & concurrency posture (issue #1068). This backend makes a
        // deliberate, conservative durability claim (a committed transaction
        // survives a crash/power-loss — the crash-then-reopen tests depend on it),
        // so make it explicit rather than leaning on driver defaults:
        // - `synchronous = FULL`: fsync on every COMMIT. WAL + FULL is fully
        //   durable (every commit is flushed), just reader-friendly.
        // - `journal_mode = WAL`: the idiomatic embedded choice for a single
        //   writer plus an occasional external reader (a monitoring/inspector
        //   connection coexists with the writer without an immediate
        //   `SQLITE_BUSY`). Persisted in the file header; a harmless no-op on an
        //   in-memory database (always "memory"-journalled).
        // - `busy_timeout = 5000` (ms): a second connection that races the
        //   writer's `BEGIN IMMEDIATE` claim lock retries briefly instead of
        //   failing instantly with `SQLITE_BUSY`.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;",
        )?;
        conn.execute_batch(schema::SCHEMA)?;
        // Single-server crash recovery: any task left `RUNNING` was claimed by a
        // process that exited without finalizing — flip it back to `PENDING` so
        // its body re-runs (at-least-once).
        queue::reclaim_orphaned_running(&conn)?;
        Ok(Self {
            conn,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            workflow_panic_strikes: HashMap::new(),
            now_fn: std::sync::Arc::new(Utc::now),
        })
    }

    /// Install a controllable wall-clock source for the public drivers
    /// ([`run_until_blocked`](Self::run_until_blocked) /
    /// [`poll_once`](Self::poll_once) / [`run_until_idle`](Self::run_until_idle)),
    /// which read it **once per decision cycle**. A deterministic-test seam only —
    /// production uses the [`Utc::now`] default. Prefer the `_as_of` drivers for a
    /// caller-fixed single timestamp; use this to assert the *public* path re-reads
    /// the clock per cycle (e.g. an advancing clock proving a post-activity timer
    /// arms at its true, later instant).
    #[doc(hidden)]
    pub fn set_clock(&mut self, now_fn: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) {
        self.now_fn = std::sync::Arc::new(now_fn);
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
    // Owned-value ingress API: callers hand over a `serde_json::Value` payload
    // that is serialized into durable storage; `store::stage_signal` only borrows
    // it at the leaf, so taking it by value keeps the call site ergonomic
    // (`rt.send_signal(exec, "go", json!({..}))`) rather than forcing the caller
    // to keep the value alive.
    #[allow(clippy::needless_pass_by_value)]
    pub fn send_signal(
        &mut self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
    ) -> SqliteResult<()> {
        self.send_signal_as_of(exec, name, payload, Utc::now())
    }

    /// Like [`send_signal`](Self::send_signal) but with an injected arrival time
    /// (`received_at`), so tests can deterministically place a signal before or
    /// after a `wait_for_signal_timeout` deadline without sleeping.
    ///
    /// A deterministic-simulation seam paired with the `*_as_of` drivers: the
    /// staged `received_at` is compared against a deadline timer's `fire_at` by the
    /// wake-event ingest (issue #476), so — exactly like the driver caveat on
    /// [`run_until_blocked_as_of`](Self::run_until_blocked_as_of) — do **not** mix a
    /// wall-clock `send_signal` with `*_as_of` drivers (or vice versa) for the
    /// *same* execution: the two clocks are incomparable, so a signal's arrival
    /// could order surprisingly against the deadline. (A pending signal only ever
    /// interleaves with a deadline timer that is already **due** at drive time, so
    /// a plain `wait_for_signal` and any wait driven *before* its deadline are
    /// unaffected by the clock choice.)
    ///
    /// # Errors
    ///
    /// Returns a persistence error if the signal row cannot be written.
    #[doc(hidden)]
    #[allow(clippy::needless_pass_by_value)]
    pub fn send_signal_as_of(
        &mut self,
        exec: ExecutionId,
        name: &str,
        payload: Value,
        received_at: DateTime<Utc>,
    ) -> SqliteResult<()> {
        // Validate the target BEFORE staging (Codex #1069 P2). `harvest_signals`
        // has no FK/state check, so an unconditional stage returns `Ok(())` for a
        // typoed id or a sealed run and silently loses the wakeup — no live
        // `wait_for_signal` will ever consume it. Mirror the Postgres `send_signal`
        // rejections (and the [`outcome`](Self::outcome) not-found pattern): an
        // unknown id → `ExecutionNotFound`; a terminal execution →
        // `WorkflowNotRunning`. A RUNNING (non-terminal) execution stages as before.
        //
        // The single-writer runtime holds `SQLite`'s only write handle, so the
        // state read and the stage cannot race a concurrent transition: no other
        // writer exists to seal the execution between the check and the insert.
        if !store::execution_exists(&self.conn, exec)? {
            return Err(SqliteError::ExecutionNotFound(exec));
        }
        let state = store::execution_state(&self.conn, exec)?;
        if autumn_harvest::erase::is_terminal_state(&state) {
            return Err(SqliteError::WorkflowNotRunning {
                execution_id: exec,
                state,
            });
        }
        // Millisecond precision (issue #1069 P2): `received_at` is compared against
        // a deadline timer's `fire_at` (both epoch-MILLISECOND) by the wake-event
        // ingest, so a whole-second floor could flip an on-time signal past its
        // deadline near a second boundary.
        store::stage_signal(
            &self.conn,
            exec,
            name,
            &payload,
            received_at.timestamp_millis(),
        )
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
    /// **Name-scoped:** the audit view keys on `(exec_id, activity_name)`, not the
    /// per-instance `ActivityExecId`. A workflow that schedules the same activity
    /// *name* more than once gets a single merged, attempt-ordered list across all
    /// those instances (each instance's `attempt` counter restarts at 1, so
    /// numbers can repeat). This is a debug/audit accessor; the replayable
    /// history in [`load_history`](Self::load_history) is the per-instance source
    /// of truth.
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
    /// real wall clock (re-read once **per decision cycle**) to decide which
    /// timers are due. Refreshing per cycle — rather than capturing one timestamp
    /// for the whole call — anchors a timer armed after a real-time activity to
    /// its true arm instant, so a `timer(1s)` following a 2s activity waits ~1s
    /// from when it was armed instead of firing immediately (issue #1069 P2).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Stuck`] if the run makes no progress and cannot be
    /// classified, [`SqliteError::Unsupported`] for an unsupported command, or a
    /// persistence error.
    pub async fn run_until_blocked(&mut self, exec: ExecutionId) -> SqliteResult<RunState> {
        // Re-read the wall clock at the START of EACH cycle (issue #1069 P2), NOT
        // once for the whole call: a cycle can consume real time (e.g. a 2s
        // activity body runs before the next cycle arms a timer), and a stale,
        // call-start `now` would anchor that later timer's absolute deadline to the
        // start of the call — arming a `timer(1s)` after a 2s activity already
        // overdue, so it fires immediately instead of ~1s after it was armed. The
        // `_as_of` seam keeps a caller-fixed `now` for deterministic tests; only
        // this public wall-clock wrapper refreshes.
        for _ in 0..MAX_ITERATIONS {
            let now = (self.now_fn)().timestamp_millis();
            match self.drive_one_cycle(exec, now).await? {
                RunState::InProgress => {}
                terminal_or_blocked => return Ok(terminal_or_blocked),
            }
        }
        Err(SqliteError::Stuck(exec))
    }

    /// Like [`run_until_blocked`](Self::run_until_blocked) but with an injected
    /// "as-of" time, so tests can arm a durable timer, restart, and fire it past
    /// its absolute deadline without sleeping. The runtime stores each timer's
    /// deadline as an absolute epoch millisecond (`now + duration`, issue #1069
    /// P2) at arm time, so this is naturally monotonic across restarts — there is
    /// no virtual clock to persist or reset — and never fires before the true
    /// sub-second deadline.
    ///
    /// A deterministic-simulation seam. Timers store an absolute epoch deadline,
    /// so an adversarial `now` only changes due-ness (fires early/late) and can
    /// never corrupt stored state — but do **not** mix `_as_of` and wall-clock
    /// drivers for the *same* execution: a timer armed under an injected `now`
    /// anchors its deadline to that test epoch, so a later wall-clock fire is
    /// surprising.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    #[doc(hidden)]
    pub async fn run_until_blocked_as_of(
        &mut self,
        exec: ExecutionId,
        now: DateTime<Utc>,
    ) -> SqliteResult<RunState> {
        // Millisecond precision (issue #1069 P2): a whole-second floor would arm a
        // timer up to ~1s early and could flip an on-time signal-vs-deadline race
        // near a second boundary. Every internal epoch value (`fire_at`,
        // `received_at`, `run_at`) is a relative epoch-MILLISECOND, so the whole
        // comparison stays self-consistent — no DDL change (the columns are
        // `INTEGER`).
        let now = now.timestamp_millis();
        for _ in 0..MAX_ITERATIONS {
            match self.drive_one_cycle(exec, now).await? {
                RunState::InProgress => {}
                terminal_or_blocked => return Ok(terminal_or_blocked),
            }
        }
        Err(SqliteError::Stuck(exec))
    }

    /// Drive every non-terminal execution one cycle, using the real wall clock
    /// (re-read per driven cycle, issue #1069 P2). Returns `true` if any execution
    /// made durable progress this pass.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    pub async fn poll_once(&mut self) -> SqliteResult<bool> {
        // Re-read the wall clock per driven cycle (issue #1069 P2) — see the
        // rationale on `run_until_blocked`. The `_as_of` variant keeps a fixed
        // caller `now` for deterministic simulation.
        let mut progress = false;
        for exec in store::running_executions(&self.conn)? {
            let now = (self.now_fn)().timestamp_millis();
            match self.drive_one_cycle(exec, now).await? {
                RunState::WaitingSignal(_) | RunState::WaitingTimer => {}
                _ => progress = true,
            }
        }
        Ok(progress)
    }

    /// Like [`poll_once`](Self::poll_once) but with an injected "as-of" time. A
    /// deterministic-simulation seam — see the caveat on
    /// [`run_until_blocked_as_of`](Self::run_until_blocked_as_of) about not mixing
    /// `_as_of` and wall-clock drivers for the same execution.
    ///
    /// # Errors
    ///
    /// See [`run_until_blocked`](Self::run_until_blocked).
    #[doc(hidden)]
    pub async fn poll_once_as_of(&mut self, now: DateTime<Utc>) -> SqliteResult<bool> {
        // Millisecond precision — see `run_until_blocked_as_of` (issue #1069 P2).
        let now = now.timestamp_millis();
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
    /// Returns [`SqliteError::Runaway`] if the fleet never quiesces within the
    /// [`MAX_ITERATIONS`] safety bound — surfaced honestly (mirroring
    /// [`run_until_blocked`](Self::run_until_blocked)'s [`SqliteError::Stuck`])
    /// rather than swallowed as a clean `Ok(())` a caller cannot distinguish from
    /// genuine quiescence. Also propagates any per-execution error (see
    /// [`run_until_blocked`](Self::run_until_blocked)).
    pub async fn run_until_idle(&mut self) -> SqliteResult<()> {
        for _ in 0..MAX_ITERATIONS {
            if !self.poll_once().await? {
                return Ok(());
            }
        }
        Err(SqliteError::Runaway)
    }

    /// Run exactly one decision cycle at logical time `now` (epoch milliseconds):
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

        // The reused, backend-neutral determinism core. Use the caps-carrying,
        // value-returning entry point rather than `run_workflow`: on a
        // Completed/Failed cycle it hands back the drained `pending` commands
        // (`RecordSideEffect`/`RecordMarker` emitted at the live frontier in the
        // SAME cycle the workflow returns) so we can persist them BEFORE the
        // terminal event. `run_workflow` discards that vector, silently dropping a
        // side effect emitted after the last suspension (e.g. `ctx.new_uuid()`
        // right before `Ok(..)`) and producing a history that fails the core
        // `WorkflowReplayer` with `SideEffectDrift`.
        //
        // FIX C (cross-backend fidelity, Codex #1069): thread the real
        // `workflow_name` into the context via the dedicated `workflow_name`
        // parameter (the one that populates `ctx.info().workflow_type`, issue #698 —
        // `span_meta.workflow_name` feeds only the OTel span, which this backend has
        // no worker for). The lighter `run_workflow_with_state` hard-codes it to
        // `""`, so a workflow that records/branches on the replay-safe
        // `ctx.info().workflow_type` would persist `""` here while the Postgres
        // worker injects the real handler name — a divergence on the very
        // byte-identity guarantee this crate makes. All other arguments reproduce
        // `run_workflow_with_state`'s own defaults exactly (unchanged behavior);
        // `span_meta` stays `None` (OTel-only) and there is no queue notion here.
        let (outcome, pending, _span) = run_workflow_with_state_history_policy_and_caps(
            exec,
            history.clone(),
            handler,
            input,
            empty_shared_state(),
            WorkflowHistoryPolicy::default(),
            None,           // span_meta — OTel span only; this backend has no OTel worker.
            &[],            // declarative query handlers (none on this backend)
            &[],            // declarative update handlers (none on this backend)
            &workflow_name, // FIX C: the load-bearing name → ctx.info().workflow_type
            DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            std::collections::HashMap::new(), // context headers (none)
            None,                             // payload offload threshold (none)
            std::sync::Arc::new(NoOpMetrics),
        )
        .await;

        // Contained workflow-handler panic gate (issue #782 analog), BEFORE any
        // terminal side effect and beside the non-determinism gate below. A
        // `#[workflow]` body that `panic!()`s (rather than returning `Err`) is
        // caught by the core at the dispatch boundary and surfaced as
        // `Failed { handler_panic: true }` (with an EMPTY `pending` — the panicked
        // cycle's commands are untrustworthy and already dropped by the executor).
        // Do NOT seal it `FAILED` on the first strike like an author `Err`: under
        // the bounded panic budget the divergent cycle is discarded (no event
        // appended, no bookkeeping persisted, state stays `RUNNING`) and the
        // divergence is surfaced so a hotfix/rollback can re-drive the UNCHANGED
        // history to completion. Only at/over budget is the run sealed with the
        // typed `HandlerPanic` error. Every OTHER (non-panic) outcome clears the
        // consecutive-panic strike so a later transient panic starts fresh — this
        // must run before the `match` so the ND/author-Err/completed/suspended arms
        // all reset it (mirrors the Postgres worker's clear-on-every-non-panic-path).
        if let WorkflowOutcome::Failed {
            handler_panic: true,
            error,
            ..
        } = &outcome
        {
            return self.contain_workflow_panic(exec, error);
        }
        self.workflow_panic_strikes.remove(&exec);

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                let tx = self.conn.transaction()?;
                // Persist terminal-cycle bookkeeping (issue #1068) BEFORE the
                // terminal event, in command order, inside the SAME transaction —
                // mirroring the Postgres `persist_terminal_outcome_commands`.
                persist_terminal_pending_commands(&tx, exec, &pending)?;
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
            // Engine-detected replay non-determinism is NON-TERMINAL (Codex
            // #1069 P1; mirrors the Postgres engine's issue #603 gate). When the
            // core returns `Failed { non_deterministic_details: Some(_) }` the
            // current workflow code drifted from the recorded history — a
            // recoverable code-deploy bug, NOT an author failure. DISCARD the
            // whole divergent decision cycle: append NO terminal event, persist
            // NONE of the cycle's pending bookkeeping commands (a marker from the
            // bad build would poison history against the rolled-back code), and
            // do NOT transition state. Leave the execution RUNNING/resumable and
            // surface the divergence to the caller (`SqliteError::NonDeterministic`)
            // rather than sealing it FAILED. `?` never runs, so `harvest_events`
            // for this execution is byte-identical to before the drive; a later
            // drive with a corrected (compatible) handler resumes from the same
            // unchanged history and completes normally. Propagating the error out
            // of the drive also terminates the decision loop, so
            // `run_until_blocked` / `poll_once` / `run_until_idle` cannot spin on
            // it — the operator fixes/rolls back the code and re-drives.
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details: Some(_),
                ..
            } => Err(SqliteError::NonDeterministic {
                execution_id: exec,
                details: error,
            }),
            // A genuine author `Err(...)` carries `non_deterministic_details:
            // None` and fails terminally, exactly as before.
            WorkflowOutcome::Failed { error, .. } => {
                // Preserve typed workflow-failure metadata (issue #1069 P2,
                // Codex #767). When the workflow returns a typed `WorkflowFailure`
                // the macro shim encodes it into a `harvest_workflow_failure_v1`
                // envelope carried on `error`; the Postgres path DECODES it and
                // writes `WorkflowFailed` with `error_type`/`details` (+ the human
                // message) — storing the raw envelope here instead would lose that
                // metadata and break the crate's cross-backend byte-identity
                // guarantee (AC5). Decode BEFORE appending/updating, exactly as the
                // Postgres terminal path does: the `WorkflowFailed` event carries the
                // decoded typed fields, and the execution `error` column holds the
                // human message (`decoded.message`), never the raw envelope. A legacy
                // untyped `Err(String)` decodes to `message = error` with all typed
                // fields `None`, so its stored form is unchanged.
                let decoded = autumn_harvest::failure::decode_workflow_failure(&error);
                let tx = self.conn.transaction()?;
                persist_terminal_pending_commands(&tx, exec, &pending)?;
                store::append_event(&tx, exec, &WorkflowEvent::workflow_failed_typed(&decoded))?;
                store::set_failed(&tx, exec, &decoded.message)?;
                tx.commit()?;
                Ok(RunState::Failed(decoded.message))
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
        // Timer ids armed within THIS batch, so a (pathological) duplicate
        // `StartTimer` for the same id in one cycle can't double-append.
        let mut armed_this_batch: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for cmd in commands {
            match cmd {
                cmd @ WorkflowCommand::ScheduleActivity { .. } => {
                    if apply_schedule_activity(&tx, exec, history, cmd, now)? {
                        produced = true;
                    }
                }
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => {
                    // Occurrence-keyed arm idempotency (issue #1068). A timer id may
                    // be RE-ARMED under the same name (the poll-loop idiom
                    // `loop { ctx.timer("tick", 1).await?; … }`), which the core
                    // supports via cursor-based per-id FIFO pairing. A bare-id
                    // "already has a TimerStarted?" guard would wrongly match the
                    // PRIOR fired arm and skip the re-arm, wedging the run at
                    // `Stuck`. Persist a NEW arm only when every prior arm of this
                    // id has already fired (no pending arm): a still-pending arm
                    // re-emits its `StartTimer` on each replay cycle until it fires
                    // — skip those (they are already in history); a genuine re-arm
                    // after a fire has zero pending arms and MUST persist a fresh
                    // `TimerStarted` + timer row.
                    let tid = timer_id.to_string();
                    if store::pending_timer_arms(history, &tid) == 0 && armed_this_batch.insert(tid)
                    {
                        // Absolute deadline computed from `now` (real wall clock in
                        // the public API), so it stays valid across a restart —
                        // there is no virtual clock to reset. `now` is an
                        // epoch-MILLISECOND (issue #1069 P2), so the whole-second
                        // `duration_secs` is scaled by 1000 and ADDED to preserve
                        // the sub-second arm instant: a timer armed at `10.900` with
                        // duration `1` fires at `11.900`, never `11.001`. Every
                        // step is checked (mul then add): a pathological duration
                        // saturates to an effectively-infinite deadline instead of
                        // panicking or overflowing.
                        let fire_at = i64::try_from(*duration_secs)
                            .ok()
                            .and_then(|secs| secs.checked_mul(1000))
                            .and_then(|ms| now.checked_add(ms))
                            .unwrap_or(i64::MAX);
                        persist_started_timer(&tx, exec, timer_id, *duration_secs, fire_at)?;
                        produced = true;
                    }
                }
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    // Consume the awaited signal (if staged), interleaving a due
                    // deadline timer chronologically against its arrival time
                    // (issue #476): a signal that arrived AFTER an expired
                    // `wait_for_signal_timeout` deadline records the `TimerFired`
                    // FIRST, so the timeout — not the late signal — wins the race.
                    // A plain `wait_for_signal` (no deadline timer) and an on-time
                    // signal are unchanged. See [`worker::ingest_awaited_signal`].
                    if worker::ingest_awaited_signal(&tx, exec, signal_name, now)? {
                        produced = true;
                    }
                }
                // Benign bookkeeping the single-writer backend has no surface for
                // yet, but which is safe to DROP (never an error): each is
                // replay-suppressed in the core, appends no `WorkflowEvent`, and
                // gates no control flow. `WaitForActivity` is a re-park of an
                // already-scheduled activity the worker pass completes;
                // `SetCurrentDetails` is an operator status breadcrumb (issue #593)
                // the core actively encourages — hard-erroring it would wedge an
                // otherwise fully in-subset workflow.
                WorkflowCommand::WaitForActivity { .. }
                | WorkflowCommand::SetCurrentDetails { .. } => {}
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
                // Race-loser teardown (issue #476/#600). Emitted when a resolved
                // `ctx.race()` / `wait_for_signal_timeout` picks a winner and rides
                // in the SAME suspending batch when the workflow blocks again
                // afterwards (e.g. `wait_for_signal_timeout(...)` then another
                // `execute_activity`). Delete each losing timer / cancel each losing
                // activity in this cycle's transaction. `produced` is set only when a
                // durable row actually changed, so the idempotent re-push on every
                // later replay cycle reports no progress once torn down and the loop
                // converges instead of spinning.
                WorkflowCommand::CancelRaceLosers {
                    activities,
                    children,
                    timers,
                } => {
                    if apply_cancel_race_losers(&tx, exec, activities, children, timers)? {
                        produced = true;
                    }
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
        // A backing-off activity retry (a `PENDING` task with a future `run_at`
        // from a delayed requeue, issue #1069 P2 / Codex `runtime.rs:985`) is a
        // genuine time-based block, NOT a wedge: `WaitingTimer` (a no-progress
        // driver state) so a poll before the backoff deadline neither re-runs the
        // body nor spins to `Stuck`; the caller advances the clock and re-drives
        // (the raw/no-policy path requeues immediately at `now`, converges in one
        // drain pass, and never reaches this branch). Named `WaitingTimer` because
        // it IS a wait for a scheduled time to arrive — the same class as an armed
        // durable timer, and `poll_once`/`run_until_idle` treat both as blocked.
        if queue::has_pending_activity(&self.conn, exec)? {
            return Ok(RunState::WaitingTimer);
        }
        Err(SqliteError::Stuck(exec))
    }

    /// Apply the bounded panic budget to a contained workflow-handler panic
    /// (issue #782 analog).
    ///
    /// Increments the execution's consecutive-panic strike:
    /// - **Under budget** (strikes below [`WORKFLOW_PANIC_MAX_ATTEMPTS`]): discard
    ///   the panicked cycle — append NO terminal event, persist NONE of its
    ///   (already-empty) pending commands, leave the execution `RUNNING` — and
    ///   return [`SqliteError::WorkflowPanicked`], which propagates out of the
    ///   drive loop (mirroring the non-determinism gate) so the caller can
    ///   fix/roll back the build and re-drive the unchanged history.
    /// - **At/over budget**: clear the strike and seal a CLEAN terminal
    ///   `WorkflowFailed` carrying the typed `HandlerPanic` error.
    fn contain_workflow_panic(&mut self, exec: ExecutionId, error: &str) -> SqliteResult<RunState> {
        let strikes = {
            let count = self.workflow_panic_strikes.entry(exec).or_insert(0);
            *count = count.saturating_add(1);
            *count
        };
        if strikes >= WORKFLOW_PANIC_MAX_ATTEMPTS {
            // Budget exhausted: fail terminally with the typed HandlerPanic error.
            // `error` is the encoded `harvest_workflow_failure_v1` envelope the core
            // produces for a contained handler panic (`error_type = "HandlerPanic"`),
            // so DECODE it (issue #1069 P2, Codex #767) exactly like the author-`Err`
            // terminal path above: the `WorkflowFailed` event carries the typed
            // `error_type`/`details`, and the execution `error` column holds the human
            // `decoded.message`, never the raw envelope — byte-equivalent to the
            // Postgres worker's terminal panic path and preserving cross-backend
            // fidelity (AC5).
            self.workflow_panic_strikes.remove(&exec);
            let decoded = autumn_harvest::failure::decode_workflow_failure(error);
            let tx = self.conn.transaction()?;
            store::append_event(&tx, exec, &WorkflowEvent::workflow_failed_typed(&decoded))?;
            store::set_failed(&tx, exec, &decoded.message)?;
            tx.commit()?;
            Ok(RunState::Failed(decoded.message))
        } else {
            // Under budget: non-terminal, resumable. No event, no state change.
            Err(SqliteError::WorkflowPanicked {
                execution_id: exec,
                details: error.to_string(),
            })
        }
    }
}

/// Apply one [`WorkflowCommand::ScheduleActivity`] inside the cycle transaction
/// (extracted from [`SqliteRuntime::apply_commands`]).
///
/// **Closes the dropped-command-field class (issue #1069 P2):** every
/// author-declared field on `ScheduleActivity` is either HONORED or REJECTED
/// LOUDLY — none is silently ignored.
/// - `activity_id`/`name`/`input`/`queue` — honored (persisted onto the task row;
///   single-process has one worker draining all queues, so `queue` *routing* is a
///   no-op, but it is still recorded for fidelity).
/// - `retry_policy_override` — honored IN FULL (issue #1069 P2, Codex
///   `runtime.rs:985`): the WHOLE policy is serialized onto the task row
///   (`retry_policy_json`), so the worker uses its `max_attempts` over the
///   registered [`ActivitySpec`] default AND honors `initial_interval` /
///   `backoff_coefficient` / `max_interval` (backoff timing via
///   `compute_retry_delay`) and `non_retryable_errors` (non-retryable
///   classification via `is_non_retryable`). `None` = raw path (fallback attempt
///   cap, immediate requeue, no policy non-retryable list).
/// - `start_to_close_override` — honored: persisted (milliseconds) so the worker
///   enforces it as a post-execution outcome (a body exceeding its budget records a
///   terminal `ActivityTimedOut { StartToClose }`, byte-equivalent to the Postgres
///   timeout scanner). `None` = no budget (unbounded).
/// - `session_id`/`session_worker_id` (worker sessions, issue #606) and the
///   session-acquire-only `schedule_to_start_override` — REJECTED LOUDLY with
///   [`SqliteError::Unsupported`]: they are outside the single-writer subset (worker
///   sessions co-locate a pipeline across workers this backend has no notion of), so
///   a `Some` means the workflow used `ctx.create_session(...)`, whose pinning this
///   backend cannot honor. Rejecting beats dropping the setting and running the
///   activity un-pinned. The `?` propagation drops the caller's uncommitted cycle
///   transaction, so no partial event/row is left behind.
///
/// Returns `true` iff a durable row was produced (a fresh schedule).
fn apply_schedule_activity(
    conn: &Connection,
    exec: ExecutionId,
    history: &[WorkflowEvent],
    cmd: &WorkflowCommand,
    now: i64,
) -> SqliteResult<bool> {
    let WorkflowCommand::ScheduleActivity {
        activity_id,
        name,
        input,
        queue,
        retry_policy_override,
        start_to_close_override,
        session_id,
        session_worker_id,
        schedule_to_start_override,
        ..
    } = cmd
    else {
        // Only ever called with a `ScheduleActivity` (matched at the call site).
        return Ok(false);
    };
    if session_id.is_some() || session_worker_id.is_some() {
        return Err(SqliteError::Unsupported(
            "ScheduleActivity.session_id — worker sessions (issue #606) \
             are outside the sqlite subset"
                .to_string(),
        ));
    }
    if schedule_to_start_override.is_some() {
        return Err(SqliteError::Unsupported(
            "ScheduleActivity.schedule_to_start_override — used only by the \
             session-acquire dispatch, which is outside the sqlite subset"
                .to_string(),
        ));
    }
    if store::history_has_activity_scheduled(history, &activity_id.to_string()) {
        return Ok(false);
    }
    let max_attempts = retry_policy_override.as_ref().map(|p| p.max_attempts);
    // Persist the WHOLE retry policy, not just its `max_attempts` (issue #1069 P2,
    // Codex `runtime.rs:985`): the worker reads `initial_interval` /
    // `backoff_coefficient` / `max_interval` (backoff timing) and
    // `non_retryable_errors` (classification) from it. `None` on the raw path.
    let retry_policy_json = retry_policy_override
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let start_to_close_ms = start_to_close_override.map(duration_to_millis_saturating);
    persist_scheduled_activity(
        conn,
        exec,
        *activity_id,
        name,
        input,
        queue,
        now,
        max_attempts,
        retry_policy_json.as_deref(),
        start_to_close_ms,
    )?;
    Ok(true)
}

/// Atomic dispatch pair (AC6): append `ActivityScheduled` **and** insert the
/// task-queue row on the same connection/transaction. Callers pass a
/// `rusqlite::Transaction` (via `&tx`) so a crash cannot land the event without
/// the row.
///
/// `max_attempts` carries the scheduling command's `retry_policy_override`
/// (issue #1069 P2): `Some(n)` persists the workflow's declared/per-call retry cap
/// onto the task row; `None` leaves it NULL so the worker falls back to the
/// registered [`ActivitySpec`] default.
///
/// `retry_policy_json` carries the WHOLE serialized `retry_policy_override` (issue
/// #1069 P2, Codex `runtime.rs:985`) so the worker honors backoff timing and
/// non-retryable classification, not just the attempt cap; `None` on the raw path.
///
/// `start_to_close_ms` carries the scheduling command's `start_to_close_override`
/// (issue #1069 P2): `Some(ms)` persists the per-activity start-to-close budget
/// onto the task row so the worker enforces it as a post-execution outcome; `None`
/// leaves it NULL (no budget, prior behavior).
#[allow(clippy::too_many_arguments)]
pub fn persist_scheduled_activity(
    conn: &Connection,
    exec: ExecutionId,
    activity_id: ActivityExecId,
    name: &str,
    input: &Value,
    queue_name: &str,
    run_at: i64,
    max_attempts: Option<u32>,
    retry_policy_json: Option<&str>,
    start_to_close_ms: Option<i64>,
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
    queue::enqueue_activity(
        conn,
        exec,
        activity_id,
        name,
        input,
        queue_name,
        run_at,
        max_attempts,
        retry_policy_json,
        start_to_close_ms,
    )?;
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

/// Persist a terminal cycle's drained bookkeeping commands (issue #1068).
///
/// A `RecordSideEffect`/`RecordMarker` emitted in the SAME decision cycle a
/// workflow completes or fails (e.g. `ctx.new_uuid()` / `ctx.system_now()` /
/// `ctx.random_*()` / `ctx.side_effect()` right before `Ok(..)`/`Err(..)`, with no
/// further suspension) MUST be appended to history, in command order, BEFORE the
/// terminal `WorkflowCompleted`/`WorkflowFailed` event and inside the SAME
/// terminal transaction — mirroring the Postgres worker's
/// `persist_terminal_outcome_commands`. Dropping it would let the core
/// `WorkflowReplayer` re-mint a different value on replay (`SideEffectDrift`),
/// violating the crate's cross-backend guarantee.
///
/// `SetCurrentDetails` is a benign no-op here (as in [`apply_commands`]). Any
/// OTHER command in a terminal drain is out of the supported subset and is
/// rejected LOUDLY with [`SqliteError::Unsupported`] — the `?` drops the caller's
/// uncommitted terminal transaction, rolling back every side effect persisted so
/// far this cycle. It is never silently dropped.
fn persist_terminal_pending_commands(
    conn: &Connection,
    exec: ExecutionId,
    pending: &[WorkflowCommand],
) -> SqliteResult<()> {
    for cmd in pending {
        match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                store::append_event(
                    conn,
                    exec,
                    &WorkflowEvent::MarkerRecorded {
                        name: name.clone(),
                        details: details.clone(),
                    },
                )?;
            }
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                store::append_event(
                    conn,
                    exec,
                    &WorkflowEvent::SideEffectRecorded {
                        kind: *kind,
                        name: name.clone(),
                        value: value.clone(),
                    },
                )?;
            }
            // Race-loser teardown (issue #476/#600) drained in the SAME cycle the
            // workflow completes/fails — the canonical `wait_for_signal_timeout(...)`
            // signal-win shape: `let s = ctx.wait_for_signal_timeout(..).await?;
            // Ok(s)`. The losing deadline timer MUST be deleted here (an unfired
            // `harvest_timers` row would otherwise outlive the terminal run), inside
            // the SAME terminal transaction, BEFORE the terminal event. Without this
            // arm the drain rejected it as `Unsupported`, wedging every
            // pull-signal-plus-timeout workflow (Codex #1069 P2).
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                timers,
            } => {
                apply_cancel_race_losers(conn, exec, activities, children, timers)?;
            }
            // Benign bookkeeping — appends no event, gates no control flow.
            WorkflowCommand::WaitForActivity { .. } | WorkflowCommand::SetCurrentDetails { .. } => {
            }
            other => return Err(SqliteError::Unsupported(command_name(other).to_string())),
        }
    }
    Ok(())
}

/// Resolve a [`WorkflowCommand::CancelRaceLosers`] bookkeeping command (issue
/// #476/#600): durably tear down the losing branches of a resolved `ctx.race()` /
/// `wait_for_signal_timeout` race. The caller runs this inside its own
/// transaction (the same one that persists the race's winner marker, or the
/// terminal event), so a crash between the two can never leak a row. Mirrors the
/// Postgres `worker::apply_race_loser_cancellations`.
///
/// Returns `true` iff a durable row actually changed (a timer was deleted or an
/// activity was cancelled), so [`apply_commands`](SqliteRuntime::apply_commands)
/// can treat the idempotent re-push on later replay cycles as no progress and let
/// the decision loop converge.
///
/// - `timers`: delete each still-pending `harvest_timers` row (the common
///   `wait_for_signal_timeout` signal-win case). Removing the unfired row keeps
///   the completed run from being pinned by a stray armed timer and from
///   surfacing a stray later `TimerFired`.
/// - `activities`: cancel each still-open (`PENDING`/`RUNNING`) task row and — only
///   for a row actually cancelled — append the synthetic
///   `ActivityFailed { error: "lost race to a sibling branch", .. }` the core
///   records (reusing the existing event variant, no new `WorkflowEvent`), so a
///   future replay resolves that branch to a terminal instead of looping on
///   `ActivityInProgress`. An activity that genuinely completed first is `DONE`,
///   is left untouched, and keeps its real `ActivityCompleted`.
/// - `children`: child-workflow races are OUT of the single-writer subset
///   (`StartChildWorkflow` is [`SqliteError::Unsupported`]), so a child race can
///   never have been dispatched on this backend and `children` is always empty. A
///   non-empty `children` is rejected LOUDLY rather than silently dropping a loser
///   the core would have cancelled.
fn apply_cancel_race_losers(
    conn: &Connection,
    exec: ExecutionId,
    activities: &[ActivityExecId],
    children: &[ExecutionId],
    timers: &[TimerId],
) -> SqliteResult<bool> {
    if !children.is_empty() {
        return Err(SqliteError::Unsupported(
            "CancelRaceLosers.children — child-workflow races are outside the sqlite subset"
                .to_string(),
        ));
    }
    let mut changed = false;
    for timer_id in timers {
        if queue::delete_pending_timer(conn, exec, &timer_id.to_string())? {
            changed = true;
        }
    }
    for activity_id in activities {
        if queue::cancel_activity_task(conn, *activity_id)? {
            store::append_event(
                conn,
                exec,
                &WorkflowEvent::ActivityFailed {
                    activity_id: *activity_id,
                    error: "lost race to a sibling branch".to_string(),
                    attempt: 1,
                    error_type: "Error".to_string(),
                    non_retryable: true,
                    details: None,
                },
            )?;
            changed = true;
        }
    }
    Ok(changed)
}

/// Convert a [`std::time::Duration`] budget to whole milliseconds, saturating a
/// pathologically-large value to [`i64::MAX`] rather than panicking/overflowing
/// (issue #1069 P2). Used to persist an activity's `start_to_close_override` onto
/// the task row on the same millisecond clock as every other epoch value.
fn duration_to_millis_saturating(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
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
                None,
                None,
                None,
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
                None,
                None,
                None,
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
