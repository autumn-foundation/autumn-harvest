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
    ActivityExecId, ActivityInfo, ExecutionId, NoOpMetrics, TimerId, WorkflowCommand,
    WorkflowEvent, WorkflowHandlerFn, WorkflowHistoryPolicy, WorkflowInfo, WorkflowOutcome,
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

/// A registered activity: its body plus the declared-default metadata this
/// backend honors at dispatch/finalize.
pub struct ActivitySpec {
    // `ActivitySpec` is re-exported publicly; these fields are crate-internal
    // (read by the worker). `pub` would leak them onto the public type, so
    // `pub(crate)` is deliberate and NOT redundant despite the private module.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) body: ActivityBody,
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) max_attempts: u32,
    /// The activity's declared total (cross-retry) deadline
    /// (`#[activity(schedule_to_close = "…")]` → `ActivityInfo::default_schedule_to_close`,
    /// issue #378). Populated ONLY by the info-based
    /// [`register_activity`](SqliteRuntime::register_activity) — the hand-made
    /// [`ActivitySpec::new`] path has no `ActivityInfo` and so is always `None`.
    /// The `ScheduleActivity` command does NOT carry this field (Codex #1069 P2
    /// `runtime.rs:39`), so this registry entry is where the dispatch site resolves
    /// it by name — exactly as the Postgres worker resolves it from its
    /// `HandlerRegistry`. `None` = no total deadline.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) schedule_to_close: Option<std::time::Duration>,
}

impl ActivitySpec {
    /// Register an activity body that may be attempted up to `max_attempts` times
    /// (`1` = no retry; values below `1` are clamped to `1`).
    ///
    /// This is the **hand-made** spec used by
    /// [`register_activity_raw`](SqliteRuntime::register_activity_raw): it carries
    /// no [`ActivityInfo`](autumn_harvest::ActivityInfo), so no declared-default
    /// audit runs and no `ActivityInfo`-level deadline
    /// (`schedule_to_close`) is honored. For the audited, `ActivityInfo`-driven
    /// path use [`register_activity`](SqliteRuntime::register_activity).
    #[must_use]
    pub fn new(
        max_attempts: u32,
        body: impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            body: Box::new(body),
            max_attempts: max_attempts.max(1),
            schedule_to_close: None,
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
    ///
    /// Only `name` + `handler` are retained; every other `WorkflowInfo` field is
    /// either pure metadata / observability (accepted and inert) or an
    /// execution/admission-affecting feature this v0.1 backend does not implement.
    /// The latter are **rejected LOUDLY at registration** rather than silently
    /// dropped — see [`unsupported_workflow_feature`] and the crate-level
    /// "Unsupported `WorkflowInfo`-level features" section for the full audit.
    ///
    /// # Panics
    ///
    /// Panics (setup-time, mirroring the Postgres `HarvestPlugin::build()` panic on
    /// registration misconfiguration) if `info` declares an execution- or
    /// admission-affecting feature this backend cannot honor — `execution_timeout`,
    /// a workflow-level `retry_policy` (`#[workflow(retry(...))]`), `concurrency`,
    /// `debounce`, `batch`, `throttle`, or a raised `max_input_bytes`. The panic
    /// message names the specific feature and points at the supported alternative;
    /// running the workflow with the feature silently missing would diverge from the
    /// declared `#[workflow(...)]` contract (Codex #1069 P2, `runtime.rs:602`/`:686`).
    pub fn register_workflow(&mut self, info: &WorkflowInfo) {
        if let Some((feature, hint)) = unsupported_workflow_feature(info) {
            panic!(
                "autumn-harvest-sqlite (v0.1): workflow `{}` declares the unsupported \
                 `{feature}` feature. This embedded single-writer backend retains only \
                 the handler at registration, so honoring it is a v0.1 non-goal (issue \
                 #1068 follow-ups) — registering it anyway would silently drop the \
                 setting and diverge from the #[workflow(...)] contract. {hint}",
                info.name,
            );
        }
        self.workflows.insert(info.name.to_string(), info.handler);
    }

    /// Register an activity from its macro-generated [`ActivityInfo`] plus its
    /// (synchronous) body.
    ///
    /// This is the **audited, `ActivityInfo`-driven** registration path — the
    /// analog of [`register_workflow`](Self::register_workflow) — and the one that
    /// closes the third author-declared-feature ingress surface (Codex #1069 P2,
    /// `runtime.rs:39`). Beyond the command layer ([`apply_schedule_activity`],
    /// which carries the per-dispatch `retry_policy_override` /
    /// `start_to_close_override`) and the workflow-registration layer
    /// ([`unsupported_workflow_feature`]), an activity's `#[activity(...)]`
    /// declares *dispatch-time defaults the Postgres worker enforces from
    /// `ActivityInfo` but which are NOT copied into the `ScheduleActivity`
    /// command* — so a name-only registration would silently drop them. Here every
    /// such field is HONORED or REJECTED LOUDLY, never silently dropped
    /// (see [`unsupported_activity_feature`] and the crate-level "Unsupported
    /// `ActivityInfo`-level features" section for the full audit):
    ///
    /// - **HONORED**: `default_schedule_to_close` (issue #378 — the total,
    ///   cross-retry wall-clock deadline; persisted onto each dispatched task and
    ///   enforced as a terminal `ActivityTimedOut { ScheduleToClose }`),
    ///   `default_retry_policy` (its `max_attempts` becomes the registered fallback
    ///   attempt cap; the *whole* policy also reaches the worker via the command's
    ///   `retry_policy_override`), `default_start_to_close` (honored via the
    ///   command's `start_to_close_override`), and `default_queue`
    ///   (recorded; single-writer routing is a no-op).
    /// - **REJECTED** at registration (setup-time panic, mirroring
    ///   [`register_workflow`]): `default_heartbeat_timeout`,
    ///   `default_schedule_to_start`, `circuit_breaker`, `rate_limit_*`,
    ///   `max_concurrent`, `requires`, `is_local`, and a raised
    ///   `max_input_bytes` / `max_result_bytes`.
    /// - **ACCEPTED, inert**: `name` (registration key), `module` (diagnostics),
    ///   and a lone `concurrency_key` (groups a cap that is not set). The core
    ///   async `ActivityInfo::handler` is intentionally NOT used — this backend
    ///   runs a caller-supplied SYNCHRONOUS body (no `ActivityContext`, no I/O
    ///   framework), matching the crate's activity model.
    ///
    /// # Panics
    ///
    /// Panics (setup-time) if `info` declares an unsupported `ActivityInfo`-level
    /// feature (see the list above). The panic names the feature and points at the
    /// supported alternative; registering it anyway would silently drop the setting
    /// and diverge from the declared `#[activity(...)]` contract. Use
    /// [`register_activity_raw`](Self::register_activity_raw) for a hand-made body
    /// that carries no declared defaults.
    pub fn register_activity(
        &mut self,
        info: &ActivityInfo,
        body: impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    ) {
        if let Some((feature, hint)) = unsupported_activity_feature(info) {
            panic!(
                "autumn-harvest-sqlite (v0.1): activity `{}` declares the unsupported \
                 `{feature}` feature. This embedded single-writer backend has no \
                 dispatch-admission or cross-worker machinery to honor it, so \
                 registering it anyway would silently drop the setting and diverge \
                 from the #[activity(...)] contract (Codex #1069 P2, runtime.rs:39). \
                 {hint}",
                info.name,
            );
        }
        // Derive the fallback attempt cap from the declared retry policy (the WHOLE
        // policy also reaches the worker per-dispatch via the command's
        // `retry_policy_override`, which wins over this fallback). No declared
        // policy ⇒ a single attempt (matching `ActivitySpec::new`'s `>= 1` clamp).
        let max_attempts = info
            .default_retry_policy
            .as_ref()
            .map_or(1, |p| p.max_attempts.max(1));
        self.activities.insert(
            info.name.to_string(),
            ActivitySpec {
                body: Box::new(body),
                max_attempts,
                schedule_to_close: info.default_schedule_to_close,
            },
        );
    }

    /// Register a hand-made activity body under `name`, bypassing the
    /// [`ActivityInfo`] declared-default audit.
    ///
    /// The escape hatch analog of `execute_activity_raw`: it carries no
    /// `ActivityInfo`, so no `ActivityInfo`-level default (e.g.
    /// `schedule_to_close`) is honored or rejected — the caller is responsible for
    /// any such semantics. Prefer [`register_activity`](Self::register_activity)
    /// (which audits and honors declared defaults) whenever you have the
    /// macro-generated `ActivityInfo`.
    pub fn register_activity_raw(&mut self, name: impl Into<String>, spec: ActivitySpec) {
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
        // Enforce the workflow-input cap BEFORE inserting the execution row or
        // appending `WorkflowStarted` (issue #252, Codex #1069 `runtime.rs:264`).
        // The Postgres start path rejects an oversized input with
        // `PayloadTooLarge` before persisting anything; without this the SQLite
        // backend would store/replay an unbounded input and accept a start other
        // backends reject. Measured as the serialized-JSON byte length, exactly as
        // the core does (`serde_json::to_string(&input).len()`). A zero cap means
        // uncapped (the core convention); the default `DEFAULT_MAX_WORKFLOW_INPUT_BYTES`
        // (2 MiB) is > 0, so the check is active. The reused core context already
        // enforces the *continue-as-new* and *side-effect* input caps; this closes
        // the one boundary the executor never sees — the fresh START input.
        if DEFAULT_MAX_WORKFLOW_INPUT_BYTES > 0 {
            let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
            if observed > DEFAULT_MAX_WORKFLOW_INPUT_BYTES {
                return Err(SqliteError::PayloadTooLarge {
                    kind: "workflow_input",
                    observed_bytes: observed,
                    cap_bytes: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
                });
            }
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
        // Enforce the signal-payload cap at INGRESS (issue #252, Codex #1069
        // `runtime.rs:346`), before staging the `harvest_signals` row — otherwise
        // the next `wait_for_signal` records an oversized `SignalReceived` event,
        // bloating history and accepting a payload the Postgres path rejects at its
        // boundary. Checked AFTER the not-found / terminal rejections above: the
        // target's existence/state is a precondition for accepting ANY signal, so a
        // typoed or sealed target reports that (the more actionable error) rather
        // than a size complaint — the prompt's chosen order. (The Postgres HTTP
        // boundary happens to check the cap *before* `send_signal`'s state check;
        // both reject and neither stages the row, so the only observable difference
        // is the error KIND for the pathological "oversized payload to an
        // unknown/terminal target" case — a caller bug either way.) Measured as the
        // serialized-JSON byte length, matching the core (`serde_json::to_string`).
        if DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES > 0 {
            let observed = serde_json::to_string(&payload).map_or(0, |s| s.len() as u64);
            if observed > DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES {
                return Err(SqliteError::PayloadTooLarge {
                    kind: "signal_payload",
                    observed_bytes: observed,
                    cap_bytes: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
                });
            }
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
        // The activity-finalize clock seam (Codex #1069 P2, `worker.rs:328`): a
        // retry's backoff anchors to the instant the body actually failed. The
        // wall-clock driver re-reads `now_fn` (a cheap `Arc` clone captured once, so
        // it does not borrow `self` across the `&mut self` cycle call).
        let now_fn = self.now_fn.clone();
        let failure_now = move || now_fn().timestamp_millis();
        for _ in 0..MAX_ITERATIONS {
            let now = (self.now_fn)().timestamp_millis();
            match self.drive_one_cycle(exec, now, &failure_now).await? {
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
        // `_as_of` simulation keeps `failure_now() == now` (the caller-fixed epoch),
        // so a policy retry's backoff anchor is byte-identical to a wall-clock cycle
        // with an instant body — deterministic, sleep-free (Codex #1069 P2).
        let failure_now = move || now;
        for _ in 0..MAX_ITERATIONS {
            match self.drive_one_cycle(exec, now, &failure_now).await? {
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
        // Activity-finalize clock seam (Codex #1069 P2, `worker.rs:328`) — re-reads
        // `now_fn` per finalize; cloned once so it does not borrow `self`.
        let now_fn = self.now_fn.clone();
        let failure_now = move || now_fn().timestamp_millis();
        let mut progress = false;
        for exec in store::running_executions(&self.conn)? {
            let now = (self.now_fn)().timestamp_millis();
            match self.drive_one_cycle(exec, now, &failure_now).await? {
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
        // `_as_of` simulation: `failure_now() == now` (deterministic, sleep-free).
        let failure_now = move || now;
        let mut progress = false;
        for exec in store::running_executions(&self.conn)? {
            match self.drive_one_cycle(exec, now, &failure_now).await? {
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
    ///
    /// `failure_now` is the clock seam read at activity-*finalize* time (after a
    /// body returns) so a policy retry's backoff anchors to the real failure
    /// instant, not the pre-body cycle-start `now` (Codex #1069 P2, `worker.rs:328`).
    /// The wall-clock drivers pass a closure that re-reads [`Self::now_fn`]; the
    /// `_as_of` drivers pass one that returns the caller-fixed epoch, so simulation
    /// stays deterministic (`failure_now() == now`).
    async fn drive_one_cycle(
        &mut self,
        exec: ExecutionId,
        now: i64,
        // `+ Send + Sync` keeps this async fn's future `Send` (clippy
        // `future_not_send`) — the drivers' closures satisfy it.
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> SqliteResult<RunState> {
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
                let drained =
                    worker::drain_ready(&mut self.conn, exec, now, failure_now, &self.activities)?;
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
                cmd @ WorkflowCommand::ScheduleActivity { name, .. } => {
                    // Resolve the activity's total (cross-retry) deadline BY NAME
                    // from the registry (Codex #1069 P2, `runtime.rs:39`). The
                    // `ScheduleActivity` command does not carry
                    // `default_schedule_to_close`, so — exactly as the Postgres
                    // worker resolves `ActivityInfo` from its `HandlerRegistry` at
                    // enqueue — the dispatch site reads it here. Only the info-based
                    // `register_activity(&info, …)` populates it; a raw-registered
                    // body yields `None`. Disjoint-field borrow: `tx` holds
                    // `&mut self.conn`, this reads `&self.activities` (same shape as
                    // the `drain_ready(&mut self.conn, …, &self.activities)` call).
                    let schedule_to_close =
                        self.activities.get(name).and_then(|s| s.schedule_to_close);
                    if apply_schedule_activity(&tx, exec, history, cmd, now, schedule_to_close)? {
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
                // Cancellable durable timers (issue #768) — a v0.1 non-goal.
                // Reject LOUDLY and by NAME with an actionable message pointing at
                // the supported fire-once `ctx.timer(...)`, rather than letting them
                // fall through to the generic catch-all (Codex #1069 P2,
                // `runtime.rs:840`). `?`-dropping the uncommitted `tx` rolls the
                // whole batch back.
                cmd @ (WorkflowCommand::ArmTimer { .. } | WorkflowCommand::CancelTimer { .. }) => {
                    return Err(cancellable_timer_unsupported(cmd));
                }
                // Every OTHER unsupported command (child workflows, external
                // activities/signals/cancels, local activities, updates, search
                // attributes, detached children) is rejected BY NAME via
                // `command_name` — no opaque `"Unknown"` for any current variant
                // (Codex #1069 P2, `runtime.rs:840`). Dropping `tx` here
                // (un-committed) rolls back the whole batch — an unsupported command
                // after a supported one leaves NEITHER the supported command's event
                // NOR its queue/timer row persisted.
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
/// - `schedule_to_close` (the caller-resolved `ActivityInfo::default_schedule_to_close`,
///   issue #378, Codex #1069 P2 `runtime.rs:39`) — honored: an ABSOLUTE deadline
///   `now + schedule_to_close` is persisted on the task row so the worker enforces
///   the total cross-retry cap (terminal `ActivityTimedOut { ScheduleToClose }`).
///   NOT a command field — resolved by name from the registry at the call site.
///   `None` = no total deadline.
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
    schedule_to_close: Option<std::time::Duration>,
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
    // The total (cross-retry) deadline is ABSOLUTE: `now + schedule_to_close`, so it
    // stays valid across a restart (like the timer's absolute `fire_at`) — there is
    // no virtual clock to reset. `duration_to_millis_saturating` clamps a
    // pathological duration; `checked_add` keeps a saturating duration from
    // wrapping to an early deadline. `None` = no total deadline.
    let schedule_to_close_at = schedule_to_close.map(|d| {
        let ms = duration_to_millis_saturating(d);
        now.checked_add(ms).unwrap_or(i64::MAX)
    });
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
        schedule_to_close_at,
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
///
/// `schedule_to_close_at` carries the ABSOLUTE total (cross-retry) deadline
/// resolved from the registered `ActivityInfo::default_schedule_to_close` (issue
/// #378, Codex #1069 P2 `runtime.rs:39`): `Some(at)` persists it so the worker
/// enforces the total cap; `None` leaves it NULL (no total deadline).
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
    schedule_to_close_at: Option<i64>,
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
        schedule_to_close_at,
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
            // Cancellable durable timers (issue #768) — a v0.1 non-goal — can also
            // be drained in the terminal cycle (e.g. `ctx.start_timer(...)` right
            // before `Ok(..)`). Reject by NAME with the same actionable message as
            // the suspend path (Codex #1069 P2, `runtime.rs:840`); `?` drops the
            // caller's uncommitted terminal transaction.
            cmd @ (WorkflowCommand::ArmTimer { .. } | WorkflowCommand::CancelTimer { .. }) => {
                return Err(cancellable_timer_unsupported(cmd));
            }
            // Any OTHER out-of-subset command in a terminal drain is rejected BY
            // NAME via `command_name` — no opaque `"Unknown"` for any current
            // variant (Codex #1069 P2, `runtime.rs:840`).
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

/// Human name for a command (for error messages). EVERY current
/// [`WorkflowCommand`] variant is named explicitly, so an unsupported command is
/// always rejected BY NAME rather than as an opaque `"Unknown"` (issue #1069 P2,
/// Codex `runtime.rs:840`). The match is deliberately EXHAUSTIVE (no `_` arm): a
/// future core `WorkflowCommand` variant becomes a compile error here, forcing
/// this backend to make an explicit supported/unsupported decision for it rather
/// than silently mislabelling it `"Unknown"`.
const fn command_name(cmd: &WorkflowCommand) -> &'static str {
    match cmd {
        WorkflowCommand::ScheduleActivity { .. } => "ScheduleActivity",
        WorkflowCommand::WaitForActivity { .. } => "WaitForActivity",
        WorkflowCommand::StartTimer { .. } => "StartTimer",
        WorkflowCommand::StartChildWorkflow { .. } => "StartChildWorkflow",
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => "SpawnDetachedChildWorkflow",
        WorkflowCommand::RecordMarker { .. } => "RecordMarker",
        WorkflowCommand::RecordSideEffect { .. } => "RecordSideEffect",
        WorkflowCommand::ScheduleExternalActivity { .. } => "ScheduleExternalActivity",
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::SignalExternalWorkflow { .. } => "SignalExternalWorkflow",
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => "RequestCancelExternalWorkflow",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
        WorkflowCommand::RecordUpdateResult { .. } => "RecordUpdateResult",
        WorkflowCommand::UpsertSearchAttributes { .. } => "UpsertSearchAttributes",
        WorkflowCommand::SetCurrentDetails { .. } => "SetCurrentDetails",
        WorkflowCommand::CancelRaceLosers { .. } => "CancelRaceLosers",
        WorkflowCommand::ArmTimer { .. } => "ArmTimer",
        WorkflowCommand::CancelTimer { .. } => "CancelTimer",
    }
}

/// The clear, actionable [`SqliteError::Unsupported`] message for a cancellable
/// durable-timer command (issue #768) — `WorkflowCommand::ArmTimer` /
/// `CancelTimer`, emitted by `ctx.start_timer(...)` and
/// `TimerHandle::await_fire`/`cancel`/`reset`. These are a v0.1 non-goal for this
/// single-writer backend (only fire-once `ctx.timer(...)` durable timers are
/// supported), so name the command AND point at the supported alternative rather
/// than letting it surface as an opaque error (Codex #1069 P2, `runtime.rs:840`).
fn cancellable_timer_unsupported(cmd: &WorkflowCommand) -> SqliteError {
    SqliteError::Unsupported(format!(
        "{} — cancellable timers (ctx.start_timer / TimerHandle await_fire·cancel·reset, \
         issue #768) are a v0.1 non-goal for the sqlite backend; use ctx.timer(...) for \
         fire-once durable timers",
        command_name(cmd),
    ))
}

/// Audit a [`WorkflowInfo`] at [`register_workflow`](SqliteRuntime::register_workflow)
/// for a declared feature this v0.1 backend does not implement — returning the first
/// such `(feature, hint)` found, or `None` if the workflow is registrable.
///
/// [`register_workflow`](SqliteRuntime::register_workflow) retains ONLY `name` +
/// `handler`, so every other `WorkflowInfo` field would otherwise be silently
/// dropped. This closes the "silently-dropped `WorkflowInfo`-level feature" class
/// (Codex #1069 P2, `runtime.rs:602` `execution_timeout`, `:686` `retry_policy`) at the
/// single registration choke point: an execution- or admission-affecting field that
/// is `Some(_)` (or a raised cap) is REJECTED so it can never take silently-wrong
/// effect, mirroring how the command layer already rejects out-of-subset
/// `WorkflowCommand`s ([`cancellable_timer_unsupported`]) and `ScheduleActivity`
/// fields ([`apply_schedule_activity`]).
///
/// The partition (see the crate-level "Unsupported `WorkflowInfo`-level features"
/// section for the full audit):
///
/// - **REJECTED** — changes lifecycle / admission / retry semantics if ignored:
///   `execution_timeout` (#243), workflow-level `retry_policy` (#523), `concurrency`
///   (#247), `debounce` (#499), `batch` (#518), `throttle` (#607), and a raised
///   `max_input_bytes` (#252, which lowers the effective input cap vs. the declared
///   contract if dropped).
/// - **ACCEPTED, inert** — pure metadata / observability, harmless to ignore because
///   execution never consults them here: `sla` (#487 — no scanner, so no
///   `sla_breached` metric; NO wrong execution), `owner` / `runbook_url` / `severity`
///   (#372), `description` + `input_schema` / `output_schema` / `error_schema` (#373 —
///   HTTP-edge discovery/validation, and this backend has no HTTP edge), `mcp` (#597 —
///   HTTP-edge tool exposure), and `module` (diagnostics).
/// - **HONORED**: `name` (registration key) and `handler` (dispatch fn).
const fn unsupported_workflow_feature(info: &WorkflowInfo) -> Option<(&'static str, &'static str)> {
    // Ordered most-impactful first; the first match is reported. Each hint points at
    // the supported alternative (or states none exists) so an author gets an
    // actionable failure at setup, not a silent runtime divergence.
    if info.execution_timeout.is_some() {
        return Some((
            "execution_timeout",
            "There is no execution-timeout scanner on this single-writer backend, so a \
             declared hard deadline would never fire. Enforce a deadline inside the \
             workflow body with a `ctx.timer(...)` branch (or `ctx.sleep_until`) instead.",
        ));
    }
    if info.retry_policy.is_some() {
        return Some((
            "retry_policy (#[workflow(retry(...))])",
            "Workflow-level auto-retry (a fresh retry successor after an author failure) \
             is not orchestrated here. Use per-activity retry (`#[activity(retry = ...)]`, \
             which this backend honors in full) or handle the failure in the workflow body.",
        ));
    }
    if info.concurrency.is_some() {
        return Some((
            "concurrency",
            "Per-key concurrency limits require the shared Postgres task queue's \
             claim-time advisory lock, which the single-writer backend has no analog for.",
        ));
    }
    if info.debounce.is_some() {
        return Some((
            "debounce",
            "Trigger-burst debounce is a pre-start admission-gate feature with no \
             equivalent on this backend.",
        ));
    }
    if info.batch.is_some() {
        return Some((
            "batch",
            "Trigger batching is a pre-start admission-gate feature with no equivalent \
             on this backend.",
        ));
    }
    if info.throttle.is_some() {
        return Some((
            "throttle",
            "Start-throttle pacing is a pre-start admission-gate feature with no \
             equivalent on this backend.",
        ));
    }
    if info.max_input_bytes.is_some() {
        return Some((
            "max_input_bytes",
            "A per-workflow raised input cap is not threaded into this backend's replay \
             caps, so the global default cap would apply — silently REJECTING an input \
             the workflow declared as acceptable. Rejected so the divergence surfaces at \
             setup rather than as a surprising runtime PayloadTooLarge.",
        ));
    }
    None
}

/// Audit an [`ActivityInfo`] at
/// [`register_activity`](SqliteRuntime::register_activity) for a declared,
/// dispatch-time default this v0.1 backend does not implement — returning the
/// first such `(feature, hint)`, or `None` if the activity is registrable.
///
/// This closes the **third** author-declared-feature ingress surface (Codex #1069
/// P2, `runtime.rs:39`), after the `ScheduleActivity` **command** fields
/// ([`apply_schedule_activity`]) and the **`WorkflowInfo`** fields
/// ([`unsupported_workflow_feature`]). A `#[activity(...)]` can declare defaults the
/// Postgres worker enforces *from `ActivityInfo` at dispatch* but which are NOT
/// copied into the `ScheduleActivity` command — `default_schedule_to_close`,
/// `default_heartbeat_timeout`, `default_schedule_to_start`, `circuit_breaker`,
/// `rate_limit_*`, `max_concurrent`, `requires`, `is_local`, and the per-activity
/// cap raisers. A name-only registration never saw them, so they were neither
/// honored nor rejected. The partition (see the crate-level "Unsupported
/// `ActivityInfo`-level features" section):
///
/// - **HONORED** (handled by [`register_activity`], NOT reported here):
///   `default_schedule_to_close` (persisted + enforced as terminal
///   `ActivityTimedOut { ScheduleToClose }`), `default_retry_policy` (fallback
///   attempt cap; the whole policy reaches the worker per-dispatch),
///   `default_start_to_close` (via the command), `default_queue` (recorded).
/// - **REJECTED** (reported here) — a dispatch-admission / cross-worker semantic
///   this single-writer backend has no analog for, or a cap raiser that would
///   otherwise silently apply the STRICTER global cap:
///   `default_heartbeat_timeout` (no heartbeating in the inline drain),
///   `default_schedule_to_start` (no queue-wait scanner), `circuit_breaker`
///   (#369), `rate_limit_*` (#332), `max_concurrent` (#247), `requires`
///   (capability routing), `is_local` (local-activity `RunLocalActivity` dispatch
///   is a command-layer non-goal), and a raised `max_input_bytes` /
///   `max_result_bytes` (#252).
/// - **ACCEPTED, inert**: `name`, `module`, and a lone `concurrency_key` (groups a
///   cap that is not set).
///
/// The chain is EXHAUSTIVE by intent: adding an execution-affecting field to core
/// `ActivityInfo` should extend this audit rather than silently pass through.
const fn unsupported_activity_feature(info: &ActivityInfo) -> Option<(&'static str, &'static str)> {
    // Ordered most-impactful first; the first match is reported.
    if info.is_local {
        return Some((
            "local = true",
            "Local activities dispatch via the RunLocalActivity command, which this \
             single-writer backend rejects at the command layer. Register a regular \
             activity instead — this backend already runs every activity body inline.",
        ));
    }
    if info.default_schedule_to_start.is_some() {
        return Some((
            "schedule_to_start",
            "There is no queue-wait (schedule-to-start) timeout scanner on this \
             single-writer backend, so the declared bound would never fire.",
        ));
    }
    if info.default_heartbeat_timeout.is_some() {
        return Some((
            "heartbeat_timeout",
            "Activity bodies run synchronously inline (no ActivityContext, no \
             heartbeating), so a heartbeat-timeout could never be observed. Use \
             start_to_close (honored) for a per-attempt bound, or schedule_to_close \
             (honored) for a total cross-retry deadline.",
        ));
    }
    if info.circuit_breaker.is_some() {
        return Some((
            "circuit_breaker",
            "The per-activity circuit breaker (#369) is a dispatch-admission feature \
             with no analog on this backend.",
        ));
    }
    if info.rate_limit_rps.is_some()
        || info.rate_limit_burst.is_some()
        || info.rate_limit_key.is_some()
    {
        return Some((
            "rate_limit",
            "Per-activity rate limiting (#332) is a dispatch-admission feature backed \
             by the shared task queue's token buckets, which the single-writer backend \
             has no analog for.",
        ));
    }
    if info.max_concurrent.is_some() {
        return Some((
            "max_concurrent",
            "A cluster-wide per-activity concurrency cap (#247) requires the shared \
             task queue's claim-time advisory lock, absent on this backend.",
        ));
    }
    if info.requires.is_some() {
        return Some((
            "requires",
            "Capability-based worker routing (#[activity(requires = \"…\")]) targets a \
             heterogeneous worker fleet, which this single-process backend does not have.",
        ));
    }
    if info.max_input_bytes.is_some() {
        return Some((
            "max_input_bytes",
            "A per-activity RAISED input cap is not threaded into this backend's caps, \
             so the STRICTER global default would apply — silently REJECTING an input \
             the activity declared as acceptable. Rejected so the divergence surfaces \
             at setup rather than as a surprising runtime PayloadTooLarge.",
        ));
    }
    if info.max_result_bytes.is_some() {
        return Some((
            "max_result_bytes",
            "A per-activity RAISED result cap is not threaded into this backend's caps, \
             so the STRICTER global default would apply — silently REJECTING a result \
             the activity declared as acceptable. Rejected so the divergence surfaces \
             at setup rather than as a surprising runtime PayloadTooLarge.",
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use autumn_harvest::{ActivityExecId, ExecutionId, TimerId, WorkflowCommand};
    use rusqlite::Connection;

    use super::{
        cancellable_timer_unsupported, command_name, persist_scheduled_activity,
        persist_started_timer, unsupported_activity_feature,
    };
    use crate::error::SqliteError;
    use crate::{queue, schema, store};

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    // ── unsupported_activity_feature audit (Codex #1069 P2, runtime.rs:39) ────────
    //
    // The THIRD author-declared-feature ingress surface: dispatch-time `ActivityInfo`
    // defaults the Postgres worker enforces but which are NOT carried on the
    // `ScheduleActivity` command. This exhaustively pins the honor/reject/inert
    // partition directly against the private audit fn, using struct-update over a
    // macro-built base `ActivityInfo` to flip one field at a time. `register_activity`
    // (public) drives the same fn's panic path in `tests/activity_info_features.rs`.
    mod activity_audit {
        use std::time::Duration;

        use autumn_harvest::ActivityInfo;
        use autumn_harvest::policy::{CircuitBreakerPolicy, RetryPolicy};
        use autumn_harvest::prelude::*;

        use super::unsupported_activity_feature;

        // A minimal, plain remote activity — its `_info()` supplies a real
        // `ActivityHandlerFn` for `..base_info()` struct-update. Its await-free body
        // is never run (the audit reads only the info), so `unused_async` is allowed.
        #[activity]
        #[allow(clippy::unused_async)]
        async fn base_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
            Ok(n)
        }

        fn base_info() -> ActivityInfo {
            base_act_info()
        }

        fn rejected_feature(info: &ActivityInfo) -> Option<&'static str> {
            unsupported_activity_feature(info).map(|(feature, _)| feature)
        }

        #[test]
        fn a_plain_activity_is_registrable() {
            // No declared defaults at all → nothing to reject.
            assert_eq!(rejected_feature(&base_info()), None);
        }

        #[test]
        fn honored_and_inert_fields_do_not_reject() {
            // HONORED (handled by register_activity, not reported by the audit):
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_schedule_to_close: Some(Duration::from_secs(5)),
                    ..base_info()
                }),
                None,
                "schedule_to_close is HONORED (persisted + enforced), not rejected",
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_retry_policy: Some(RetryPolicy::fixed(3, Duration::from_secs(1))),
                    ..base_info()
                }),
                None,
                "retry policy is honored (fallback attempt cap + per-dispatch command)",
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_start_to_close: Some(Duration::from_secs(2)),
                    ..base_info()
                }),
                None,
                "start_to_close is honored via the ScheduleActivity command",
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_queue: Some("email"),
                    ..base_info()
                }),
                None,
                "queue is recorded (single-writer routing is a no-op)",
            );
            // INERT: a lone concurrency_key groups a cap that is not set.
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    concurrency_key: Some("tenant"),
                    ..base_info()
                }),
                None,
                "a lone concurrency_key (no max_concurrent) is inert",
            );
        }

        #[test]
        fn every_unsupported_field_is_rejected_by_name() {
            // Each dispatch-admission / cross-worker / cap-raiser field, flipped in
            // isolation, is rejected and NAMED — never silently dropped.
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    is_local: true,
                    ..base_info()
                }),
                Some("local = true"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_schedule_to_start: Some(Duration::from_secs(300)),
                    ..base_info()
                }),
                Some("schedule_to_start"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    default_heartbeat_timeout: Some(Duration::from_secs(10)),
                    ..base_info()
                }),
                Some("heartbeat_timeout"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    circuit_breaker: Some(CircuitBreakerPolicy::new(
                        3,
                        Duration::from_secs(30),
                        Duration::from_secs(10),
                    )),
                    ..base_info()
                }),
                Some("circuit_breaker"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    rate_limit_rps: Some(10.0),
                    ..base_info()
                }),
                Some("rate_limit"),
            );
            // A lone rate_limit_key (no rps) is still part of the rate-limit family.
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    rate_limit_key: Some("bucket"),
                    ..base_info()
                }),
                Some("rate_limit"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    max_concurrent: Some(4),
                    ..base_info()
                }),
                Some("max_concurrent"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    requires: Some("gpu=true"),
                    ..base_info()
                }),
                Some("requires"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    max_input_bytes: Some(8 * 1024 * 1024),
                    ..base_info()
                }),
                Some("max_input_bytes"),
            );
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    max_result_bytes: Some(8 * 1024 * 1024),
                    ..base_info()
                }),
                Some("max_result_bytes"),
            );
        }
    }

    // FIX B / AUDIT (Codex #1069 P2, runtime.rs:840): the previously-"Unknown"
    // cancellable-timer commands are named by `command_name`, and
    // `cancellable_timer_unsupported` produces a CLEAR, actionable message that
    // names the command AND points at the supported fire-once `ctx.timer(...)`. The
    // `command_name` match is EXHAUSTIVE (no `_ => "Unknown"` arm), so every other
    // current variant is likewise named by construction — a future core variant is
    // a compile error here rather than a silent "Unknown".
    #[test]
    fn cancellable_timer_commands_are_named_and_rejected_actionably() {
        let arm = WorkflowCommand::ArmTimer {
            timer_id: TimerId::new("t".to_string()),
            duration_secs: 1,
            for_await: false,
        };
        let cancel = WorkflowCommand::CancelTimer {
            timer_id: TimerId::new("t".to_string()),
        };
        assert_eq!(command_name(&arm), "ArmTimer");
        assert_eq!(command_name(&cancel), "CancelTimer");
        assert_ne!(command_name(&arm), "Unknown");

        for cmd in [&arm, &cancel] {
            let SqliteError::Unsupported(msg) = cancellable_timer_unsupported(cmd) else {
                panic!("must be Unsupported");
            };
            assert!(
                msg.contains(command_name(cmd)),
                "must name the command: {msg}"
            );
            assert!(
                !msg.contains("Unknown"),
                "must not be the opaque Unknown: {msg}"
            );
            assert!(
                msg.contains("ctx.timer"),
                "must point at the supported fire-once alternative: {msg}"
            );
            assert!(
                msg.contains("#768"),
                "must reference the primitive's issue: {msg}"
            );
        }
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

/// Pure-function coverage for [`unsupported_workflow_feature`] — the registration
/// audit closing the "silently-dropped `WorkflowInfo`-level feature" class (Codex
/// #1069 P2). Isolated from the `tests` module above because it pulls the macro
/// prelude and several policy types the other tests do not need.
#[cfg(test)]
// The `#[workflow]` macro references the input param and holds no `.await`, so a
// deliberately-minimal base workflow reads as an unused-underscore-binding /
// unused-async through the expansion — matching the integration test files' allows.
#[allow(clippy::used_underscore_binding, clippy::unused_async)]
mod feature_gate_tests {
    use std::time::Duration;

    use autumn_harvest::concurrency::ConcurrencyPolicy;
    use autumn_harvest::debounce::DebouncePolicy;
    use autumn_harvest::event_batch::BatchPolicy;
    use autumn_harvest::policy::RetryPolicy;
    use autumn_harvest::prelude::*;
    use autumn_harvest::throttle::ThrottlePolicy;

    use super::unsupported_workflow_feature;

    // A minimal, registrable workflow: no execution/admission-affecting feature set.
    // Its macro-generated `WorkflowInfo` is the clean base each case mutates.
    #[workflow]
    async fn base_wf(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
        let _ = ctx;
        Ok(0)
    }

    fn rejected(feature_substr: &str, info: &WorkflowInfo) {
        let (feature, hint) = unsupported_workflow_feature(info)
            .unwrap_or_else(|| panic!("`{feature_substr}` must be rejected"));
        assert!(
            feature.contains(feature_substr),
            "reported feature `{feature}` must name `{feature_substr}`",
        );
        assert!(
            !hint.is_empty(),
            "a rejected feature must carry an actionable hint"
        );
    }

    #[test]
    fn plain_workflow_is_registrable() {
        assert!(
            unsupported_workflow_feature(&base_wf_info()).is_none(),
            "a plain workflow (name + handler only) must register",
        );
    }

    #[test]
    fn every_execution_affecting_field_is_rejected() {
        let mut info = base_wf_info();
        info.execution_timeout = Some(Duration::from_secs(30));
        rejected("execution_timeout", &info);

        let mut info = base_wf_info();
        info.retry_policy = Some(RetryPolicy::fixed(2, Duration::from_secs(1)));
        rejected("retry_policy", &info);

        let mut info = base_wf_info();
        info.concurrency = Some(ConcurrencyPolicy {
            key_expr: "input.tenant_id",
            limit: 10,
        });
        rejected("concurrency", &info);

        let mut info = base_wf_info();
        info.debounce = Some(DebouncePolicy {
            key_expr: "input.k",
            window: Duration::from_secs(30),
            max_wait: None,
        });
        rejected("debounce", &info);

        let mut info = base_wf_info();
        info.batch = Some(BatchPolicy {
            key_expr: "input.k".to_string(),
            max_size: 10,
            max_wait: Duration::from_secs(30),
        });
        rejected("batch", &info);

        let mut info = base_wf_info();
        info.throttle = Some(
            ThrottlePolicy::from_rate_str("100/m", Some(20.0), Some("input.k"), None)
                .expect("valid rate"),
        );
        rejected("throttle", &info);

        let mut info = base_wf_info();
        info.max_input_bytes = Some(8 * 1024 * 1024);
        rejected("max_input_bytes", &info);
    }

    #[test]
    fn pure_metadata_and_observability_are_accepted_inert() {
        // sla (observability-only), owner/runbook/severity, description, schemas,
        // and mcp all change NO execution semantics here, so they never block
        // registration.
        let mut info = base_wf_info();
        info.sla = Some(Duration::from_secs(3600));
        info.owner = Some("payments-team");
        info.runbook_url = Some("https://runbook.example/x");
        info.severity = Some("high");
        info.description = Some("does a thing");
        info.input_schema = Some(|| serde_json::json!({"type": "integer"}));
        info.output_schema = Some(|| serde_json::json!({"type": "integer"}));
        info.error_schema = Some(|| serde_json::json!({"type": "string"}));
        info.mcp = true;
        assert!(
            unsupported_workflow_feature(&info).is_none(),
            "metadata/observability-only fields must be accepted (inert)",
        );
    }

    #[test]
    fn execution_timeout_is_reported_before_retry_policy() {
        // Deterministic first-match ordering: when several are set, the most
        // impactful (execution_timeout) is surfaced first.
        let mut info = base_wf_info();
        info.execution_timeout = Some(Duration::from_secs(30));
        info.retry_policy = Some(RetryPolicy::fixed(2, Duration::from_secs(1)));
        let (feature, _) = unsupported_workflow_feature(&info).expect("rejected");
        assert!(feature.contains("execution_timeout"), "got `{feature}`");
    }

    // ── Payload caps: direct "nothing persisted" proofs (issue #252) ───────────
    // These reuse `base_wf` and access the runtime's private `conn` (a descendant
    // module of `runtime`) to assert the ROW COUNTS directly — the strongest proof
    // that an oversized ingress payload never enters storage.

    // FIX A (Codex #1069 `runtime.rs:264`): an oversized START input is rejected
    // BEFORE the insert transaction is opened — no execution row, no
    // `WorkflowStarted` event. RED pre-fix: both were inserted.
    #[test]
    fn oversized_start_input_persists_nothing() {
        let mut rt = super::SqliteRuntime::open_in_memory().unwrap();
        rt.register_workflow(&base_wf_info());

        let big = serde_json::json!("x".repeat(2 * 1024 * 1024 + 1)); // > 2 MiB
        let err = rt.start_workflow("base_wf", big).unwrap_err();
        assert!(
            matches!(
                err,
                super::SqliteError::PayloadTooLarge {
                    kind: "workflow_input",
                    ..
                }
            ),
            "got {err:?}"
        );

        let execs: i64 = rt
            .conn
            .query_row("SELECT COUNT(*) FROM harvest_executions", [], |r| r.get(0))
            .unwrap();
        let events: i64 = rt
            .conn
            .query_row("SELECT COUNT(*) FROM harvest_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(execs, 0, "no execution row persisted");
        assert_eq!(events, 0, "no WorkflowStarted event persisted");
    }

    // FIX C (Codex #1069 `runtime.rs:346`): an oversized signal payload to a
    // RUNNING target is rejected at ingress — no `harvest_signals` row staged. A
    // fresh (undriven) execution is RUNNING, so the cap check (which runs AFTER the
    // not-found/terminal checks) is reached. RED pre-fix: the oversized row staged.
    #[test]
    fn oversized_signal_payload_stages_no_row() {
        let mut rt = super::SqliteRuntime::open_in_memory().unwrap();
        rt.register_workflow(&base_wf_info());
        let exec = rt.start_workflow("base_wf", serde_json::json!(0)).unwrap();

        let big = serde_json::json!("y".repeat(256 * 1024 + 1)); // > 256 KiB
        let err = rt.send_signal(exec, "go", big).unwrap_err();
        assert!(
            matches!(
                err,
                super::SqliteError::PayloadTooLarge {
                    kind: "signal_payload",
                    ..
                }
            ),
            "got {err:?}"
        );

        let signals: i64 = rt
            .conn
            .query_row("SELECT COUNT(*) FROM harvest_signals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(signals, 0, "no signal row staged");
    }
}
