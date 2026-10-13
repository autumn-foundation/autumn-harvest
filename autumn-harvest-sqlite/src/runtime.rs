//! The embedded, single-writer runtime: registration, start/signal ingress, the
//! decision loop, and the two atomic-persist sites (dispatch + terminal).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use autumn_harvest::builder::{
    DEFAULT_MAX_ACTIVITY_INPUT_BYTES, DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
    DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::context::{DEFAULT_CURRENT_DETAILS_CAP_BYTES, empty_shared_state};
use autumn_harvest::executor::{
    QueryReplayOutcome, WorkflowExecuteSpanMeta, drive_query_replay_async,
    run_workflow_with_state_history_policy_and_caps,
};
use autumn_harvest::{
    ActivityExecId, ActivityHandlerFn, ActivityInfo, ExecutionId, NoOpMetrics, TimerId,
    UpdateHandlerInfo, UpdateId, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn,
    WorkflowHistoryPolicy, WorkflowIdReusePolicy, WorkflowInfo, WorkflowOutcome,
};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::history_cap::{CapCheck, HistoryCapReason};
use crate::lock::WriterLock;
use crate::query::{DEFAULT_QUERY_TIMEOUT, QueryReplay};
use crate::{composition, lock, queue, schema, store, update, worker};

/// A synchronous activity body, keyed by name.
///
/// [`ActivitySpec::new`] and
/// [`register_activity_with_body`](SqliteRuntime::register_activity_with_body)
/// take one. It gets no `ActivityContext`. The default
/// [`register_activity`](SqliteRuntime::register_activity) runs the
/// `#[activity]` body instead (issue #2024).
pub type ActivityBody = Box<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// What runs for one registered activity (issue #2024).
// Crate-internal like the `ActivitySpec` fields that hold it.
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum ActivityRunner {
    /// A synchronous closure. It runs inline and gets no `ActivityContext`.
    Closure(ActivityBody),
    /// The `#[activity]` handler. The drain awaits it with an `ActivityContext`.
    Handler(ActivityHandlerFn),
}

/// A registered activity: its body plus the declared-default metadata this
/// backend honors at dispatch/finalize.
pub struct ActivitySpec {
    // `ActivitySpec` is re-exported publicly; these fields are crate-internal
    // (read by the worker). `pub` would leak them onto the public type, so
    // `pub(crate)` is deliberate and NOT redundant despite the private module.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) body: ActivityRunner,
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
    /// The activity's declared full default retry policy
    /// (`#[activity(retry = …)]` → `ActivityInfo::default_retry_policy`). Populated
    /// ONLY by the info-based [`register_activity`](SqliteRuntime::register_activity)
    /// — the hand-made [`ActivitySpec::new`] path has no `ActivityInfo` and so is
    /// always `None` (it carries only [`Self::max_attempts`]).
    ///
    /// The whole policy also reaches the worker per-dispatch via the command's
    /// `retry_policy_override` — but ONLY on the TYPED `execute_activity` path. The
    /// RAW `execute_activity_raw` path carries NO override (`task.retry_policy ==
    /// None`), so — mirroring the Postgres dispatch fallback to the registered
    /// activity's `default_retry_policy` — the worker resolves this registry entry
    /// BY NAME at CLAIM time (Codex #1069 P2, `runtime.rs:331`), so a raw call to an
    /// info-registered activity honors the WHOLE policy (backoff timing +
    /// `non_retryable_errors`), not just [`Self::max_attempts`]. `None` = no declared
    /// policy (raw-registered, or `#[activity]` with no `retry`).
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) default_retry_policy: Option<autumn_harvest::policy::RetryPolicy>,
    /// The activity's declared per-attempt start-to-close budget
    /// (`#[activity(start_to_close = "…")]` → `ActivityInfo::default_start_to_close`,
    /// issue #1069 P2). Populated ONLY by the info-based
    /// [`register_activity`](SqliteRuntime::register_activity); the hand-made
    /// [`ActivitySpec::new`] path leaves it `None`.
    ///
    /// Like [`Self::default_retry_policy`], it reaches the worker per-dispatch via the
    /// command's `start_to_close_override` ONLY on the TYPED path. The RAW
    /// `execute_activity_raw` path carries NO override, so the worker resolves this
    /// registry entry BY NAME at CLAIM time (Codex #1069 P2 class audit, sibling of
    /// `runtime.rs:331`) — otherwise a raw call to an info-registered activity would
    /// silently lose its declared start-to-close budget. `None` = no per-attempt
    /// budget (unbounded).
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) start_to_close: Option<std::time::Duration>,
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
            body: ActivityRunner::Closure(Box::new(body)),
            max_attempts: max_attempts.max(1),
            schedule_to_close: None,
            default_retry_policy: None,
            start_to_close: None,
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
    /// The run ended `CANCELLED` (issue #2021). The payload is the reason.
    Cancelled(String),
    /// The run ended `TERMINATED` (issue #2021). The payload is the reason, not
    /// the state name that [`ExecutionOutcome::Terminated`] carries.
    Terminated(String),
    /// The run continued as new (issue #2023). It is sealed
    /// `CONTINUED_AS_NEW`. Drive the successor with this id.
    ContinuedAsNew(ExecutionId),
    /// Blocked awaiting another run that is not terminal (issue #2026).
    ///
    /// The id names the run this run waits on directly: an awaited child or an
    /// `await_external_workflow` target. That run can wait on another run in
    /// turn. Call `run_until_blocked` on it to find the run that needs input.
    ///
    /// This state wins over the other waits, because the runtime can drive
    /// that run itself. The run can also wait on a timer, such as a child
    /// deadline. Drive it again after the deadline.
    WaitingWorkflow(ExecutionId),
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
    /// A terminal state OTHER than a clean success/failure — inert and never
    /// resumable, but carrying no output (unlike [`Self::Completed`]) and no
    /// failure error (unlike [`Self::Failed`]). [`SqliteRuntime::cancel`] and
    /// [`SqliteRuntime::terminate`] write `CANCELLED` and `TERMINATED` (issue
    /// #2021). A `continue_as_new` (issue #2023) or a reuse-policy replace
    /// (issue #1068) writes `CONTINUED_AS_NEW`. [`SqliteRuntime::successor`]
    /// tells those two apart. A drive returns the cancel or terminate reason in
    /// [`RunState`]. Another run can also cancel a run (issue #2026).
    /// The payload is the RAW stored state name (e.g. `"CONTINUED_AS_NEW"`) so a
    /// caller can distinguish which terminal it is. Reporting these terminally —
    /// rather than as [`Self::Running`] — keeps `outcome()` consistent with the
    /// driver's `erase::is_terminal_state` short-circuit (Codex #1080 P2): a client
    /// polling a superseded prior must see it end, not spin forever.
    Terminated(String),
}

/// The result of a start-boundary reuse-policy decision (issue #1068) — returned by
/// [`SqliteRuntime::start_workflow_with_reuse_policy`](crate::SqliteRuntime::start_workflow_with_reuse_policy).
///
/// Mirrors the Postgres core start path's `started_fresh` reporting: `created`
/// distinguishes a freshly inserted run from an attach / return-existing (the
/// caller learns whether its call is the one that started the run).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartOutcome {
    /// The execution the start resolved to — a newly minted id when `created`, or
    /// the existing run's id when the policy attached to / returned it.
    pub exec_id: ExecutionId,
    /// `true` for a fresh insert (a new `WorkflowStarted` was appended); `false`
    /// when the policy returned an existing execution without starting a new run.
    pub created: bool,
}

/// The `error_type` of the `WorkflowFailed` event that seals an execution
/// which reached an unsupported feature (issue #1834).
pub const UNSUPPORTED_FEATURE_ERROR_TYPE: &str = "UnsupportedFeature";

/// The time limit for the replay that rebuilds update handlers, and the
/// default limit for an update handler (issue #2025). It matches the
/// `WorkflowTestEnv` mid-run limit.
const UPDATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
    /// Declarative `#[update]` handlers, keyed by workflow name (issue #2025).
    update_handlers: HashMap<String, Vec<UpdateHandlerInfo>>,
    /// The time limit for one update handler (issue #2025).
    update_timeout: std::time::Duration,
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
    /// The single-writer lock (issue #1834). It is released when the runtime
    /// drops.
    writer_lock: Option<WriterLock>,
    /// The history caps and the soft continue-as-new threshold (issue #2023).
    /// The defaults match the Postgres worker.
    history_policy: WorkflowHistoryPolicy,
    /// The waited-on runs that one `run_until_blocked` call found blocked
    /// (issue #2026). The call clears it at its start and end. Any progress
    /// also clears it, because progress can unblock a run.
    drive_blocked: HashSet<ExecutionId>,
    /// The time limit of one query replay (issue #2022).
    query_timeout: std::time::Duration,
}

impl Drop for SqliteRuntime {
    fn drop(&mut self) {
        if let Some(lock) = &self.writer_lock {
            lock.release(&self.conn);
        }
    }
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
    /// The open first takes the single-writer lock, `<path>.lock` (issue
    /// #1834). It fails fast when another runtime holds that lock. It fails
    /// before any pragma, schema step or reclaim, so it changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::DatabaseLocked`] if another process or runtime
    /// has the file open. Returns [`SqliteError::Io`] if the lock file cannot
    /// be opened. Returns [`SqliteError::Sqlite`] if the file cannot be
    /// opened, the schema cannot be applied, or the orphan reclaim fails.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        let conn = Connection::open(path.as_ref())?;
        let writer_lock = lock::acquire(&conn, path.as_ref())?;
        Self::from_connection(conn, writer_lock)
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
        let writer_lock = lock::acquire(&conn, Path::new(":memory:"))?;
        Self::from_connection(conn, writer_lock)
    }

    /// Apply the pragmas and the schema, and reclaim orphaned tasks.
    fn prepare(conn: &Connection) -> SqliteResult<()> {
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
        // Bring an existing file up to the current shape (issue #1068): the
        // `CREATE TABLE IF NOT EXISTS` above leaves a pre-existing table with its
        // ORIGINAL columns, so this additive, idempotent step adds any column
        // introduced since the file was created (e.g. `harvest_tasks`'s freeze
        // columns) before any read/insert touches them. A no-op on a fresh file.
        schema::migrate(conn)?;
        // Single-server crash recovery: any task left `RUNNING` was claimed by a
        // process that exited without finalizing — flip it back to `PENDING` so
        // its body re-runs (at-least-once).
        queue::reclaim_orphaned_running(conn)?;
        Ok(())
    }

    fn from_connection(conn: Connection, writer_lock: Option<WriterLock>) -> SqliteResult<Self> {
        // A failed setup builds no runtime, so `Drop` never runs. Release the
        // lock here, or an in-memory owner row would outlive the failed open.
        if let Err(err) = Self::prepare(&conn) {
            if let Some(lock) = &writer_lock {
                lock.release(&conn);
            }
            return Err(err);
        }
        Ok(Self {
            conn,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            update_handlers: HashMap::new(),
            update_timeout: UPDATE_TIMEOUT,
            workflow_panic_strikes: HashMap::new(),
            now_fn: std::sync::Arc::new(Utc::now),
            writer_lock,
            history_policy: WorkflowHistoryPolicy::default(),
            drive_blocked: HashSet::new(),
            query_timeout: DEFAULT_QUERY_TIMEOUT,
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

    /// The event cap per run (issue #2023). `None` is unlimited.
    #[must_use]
    pub const fn history_event_hard_cap(&self) -> Option<u64> {
        self.history_policy.event_hard_cap()
    }

    /// Set the event cap per run (issue #2023). `None` removes the cap.
    ///
    /// The default is
    /// [`DEFAULT_HISTORY_EVENT_HARD_CAP`](autumn_harvest::DEFAULT_HISTORY_EVENT_HARD_CAP).
    /// A run that reaches the cap fails with
    /// [`HistoryCapReason::HistoryCapExceeded`](crate::HistoryCapReason::HistoryCapExceeded).
    /// The Postgres builder spells this `history_event_hard_cap(n)` and
    /// `history_event_hard_cap_unlimited()`.
    ///
    /// A cap at or below the soft threshold of `should_continue_as_new` logs a
    /// warning. A run that waits for that advice then fails before it gets it.
    pub fn set_history_event_hard_cap(&mut self, cap: Option<u64>) {
        self.history_policy = match cap {
            Some(cap) => self.history_policy.with_event_hard_cap(cap),
            None => self.history_policy.without_event_hard_cap(),
        };
        let threshold = self.history_policy.continue_as_new_threshold();
        if let Some(cap) = cap
            && cap <= threshold
        {
            tracing::warn!(
                cap,
                threshold,
                "the history event cap is at or below the continue-as-new threshold; \
                 should_continue_as_new() fires only after the cap fails the run"
            );
        }
    }

    /// The stored-history byte cap per run (issue #2023). `None` is unlimited.
    #[must_use]
    pub const fn history_byte_hard_cap(&self) -> Option<u64> {
        self.history_policy.byte_hard_cap()
    }

    /// Set the stored-history byte cap per run (issue #2023). `None` removes
    /// the cap.
    ///
    /// The default is
    /// [`DEFAULT_HISTORY_BYTE_HARD_CAP`](autumn_harvest::DEFAULT_HISTORY_BYTE_HARD_CAP).
    /// A run that reaches the cap fails with
    /// [`HistoryCapReason::HistoryBytesCapExceeded`](crate::HistoryCapReason::HistoryBytesCapExceeded).
    /// The Postgres builder spells this `history_byte_hard_cap(n)` and
    /// `history_byte_hard_cap_unlimited()`.
    pub const fn set_history_byte_hard_cap(&mut self, cap: Option<u64>) {
        self.history_policy = match cap {
            Some(cap) => self.history_policy.with_byte_hard_cap(cap),
            None => self.history_policy.without_byte_hard_cap(),
        };
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
    /// registration misconfiguration) if `info` declares an unsupported execution- or
    /// admission-affecting feature. That includes `execution_timeout`, a
    /// workflow-level `retry_policy` (`#[workflow(retry(...))]`), `concurrency`,
    /// `debounce`, `batch`, `throttle`, `quota`, or a raised `max_input_bytes`. The
    /// panic message names the specific feature and points at the supported
    /// alternative. Running the workflow with the feature silently missing would
    /// diverge from the declared `#[workflow(...)]` contract (Codex #1069 P2,
    /// `runtime.rs:602`/`:686`).
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

    /// Register an activity from its macro-generated [`ActivityInfo`]. The
    /// `#[activity]` body itself runs (issue #2024).
    ///
    /// The drain awaits `info.handler` on the caller's async runtime, with a real
    /// [`ActivityContext`](autumn_harvest::ActivityContext). So one `#[activity]`
    /// fn runs unchanged on this backend and on Postgres:
    ///
    /// - `info()` reports the same dispatch values as the Postgres worker. They
    ///   are the run, the activity, the 1-based attempt, the frozen attempt cap,
    ///   the task queue and the task row id.
    /// - `deadline()` is the earlier of claim time plus `start_to_close` and the
    ///   `schedule_to_close` deadline.
    /// - `heartbeat(..)` persists the latest payload on the task row. The next
    ///   attempt, also after a crash and reopen, reads it with
    ///   `heartbeat_details()`.
    /// - `previous_failure()` is the previous attempt's error message.
    /// - The cancellation token never fires on this backend.
    ///
    /// A panic in the body becomes a retryable `HandlerPanic` failure, as on
    /// Postgres. A body that never returns blocks the drive. If the caller drops
    /// the drive future, for example with a timeout, the next drive of that run
    /// re-runs the body, as after a crash.
    ///
    /// Some values differ from Postgres by design:
    ///
    /// - With no declared `retry`, this backend runs one attempt, so
    ///   `max_attempts()` is `1`. Postgres applies its engine default.
    /// - A re-run after a crash or a dropped drive keeps its `attempt` number.
    ///   Postgres counts the lost attempt.
    /// - `state()` is always `None`, `headers()` is empty, and `metrics()`
    ///   records nothing.
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
    ///   `default_retry_policy` (the *whole* policy — backoff timing +
    ///   `non_retryable_errors`, not just `max_attempts` — reaches the worker via the
    ///   command's `retry_policy_override` on the TYPED path AND, for the RAW
    ///   `execute_activity_raw` path (no override), via a CLAIM-time fallback to this
    ///   registry entry, Codex #1069 P2 `runtime.rs:331`), `default_start_to_close`
    ///   (honored via the command's `start_to_close_override` on the TYPED path AND
    ///   via the same claim-time registry fallback on the RAW path), and
    ///   `default_queue` (recorded; single-writer routing is a no-op).
    /// - **REJECTED** at registration (setup-time panic, mirroring
    ///   [`register_workflow`]): `default_heartbeat_timeout`,
    ///   `default_schedule_to_start`, `circuit_breaker`, `rate_limit_*`,
    ///   `max_concurrent`, `requires`, `is_local`, and a raised
    ///   `max_input_bytes` / `max_result_bytes`.
    /// - **ACCEPTED, inert**: `name` (registration key), `module` (diagnostics),
    ///   and a lone `concurrency_key` (groups a cap that is not set).
    ///
    /// # Panics
    ///
    /// Panics (setup-time) if `info` declares an unsupported `ActivityInfo`-level
    /// feature (see the list above). The panic names the feature and points at the
    /// supported alternative; registering it anyway would silently drop the setting
    /// and diverge from the declared `#[activity(...)]` contract. Use
    /// [`register_activity_raw`](Self::register_activity_raw) for a hand-made body
    /// that carries no declared defaults.
    pub fn register_activity(&mut self, info: &ActivityInfo) {
        self.register_audited_activity(info, ActivityRunner::Handler(info.handler));
    }

    /// Register an activity from its [`ActivityInfo`], but run a synchronous
    /// `body` in place of the `#[activity]` body.
    ///
    /// The audit and the declared defaults are the same as for
    /// [`register_activity`](Self::register_activity). The closure gets no
    /// `ActivityContext`. Use it to stub an activity in a test. Before issue
    /// #2024 this was the only form, under the name `register_activity`.
    ///
    /// # Panics
    ///
    /// Panics on the same unsupported features as
    /// [`register_activity`](Self::register_activity).
    pub fn register_activity_with_body(
        &mut self,
        info: &ActivityInfo,
        body: impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    ) {
        self.register_audited_activity(info, ActivityRunner::Closure(Box::new(body)));
    }

    /// Audit `info` and register `body` under its name with its declared
    /// defaults.
    fn register_audited_activity(&mut self, info: &ActivityInfo, body: ActivityRunner) {
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
        // Derive the fallback attempt cap from the declared retry policy. On the TYPED
        // `execute_activity` path the WHOLE policy also reaches the worker per-dispatch
        // via the command's `retry_policy_override`, which wins over the registry
        // fallback; on the RAW `execute_activity_raw` path the command carries NO
        // override, so the worker resolves the full `default_retry_policy` (below) from
        // this registry entry at CLAIM time. No declared policy ⇒ a single attempt
        // (matching `ActivitySpec::new`'s `>= 1` clamp).
        let max_attempts = info
            .default_retry_policy
            .as_ref()
            .map_or(1, |p| p.max_attempts.max(1));
        self.activities.insert(
            info.name.to_string(),
            ActivitySpec {
                body,
                max_attempts,
                schedule_to_close: info.default_schedule_to_close,
                // Store the WHOLE policy (backoff + `non_retryable_errors`), not just
                // its `max_attempts` — the RAW dispatch path falls back to it at claim
                // time (Codex #1069 P2, `runtime.rs:331`).
                default_retry_policy: info.default_retry_policy.clone(),
                // Store the declared start-to-close so the RAW path can fall back to it
                // at claim time too (class-audit sibling of `default_retry_policy`).
                start_to_close: info.default_start_to_close,
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

    /// Set the time limit for one update handler. The default is 5 seconds.
    /// A deterministic-test seam only, as [`set_clock`](Self::set_clock) is.
    #[doc(hidden)]
    pub const fn set_update_timeout(&mut self, timeout: std::time::Duration) {
        self.update_timeout = timeout;
    }

    /// Register a declarative `#[update]` handler from its
    /// [`UpdateHandlerInfo`] (issue #2025).
    ///
    /// The handler serves only runs of `info.workflow`. The runtime keeps the
    /// first handler for a name and ignores a later one, as the core does.
    pub fn register_update(&mut self, info: &UpdateHandlerInfo) {
        let handlers = self
            .update_handlers
            .entry(info.workflow.to_string())
            .or_default();
        if !handlers.iter().any(|h| h.name == info.name) {
            handlers.push(info.clone());
        }
    }

    /// Start a fresh workflow execution, appending its `WorkflowStarted` event.
    ///
    /// The run's business `workflow_id` (the value `ctx.info().workflow_id` reports,
    /// issue #698 — documented for minting idempotency keys) defaults to the
    /// generated [`ExecutionId`]'s string form, so it is never empty and distinct
    /// per run. To supply your own business id (cross-backend parity with the
    /// Postgres `StartWorkflowParams.workflow_id`), use
    /// [`start_workflow_with_id`](Self::start_workflow_with_id).
    ///
    /// Every call starts a new, independent execution. Because the default
    /// `workflow_id` is derived from a fresh [`ExecutionId`], id reuse cannot even
    /// arise here; the v0.1 non-idempotent start contract (no
    /// `(workflow_name, workflow_id)` uniqueness / no reuse-policy matrix) is
    /// documented in full on
    /// [`start_workflow_with_id`](Self::start_workflow_with_id).
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
        // No business id ⇒ the run-scoped `workflow_id` defaults to the fresh
        // exec-id string form (unique per run), so the reuse lookup never matches
        // a prior and every call creates a distinct execution — unchanged v0.1
        // behavior. Routed through the policy path (with the `AllowDuplicate`
        // default) purely for code reuse; the outcome is always `created == true`.
        Ok(self
            .start_workflow_with_policy_inner(
                workflow_name,
                None,
                input,
                WorkflowIdReusePolicy::AllowDuplicate,
            )?
            .exec_id)
    }

    /// Like [`start_workflow`](Self::start_workflow) but with a caller-supplied
    /// business `workflow_id` — the run-scoped identifier `ctx.info().workflow_id`
    /// reports (issue #698), for minting idempotency keys or correlating a run to
    /// an external entity. This is the cross-backend analog of the Postgres
    /// `StartWorkflowParams.workflow_id`: the workflow observes EXACTLY the id
    /// passed here, stable across every replay.
    ///
    /// An empty/whitespace-only `workflow_id` falls back to the generated
    /// [`ExecutionId`]'s string form (guaranteeing `ctx.info().workflow_id` is
    /// never empty) and therefore has no reuse key — so a blank id ALWAYS creates a
    /// fresh, distinct run, matching the [`start_workflow`](Self::start_workflow)
    /// default.
    ///
    /// # Start semantics — idempotent by default (issue #1068)
    ///
    /// This method applies the DEFAULT reuse policy
    /// [`WorkflowIdReusePolicy::AllowDuplicate`]: a non-blank `workflow_id` that
    /// matches an existing, non-sealed execution for the same `workflow_name`
    /// **attaches** — the SAME [`ExecutionId`] is returned and **no** second run is
    /// started — so a duplicate delivery (a retried webhook) is idempotent rather
    /// than starting a second run and repeating its side effects.
    ///
    /// **This is an intentional behavior change from v0.1**, which always minted a
    /// fresh execution here (a duplicate delivery started a second run). For any
    /// other policy — reject-the-duplicate, replace-a-failed-run, or
    /// terminate-and-restart — and to learn whether the call created a fresh run or
    /// attached, use
    /// [`start_workflow_with_reuse_policy`](Self::start_workflow_with_reuse_policy),
    /// which exposes the full `(workflow_name, workflow_id)` reuse matrix and
    /// returns a [`StartOutcome`].
    ///
    /// The reuse decision is made atomically in the single-writer start
    /// transaction (there are no concurrent writers, so a lookup-then-decide is
    /// race-free without a database uniqueness index). A prior in a SEALED state
    /// (`CONTINUED_AS_NEW`/`TERMINATED`) is invisible to the reuse check, so a
    /// superseded run never blocks a fresh start — mirroring the Postgres core's
    /// uniqueness-index-excludes-terminal nuance.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::UnknownWorkflow`] if `workflow_name` has no
    /// registered handler, [`SqliteError::PayloadTooLarge`] if `input` exceeds the
    /// workflow-input cap, or [`SqliteError::Sqlite`]/[`SqliteError::Json`] on a
    /// persistence failure. (Under `AllowDuplicate` it never returns
    /// [`SqliteError::AlreadyExists`] — that is only for
    /// [`RejectDuplicate`](WorkflowIdReusePolicy::RejectDuplicate).)
    pub fn start_workflow_with_id(
        &mut self,
        workflow_name: &str,
        workflow_id: &str,
        input: Value,
    ) -> SqliteResult<ExecutionId> {
        Ok(self
            .start_workflow_with_policy_inner(
                workflow_name,
                Some(workflow_id),
                input,
                WorkflowIdReusePolicy::AllowDuplicate,
            )?
            .exec_id)
    }

    /// Start (or reuse) a workflow under an explicit
    /// [`WorkflowIdReusePolicy`](autumn_harvest::WorkflowIdReusePolicy),
    /// enforcing the `(workflow_name, workflow_id)` reuse matrix (issue #1068), and
    /// return a [`StartOutcome`] reporting the resolved execution and whether a
    /// fresh run was created.
    ///
    /// The single-writer PLAIN-start matrix covers the states this backend can
    /// hold: none, `RUNNING`, `COMPLETED`, `FAILED` and `CANCELLED`. A replaced
    /// prior is SEALED to `CONTINUED_AS_NEW`.
    ///
    /// | Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
    /// |-------------|------------------|-------------------|----------------------------|----------------------|
    /// | none        | fresh            | fresh             | fresh                      | fresh                |
    /// | RUNNING     | existing         | `AlreadyExists`   | existing                   | cancel + fresh       |
    /// | COMPLETED   | existing         | `AlreadyExists`   | existing                   | fresh (no cancel)    |
    /// | FAILED      | existing         | `AlreadyExists`   | **fresh (replace)**        | fresh (no cancel)    |
    /// | CANCELLED   | existing         | `AlreadyExists`   | **fresh (replace)**        | fresh (no cancel)    |
    ///
    /// - A prior in a SEALED state (`CONTINUED_AS_NEW`/`TERMINATED`) is invisible to
    ///   the reuse check — ALL policies (including `RejectDuplicate`) create fresh
    ///   over it, mirroring core's uniqueness-index-excludes-terminal nuance.
    /// - "Replace" SEALS the prior to `CONTINUED_AS_NEW`, so it leaves the active
    ///   set and the fresh run takes the key (mirroring core's
    ///   `replace_execution`). `AllowDuplicateFailedOnly` replaces a FAILED or
    ///   `CANCELLED` prior. `TerminateIfRunning` replaces any prior.
    /// - `TerminateIfRunning` over a still-`RUNNING` prior additionally appends a
    ///   `WorkflowCancelled` event to the prior's history (recording the forced
    ///   cancellation) and cleans up its PENDING task rows and unfired timers, then
    ///   seals it. Because the single-writer backend replaces the prior atomically
    ///   in the same transaction and never drives it again, this forcefully seals
    ///   the prior (observably equivalent to core's outcome — prior inactive with a
    ///   cancellation recorded, new run active) rather than delivering cooperative
    ///   cancellation the prior's body could observe.
    /// - A blank/whitespace-only `workflow_id` has no reuse key, so every call
    ///   creates a fresh, distinct run (`created == true`) regardless of `policy`.
    ///
    /// This is NOT the signal-with-start matrix — a plain start RETURNS a terminal
    /// `COMPLETED`/`FAILED` prior under `AllowDuplicate` (it does not escalate to a
    /// fresh start).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::AlreadyExists`] when `policy` is
    /// [`RejectDuplicate`](WorkflowIdReusePolicy::RejectDuplicate) and a non-sealed
    /// prior exists; [`SqliteError::UnknownWorkflow`] for an unregistered workflow;
    /// [`SqliteError::PayloadTooLarge`] for an oversized `input`; or
    /// [`SqliteError::Sqlite`]/[`SqliteError::Json`] on a persistence failure.
    pub fn start_workflow_with_reuse_policy(
        &mut self,
        workflow_name: &str,
        workflow_id: &str,
        input: Value,
        policy: WorkflowIdReusePolicy,
    ) -> SqliteResult<StartOutcome> {
        self.start_workflow_with_policy_inner(workflow_name, Some(workflow_id), input, policy)
    }

    /// Shared start path applying the `(workflow_name, workflow_id)` reuse matrix
    /// (issue #1068). `workflow_id`: `Some(id)` uses the caller's business id (a
    /// blank id has no reuse key ⇒ always fresh); `None` (from
    /// [`start_workflow`](Self::start_workflow)) always creates fresh with the
    /// exec-id string as its `workflow_id`.
    ///
    /// The reuse lookup + decision + all writes happen in ONE transaction; the
    /// single-writer contract makes the lookup-then-decide atomic without a
    /// database uniqueness index. A `RejectDuplicate` collision returns
    /// `Err(AlreadyExists)`, dropping the transaction uncommitted (no writes).
    fn start_workflow_with_policy_inner(
        &mut self,
        workflow_name: &str,
        workflow_id: Option<&str>,
        input: Value,
        policy: WorkflowIdReusePolicy,
    ) -> SqliteResult<StartOutcome> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(SqliteError::UnknownWorkflow(workflow_name.to_string()));
        }
        // Enforce the workflow-input cap BEFORE any write (issue #252, Codex #1069
        // `runtime.rs:264`). The Postgres start path rejects an oversized input with
        // `PayloadTooLarge` before persisting anything; without this the SQLite
        // backend would store/replay an unbounded input and accept a start other
        // backends reject. Measured as the serialized-JSON byte length, exactly as
        // the core does. A zero cap means uncapped (the core convention); the default
        // `DEFAULT_MAX_WORKFLOW_INPUT_BYTES` (2 MiB) is > 0, so the check is active.
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

        // A non-blank caller-supplied id is the reuse key; a blank/None id has no
        // reuse key (it defaults to the fresh exec-id string form, unique per run),
        // so it always creates a distinct execution regardless of `policy`.
        let business_id = workflow_id.filter(|id| !id.trim().is_empty());

        let tx = self.conn.transaction()?;
        let outcome = match business_id {
            // No reuse key → always fresh; `workflow_id` defaults to the exec-id
            // string form so `ctx.info().workflow_id` is never empty and is
            // distinct per run (issue #698).
            None => insert_fresh_execution(&tx, workflow_name, None, input)?,
            Some(id) => {
                let prior = store::find_active_execution_by_key(&tx, workflow_name, id)?;
                match (policy, prior) {
                    // No active prior → create fresh under every policy.
                    (_, None) => insert_fresh_execution(&tx, workflow_name, Some(id), input)?,

                    // Return the existing execution unchanged.
                    (WorkflowIdReusePolicy::AllowDuplicate, Some((prior_exec, _))) => {
                        StartOutcome {
                            exec_id: prior_exec,
                            created: false,
                        }
                    }

                    // Refuse the duplicate — drop the (uncommitted) transaction so no
                    // fresh row is written, and surface the collision.
                    (
                        WorkflowIdReusePolicy::RejectDuplicate,
                        Some((existing_exec_id, existing_state)),
                    ) => {
                        return Err(SqliteError::AlreadyExists {
                            existing_exec_id,
                            existing_state,
                        });
                    }

                    // Replace a FAILED or CANCELLED prior, as on Postgres.
                    // Otherwise return the existing run.
                    (
                        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
                        Some((prior_exec, prior_state)),
                    ) => {
                        if prior_state == "FAILED" || prior_state == CANCELLED_STATE {
                            cancel_and_seal_prior(&tx, prior_exec, &prior_state)?;
                            insert_fresh_execution(&tx, workflow_name, Some(id), input)?
                        } else {
                            StartOutcome {
                                exec_id: prior_exec,
                                created: false,
                            }
                        }
                    }

                    // Cancel + seal EVERY active prior for the key, then start fresh.
                    // A pre-#1068 database may already hold MULTIPLE RUNNING rows for
                    // one key (the old always-fresh behavior), so sealing only the
                    // newest (`Some(_)` here) would leave older RUNNING duplicates
                    // active and still driven by `run_until_idle` — defeating the
                    // terminate-and-restart guarantee (Codex #1080 P2). Re-query the
                    // FULL active set and reconcile every row: a still-RUNNING prior
                    // records a `WorkflowCancelled` event and has its PENDING tasks /
                    // unfired timers cleaned up; an already-terminal prior is sealed
                    // without a cancellation event. All happen in this one start
                    // transaction, so the result is exactly one active row (the fresh
                    // run) with no orphaned RUNNING prior. (When there is no active
                    // prior at all the `(_, None)` arm above already handles it.)
                    (WorkflowIdReusePolicy::TerminateIfRunning, Some(_)) => {
                        for (prior_exec, prior_state) in
                            store::find_active_executions_by_key(&tx, workflow_name, id)?
                        {
                            cancel_and_seal_prior(&tx, prior_exec, &prior_state)?;
                        }
                        insert_fresh_execution(&tx, workflow_name, Some(id), input)?
                    }
                }
            }
        };
        tx.commit()?;
        Ok(outcome)
    }

    /// Deliver an inbound signal. The next drive appends it to history.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id and
    /// [`SqliteError::WorkflowNotRunning`] for a terminal execution. Returns
    /// [`SqliteError::PayloadTooLarge`] for an oversized payload. Returns a
    /// persistence error if the signal row cannot be written.
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
    /// See [`send_signal`](Self::send_signal).
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
        // has no FK or state check. An unconditional stage returns `Ok(())` for
        // a typoed id or a sealed run. No drive will ever consume that row, so
        // the wakeup is silently lost. Mirror the Postgres `send_signal`
        // rejections. `store::execution_state` returns `ExecutionNotFound` for an
        // unknown id. A terminal execution is `WorkflowNotRunning`. A RUNNING
        // (non-terminal) execution stages as before.
        //
        // The single-writer runtime holds `SQLite`'s only write handle, so the
        // state read and the stage cannot race a concurrent transition: no other
        // writer exists to seal the execution between the check and the insert.

        // Issue #1985: a promise signal must carry a settlement, and a promise
        // settles once. The Postgres path drops a second settlement through its
        // idempotency key. This backend has no key, so it skips the stage. The
        // check runs before the terminal check, so a retry after the run ends
        // is a no-op here too, as on Postgres.
        if name.starts_with(autumn_harvest::durable_promise::PROMISE_SIGNAL_PREFIX) {
            // A missing run reports `NotFound` before the payload check.
            store::execution_state(&self.conn, exec)?;
            autumn_harvest::durable_promise::settlement_idempotency_key(name, &payload, None)
                .map_err(|e| SqliteError::InvalidSignal(e.to_string()))?;
            if store::has_signal(&self.conn, exec, name)? {
                return Ok(());
            }
        }
        self.refuse_unless_running(exec)?;
        // Enforce the signal-payload cap at INGRESS (issue #252, Codex #1069
        // `runtime.rs:346`), before the `harvest_signals` row is staged.
        // Otherwise the next drive records an oversized `SignalReceived` event.
        // That bloats history and accepts a payload that Postgres rejects at its
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

    /// Request a cooperative cancel of `exec` (issue #2021).
    ///
    /// The call stores the request and appends no event. The next drive
    /// replays the body with `ctx.cancellation_reason()` set to `reason`, so
    /// the body can run cleanup activities. A blank `reason` becomes
    /// `"workflow cancellation requested"`, as on Postgres.
    ///
    /// The run ends `CANCELLED` when one of these occurs:
    ///
    /// - The body returns `Ok` or `Err`, or continues as new. The output of an
    ///   `Ok` is not kept, and no successor starts.
    /// - The body blocks on a signal or a timer with no activity task left to
    ///   run. A parked await cannot see the request.
    /// - The cycle diverges or deadlocks. The body sees the request from the
    ///   start of its replay, so a check before recorded commands diverges.
    /// - The run has added [`CANCEL_CLEANUP_EVENT_BUDGET`] events since the
    ///   request, so a body that ignores the request cannot run forever.
    /// - The drive meets an error that repeats on every drive: an
    ///   unregistered activity or workflow, or a stuck cycle.
    ///
    /// The run then gets `WorkflowCancelled` as its last event, and its pending
    /// tasks, unfired timers and staged signals are deleted. The drive that ends
    /// the run returns [`RunState::Cancelled`].
    ///
    /// A repeat call on a run with a pending request keeps the first reason.
    /// Use [`terminate`](Self::terminate) to stop a run at once.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id and
    /// [`SqliteError::WorkflowNotRunning`] for a terminal execution. Postgres
    /// treats a cancel of a `CANCELLED` run as a no-op, but this backend refuses
    /// it. Returns a persistence error if the request cannot be written.
    pub fn cancel(&mut self, exec: ExecutionId, reason: &str) -> SqliteResult<()> {
        self.refuse_unless_running(exec)?;
        let reason = reason_or(reason, DEFAULT_CANCEL_REASON);
        store::request_cancel(&self.conn, exec, &reason)
    }

    /// Terminate `exec` at once (issue #2021).
    ///
    /// One transaction appends `WorkflowCancelled { reason }`, ends the run
    /// `TERMINATED`, and deletes its pending tasks, unfired timers and staged
    /// signals. The body does not run again, and `terminate` drops a pending
    /// cancel request. A later drive returns [`RunState::Terminated`]. A blank
    /// `reason` becomes `"workflow termination requested"`, as on Postgres.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id and
    /// [`SqliteError::WorkflowNotRunning`] for a terminal execution. Postgres
    /// treats a terminate of a terminal run as a no-op, but this backend refuses
    /// it. Returns a persistence error if the seal cannot be written.
    pub fn terminate(&mut self, exec: ExecutionId, reason: &str) -> SqliteResult<()> {
        self.refuse_unless_running(exec)?;
        let reason = reason_or(reason, DEFAULT_TERMINATE_REASON);
        let tx = self.conn.transaction()?;
        let now = (self.now_fn)().timestamp_millis();
        seal_with_cancel_event(&tx, exec, TERMINATED_STATE, &reason, now)?;
        tx.commit()?;
        self.workflow_panic_strikes.remove(&exec);
        Ok(())
    }

    /// Returns `Ok` only when `exec` is a stored, non-terminal execution.
    ///
    /// The single writer holds the only write handle, so no transition can
    /// happen between this check and the caller's write.
    fn refuse_unless_running(&self, exec: ExecutionId) -> SqliteResult<()> {
        let state = store::execution_state(&self.conn, exec)?;
        if autumn_harvest::erase::is_terminal_state(&state) {
            return Err(SqliteError::WorkflowNotRunning {
                execution_id: exec,
                state,
            });
        }
        Ok(())
    }

    /// End a cancel-requested `exec` `CANCELLED` (issue #2021).
    ///
    /// `batch` holds the events of the cycle that ends the run. They persist
    /// before the terminal `WorkflowCancelled` event, as on the completed and
    /// failed paths (issue #2020). A batch that reaches a history cap or the
    /// cleanup budget is discarded.
    ///
    /// The seal drops each pending task. The body can await such a task, and
    /// replay then parks before `WorkflowCancelled`. So each dropped task first
    /// gets a non-retryable `ActivityFailed`. The failures must fit the history
    /// caps. They do not count toward the cleanup budget, because each one
    /// resolves a task that the budget already admitted. Failures that reach a
    /// cap roll back, and the seal then adds only `WorkflowCancelled`.
    fn seal_cancelled(
        &mut self,
        exec: ExecutionId,
        reason: &str,
        batch: Option<CancelBatch<'_>>,
        now: i64,
    ) -> SqliteResult<RunState> {
        if let Some(batch) = batch {
            let caps = CapCheck {
                policy: self.history_policy,
                start_bytes: batch.caps.start_bytes,
                workflow_type: batch.caps.workflow_type,
            };
            let tx = self.conn.transaction()?;
            batch.ingest.apply(&tx, exec)?;
            let history = store::load_history(&tx, exec)?;
            let drain = TerminalDrain {
                workflows: &self.workflows,
                history: &history,
                cycle: TerminalCycle::Completing,
                now,
            };
            persist_terminal_pending_commands(&tx, &drain, exec, batch.pending)?;
            if batch.caps.reached(&tx, exec)?.is_none() {
                fail_dropped_tasks(&tx, exec, &self.activities)?;
                if caps.reached(&tx, exec)?.is_none() {
                    seal_with_cancel_event(&tx, exec, CANCELLED_STATE, reason, now)?;
                    tx.commit()?;
                    return Ok(RunState::Cancelled(reason.to_string()));
                }
            }
        }
        let (_, history_bytes) = store::load_history_sized(&self.conn, exec)?;
        let workflow_name = store::workflow_name_of(&self.conn, exec)?;
        let caps = CapCheck {
            policy: self.history_policy,
            start_bytes: history_bytes,
            workflow_type: &workflow_name,
        };
        let tx = self.conn.transaction()?;
        fail_dropped_tasks(&tx, exec, &self.activities)?;
        if caps.reached(&tx, exec)?.is_some() {
            drop(tx);
            let tx = self.conn.transaction()?;
            seal_with_cancel_event(&tx, exec, CANCELLED_STATE, reason, now)?;
            tx.commit()?;
            return Ok(RunState::Cancelled(reason.to_string()));
        }
        seal_with_cancel_event(&tx, exec, CANCELLED_STATE, reason, now)?;
        tx.commit()?;
        Ok(RunState::Cancelled(reason.to_string()))
    }

    /// Run the update `name` with `input` against `exec`, and return its
    /// result (issue #2025).
    ///
    /// The runtime replays the history into a new context, as a query does, so
    /// the workflow body registers its handlers. Declarative handlers from
    /// [`register_update`](Self::register_update) register too. The arg schema
    /// and the validator run first. A rejection writes no event.
    ///
    /// The handler then runs, with a 5 second limit. It can await any future,
    /// but not a durable command. One transaction appends
    /// `UpdateAdmitted` and its `UpdateCompleted` or `UpdateFailed`, so a crash
    /// leaves no admitted update without a result. The events are the core
    /// update events, so the history replays on any backend.
    ///
    /// The update does not drive the run. Drive it with
    /// [`run_until_blocked`](Self::run_until_blocked), as after
    /// [`send_signal`](Self::send_signal).
    ///
    /// The handler runs once, at admission, on that separate replay. State that
    /// the handler writes does not reach the workflow body. Use an update to
    /// compute a result from the workflow state, not to change that state.
    /// Replay returns the recorded result and does not run the handler again.
    ///
    /// # Errors
    ///
    /// Each error below writes no event, except
    /// [`SqliteError::UpdateFailed`].
    ///
    /// - [`SqliteError::ExecutionNotFound`] for an unknown id.
    /// - [`SqliteError::WorkflowNotRunning`] for a terminal run.
    /// - [`SqliteError::UnknownWorkflow`] if the workflow is not registered.
    /// - [`SqliteError::UpdateInputInvalid`] if `input` fails the arg schema
    ///   of a declarative handler.
    /// - [`SqliteError::UpdateRejected`] if the validator rejects `input` or
    ///   panics.
    /// - [`SqliteError::UpdateHandlerNotFound`] if no handler has the name.
    /// - [`SqliteError::WorkflowPanicked`] or [`SqliteError::TaskFailed`] if
    ///   the replay that rebuilds the handlers panics or does not settle.
    /// - [`SqliteError::NonDeterministic`] if that replay diverges from the
    ///   history.
    /// - [`SqliteError::UpdateTimedOut`] if the handler does not finish in
    ///   time, for example because it awaits a durable command.
    /// - [`SqliteError::UpdateHistoryCapReached`] if the two update events
    ///   would reach a history cap. The run then fails at the cap, as a drive
    ///   fails it. Under a pending cancel the cleanup budget is one more cap,
    ///   and the run ends `CANCELLED` instead.
    /// - [`SqliteError::UpdateFailed`] if the handler returns an error or
    ///   panics. History records `WorkflowEvent::UpdateFailed`.
    /// - A persistence error if the history cannot be read or written.
    pub async fn update(
        &mut self,
        exec: ExecutionId,
        name: &str,
        input: Value,
    ) -> SqliteResult<Value> {
        self.refuse_unless_running(exec)?;
        let workflow_name = store::workflow_name_of(&self.conn, exec)?;
        let handler = *self
            .workflows
            .get(&workflow_name)
            .ok_or_else(|| SqliteError::UnknownWorkflow(workflow_name.clone()))?;
        let declarative = self.declarative_updates(&workflow_name);
        let rejected = |reason: String| SqliteError::UpdateRejected {
            execution_id: exec,
            name: name.to_string(),
            reason,
        };
        // The arg schema runs before the validator, as on Postgres (issue #610).
        if let Some(info) = declarative.iter().find(|h| h.name == name)
            && let Err(violations) = info.validate_arg(&input)
        {
            return Err(SqliteError::UpdateInputInvalid {
                execution_id: exec,
                name: name.to_string(),
                violations,
            });
        }

        let (ctx, run_input, history_bytes) =
            self.update_context(exec, &workflow_name, &declarative)?;
        settle_update_context(exec, &ctx, handler, run_input).await?;
        // A validator is user code. Contain its panic, as the handler poll does.
        let validated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.validate_update(name, &input)
        }))
        .map_err(|panic| {
            rejected(format!(
                "update validator panicked: {}",
                autumn_harvest::error::panic_message(panic)
            ))
        })?;
        validated.map_err(|error| match error {
            autumn_harvest::HarvestError::UpdateRejected { reason } => rejected(reason),
            autumn_harvest::HarvestError::UpdateHandlerNotFound(_) => {
                SqliteError::UpdateHandlerNotFound {
                    execution_id: exec,
                    name: name.to_string(),
                }
            }
            other => rejected(other.to_string()),
        })?;

        // The rebuild replayed every event, so the new update has no result in
        // history. `execute_admitted_update` therefore runs the handler live.
        let update_id = UpdateId::new();
        let run = update::run_handler(
            ctx.execute_admitted_update(update_id, name, input.clone()),
            self.update_timeout,
        )
        .await;
        let update::HandlerRun::Finished(result) = run else {
            return Err(SqliteError::UpdateTimedOut {
                execution_id: exec,
                name: name.to_string(),
            });
        };

        let now = (self.now_fn)();
        let admitted = WorkflowEvent::UpdateAdmitted {
            update_id,
            name: name.to_string(),
            input,
            timestamp: now,
        };
        let outcome = update::result_event(update_id, &result);
        // The caps bound history whether or not a drive follows (issue #2023).
        // The check counts both new events, as the drive counts a decision.
        let added = [&admitted, &outcome]
            .iter()
            .map(|e| serde_json::to_string(e).map(|json| json.len() as u64))
            .sum::<Result<u64, _>>()?;
        // Issue #2021: under a pending cancel the cleanup budget is one more
        // event cap, as on the drive.
        let budget_end = store::cancel_request(&self.conn, exec)?
            .map(|(_, from)| from.saturating_add(CANCEL_CLEANUP_EVENT_BUDGET));
        let caps = CapCheck {
            policy: self.cancel_policy(budget_end),
            start_bytes: history_bytes.saturating_add(added),
            workflow_type: &workflow_name,
        };
        let tx = self.conn.transaction()?;
        store::append_event(&tx, exec, &admitted)?;
        store::append_event(&tx, exec, &outcome)?;
        if let Some(reason) = caps.reached(&tx, exec)? {
            // Roll the update back, then end the run as a drive does. A
            // pending cancel wins over the cap (issue #2021).
            drop(tx);
            if let Some((cancel, _)) = store::cancel_request(&self.conn, exec)? {
                self.seal_cancelled(exec, &cancel, None, now.timestamp_millis())?;
            } else {
                self.fail_for_history_cap(exec, &reason, now.timestamp_millis())?;
            }
            return Err(SqliteError::UpdateHistoryCapReached {
                execution_id: exec,
                name: name.to_string(),
                reason,
            });
        }
        tx.commit()?;

        result.map_err(|error| SqliteError::UpdateFailed {
            execution_id: exec,
            name: name.to_string(),
            error,
        })
    }

    /// The declarative update handlers of `workflow_name`.
    fn declarative_updates(&self, workflow_name: &str) -> Vec<UpdateHandlerInfo> {
        self.update_handlers
            .get(workflow_name)
            .cloned()
            .unwrap_or_default()
    }

    /// A replay context for `exec`, with the declarative update handlers of
    /// its workflow, the run input and the stored history bytes.
    ///
    /// It reads but does not drive. [`settle_update_context`] replays the
    /// body so it registers its own handlers.
    fn update_context(
        &self,
        exec: ExecutionId,
        workflow_name: &str,
        declarative: &[UpdateHandlerInfo],
    ) -> SqliteResult<(WorkflowContext, Value, u64)> {
        let (history, history_bytes) = store::load_history_sized(&self.conn, exec)?;
        let input = store::execution_input(&self.conn, exec)?;
        let workflow_id = store::workflow_id_of(&self.conn, exec)?;
        // The body sees a pending cancel here, as on the next drive (issue
        // #2021).
        let cancel_reason = store::cancel_request(&self.conn, exec)?.map(|(reason, _)| reason);
        // A child sees its parent here, as on a drive (issue #2026).
        let parent = store::parent_link(&self.conn, exec)?.map(|(parent, _)| parent);
        // The same policy as the drive, so `should_continue_as_new` replays the
        // same branch (issue #2023).
        let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
            exec,
            history,
            empty_shared_state(),
            self.history_policy,
        )
        .with_workflow_name(workflow_name)
        .with_workflow_id(workflow_id)
        .with_parent_execution_id(parent)
        .with_pending_cancellation(cancel_reason);
        for info in declarative {
            ctx.register_declarative_update_handler(info);
        }
        Ok((ctx, input, history_bytes))
    }

    /// The run that `exec` continued as, if any (issue #2023).
    ///
    /// `Some` only for a run that `continue_as_new` sealed. A reuse-policy seal
    /// shares the `CONTINUED_AS_NEW` state but has no successor.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns a
    /// persistence error if the history cannot be read.
    pub fn successor(&self, exec: ExecutionId) -> SqliteResult<Option<ExecutionId>> {
        let state = store::execution_state(&self.conn, exec)?;
        if state != SEALED_STATE {
            return Ok(None);
        }
        store::successor_of(&self.conn, exec)
    }

    /// The full ordered event history — the canonical, replayable log.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns a
    /// persistence error if the history cannot be read or a stored event
    /// cannot be parsed.
    pub fn load_history(&self, exec: ExecutionId) -> SqliteResult<Vec<WorkflowEvent>> {
        self.known_or_not_found(exec, store::load_history(&self.conn, exec)?)
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
    /// A known execution with no attempts for `activity_name` returns an empty
    /// list.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns a
    /// persistence error if the audit rows cannot be read.
    pub fn activity_attempts(
        &self,
        exec: ExecutionId,
        activity_name: &str,
    ) -> SqliteResult<Vec<store::ActivityAttempt>> {
        self.known_or_not_found(exec, store::load_attempts(&self.conn, exec, activity_name)?)
    }

    /// Returns `rows`, or [`SqliteError::ExecutionNotFound`] when `rows` is
    /// empty and `exec` has no execution row (issue #1735).
    ///
    /// The existence check runs only on an empty result. A non-empty result
    /// proves that `exec` exists, so the common read costs no extra query.
    /// The proof holds because the runtime never deletes an execution row and
    /// writes child rows only for a stored execution.
    fn known_or_not_found<T>(&self, exec: ExecutionId, rows: Vec<T>) -> SqliteResult<Vec<T>> {
        if rows.is_empty() && !store::execution_exists(&self.conn, exec)? {
            return Err(SqliteError::ExecutionNotFound(exec));
        }
        Ok(rows)
    }

    /// The parent of `exec`, or `None` for a top-level run (issue #2026). A pure
    /// read. It matches `ctx.info().parent_execution_id` inside the run.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// persistence error.
    pub fn parent_execution(&self, exec: ExecutionId) -> SqliteResult<Option<ExecutionId>> {
        if !store::execution_exists(&self.conn, exec)? {
            return Err(SqliteError::ExecutionNotFound(exec));
        }
        Ok(store::parent_link(&self.conn, exec)?.map(|(parent, _)| parent))
    }

    /// The stored, non-driving outcome of `exec` (a pure read; does not advance
    /// the workflow).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, or a
    /// persistence error.
    pub fn outcome(&self, exec: ExecutionId) -> SqliteResult<ExecutionOutcome> {
        match store::execution_state(&self.conn, exec)?.as_str() {
            "COMPLETED" => Ok(ExecutionOutcome::Completed(
                store::execution_output(&self.conn, exec)?.unwrap_or(Value::Null),
            )),
            "FAILED" => Ok(ExecutionOutcome::Failed(
                store::execution_error(&self.conn, exec)?.unwrap_or_default(),
            )),
            // Any OTHER terminal state — a prior SEALED to `CONTINUED_AS_NEW` by a
            // `TerminateIfRunning`/`AllowDuplicateFailedOnly` replace (issue #1068),
            // or a `CANCELLED`/`TIMED_OUT`/`TERMINATED` run — is inert and must be
            // reported TERMINALLY, consistent with the driver's
            // `erase::is_terminal_state` short-circuit (Codex #1080 P2). Mapping it
            // to `Running` (the old behavior) left a client polling a superseded run
            // seeing it active forever, even though `drive_one_cycle` already treats
            // it as terminal.
            other if autumn_harvest::erase::is_terminal_state(other) => {
                Ok(ExecutionOutcome::Terminated(other.to_string()))
            }
            _ => Ok(ExecutionOutcome::Running),
        }
    }

    /// Run the query `name` on `exec` with `args`, and return its answer
    /// (issue #2022).
    ///
    /// The query replays the stored history to the run's frontier, then calls
    /// the handler that `ctx.register_query` registered. It writes nothing:
    /// no event, no row, no staged-signal delivery, no activity body.
    ///
    /// - A running run answers with the state at its frontier: the first
    ///   command that the history does not record.
    /// - A terminal run answers with the state at its recorded end.
    ///
    /// The replay runs workflow code up to the frontier, as a drive does.
    /// Code past the last recorded event runs again in each query. That
    /// includes a `ctx.side_effect` closure the history does not record yet.
    ///
    /// The call reads the rows immediately. The returned future then replays
    /// them, so it borrows nothing from the runtime. The answer is a
    /// point-in-time read as of the call.
    ///
    /// ```no_run
    /// # async fn example(
    /// #     rt: &autumn_harvest_sqlite::SqliteRuntime,
    /// #     exec: autumn_harvest_sqlite::ExecutionId,
    /// # ) -> autumn_harvest_sqlite::SqliteResult<()> {
    /// let stage = rt.query(exec, "stage", serde_json::Value::Null).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The future resolves to an error in these cases:
    ///
    /// - [`SqliteError::ExecutionNotFound`]: the id is unknown.
    /// - [`SqliteError::UnknownWorkflow`]: the workflow is not registered.
    /// - [`SqliteError::QueryNotFound`]: the replay registers no handler
    ///   named `name`.
    /// - [`SqliteError::QueryFailed`]: the handler returns `Err`.
    /// - [`SqliteError::QueryPanicked`]: the handler panics, or the workflow
    ///   panics during the replay of a running run.
    /// - [`SqliteError::QueryTimedOut`]: the replay passes the query timeout.
    /// - [`SqliteError::QueryUnavailable`]: a terminal run cannot give a
    ///   stable answer.
    /// - [`SqliteError::NonDeterministic`]: the workflow code changed, so the
    ///   replay of a running run diverges. A drive returns the same error.
    /// - A persistence error: a row cannot be read.
    pub fn query(
        &self,
        exec: ExecutionId,
        name: &str,
        args: Value,
    ) -> impl std::future::Future<Output = SqliteResult<Value>> + Send + 'static {
        let loaded = QueryReplay::load(&self.conn, &self.workflows, self.history_policy, exec);
        let name = name.to_string();
        let timeout = self.query_timeout;
        async move { loaded?.run(name, args, timeout).await }
    }

    /// Set the time limit of a query replay. The default is
    /// [`DEFAULT_QUERY_TIMEOUT`](crate::DEFAULT_QUERY_TIMEOUT), 5 s.
    ///
    /// A future that [`query`](Self::query) already returned keeps the old
    /// limit. A limit too large to add to the current time means no limit.
    pub const fn set_query_timeout(&mut self, timeout: std::time::Duration) {
        self.query_timeout = timeout;
    }

    // ── Drivers ──────────────────────────────────────────────────────────────

    /// Drive `exec` to a terminal state or an external-input block, using the
    /// real wall clock (re-read once **per decision cycle**) to decide which
    /// timers are due. Refreshing per cycle — rather than capturing one timestamp
    /// for the whole call — anchors a timer armed after a real-time activity to
    /// its true arm instant, so a `timer(1s)` following a 2s activity waits ~1s
    /// from when it was armed instead of firing immediately (issue #1069 P2).
    ///
    /// This call also drives each `RUNNING` child or await target that the run
    /// waits on (issue #2026). When every such target blocks, this returns
    /// [`RunState::WaitingWorkflow`] naming one of them.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns
    /// [`SqliteError::Stuck`] if the run makes no progress and cannot be
    /// classified. An error from a waited-on run also returns here, unless
    /// that run is now terminal. Such an error can name another run, or no
    /// run, so it does not always concern `exec`. Returns
    /// [`SqliteError::Unsupported`] for an unsupported
    /// command, after it seals the run `FAILED` (issue #1834). Returns
    /// [`SqliteError::UnknownWorkflow`] for an unregistered workflow.
    /// Returns [`SqliteError::UnregisteredActivity`] for an unregistered activity.
    /// Returns [`SqliteError::NonDeterministic`] on replay divergence and
    /// [`SqliteError::WorkflowPanicked`] for a contained panic. Returns
    /// [`SqliteError::TaskFailed`] when a cycle deadlocks (issue #1797).
    /// Otherwise returns a persistence error.
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
        let now_fn = self.now_fn.clone();
        let cycle_now = move || now_fn().timestamp_millis();
        self.drive_until_blocked(exec, &cycle_now, &failure_now)
            .await
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
        self.drive_until_blocked(exec, &failure_now, &failure_now)
            .await
    }

    /// Drive `root` until it is terminal or blocked, and drive in the same call
    /// each run it waits on (issue #2026).
    ///
    /// A cycle that parks on a `RUNNING` child or await target returns
    /// [`RunState::WaitingWorkflow`]. That run is local work, not an external
    /// input, so it goes on a stack and is driven next. When it ends, its
    /// waiter is driven again and sees the result.
    ///
    /// A waited-on run that blocks is marked and popped. Its waiter is driven
    /// again and picks another unmarked run, so a sibling that needs no input
    /// still runs. A waiter whose every waited-on run is blocked is blocked
    /// too. Progress clears the marks, because it can unblock a run. A run on
    /// the stack is never pushed again, so a mutual wait blocks instead of
    /// spinning.
    ///
    /// Each run gets its own budget of [`MAX_ITERATIONS`] cycles. A run that
    /// spends it returns [`SqliteError::Stuck`] naming that run.
    ///
    /// An error from the root returns at once. An error from a waited-on run
    /// returns too, unless that run is now terminal. A sealed run is terminal,
    /// and its waiter records the typed failure, so the drive goes on.
    async fn drive_until_blocked(
        &mut self,
        root: ExecutionId,
        cycle_now: &(dyn Fn() -> i64 + Send + Sync),
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> SqliteResult<RunState> {
        self.drive_blocked.clear();
        let result = self.drive_stack(root, cycle_now, failure_now).await;
        self.drive_blocked.clear();
        result
    }

    /// The loop of [`drive_until_blocked`](Self::drive_until_blocked).
    async fn drive_stack(
        &mut self,
        root: ExecutionId,
        cycle_now: &(dyn Fn() -> i64 + Send + Sync),
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> SqliteResult<RunState> {
        let mut stack = vec![root];
        let mut cycles: HashMap<ExecutionId, usize> = HashMap::new();
        while let Some(&exec) = stack.last() {
            let spent = cycles.entry(exec).or_insert(0);
            *spent += 1;
            if *spent > MAX_ITERATIONS {
                return Err(SqliteError::Stuck(exec));
            }
            let is_root = stack.len() == 1;
            let result = self.drive_one_cycle(exec, cycle_now(), failure_now).await;
            match result {
                Ok(RunState::InProgress) => self.drive_blocked.clear(),
                Ok(RunState::WaitingWorkflow(target))
                    if !stack.contains(&target) && !self.drive_blocked.contains(&target) =>
                {
                    stack.push(target);
                }
                Ok(
                    state @ (RunState::Completed(_)
                    | RunState::Failed(_)
                    | RunState::Cancelled(_)
                    | RunState::Terminated(_)
                    | RunState::ContinuedAsNew(_)),
                ) => {
                    if is_root {
                        return Ok(state);
                    }
                    stack.pop();
                    self.drive_blocked.clear();
                }
                Ok(blocked) => {
                    if is_root {
                        return Ok(blocked);
                    }
                    self.drive_blocked.insert(exec);
                    stack.pop();
                }
                Err(err) => {
                    let state = store::execution_state(&self.conn, exec)?;
                    if is_root || !autumn_harvest::erase::is_terminal_state(&state) {
                        return Err(err);
                    }
                    tracing::warn!(
                        execution_id = %exec,
                        error = %err,
                        "a waited-on run ended with an error; its waiter records the failure"
                    );
                    stack.pop();
                    self.drive_blocked.clear();
                }
            }
        }
        Err(SqliteError::Stuck(root))
    }

    /// Drive every non-terminal execution one cycle, using the real wall clock
    /// (re-read per driven cycle, issue #1069 P2). Returns `true` if any execution
    /// made durable progress this pass.
    ///
    /// One execution's error (issue #1530) does NOT stop this pass. This pass
    /// still drives every other execution.
    ///
    /// [`run_until_blocked`](Self::run_until_blocked) targets ONE execution
    /// the caller already knows, plus each run it waits on, so it stays
    /// fail-fast on error. This call drives many UNRELATED executions, so one failure must not silently
    /// starve the rest.
    ///
    /// # Errors
    ///
    /// Returns the FIRST execution's error, in `ExecutionId` order, if any.
    /// Every other error in the same pass is logged (`tracing::warn!`), not
    /// dropped. See [`run_until_blocked`](Self::run_until_blocked) for the
    /// error variants a single execution can produce. A fleet pass never
    /// returns `ExecutionNotFound`, because it drives only stored executions.
    pub async fn poll_once(&mut self) -> SqliteResult<bool> {
        let mut skip = HashSet::new();
        let (progress, first_error) = self.poll_once_pass(&mut skip).await?;
        first_error.map_or(Ok(progress), Err)
    }

    /// One fleet-wide driving pass, shared by [`poll_once`](Self::poll_once)
    /// and [`run_until_idle`](Self::run_until_idle).
    ///
    /// Returns progress and the first error SEPARATELY (issue #1555).
    /// `poll_once` collapses both into its public `Result<bool>`, which
    /// loses whether the pass still made progress on an `Err`.
    /// `run_until_idle` needs that separate signal. It keeps converging the
    /// rest of the fleet past a persistently-broken execution, instead of
    /// stopping after one pass.
    ///
    /// `skip` names executions to leave alone this pass. A NEWLY-erroring
    /// execution is added to it before this call returns (issue #1555
    /// follow-up review, Codex P1).
    ///
    /// `poll_once` passes a fresh, empty set every call. This is a no-op
    /// change for it: it still drives every execution exactly once.
    /// `run_until_idle` reuses ONE set across all its internal passes. A
    /// broken execution is then driven AT MOST ONCE per external call, even
    /// across many internal passes.
    ///
    /// Re-driving it on every pass would keep striking
    /// [`contain_workflow_panic`](Self::contain_workflow_panic)'s bounded
    /// panic budget. That seals a panicking workflow `FAILED` within one
    /// call, instead of leaving each strike visible between calls.
    async fn poll_once_pass(
        &mut self,
        skip: &mut HashSet<ExecutionId>,
    ) -> SqliteResult<(bool, Option<SqliteError>)> {
        // Re-read the wall clock per driven cycle (issue #1069 P2) — see the
        // rationale on `run_until_blocked`. The `_as_of` variant keeps a fixed
        // caller `now` for deterministic simulation.
        // Activity-finalize clock seam (Codex #1069 P2, `worker.rs:328`) — re-reads
        // `now_fn` per finalize; cloned once so it does not borrow `self`.
        let now_fn = self.now_fn.clone();
        let failure_now = move || now_fn().timestamp_millis();
        let mut progress = false;
        let mut first_error = None;
        for exec in store::running_executions(&self.conn)? {
            if skip.contains(&exec) {
                continue;
            }
            let now = (self.now_fn)().timestamp_millis();
            match self.drive_one_cycle(exec, now, &failure_now).await {
                Ok(
                    RunState::WaitingSignal(_)
                    | RunState::WaitingTimer
                    | RunState::WaitingWorkflow(_),
                ) => {}
                Ok(_) => progress = true,
                Err(err) => {
                    skip.insert(exec);
                    record_fleet_error(exec, err, &mut first_error);
                }
            }
        }
        Ok((progress, first_error))
    }

    /// Like [`poll_once`](Self::poll_once) but with an injected "as-of" time. A
    /// deterministic-simulation seam — see the caveat on
    /// [`run_until_blocked_as_of`](Self::run_until_blocked_as_of) about not mixing
    /// `_as_of` and wall-clock drivers for the same execution.
    ///
    /// # Errors
    ///
    /// See [`poll_once`](Self::poll_once) — one execution's error does not stop
    /// this pass from driving the rest of the fleet (issue #1530).
    #[doc(hidden)]
    pub async fn poll_once_as_of(&mut self, now: DateTime<Utc>) -> SqliteResult<bool> {
        // Millisecond precision — see `run_until_blocked_as_of` (issue #1069 P2).
        let now = now.timestamp_millis();
        // `_as_of` simulation: `failure_now() == now` (deterministic, sleep-free).
        let failure_now = move || now;
        let mut progress = false;
        let mut first_error = None;
        for exec in store::running_executions(&self.conn)? {
            match self.drive_one_cycle(exec, now, &failure_now).await {
                Ok(
                    RunState::WaitingSignal(_)
                    | RunState::WaitingTimer
                    | RunState::WaitingWorkflow(_),
                ) => {}
                Ok(_) => progress = true,
                Err(err) => record_fleet_error(exec, err, &mut first_error),
            }
        }
        first_error.map_or_else(|| Ok(progress), Err)
    }

    /// Repeatedly drive one fleet-wide pass until the fleet is quiescent.
    /// Quiescent means no execution makes progress: every remaining run is
    /// terminal or blocked on an external input.
    ///
    /// A pass with a per-execution error still counts as progress if any
    /// OTHER execution advanced (issue #1555). This loop keeps calling
    /// passes past such an error. One [`poll_once`](Self::poll_once) pass
    /// already drives every other execution past a broken one, the same way
    /// (issue #1530).
    ///
    /// The first error seen across ALL passes is deferred. It returns once
    /// the fleet actually quiesces, not the first time any pass errors. One
    /// external call now converges the whole fleet. It no longer stalls at
    /// one decision cycle per persistently-broken execution.
    ///
    /// An execution that errors is driven AT MOST ONCE for the rest of this
    /// call (Codex P1 follow-up review to issue #1555). Without that, a
    /// long-running call could re-drive the SAME broken execution on every
    /// internal pass. For a panicking workflow, that burns through its
    /// bounded panic budget. It seals `FAILED` within one call, instead of
    /// one strike per call. A fresh external call re-attempts it.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Runaway`] if the fleet never quiesces within
    /// the [`MAX_ITERATIONS`] safety bound and no per-execution error ever
    /// occurred. This is surfaced honestly, mirroring
    /// [`run_until_blocked`](Self::run_until_blocked)'s
    /// [`SqliteError::Stuck`]. A clean `Ok(())` here would be
    /// indistinguishable from genuine quiescence. Otherwise this returns the
    /// first per-execution error, once quiescent or at the safety bound.
    pub async fn run_until_idle(&mut self) -> SqliteResult<()> {
        let mut first_error = None;
        let mut skip = HashSet::new();
        for _ in 0..MAX_ITERATIONS {
            let (progress, err) = self.poll_once_pass(&mut skip).await?;
            if let Some(err) = err {
                first_error.get_or_insert(err);
            }
            if !progress {
                return first_error.map_or(Ok(()), Err);
            }
        }
        Err(first_error.unwrap_or(SqliteError::Runaway))
    }

    /// Run one decision cycle, and seal the run `FAILED` when the cycle meets an
    /// unsupported feature (issue #1834).
    ///
    /// The cycle's own transaction has already rolled back at that point. The
    /// seal is a new transaction, so no half-written cycle survives. The error
    /// still returns to the caller, so the drive that seals the run reports why.
    /// Every other error leaves the run `RUNNING`, because a fix in the same
    /// runtime can still resume it.
    async fn drive_one_cycle(
        &mut self,
        exec: ExecutionId,
        now: i64,
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> SqliteResult<RunState> {
        let result = self.drive_one_cycle_inner(exec, now, failure_now).await;
        if let Err(SqliteError::Unsupported(message)) = &result
            && let Err(seal_error) = self.seal_unsupported(exec, message, now)
        {
            // Keep the reason the caller needs. The run stays `RUNNING`, so
            // the next drive meets the feature again and retries the seal.
            tracing::warn!(%exec, error = %seal_error, "could not seal an unsupported run FAILED");
        }
        // Issue #2021: these errors repeat on every drive until an operator
        // changes the runtime. A pending cancel must still end the run, so it
        // ends here. A panic keeps its strike budget. A persistence error
        // propagates.
        if let Err(
            error @ (SqliteError::UnregisteredActivity(_)
            | SqliteError::UnknownWorkflow(_)
            | SqliteError::Stuck(_)),
        ) = &result
            && let Some((reason, _)) = store::cancel_request(&self.conn, exec)?
        {
            tracing::warn!(%exec, ?error, "a pending cancel ends a run the drive cannot advance");
            return self.seal_cancelled(exec, &reason, None, now);
        }
        result
    }

    /// The history policy of a cycle (issue #2021). Under a pending cancel,
    /// `budget_end` is one more event cap.
    fn cancel_policy(&self, budget_end: Option<usize>) -> WorkflowHistoryPolicy {
        budget_end.map_or(self.history_policy, |end| {
            let end = u64::try_from(end).unwrap_or(u64::MAX);
            let cap = self
                .history_policy
                .event_hard_cap()
                .map_or(end, |cap| cap.min(end));
            self.history_policy.with_event_hard_cap(cap)
        })
    }

    /// Seal `exec` `FAILED` with the typed unsupported-feature reason, and
    /// remove its pending tasks, unfired timers and staged signals.
    ///
    /// The `WorkflowFailed` event carries [`UNSUPPORTED_FEATURE_ERROR_TYPE`]
    /// and `non_retryable = true`. A retry would meet the same feature again.
    /// `details.feature` is a stable token, such as `RunLocalActivity` or
    /// `ScheduleActivity.session_id`. `details.message` holds the full text.
    fn seal_unsupported(
        &mut self,
        exec: ExecutionId,
        unsupported: &str,
        now: i64,
    ) -> SqliteResult<()> {
        let feature = unsupported
            .split(" — ")
            .next()
            .unwrap_or(unsupported)
            .trim();
        let message = SqliteError::Unsupported(unsupported.to_string()).to_string();
        let event = WorkflowEvent::WorkflowFailed {
            error: message.clone(),
            error_type: Some(UNSUPPORTED_FEATURE_ERROR_TYPE.to_string()),
            details: Some(serde_json::json!({ "feature": feature, "message": unsupported })),
            non_retryable: Some(true),
        };
        self.seal_failed(exec, &event, &message, now)
    }

    /// Fail `exec` at a history cap (issue #2023).
    ///
    /// The breaching decision has already rolled back. The `WorkflowFailed`
    /// event holds the reason JSON as `error` and nothing else. The Postgres
    /// worker writes the same event, so the stored bytes match.
    fn fail_for_history_cap(
        &mut self,
        exec: ExecutionId,
        reason: &HistoryCapReason,
        now: i64,
    ) -> SqliteResult<RunState> {
        let error = reason.to_string();
        tracing::warn!(%exec, %error, "workflow history reached a hard cap; failing the run");
        let event = WorkflowEvent::workflow_failed(error.clone());
        self.seal_failed(exec, &event, &error, now)?;
        Ok(RunState::Failed(error))
    }

    /// Plan the staged signals of `exec` within the room the history caps
    /// leave after `count` events and `bytes` stored bytes (issue #2020).
    fn plan_ingest(
        &self,
        exec: ExecutionId,
        now: i64,
        count: usize,
        bytes: u64,
    ) -> SqliteResult<worker::SignalIngest> {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        let budget = worker::IngestBudget::remaining(self.history_policy, count, bytes);
        worker::SignalIngest::plan(&self.conn, exec, now, budget)
    }

    /// Append `event`, set `exec` `FAILED` with `error`, and remove its pending
    /// tasks, unfired timers and staged signals, in one transaction.
    fn seal_failed(
        &mut self,
        exec: ExecutionId,
        event: &WorkflowEvent,
        error: &str,
        now: i64,
    ) -> SqliteResult<()> {
        let tx = self.conn.transaction()?;
        // The failed cycle replayed the staged signals, and its transaction
        // rolled back. A new plan at the same `now` gives the same events, so
        // the terminal history holds the input that the cycle saw (#2020).
        let (history, history_bytes) = store::load_history_sized(&tx, exec)?;
        let budget = worker::IngestBudget::remaining(
            self.history_policy,
            u64::try_from(history.len()).unwrap_or(u64::MAX),
            history_bytes,
        );
        worker::SignalIngest::plan(&tx, exec, now, budget)?.apply(&tx, exec)?;
        store::append_event(&tx, exec, event)?;
        store::set_failed(&tx, exec, error)?;
        // The cycle's own cleanup, such as a race's loser cancellation, rolled
        // back with the cycle. This backend has no retention pass, so remove
        // the work no one can use any more, as the `TerminateIfRunning` seal does.
        delete_open_rows(&tx, exec)?;
        composition::close_execution(&tx, exec, event, now)?;
        tx.commit()?;
        self.workflow_panic_strikes.remove(&exec);
        Ok(())
    }

    /// Seal `exec` `CONTINUED_AS_NEW` and start its successor, in one
    /// transaction (issue #2023).
    ///
    /// The successor has the same `workflow_name` and `workflow_id`. Its
    /// `WorkflowStarted` carries `input`. The signals that this cycle replayed
    /// are delivered to `exec` first (issue #2020), as on Postgres. The run
    /// could have read them, so a move would deliver them twice. A row past
    /// the room the caps leave was not replayed, so it moves to the successor.
    /// Each signal thus arrives exactly once.
    ///
    /// The drain keeps its bookkeeping events, such as a side effect. It drops
    /// a dispatch, as the Postgres worker does: the old run never resumes, so
    /// no one can await the result. Pending tasks and unfired timers of `exec`
    /// are deleted for the same reason.
    // The inputs of one hand-off. A struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    fn continue_as_new(
        &mut self,
        exec: ExecutionId,
        workflow_name: &str,
        workflow_id: &str,
        ingest: &worker::SignalIngest,
        pending: &[WorkflowCommand],
        input: Value,
        now: i64,
    ) -> SqliteResult<RunState> {
        let dropped = pending.iter().filter(|cmd| is_dispatch(cmd)).count();
        if dropped > 0 {
            // Postgres logs the same drop (issue #952). A drop is never silent.
            tracing::warn!(
                %exec,
                dropped_dispatches = dropped,
                "continue-as-new dropped dispatched work; the old run never resumes"
            );
        }
        let successor = ExecutionId::new();
        let tx = self.conn.transaction()?;
        ingest.apply(&tx, exec)?;
        let history = store::load_history(&tx, exec)?;
        let drain = TerminalDrain {
            workflows: &self.workflows,
            history: &history,
            cycle: TerminalCycle::Completing,
            now,
        };
        persist_terminal_pending_commands(
            &tx,
            &drain,
            exec,
            pending.iter().filter(|cmd| !is_dispatch(cmd)),
        )?;
        store::append_event(
            &tx,
            exec,
            &WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: successor,
                input: input.clone(),
                new_workflow_type: None,
            },
        )?;
        queue::delete_pending_tasks_for_execution(&tx, exec)?;
        queue::delete_unfired_timers_for_execution(&tx, exec)?;
        store::seal_execution(&tx, exec, SEALED_STATE)?;
        insert_run(&tx, successor, workflow_name, workflow_id, input)?;
        queue::move_undelivered_signals(&tx, exec, successor)?;
        tx.commit()?;
        Ok(RunState::ContinuedAsNew(successor))
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
    #[allow(clippy::too_many_lines)] // one arm per `WorkflowOutcome` variant
    async fn drive_one_cycle_inner(
        &mut self,
        exec: ExecutionId,
        now: i64,
        // `+ Send + Sync` keeps this async fn's future `Send` (clippy
        // `future_not_send`) — the drivers' closures satisfy it.
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
    ) -> SqliteResult<RunState> {
        // Already terminal? Return the stored outcome without re-running. The
        // core `erase::is_terminal_state` covers COMPLETED/FAILED and every SEALED
        // or terminal state (issue #1068). A run replaced by the reuse-policy path
        // is sealed `CONTINUED_AS_NEW`. `cancel` and `terminate`
        // write `CANCELLED` and `TERMINATED` (issue #2021), and `TIMED_OUT` is
        // handled defensively. The fleet poll loop never reaches
        // these (`store::running_executions` filters to `state = 'RUNNING'`), but a
        // DIRECT `run_until_blocked(sealed_exec)` must short-circuit rather than
        // re-drive a superseded run's handler.
        let state = store::execution_state(&self.conn, exec)?;
        if autumn_harvest::erase::is_terminal_state(&state) {
            return match state.as_str() {
                "COMPLETED" => Ok(RunState::Completed(
                    store::execution_output(&self.conn, exec)?.unwrap_or(Value::Null),
                )),
                "FAILED" => Ok(RunState::Failed(
                    store::execution_error(&self.conn, exec)?.unwrap_or_default(),
                )),
                CANCELLED_STATE => Ok(RunState::Cancelled(
                    store::execution_error(&self.conn, exec)?.unwrap_or_default(),
                )),
                TERMINATED_STATE => Ok(RunState::Terminated(
                    store::execution_error(&self.conn, exec)?.unwrap_or_default(),
                )),
                // A continued run names its successor again (issue #2023). It
                // starts no second successor.
                SEALED_STATE => Ok(store::successor_of(&self.conn, exec)?.map_or_else(
                    || RunState::Failed(format!("execution is terminal ({state}); not resumable")),
                    RunState::ContinuedAsNew,
                )),
                // Any other terminal state (`TIMED_OUT`) is inert. Surface a
                // terminal `Failed` that names it.
                other => Ok(RunState::Failed(format!(
                    "execution is terminal ({other}); not resumable"
                ))),
            };
        }

        // A drive dropped mid-body leaves its task `RUNNING` (issue #2024).
        // Nothing else owns it now, so hand it back to the claim.
        queue::reclaim_running_for_execution(&self.conn, exec)?;
        // Plan every staged signal before replay (issue #2020). Replay reads
        // the planned events in memory. Each persisting transaction below
        // applies the plan first, so a discarded cycle consumes no signal.
        // The plan stops at the room the history caps leave.
        let (mut history, history_bytes) = store::load_history_sized(&self.conn, exec)?;
        // Issue #2021: a stored cancel request reaches the body through the
        // span meta. The history gets `WorkflowCancelled` only at the seal.
        // A body that ignores the request and keeps working could run forever,
        // so the cleanup may add only a bounded number of events.
        let cancel = store::cancel_request(&self.conn, exec)?;
        if let Some((reason, from)) = &cancel
            && history.len().saturating_sub(*from) >= CANCEL_CLEANUP_EVENT_BUDGET
        {
            return self.seal_cancelled(exec, reason, None, now);
        }
        let (cancel_reason, budget_end) = cancel.map_or((None, None), |(reason, from)| {
            (
                Some(reason),
                Some(from.saturating_add(CANCEL_CLEANUP_EVENT_BUDGET)),
            )
        });
        let ingest = self.plan_ingest(exec, now, history.len(), history_bytes)?;
        history.extend(ingest.events().cloned());
        let input = store::execution_input(&self.conn, exec)?;
        let workflow_name = store::workflow_name_of(&self.conn, exec)?;
        // Re-read the persisted business `workflow_id` on EVERY cycle (issue #698,
        // Codex #1069 P2 runtime.rs:780) so `ctx.info().workflow_id` is STABLE across
        // replays — it is threaded into the context below, never regenerated.
        let workflow_id = store::workflow_id_of(&self.conn, exec)?;
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
        // parameter (the one that populates `ctx.info().workflow_type`, issue #698).
        // The lighter `run_workflow_with_state` hard-codes it to `""`, so a workflow
        // that records/branches on the replay-safe `ctx.info().workflow_type` would
        // persist `""` here while the Postgres worker injects the real handler name.
        //
        // FIX (Codex #1069 P2, runtime.rs:780): the business `workflow_id` has NO
        // dedicated parameter on this entry point — the core reads it ONLY from
        // `span_meta.workflow_id`, so build a minimal meta carrying it (see
        // [`drive_span_meta`]).
        let parent = store::parent_link(&self.conn, exec)?.map(|(parent, _)| parent);
        let span_meta =
            drive_span_meta(&workflow_name, &workflow_id, cancel_reason.clone(), parent);
        let declarative = self.declarative_updates(&workflow_name);
        let declarative: Vec<&UpdateHandlerInfo> = declarative.iter().collect();
        let (outcome, pending, _span, _resolved_router) =
            run_workflow_with_state_history_policy_and_caps(
                exec,
                history.clone(),
                handler,
                input,
                empty_shared_state(),
                // Issue #2023: the soft threshold for `should_continue_as_new`.
                self.history_policy,
                Some(&span_meta), // FIX: the sole seam carrying business workflow_id → ctx.info()
                &[],              // declarative query handlers (none on this backend)
                &declarative,     // declarative update handlers (issue #2025)
                &workflow_name,   // FIX C: the load-bearing name → ctx.info().workflow_type
                DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
                DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
                DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
                DEFAULT_CURRENT_DETAILS_CAP_BYTES,
                // Issue #790: the durable per-execution log sink is a Postgres-only
                // table this backend does not have, so it is always disabled here.
                // `ctx.logger()` still emits to `tracing` exactly as before.
                None,
                std::collections::HashMap::new(), // context headers (none)
                None,                             // payload offload threshold (none)
                std::sync::Arc::new(NoOpMetrics),
                // Issue #620: builder-level default activity retry/timeout floor. This
                // lightweight SQLite driver has no `HarvestBuilder` behind it, so there
                // is no fleet-wide default to thread — `None`/`None` preserve today's
                // "no floor" behavior (call-site/activity-level defaults still apply).
                None,
                None,
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
            return self.contain_workflow_panic(exec, &ingest, error, now);
        }
        self.workflow_panic_strikes.remove(&exec);

        // Issue #2023: the #1804 caps. Each arm that writes checks them inside
        // its transaction, before its terminal event. Issue #2021: under a
        // pending cancel the cleanup budget is one more event cap. A
        // suspending batch and a final batch both stop at it.
        let caps = CapCheck {
            policy: self.cancel_policy(budget_end),
            start_bytes: history_bytes.saturating_add(ingest.bytes()),
            workflow_type: &workflow_name,
        };

        // Issue #2021: a pending cancel request ends the run here.
        // - A body that returns `Ok` or `Err` has done its cleanup. Its
        //   bookkeeping commands persist before the terminal event.
        // - The body sees the request from the start of its replay. A check
        //   before recorded commands therefore diverges, and a deadlock never
        //   resolves. The driver discards that cycle and ends the run, so a
        //   cancel always ends the run.
        // - A body that continues as new has returned too. The cancel wins,
        //   so the run ends without a successor.
        // - The final batch must fit the history caps and the cleanup budget.
        if let Some(reason) = &cancel_reason {
            let batch = |pending| CancelBatch {
                ingest: &ingest,
                pending,
                caps: &caps,
            };
            match &outcome {
                WorkflowOutcome::Completed { .. }
                | WorkflowOutcome::ContinuedAsNew { .. }
                | WorkflowOutcome::Failed {
                    non_deterministic_details: None,
                    ..
                } => return self.seal_cancelled(exec, reason, Some(batch(&pending)), now),
                WorkflowOutcome::Failed { .. } | WorkflowOutcome::TaskFailed { .. } => {
                    return self.seal_cancelled(exec, reason, Some(batch(&[])), now);
                }
                WorkflowOutcome::Suspended { .. } => {}
            }
        }

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                let tx = self.conn.transaction()?;
                ingest.apply(&tx, exec)?;
                // Persist terminal-cycle bookkeeping (issue #1068) BEFORE the
                // terminal event, in command order, inside the SAME transaction —
                // mirroring the Postgres `persist_terminal_outcome_commands`.
                let drain = TerminalDrain {
                    workflows: &self.workflows,
                    history: &history,
                    cycle: TerminalCycle::Completing,
                    now,
                };
                persist_terminal_pending_commands(&tx, &drain, exec, &pending)?;
                let cascade = composition::cascade_event_count(&tx, exec)?;
                if let Some(reason) = caps.reached_with(&tx, exec, cascade)? {
                    drop(tx);
                    return self.fail_for_history_cap(exec, &reason, now);
                }
                let terminal = WorkflowEvent::WorkflowCompleted {
                    output: output.clone(),
                };
                store::append_event(&tx, exec, &terminal)?;
                store::set_completed(&tx, exec, &output)?;
                composition::close_execution(&tx, exec, &terminal, now)?;
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
                ingest.apply(&tx, exec)?;
                let drain = TerminalDrain {
                    workflows: &self.workflows,
                    history: &history,
                    cycle: TerminalCycle::Failing,
                    now,
                };
                persist_terminal_pending_commands(&tx, &drain, exec, &pending)?;
                let cascade = composition::cascade_event_count(&tx, exec)?;
                if let Some(reason) = caps.reached_with(&tx, exec, cascade)? {
                    drop(tx);
                    return self.fail_for_history_cap(exec, &reason, now);
                }
                let terminal = WorkflowEvent::workflow_failed_typed(&decoded);
                store::append_event(&tx, exec, &terminal)?;
                store::set_failed(&tx, exec, &decoded.message)?;
                composition::close_execution(&tx, exec, &terminal, now)?;
                tx.commit()?;
                Ok(RunState::Failed(decoded.message))
            }
            // Issue #2023. A continuing run is exempt from the caps: its
            // successor starts a new history.
            // A child cannot continue as new, as on Postgres (issue #2026). A new
            // run would orphan the parent's wait. The child fails instead, and
            // an awaiting parent records the failure.
            WorkflowOutcome::ContinuedAsNew { .. }
                if store::parent_link(&self.conn, exec)?.is_some() =>
            {
                let event = WorkflowEvent::workflow_failed(CHILD_CONTINUE_AS_NEW_ERROR);
                self.seal_failed(exec, &event, CHILD_CONTINUE_AS_NEW_ERROR, now)?;
                Ok(RunState::Failed(CHILD_CONTINUE_AS_NEW_ERROR.to_string()))
            }
            WorkflowOutcome::ContinuedAsNew {
                input,
                new_workflow_type: None,
            } => self.continue_as_new(
                exec,
                &workflow_name,
                &workflow_id,
                &ingest,
                &pending,
                input,
                now,
            ),
            // A cross-type successor needs its own type's defaults. This backend
            // does not resolve them, so it rejects the command by name.
            WorkflowOutcome::ContinuedAsNew {
                new_workflow_type: Some(_),
                ..
            } => Err(SqliteError::Unsupported(
                "ContinueAsNew.new_workflow_type — continuing as a different workflow type \
                 is outside the sqlite subset; use continue_as_new to keep the same type"
                    .to_string(),
            )),
            // Issue #1797: discard a deadlocked cycle, as for non-determinism
            // above. The error stops the decision loop, so it cannot spin.
            WorkflowOutcome::TaskFailed { error } => Err(SqliteError::TaskFailed {
                execution_id: exec,
                details: error,
            }),
            WorkflowOutcome::Suspended { commands } => {
                let owner = worker::RunOwner {
                    workflow_id: &workflow_id,
                    workflow_type: &workflow_name,
                };
                self.drive_suspension(
                    exec,
                    &owner,
                    &ingest,
                    &history,
                    &commands,
                    &caps,
                    now,
                    failure_now,
                    cancel_reason.as_deref(),
                )
                .await
            }
        }
    }

    /// Handle a `WorkflowOutcome::Suspended` decision cycle: persist the batch,
    /// drain ready work, and classify the resulting block.
    ///
    /// `drain_ready` is gated by `worker::activity_race_in_flight` (issue #600;
    /// Codex #1069 P1): under an unsettled activity `ctx.race()` it drains only ONE
    /// branch to terminal so the workflow re-settles and cancels the losers before
    /// they run. Fan-out and ordinary activities never trip the gate, so the drain
    /// is the byte-for-byte prior drain-ALL behavior.
    ///
    /// `cancel_reason` is a pending cancel request (issue #2021). A blocked
    /// cycle with no activity task left seals the run `CANCELLED`, because the
    /// parked await cannot see the request. A pending task can still resolve
    /// the await, so the run waits for it.
    // The inputs of one decision cycle. A struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    async fn drive_suspension(
        &mut self,
        exec: ExecutionId,
        owner: &worker::RunOwner<'_>,
        ingest: &worker::SignalIngest,
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
        caps: &CapCheck<'_>,
        now: i64,
        failure_now: &(dyn Fn() -> i64 + Send + Sync),
        cancel_reason: Option<&str>,
    ) -> SqliteResult<RunState> {
        let applied = match self.apply_commands(exec, ingest, history, commands, caps, now)? {
            Applied::Committed { produced } => produced,
            Applied::CapReached(reason) => {
                // Issue #2021: a pending cancel wins over a cap that its
                // cleanup reaches. The seal adds only `WorkflowCancelled`.
                if let Some(cancel) = cancel_reason {
                    return self.seal_cancelled(exec, cancel, None, now);
                }
                return self.fail_for_history_cap(exec, &reason, now);
            }
        };
        // The batch sealed this run (issue #2026). Run none of its work. The
        // next cycle reports the terminal state.
        if store::execution_state(&self.conn, exec)? != "RUNNING" {
            return Ok(RunState::InProgress);
        }
        let settle_after_first_terminal = worker::activity_race_in_flight(history, commands);
        let drained = worker::drain_ready(
            &mut self.conn,
            exec,
            owner,
            now,
            failure_now,
            &self.activities,
            settle_after_first_terminal,
        )
        .await?;
        if applied || drained {
            return Ok(RunState::InProgress);
        }
        if let Some(reason) = cancel_reason
            && !queue::has_pending_activity(&self.conn, exec)?
        {
            return self.seal_cancelled(exec, reason, None, now);
        }
        self.classify_block(exec, commands)
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
    ///
    /// The cap check (issue #2023) runs after the appends and before the
    /// commit. A breach drops the transaction, so the decision rolls back.
    fn apply_commands(
        &mut self,
        exec: ExecutionId,
        ingest: &worker::SignalIngest,
        history: &[WorkflowEvent],
        commands: &[WorkflowCommand],
        caps: &CapCheck<'_>,
        now: i64,
    ) -> SqliteResult<Applied> {
        let tx = self.conn.transaction()?;
        // The planned signals come first. Replay read them ahead of this
        // cycle's commands (issue #2020).
        let mut produced = ingest.apply(&tx, exec)?;
        // Timer ids armed within THIS batch, so a (pathological) duplicate
        // `StartTimer` for the same id in one cycle can't double-append.
        let mut armed_this_batch: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for cmd in commands {
            match cmd {
                cmd @ WorkflowCommand::ScheduleActivity { .. } => {
                    // Freeze the resolved defaults onto the task row when the activity
                    // is already registered at schedule time (issue #1068): the four
                    // scalars (retry policy, max_attempts, start_to_close,
                    // schedule_to_close deadline) are resolved ONCE from the command
                    // override / registered spec and made immutable for the run's
                    // life. If the activity is NOT yet registered here (a late
                    // registration — `self.activities.get(name)` is `None`), the row
                    // is enqueued UNFROZEN and [`worker::drain_ready`] resolves and
                    // freezes the defaults from the now-present spec at first CLAIM.
                    // `self.conn` and `self.activities` are disjoint fields, so
                    // borrowing the registry while the cycle transaction holds
                    // `self.conn` is sound.
                    if apply_schedule_activity(&tx, &self.activities, exec, history, cmd, now)? {
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
                // Benign bookkeeping the single-writer backend has no surface for
                // yet, but which is safe to DROP (never an error): each is
                // replay-suppressed in the core, appends no `WorkflowEvent`, and
                // gates no control flow. `WaitForActivity` is a re-park of an
                // already-scheduled activity the worker pass completes;
                // `SetCurrentDetails` is an operator status breadcrumb (issue #593)
                // the core actively encourages — hard-erroring it would wedge an
                // otherwise fully in-subset workflow. `PublishProgress` (issue #791)
                // is an ephemeral live-output side channel delivered over Postgres
                // LISTEN/NOTIFY, which this single-writer backend has no equivalent
                // for — so it is a benign NO-OP here (streaming is Postgres-only).
                // `WaitForSignal` writes nothing here. The cycle-start ingest
                // already consumed every staged signal (issue #2020).
                // `classify_block` still reads it to name the wait.
                WorkflowCommand::WaitForActivity { .. }
                | WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::SetCurrentDetails { .. }
                | WorkflowCommand::PublishProgress { .. }
                // Durable streams (issue #1974) need the Postgres-only
                // `harvest_stream_chunks` table. A benign NO-OP here, as for
                // `PublishProgress`.
                | WorkflowCommand::PublishDurableProgress { .. }
                // `RecordLog` (issue #790) is the OPT-IN durable per-execution
                // log sink, backed by a Postgres-only `harvest_workflow_logs`
                // table this backend does not have. A benign NO-OP here: the
                // line still reaches `tracing` (the core emits that
                // unconditionally), only the durable copy is skipped. It
                // appends no event and gates no control flow, so dropping it
                // cannot affect replay.
                | WorkflowCommand::RecordLog { .. } => {}
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
                // A body-driven update result (issue #2025). The Postgres worker
                // appends the same event in command order. It is not progress:
                // replay skips update events, so the result cannot unblock the
                // body. A body that mints a new update id on each cycle then
                // appends one event per drive, as on Postgres, and does not spin.
                WorkflowCommand::RecordUpdateResult { update_id, result } => {
                    store::append_event(&tx, exec, &update::result_event(*update_id, result))?;
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
                    if apply_cancel_race_losers(&tx, exec, activities, children, timers, now)? {
                        produced = true;
                    }
                }
                // Composition (issue #2026). The `composition` module resolves
                // each of these in this cycle transaction.
                cmd @ (WorkflowCommand::StartChildWorkflow { .. }
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                | WorkflowCommand::SignalExternalWorkflow { .. }
                | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                | WorkflowCommand::AwaitExternalWorkflow { .. }) => {
                    if composition::apply_command(&tx, &self.workflows, exec, history, cmd, now)? {
                        produced = true;
                    }
                    // A cancel can reach this run through a parent-close cascade.
                    // A sealed run applies no more of its batch.
                    if store::execution_state(&tx, exec)? != "RUNNING" {
                        break;
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
                // Reject every OTHER unsupported command BY NAME via
                // `command_name`. Examples are external and local activities
                // and search attributes. No current variant is an
                // opaque `"Unknown"` (Codex #1069 P2, `runtime.rs:840`). Dropping
                // the uncommitted `tx` rolls back the whole batch. So an
                // unsupported command after a supported one persists NEITHER the
                // event NOR the queue or timer row of the supported command.
                other => return Err(SqliteError::Unsupported(command_name(other).to_string())),
            }
        }
        if let Some(reason) = caps.reached(&tx, exec)? {
            return Ok(Applied::CapReached(reason));
        }
        tx.commit()?;
        Ok(Applied::Committed { produced })
    }

    /// Classify why a no-progress cycle is blocked.
    ///
    /// A no-progress `Suspended` cycle is a legitimate *wait* for one of three
    /// reasons. They are a `RUNNING` child or await target (issue #2026), an
    /// undelivered signal, and a not-yet-due durable timer. Each is
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
        // A wait on another run comes first: the driver can drive that run
        // itself (issue #2026). The other waits need input from outside.
        // The driver marks a run it found blocked. Prefer a run not yet marked,
        // so each waited-on run gets driven.
        let running = composition::awaited_running_runs(&self.conn, commands)?;
        if let Some(target) = running
            .iter()
            .find(|run| !self.drive_blocked.contains(run))
            .or_else(|| running.first())
        {
            return Ok(RunState::WaitingWorkflow(*target));
        }
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
    fn contain_workflow_panic(
        &mut self,
        exec: ExecutionId,
        ingest: &worker::SignalIngest,
        error: &str,
        now: i64,
    ) -> SqliteResult<RunState> {
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
            // The sealing strike replayed the planned signals. Record them, so
            // the terminal history holds the input that caused the panic.
            ingest.apply(&tx, exec)?;
            let event = WorkflowEvent::workflow_failed_typed(&decoded);
            store::append_event(&tx, exec, &event)?;
            store::set_failed(&tx, exec, &decoded.message)?;
            // The plan can stop at the cap room. No drive reads the rest, so
            // remove it, as `seal_failed` does.
            queue::delete_undelivered_signals_for_execution(&tx, exec)?;
            composition::close_execution(&tx, exec, &event, now)?;
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
///   terminal `ActivityTimedOut { StartToClose }`). `None` = no budget (unbounded).
///   Postgres differs here: it retries a start-to-close timeout per the retry
///   policy (issue #1809, ADR 0005). This backend does not.
/// - `schedule_to_close` (the `ActivityInfo::default_schedule_to_close`, issue #378)
///   and the other three resolved defaults — FROZEN onto the row here when the
///   activity is already registered (issue #1068). It is not a command field, so it
///   is resolved from the registered spec's `default_schedule_to_close` anchored to
///   the stable schedule instant (`now`) as the absolute deadline
///   `scheduled_at + default_schedule_to_close`, and persisted alongside the frozen
///   retry policy / `max_attempts` / `start_to_close` with `defaults_frozen = 1`. If
///   the activity is NOT yet registered at schedule time (a late registration), the
///   row is enqueued UNFROZEN (command overrides only, `defaults_frozen = 0`) and
///   [`worker::drain_ready`](crate::worker::drain_ready) resolves and freezes the
///   deadline (and the other defaults) from the now-present spec at first claim.
///   Freezing makes an in-flight activity's cross-retry deadline immutable across a
///   crash/reopen or a re-registration under a changed spec, mirroring the Postgres
///   engine (terminal `ActivityTimedOut { ScheduleToClose }` on the cap).
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
    activities: &HashMap<String, ActivitySpec>,
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
        result_writer,
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
    if *result_writer {
        return Err(SqliteError::Unsupported(
            "ScheduleActivity.result_writer — the fan-out result writer (issue #1986) \
             needs a PayloadStore, which is outside the sqlite subset"
                .to_string(),
        ));
    }
    if store::history_has_activity_scheduled(history, &activity_id.to_string()) {
        return Ok(false);
    }

    // Resolve the four defaults ONCE (issue #1068). When the activity is already
    // registered, resolve the effective retry policy / `max_attempts` /
    // `start_to_close` / `schedule_to_close` deadline from the command override
    // (typed path) falling back to the registered spec (raw path / declared
    // defaults), and FREEZE them onto the row (`defaults_frozen = true`). A later
    // claim then reads them verbatim rather than re-resolving against a mutated
    // registry — so a crash/reopen or a re-registration cannot alter an in-flight
    // activity's contract. When the activity is NOT yet registered (a late
    // registration), enqueue the command overrides only (`defaults_frozen = false`);
    // the worker resolves and freezes the defaults from the now-present spec at first
    // claim, anchored to the stable `scheduled_at` (== `now` here). `now` is recorded
    // as `scheduled_at` inside `enqueue_activity` (the initial `run_at` doubles as it).
    let (max_attempts, retry_policy_json, start_to_close_ms, schedule_to_close_at, defaults_frozen) =
        if let Some(spec) = activities.get(name.as_str()) {
            // The effective retry policy: the command override (typed path) wins;
            // otherwise the registered spec's declared `default_retry_policy` (raw
            // path). Serialize the WHOLE policy so the worker honors backoff timing +
            // `non_retryable_errors`, not just the attempt cap.
            let retry_policy = retry_policy_override
                .clone()
                .or_else(|| spec.default_retry_policy.clone());
            let retry_json = retry_policy
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            // The attempt cap: the override's cap wins; otherwise the spec's derived
            // `max_attempts` (which `register_activity` derives from the same
            // `default_retry_policy`, so it agrees with `retry_json`).
            let max_attempts = retry_policy_override
                .as_ref()
                .map_or(spec.max_attempts, |p| p.max_attempts);
            // The per-attempt start-to-close budget: override (typed) wins, else the
            // spec's declared `default_start_to_close` (raw path).
            let start_to_close_ms = start_to_close_override
                .map(duration_to_millis_saturating)
                .or_else(|| spec.start_to_close.map(duration_to_millis_saturating));
            // The total (cross-retry) deadline: `scheduled_at (== now) +
            // default_schedule_to_close`. There is no per-call schedule_to_close
            // override, so the spec always supplies it.
            let deadline = spec
                .schedule_to_close
                .map(|d| now.saturating_add(duration_to_millis_saturating(d)));
            (
                Some(max_attempts),
                retry_json,
                start_to_close_ms,
                deadline,
                true,
            )
        } else {
            // Late registration: carry the command overrides only; the worker freezes
            // the rest from the now-present spec at claim (`defaults_frozen = false`).
            let max_attempts = retry_policy_override.as_ref().map(|p| p.max_attempts);
            let retry_json = retry_policy_override
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            let start_to_close_ms = start_to_close_override.map(duration_to_millis_saturating);
            (max_attempts, retry_json, start_to_close_ms, None, false)
        };

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
        defaults_frozen,
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
/// `schedule_to_close_at`/`defaults_frozen` carry the FROZEN total (cross-retry)
/// deadline and the resolved-defaults flag (issue #1068): `Some(at)` + `true` when
/// the activity was registered at schedule time (the four scalars are frozen on the
/// row), else `None` + `false` for a late registration (the worker freezes them at
/// first claim). `run_at` is recorded as the stable `scheduled_at` anchor (see
/// [`queue::enqueue_activity`]) so the frozen deadline is measured from when the
/// activity was first scheduled.
/// The sealed terminal state a reuse-policy "replace" moves a superseded prior to
/// (issue #1068), mirroring the Postgres core's `replace_execution` — sealed rows
/// are excluded from the active-run reuse lookup
/// ([`store::find_active_execution_by_key`]) so the fresh run takes the key.
const SEALED_STATE: &str = "CONTINUED_AS_NEW";

/// Cancel-and-seal ONE active prior for a `TerminateIfRunning` replace (issue
/// #1068; Codex #1080 P2), inside the caller's start transaction.
///
/// If the prior is still `RUNNING`, append a `WorkflowCancelled` event recording the
/// forced cancellation and clean up its PENDING task rows and unfired timers (so
/// no orphan can be claimed / fire against a run that is never driven again). An
/// already-terminal prior (`COMPLETED`, `FAILED` or `CANCELLED`) is sealed WITHOUT a cancellation
/// event and has no PENDING tasks or unfired timers left to clean up. Every prior
/// being sealed — RUNNING or already-terminal — also has its undelivered staged
/// signals deleted. A signal can be staged while the prior is RUNNING. It can then
/// outlive the prior even if the prior reaches COMPLETED/FAILED on its own (the
/// workflow never awaited that signal name). The row is then just as unreachable,
/// regardless of which state sealed it. This backend has no retention/GC pass, so
/// the row would otherwise survive forever. Then seal to [`SEALED_STATE`] so the prior
/// leaves the active set. Byte-identical
/// to the pre-#1080 single-prior inline path — extracted so the `TerminateIfRunning`
/// arm can loop it over EVERY active row for the key
/// ([`store::find_active_executions_by_key`]), reconciling a pre-#1068 database that
/// already holds multiple RUNNING duplicates.
fn cancel_and_seal_prior(
    conn: &Connection,
    prior_exec: ExecutionId,
    prior_state: &str,
) -> SqliteResult<()> {
    let cancelled = (prior_state == "RUNNING").then(|| WorkflowEvent::WorkflowCancelled {
        reason: "superseded by a TerminateIfRunning start (#1068)".to_string(),
    });
    if let Some(event) = &cancelled {
        store::append_event(conn, prior_exec, event)?;
        queue::delete_pending_tasks_for_execution(conn, prior_exec)?;
        queue::delete_unfired_timers_for_execution(conn, prior_exec)?;
    }
    // Unconditional (not inside the `if` above): an undelivered signal can outlive
    // a prior that reached COMPLETED/FAILED on its own, not only a cancelled one.
    queue::delete_undelivered_signals_for_execution(conn, prior_exec)?;
    store::seal_execution(conn, prior_exec, SEALED_STATE)?;
    // A RUNNING prior closes here, so settle its parent and its detached
    // children (issue #2026). A terminal prior settled them when it ended.
    if let Some(event) = &cancelled {
        composition::close_execution(conn, prior_exec, event, Utc::now().timestamp_millis())?;
    }
    Ok(())
}

/// The events of the cycle that ends a cancelled run, and the limits they
/// must fit (issue #2021).
struct CancelBatch<'a> {
    /// The cycle's signal plan.
    ingest: &'a worker::SignalIngest,
    /// The bookkeeping commands of a cycle that returned.
    pending: &'a [WorkflowCommand],
    /// The history caps, with the cleanup budget as one more event cap.
    caps: &'a CapCheck<'a>,
}

/// The most events that a cleanup can add after a cancel request (issue
/// #2021). A drive that finds more ends the run `CANCELLED` before replay.
/// One activity adds two events, so the cleanup has room for about 500.
const CANCEL_CLEANUP_EVENT_BUDGET: usize = 1_000;

/// The `error_type` of the `ActivityFailed` that a cancel seal records for an
/// unregistered activity (issue #2021).
const UNREGISTERED_ACTIVITY_ERROR_TYPE: &str = "UnregisteredActivity";

/// The `error_type` of the `ActivityFailed` that a cancel seal records for a
/// registered activity that it drops (issue #2021).
const CANCEL_SEALED_ERROR_TYPE: &str = "CancelSealed";

/// The failure text of a child that continues as new. It matches the Postgres
/// worker (issue #2026).
const CHILD_CONTINUE_AS_NEW_ERROR: &str =
    "continue_as_new is not supported in child workflows in this release";

/// The state a cooperative cancel seals (issue #2021).
const CANCELLED_STATE: &str = "CANCELLED";

/// The state `terminate` seals (issue #2021).
const TERMINATED_STATE: &str = "TERMINATED";

/// The Postgres default reason for a blank cancel reason.
const DEFAULT_CANCEL_REASON: &str = "workflow cancellation requested";

/// The Postgres default reason for a blank terminate reason.
const DEFAULT_TERMINATE_REASON: &str = "workflow termination requested";

/// `reason` with its outer whitespace trimmed, or `default` when it is blank.
fn reason_or(reason: &str, default: &str) -> String {
    let reason = reason.trim();
    if reason.is_empty() { default } else { reason }.to_string()
}

/// Append a non-retryable `ActivityFailed` for each pending task of `exec`,
/// inside the caller's seal transaction (issue #2021). The error type is
/// `UnregisteredActivity` for an unregistered activity, and `CancelSealed`
/// otherwise.
fn fail_dropped_tasks(
    conn: &Connection,
    exec: ExecutionId,
    activities: &HashMap<String, ActivitySpec>,
) -> SqliteResult<()> {
    for (activity_id, name, attempt) in queue::pending_tasks(conn, exec)? {
        let (error, error_type) = if activities.contains_key(&name) {
            (
                format!("the cancel seal dropped pending activity: {name}"),
                CANCEL_SEALED_ERROR_TYPE,
            )
        } else {
            (
                SqliteError::unregistered(&name).to_string(),
                UNREGISTERED_ACTIVITY_ERROR_TYPE,
            )
        };
        store::append_event(
            conn,
            exec,
            &WorkflowEvent::ActivityFailed {
                activity_id,
                error,
                attempt: attempt.saturating_add(1),
                error_type: error_type.to_string(),
                non_retryable: true,
                details: None,
            },
        )?;
    }
    Ok(())
}

/// Append `WorkflowCancelled { reason }` and seal `exec` in `state` with
/// `reason` as its error, inside the caller's transaction (issue #2021).
///
/// Both seals record the core `WorkflowCancelled` event, as Postgres does for
/// cancel and terminate. The event is the last one in the history, so a
/// cleanup that ran before it replays. The seal also calls
/// [`delete_open_rows`].
fn seal_with_cancel_event(
    conn: &Connection,
    exec: ExecutionId,
    state: &str,
    reason: &str,
    now: i64,
) -> SqliteResult<()> {
    let event = WorkflowEvent::WorkflowCancelled {
        reason: reason.to_string(),
    };
    store::append_event(conn, exec, &event)?;
    store::set_terminal_with_reason(conn, exec, state, reason)?;
    delete_open_rows(conn, exec)?;
    // Settle the parent and the detached children of the closed run (issue
    // #2026).
    composition::close_execution(conn, exec, &event, now)
}

/// Delete the pending tasks, unfired timers and staged signals of a sealed
/// `exec`, inside the caller's transaction. No drive uses them again, and
/// this backend has no retention pass.
fn delete_open_rows(conn: &Connection, exec: ExecutionId) -> SqliteResult<()> {
    queue::delete_pending_tasks_for_execution(conn, exec)?;
    queue::delete_unfired_timers_for_execution(conn, exec)?;
    queue::delete_undelivered_signals_for_execution(conn, exec)?;
    Ok(())
}

/// Insert a fresh execution row + its `WorkflowStarted` event inside the caller's
/// start transaction (issue #1068), returning a `created == true`
/// [`StartOutcome`]. `business_id`: `Some(id)` records the caller's verbatim
/// business `workflow_id`; `None` defaults it to the freshly minted exec-id string
/// form so `ctx.info().workflow_id` is never empty and is distinct per run (issue
/// #698). The `input` is moved into the `WorkflowStarted` event (borrowed once for
/// the row insert first), so it is serialized exactly once.
fn insert_fresh_execution(
    conn: &Connection,
    workflow_name: &str,
    business_id: Option<&str>,
    input: Value,
) -> SqliteResult<StartOutcome> {
    let exec = ExecutionId::new();
    let workflow_id = business_id.map_or_else(|| exec.to_string(), str::to_string);
    insert_run(conn, exec, workflow_name, &workflow_id, input)?;
    Ok(StartOutcome {
        exec_id: exec,
        created: true,
    })
}

/// Insert a `RUNNING` row for `exec` and its `WorkflowStarted` event. A
/// start and a continue-as-new successor share it.
fn insert_run(
    conn: &Connection,
    exec: ExecutionId,
    workflow_name: &str,
    workflow_id: &str,
    input: Value,
) -> SqliteResult<()> {
    store::insert_execution(conn, exec, workflow_name, workflow_id, &input)?;
    store::append_event(
        conn,
        exec,
        &WorkflowEvent::workflow_started(input, Utc::now()),
    )
}

/// The result of [`SqliteRuntime::apply_commands`].
enum Applied {
    /// The batch committed. `produced` is `true` when a durable row changed.
    Committed { produced: bool },
    /// The batch reached a history cap and rolled back (issue #2023).
    CapReached(HistoryCapReason),
}

/// A command that dispatches work or waits for it. A continue-as-new drain
/// drops it (issue #2023).
const fn is_dispatch(cmd: &WorkflowCommand) -> bool {
    matches!(
        cmd,
        WorkflowCommand::ScheduleActivity { .. }
            | WorkflowCommand::StartTimer { .. }
            | WorkflowCommand::WaitForSignal { .. }
    )
}

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
    defaults_frozen: bool,
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
        defaults_frozen,
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

/// Replay the body into `ctx`, so it registers its update handlers
/// (issue #2025).
///
/// The core query driver runs the replay. It writes nothing and contains a
/// workflow panic.
async fn settle_update_context(
    exec: ExecutionId,
    ctx: &WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
) -> SqliteResult<()> {
    match drive_query_replay_async(ctx, handler, input, UPDATE_TIMEOUT).await {
        // A replay that diverged rebuilds the wrong state. A drive would fail
        // the same history as non-deterministic, so the update must not run.
        QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => {
            let drift = ctx
                .take_deferred_nd_error()
                .or_else(|| ctx.take_nd_details().map(|d| format!("{d:?}")));
            drift.map_or(Ok(()), |details| {
                Err(SqliteError::NonDeterministic {
                    execution_id: exec,
                    details,
                })
            })
        }
        QueryReplayOutcome::Panicked => Err(SqliteError::WorkflowPanicked {
            execution_id: exec,
            details: "the workflow panicked while its update handlers were rebuilt".to_string(),
        }),
        QueryReplayOutcome::TimedOut => Err(SqliteError::TaskFailed {
            execution_id: exec,
            details: "the replay that rebuilds the update handlers did not settle".to_string(),
        }),
    }
}

/// How a terminal cycle ends. Only a failing cycle records an abandoned child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalCycle {
    /// The workflow returned `Ok`.
    Completing,
    /// The workflow returned `Err`.
    Failing,
}

/// The context that a terminal drain needs to start or record a child (issue
/// #2026).
struct TerminalDrain<'a> {
    /// The registered workflow handlers. A detached spawn checks its type here.
    workflows: &'a HashMap<String, WorkflowHandlerFn>,
    /// The history that the cycle replayed.
    history: &'a [WorkflowEvent],
    /// How the cycle ends.
    cycle: TerminalCycle,
    /// The clock of the cycle, in epoch milliseconds.
    now: i64,
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
fn persist_terminal_pending_commands<'c>(
    conn: &Connection,
    drain: &TerminalDrain<'_>,
    exec: ExecutionId,
    pending: impl IntoIterator<Item = &'c WorkflowCommand>,
) -> SqliteResult<()> {
    for cmd in pending {
        match cmd {
            // A detached spawn in the terminal cycle starts its child before the
            // parent's terminal event, as on Postgres (issue #2026). The close
            // then applies the child's parent-close policy.
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                input,
                parent_close_policy,
            } => {
                composition::start_child(
                    conn,
                    drain.workflows,
                    exec,
                    drain.history,
                    *child_id,
                    workflow_name,
                    input,
                    Some(*parent_close_policy),
                )?;
            }
            // A failing cycle that dropped a fresh child dispatch records it as
            // abandoned (issue #952). A completing cycle, or a re-park of a
            // started child, records nothing.
            WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                input,
                ..
            } => {
                if drain.cycle == TerminalCycle::Failing {
                    composition::record_abandoned_child(
                        conn,
                        exec,
                        drain.history,
                        *child_id,
                        workflow_name,
                        input,
                    )?;
                }
            }
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
            // A body-driven update result (issue #2025). It precedes the
            // terminal event, as on Postgres.
            WorkflowCommand::RecordUpdateResult { update_id, result } => {
                store::append_event(conn, exec, &update::result_event(*update_id, result))?;
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
                apply_cancel_race_losers(conn, exec, activities, children, timers, drain.now)?;
            }
            // Benign bookkeeping — appends no event, gates no control flow.
            // `PublishProgress` (issue #791) is an ephemeral Postgres-only
            // LISTEN/NOTIFY side channel — a benign NO-OP on this backend.
            // The cycle never ran an outbound signal, cancel or await, so it
            // records none of them (issue #2026). Postgres does the same.
            WorkflowCommand::SignalExternalWorkflow { .. }
            | WorkflowCommand::RequestCancelExternalWorkflow { .. }
            | WorkflowCommand::AwaitExternalWorkflow { .. }
            | WorkflowCommand::WaitForActivity { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            // Durable streams (issue #1974): Postgres-only, a NO-OP here.
            | WorkflowCommand::PublishDurableProgress { .. }
            // Durable workflow logs (issue #790) — Postgres-only sink; a benign
            // NO-OP here for the same reason as `PublishProgress`.
            | WorkflowCommand::RecordLog { .. } => {}
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
/// - `children`: cancel each still-`RUNNING` losing child (issue #2026). The
///   child ends `CANCELLED`, and this parent records `ChildWorkflowFailed` with
///   the text `child workflow cancelled: lost race to a sibling branch`.
fn apply_cancel_race_losers(
    conn: &Connection,
    exec: ExecutionId,
    activities: &[ActivityExecId],
    children: &[ExecutionId],
    timers: &[TimerId],
    now: i64,
) -> SqliteResult<bool> {
    let mut changed = false;
    // Cancel a losing child, as Postgres does (issue #2026). Its close then
    // writes the untyped `ChildWorkflowFailed` to this parent. A loser that
    // never started has no row, and Postgres skips it too.
    for child in children {
        if composition::state_of(conn, *child)?.as_deref() == Some("RUNNING") {
            composition::cancel_execution(conn, *child, composition::RACE_LOSER_REASON, now)?;
            changed = true;
        }
    }
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
/// the task row on the same millisecond clock as every other epoch value, and by
/// [`worker::drain_ready`](crate::worker::drain_ready) to resolve an activity's
/// `schedule_to_close` deadline at claim time (Codex #1069 P2, `runtime.rs:944`).
pub fn duration_to_millis_saturating(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Human name for a command (for error messages). EVERY current
/// [`WorkflowCommand`] variant is named explicitly, so an unsupported command is
/// always rejected BY NAME rather than as an opaque `"Unknown"` (issue #1069 P2,
/// Codex `runtime.rs:840`). The match is deliberately EXHAUSTIVE (no `_` arm): a
/// future core `WorkflowCommand` variant becomes a compile error here, forcing
/// this backend to make an explicit supported/unsupported decision for it rather
/// than silently mislabelling it `"Unknown"`.
/// Build the [`WorkflowExecuteSpanMeta`] the drive path threads into the core
/// executor each cycle (Codex #1069 P2, `runtime.rs:780`).
///
/// The `run_workflow_with_state_history_policy_and_caps` entry point reads the
/// business `workflow_id` (`ctx.info().workflow_id`, issue #698 — documented for
/// minting idempotency keys) ONLY from `span_meta.workflow_id`; there is no
/// dedicated parameter for it (unlike `workflow_name`, which populates
/// `ctx.info().workflow_type`). Passing `span_meta = None` therefore left
/// `ctx.info().workflow_id` empty for EVERY execution, collapsing all `SQLite` runs
/// onto one empty id while the Postgres worker injects the real
/// `StartWorkflowParams.workflow_id` — a silent divergence on a documented core
/// surface.
///
/// Every other field is set to the exact value the prior `None` produced
/// (`queue ""`, build/timeout/deadline `None`, `is_replay = false`).
/// Issue #2021 adds `cancel_reason`, a pending cancel request that
/// `ctx.cancellation_reason()` reports.
///
/// `parent_execution_id` is the stored parent link of a child (issue #2026).
/// It is `None` for a top-level run. The Postgres worker sets the same value,
/// so `ctx.info().parent_execution_id` agrees on both backends. The recorded
/// span fields are inert here (no `OTel` worker/subscriber on this backend).
fn drive_span_meta(
    workflow_name: &str,
    workflow_id: &str,
    cancel_reason: Option<String>,
    parent_execution_id: Option<ExecutionId>,
) -> WorkflowExecuteSpanMeta {
    WorkflowExecuteSpanMeta {
        workflow_name: workflow_name.to_string(),
        workflow_id: workflow_id.to_string(),
        shard_id: 0,
        queue_name: String::new(),
        is_replay: false,
        link_traceparent: None,
        build_id: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id,
        cancellation_reason: cancel_reason,
    }
}

/// Record one execution's fleet-pass error (issue #1530).
///
/// Log every error so none is silently dropped. Keep only the first for
/// the caller. Later executions in this pass still run.
fn record_fleet_error(exec: ExecutionId, err: SqliteError, first_error: &mut Option<SqliteError>) {
    tracing::warn!(
        execution_id = %exec,
        error = %err,
        "execution failed this decision cycle; the fleet poll continues with the rest"
    );
    first_error.get_or_insert(err);
}

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
        WorkflowCommand::AwaitExternalWorkflow { .. } => "AwaitExternalWorkflow",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
        WorkflowCommand::RecordUpdateResult { .. } => "RecordUpdateResult",
        WorkflowCommand::UpsertSearchAttributes { .. } => "UpsertSearchAttributes",
        WorkflowCommand::SetCurrentDetails { .. } => "SetCurrentDetails",
        WorkflowCommand::PublishProgress { .. } => "PublishProgress",
        WorkflowCommand::PublishDurableProgress { .. } => "PublishDurableProgress",
        WorkflowCommand::RecordLog { .. } => "RecordLog",
        WorkflowCommand::CancelRaceLosers { .. } => "CancelRaceLosers",
        WorkflowCommand::ArmTimer { .. } => "ArmTimer",
        WorkflowCommand::CancelTimer { .. } => "CancelTimer",
        WorkflowCommand::AcquireMutex { .. } => "AcquireMutex",
        WorkflowCommand::ReleaseMutex { .. } => "ReleaseMutex",
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
/// - **REJECTED** — changes lifecycle, admission, or retry semantics if ignored:
///   `execution_timeout` (#243), workflow-level `retry_policy` (#523), `concurrency`
///   (#247), `debounce` (#499), `batch` (#518), and `throttle` (#607). Also `quota`
///   (#946). A raised `max_input_bytes` (#252) lowers the effective input cap versus
///   the declared contract if dropped.
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
    if info.quota.is_some() {
        return Some((
            "quota",
            "Per-tenant resource quota enforcement needs an admission-time advisory \
             lock (`lock_quota_key`) on the shared Postgres backend, which the \
             single-writer backend has no analog for.",
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
///   `ActivityTimedOut { ScheduleToClose }`), `default_retry_policy` (the whole
///   policy — backoff + `non_retryable_errors` — via the command on the TYPED path
///   and a claim-time registry fallback on the RAW path, `runtime.rs:331`),
///   `default_start_to_close` (via the command on the TYPED path and the same
///   claim-time fallback on the RAW path), `default_queue` (recorded).
/// - **REJECTED** (reported here): each is a dispatch-admission or cross-worker
///   semantic with no single-writer analog, or a cap raiser. A cap raiser would
///   otherwise silently apply the STRICTER global cap.
///   - `default_heartbeat_timeout`: heartbeats are stored, but no scanner times
///     out a silent attempt.
///   - `default_schedule_to_start`: no queue-wait scanner.
///   - `circuit_breaker` (#369), `rate_limit_*` (#332), `max_concurrent` (#247).
///   - `requires`: capability routing.
///   - `is_local`: local-activity `RunLocalActivity` dispatch is a command-layer
///     non-goal.
///   - A raised `max_input_bytes` / `max_result_bytes` (#252).
/// - **ACCEPTED, inert**: `name`, `module`, and a lone `concurrency_key` (groups a
///   cap that is not set). Also `input_schema` and `output_schema`: only the
///   schema contract reads them (issue #1994).
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
            "ctx.heartbeat(..) stores a checkpoint, but no scanner here times out \
             a silent attempt, so a heartbeat-timeout could never be observed. Use \
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
        || info.rate_limit_key_expr.is_some()
    {
        return Some((
            "rate_limit",
            "Per-activity rate limiting (#332, per-key #699) is a dispatch-admission \
             feature backed by the shared task queue's token buckets, which the \
             single-writer backend has no analog for.",
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
        schema::migrate(&conn).unwrap();
        conn
    }

    // Issue #2026: a race can name a losing child it never started. That child
    // has no row. The teardown skips it, as Postgres does, and changes nothing.
    #[test]
    fn a_race_loser_child_with_no_row_is_skipped() {
        let conn = open();
        let parent = ExecutionId::new();
        store::insert_execution(
            &conn,
            parent,
            "wf",
            &parent.to_string(),
            &serde_json::json!(0),
        )
        .unwrap();
        let changed =
            super::apply_cancel_race_losers(&conn, parent, &[], &[ExecutionId::new()], &[], 0)
                .unwrap();
        assert!(!changed);
        assert!(store::load_history(&conn, parent).unwrap().is_empty());
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
            // A dynamic per-key rate-limit expression (issue #699) is equally
            // unsupported on the single-writer backend.
            assert_eq!(
                rejected_feature(&ActivityInfo {
                    rate_limit_key_expr: Some("input.tenant_id"),
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
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();

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
                false,
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
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();

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
                false,
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
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();

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
    use autumn_harvest::quota::QuotaPolicy;
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
        info.concurrency = Some(ConcurrencyPolicy::new("input.tenant_id", 10));
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
        info.quota = Some(QuotaPolicy::new("input.tenant_id").with_max_active_executions(100));
        rejected("quota", &info);

        let mut info = base_wf_info();
        info.max_input_bytes = Some(8 * 1024 * 1024);
        rejected("max_input_bytes", &info);
    }

    #[test]
    fn a_capless_quota_is_rejected_too() {
        // `#[workflow(quota(key = "..."))]` with no cap arg is a legal, if
        // unusual, declaration (`QuotaPolicy::has_any_cap` can be false). The
        // gate checks `info.quota.is_some()`, not the caps inside it, so an
        // author-declared policy is rejected even before any cap is attached.
        let mut info = base_wf_info();
        info.quota = Some(QuotaPolicy::new("input.tenant_id"));
        rejected("quota", &info);
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
        // Issue #802: declared activity/child dependencies are preflight-only
        // registration metadata — this backend runs no preflight and the fields
        // change no execution semantics, so they are accepted inert.
        info.declared_activities = Some(&["some_activity"]);
        info.declared_children = Some(&["some_child"]);
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

    /// Counts the staged rows of signal `name`.
    fn staged(rt: &super::SqliteRuntime, name: &str) -> i64 {
        rt.conn
            .query_row(
                "SELECT COUNT(*) FROM harvest_signals WHERE name = ?1",
                [name],
                |r| r.get(0),
            )
            .unwrap()
    }

    // Issue #1985: a promise signal must carry a settlement, as on Postgres.
    #[test]
    fn a_promise_signal_without_a_settlement_stages_no_row() {
        let mut rt = super::SqliteRuntime::open_in_memory().unwrap();
        rt.register_workflow(&base_wf_info());
        let exec = rt.start_workflow("base_wf", serde_json::json!(0)).unwrap();

        let name = "harvest.promise:approval";
        let err = rt
            .send_signal(exec, name, serde_json::json!("not a settlement"))
            .unwrap_err();
        assert!(
            matches!(err, super::SqliteError::InvalidSignal(_)),
            "got {err:?}"
        );
        assert_eq!(staged(&rt, name), 0, "no signal row staged");
    }

    // Issue #1985: a promise settles once. A second settlement stages no row,
    // as the Postgres idempotency key drops it. Other signals still stage
    // every send.
    #[test]
    fn a_second_promise_settlement_stages_no_row() {
        let mut rt = super::SqliteRuntime::open_in_memory().unwrap();
        rt.register_workflow(&base_wf_info());
        let exec = rt.start_workflow("base_wf", serde_json::json!(0)).unwrap();

        let name = "harvest.promise:approval";
        let first =
            autumn_harvest::durable_promise::PromiseSettlement::resolved(serde_json::json!(1))
                .to_value();
        let second =
            autumn_harvest::durable_promise::PromiseSettlement::rejected("late").to_value();
        rt.send_signal(exec, name, first).unwrap();
        rt.send_signal(exec, name, second).unwrap();
        assert_eq!(staged(&rt, name), 1, "only the first settlement stages");

        rt.send_signal(exec, "go", serde_json::json!(1)).unwrap();
        rt.send_signal(exec, "go", serde_json::json!(2)).unwrap();
        assert_eq!(staged(&rt, "go"), 2, "other signals stage every send");
    }

    // Issue #1985: a retried settlement after the run ends is a no-op, as on
    // Postgres. A settlement the run never had still fails on a terminal run.
    #[test]
    fn a_retried_settlement_after_the_run_ends_is_a_no_op() {
        let mut rt = super::SqliteRuntime::open_in_memory().unwrap();
        rt.register_workflow(&base_wf_info());
        let exec = rt.start_workflow("base_wf", serde_json::json!(0)).unwrap();

        let name = "harvest.promise:approval";
        let settlement =
            autumn_harvest::durable_promise::PromiseSettlement::resolved(serde_json::json!(1))
                .to_value();
        rt.send_signal(exec, name, settlement.clone()).unwrap();
        rt.conn
            .execute(
                "UPDATE harvest_signals SET delivered = 1 WHERE name = ?1",
                [name],
            )
            .unwrap();
        rt.conn
            .execute(
                "UPDATE harvest_executions SET state = 'COMPLETED' WHERE exec_id = ?1",
                [exec.to_string()],
            )
            .unwrap();

        rt.send_signal(exec, name, settlement.clone()).unwrap();
        assert_eq!(staged(&rt, name), 1, "the retry stages no row");

        let err = rt
            .send_signal(exec, "harvest.promise:other", settlement)
            .unwrap_err();
        assert!(
            matches!(err, super::SqliteError::WorkflowNotRunning { .. }),
            "got {err:?}"
        );
        let err = rt
            .send_signal(exec, "go", serde_json::json!(1))
            .unwrap_err();
        assert!(
            matches!(err, super::SqliteError::WorkflowNotRunning { .. }),
            "got {err:?}"
        );
    }
}
