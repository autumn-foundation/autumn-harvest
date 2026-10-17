//! Typed error surface for the `SQLite` backend.

use autumn_harvest::ExecutionId;

/// Errors surfaced by [`SqliteRuntime`](crate::SqliteRuntime) and its
/// persistence layer.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// A `rusqlite` / `SQLite`-level error.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Another runtime holds the single-writer lock on this database file
    /// (issue #1834).
    ///
    /// The holder is another process, or another runtime in this process. The
    /// open fails before it changes the file. The lock dies with its holder,
    /// so retry after that runtime drops or its process exits.
    #[error(
        "database is open in another process or runtime: the single-writer lock {} is held",
        path.display()
    )]
    DatabaseLocked {
        /// The lock file next to the database.
        path: std::path::PathBuf,
    },

    /// A file-system error on a path this backend manages, such as the
    /// single-writer lock file (issue #1834).
    #[error("i/o error on {}: {source}", path.display())]
    Io {
        /// The path the operation used.
        path: std::path::PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },

    /// A JSON (de)serialization error while reading or writing a payload.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A workflow name with no registered handler.
    ///
    /// A drive also returns it when a workflow starts a child whose name has
    /// no handler (issue #2026). The cycle rolls back, and the parent stays
    /// `RUNNING`.
    #[error("unknown workflow: {0}")]
    UnknownWorkflow(String),

    /// An activity name a workflow scheduled that has no registered body.
    #[error("unregistered activity: {0}")]
    UnregisteredActivity(String),

    /// A referenced execution id does not exist in this database.
    ///
    /// Every public accessor that takes an `ExecutionId` returns this for an
    /// unknown id (issue #1735). A list read never returns an empty list for
    /// an unknown id.
    #[error("execution not found: {0}")]
    ExecutionNotFound(ExecutionId),

    /// A signal, update, cancel or terminate targeted an execution in a
    /// terminal state (`COMPLETED`, `FAILED`, `CANCELLED`, `TERMINATED` or
    /// `CONTINUED_AS_NEW`).
    ///
    /// [`send_signal`](crate::SqliteRuntime::send_signal) mirrors the Postgres
    /// rejection of a signal to a terminal target. No drive consumes a row
    /// staged for a sealed run, so the wakeup would be lost silently.
    /// [`send_signal_by_workflow_id`](crate::SqliteRuntime::send_signal_by_workflow_id)
    /// returns it when the newest run of the key is terminal (issue #2028). `update`
    /// also returns it (issue #2025), and so do
    /// [`cancel`](crate::SqliteRuntime::cancel) and
    /// [`terminate`](crate::SqliteRuntime::terminate) (issue #2021). An unknown
    /// id is [`ExecutionNotFound`](Self::ExecutionNotFound). This variant is
    /// only for a stored, terminal run.
    #[error(
        "workflow execution {execution_id} is not running (state: {state}); cannot accept the request"
    )]
    WorkflowNotRunning {
        /// The terminal execution the request targeted.
        execution_id: ExecutionId,
        /// Its current terminal state, for example `COMPLETED` or `CANCELLED`.
        state: String,
    },

    /// The workflow has no update handler with this name (issue #2025).
    ///
    /// The call writes no event.
    #[error("execution {execution_id} has no update handler named {name:?}")]
    UpdateHandlerNotFound {
        /// The execution the update targeted.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
    },

    /// The input fails the arg schema of a declarative `#[update]` handler
    /// (issue #2025).
    ///
    /// It keeps each violation, as the core
    /// [`HarvestError::InputValidationFailed`](autumn_harvest::HarvestError::InputValidationFailed)
    /// does. The call writes no event.
    #[error(
        "update {name:?} to execution {execution_id} has an invalid input: {}",
        violations.iter().map(|v| v.message.as_str()).collect::<Vec<_>>().join("; ")
    )]
    UpdateInputInvalid {
        /// The execution the update targeted.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
        /// Each way the input fails the schema.
        violations: Vec<autumn_harvest::info::SchemaViolation>,
    },

    /// The validator rejects the update, or panics (issue #2025).
    ///
    /// The call writes no event.
    #[error("update {name:?} to execution {execution_id} was rejected: {reason}")]
    UpdateRejected {
        /// The execution the update targeted.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
        /// Why the update was rejected.
        reason: String,
    },

    /// The update handler returns an error or panics (issue #2025).
    ///
    /// The runtime admitted the update. History holds `UpdateAdmitted` and
    /// a `WorkflowEvent::UpdateFailed` with this error.
    #[error("update {name:?} to execution {execution_id} failed: {error}")]
    UpdateFailed {
        /// The execution the update targeted.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
        /// The handler error.
        error: String,
    },

    /// The update would reach a history cap, so the run fails at the cap
    /// (issues #2025 and #2023).
    ///
    /// The call writes no update event. It seals the run `FAILED` with the
    /// cap reason, as a drive that reaches the cap does.
    #[error("update {name:?} to execution {execution_id} reached a history cap: {reason}")]
    UpdateHistoryCapReached {
        /// The execution the update targeted. It is now `FAILED`.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
        /// The cap that the update would reach.
        reason: crate::HistoryCapReason,
    },

    /// The update handler did not finish within its time limit (issue #2025).
    ///
    /// The limit is 5 seconds. A handler that awaits a durable command, such
    /// as an activity or a timer, never finishes on this backend. The call
    /// writes no event.
    #[error("update {name:?} to execution {execution_id} did not finish within its time limit")]
    UpdateTimedOut {
        /// The execution the update targeted.
        execution_id: ExecutionId,
        /// The update name.
        name: String,
    },

    /// A decision cycle failed the workflow **task**, not the run
    /// (issue #1797). The handler waited too long on a future that is not a
    /// Harvest future. Nothing was persisted and the run stays `RUNNING`, so
    /// a later drive retries it.
    ///
    /// `update` also returns this variant when the replay that rebuilds the
    /// update handlers does not settle (issue #2025). Retry the update.
    #[error("workflow task for execution {execution_id} failed and can be retried: {details}")]
    TaskFailed {
        /// The execution whose task failed.
        execution_id: ExecutionId,
        /// Why the task failed.
        details: String,
    },

    /// A workflow emitted a [`WorkflowCommand`](autumn_harvest::WorkflowCommand)
    /// outside the single-writer backend's supported subset (continue-as-new,
    /// local activities, updates, …).
    ///
    /// The run is terminal (issue #1834). The drive that returns this error
    /// first rolls back the cycle. It then seals the run `FAILED` with a typed
    /// `WorkflowFailed` event, whose `error_type` is
    /// [`UNSUPPORTED_FEATURE_ERROR_TYPE`](crate::UNSUPPORTED_FEATURE_ERROR_TYPE).
    /// A later drive returns [`RunState::Failed`](crate::RunState::Failed).
    #[error("unsupported workflow command for the sqlite backend: {0}")]
    Unsupported(String),

    /// A caller-supplied payload exceeded its configured byte cap at an ingress
    /// boundary (issue #252). Mirrors the Postgres path's
    /// [`HarvestError::PayloadTooLarge`](autumn_harvest::HarvestError::PayloadTooLarge)
    /// rejection: the oversized payload is refused BEFORE it is persisted, so an
    /// unbounded workflow input or signal payload can never enter `harvest_events`
    /// (which would bloat history and make this backend accept a payload other
    /// backends reject). `kind` names the boundary (`"workflow_input"` /
    /// `"signal_payload"`); `observed_bytes`/`cap_bytes` mirror the core error's
    /// fields. An oversized activity *result* is NOT surfaced through this variant
    /// — it is normalized into a non-retryable `PayloadTooLarge` `ActivityFailed`
    /// event (the workflow observes the failure), exactly as the Postgres worker
    /// does, so the run continues rather than the caller erroring.
    #[error(
        "payload too large: {kind} exceeded cap of {cap_bytes} bytes \
         (observed {observed_bytes} bytes)"
    )]
    PayloadTooLarge {
        /// The ingress boundary that was violated (`"workflow_input"` /
        /// `"signal_payload"`).
        kind: &'static str,
        /// Actual serialized byte length of the rejected payload.
        observed_bytes: u64,
        /// The configured cap (bytes) at that boundary.
        cap_bytes: u64,
    },

    /// A caller-supplied payload nests deeper than
    /// [`MAX_PAYLOAD_DEPTH`](crate::MAX_PAYLOAD_DEPTH) at an ingress boundary
    /// (issue #2157).
    ///
    /// `serde_json` writes any depth but refuses to read past 128 levels. The
    /// payload is refused before it is stored, so a later drive never fails on
    /// a value it cannot read back. `kind` names the boundary
    /// (`"workflow_input"`, `"memo"`, `"signal_payload"` or `"update_input"`).
    #[error("payload too deep: {kind} nests more than {cap_depth} levels")]
    PayloadTooDeep {
        /// The ingress boundary that was violated.
        kind: &'static str,
        /// The deepest nesting that ingress accepts.
        cap_depth: usize,
    },

    /// A start carrying the [`RejectDuplicate`](autumn_harvest::WorkflowIdReusePolicy::RejectDuplicate)
    /// reuse policy collided with an existing, non-sealed execution for the same
    /// `(workflow_name, workflow_id)` key (issue #1068). Mirrors the Postgres core
    /// start path's [`HarvestError::AlreadyExists`](autumn_harvest::HarvestError::AlreadyExists):
    /// the second request is refused rather than attaching a signal / silently
    /// returning the prior, so the caller can decide what to do. A prior in a
    /// SEALED state (`CONTINUED_AS_NEW`/`TERMINATED`) is invisible to the reuse
    /// check and never triggers this — matching core's uniqueness-index-excludes-
    /// terminal nuance.
    #[error(
        "workflow execution already exists for this (workflow_name, workflow_id) key: \
         {existing_exec_id} (state: {existing_state})"
    )]
    AlreadyExists {
        /// The existing (non-sealed) execution the start collided with.
        existing_exec_id: ExecutionId,
        /// Its current state — `RUNNING`, `COMPLETED`, or `FAILED`.
        existing_state: String,
    },

    /// No run exists for this `(workflow_name, workflow_id)` key (issue
    /// #2028). [`send_signal_by_workflow_id`](crate::SqliteRuntime::send_signal_by_workflow_id)
    /// returns it.
    #[error("no workflow run for ({workflow_name}, {workflow_id})")]
    WorkflowIdNotFound {
        /// The workflow type of the request.
        workflow_name: String,
        /// The business id of the request.
        workflow_id: String,
    },

    /// A call that addresses a run by its business id has a blank id (issue
    /// #2028). A blank id has no reuse key, so it matches no run.
    #[error("the workflow_id must not be blank")]
    BlankWorkflowId,

    /// A signal broke a rule of its name. A `harvest.promise:` signal must
    /// carry a [`PromiseSettlement`](autumn_harvest::durable_promise::PromiseSettlement)
    /// (issue #1985).
    #[error("invalid signal: {0}")]
    InvalidSignal(String),

    /// A search attribute at start, or in a list filter, is not a JSON
    /// primitive (issue #2027).
    #[error("invalid search attribute: {0}")]
    InvalidSearchAttribute(String),

    /// A list filter has more terms than
    /// [`MAX_FILTER_TERMS`](crate::MAX_FILTER_TERMS) allows (issue #2027).
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    /// A schedule has an invalid cron expression, timezone or interval
    /// (issue #2031).
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),

    /// No schedule exists for this workflow name (issue #2031).
    #[error("schedule not found: {0}")]
    ScheduleNotFound(String),

    /// A value stored in `SQLite` could not be parsed back into its Rust type.
    #[error("corrupt stored value: {0}")]
    Corrupt(String),

    /// The engine detected a replay **non-determinism** divergence: the current
    /// workflow code produced a command stream that disagrees with the recorded
    /// history (workflow-code drift across a deploy). This is **non-terminal**
    /// (mirroring the Postgres engine's issue-#603 semantics): the divergent
    /// decision cycle is **discarded** — nothing is appended to history, no
    /// bookkeeping command is persisted, and the execution stays `RUNNING` /
    /// resumable. A fixed or rolled-back workflow build can then re-drive the
    /// **unchanged** history to completion; the run is never sealed `FAILED` and
    /// its history is never poisoned by the bad build.
    ///
    /// This backend surfaces the divergence to the caller instead of sealing it,
    /// so the operator fixes/rolls back the code and re-drives. The full
    /// automatic block-with-backoff of the Postgres engine (issue #603) is a
    /// documented follow-up; a discard-and-surface is the minimal correct
    /// behavior for the single-writer runtime.
    ///
    /// `update` also returns this variant when the replay that rebuilds the
    /// update handlers diverges (issue #2025). The call writes no event.
    #[error(
        "execution {execution_id} hit a replay non-determinism divergence \
         (discarded; resumable after a workflow code fix/rollback): {details}"
    )]
    NonDeterministic {
        /// The diverging execution (still `RUNNING`, resumable).
        execution_id: ExecutionId,
        /// The engine's human-readable divergence description (expected vs. actual).
        details: String,
    },

    /// A `#[workflow]` handler **panicked** (rather than returning `Err`) and the
    /// engine caught it at the dispatch boundary (issue #782). Like a replay
    /// non-determinism divergence, this is **non-terminal and resumable**: the
    /// panicked decision cycle is **discarded** (no event appended, no bookkeeping
    /// command persisted), the execution stays `RUNNING`, and the divergence is
    /// surfaced so the drive loop stops instead of spinning. Under the panic budget
    /// a fixed/rolled-back build can re-drive the **unchanged** history to
    /// completion; only after the budget is exhausted is the run sealed `FAILED`
    /// with the typed `HandlerPanic` error (returned as a normal
    /// [`RunState::Failed`](crate::RunState::Failed), not this error).
    ///
    /// A genuine author `Err(...)` carries no panic flag and still fails terminally
    /// on the first strike, unchanged.
    ///
    /// `update` also returns this variant when the replay that rebuilds the
    /// update handlers panics (issue #2025). That panic adds no strike.
    #[error(
        "execution {execution_id} handler panicked \
         (contained non-terminally; resumable under the panic budget): {details}"
    )]
    WorkflowPanicked {
        /// The panicking execution (still `RUNNING`, resumable).
        execution_id: ExecutionId,
        /// The engine's typed `HandlerPanic` error payload / message.
        details: String,
    },

    /// The run has no query handler with this name (issue #2022).
    ///
    /// The replay registers every query that the workflow reaches before its
    /// frontier. The replay does not know a query that the workflow registers
    /// past the frontier.
    #[error("execution {execution_id} has no query handler named {name:?}")]
    QueryNotFound {
        /// The queried execution.
        execution_id: ExecutionId,
        /// The query name.
        name: String,
    },

    /// The query handler returned an `Err` (issue #2022).
    #[error("query {name:?} on execution {execution_id} failed: {details}")]
    QueryFailed {
        /// The queried execution.
        execution_id: ExecutionId,
        /// The query name.
        name: String,
        /// The handler's error.
        details: String,
    },

    /// The query handler panicked, or the workflow panicked during the replay
    /// of a running run (issue #2022). The panic does not reach the caller.
    /// The run does not change.
    #[error("query {name:?} on execution {execution_id} panicked: {details}")]
    QueryPanicked {
        /// The queried execution.
        execution_id: ExecutionId,
        /// The query name.
        name: String,
        /// The panic message of a query handler. A workflow panic during the
        /// replay gives a fixed text, and the core logs the message.
        details: String,
    },

    /// The query replay did not stop before the query timeout (issue #2022).
    ///
    /// The usual cause is a workflow that spins on a non-Harvest future. Set
    /// the timeout with
    /// [`set_query_timeout`](crate::SqliteRuntime::set_query_timeout). The
    /// timeout holds only while the replay yields. A workflow that loops or
    /// blocks without an `.await` is not bounded.
    #[error("query {name:?} on execution {execution_id} timed out after {timeout_ms} ms")]
    QueryTimedOut {
        /// The queried execution.
        execution_id: ExecutionId,
        /// The query name.
        name: String,
        /// The timeout that elapsed.
        timeout_ms: u64,
    },

    /// A terminal run cannot give a stable answer (issue #2022).
    ///
    /// The query serves no answer rather than a wrong one. There are three
    /// causes:
    ///
    /// - The workflow code changed, so the replay diverges from the history.
    /// - The workflow panics during the replay. The panic recurs on every
    ///   retry, as on the Postgres HTTP path.
    /// - A seal for an unsupported feature or a history cap rolled back the
    ///   last cycle. The replay then mints side-effect values that the
    ///   history does not record.
    #[error("query {name:?} on terminal execution {execution_id} is unavailable: {details}")]
    QueryUnavailable {
        /// The queried execution.
        execution_id: ExecutionId,
        /// The query name.
        name: String,
        /// Why the replay cannot serve.
        details: String,
    },

    /// A driven execution made no durable progress. It waits on no timer, no
    /// signal and no other run, so this error surfaces it instead of a loop.
    /// It also names a run that spent its cycle budget in one
    /// `run_until_blocked` call.
    /// The most common cause is a task stranded `RUNNING` by a crash that the
    /// orphan reclaim on [`SqliteRuntime::open`](crate::SqliteRuntime::open) did
    /// not clear (e.g. a different database file).
    #[error("execution {0} made no progress and could not be classified (stuck)")]
    Stuck(ExecutionId),

    /// The fleet-wide driver
    /// ([`run_until_idle`](crate::SqliteRuntime::run_until_idle)) never quiesced
    /// within its iteration safety bound — surfaced honestly rather than swallowed
    /// as a clean `Ok(())` a caller could not distinguish from genuine
    /// quiescence. The per-execution analog is [`Stuck`](Self::Stuck).
    #[error("fleet driver made no progress within the iteration safety bound (runaway)")]
    Runaway,
}

impl SqliteError {
    // These are internal constructors. `SqliteError` is re-exported publicly, so
    // `pub` here would leak them into the crate's public API — `pub(crate)` is
    // deliberate and NOT redundant despite the private enclosing module.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn corrupt(field: &str) -> Self {
        Self::Corrupt(field.to_string())
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn unregistered(name: &str) -> Self {
        Self::UnregisteredActivity(name.to_string())
    }
}

/// Convenience alias for a `Result` over [`SqliteError`].
pub type SqliteResult<T> = Result<T, SqliteError>;
