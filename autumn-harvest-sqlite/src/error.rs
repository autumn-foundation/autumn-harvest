//! Typed error surface for the `SQLite` backend.

use autumn_harvest::ExecutionId;

/// Errors surfaced by [`SqliteRuntime`](crate::SqliteRuntime) and its
/// persistence layer.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// A `rusqlite` / `SQLite`-level error.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// A JSON (de)serialization error while reading or writing a payload.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A workflow name with no registered handler.
    #[error("unknown workflow: {0}")]
    UnknownWorkflow(String),

    /// An activity name a workflow scheduled that has no registered body.
    #[error("unregistered activity: {0}")]
    UnregisteredActivity(String),

    /// A referenced execution id does not exist in this database.
    #[error("execution not found: {0}")]
    ExecutionNotFound(ExecutionId),

    /// A signal was sent to an execution that is not accepting signals — it has
    /// reached a terminal state (`COMPLETED`/`FAILED`/…). Mirrors the Postgres
    /// engine's rejection of a signal to a terminal target
    /// ([`send_signal`](crate::SqliteRuntime::send_signal)): staging it would
    /// silently drop the wakeup, since no live `wait_for_signal` will ever consume
    /// it. An unknown id is [`ExecutionNotFound`](Self::ExecutionNotFound); this
    /// variant is only for a *present but sealed* run.
    #[error(
        "workflow execution {execution_id} is not running (state: {state}); cannot accept a signal"
    )]
    WorkflowNotRunning {
        /// The terminal execution the signal targeted.
        execution_id: ExecutionId,
        /// Its current (terminal) state — `COMPLETED`, `FAILED`, etc.
        state: String,
    },

    /// A workflow emitted a [`WorkflowCommand`](autumn_harvest::WorkflowCommand)
    /// outside the single-writer backend's supported subset (child workflows,
    /// external signals/cancels, continue-as-new, local activities, …).
    #[error("unsupported workflow command for the sqlite backend: {0}")]
    Unsupported(String),

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

    /// A driven execution made no durable progress and could not be classified
    /// as waiting on a timer or a signal — surfaced honestly instead of looping.
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
