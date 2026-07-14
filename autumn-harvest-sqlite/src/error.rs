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

    /// A workflow emitted a [`WorkflowCommand`](autumn_harvest::WorkflowCommand)
    /// outside the single-writer backend's supported subset (child workflows,
    /// external signals/cancels, continue-as-new, local activities, …).
    #[error("unsupported workflow command for the sqlite backend: {0}")]
    Unsupported(String),

    /// A value stored in `SQLite` could not be parsed back into its Rust type.
    #[error("corrupt stored value: {0}")]
    Corrupt(String),

    /// A driven execution made no durable progress and could not be classified
    /// as waiting on a timer or a signal — surfaced honestly instead of looping.
    /// The most common cause is a task stranded `RUNNING` by a crash that the
    /// orphan reclaim on [`SqliteRuntime::open`](crate::SqliteRuntime::open) did
    /// not clear (e.g. a different database file).
    #[error("execution {0} made no progress and could not be classified (stuck)")]
    Stuck(ExecutionId),
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
