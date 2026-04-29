//! Error types for the harvest engine.
//!
//! `HarvestError` is a proper `std::error::Error` (via thiserror) so it can be
//! propagated with `?` through internal engine code and wrapped in `AutumnError`
//! at the boundary where workflow/activity results leave the engine.
//!
//! Note: `AutumnError` (from autumn-web) is intentionally NOT `std::error::Error`
//! — it's an HTTP response wrapper. `HarvestError` converts to `AutumnError` via
//! the blanket `From<E: Error> for AutumnError` impl automatically.

use crate::types::ExecutionId;

/// The kind of timeout that fired.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::error::TimeoutType;
///
/// let timeout = TimeoutType::StartToClose;
/// assert_eq!(timeout.to_string(), "StartToClose");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimeoutType {
    /// Worker claimed the task but didn't finish in time.
    StartToClose,
    /// Task was enqueued but no worker claimed it in time.
    ScheduleToStart,
    /// Total time from enqueue to final completion exceeded limit.
    ScheduleToClose,
    /// Activity stopped sending heartbeats.
    Heartbeat,
}

impl std::fmt::Display for TimeoutType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
            Self::ScheduleToClose => write!(f, "ScheduleToClose"),
            Self::Heartbeat => write!(f, "Heartbeat"),
        }
    }
}

/// Errors produced by the autumn-harvest workflow engine.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::error::HarvestError;
///
/// let error = HarvestError::NotFound("workflow-123".into());
/// assert!(error.to_string().contains("workflow execution not found"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum HarvestError {
    /// An activity execution failed and exhausted its retries (if any).
    #[error("activity failed: {name} (attempt {attempt}): {source}")]
    ActivityFailed {
        /// The name of the failed activity.
        name: String,
        /// The attempt number that failed.
        attempt: u32,
        /// The underlying error source.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A workflow execution failed permanently.
    #[error("workflow failed: {name}: {reason}")]
    WorkflowFailed {
        /// The name of the failed workflow.
        name: String,
        /// The reason string describing the failure.
        reason: String,
    },

    /// The engine detected non-deterministic behavior during workflow replay.
    #[error("non-deterministic replay: {0}")]
    NonDeterministic(String),

    /// The workflow was explicitly cancelled.
    #[error("workflow cancelled: {0}")]
    Cancelled(String),

    /// A Saga compensation sequence failed while trying to rollback.
    #[error(
        "saga compensation failed after original error: {original}; compensation errors: {compensation_errors:?}"
    )]
    SagaCompensationFailed {
        /// The original error that triggered the compensation.
        original: String,
        /// The list of errors encountered during compensation steps.
        compensation_errors: Vec<String>,
    },

    /// A timeout occurred for a workflow, activity, or execution component.
    #[error("timeout: {timeout_type} for {task_name}")]
    Timeout {
        /// The specific type of timeout that occurred.
        timeout_type: TimeoutType,
        /// The name of the task or entity that timed out.
        task_name: String,
    },

    /// A payload could not be serialized or deserialized.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A database operation failed.
    #[error("database error: {0}")]
    Database(String),

    /// A task queue reached its maximum capacity.
    #[error("task queue is full (queue: {queue}, depth: {depth})")]
    QueueFull {
        /// The name of the full task queue.
        queue: String,
        /// The current depth/size of the queue.
        depth: usize,
    },

    /// The requested workflow execution could not be found.
    #[error("workflow execution not found: {0}")]
    NotFound(String),

    /// Invalid configuration provided to the engine.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A workflow execution with the same `(workflow_name, workflow_id)` already
    /// exists and the caller's reuse policy does not permit reuse.
    ///
    /// Returned by `start_or_load_workflow_execution` when the policy is
    /// `WorkflowIdReusePolicy::RejectDuplicate`.
    #[error(
        "workflow execution already exists: {existing_exec_id} (state: {existing_state})"
    )]
    AlreadyExists {
        /// The execution ID of the conflicting prior run.
        existing_exec_id: ExecutionId,
        /// The state of the conflicting prior run (e.g. `"RUNNING"`, `"COMPLETED"`).
        existing_state: String,
    },
}

/// Standard result type for internal harvest engine operations.
pub type HarvestResult<T> = Result<T, HarvestError>;

/// Wrap any displayable error into [`HarvestError::Database`].
///
/// Use with `.map_err(database_error)` to reduce boilerplate on diesel calls.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::error::{HarvestError, database_error};
///
/// let err = database_error("connection failed");
/// match err {
///     HarvestError::Database(msg) => assert_eq!(msg, "connection failed"),
///     _ => panic!("Expected Database error"),
/// }
/// ```
pub fn database_error(e: impl std::fmt::Display) -> HarvestError {
    HarvestError::Database(e.to_string())
}

#[cfg(feature = "db")]
impl From<diesel::result::Error> for HarvestError {
    fn from(value: diesel::result::Error) -> Self {
        database_error(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harvest_error_is_std_error() {
        let e: &dyn std::error::Error = &HarvestError::NonDeterministic("test".into());
        assert!(e.to_string().contains("non-deterministic"));
    }

    #[test]
    fn harvest_error_display_includes_task_name() {
        let e = HarvestError::Timeout {
            timeout_type: TimeoutType::StartToClose,
            task_name: "send_email".into(),
        };
        assert!(e.to_string().contains("send_email"));
        assert!(e.to_string().contains("StartToClose"));
    }

    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn harvest_result_ok() -> HarvestResult<()> {
        let r: HarvestResult<i32> = Ok(42);
        assert_eq!(r?, 42);
        Ok(())
    }
    #[test]
    fn harvest_error_saga_compensation_failed() {
        let e = HarvestError::SagaCompensationFailed {
            original: "network timeout".into(),
            compensation_errors: vec!["db locked".into(), "disk full".into()],
        };
        assert!(e.to_string().contains("network timeout"));
        assert!(e.to_string().contains("db locked"));
        assert!(e.to_string().contains("disk full"));
    }

    #[test]
    fn timeout_type_display_is_correct() {
        assert_eq!(TimeoutType::StartToClose.to_string(), "StartToClose");
        assert_eq!(TimeoutType::ScheduleToStart.to_string(), "ScheduleToStart");
        assert_eq!(TimeoutType::ScheduleToClose.to_string(), "ScheduleToClose");
        assert_eq!(TimeoutType::Heartbeat.to_string(), "Heartbeat");
    }

    #[test]
    fn database_error_conversion() {
        let err = database_error("connection refused");
        match err {
            HarvestError::Database(msg) => assert_eq!(msg, "connection refused"),
            _ => panic!("Expected Database error"),
        }
    }
}
