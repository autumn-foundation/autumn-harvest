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
    /// Total wall-clock execution time from `WorkflowStarted` to terminal state
    /// exceeded the configured `execution_timeout` (issue #243).
    WorkflowExecution,
}

impl std::fmt::Display for TimeoutType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
            Self::ScheduleToClose => write!(f, "ScheduleToClose"),
            Self::Heartbeat => write!(f, "Heartbeat"),
            Self::WorkflowExecution => write!(f, "WorkflowExecution"),
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

    /// The workflow was forcibly terminated.
    #[error("workflow terminated: {0}")]
    Terminated(String),

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

    /// Replay encountered a payload encoded with an unregistered codec id.
    #[error("unknown payload codec: {id}")]
    UnknownPayloadCodec {
        /// The codec identifier stored on the event payload.
        id: String,
    },

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
    #[error("workflow execution already exists: {existing_exec_id} (state: {existing_state})")]
    AlreadyExists {
        /// The execution ID of the conflicting prior run.
        existing_exec_id: ExecutionId,
        /// The state of the conflicting prior run (e.g. `"RUNNING"`, `"COMPLETED"`).
        existing_state: String,
    },

    /// An update request was rejected by the handler's validator before being
    /// admitted to the workflow's event history.
    ///
    /// No event is written to `harvest_events`. The caller receives the reason
    /// string and should surface it as a `409 Conflict` or `400 Bad Request`.
    #[error("update rejected: {reason}")]
    UpdateRejected {
        /// Human-readable rejection reason returned by the validator.
        reason: String,
    },

    /// No update handler is registered under the given name.
    ///
    /// Surfaces as `404 Not Found` at the management API layer.
    #[error("update handler not found: {0}")]
    UpdateHandlerNotFound(String),

    /// A search attribute key or value violated a documented constraint.
    ///
    /// Returned by [`crate::context::WorkflowContext::upsert_search_attrs`] when:
    /// - The key is empty or longer than 64 characters.
    /// - The key contains characters outside `[a-zA-Z0-9_-]`.
    /// - The key is engine-reserved (`exec_id`, `workflow_name`, `shard_id`,
    ///   `status`, `run_id`) or starts with the `_harvest` prefix.
    /// - The value is a JSON object or array (only primitives and null allowed).
    #[error("invalid search attribute: {reason}")]
    InvalidSearchAttribute {
        /// Human-readable description of the constraint that was violated.
        reason: String,
    },

    /// No query handler is registered under the given name.
    ///
    /// Surfaces as `404 Not Found` at the management API layer.
    #[error("query handler not found: {0}")]
    QueryHandlerNotFound(String),

    /// The workflow execution is not in a running state and therefore cannot
    /// answer a query (it may have completed, failed, or not yet started).
    ///
    /// Surfaces as `409 Conflict` at the management API layer.
    #[error("workflow not running: {0}")]
    WorkflowNotRunning(ExecutionId),

    /// The query handler returned an application-level error (not a panic).
    ///
    /// This is distinct from [`QueryHandlerPanicked`][Self::QueryHandlerPanicked]:
    /// the handler ran to completion and intentionally returned `Err(msg)`.
    /// Surfaces as `400 Bad Request` at the management API layer.
    #[error("query handler error: {0}")]
    QueryHandlerFailed(String),

    /// The query handler panicked during execution.
    ///
    /// The panic message is captured via `std::panic::catch_unwind` and
    /// surfaced to the caller as `503 Service Unavailable`.
    #[error("query handler panicked: {0}")]
    QueryHandlerPanicked(String),

    /// The query execution exceeded the configured per-query timeout.
    ///
    /// The handler is terminated and this error is returned to the caller.
    /// Surfaces as `408 Request Timeout` or `504 Gateway Timeout` depending
    /// on whether the client or the engine imposed the limit.
    #[error("query timed out: {query_name} (timeout: {timeout_ms}ms)")]
    QueryTimedOut {
        /// Name of the query handler that timed out.
        query_name: String,
        /// Configured timeout in milliseconds.
        timeout_ms: u64,
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
        assert_eq!(TimeoutType::WorkflowExecution.to_string(), "WorkflowExecution");
    }

    #[test]
    fn timeout_type_workflow_execution_round_trips_serde() {
        let t = TimeoutType::WorkflowExecution;
        let json = serde_json::to_string(&t).unwrap();
        let back: TimeoutType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, TimeoutType::WorkflowExecution);
    }

    #[test]
    fn harvest_error_timeout_workflow_execution_display() {
        let e = HarvestError::Timeout {
            timeout_type: TimeoutType::WorkflowExecution,
            task_name: "billing_reconciliation".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("billing_reconciliation"));
        assert!(msg.contains("WorkflowExecution"));
    }

    #[test]
    fn database_error_conversion() {
        let err = database_error("connection refused");
        match err {
            HarvestError::Database(msg) => assert_eq!(msg, "connection refused"),
            _ => panic!("Expected Database error"),
        }
    }

    #[test]
    fn harvest_error_activity_failed_display() {
        let e = HarvestError::ActivityFailed {
            name: "test_activity".into(),
            attempt: 3,
            source: Box::new(std::io::Error::other("io error")),
        };
        let msg = e.to_string();
        assert!(msg.contains("test_activity"));
        assert!(msg.contains("attempt 3"));
        assert!(msg.contains("io error"));
    }

    #[test]
    fn harvest_error_workflow_failed_display() {
        let e = HarvestError::WorkflowFailed {
            name: "test_workflow".into(),
            reason: "logic error".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("test_workflow"));
        assert!(msg.contains("logic error"));
    }

    #[test]
    fn harvest_error_cancelled_display() {
        let e = HarvestError::Cancelled("user requested".into());
        let msg = e.to_string();
        assert!(msg.contains("user requested"));
    }

    #[test]
    fn harvest_error_serialization_display() {
        let serde_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let e = HarvestError::Serialization(serde_err);
        let msg = e.to_string();
        assert!(msg.contains("serialization error"));
    }

    #[test]
    fn harvest_error_unknown_payload_codec_display() {
        let e = HarvestError::UnknownPayloadCodec {
            id: "custom_codec".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("custom_codec"));
    }

    #[test]
    fn harvest_error_queue_full_display() {
        let e = HarvestError::QueueFull {
            queue: "fast_queue".into(),
            depth: 1000,
        };
        let msg = e.to_string();
        assert!(msg.contains("fast_queue"));
        assert!(msg.contains("1000"));
    }

    #[test]
    fn harvest_error_not_found_display() {
        let e = HarvestError::NotFound("some_id".into());
        let msg = e.to_string();
        assert!(msg.contains("some_id"));
    }

    #[test]
    fn harvest_error_config_display() {
        let e = HarvestError::Config("bad value".into());
        let msg = e.to_string();
        assert!(msg.contains("bad value"));
    }

    #[test]
    fn harvest_error_already_exists_display() {
        use crate::types::{ExecutionId, ShardId};
        let exec_id = ExecutionId::new_for_shard(ShardId::new(1));
        let e = HarvestError::AlreadyExists {
            existing_exec_id: exec_id,
            existing_state: "RUNNING".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains(&exec_id.to_string()));
        assert!(msg.contains("RUNNING"));
    }

    #[test]
    fn harvest_error_update_rejected_display() {
        let e = HarvestError::UpdateRejected {
            reason: "validation failed".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("validation failed"));
    }

    #[test]
    fn harvest_error_update_handler_not_found_display() {
        let e = HarvestError::UpdateHandlerNotFound("handler1".into());
        let msg = e.to_string();
        assert!(msg.contains("handler1"));
    }

    #[test]
    fn harvest_error_invalid_search_attribute_display() {
        let e = HarvestError::InvalidSearchAttribute {
            reason: "key too long".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("key too long"));
    }
}
