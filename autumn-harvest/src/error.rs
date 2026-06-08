//! Error types for the harvest engine.
//!
//! `HarvestError` is a proper `std::error::Error` (via thiserror) so it can be
//! propagated with `?` through internal engine code and wrapped in `AutumnError`
//! at the boundary where workflow/activity results leave the engine.
//!
//! Note: `AutumnError` (from autumn-web) is intentionally NOT `std::error::Error`
//! — it's an HTTP response wrapper. `HarvestError` converts to `AutumnError` via
//! the blanket `From<E: Error> for AutumnError` impl automatically.

use uuid::Uuid;

use crate::types::{ExecutionId, ExternalSignalId};

// ---------------------------------------------------------------------------
// PayloadKind
// ---------------------------------------------------------------------------

/// The category of payload that exceeded a configured size cap (issue #252).
///
/// Used in [`HarvestError::PayloadTooLarge`] to identify which boundary
/// triggered the rejection so operators can attribute the cost to a specific
/// workflow type or activity.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PayloadKind {
    /// Activity input serialized at schedule time.
    ActivityInput,
    /// Activity result returned at completion time.
    ActivityResult,
    /// Signal payload delivered via the management API.
    SignalPayload,
    /// Workflow start input provided via the management API.
    WorkflowInput,
    /// Child-workflow input at child-start time.
    ChildWorkflowInput,
    /// Child-workflow result at child-complete time.
    ChildWorkflowResult,
    /// Value passed to `WorkflowContext::side_effect` at recording time.
    SideEffectValue,
}

impl std::fmt::Display for PayloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActivityInput => write!(f, "ActivityInput"),
            Self::ActivityResult => write!(f, "ActivityResult"),
            Self::SignalPayload => write!(f, "SignalPayload"),
            Self::WorkflowInput => write!(f, "WorkflowInput"),
            Self::ChildWorkflowInput => write!(f, "ChildWorkflowInput"),
            Self::ChildWorkflowResult => write!(f, "ChildWorkflowResult"),
            Self::SideEffectValue => write!(f, "SideEffectValue"),
        }
    }
}

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
        /// The stable, low-cardinality error-type class carried by the failure
        /// (issue #227 / #369), e.g. `"CircuitOpen"`, `"InvalidInput"`, or the
        /// `"Error"` fallback for legacy `Err(String)` failures. Lets workflow
        /// code branch on the failure class without parsing the human message.
        error_type: String,
        /// Optional structured details carried by a typed `ActivityFailure`
        /// (e.g. `retry_after_secs` / `forced` for a `CircuitOpen` failure).
        /// `None` for legacy or detail-less failures.
        details: Option<serde_json::Value>,
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

    /// An in-flight activity was notified that its owning workflow has been
    /// cancelled.
    ///
    /// Returned by [`ActivityContext::heartbeat`](crate::context::ActivityContext::heartbeat)
    /// and [`ActivityContext::check_cancellation`](crate::context::ActivityContext::check_cancellation)
    /// when the task queue row has been marked cancelled or the worker's
    /// cancellation token has been triggered.  Activities should treat this as a
    /// signal to stop work and return early; the workflow-level cancellation
    /// event is recorded separately.
    #[error("activity cancelled: {0}")]
    ActivityCancelled(String),

    /// The workflow was forcibly terminated.
    #[error("workflow terminated: {0}")]
    Terminated(String),

    /// An operation was rejected because the workflow execution is currently
    /// paused (issue #383).
    ///
    /// Returned when an update is submitted against a paused execution: updates
    /// may admit-and-mutate workflow state, so they are rejected rather than
    /// silently queued. Surfaces as `409 Conflict` at the management API layer.
    #[error("workflow paused: {0}")]
    WorkflowPaused(ExecutionId),

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

    /// A payload exceeded the configured size cap at a write boundary.
    ///
    /// Returned synchronously at the moment the oversized payload is detected
    /// (activity input at schedule time, activity result at completion time,
    /// signal payload at send time, etc.). No event is written to
    /// `harvest_events` for pre-event rejections.
    ///
    /// The cap is enforced **only on new writes**. Payloads already stored in
    /// history replay correctly even if they exceed the current cap — the replay
    /// engine never re-checks sizes on existing events.
    ///
    #[error(
        "payload too large: {kind} for workflow '{workflow_type}' exceeded cap of \
         {cap_bytes} bytes (observed {observed_bytes} bytes)"
    )]
    PayloadTooLarge {
        /// The kind of payload boundary that was violated.
        kind: PayloadKind,
        /// Actual byte length of the payload that was rejected.
        observed_bytes: u64,
        /// Configured cap (bytes) at the enforcement boundary.
        cap_bytes: u64,
        /// The workflow type name that issued the oversized payload.
        workflow_type: String,
        /// The activity name, when the violation is activity-scoped.
        /// `None` for workflow-input, signal-payload, and side-effect violations.
        activity_name: Option<String>,
    },

    /// A new workflow start was blocked by an active admission gate (issue #377).
    ///
    /// Returned by the admission check when at least one active gate matches
    /// the incoming start request. The `gate_id` identifies which gate fired
    /// so operators can correlate with the audit trail. Blocked callers receive
    /// this error synchronously; no workflow execution is created and nothing
    /// is written to `harvest_events`.
    ///
    /// Surfaces as `503 Service Unavailable` at the management API layer with a
    /// JSON body containing `gate_id` and `reason`.
    #[error("admission blocked by gate {gate_id}: {reason}")]
    AdmissionBlocked {
        /// The UUID of the matching admission gate.
        gate_id: Uuid,
        /// Human-readable reason recorded on the gate.
        reason: String,
    },

    /// Delivery of a `signal_external_workflow` call failed permanently.
    ///
    /// `reason_code` is one of:
    /// - `"target_terminal"` — the target workflow is already in a terminal state.
    /// - `"target_unknown"` — no execution matching `target` was found within
    ///   the configured grace window.
    #[error(
        "external signal '{signal_name}' to {target} failed: {reason_code} (signal_id={signal_id})"
    )]
    ExternalSignalFailed {
        /// The `ExternalSignalId` recorded in the initiating event.
        signal_id: ExternalSignalId,
        /// The target workflow execution ID.
        target: ExecutionId,
        /// The signal channel name.
        signal_name: String,
        /// Machine-readable failure reason (`"target_terminal"` or `"target_unknown"`).
        reason_code: String,
    },
}

impl HarvestError {
    /// Build an [`ActivityFailed`](HarvestError::ActivityFailed) from a recorded
    /// failure payload, decoding the typed `error_type` / `details` (issue #227
    /// / #369) so workflow code can branch on the failure class without parsing
    /// the human message.
    ///
    /// `payload` is the engine-internal failure string (a typed `ActivityFailure`
    /// wire envelope, or a legacy `Err(String)` which maps to `error_type =
    /// "Error"`). The `source` is the human-readable message.
    #[must_use]
    pub fn activity_failed(name: impl Into<String>, attempt: u32, payload: &str) -> Self {
        let failure = crate::failure::parse_error_payload_full(payload);
        Self::ActivityFailed {
            name: name.into(),
            attempt,
            error_type: failure.error_type,
            details: failure.details,
            source: failure.message.into(),
        }
    }

    /// If this is an [`ActivityFailed`](HarvestError::ActivityFailed), return its
    /// stable error-type class (e.g. `"CircuitOpen"`). `None` for other variants.
    ///
    /// Lets workflow code branch on the failure class:
    /// ```rust,ignore
    /// if err.activity_error_type() == Some("CircuitOpen") { /* compensate */ }
    /// ```
    #[must_use]
    pub const fn activity_error_type(&self) -> Option<&str> {
        match self {
            Self::ActivityFailed { error_type, .. } => Some(error_type.as_str()),
            _ => None,
        }
    }

    /// If this is an [`ActivityFailed`](HarvestError::ActivityFailed), return the
    /// structured `details` carried by a typed failure (e.g. `retry_after_secs`
    /// / `forced` for a `CircuitOpen`). `None` for other variants or detail-less
    /// failures.
    #[must_use]
    pub const fn activity_details(&self) -> Option<&serde_json::Value> {
        match self {
            Self::ActivityFailed { details, .. } => details.as_ref(),
            _ => None,
        }
    }

    /// `true` if this is an activity failure synthesised because the activity's
    /// circuit breaker was open (issue #369).
    #[must_use]
    pub fn is_circuit_open(&self) -> bool {
        self.activity_error_type() == Some(crate::failure::ERROR_TYPE_CIRCUIT_OPEN)
    }
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
    fn activity_cancelled_variant_exists_and_displays_correctly() {
        let e = HarvestError::ActivityCancelled("workflow was cancelled".into());
        let msg = e.to_string();
        assert!(
            msg.contains("workflow was cancelled"),
            "ActivityCancelled display should include the reason; got: {msg}"
        );
        assert!(
            msg.contains("activity cancelled"),
            "ActivityCancelled display should contain 'activity cancelled'; got: {msg}"
        );
    }

    #[test]
    fn activity_cancelled_is_distinct_from_cancelled() {
        let activity = HarvestError::ActivityCancelled("reason".into());
        let workflow = HarvestError::Cancelled("reason".into());
        assert_ne!(
            activity.to_string(),
            workflow.to_string(),
            "ActivityCancelled and Cancelled should have distinct display strings"
        );
        assert!(
            !matches!(workflow, HarvestError::ActivityCancelled(_)),
            "Cancelled must not match ActivityCancelled pattern"
        );
    }

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
        assert_eq!(
            TimeoutType::WorkflowExecution.to_string(),
            "WorkflowExecution"
        );
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
            error_type: "Error".into(),
            details: None,
            source: Box::new(std::io::Error::other("io error")),
        };
        let msg = e.to_string();
        assert!(msg.contains("test_activity"));
        assert!(msg.contains("attempt 3"));
        assert!(msg.contains("io error"));
    }

    #[test]
    fn activity_failed_decodes_typed_circuit_open_payload() {
        use crate::failure::{ActivityFailure, IntoActivityErrorString};
        let payload = ActivityFailure::circuit_open(
            "charge_card",
            None,
            Some(std::time::Duration::from_secs(30)),
        )
        .into_error_payload();
        let e = HarvestError::activity_failed("charge_card", 1, &payload);
        assert_eq!(e.activity_error_type(), Some("CircuitOpen"));
        assert!(e.is_circuit_open());
        let details = e.activity_details().expect("CircuitOpen carries details");
        assert!((details["retry_after_secs"].as_f64().unwrap() - 30.0).abs() < 0.001);
    }

    #[test]
    fn activity_failed_legacy_string_is_error_type_error() {
        let e = HarvestError::activity_failed("send_email", 2, "connection refused");
        assert_eq!(e.activity_error_type(), Some("Error"));
        assert!(!e.is_circuit_open());
        assert!(e.activity_details().is_none());
        // The human message is preserved as the source.
        assert!(e.to_string().contains("connection refused"));
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

    #[test]
    fn harvest_error_external_signal_failed_display() {
        use crate::types::ExternalSignalId;
        let signal_id = ExternalSignalId::new();
        let target = ExecutionId::new_for_shard(crate::types::ShardId::new(0));
        let e = HarvestError::ExternalSignalFailed {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            reason_code: "target_terminal".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("tenant_cancel"));
        assert!(msg.contains("target_terminal"));
        assert!(msg.contains(&target.to_string()));
    }

    #[test]
    fn harvest_error_workflow_paused_display() {
        let e =
            HarvestError::WorkflowPaused("00000000-0000-0000-0000-000000000001".parse().unwrap());
        let msg = e.to_string();
        assert!(
            msg.contains("paused"),
            "WorkflowPaused display should mention 'paused'; got: {msg}"
        );
        assert!(msg.contains("00000000-0000-0000-0000-000000000001"));
    }

    #[test]
    fn harvest_error_external_signal_failed_unknown_target() {
        use crate::types::ExternalSignalId;
        let signal_id = ExternalSignalId::new();
        let target = ExecutionId::new();
        let e = HarvestError::ExternalSignalFailed {
            signal_id,
            target,
            signal_name: "notify".into(),
            reason_code: "target_unknown".into(),
        };
        assert!(e.to_string().contains("target_unknown"));
        assert!(e.to_string().contains("notify"));
    }
}
