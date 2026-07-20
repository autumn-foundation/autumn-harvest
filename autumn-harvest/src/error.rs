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

use crate::types::{ExecutionId, ExternalAwaitId, ExternalCancelId, ExternalSignalId};

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NonDeterministicDetails {
    pub event_index: Option<i32>,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub workflow_type: Option<String>,
    pub build_id: Option<String>,
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
    ///
    /// The typed fields (issue #767) carry a decoded
    /// [`WorkflowFailure`](crate::failure::WorkflowFailure) when the workflow
    /// returned one; they are `None` for untyped `Err(String)` failures.
    #[error("workflow failed: {name}: {reason}")]
    WorkflowFailed {
        /// The name of the failed workflow.
        name: String,
        /// The reason string describing the failure (human-readable message).
        reason: String,
        /// Stable error-type name from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure); `None` for
        /// untyped failures.
        error_type: Option<String>,
        /// Structured details from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure); `None` for
        /// untyped failures.
        details: Option<serde_json::Value>,
        /// Advisory non-retryable classification hint from a typed
        /// [`WorkflowFailure`](crate::failure::WorkflowFailure); `None` for
        /// untyped failures. This is a hint for the caller / completion-trigger,
        /// **not** a control input to the engine's workflow-level retry (#523)
        /// loop (issue #767 scope).
        non_retryable: Option<bool>,
    },

    /// The engine detected non-deterministic behavior during workflow replay.
    #[error("non-deterministic replay: {reason}")]
    NonDeterministic {
        reason: String,
        details: Box<NonDeterministicDetails>,
    },

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

    /// Returned (under the start transaction's `FOR UPDATE` lock) when a start
    /// would create a **fresh** execution for a workflow that has a debounce
    /// policy, via an entry point that does not itself perform debounce
    /// admission (the fresh path of plain start, signal-with-start,
    /// update-with-start, or batch start). The caller routes this to debounce
    /// admission (plain start) or rejects the request (signal/update/batch).
    /// Because the decision is made under the lock, an attach/idempotent call
    /// (no fresh execution created) never raises it — closing the TOCTOU of an
    /// unlocked pre-scan (issue #499).
    #[error(
        "workflow '{workflow_name}' (id '{workflow_id}') has a debounce policy; \
         a fresh start must go through debounce admission, not this endpoint"
    )]
    DebounceFreshStart {
        /// The workflow type name.
        workflow_name: String,
        /// The requested workflow id.
        workflow_id: String,
    },

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

    /// The workflow's published input schema (issue #373) rejected
    /// `start_input` on a **genuine fresh start** initiated through
    /// [`crate::execution::signal_with_start_workflow_execution`].
    ///
    /// Raised from inside the start transaction's `FOR UPDATE` lock, only
    /// when the call actually creates a new execution -- a call that merely
    /// attaches to an already-running execution never validates `start_input`
    /// (it's never written), closing the TOCTOU window a caller-side,
    /// pre-lock check would otherwise need to work around. Carries the same
    /// structured violations `POST /workflows/{name}/start` returns.
    #[error("input validation failed: {} violation(s)", violations.len())]
    InputValidationFailed {
        /// The schema violations reported by `WorkflowInfo::validate_input`.
        violations: Vec<crate::info::SchemaViolation>,
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

    /// A payload-store operation (offload `put`, fetch `get`, or `delete`)
    /// failed, or an offloaded payload could not be reconstructed (issue #524).
    ///
    /// Covers external-store I/O errors, an unknown `store_id` on read, and a
    /// content-checksum mismatch between the fetched blob and the recorded
    /// reference envelope (which would otherwise silently corrupt replay).
    #[error("payload offload error: {0}")]
    PayloadOffload(String),

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
    ///   `status`, `run_id`, or the six replay-non-determinism diagnostic keys
    ///   `failure_cause`/`event_index`/`expected`/`actual`/`workflow_type`/
    ///   `build_id`, issue #603) or starts with the `_harvest` prefix.
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

    /// The execution row exists and is terminal, but its recorded history can
    /// no longer be replayed to reconstruct final state for a post-mortem query
    /// (issue #612). This happens when the history was pruned by retention,
    /// released on reset (`TERMINATED`), or had its payloads erased (issue #495).
    ///
    /// This is **distinct** from [`NotFound`][Self::NotFound] (row gone → 404):
    /// here the row is present but its history is unqueryable, so returning a
    /// partial/empty/erased answer would be misleading. Surfaces as `410 Gone`
    /// at the management API layer.
    #[error("history unavailable: {reason} ({exec_id})")]
    HistoryUnavailable {
        /// The execution whose history cannot be queried.
        exec_id: ExecutionId,
        /// Human-readable reason (e.g. "pruned by retention", "payloads erased").
        reason: String,
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

    /// Delivery of a `request_cancel_external_workflow` call failed permanently.
    ///
    /// `reason_code` is one of:
    /// - `"target_unknown"` — no execution matching `target` was found within
    ///   the configured grace window.
    /// - `"self_cancel"` — the target is the calling workflow's own `ExecutionId`.
    #[error("external cancel of {target} failed: {reason_code} (cancel_id={cancel_id})")]
    ExternalCancelFailed {
        /// The `ExternalCancelId` recorded in the initiating event.
        cancel_id: ExternalCancelId,
        /// The target workflow execution ID.
        target: ExecutionId,
        /// Machine-readable failure reason (`"target_unknown"` or `"self_cancel"`).
        reason_code: String,
    },

    /// A `ctx.await_external_workflow(...)` call could not observe the target's
    /// terminal outcome (issue #757) — a **transport** failure, distinct from a
    /// target that reached a non-`COMPLETED` terminal state (which surfaces as a
    /// typed [`WorkflowFailed`](HarvestError::WorkflowFailed) carrying the
    /// target's own terminal cause).
    ///
    /// `reason_code` is one of:
    /// - `"self_await"` — the target is the calling workflow's own `ExecutionId`.
    /// - `"target_unknown"` — no execution matching `target` was found within
    ///   the configured grace window.
    #[error("external await of {target} failed: {reason_code} (await_id={await_id})")]
    ExternalAwaitFailed {
        /// The `ExternalAwaitId` recorded in the initiating event (nil UUID for
        /// the immediate `self_await` rejection, which records no history).
        await_id: ExternalAwaitId,
        /// The target workflow execution ID.
        target: ExecutionId,
        /// Machine-readable failure reason (`"self_await"` or `"target_unknown"`).
        reason_code: String,
    },

    /// `ctx.create_session(...)` could not acquire a host worker within the
    /// configured acquisition timeout (issue #606).
    ///
    /// No worker on `queue` advertised a free session slot
    /// (`max_concurrent_sessions`) before the deadline. Author-catchable —
    /// the workflow may retry `create_session` or fall back to plain
    /// activities.
    #[error("session {session_id} acquisition timed out after {timeout_ms}ms on queue '{queue}'")]
    SessionAcquireTimeout {
        /// The session identity that failed to acquire a host.
        session_id: crate::types::SessionId,
        /// The queue the acquisition was attempted on.
        queue: String,
        /// The configured acquisition timeout, in milliseconds.
        timeout_ms: u64,
    },

    /// A worker session's host worker died or drained mid-session (issue
    /// #606).
    ///
    /// Distinct from an ordinary activity failure: partial local state
    /// (a downloaded file, a warmed cache) may be lost, so this is
    /// deliberately never silently retried onto a different worker. The
    /// workflow author must re-establish the session and restart the
    /// affected steps.
    #[error("session {session_id} broken: {reason}")]
    SessionBroken {
        /// The broken session's identity.
        session_id: crate::types::SessionId,
        /// Why the session was declared broken (e.g. "host worker lost
        /// heartbeat", "host worker draining", "session lease expired").
        reason: String,
    },

    /// A workflow tried to re-acquire a durable mutex key it already holds
    /// (issue #691).
    ///
    /// Durable mutexes are non-reentrant: acquiring a key the same workflow
    /// already holds would deadlock (the holder can never release while it is
    /// blocked waiting for itself), so `ctx.mutex(key).acquire()` returns this
    /// synchronously instead of parking. Author-catchable.
    #[error("workflow already holds mutex '{key}'; re-acquiring the same key would deadlock")]
    MutexSelfDeadlock {
        /// The lock key the workflow already holds.
        key: String,
    },
}

/// The Postgres constraint name backing the `harvest_events`
/// `UNIQUE (workflow_exec_id, event_id)` table constraint.
///
/// Auto-generated by Postgres from the inline `UNIQUE (workflow_exec_id,
/// event_id)` constraint in migration `20260409000000_harvest_initial`, and
/// stable across deployments. Used to classify a transient wake-event-ingest
/// event-id conflict (issue #779) — see
/// [`HarvestError::is_event_id_unique_violation`].
pub(crate) const EVENTS_EVENT_ID_UNIQUE_CONSTRAINT: &str =
    "harvest_events_workflow_exec_id_event_id_key";

impl HarvestError {
    /// Build a [`NonDeterministic`](HarvestError::NonDeterministic) error variant.
    #[must_use]
    pub fn non_deterministic(
        reason: impl Into<String>,
        event_index: Option<i32>,
        expected: Option<String>,
        actual: Option<String>,
        workflow_type: Option<String>,
        build_id: Option<String>,
    ) -> Self {
        Self::NonDeterministic {
            reason: reason.into(),
            details: Box::new(NonDeterministicDetails {
                event_index,
                expected,
                actual,
                workflow_type,
                build_id,
            }),
        }
    }

    /// Build a [`NonDeterministic`](HarvestError::NonDeterministic) error variant with reason only.
    #[must_use]
    pub fn non_deterministic_simple(reason: impl Into<String>) -> Self {
        Self::NonDeterministic {
            reason: reason.into(),
            details: Box::new(NonDeterministicDetails {
                event_index: None,
                expected: None,
                actual: None,
                workflow_type: None,
                build_id: None,
            }),
        }
    }

    /// Retrieve the structured [`NonDeterministicDetails`] if this is a
    /// [`NonDeterministic`](HarvestError::NonDeterministic) error.
    #[must_use]
    pub fn non_deterministic_details(&self) -> Option<NonDeterministicDetails> {
        match self {
            Self::NonDeterministic { details, .. } => Some((**details).clone()),
            _ => None,
        }
    }

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

    /// `true` if this is an activity failure synthesised because an operator
    /// force-failed the hung in-flight activity (issue #765).
    ///
    /// ```rust,ignore
    /// if err.is_operator_force_failed() { /* compensate */ }
    /// ```
    #[must_use]
    pub fn is_operator_force_failed(&self) -> bool {
        self.activity_error_type() == Some(crate::failure::ERROR_TYPE_OPERATOR_FORCE_FAILED)
    }

    /// Construct a [`WorkflowFailed`](HarvestError::WorkflowFailed) by decoding a
    /// workflow failure payload, recovering the typed `error_type` / `details` /
    /// `non_retryable` from a [`WorkflowFailure`](crate::failure::WorkflowFailure)
    /// wire envelope (issue #767). A legacy `Err(String)` payload decodes to
    /// `None` typed fields.
    #[must_use]
    pub fn workflow_failed(name: impl Into<String>, payload: &str) -> Self {
        let decoded = crate::failure::decode_workflow_failure(payload);
        Self::WorkflowFailed {
            name: name.into(),
            reason: decoded.message,
            error_type: decoded.error_type,
            details: decoded.details,
            non_retryable: decoded.non_retryable,
        }
    }

    /// Construct an untyped [`WorkflowFailed`](HarvestError::WorkflowFailed): the
    /// typed fields default to `None` (issue #767).
    #[must_use]
    pub fn workflow_failed_untyped(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::WorkflowFailed {
            name: name.into(),
            reason: reason.into(),
            error_type: None,
            details: None,
            non_retryable: None,
        }
    }

    /// If this is a [`WorkflowFailed`](HarvestError::WorkflowFailed), return its
    /// stable error-type class from a typed
    /// [`WorkflowFailure`](crate::failure::WorkflowFailure). `None` for untyped
    /// failures or other variants.
    #[must_use]
    pub fn workflow_error_type(&self) -> Option<&str> {
        match self {
            Self::WorkflowFailed { error_type, .. } => error_type.as_deref(),
            _ => None,
        }
    }

    /// If this is a [`WorkflowFailed`](HarvestError::WorkflowFailed), return the
    /// structured `details` carried by a typed failure. `None` for untyped
    /// failures or other variants.
    #[must_use]
    pub const fn workflow_details(&self) -> Option<&serde_json::Value> {
        match self {
            Self::WorkflowFailed { details, .. } => details.as_ref(),
            _ => None,
        }
    }

    /// `true` if this is a [`WorkflowFailed`](HarvestError::WorkflowFailed)
    /// carrying a typed non-retryable [`WorkflowFailure`](crate::failure::WorkflowFailure).
    /// `false` for untyped failures or other variants.
    ///
    /// This is an **advisory** classification hint for the caller /
    /// completion-trigger — it is not a retry control input; the engine's
    /// workflow-level retry (#523) loop never consults it (issue #767 scope).
    /// To gate the retry loop on a failure class, list its `error_type` in the
    /// workflow's `RetryPolicy::non_retryable_errors` — the scheduler matches
    /// that list against the decoded `error_type`, not against this flag.
    #[must_use]
    pub fn is_workflow_non_retryable(&self) -> bool {
        match self {
            Self::WorkflowFailed { non_retryable, .. } => non_retryable.unwrap_or(false),
            _ => false,
        }
    }

    /// Returns `true` if this is a
    /// [`MutexSelfDeadlock`](HarvestError::MutexSelfDeadlock) — a workflow
    /// re-acquiring a durable mutex key it already holds (issue #691).
    #[must_use]
    pub const fn is_mutex_self_deadlock(&self) -> bool {
        matches!(self, Self::MutexSelfDeadlock { .. })
    }

    /// If this is an [`ExternalAwaitFailed`](HarvestError::ExternalAwaitFailed)
    /// **transport** failure (issue #757), return its machine-readable
    /// `reason_code` (`"self_await"` or `"target_unknown"`). `None` for other
    /// variants — a target that reached a non-`COMPLETED` terminal state
    /// surfaces as a typed [`WorkflowFailed`](HarvestError::WorkflowFailed), not
    /// this variant.
    #[must_use]
    pub const fn external_await_reason_code(&self) -> Option<&str> {
        match self {
            Self::ExternalAwaitFailed { reason_code, .. } => Some(reason_code.as_str()),
            _ => None,
        }
    }

    /// `true` if this is a database error caused by a UNIQUE violation on the
    /// `harvest_events (workflow_exec_id, event_id)` constraint
    /// (`harvest_events_workflow_exec_id_event_id_key`) — issue #779.
    ///
    /// This is a **transient, self-inflicted** conflict, not a logic error. The
    /// worker's wake-event ingest (`ingest_due_timers_and_signals`) appends
    /// `TimerFired`/`SignalReceived` at a `next_event_id` precomputed from an
    /// earlier history load, with no execution-row lock and no `MAX(event_id)`
    /// recompute. A concurrent `append_single_event` writer — timeout
    /// enforcement (`timeout.rs`), external-task completion (`external_task.rs`),
    /// or the issue #779 child-timeout deadline materializer — can commit an
    /// event at that same `event_id` first, and the ingest's batch insert then
    /// hits this unique constraint.
    ///
    /// The correct response is to **re-drive the parent workflow task**, not to
    /// terminally fail a healthy run: a fresh history load advances
    /// `next_event_id` past the winner's already-committed event and an
    /// already-`fired` timer is excluded from the next ingest, so the retry
    /// provably cannot re-hit the same conflict — convergent in a single retry,
    /// with no infinite loop.
    ///
    /// Matched on the exact Postgres constraint name embedded in the
    /// duplicate-key error message. A genuine/unrelated database error, or a
    /// unique violation on any *other* constraint, does not match and still
    /// fails as before.
    #[must_use]
    pub fn is_event_id_unique_violation(&self) -> bool {
        matches!(self, Self::Database(msg) if msg.contains(EVENTS_EVENT_ID_UNIQUE_CONSTRAINT))
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

/// Extracts a human-readable message from a caught panic payload.
///
/// Shared by every `std::panic::catch_unwind` call site that guards a
/// user-supplied handler closure (query, declarative query, and signal
/// handlers) so the `&str` / `String` / "unknown panic" fallback chain is
/// defined exactly once.
///
/// Deliberately takes `payload` **by value** rather than by reference
/// (`#[allow(clippy::needless_pass_by_value)]`): `Box<dyn Any + Send>` itself
/// satisfies the blanket `impl<T: 'static> Any for T`, so a caller passing
/// `&e` for `e: Box<dyn Any + Send>` to a by-reference `&(dyn Any + Send)`
/// parameter can silently coerce by unsizing the *outer* `Box` (which *is*
/// `Any`) rather than deref-coercing to the *inner* erased payload --
/// `downcast_ref` then always misses and this always falls through to
/// `"unknown panic"`. Taking the `Box` by value forces every call site to
/// pass the exact value `catch_unwind` produced, with no coercion ambiguity.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::error::panic_message;
///
/// let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
/// assert_eq!(panic_message(payload), "boom");
/// ```
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Contain a panic raised while **constructing** a handler future (issue #782).
///
/// The poll-time `catch_unwind` at each dispatch site wraps only the *future*
/// returned by a handler `fn`. But a hand-written `WorkflowInfo`/`ActivityInfo`
/// handler (a supported surface — the public `handler` fields,
/// `WorkflowReplayer::register_fn`, etc.) may run synchronous work (an
/// `unwrap()`, a `panic!()`) **before** returning its boxed future; that work
/// runs when the handler is *called*, before the returned future is ever polled,
/// so the poll-time guard does not cover it and the panic would otherwise unwind
/// the spawned worker task uncaught — bypassing the `HandlerPanic` conversion and
/// leaving the task on the poison-pill path. Wrapping the construction call here
/// closes that gap uniformly. Macro-generated handlers put the whole body inside
/// the returned async block, so this only ever fires for hand-written handlers.
///
/// Returns `Ok(fut)` with the constructed future, or `Err(message)` with the
/// extracted panic message (via [`panic_message`]) on a construction-phase panic.
///
/// `AssertUnwindSafe` is sound: on a construction panic the future is never
/// produced and any context the closure borrowed is dropped without further use.
pub(crate) fn catch_construct<F, Fut>(construct: F) -> Result<Fut, String>
where
    F: FnOnce() -> Fut,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(construct)).map_err(panic_message)
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
    fn panic_message_extracts_str_literal_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(panic_message(payload), "boom");
    }

    #[test]
    fn panic_message_extracts_string_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("formatted boom"));
        assert_eq!(panic_message(payload), "formatted boom");
    }

    #[test]
    fn panic_message_falls_back_for_unknown_payload_type() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42_i32);
        assert_eq!(panic_message(payload), "unknown panic");
    }

    #[test]
    fn panic_message_matches_a_real_caught_panic() {
        let payload =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panic!("real panic")))
                .unwrap_err();
        assert_eq!(panic_message(payload), "real panic");
    }

    #[test]
    fn catch_construct_returns_the_constructed_value_when_no_panic() {
        // Issue #782 / PR #1012 review: on the happy path the constructed value
        // (a future, in practice) is passed through unchanged.
        let out: Result<u32, String> = catch_construct(|| 7_u32);
        assert_eq!(out, Ok(7));
    }

    #[test]
    fn catch_construct_contains_a_construction_phase_panic() {
        // A panic raised while constructing the value is caught and its message
        // extracted, rather than unwinding the caller.
        let out: Result<u32, String> = catch_construct(|| panic!("construct boom"));
        assert_eq!(out, Err("construct boom".to_string()));
    }

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
        let e: &dyn std::error::Error =
            &HarvestError::non_deterministic("test", None, None, None, None, None);
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

    // ── transient wake-event-ingest event-id conflict classifier (issue #779) ──

    #[test]
    fn is_event_id_unique_violation_matches_the_events_constraint() {
        // The Display form of a Postgres unique-violation error carries the
        // exact constraint name; `database_error` preserves it verbatim in the
        // `HarvestError::Database` string. This is the real message shape the
        // ingest sees when a concurrent append_single_event took its stale
        // next_event_id first.
        let err = database_error(
            "duplicate key value violates unique constraint \
             \"harvest_events_workflow_exec_id_event_id_key\"",
        );
        assert!(
            err.is_event_id_unique_violation(),
            "an event-id unique violation must be classified as transient; got: {err}"
        );
    }

    #[test]
    fn is_event_id_unique_violation_rejects_other_unique_constraints() {
        // A unique violation on a DIFFERENT constraint (e.g. the executions
        // uniqueness index) must NOT be swept into the transient-conflict
        // requeue path — it still fails as before.
        let err = database_error(
            "duplicate key value violates unique constraint \
             \"uq_harvest_workflow_executions_active_name_id\"",
        );
        assert!(
            !err.is_event_id_unique_violation(),
            "a unique violation on an unrelated constraint must not be classified transient"
        );
    }

    #[test]
    fn is_event_id_unique_violation_rejects_genuine_database_errors() {
        // A genuine, unrelated database error must fail as today.
        let err = database_error("connection refused");
        assert!(!err.is_event_id_unique_violation());
        let err2 = database_error("deadlock detected");
        assert!(!err2.is_event_id_unique_violation());
    }

    #[test]
    fn is_event_id_unique_violation_rejects_non_database_variants() {
        // A non-Database HarvestError never matches — the classifier must not
        // over-broaden across variants.
        assert!(!HarvestError::NotFound("x".into()).is_event_id_unique_violation());
        assert!(!HarvestError::Config("x".into()).is_event_id_unique_violation());
        assert!(
            !HarvestError::workflow_failed_untyped("wf", "boom").is_event_id_unique_violation()
        );
        assert!(!HarvestError::non_deterministic_simple("drift").is_event_id_unique_violation());
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
    fn activity_failed_decodes_typed_operator_force_failed_payload() {
        // Issue #765 AC: the workflow can read a distinct failure cause to
        // tell an operator-forced failure apart from a genuine activity error.
        use crate::failure::{ActivityFailure, IntoActivityErrorString};
        let payload = ActivityFailure::operator_force_failed(Some("wedged on dead downstream"))
            .into_error_payload();
        let e = HarvestError::activity_failed("charge_card", 1, &payload);
        assert_eq!(e.activity_error_type(), Some("OperatorForceFailed"));
        assert!(e.is_operator_force_failed());
        assert!(!e.is_circuit_open());
        let details = e
            .activity_details()
            .expect("OperatorForceFailed carries details");
        assert_eq!(details["reason"], "wedged on dead downstream");
    }

    #[test]
    fn activity_failed_legacy_string_is_error_type_error() {
        let e = HarvestError::activity_failed("send_email", 2, "connection refused");
        assert_eq!(e.activity_error_type(), Some("Error"));
        assert!(!e.is_circuit_open());
        assert!(!e.is_operator_force_failed());
        assert!(e.activity_details().is_none());
        // The human message is preserved as the source.
        assert!(e.to_string().contains("connection refused"));
    }

    #[test]
    fn harvest_error_workflow_failed_display() {
        let e = HarvestError::workflow_failed_untyped("test_workflow", "logic error");
        let msg = e.to_string();
        assert!(msg.contains("test_workflow"));
        assert!(msg.contains("logic error"));
    }

    // ── typed workflow failures (issue #767) ──────────────────────────────

    #[test]
    fn workflow_error_type_returns_type_for_typed_failure() {
        use crate::failure::{IntoWorkflowErrorString, WorkflowFailure};
        let payload =
            WorkflowFailure::new("ValidationRejected", "bad").into_workflow_error_payload();
        let e = HarvestError::workflow_failed("wf", &payload);
        assert_eq!(e.workflow_error_type(), Some("ValidationRejected"));
        assert!(e.to_string().contains("bad"));
    }

    #[test]
    fn workflow_error_type_none_for_untyped() {
        let e = HarvestError::workflow_failed("wf", "plain");
        assert!(e.workflow_error_type().is_none());
        assert!(e.workflow_details().is_none());
        assert!(!e.is_workflow_non_retryable());
        assert!(e.to_string().contains("plain"));
    }

    #[test]
    fn workflow_details_returns_details() {
        use crate::failure::{IntoWorkflowErrorString, WorkflowFailure};
        let payload = WorkflowFailure::new("X", "y")
            .with_details(serde_json::json!({"code": 42}))
            .into_workflow_error_payload();
        let e = HarvestError::workflow_failed("wf", &payload);
        let details = e.workflow_details().expect("typed failure carries details");
        assert_eq!(details["code"], 42);
    }

    #[test]
    fn is_workflow_non_retryable_true_for_non_retryable_envelope() {
        use crate::failure::{IntoWorkflowErrorString, WorkflowFailure};
        let payload = WorkflowFailure::new("Permanent", "no")
            .non_retryable()
            .into_workflow_error_payload();
        let e = HarvestError::workflow_failed("wf", &payload);
        assert!(e.is_workflow_non_retryable());
    }

    #[test]
    fn workflow_failed_decodes_envelope() {
        use crate::failure::{IntoWorkflowErrorString, WorkflowFailure};
        let payload = WorkflowFailure::new("BudgetExceeded", "over cap")
            .with_details(serde_json::json!({"limit": 100}))
            .non_retryable()
            .into_workflow_error_payload();
        let e = HarvestError::workflow_failed("billing", &payload);
        match &e {
            HarvestError::WorkflowFailed {
                name,
                reason,
                error_type,
                details,
                non_retryable,
            } => {
                assert_eq!(name, "billing");
                assert_eq!(reason, "over cap");
                assert_eq!(error_type.as_deref(), Some("BudgetExceeded"));
                assert_eq!(*details, Some(serde_json::json!({"limit": 100})));
                assert_eq!(*non_retryable, Some(true));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn workflow_error_type_none_for_other_variants() {
        let e = HarvestError::Cancelled("stop".into());
        assert!(e.workflow_error_type().is_none());
        assert!(e.workflow_details().is_none());
        assert!(!e.is_workflow_non_retryable());
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

    #[test]
    fn harvest_error_session_acquire_timeout_display() {
        use crate::types::SessionId;
        let session_id = SessionId::new();
        let e = HarvestError::SessionAcquireTimeout {
            session_id,
            queue: "gpu-workers".into(),
            timeout_ms: 30_000,
        };
        let msg = e.to_string();
        assert!(msg.contains("gpu-workers"));
        assert!(msg.contains("30000") || msg.contains("30 s") || msg.contains("30000ms"));
        assert!(msg.contains(&session_id.to_string()));
    }

    #[test]
    fn harvest_error_session_broken_display() {
        use crate::types::SessionId;
        let session_id = SessionId::new();
        let e = HarvestError::SessionBroken {
            session_id,
            reason: "host worker lost heartbeat".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("host worker lost heartbeat"));
        assert!(msg.contains(&session_id.to_string()));
    }

    #[test]
    fn session_acquire_timeout_is_distinct_from_session_broken() {
        use crate::types::SessionId;
        let session_id = SessionId::new();
        let timeout = HarvestError::SessionAcquireTimeout {
            session_id,
            queue: "default".into(),
            timeout_ms: 1000,
        };
        let broken = HarvestError::SessionBroken {
            session_id,
            reason: "reason".into(),
        };
        assert_ne!(timeout.to_string(), broken.to_string());
        assert!(!matches!(
            broken,
            HarvestError::SessionAcquireTimeout { .. }
        ));
        assert!(!matches!(timeout, HarvestError::SessionBroken { .. }));
    }
}
