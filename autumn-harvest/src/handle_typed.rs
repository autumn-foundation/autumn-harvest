//! Type-safe client handle wrappers and start options for compile-time-safe workflows.
//!
//! Exposes the generic [`TypedWorkflowHandle`] wrapper and options structs
//! representing the inputs and return structures of typed workflow stubs.

use std::marker::PhantomData;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};
use crate::handle::{WorkflowHandle, WorkflowResultState};
use crate::types::ExecutionId;

/// Compact type-safe workflow result payload.
///
/// This struct holds the final resting state of a completed or failed workflow.
/// It wraps the raw execution result, providing access to the terminal state,
/// strongly-typed output payload, error message, and completion timestamp.
///
/// # Examples
///
/// ```
/// use autumn_harvest::handle::WorkflowResultState;
/// use autumn_harvest::handle_typed::TypedWorkflowResult;
///
/// let result: TypedWorkflowResult<i32> = TypedWorkflowResult {
///     state: WorkflowResultState::Completed,
///     output: Some(42),
///     error: None,
///     completed_at: Some(chrono::Utc::now()),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedWorkflowResult<T> {
    /// Current compact state.
    pub state: WorkflowResultState,
    /// Present only for successful terminal states with a result payload.
    pub output: Option<T>,
    /// Present only for failed/cancelled/timed-out/terminated states.
    pub error: Option<String>,
    /// Timestamp when the execution entered a terminal state.
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Type-safe awaitable handle for one workflow execution.
///
/// A `TypedWorkflowHandle` serves as a client-side proxy to an executing workflow.
/// It wraps the untyped [`WorkflowHandle`] and carries a PhantomData marker for the
/// expected success return type `T`. It enables type-safe polling for results
/// and interacting with the workflow via signals or cancellation requests.
///
/// # Examples
///
/// ```compile_fail
/// use autumn_harvest::handle_typed::TypedWorkflowHandle;
///
/// // Handles are typically returned by typed stubs or the worker registry.
/// // They represent an active execution waiting to be resolved.
/// let handle: TypedWorkflowHandle<String> = my_workflow_stub.start(input).await?;
/// let result = handle.result().await?;
/// ```
#[derive(Debug, Clone)]
pub struct TypedWorkflowHandle<T> {
    inner: WorkflowHandle,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for TypedWorkflowHandle<T> {}
unsafe impl<T> Sync for TypedWorkflowHandle<T> {}

impl<T> TypedWorkflowHandle<T> {
    /// Wrap an untyped [[`WorkflowHandle`]] with type parameter `T` representing
    /// the expected success return type.
    #[must_use]
    pub const fn new(inner: WorkflowHandle) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Execution ID this handle awaits.
    #[must_use]
    pub const fn exec_id(&self) -> ExecutionId {
        self.inner.exec_id()
    }

    /// Access the underlying untyped [[`WorkflowHandle`]].
    #[must_use]
    pub const fn inner(&self) -> &WorkflowHandle {
        &self.inner
    }

    /// Gracefully cancel this workflow execution (cooperative path).
    ///
    /// Delegates to [`WorkflowHandle::cancel`].
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution does not exist,
    /// [`HarvestError::Config`] when the execution is already terminal, and
    /// [`HarvestError::Database`] for persistence failures.
    pub async fn cancel(
        &self,
        reason: &str,
    ) -> HarvestResult<crate::execution::CancelledWorkflowExecution> {
        self.inner.cancel(reason).await
    }

    /// Forcefully terminate this workflow execution (operator escape hatch).
    ///
    /// Delegates to [`WorkflowHandle::terminate`]; seals the run as
    /// `TERMINATED`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution does not exist and
    /// [`HarvestError::Database`] for persistence failures.
    pub async fn terminate(
        &self,
        reason: &str,
    ) -> HarvestResult<crate::execution::CancelledWorkflowExecution> {
        self.inner.terminate(reason).await
    }

    /// Wait until the workflow reaches a terminal state and deserialize its success
    /// output into `T`. Failure terminal states are returned as typed [`HarvestError`]
    /// variants.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, database errors, listener setup errors,
    /// deserialization errors, or not-found errors.
    pub async fn result(&self) -> HarvestResult<T>
    where
        T: DeserializeOwned,
    {
        let raw = self.inner.result_raw().await?;
        serde_json::from_value(raw).map_err(HarvestError::Serialization)
    }

    /// Wait until the workflow reaches a terminal state, returning
    /// [`HarvestError::Timeout`] if the deadline elapses while it is still
    /// running.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, timeout, database errors, listener
    /// setup errors, deserialization errors, or not-found errors.
    pub async fn result_with_timeout(&self, timeout: Duration) -> HarvestResult<T>
    where
        T: DeserializeOwned,
    {
        let raw = self.inner.result_raw_with_timeout(timeout).await?;
        serde_json::from_value(raw).map_err(HarvestError::Serialization)
    }

    /// Return the current compact result snapshot with the output deserialized
    /// into `Option<T>` if present.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution row does not exist,
    /// [`HarvestError::Serialization`] if the output cannot be deserialized,
    /// or [`HarvestError::Database`] for connection/query failures.
    pub async fn result_snapshot(&self) -> HarvestResult<TypedWorkflowResult<T>>
    where
        T: DeserializeOwned,
    {
        let snap = self.inner.result_snapshot().await?;
        let output = match snap.output {
            Some(val) => Some(serde_json::from_value(val).map_err(HarvestError::Serialization)?),
            None => None,
        };
        Ok(TypedWorkflowResult {
            state: snap.state,
            output,
            error: snap.error,
            completed_at: snap.completed_at,
        })
    }
}

/// Optional configurations when starting a typed workflow.
///
/// This struct enables callers to override default start parameters (like the
/// execution ID, queue name, and timeout) when initiating a type-safe workflow.
/// It provides a builder-like structure for fine-grained control over execution.
///
/// # Examples
///
/// ```
/// use autumn_harvest::handle_typed::TypedStartOptions;
/// use autumn_harvest::types::ExecutionId;
/// use uuid::Uuid;
/// use std::time::Duration;
///
/// let options = TypedStartOptions {
///     exec_id: Some(ExecutionId(Uuid::new_v4())),
///     parent_id: None,
///     queue_name: Some("high-priority".to_string()),
///     execution_timeout: Some(Duration::from_secs(3600)),
///     memo: None,
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct TypedStartOptions {
    /// Explicitly override the `ExecutionId` for this run.
    pub exec_id: Option<ExecutionId>,
    /// Set a parent workflow execution ID.
    pub parent_id: Option<uuid::Uuid>,
    /// Override the task queue name (defaults to `"default"`).
    pub queue_name: Option<String>,
    /// Set a custom workflow-level execution timeout.
    pub execution_timeout: Option<Duration>,
    /// Attach arbitrary metadata to the execution.
    pub memo: Option<Value>,
    /// Search attributes for filtering.
    pub search_attrs: Option<Value>,
    /// Behavior when encountering a workflow ID collision (defaults to `AllowDuplicate`).
    pub reuse_policy: Option<crate::types::WorkflowIdReusePolicy>,
    /// W3C trace context carrier for propagation.
    pub trace_context: Option<crate::telemetry::TraceContextCarrier>,
    /// Within-queue claim priority.
    pub priority: Option<crate::types::Priority>,
    /// Queue scheduled start at time.
    pub start_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Queue start delay duration.
    pub delay: Option<chrono::Duration>,
    /// Maximum allowed delay before starting.
    pub max_workflow_start_delay: Option<chrono::Duration>,
    /// Per-execution context headers propagated automatically to activities and children.
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA duration: emits `harvest.workflow.sla_breached` once when the run exceeds this
    /// without terminating it. Overrides `WorkflowInfo::sla`; clamped to `execution_timeout`.
    pub sla: Option<Duration>,
    /// Optional batch policy for this start request. Overrides `WorkflowInfo::batch`.
    pub batch: Option<crate::event_batch::BatchPolicy>,
}

/// Optional configurations when invoking an update and starting a workflow atomically.
#[derive(Debug, Clone, Default)]
pub struct TypedUpdateWithStartOptions {
    /// Explicitly override the `ExecutionId` for this run.
    pub exec_id: Option<ExecutionId>,
    /// Set a parent workflow execution ID.
    pub parent_id: Option<uuid::Uuid>,
    /// Override the task queue name (defaults to `"default"`).
    pub queue_name: Option<String>,
    /// Set a custom workflow-level execution timeout.
    pub execution_timeout: Option<std::time::Duration>,
    /// Attach arbitrary metadata to the execution.
    pub memo: Option<Value>,
    /// Search attributes for filtering.
    pub search_attrs: Option<Value>,
    /// Behavior when encountering a workflow ID collision (defaults to `AllowDuplicate`).
    pub reuse_policy: Option<crate::types::WorkflowIdReusePolicy>,
    /// W3C trace context carrier for propagation.
    pub trace_context: Option<crate::telemetry::TraceContextCarrier>,
    /// Dedup key scoped to `(workflow_name, workflow_id)`.
    ///
    /// When provided the caller should derive `update_id` deterministically
    /// (e.g. `UUIDv5` from this key) so that retried calls hit the dedupe lookup
    /// and return without re-starting or re-admitting the update.
    pub idempotency_key: Option<String>,
    /// Per-execution context headers propagated automatically to activities and children.
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA duration: emits `harvest.workflow.sla_breached` once when the run exceeds this
    /// without terminating it. Overrides `WorkflowInfo::sla`; clamped to `execution_timeout`.
    pub sla: Option<std::time::Duration>,
}

/// Optional configurations when signaling and starting a workflow atomically.
#[derive(Debug, Clone, Default)]
pub struct TypedSignalWithStartOptions {
    /// Explicitly override the `ExecutionId` for this run.
    pub exec_id: Option<ExecutionId>,
    /// Set a parent workflow execution ID.
    pub parent_id: Option<uuid::Uuid>,
    /// Override the task queue name (defaults to `"default"`).
    pub queue_name: Option<String>,
    /// Set a custom workflow-level execution timeout.
    pub execution_timeout: Option<Duration>,
    /// Attach arbitrary metadata to the execution.
    pub memo: Option<Value>,
    /// Search attributes for filtering.
    pub search_attrs: Option<Value>,
    /// Behavior when encountering a workflow ID collision (defaults to `AllowDuplicate`).
    pub reuse_policy: Option<crate::types::WorkflowIdReusePolicy>,
    /// W3C trace context carrier for propagation.
    pub trace_context: Option<crate::telemetry::TraceContextCarrier>,
    /// Dedup key for this signal event.
    pub idempotency_key: Option<String>,
    /// Limit the maximum size of the signal payload (defaults to 256 KiB).
    pub max_signal_payload_bytes: Option<u64>,
    /// Per-execution context headers propagated automatically to activities and children.
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA duration: emits `harvest.workflow.sla_breached` once when the run exceeds this
    /// without terminating it. Overrides `WorkflowInfo::sla`; clamped to `execution_timeout`.
    pub sla: Option<Duration>,
}
