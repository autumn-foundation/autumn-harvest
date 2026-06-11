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
#[derive(Debug, Clone)]
pub struct TypedWorkflowHandle<T> {
    inner: WorkflowHandle,
    _marker: PhantomData<T>,
}

unsafe impl<T> Send for TypedWorkflowHandle<T> {}
unsafe impl<T> Sync for TypedWorkflowHandle<T> {}

impl<T> TypedWorkflowHandle<T> {
    /// Wrap an untyped [`WorkflowHandle`] with type parameter `T` representing
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

    /// Access the underlying untyped [`WorkflowHandle`].
    #[must_use]
    pub const fn inner(&self) -> &WorkflowHandle {
        &self.inner
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
}
