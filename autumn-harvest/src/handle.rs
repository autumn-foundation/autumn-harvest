//! In-process workflow result handles for request/response embedders.
//!
//! `WorkflowHandle` is the small client-side primitive for code that starts a
//! workflow behind an HTTP route and wants to await the terminal result without
//! polling the full event history.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult, TimeoutType, database_error};
use crate::execution::{
    StartWorkflowParams, StartedWorkflowExecution, start_or_load_workflow_execution,
};
use crate::models::WorkflowExecution;
use crate::notify::{WorkflowEventListener, WorkflowEventWaitOutcome};
use crate::schema::harvest_workflow_executions;
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::{ExecutionId, ShardId};
use crate::worker::DbPool;

/// Compact public state for a workflow result response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowResultState {
    /// Execution is still active.
    Running,
    /// Execution completed successfully and may have an output payload.
    Completed,
    /// Execution failed permanently.
    Failed,
    /// Execution was explicitly cancelled.
    Cancelled,
    /// Execution exceeded its workflow-level timeout.
    TimedOut,
    /// Execution was forcefully terminated.
    Terminated,
    /// Execution sealed itself and started a successor run.
    ContinuedAsNew,
}

impl WorkflowResultState {
    /// Convert a stored execution state into the compact API state.
    #[must_use]
    pub fn from_execution_state(state: &str) -> Self {
        match state {
            "COMPLETED" => Self::Completed,
            "FAILED" => Self::Failed,
            "CANCELLED" => Self::Cancelled,
            "TIMED_OUT" => Self::TimedOut,
            "TERMINATED" => Self::Terminated,
            "CONTINUED_AS_NEW" => Self::ContinuedAsNew,
            _ => Self::Running,
        }
    }

    /// Whether this state is terminal for the current execution.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Compact workflow result payload.
///
/// This intentionally omits event history. The management API uses the same
/// shape for `GET /workflows/{id}/result`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowResult {
    /// Current compact state.
    pub state: WorkflowResultState,
    /// Present only for successful terminal states with a result payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Present only for failed/cancelled/timed-out/terminated states.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Timestamp when the execution entered a terminal state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WorkflowResult {
    /// Build a compact result from a workflow execution row.
    #[must_use]
    pub fn from_execution(execution: &WorkflowExecution) -> Self {
        let state = WorkflowResultState::from_execution_state(&execution.state);
        match state {
            WorkflowResultState::Completed | WorkflowResultState::ContinuedAsNew => Self {
                state,
                output: execution.output.clone(),
                error: None,
                completed_at: execution.completed_at,
            },
            WorkflowResultState::Failed
            | WorkflowResultState::Cancelled
            | WorkflowResultState::TimedOut
            | WorkflowResultState::Terminated => Self {
                state,
                output: None,
                error: execution.error.clone(),
                completed_at: execution.completed_at,
            },
            WorkflowResultState::Running => Self {
                state,
                output: None,
                error: None,
                completed_at: None,
            },
        }
    }

    /// Build a successful terminal result.
    #[must_use]
    pub const fn completed(
        state: WorkflowResultState,
        output: Value,
        completed_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        Self {
            state,
            output: Some(output),
            error: None,
            completed_at,
        }
    }

    /// Build a non-terminal result.
    #[must_use]
    pub const fn running() -> Self {
        Self {
            state: WorkflowResultState::Running,
            output: None,
            error: None,
            completed_at: None,
        }
    }

    /// Whether this result represents a terminal execution state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(Clone)]
struct WorkflowHandleClientInner {
    pools: ShardedDbPool,
    router: ShardRouter,
    notification_database_urls: BTreeMap<ShardId, String>,
    payload_codecs: crate::payload_codec::PayloadCodecs,
    shared_state: crate::context::SharedState,
    update_handlers: Vec<crate::info::UpdateHandlerInfo>,
    query_handlers: Vec<crate::info::QueryHandlerInfo>,
}

impl std::fmt::Debug for WorkflowHandleClientInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandleClientInner")
            .field("pools", &self.pools)
            .field("router", &self.router)
            .field(
                "notification_database_urls",
                &self.notification_database_urls,
            )
            .field("payload_codecs", &"<PayloadCodecs>")
            .field("shared_state", &"<SharedState>")
            .field("update_handlers_count", &self.update_handlers.len())
            .field("query_handlers_count", &self.query_handlers.len())
            .finish()
    }
}

/// Factory for shard-aware workflow handles.
///
/// The client owns the storage pools, shard router, and per-shard database URLs
/// used for LISTEN/NOTIFY. A handle cloned from this client routes reads through
/// [`ShardRouter::shard_for_execution`] using its `ExecutionId`.
#[derive(Debug, Clone)]
pub struct WorkflowHandleClient {
    inner: Arc<WorkflowHandleClientInner>,
}

impl WorkflowHandleClient {
    /// Build a single-shard handle client.
    #[must_use]
    pub fn single(pool: DbPool, notification_database_url: impl Into<String>) -> Self {
        Self::new(
            ShardedDbPool::single(pool),
            ShardRouter::single(),
            [(ShardId::new(0), notification_database_url)],
        )
    }

    /// Build a client for a sharded deployment.
    ///
    /// `notification_database_urls` must include every readable shard that may
    /// be awaited. Missing entries are reported as [`HarvestError::Config`]
    /// when a handle tries to wait on that shard.
    #[must_use]
    pub fn new<I, S>(
        pools: ShardedDbPool,
        router: ShardRouter,
        notification_database_urls: I,
    ) -> Self
    where
        I: IntoIterator<Item = (ShardId, S)>,
        S: Into<String>,
    {
        Self {
            inner: Arc::new(WorkflowHandleClientInner {
                pools,
                router,
                notification_database_urls: notification_database_urls
                    .into_iter()
                    .map(|(shard, url)| (shard, url.into()))
                    .collect(),
                payload_codecs: crate::payload_codec::PayloadCodecs::default(),
                shared_state: crate::context::empty_shared_state(),
                update_handlers: Vec::new(),
                query_handlers: Vec::new(),
            }),
        }
    }

    /// Add custom payload codecs to the client.
    #[must_use]
    pub fn with_codecs(self, codecs: crate::payload_codec::PayloadCodecs) -> Self {
        let mut inner = (*self.inner).clone();
        inner.payload_codecs = codecs;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add shared state to the client.
    #[must_use]
    pub fn with_shared_state(self, shared_state: crate::context::SharedState) -> Self {
        let mut inner = (*self.inner).clone();
        inner.shared_state = shared_state;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Add query and update handlers to the client.
    #[must_use]
    pub fn with_handlers(
        self,
        query_handlers: Vec<crate::info::QueryHandlerInfo>,
        update_handlers: Vec<crate::info::UpdateHandlerInfo>,
    ) -> Self {
        let mut inner = (*self.inner).clone();
        inner.query_handlers = query_handlers;
        inner.update_handlers = update_handlers;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Pick which shard a new workflow execution with `(name, id)` should land on.
    #[must_use]
    pub fn pick_shard_for_new_workflow(&self, workflow_name: &str, workflow_id: &str) -> ShardId {
        self.inner
            .router
            .pick_for_new_workflow(workflow_name, workflow_id)
    }

    /// Create a handle for an existing workflow execution.
    #[must_use]
    pub fn handle(&self, exec_id: ExecutionId) -> WorkflowHandle {
        WorkflowHandle {
            exec_id,
            client: self.clone(),
        }
    }

    /// Start or load a workflow, returning the normal start metadata and a
    /// result handle for the resolved execution.
    ///
    /// # Errors
    ///
    /// Propagates [`start_or_load_workflow_execution`] failures.
    pub async fn start_or_load(
        &self,
        conn: &mut AsyncPgConnection,
        request: StartWorkflowParams<'_>,
    ) -> HarvestResult<StartedWorkflowHandle> {
        let started = start_or_load_workflow_execution(conn, request).await?;
        let handle = self.handle(started.exec_id);
        Ok(StartedWorkflowHandle { started, handle })
    }
}

/// Result of starting/loading a workflow together with an awaitable handle.
#[derive(Debug, Clone)]
pub struct StartedWorkflowHandle {
    /// Standard start/load metadata.
    pub started: StartedWorkflowExecution,
    /// Awaitable result handle for `started.exec_id`.
    pub handle: WorkflowHandle,
}

/// Awaitable handle for one workflow execution.
#[derive(Debug, Clone)]
pub struct WorkflowHandle {
    exec_id: ExecutionId,
    client: WorkflowHandleClient,
}

impl WorkflowHandle {
    /// Execution ID this handle awaits.
    #[must_use]
    pub const fn exec_id(&self) -> ExecutionId {
        self.exec_id
    }

    /// Return the current compact result snapshot without waiting.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] when the execution row does not exist,
    /// or [`HarvestError::Database`] for connection/query failures.
    pub async fn result_snapshot(&self) -> HarvestResult<WorkflowResult> {
        let execution = self.load_execution().await?;
        Ok(WorkflowResult::from_execution(&execution))
    }

    /// Wait up to `timeout` for a terminal compact snapshot.
    ///
    /// Returns `Ok(None)` when the execution is still running after the wait
    /// window elapses. This is the long-poll shape used by HTTP routes that
    /// need to return `204 No Content` instead of treating an elapsed wait as a
    /// workflow timeout.
    ///
    /// # Errors
    ///
    /// Returns database, listener setup, notification payload, or not-found
    /// errors.
    pub async fn result_snapshot_with_wait(
        &self,
        timeout: Duration,
    ) -> HarvestResult<Option<WorkflowResult>> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarvestError::Config("workflow result wait duration overflowed".to_string())
        })?;
        let snapshot = self.result_snapshot().await?;
        if snapshot.is_terminal() {
            return Ok(Some(snapshot));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let snapshot = self.result_snapshot().await?;
            if snapshot.is_terminal() {
                return Ok(Some(snapshot));
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(now);
            match listener.wait_for_notification_timeout(remaining).await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {
                    let snapshot = self.result_snapshot().await?;
                    return if snapshot.is_terminal() {
                        Ok(Some(snapshot))
                    } else {
                        Ok(None)
                    };
                }
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    /// Wait until the workflow reaches a terminal state and return its raw JSON
    /// output. Failure terminal states are returned as typed [`HarvestError`]
    /// variants.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, database errors, listener setup errors,
    /// or not-found errors.
    pub async fn result_raw(&self) -> HarvestResult<Value> {
        let execution = self.load_execution().await?;
        if let Some(result) = terminal_raw_result(&execution) {
            return result;
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let execution = self.load_execution().await?;
            if let Some(result) = terminal_raw_result(&execution) {
                return result;
            }

            match listener.wait_for_notification().await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {}
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    /// Wait until the workflow reaches a terminal state, returning
    /// [`HarvestError::Timeout`] if the deadline elapses while it is still
    /// running.
    ///
    /// # Errors
    ///
    /// Returns terminal workflow errors, timeout, database errors, listener
    /// setup errors, or not-found errors.
    pub async fn result_raw_with_timeout(&self, timeout: Duration) -> HarvestResult<Value> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarvestError::Config("workflow result timeout duration overflowed".to_string())
        })?;
        let execution = self.load_execution().await?;
        if let Some(result) = terminal_raw_result(&execution) {
            return result;
        }
        if Instant::now() >= deadline {
            return Err(wait_timeout_error(&execution));
        }

        let mut listener = self.connect_listener().await?;

        loop {
            let execution = self.load_execution().await?;
            if let Some(result) = terminal_raw_result(&execution) {
                return result;
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(wait_timeout_error(&execution));
            }
            let remaining = deadline.saturating_duration_since(now);
            match listener.wait_for_notification_timeout(remaining).await? {
                WorkflowEventWaitOutcome::Notification(_payload) => {}
                WorkflowEventWaitOutcome::TimedOut => {
                    let execution = self.load_execution().await?;
                    if let Some(result) = terminal_raw_result(&execution) {
                        return result;
                    }
                    return Err(wait_timeout_error(&execution));
                }
                WorkflowEventWaitOutcome::ChannelClosed => {
                    listener = self.connect_listener().await?;
                }
            }
        }
    }

    fn shard(&self) -> ShardId {
        self.client.inner.router.shard_for_execution(self.exec_id)
    }

    fn notification_database_url(&self) -> HarvestResult<String> {
        let shard = self.shard();
        self.client
            .inner
            .notification_database_urls
            .get(&shard)
            .cloned()
            .ok_or_else(|| {
                HarvestError::Config(format!(
                    "no workflow notification database URL configured for shard {shard}"
                ))
            })
    }

    async fn connect_listener(&self) -> HarvestResult<WorkflowEventListener> {
        WorkflowEventListener::connect(&self.notification_database_url()?).await
    }

    async fn load_execution(&self) -> HarvestResult<WorkflowExecution> {
        let shard = self.shard();
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        harvest_workflow_executions::table
            .find(self.exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {}", self.exec_id)))
    }

    /// Execute a registered query handler in-process by replaying event history.
    ///
    /// # Errors
    ///
    /// Returns query execution or hydration errors.
    pub async fn execute_query_in_process(
        &self,
        workflow_info: &crate::info::WorkflowInfo,
        query_info: &crate::info::QueryHandlerInfo,
        query_name: &str,
        args: Value,
    ) -> HarvestResult<Value> {
        let execution = self.load_execution().await?;
        if WorkflowResultState::from_execution_state(&execution.state).is_terminal() {
            return Err(HarvestError::WorkflowNotRunning(self.exec_id));
        }

        let shard = self.shard();
        let mut conn = self
            .client
            .inner
            .pools
            .pool_for(shard)
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;
        let history = crate::store::load_history_with_codecs(
            &mut conn,
            self.exec_id,
            &self.client.inner.payload_codecs,
        )
        .await?;
        drop(conn);

        let ctx = crate::context::WorkflowContext::for_replay_with_state(
            self.exec_id,
            history.events,
            self.client.inner.shared_state.clone(),
        );
        for q_info in &self.client.inner.query_handlers {
            if q_info.workflow == workflow_info.name {
                ctx.register_declarative_query_handler(q_info);
            }
        }
        for u_info in &self.client.inner.update_handlers {
            if u_info.workflow == workflow_info.name {
                ctx.register_declarative_update_handler(u_info);
            }
        }
        ctx.register_declarative_query_handler(query_info);

        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waker_arc = Arc::new(WakerFlag(flag.clone()));
        let waker = futures::task::waker_ref(&waker_arc);
        let mut poll_cx = std::task::Context::from_waker(&waker);

        let handler_fut = (workflow_info.handler)(&ctx, execution.input.clone());
        tokio::pin!(handler_fut);

        loop {
            flag.store(false, std::sync::atomic::Ordering::Release);
            match handler_fut.as_mut().poll(&mut poll_cx) {
                std::task::Poll::Ready(_) => break,
                std::task::Poll::Pending => {
                    let was_woken = std::sync::atomic::AtomicBool::load(
                        &flag,
                        std::sync::atomic::Ordering::Acquire,
                    );
                    if !was_woken {
                        break;
                    }
                }
            }
        }

        ctx.execute_query_with_args(query_name, args)
    }

    /// Durably admit a workflow update and poll until it completes or fails.
    ///
    /// # Errors
    ///
    /// Returns update execution, admission, or polling timeout errors.
    pub async fn execute_update_in_process(
        &self,
        conn: &mut AsyncPgConnection,
        name: &str,
        input: Value,
        timeout: Duration,
    ) -> HarvestResult<Value> {
        let update_id = crate::types::UpdateId::new();
        crate::store::admit_update_event(conn, self.exec_id, update_id, name.to_string(), input)
            .await?;
        crate::queue::wake_workflow_task(conn, self.exec_id).await?;

        let start = Instant::now();
        let poll_interval = Duration::from_millis(100);

        loop {
            let result = {
                let shard = self.shard();
                let mut c = self
                    .client
                    .inner
                    .pools
                    .pool_for(shard)
                    .get()
                    .await
                    .map_err(|e| HarvestError::Database(e.to_string()))?;
                let h = crate::store::load_history_with_codecs(
                    &mut c,
                    self.exec_id,
                    &self.client.inner.payload_codecs,
                )
                .await?;
                match crate::replay::HistoryMatcher::new(h.events).match_update(update_id) {
                    crate::replay::HistoryMatch::Matched { output } => Some(Ok(output)),
                    crate::replay::HistoryMatch::Failed { error, .. } => {
                        Some(Err(HarvestError::WorkflowFailed {
                            name: self.exec_id.to_string(),
                            reason: error,
                        }))
                    }
                    _ => None,
                }
            };

            if let Some(res) = result {
                return res;
            }

            if start.elapsed() >= timeout {
                return Err(HarvestError::Timeout {
                    timeout_type: TimeoutType::ScheduleToClose,
                    task_name: format!("update {name}"),
                });
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

fn terminal_raw_result(execution: &WorkflowExecution) -> Option<HarvestResult<Value>> {
    match execution.state.as_str() {
        "COMPLETED" | "CONTINUED_AS_NEW" => {
            Some(Ok(execution.output.clone().unwrap_or(Value::Null)))
        }
        "FAILED" => Some(Err(HarvestError::WorkflowFailed {
            name: execution.workflow_name.clone(),
            reason: execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow failed".to_string()),
        })),
        "CANCELLED" => Some(Err(HarvestError::Cancelled(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow cancelled".to_string()),
        ))),
        "TIMED_OUT" => Some(Err(HarvestError::Timeout {
            timeout_type: TimeoutType::ScheduleToClose,
            task_name: execution.workflow_name.clone(),
        })),
        "TERMINATED" => Some(Err(HarvestError::Terminated(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "workflow terminated".to_string()),
        ))),
        _ => None,
    }
}

fn wait_timeout_error(execution: &WorkflowExecution) -> HarvestError {
    HarvestError::Timeout {
        timeout_type: TimeoutType::ScheduleToClose,
        task_name: execution.workflow_name.clone(),
    }
}

/// Start or load a workflow and return an awaitable [`WorkflowHandle`].
///
/// This free function mirrors [`start_or_load_workflow_execution`] for callers
/// that prefer functions over [`WorkflowHandleClient::start_or_load`].
///
/// # Errors
///
/// Propagates [`start_or_load_workflow_execution`] failures.
pub async fn start_or_load_workflow_execution_with_handle(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    client: &WorkflowHandleClient,
) -> HarvestResult<StartedWorkflowHandle> {
    client.start_or_load(conn, request).await
}

struct WakerFlag(Arc<std::sync::atomic::AtomicBool>);

impl futures::task::ArcWake for WakerFlag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_result_state_maps_database_states() {
        assert_eq!(
            WorkflowResultState::from_execution_state("COMPLETED"),
            WorkflowResultState::Completed
        );
        assert_eq!(
            WorkflowResultState::from_execution_state("CONTINUED_AS_NEW"),
            WorkflowResultState::ContinuedAsNew
        );
        assert_eq!(
            WorkflowResultState::from_execution_state("RUNNING"),
            WorkflowResultState::Running
        );
    }
}
