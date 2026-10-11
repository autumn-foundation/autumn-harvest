//! Durable outbound calls to a remote MCP or A2A task (issue #2006).
//!
//! A workflow starts a remote task and suspends until the task ends. The
//! wait holds no worker slot and no open connection. See `DESIGN-2006.md`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::WorkflowContext;
use crate::error::HarvestResult;
use crate::info::ActivityInfo;

/// The activity that starts a remote task.
pub const START_ACTIVITY: &str = "harvest_remote_task_start";
/// The external activity that waits for a remote task.
pub const AWAIT_ACTIVITY: &str = "harvest_remote_task_await";

/// The future that a [`RemoteTaskTransport`] method returns.
pub type RemoteFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The wire protocol of a remote server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProtocol {
    /// MCP Tasks.
    Mcp,
    /// Agent2Agent.
    A2a,
}

/// The start request that the start activity records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskRequest {
    /// The server name that the transport resolves.
    pub server: String,
    /// The wire protocol.
    pub protocol: RemoteProtocol,
    /// The MCP tool name, or the A2A skill name.
    pub tool: String,
    /// The tool arguments, or the A2A message.
    pub arguments: Value,
}

/// A remote task call from a workflow.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteTaskCall {
    /// The start request.
    pub request: RemoteTaskRequest,
    /// The queue of the start activity.
    pub queue: String,
    /// The longest time to wait for the remote task.
    pub timeout: Duration,
}

impl RemoteTaskCall {
    /// Call the MCP tool `tool` on `server`.
    #[must_use]
    pub fn mcp(server: &str, tool: &str, arguments: Value, timeout: Duration) -> Self {
        let _ = (server, tool, arguments, timeout);
        todo!("issue #2006")
    }

    /// Send a message for the A2A skill `skill` to `server`.
    #[must_use]
    pub fn a2a(server: &str, skill: &str, arguments: Value, timeout: Duration) -> Self {
        let _ = (server, skill, arguments, timeout);
        todo!("issue #2006")
    }

    /// Run the start activity on `queue`.
    #[must_use]
    pub fn on_queue(self, queue: &str) -> Self {
        let _ = queue;
        todo!("issue #2006")
    }
}

/// The durable handle of one remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTaskHandle {
    /// The server name.
    pub server: String,
    /// The wire protocol.
    pub protocol: RemoteProtocol,
    /// The remote task id.
    pub task_id: String,
}

/// The result of a remote task that ended with a result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskOutcome {
    /// The tool result, or the A2A task.
    pub result: Value,
    /// `true` when the tool result has `isError: true`.
    pub is_error: bool,
}

/// What a start returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteTaskStart {
    /// The server made a task.
    Task(RemoteTaskHandle),
    /// The server answered at once.
    Completed(RemoteTaskOutcome),
}

/// The state of a remote task.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteTaskState {
    /// The task runs.
    Working,
    /// The task waits for input. Harvest does not answer it.
    InputRequired,
    /// The task ended with a result.
    Completed(RemoteTaskOutcome),
    /// The task failed.
    Failed(String),
    /// The task was cancelled.
    Cancelled(String),
}

/// A transport error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct RemoteTaskError {
    /// What went wrong.
    pub message: String,
    /// `true` when a retry can succeed.
    pub retryable: bool,
}

/// A client that starts and reads remote tasks.
pub trait RemoteTaskTransport: Send + Sync {
    /// Start a remote task. Send `idempotency_key` with the request.
    fn start<'a>(
        &'a self,
        request: &'a RemoteTaskRequest,
        idempotency_key: &'a str,
    ) -> RemoteFuture<'a, Result<RemoteTaskStart, RemoteTaskError>>;

    /// Read the state of a remote task.
    fn get<'a>(
        &'a self,
        handle: &'a RemoteTaskHandle,
    ) -> RemoteFuture<'a, Result<RemoteTaskState, RemoteTaskError>>;
}

/// The worker state that holds the transport.
#[derive(Clone)]
pub struct RemoteTasks {
    transport: Arc<dyn RemoteTaskTransport>,
}

impl std::fmt::Debug for RemoteTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTasks").finish_non_exhaustive()
    }
}

impl RemoteTasks {
    /// Wrap a transport.
    pub fn new(transport: impl RemoteTaskTransport + 'static) -> Self {
        Self {
            transport: Arc::new(transport),
        }
    }

    /// The transport.
    #[must_use]
    pub fn transport(&self) -> &Arc<dyn RemoteTaskTransport> {
        &self.transport
    }
}

/// The activities to register.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    todo!("issue #2006")
}

/// Call a remote task and wait for it to end.
///
/// # Errors
///
/// Returns an error when the start fails, the task fails or the timeout
/// ends.
pub async fn call(ctx: &WorkflowContext, call: &RemoteTaskCall) -> HarvestResult<RemoteTaskOutcome> {
    let _ = (ctx, call);
    todo!("issue #2006")
}

/// MCP Tasks wire format.
pub mod mcp {
    use super::{RemoteTaskError, RemoteTaskHandle, RemoteTaskRequest, RemoteTaskStart, RemoteTaskState};
    use serde_json::Value;

    /// The `params` of a `tools/call` that asks for a task.
    #[must_use]
    pub fn tools_call_params(request: &RemoteTaskRequest, idempotency_key: &str) -> Value {
        let _ = (request, idempotency_key);
        todo!("issue #2006")
    }

    /// Read a `tools/call` result.
    ///
    /// # Errors
    ///
    /// Returns an error for a result with no task id and no content.
    pub fn parse_tools_call_result(
        request: &RemoteTaskRequest,
        result: &Value,
    ) -> Result<RemoteTaskStart, RemoteTaskError> {
        let _ = (request, result);
        todo!("issue #2006")
    }

    /// The `params` of a `tasks/get`.
    #[must_use]
    pub fn tasks_get_params(handle: &RemoteTaskHandle) -> Value {
        let _ = handle;
        todo!("issue #2006")
    }

    /// Read a `tasks/get` result.
    ///
    /// # Errors
    ///
    /// Returns an error for a result with no known status.
    pub fn parse_task(task: &Value) -> Result<RemoteTaskState, RemoteTaskError> {
        let _ = task;
        todo!("issue #2006")
    }
}

/// A2A wire format.
pub mod a2a {
    use super::{RemoteTaskError, RemoteTaskHandle, RemoteTaskRequest, RemoteTaskStart, RemoteTaskState};
    use serde_json::Value;

    /// The `params` of a `message/send`.
    #[must_use]
    pub fn message_send_params(request: &RemoteTaskRequest, idempotency_key: &str) -> Value {
        let _ = (request, idempotency_key);
        todo!("issue #2006")
    }

    /// Read a `message/send` result.
    ///
    /// # Errors
    ///
    /// Returns an error for a result that is not a task or a message.
    pub fn parse_send_result(
        request: &RemoteTaskRequest,
        result: &Value,
    ) -> Result<RemoteTaskStart, RemoteTaskError> {
        let _ = (request, result);
        todo!("issue #2006")
    }

    /// The `params` of a `tasks/get`.
    #[must_use]
    pub fn tasks_get_params(handle: &RemoteTaskHandle) -> Value {
        let _ = handle;
        todo!("issue #2006")
    }

    /// Read a `tasks/get` result.
    ///
    /// # Errors
    ///
    /// Returns an error for a task with no known state.
    pub fn parse_task(task: &Value) -> Result<RemoteTaskState, RemoteTaskError> {
        let _ = task;
        todo!("issue #2006")
    }
}

#[cfg(feature = "db")]
mod db;
#[cfg(feature = "db")]
pub use db::{PendingRemoteTask, PollReport, RemoteTaskPoller, pending_remote_tasks, resolve};
