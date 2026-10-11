//! Durable outbound calls to a remote MCP or A2A task (issue #2006).
//!
//! A workflow starts a remote task and suspends until the task ends. The
//! wait holds no worker slot and no open connection. See `DESIGN-2006.md`
//! and `docs/remote-tasks.md`.
//!
//! [`WorkflowContext::call_remote_task`] runs two durable steps:
//!
//! 1. The activity [`START_ACTIVITY`] starts the remote task. It records a
//!    [`RemoteTaskHandle`], or the result when the server answers at once.
//! 2. The external activity [`AWAIT_ACTIVITY`] journals the handle as an
//!    external task token. The workflow suspends.
//!
//! A [`RemoteTaskPoller`] reads the handle back from history and asks the
//! remote server for its state. It settles the token when the task ends.
//! Replay reads both steps from history and never calls the server.
//!
//! A tool result with `isError: true` is a completed result. The workflow
//! gets `Ok` with [`RemoteTaskOutcome::is_error`] set, and nothing retries.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::{ActivityContext, WorkflowContext};
use crate::error::HarvestResult;
use crate::failure::{ActivityFailure, IntoActivityErrorString as _};
use crate::info::ActivityInfo;
use crate::policy::RetryPolicy;

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
    /// The A2A (`Agent2Agent`) protocol.
    A2a,
}

/// The start request that the start activity records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTaskRequest {
    /// The server name that the transport resolves.
    pub server: String,
    /// The wire protocol.
    pub protocol: RemoteProtocol,
    /// The MCP tool name, or the A2A skill name.
    pub tool: String,
    /// The tool arguments, or the A2A message data.
    pub arguments: Value,
}

/// A remote task call from a workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTaskCall {
    /// The start request.
    pub request: RemoteTaskRequest,
    /// The queue of the start activity.
    pub queue: String,
    /// The longest time to wait for the remote task.
    ///
    /// The wait starts when the token is recorded, after the start. The
    /// engine rounds it up to whole seconds, with a minimum of 1 s.
    pub timeout: Duration,
}

impl RemoteTaskCall {
    /// Call the MCP tool `tool` on `server`.
    #[must_use]
    pub fn mcp(server: &str, tool: &str, arguments: Value, timeout: Duration) -> Self {
        Self::new(server, RemoteProtocol::Mcp, tool, arguments, timeout)
    }

    /// Send a message for the A2A skill `skill` to `server`.
    #[must_use]
    pub fn a2a(server: &str, skill: &str, arguments: Value, timeout: Duration) -> Self {
        Self::new(server, RemoteProtocol::A2a, skill, arguments, timeout)
    }

    fn new(
        server: &str,
        protocol: RemoteProtocol,
        tool: &str,
        arguments: Value,
        timeout: Duration,
    ) -> Self {
        Self {
            request: RemoteTaskRequest {
                server: server.to_string(),
                protocol,
                tool: tool.to_string(),
                arguments,
            },
            queue: "default".to_string(),
            timeout,
        }
    }

    /// Run the start activity on `queue`.
    #[must_use]
    pub fn on_queue(mut self, queue: &str) -> Self {
        self.queue = queue.to_string();
        self
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTaskOutcome {
    /// The tool result, or the A2A task or message.
    #[serde(default)]
    pub result: Value,
    /// `true` when the tool result has `isError: true`.
    #[serde(default)]
    pub is_error: bool,
}

/// What a start returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteTaskStart {
    /// The server made a task.
    Task(RemoteTaskHandle),
    /// The server answered at once.
    Completed(RemoteTaskOutcome),
}

/// The state of a remote task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteTaskState {
    /// The task runs.
    Working,
    /// The task waits for input. Harvest does not answer it.
    InputRequired,
    /// The task ended with a result.
    Completed(RemoteTaskOutcome),
    /// The task failed.
    Failed(String),
    /// Something cancelled the task.
    Cancelled(String),
}

impl RemoteTaskState {
    /// `true` for a state that never changes again.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed(_) | Self::Failed(_) | Self::Cancelled(_)
        )
    }
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

impl RemoteTaskError {
    /// An error that a retry can fix, such as a network error.
    #[must_use]
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    /// An error that a retry cannot fix, such as a protocol error.
    #[must_use]
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

/// A client that starts and reads remote tasks.
///
/// Implement it with `Box::pin(async move { ... })`.
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
///
/// Pass it to `HarvestBuilder::state`. The start activity reads it.
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
    #[must_use]
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

/// The activities to register: the start activity.
///
/// The start activity makes at most five attempts, each with a limit of 60
/// s. It retries a retryable transport error, such as a network error, HTTP
/// 429 or HTTP 5xx. A retry sends the same idempotency key, so a server
/// that honours the key starts one task.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    vec![ActivityInfo {
        name: START_ACTIVITY,
        module: module_path!(),
        default_retry_policy: Some(RetryPolicy::exponential(5, Duration::from_secs(1))),
        default_start_to_close: Some(Duration::from_secs(60)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: start_handler,
    }]
}

fn non_retryable_failure(error_type: &str, message: impl Into<String>) -> String {
    ActivityFailure::non_retryable(error_type, message).into_error_payload()
}

fn start_handler(
    ctx: &ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let remote = ctx.state::<RemoteTasks>().ok_or_else(|| {
            non_retryable_failure(
                "RemoteTasksMissing",
                "no RemoteTasks is installed: pass one to HarvestBuilder::state",
            )
        })?;
        let request: RemoteTaskRequest = serde_json::from_value(input)
            .map_err(|e| non_retryable_failure("RemoteTaskInput", e.to_string()))?;
        let key = ctx
            .idempotency_key()
            .map_err(|e| non_retryable_failure("RemoteTaskKey", e.to_string()))?
            .as_str()
            .to_string();
        let start = remote.transport.start(&request, &key).await.map_err(|e| {
            let failure = if e.retryable {
                ActivityFailure::retryable("RemoteTaskStart", e.message)
            } else {
                ActivityFailure::non_retryable("RemoteTaskStart", e.message)
            };
            failure.into_error_payload()
        })?;
        serde_json::to_value(start)
            .map_err(|e| non_retryable_failure("RemoteTaskOutput", e.to_string()))
    })
}

/// The body of [`WorkflowContext::call_remote_task`].
///
/// It is crate-private so that a workflow calls the context method, which
/// `harvest-verify` classifies as a sink. A free function in this crate is a
/// trusted propagator to the analyzer, so it would hide both steps.
pub(crate) async fn call(
    ctx: &WorkflowContext,
    call: &RemoteTaskCall,
) -> HarvestResult<RemoteTaskOutcome> {
    let input = serde_json::to_value(&call.request)?;
    let started = ctx
        .execute_activity_raw(START_ACTIVITY, input, &call.queue)
        .await?;
    let handle = match serde_json::from_value::<RemoteTaskStart>(started)? {
        RemoteTaskStart::Completed(outcome) => return Ok(outcome),
        RemoteTaskStart::Task(handle) => handle,
    };
    let output = ctx
        .execute_activity_external(
            AWAIT_ACTIVITY,
            serde_json::to_value(&handle)?,
            &call.queue,
            timeout_secs(call.timeout),
        )
        .await?;
    Ok(serde_json::from_value(output)?)
}

/// `timeout` in whole seconds, rounded up, with a minimum of 1.
fn timeout_secs(timeout: Duration) -> u64 {
    let secs = timeout.as_secs();
    let up = if timeout.subsec_nanos() > 0 {
        secs.saturating_add(1)
    } else {
        secs
    };
    up.max(1)
}

/// The text of a JSON value: a string as it is, anything else as JSON.
fn text_of(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

/// MCP Tasks wire format, revision `2026-07-28`.
pub mod mcp {
    use serde_json::{Value, json};

    use super::{
        RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskOutcome, RemoteTaskRequest,
        RemoteTaskStart, RemoteTaskState, text_of,
    };

    /// The protocol revision that defines the Tasks extension.
    pub const PROTOCOL_VERSION: &str = "2026-07-28";
    /// The extension id of MCP Tasks.
    pub const TASKS_EXTENSION: &str = "io.modelcontextprotocol/tasks";
    /// The `_meta` key of the protocol version.
    pub const PROTOCOL_VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";
    /// The `_meta` key of the client capabilities.
    pub const CLIENT_CAPABILITIES_META: &str = "io.modelcontextprotocol/clientCapabilities";
    /// The `_meta` key of the start key that a Harvest server reads (#2005).
    pub const IDEMPOTENCY_KEY_META: &str = "io.autumn-harvest/idempotencyKey";

    fn meta() -> serde_json::Map<String, Value> {
        let mut meta = serde_json::Map::new();
        meta.insert(PROTOCOL_VERSION_META.into(), json!(PROTOCOL_VERSION));
        meta.insert(
            CLIENT_CAPABILITIES_META.into(),
            json!({"extensions": {TASKS_EXTENSION: {}}}),
        );
        meta
    }

    /// The `params` of a `tools/call` that asks for a task.
    #[must_use]
    pub fn tools_call_params(request: &RemoteTaskRequest, idempotency_key: &str) -> Value {
        let mut meta = meta();
        meta.insert(IDEMPOTENCY_KEY_META.into(), json!(idempotency_key));
        json!({
            "name": request.tool,
            "arguments": request.arguments,
            "_meta": meta,
        })
    }

    /// Read a `tools/call` result.
    ///
    /// A `resultType: "task"` result is a handle. A `CallToolResult` is an
    /// outcome, also with `isError: true`.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error for a result with no task id and no content.
    pub fn parse_tools_call_result(
        request: &RemoteTaskRequest,
        result: &Value,
    ) -> Result<RemoteTaskStart, RemoteTaskError> {
        if result.get("resultType").and_then(Value::as_str) == Some("task") {
            let task_id = result
                .get("taskId")
                .and_then(Value::as_str)
                .ok_or_else(|| RemoteTaskError::non_retryable("the task result has no taskId"))?;
            return Ok(RemoteTaskStart::Task(RemoteTaskHandle {
                server: request.server.clone(),
                protocol: RemoteProtocol::Mcp,
                task_id: task_id.to_string(),
            }));
        }
        if result.get("content").is_some() || result.get("structuredContent").is_some() {
            return Ok(RemoteTaskStart::Completed(outcome(result)));
        }
        Err(RemoteTaskError::non_retryable(
            "the tools/call result is not a task and not a tool result",
        ))
    }

    fn outcome(result: &Value) -> RemoteTaskOutcome {
        RemoteTaskOutcome {
            result: result.clone(),
            is_error: result.get("isError").and_then(Value::as_bool) == Some(true),
        }
    }

    /// The `params` of a `tasks/get`.
    #[must_use]
    pub fn tasks_get_params(handle: &RemoteTaskHandle) -> Value {
        json!({"taskId": handle.task_id, "_meta": meta()})
    }

    /// Read a `tasks/get` result.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error for a result with no known status.
    pub fn parse_task(task: &Value) -> Result<RemoteTaskState, RemoteTaskError> {
        let status = task.get("status").and_then(Value::as_str).unwrap_or("");
        let message = || {
            task.get("statusMessage")
                .or_else(|| task.pointer("/error/message"))
                .or_else(|| task.get("error"))
                .map(text_of)
        };
        match status {
            "working" => Ok(RemoteTaskState::Working),
            "input_required" => Ok(RemoteTaskState::InputRequired),
            "completed" => task
                .get("result")
                .filter(|result| result.is_object())
                .map(|result| RemoteTaskState::Completed(outcome(result)))
                .ok_or_else(|| {
                    RemoteTaskError::non_retryable("the completed MCP task has no result")
                }),
            "failed" => Ok(RemoteTaskState::Failed(
                message().unwrap_or_else(|| "the remote task failed".into()),
            )),
            "cancelled" => {
                Ok(RemoteTaskState::Cancelled(message().unwrap_or_else(|| {
                    "the remote server cancelled the task".into()
                })))
            }
            other => Err(RemoteTaskError::non_retryable(format!(
                "unknown MCP task status '{other}'"
            ))),
        }
    }
}

/// A2A `v0.3` wire format: `message/send` and `tasks/get`.
///
/// The state parser also reads the `v1` state names, such as
/// `TASK_STATE_INPUT_REQUIRED`. The `{"skill", "arguments"}` data part is a
/// Harvest convention, not an A2A rule.
pub mod a2a {
    use serde_json::{Value, json};

    use super::{
        RemoteProtocol, RemoteTaskError, RemoteTaskHandle, RemoteTaskOutcome, RemoteTaskRequest,
        RemoteTaskStart, RemoteTaskState,
    };

    /// The `params` of a `message/send`.
    ///
    /// The message id is the idempotency key, so a retry sends the same
    /// message. One data part holds the skill name and the arguments.
    #[must_use]
    pub fn message_send_params(request: &RemoteTaskRequest, idempotency_key: &str) -> Value {
        json!({
            "message": {
                "kind": "message",
                "role": "user",
                "messageId": idempotency_key,
                "parts": [{
                    "kind": "data",
                    "data": {"skill": request.tool, "arguments": request.arguments},
                }],
            },
        })
    }

    /// Read a `message/send` result.
    ///
    /// A task is a handle. A message is an outcome.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error for a result that is not a task or a message.
    pub fn parse_send_result(
        request: &RemoteTaskRequest,
        result: &Value,
    ) -> Result<RemoteTaskStart, RemoteTaskError> {
        let kind = result.get("kind").and_then(Value::as_str);
        if kind == Some("message") {
            return Ok(RemoteTaskStart::Completed(RemoteTaskOutcome {
                result: result.clone(),
                is_error: false,
            }));
        }
        let task_id = result.get("id").and_then(Value::as_str);
        match (kind, task_id) {
            (Some("task") | None, Some(task_id)) if result.get("status").is_some() => {
                Ok(RemoteTaskStart::Task(RemoteTaskHandle {
                    server: request.server.clone(),
                    protocol: RemoteProtocol::A2a,
                    task_id: task_id.to_string(),
                }))
            }
            _ => Err(RemoteTaskError::non_retryable(
                "the message/send result is not a task and not a message",
            )),
        }
    }

    /// The `params` of a `tasks/get`.
    #[must_use]
    pub fn tasks_get_params(handle: &RemoteTaskHandle) -> Value {
        json!({"id": handle.task_id})
    }

    /// The text parts of the status message, joined.
    fn status_text(task: &Value) -> Option<String> {
        let parts = task.pointer("/status/message/parts")?.as_array()?;
        let text: Vec<&str> = parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect();
        (!text.is_empty()).then(|| text.join(" "))
    }

    /// Read a `tasks/get` result.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error for a task with no known state.
    pub fn parse_task(task: &Value) -> Result<RemoteTaskState, RemoteTaskError> {
        let raw = task
            .pointer("/status/state")
            .and_then(Value::as_str)
            .ok_or_else(|| RemoteTaskError::non_retryable("the A2A task has no status.state"))?;
        let lower = raw.to_ascii_lowercase();
        let state = lower
            .strip_prefix("task_state_")
            .unwrap_or(&lower)
            .replace('_', "-");
        let message = || status_text(task).unwrap_or_else(|| format!("the remote task is {state}"));
        match state.as_str() {
            "submitted" | "working" => Ok(RemoteTaskState::Working),
            "input-required" | "auth-required" => Ok(RemoteTaskState::InputRequired),
            "completed" => Ok(RemoteTaskState::Completed(RemoteTaskOutcome {
                result: task.clone(),
                is_error: false,
            })),
            "failed" | "rejected" => Ok(RemoteTaskState::Failed(message())),
            "canceled" | "cancelled" => Ok(RemoteTaskState::Cancelled(message())),
            _ => Err(RemoteTaskError::non_retryable(format!(
                "unknown A2A task state '{raw}'"
            ))),
        }
    }
}

#[cfg(feature = "db")]
mod db;
#[cfg(feature = "db")]
pub use db::{PendingRemoteTask, PollReport, RemoteTaskPoller, pending_remote_tasks, resolve};
