//! The agent loop on the embedded SQLite backend.
//!
//! This backend runs a synchronous activity body with no `ActivityContext`.
//! [`register`] therefore wraps each harness method in a closure. The closure
//! runs the async body with `block_in_place` on the current Tokio runtime, so
//! drive the runtime from a multi-thread Tokio runtime.

use std::sync::Arc;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest_sqlite::{ExecutionId, SqliteResult, SqliteRuntime};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::approval::Approval;
use crate::harness::AgentHarness;
use crate::types::AgentTask;
use crate::workflow::{
    WORKFLOW_NAME, agent_loop_info, agent_model_turn_info, agent_tool_call_info,
};

/// Register the agent workflow and its two activities against `harness`.
///
/// Call it once per runtime, before the first drive. A runtime that reopens a
/// database must register again before it resumes a run.
pub fn register(rt: &mut SqliteRuntime, harness: Arc<AgentHarness>) {
    rt.register_workflow(&agent_loop_info());
    let model = Arc::clone(&harness);
    rt.register_activity(&agent_model_turn_info(), move |input| {
        call(input, |request| model.model_turn(request))
    });
    rt.register_activity(&agent_tool_call_info(), move |input| {
        call(input, |request| harness.tool_call(request))
    });
}

/// Start one agent run. Returns its execution id.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start(rt: &mut SqliteRuntime, task: &AgentTask) -> SqliteResult<ExecutionId> {
    rt.start_workflow(WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Send a reviewer decision to the wait that `signal` names.
///
/// Read `signal` from `RunState::WaitingSignal`. A decision for a wait that
/// already timed out stays unread. It never releases a later call.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the signal.
pub fn decide(
    rt: &mut SqliteRuntime,
    exec: ExecutionId,
    signal: &str,
    approval: &Approval,
) -> SqliteResult<()> {
    rt.send_signal(exec, signal, serde_json::to_value(approval)?)
}

/// Decode `input`, run `body` to completion, and encode its result.
fn call<I, O, F, Fut>(input: Value, body: F) -> Result<Value, String>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, String>>,
{
    let request: I =
        serde_json::from_value(input).map_err(|e| format!("malformed activity input: {e}"))?;
    let handle = multi_thread_handle()?;
    let output = tokio::task::block_in_place(|| handle.block_on(body(request)))?;
    serde_json::to_value(output).map_err(|e| format!("activity output is not JSON: {e}"))
}

/// The current Tokio runtime, when it can run a body with `block_in_place`.
///
/// `block_in_place` panics on a current-thread runtime. A retry cannot fix
/// that, so the call fails at once and names the fix.
fn multi_thread_handle() -> Result<tokio::runtime::Handle, String> {
    let refuse = |why: &str| {
        ActivityFailure::non_retryable(
            "MultiThreadRuntimeRequired",
            format!("{why}: drive the SQLite runtime from a multi-thread Tokio runtime"),
        )
        .into_error_payload()
    };
    let handle =
        tokio::runtime::Handle::try_current().map_err(|_| refuse("no Tokio runtime is running"))?;
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
        return Err(refuse("the Tokio runtime is current-thread"));
    }
    Ok(handle)
}
