//! The durable agent session: one workflow run per goal.
//!
//! The workflow is the agent loop. Every model call and every tool call is a
//! durable activity, so the transcript is a projection of the recorded history.
//! A restart replays the recorded turns instead of paying for them again.
//!
//! Two activities carry the whole loop:
//!
//! - `claude_turn` — one Messages API request. The reply is recorded in history.
//! - `run_tool` — one local tool call against the daemon workspace.
//!
//! A tool that changes the workspace waits for an approval signal first. The
//! wait has a durable deadline, so an unattended session denies the call and
//! continues instead of blocking forever.

use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::tools;

/// The signal-name prefix the CLI sends a decision to.
///
/// The full name carries the tool-use id (see [`approval_signal`]). A decision
/// therefore names the one call it releases. A stale or repeated approval stays
/// staged under its own name, and never releases a later, unseen call.
pub const SIGNAL_TOOL_APPROVAL: &str = "tool_approval";

/// The registered workflow name.
pub const WORKFLOW_NAME: &str = "agent_session";

/// The `stop_reason` a safety classifier declines a request with.
const STOP_REFUSAL: &str = "refusal";

/// The `stop_reason` of a turn cut short by the output cap.
const STOP_MAX_TOKENS: &str = "max_tokens";

/// The instructions the model runs under. The value is part of every request,
/// so a change to it alters the model input of later turns only.
pub const SYSTEM_PROMPT: &str = "\
You are a local coding assistant. You work inside one sandboxed workspace \
directory. Use the tools to inspect and edit files in that directory. \
Paths are relative to the workspace root. A write is gated on human approval, \
so prefer to read first and to write once. State your answer in plain text \
when the task is done.";

/// The goal one session works on.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionTask {
    /// The task in the operator's own words.
    pub goal: String,
    /// The hard bound on model calls. The session ends when it is reached.
    pub max_turns: u32,
    /// How long an approval-gated tool call waits before it is denied.
    pub approval_timeout_secs: u64,
}

/// One entry of the Messages API `messages` array.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: Value,
}

impl Message {
    /// A user turn that carries the given content blocks.
    pub fn user(content: Value) -> Self {
        Self {
            role: "user".to_string(),
            content,
        }
    }

    /// An assistant turn, replayed verbatim from the recorded reply.
    pub fn assistant(content: Value) -> Self {
        Self {
            role: "assistant".to_string(),
            content,
        }
    }
}

/// The input of one `claude_turn` activity: the whole conversation so far.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TurnRequest {
    pub messages: Vec<Message>,
}

/// One `tool_use` block the model emitted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
}

/// The durable result of one model call.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TurnReply {
    /// The assistant content blocks, exactly as the API returned them. The
    /// workflow replays this array into the next request without a change,
    /// which is what keeps thinking blocks valid on the same model.
    pub content: Value,
    pub stop_reason: String,
    /// The text blocks joined, for the operator-facing report.
    pub text: String,
    pub tool_calls: Vec<ToolCall>,
}

/// The durable result of one tool call.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ToolOutcome {
    pub output: String,
    pub is_error: bool,
}

impl ToolOutcome {
    /// A failed tool call. The model reads the reason and can choose again.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            output: message.into(),
            is_error: true,
        }
    }
}

/// The signal name that releases one specific tool call.
pub fn approval_signal(call_id: &str) -> String {
    format!("{SIGNAL_TOOL_APPROVAL}:{call_id}")
}

/// The tool-use id one approval signal name releases.
pub fn approval_call_id(signal_name: &str) -> Option<&str> {
    signal_name
        .strip_prefix(SIGNAL_TOOL_APPROVAL)?
        .strip_prefix(':')
}

/// The decision the CLI sends for an approval-gated tool call.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub approved: bool,
    pub note: Option<String>,
}

/// What one finished session produced.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionReport {
    /// The model's last text, or the reason the session stopped early.
    pub answer: String,
    pub turns: u32,
    pub tool_calls: u32,
    /// `end_turn`, `max_turns`, or `refusal`.
    pub stop: String,
}

/// The agent loop.
///
/// The loop is ordinary Rust. Durability comes from the two awaits: each one
/// suspends the run until its result is committed to history.
#[workflow]
pub async fn agent_session(
    ctx: &WorkflowContext,
    task: SessionTask,
) -> Result<SessionReport, String> {
    let mut messages = vec![Message::user(
        json!([{ "type": "text", "text": task.goal }]),
    )];
    let mut tool_calls = 0_u32;
    let mut last_text = String::new();

    for turn in 1..=task.max_turns {
        let reply: TurnReply = ctx
            .execute_activity(
                &claude_turn_info(),
                TurnRequest {
                    messages: messages.clone(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        messages.push(Message::assistant(reply.content.clone()));
        if !reply.text.is_empty() {
            last_text = reply.text.clone();
        }

        // A turn cut short by the output cap is not an answer. It ends the
        // session under its own name, so incomplete work never reads as a clean
        // finish. Its tool calls are dropped too: a truncated turn can carry a
        // partial `tool_use` block, which is not safe to run.
        if reply.stop_reason == STOP_MAX_TOKENS {
            return Ok(report(&last_text, turn, tool_calls, STOP_MAX_TOKENS));
        }
        if reply.stop_reason == STOP_REFUSAL {
            return Ok(report(&reply.text, turn, tool_calls, STOP_REFUSAL));
        }
        if reply.tool_calls.is_empty() {
            // Only a real `end_turn` reports success. Any other stop reason
            // ends the session under its own name.
            return Ok(report(&last_text, turn, tool_calls, &reply.stop_reason));
        }

        // Every tool_use block of one assistant turn must be answered in ONE
        // user message. A split teaches the model to stop calling tools in
        // parallel, so the results are collected first and pushed together.
        let mut results = Vec::with_capacity(reply.tool_calls.len());
        for call in reply.tool_calls {
            tool_calls += 1;
            let outcome = if tools::needs_approval(&call.name) {
                gated_call(ctx, &task, &call).await?
            } else {
                run_tool_call(ctx, &call).await?
            };
            results.push(tool_result_block(&call.id, &outcome));
        }
        messages.push(Message::user(Value::Array(results)));
    }

    Ok(report(&last_text, task.max_turns, tool_calls, "max_turns"))
}

/// Run one tool call that a human must release first.
///
/// The wait races this call's own approval signal against a durable deadline
/// timer. A missing decision denies the call, which keeps an unattended daemon
/// moving. The signal name carries the tool-use id, so a decision meant for an
/// earlier call cannot release this one.
async fn gated_call(
    ctx: &WorkflowContext,
    task: &SessionTask,
    call: &ToolCall,
) -> Result<ToolOutcome, String> {
    let decision: Option<ApprovalDecision> = ctx
        .receive_signal_timeout(
            &approval_signal(&call.id),
            Duration::from_secs(task.approval_timeout_secs),
        )
        .await
        .map_err(|e| e.to_string())?;

    match decision {
        Some(d) if d.approved => run_tool_call(ctx, call).await,
        Some(d) => Ok(ToolOutcome::error(format!(
            "denied by the operator: {}",
            d.note.unwrap_or_else(|| "no reason given".to_string())
        ))),
        None => Ok(ToolOutcome::error(format!(
            "denied: no approval arrived within {}s",
            task.approval_timeout_secs
        ))),
    }
}

/// Execute one tool call as a durable activity.
async fn run_tool_call(ctx: &WorkflowContext, call: &ToolCall) -> Result<ToolOutcome, String> {
    ctx.execute_activity(&run_tool_info(), call.clone())
        .await
        .map_err(|e| e.to_string())
}

/// Build the `tool_result` block the next request carries.
fn tool_result_block(tool_use_id: &str, outcome: &ToolOutcome) -> Value {
    json!({
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": outcome.output,
        "is_error": outcome.is_error,
    })
}

/// Assemble the operator-facing report.
fn report(answer: &str, turns: u32, tool_calls: u32, stop: &str) -> SessionReport {
    SessionReport {
        answer: answer.to_string(),
        turns,
        tool_calls,
        stop: stop.to_string(),
    }
}

// The two `#[activity]` functions below supply the macro-generated `*_info()`
// companions: the activity name plus the declared defaults. This backend runs
// the synchronous closure registered against that info, so the async bodies
// here never execute. See `claude::activity_body` and `tools::activity_body`.

/// One Messages API request.
///
/// The budget is generous because one turn with adaptive thinking can run for
/// minutes. A transient failure (a rate limit, a 5xx, a dropped connection)
/// retries with backoff; a rejected request fails the attempt at once.
#[activity(
    start_to_close = "15m",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn claude_turn(
    _ctx: &ActivityContext,
    _request: TurnRequest,
) -> Result<TurnReply, String> {
    Ok(TurnReply::default())
}

/// One local tool call.
///
/// A tool body is deterministic and cheap, so a single attempt is enough. A
/// failure is reported to the model as a `tool_result`, not as an activity
/// error, so the loop keeps its history clean.
#[activity(start_to_close = "60s")]
pub async fn run_tool(_ctx: &ActivityContext, _call: ToolCall) -> Result<ToolOutcome, String> {
    Ok(ToolOutcome::default())
}
