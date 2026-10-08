//! The agent loop as a workflow.
//!
//! The loop is ordinary Rust. Durability comes from three awaits:
//!
//! - [`agent_model_turn`] — one model call and the policy decisions.
//! - [`agent_tool_call`] — one tool call.
//! - an approval signal with a deadline, for a call the policy gates.
//!
//! Each await records its result in history. Replay reads the result back, so
//! a restart does not pay for a model call again or run a tool again. The
//! transcript is a projection of history, not state the worker must hold.
//!
//! The loop reads no clock and no state outside its input and the recorded
//! results. That keeps replay deterministic.

use std::time::Duration;

use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_plugin_agent::{
    Approval, ChatMessage, ChatRole, ContentPart, TokenUsage, ToolCall, ToolDecision,
};

use crate::approval::approval_signal;
use crate::bounds::{exceeds_activity_input, json_len};
use crate::harness::AgentHarness;
use crate::types::{
    AgentReport, AgentStop, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
    TurnStop, without_system,
};

/// The registered workflow name.
pub const WORKFLOW_NAME: &str = "agent_loop";

/// The mutable state of one run. Every field is rebuilt from history on
/// replay.
struct Progress {
    run_id: String,
    messages: Vec<ChatMessage>,
    usage: TokenUsage,
    steps: u32,
    tool_calls: u32,
    last_text: String,
}

impl Progress {
    fn report(&self, stop: AgentStop) -> AgentReport {
        AgentReport {
            stop,
            text: self.last_text.clone(),
            steps_used: self.steps,
            tool_calls: self.tool_calls,
            usage: self.usage,
            messages: without_system(&self.messages),
        }
    }
}

/// The agent loop.
///
/// It asks the model for a turn. A turn with no tool calls ends the run.
/// For a turn with tool calls, the loop runs each call that the policy
/// allows. It waits for a decision on each call that the policy gates. It
/// answers all the calls in one tool message, then asks the model again.
///
/// The run ends under a named [`AgentStop`] when a bound is reached. A bound
/// is a normal end, not an error.
///
/// # Errors
///
/// Returns an error when an activity fails for good, for example on a
/// non-retryable provider failure.
#[workflow]
pub async fn agent_loop(ctx: &WorkflowContext, task: AgentTask) -> Result<AgentReport, String> {
    let mut messages = Vec::with_capacity(task.history.len() + 2);
    if let Some(system) = &task.system {
        messages.push(ChatMessage::text(ChatRole::System, system.clone()));
    }
    messages.extend(task.history.iter().cloned());
    messages.push(ChatMessage::text(ChatRole::User, task.input.clone()));
    let mut progress = Progress {
        run_id: ctx.workflow_id().to_owned(),
        messages,
        usage: TokenUsage::default(),
        steps: 0,
        tool_calls: 0,
        last_text: String::new(),
    };

    loop {
        let request = ModelTurnRequest {
            run_id: progress.run_id.clone(),
            session_id: task.session_id.clone(),
            steps_used: progress.steps,
            max_steps: task.max_steps,
            usage: progress.usage,
            messages: progress.messages.clone(),
            max_output_tokens: task.max_output_tokens,
        };
        // A request the engine would refuse is not sent. The refusal is not
        // retryable, so it would fail the run after earlier work was paid for.
        if exceeds_activity_input(&request) {
            return Ok(progress.report(AgentStop::TranscriptFull));
        }
        let turn: ModelTurn = ctx
            .execute_activity(&agent_model_turn_info(), request)
            .await
            .map_err(|e| e.to_string())?;

        progress.usage = progress.usage.saturating_add(turn.usage);
        let text = turn.text();
        if !text.trim().is_empty() {
            progress.last_text = text;
        }
        let over_budget = task
            .max_total_tokens
            .is_some_and(|max| progress.usage.total() > max);

        if turn.calls.is_empty() {
            let stop = if over_budget {
                AgentStop::TokensExhausted
            } else if turn.stop == TurnStop::MaxTokens {
                AgentStop::OutputCapped
            } else {
                AgentStop::Completed
            };
            progress.messages.push(assistant(turn.content));
            return Ok(progress.report(stop));
        }
        // A turn with calls that ends here is NOT added to the transcript. A
        // tool call with no result would make the transcript invalid for the
        // next run that continues it.
        //
        // A turn cut at the output cap can hold a partial call. It never runs.
        if turn.stop == TurnStop::MaxTokens {
            return Ok(progress.report(AgentStop::OutputCapped));
        }
        if over_budget {
            return Ok(progress.report(AgentStop::TokensExhausted));
        }
        if progress.steps >= task.max_steps {
            return Ok(progress.report(AgentStop::StepsExhausted));
        }

        let step = progress.steps;
        progress.steps += 1;
        progress.messages.push(assistant(turn.content));

        // Every call of one turn is answered in ONE tool message.
        let mut results = Vec::with_capacity(turn.calls.len());
        // The results alone can pass the cap a request may carry. They cannot
        // fit any request then, so the run stops before more calls run.
        let mut spent = 0_u64;
        for (position, gated) in turn.calls.into_iter().enumerate() {
            progress.tool_calls += 1;
            let call_id = gated.call.id.clone();
            let outcome = match gated.decision {
                ToolDecision::Allow => run_tool(ctx, &progress, &task, step, gated.call).await?,
                ToolDecision::Deny { reason } => {
                    ToolOutcome::error(&format!("denied by policy: {reason}"))
                }
                ToolDecision::RequireApproval { .. } => {
                    gated_call(ctx, &progress, &task, step, position, gated.call).await?
                }
            };
            let part = ContentPart::ToolResult {
                tool_call_id: call_id,
                content: outcome.content,
            };
            spent = spent.saturating_add(json_len(&part));
            results.push(part);
            if spent > DEFAULT_MAX_ACTIVITY_INPUT_BYTES {
                // The cut round leaves the transcript, so it stays valid.
                progress.messages.pop();
                return Ok(progress.report(AgentStop::TranscriptFull));
            }
        }
        progress.messages.push(ChatMessage {
            role: ChatRole::Tool,
            content: results,
        });
    }
}

/// Wait for a decision on one gated call, then run it or refuse it.
///
/// The wait races the call's own approval signal against a durable deadline.
/// A missing decision denies the call, so an unattended run keeps moving.
async fn gated_call(
    ctx: &WorkflowContext,
    progress: &Progress,
    task: &AgentTask,
    step: u32,
    position: usize,
    mut call: ToolCall,
) -> Result<ToolOutcome, String> {
    let decision: Option<Approval> = ctx
        .receive_signal_timeout(
            &approval_signal(step, position, &call.id),
            Duration::from_secs(task.approval_timeout_secs),
        )
        .await
        .map_err(|e| e.to_string())?;
    match decision {
        Some(Approval::Approve) => run_tool(ctx, progress, task, step, call).await,
        Some(Approval::Edit { arguments }) => {
            call.arguments = arguments;
            run_tool(ctx, progress, task, step, call).await
        }
        Some(Approval::Reject { reason }) => Ok(ToolOutcome::error(&format!(
            "a reviewer rejected this call: {reason}"
        ))),
        None => Ok(ToolOutcome::error(&format!(
            "denied: no approval arrived within {}s",
            task.approval_timeout_secs
        ))),
    }
}

/// Progress one tool call as a durable activity.
async fn run_tool(
    ctx: &WorkflowContext,
    progress: &Progress,
    task: &AgentTask,
    step: u32,
    call: ToolCall,
) -> Result<ToolOutcome, String> {
    ctx.execute_activity(
        &agent_tool_call_info(),
        ToolCallRequest {
            run_id: progress.run_id.clone(),
            session_id: task.session_id.clone(),
            step,
            call,
        },
    )
    .await
    .map_err(|e| e.to_string())
}

const fn assistant(content: Vec<ContentPart>) -> ChatMessage {
    ChatMessage {
        role: ChatRole::Assistant,
        content,
    }
}

/// The installed harness, or an error that names the fix.
fn harness(ctx: &ActivityContext) -> Result<&AgentHarness, String> {
    ctx.state::<AgentHarness>().ok_or_else(|| {
        ActivityFailure::non_retryable(
            "AgentHarnessMissing",
            "no AgentHarness is installed: pass one to HarvestBuilder::state",
        )
        .into_error_payload()
    })
}

/// One model call, as an activity.
///
/// The budget is wide, because one turn of a large model can run for minutes.
/// A rate limit or a transport fault retries with backoff. Any other failure
/// fails the call at once.
///
/// # Errors
///
/// Returns the provider failure as an activity error payload.
#[activity(
    start_to_close = "15m",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn agent_model_turn(
    ctx: &ActivityContext,
    request: ModelTurnRequest,
) -> Result<ModelTurn, String> {
    harness(ctx)?.model_turn(request).await
}

/// One tool call, as an activity.
///
/// It runs once. A tool failure is a result the model reads, not an activity
/// error. A retry would run a side effect twice for no gain.
///
/// # Errors
///
/// Returns an error only when no harness is installed.
#[activity(start_to_close = "10m", retry = RetryPolicy::fixed(1, Duration::ZERO))]
pub async fn agent_tool_call(
    ctx: &ActivityContext,
    request: ToolCallRequest,
) -> Result<ToolOutcome, String> {
    harness(ctx)?.tool_call(request).await
}
