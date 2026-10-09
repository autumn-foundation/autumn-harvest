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

use crate::message::{ChatMessage, ChatRole, ContentPart, StopReason, TokenUsage, ToolCall};
use crate::policy::ToolDecision;
use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;

use crate::approval::{Approval, Decision, approval_signal, await_decision};
use crate::bounds::{exceeds_bytes, json_len};
use crate::harness::AgentHarness;
use crate::types::{
    AgentReport, AgentStop, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
    without_system,
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
    /// Add one model turn to the transcript. Its text becomes the answer.
    fn push_assistant(&mut self, turn: &ModelTurn) {
        let text = turn.text();
        if !text.trim().is_empty() {
            self.last_text = text;
        }
        self.messages.push(ChatMessage {
            role: ChatRole::Assistant,
            content: turn.content.clone(),
        });
    }

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
    // A system message inside the history is dropped.
    // Only `task.system` reaches the model as the system prompt.
    messages.extend(
        task.history
            .iter()
            .filter(|message| message.role != ChatRole::System)
            .cloned(),
    );
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
        if exceeds_bytes(&request, task.request_cap()) {
            return Ok(progress.report(AgentStop::TranscriptFull));
        }
        let turn: ModelTurn = match ctx
            .execute_activity(&agent_model_turn_info(), request)
            .await
        {
            Ok(turn) => turn,
            // A spent LLM budget is a bound, so the run ends normally.
            Err(error) if LlmBudgetExceeded::is_refusal(&error) => {
                return Ok(progress.report(AgentStop::BudgetExceeded));
            }
            Err(error) => return Err(error.to_string()),
        };

        progress.usage = progress.usage.saturating_add(turn.usage);
        let over_budget = task
            .max_total_tokens
            .is_some_and(|max| progress.usage.total() > max);
        let calls = turn.calls();

        if calls.is_empty() {
            let stop = if over_budget {
                AgentStop::TokensExhausted
            } else if turn.stop == StopReason::MaxTokens {
                AgentStop::OutputCapped
            } else {
                AgentStop::Completed
            };
            progress.push_assistant(&turn);
            return Ok(progress.report(stop));
        }
        // A turn with calls that ends here is NOT added to the transcript. A
        // tool call with no result would make the transcript invalid for the
        // next run that continues it.
        //
        // A turn cut at the output cap can hold a partial call. It never runs.
        if turn.stop == StopReason::MaxTokens {
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
        progress.push_assistant(&turn);

        if run_round(ctx, &mut progress, &task, step, &turn, calls).await? {
            return Ok(progress.report(AgentStop::TranscriptFull));
        }
    }
}

/// Answer every call of one round in ONE tool message.
///
/// Returns `true` when the results passed the request cap. No more calls run
/// then, because the results cannot fit any request. Each call still gets an
/// answer, so the transcript stays valid and shows what ran.
async fn run_round(
    ctx: &WorkflowContext,
    progress: &mut Progress,
    task: &AgentTask,
    step: u32,
    turn: &ModelTurn,
    calls: Vec<ToolCall>,
) -> Result<bool, String> {
    let cap = task.request_cap();
    let mut results = Vec::with_capacity(calls.len());
    let mut spent = 0_u64;
    let mut full = false;
    for (position, call) in calls.into_iter().enumerate() {
        progress.tool_calls += 1;
        let call_id = call.id.clone();
        let content = if full {
            ToolOutcome::error(NOT_RUN_FULL).content
        } else {
            // A recorded turn always holds one decision per call. A missing
            // one denies the call rather than guess.
            let decision =
                turn.decisions
                    .get(position)
                    .cloned()
                    .unwrap_or_else(|| ToolDecision::Deny {
                        reason: "no recorded decision".to_owned(),
                    });
            let outcome = decide(ctx, progress, task, step, position, call, decision).await?;
            let size = json_len(&outcome.content);
            if spent.saturating_add(size) > cap {
                full = true;
                ToolOutcome::error(DROPPED_FULL).content
            } else {
                spent = spent.saturating_add(size);
                outcome.content
            }
        };
        results.push(ContentPart::ToolResult {
            tool_call_id: call_id,
            content,
        });
    }
    progress.messages.push(ChatMessage {
        role: ChatRole::Tool,
        content: results,
    });
    Ok(full)
}

/// The answer to a call whose result passed the request cap.
const DROPPED_FULL: &str =
    "the call ran, but its result was dropped: the results passed the request cap";

/// The answer to a call that did not run because the round was full.
const NOT_RUN_FULL: &str = "the call did not run: the results passed the request cap";

/// Act on one recorded policy decision.
async fn decide(
    ctx: &WorkflowContext,
    progress: &Progress,
    task: &AgentTask,
    step: u32,
    position: usize,
    call: ToolCall,
    decision: ToolDecision,
) -> Result<ToolOutcome, String> {
    match decision {
        ToolDecision::Allow => run_tool(ctx, progress, task, step, call).await,
        ToolDecision::Deny { reason } => {
            Ok(ToolOutcome::error(&format!("denied by policy: {reason}")))
        }
        ToolDecision::RequireApproval { .. } => {
            gated_call(ctx, progress, task, step, position, call).await
        }
    }
}

/// Wait for a decision on one gated call, then run it or refuse it.
///
/// The wait races the call's own approval signal against a durable deadline.
/// A missing decision denies the call, so an unattended run keeps moving. A
/// payload that is not an `Approval` denies the call too. It never fails the
/// run.
async fn gated_call(
    ctx: &WorkflowContext,
    progress: &Progress,
    task: &AgentTask,
    step: u32,
    position: usize,
    mut call: ToolCall,
) -> Result<ToolOutcome, String> {
    let decision = await_decision::<Approval>(
        ctx,
        &approval_signal(step, position, &call.id),
        Duration::from_secs(task.approval_timeout_secs),
    )
    .await?;
    match decision {
        Decision::Decided(Approval::Approve) => run_tool(ctx, progress, task, step, call).await,
        Decision::Decided(Approval::Edit { arguments }) => {
            call.arguments = arguments;
            run_tool(ctx, progress, task, step, call).await
        }
        Decision::Decided(Approval::Reject { reason }) => Ok(ToolOutcome::error(&format!(
            "a reviewer rejected this call: {reason}"
        ))),
        Decision::Unreadable(why) => Ok(ToolOutcome::error(&format!(
            "denied: the decision was not a readable approval ({why})"
        ))),
        Decision::TimedOut => Ok(ToolOutcome::error(&format!(
            "denied: no approval arrived within {}s",
            task.approval_timeout_secs
        ))),
    }
}

/// Run one tool call as a durable activity.
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
/// A rate limit, a transport fault, an outage or a timeout retries with
/// backoff. Any other failure fails the call at once.
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
    harness(ctx)?
        .metered_turn(
            request,
            || ctx.check_llm_budget(),
            |usage| async move {
                ctx.record_llm_usage(&usage)
                    .await
                    .map_err(|error| error.to_string())
            },
        )
        .await
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

/// The workflow to register on a `HarvestBuilder`: `agent_loop`.
#[must_use]
pub fn workflows() -> Vec<WorkflowInfo> {
    vec![agent_loop_info()]
}

/// The activities to register on a `HarvestBuilder`: `agent_model_turn` and
/// `agent_tool_call`.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    vec![agent_model_turn_info(), agent_tool_call_info()]
}
