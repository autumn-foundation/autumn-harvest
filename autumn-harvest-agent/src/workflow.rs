//! The agent loop as a workflow.
//!
//! The loop is ordinary Rust. Durability comes from the awaits:
//!
//! - [`agent_model_turn`] — one model call and the policy decisions.
//! - [`agent_tool_call`] — one tool call.
//! - an approval signal with a deadline, for a call the policy gates.
//! - [`agent_memory_snapshot`] — the frozen memory of a segment.
//! - [`agent_deliver`] — one report to the delivery.
//! - a durable timer, before a follow-up segment.
//!
//! Each await records its result in history. Replay reads the result back, so
//! a restart does not pay for a model call again, run a tool again, or send a
//! report twice. The transcript is a projection of history, not state the
//! worker must hold.
//!
//! The loop reads no clock and no state outside its input and the recorded
//! results. That keeps replay deterministic.

use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;

use crate::approval::{Approval, Decision, approval_signal, await_decision};
use crate::bounds::{exceeds_bytes, json_len};
use crate::delivery::{Report, ReportSource, report_text};
use crate::followup::{self, FOLLOWUP_TOOL, Planned};
use crate::harness::AgentHarness;
use crate::heartbeat::{HeartbeatTask, agent_heartbeat_info};
use crate::loop_guard::{LoopTracker, LoopVerdict, warning_note};
use crate::memory::MemoryScope;
use crate::message::{ChatMessage, ChatRole, ContentPart, RunId, StopReason, TokenUsage, ToolCall};
use crate::policy::ToolDecision;
use crate::types::{
    AgentReport, AgentStop, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
    without_system,
};

/// The registered workflow name.
pub const WORKFLOW_NAME: &str = "agent_loop";

/// The mutable state of one segment. Every field is rebuilt from history on
/// replay.
struct Progress {
    run_id: String,
    messages: Vec<ChatMessage>,
    usage: TokenUsage,
    steps: u32,
    /// Tool rounds of the earlier segments. Approval names use the global
    /// step, so a name never repeats across follow-up segments.
    step_base: u32,
    tool_calls: u32,
    last_text: String,
    tracker: LoopTracker,
    /// The follow-up that this segment booked.
    followup: Option<Planned>,
    /// Follow-ups that ran before this segment.
    chain: u32,
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
            followups: self.chain,
        }
    }
}

/// The agent loop.
///
/// It asks the model for a turn. A turn with no tool calls ends the segment.
/// For a turn with tool calls, the loop runs each call that the policy
/// allows. It waits for a decision on each call that the policy gates. It
/// answers all the calls in one tool message, then asks the model again.
///
/// A segment that completed with a booked follow-up waits on a durable timer.
/// Then a new segment starts in the same conversation with the follow-up
/// prompt. The report of the last segment is the workflow output.
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
    drive(ctx, &task, ReportSource::Run)
        .await
        .map(|(report, _)| report)
}

/// Run every segment of `task`. Returns the last report and the number of
/// reports delivered.
///
/// `source` names the first segment in a delivered report. Each follow-up
/// segment is a [`ReportSource::Followup`].
pub(crate) async fn drive(
    ctx: &WorkflowContext,
    task: &AgentTask,
    source: ReportSource,
) -> Result<(AgentReport, u32), String> {
    let run_id = ctx.workflow_id().to_owned();
    // A system message inside the history is dropped. Only `task.system`
    // reaches the model as the system prompt.
    let mut history: Vec<ChatMessage> = task
        .history
        .iter()
        .filter(|message| message.role != ChatRole::System)
        .cloned()
        .collect();
    let mut input = task.input.clone();
    let mut chain = 0_u32;
    let mut step_base = 0_u32;
    let mut delivered = 0_u32;
    loop {
        let (report, followup) =
            run_segment(ctx, task, &run_id, history, input, chain, step_base).await?;
        if task.deliver {
            let source = if chain == 0 {
                source
            } else {
                ReportSource::Followup
            };
            if deliver(ctx, task, &run_id, source, &report).await {
                delivered += 1;
            }
        }
        // A follow-up runs only after a segment that completed.
        let Some(next) = followup.filter(|_| report.stop == AgentStop::Completed) else {
            return Ok((report, delivered));
        };
        ctx.timer(&format!("agent_followup:{chain}"), next.delay_secs)
            .await
            .map_err(|e| e.to_string())?;
        chain += 1;
        step_base += report.steps_used;
        history = report.messages;
        input = next.prompt;
    }
}

/// Send one report, when the run has something to say.
///
/// A failed delivery never fails the run. Its outcome is recorded, so replay
/// gives the same answer. Returns `true` when the delivery took the report.
async fn deliver(
    ctx: &WorkflowContext,
    task: &AgentTask,
    run_id: &str,
    source: ReportSource,
    report: &AgentReport,
) -> bool {
    let Some(text) = report_text(report.stop, &report.text) else {
        return false;
    };
    let report = Report {
        source,
        run_id: RunId::new(run_id),
        session_id: task.session_id.clone(),
        text,
        stop: report.stop,
    };
    let sent: HarvestResult<()> = ctx.execute_activity(&agent_deliver_info(), report).await;
    sent.is_ok()
}

/// Run one segment: model turns and tool rounds until the model answers or a
/// bound ends it. Returns its report and the follow-up it booked.
async fn run_segment(
    ctx: &WorkflowContext,
    task: &AgentTask,
    run_id: &str,
    history: Vec<ChatMessage>,
    input: String,
    chain: u32,
    step_base: u32,
) -> Result<(AgentReport, Option<Planned>), String> {
    let system = system_prompt(ctx, task).await?;
    let mut messages = Vec::with_capacity(history.len() + 2);
    if let Some(system) = system {
        messages.push(ChatMessage::text(ChatRole::System, system));
    }
    messages.extend(history);
    messages.push(ChatMessage::text(ChatRole::User, input));
    let extra_tools = task
        .followups
        .map(|_| vec![followup::definition()])
        .unwrap_or_default();
    let mut progress = Progress {
        run_id: run_id.to_owned(),
        messages,
        usage: TokenUsage::default(),
        steps: 0,
        step_base,
        tool_calls: 0,
        last_text: String::new(),
        tracker: LoopTracker::default(),
        followup: None,
        chain,
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
            read_only: task.read_only,
            memory_scope: task.memory_scope.clone(),
            extra_tools: extra_tools.clone(),
        };
        // A request the engine would refuse is not sent. The refusal is not
        // retryable, so it would fail the run after earlier work was paid for.
        if exceeds_bytes(&request, task.request_cap()) {
            return Ok((progress.report(AgentStop::TranscriptFull), None));
        }
        let turn: ModelTurn = ctx
            .execute_activity(&agent_model_turn_info(), request)
            .await
            .map_err(|e| e.to_string())?;

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
            let followup = progress.followup.take();
            return Ok((progress.report(stop), followup));
        }
        // A turn with calls that ends here is NOT added to the transcript. A
        // tool call with no result would make the transcript invalid for the
        // next run that continues it.
        //
        // A turn cut at the output cap can hold a partial call. It never runs.
        if turn.stop == StopReason::MaxTokens {
            return Ok((progress.report(AgentStop::OutputCapped), None));
        }
        if over_budget {
            return Ok((progress.report(AgentStop::TokensExhausted), None));
        }
        if progress.steps >= task.max_steps {
            return Ok((progress.report(AgentStop::StepsExhausted), None));
        }

        let step = progress.steps;
        progress.steps += 1;
        progress.push_assistant(&turn);

        match run_round(ctx, &mut progress, task, step, &turn, calls).await? {
            RoundEnd::Continue => {}
            RoundEnd::Full => return Ok((progress.report(AgentStop::TranscriptFull), None)),
            RoundEnd::Loop => return Ok((progress.report(AgentStop::LoopDetected), None)),
        }
    }
}

/// The system prompt of one segment: the task prompt and the memory
/// snapshot.
///
/// The snapshot is read once per segment, by an activity, so the prompt does
/// not change inside the segment and replay reads the recorded snapshot.
async fn system_prompt(ctx: &WorkflowContext, task: &AgentTask) -> Result<Option<String>, String> {
    let Some(scope) = &task.memory_scope else {
        return Ok(task.system.clone());
    };
    let snapshot: String = ctx
        .execute_activity(&agent_memory_snapshot_info(), scope.clone())
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(match &task.system {
        Some(system) => format!("{system}\n\n{snapshot}"),
        None => snapshot,
    }))
}

/// How one tool round ended.
enum RoundEnd {
    /// Every call has an answer. Ask the model again.
    Continue,
    /// The results passed the request cap.
    Full,
    /// The loop guard stopped the run.
    Loop,
}

/// Answer every call of one round in ONE tool message.
///
/// The results can pass the request cap. No more calls run then, because the
/// results cannot fit any request. No more calls run after the loop guard
/// stops the run either. Each call still gets an answer, so the transcript
/// stays valid and shows what ran.
async fn run_round(
    ctx: &WorkflowContext,
    progress: &mut Progress,
    task: &AgentTask,
    step: u32,
    turn: &ModelTurn,
    calls: Vec<ToolCall>,
) -> Result<RoundEnd, String> {
    let cap = task.request_cap();
    let mut results = Vec::with_capacity(calls.len());
    let mut spent = 0_u64;
    let mut full = false;
    let mut looped = false;
    for (position, call) in calls.into_iter().enumerate() {
        progress.tool_calls += 1;
        let call_id = call.id.clone();
        let content = if full {
            ToolOutcome::error(NOT_RUN_FULL).content
        } else if looped {
            ToolOutcome::error(NOT_RUN_LOOP).content
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
            let name = call.name.clone();
            let arguments = call.arguments.clone();
            let mut content = decide(ctx, progress, task, step, position, call, decision)
                .await?
                .content;
            match progress
                .tracker
                .record(&task.loop_guard, &name, &arguments, &content)
            {
                LoopVerdict::Ok => {}
                LoopVerdict::Warn(repeats) => content.push_str(&warning_note(&name, repeats)),
                LoopVerdict::Stop(_) => looped = true,
            }
            let size = json_len(&content);
            if spent.saturating_add(size) > cap {
                full = true;
                ToolOutcome::error(DROPPED_FULL).content
            } else {
                spent = spent.saturating_add(size);
                content
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
    Ok(if full {
        RoundEnd::Full
    } else if looped {
        RoundEnd::Loop
    } else {
        RoundEnd::Continue
    })
}

/// The answer to a call whose result passed the request cap.
const DROPPED_FULL: &str =
    "the call ran, but its result was dropped: the results passed the request cap";

/// The answer to a call that did not run because the round was full.
const NOT_RUN_FULL: &str = "the call did not run: the results passed the request cap";

/// The answer to a call that did not run because the loop guard stopped the
/// run.
const NOT_RUN_LOOP: &str = "the call did not run: the loop guard stopped the run";

/// Act on one recorded policy decision.
async fn decide(
    ctx: &WorkflowContext,
    progress: &mut Progress,
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
    progress: &mut Progress,
    task: &AgentTask,
    step: u32,
    position: usize,
    mut call: ToolCall,
) -> Result<ToolOutcome, String> {
    let decision = await_decision::<Approval>(
        ctx,
        &approval_signal(progress.step_base + step, position, &call.id),
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

/// Run one tool call.
///
/// The follow-up tool runs in the workflow: it only books a timer. Every
/// other call is a durable activity.
async fn run_tool(
    ctx: &WorkflowContext,
    progress: &mut Progress,
    task: &AgentTask,
    step: u32,
    call: ToolCall,
) -> Result<ToolOutcome, String> {
    if let Some(settings) = task.followups.filter(|_| call.name == FOLLOWUP_TOOL) {
        let planned = followup::plan(
            &call.arguments,
            &settings,
            progress.chain,
            progress.followup.is_some(),
        );
        return Ok(match planned {
            Ok(planned) => {
                let outcome = ToolOutcome::ok(
                    serde_json::json!({
                        "scheduled": true,
                        "in_minutes": planned.delay_secs / 60,
                    })
                    .to_string(),
                );
                progress.followup = Some(planned);
                outcome
            }
            Err(reason) => ToolOutcome::error(&reason),
        });
    }
    ctx.execute_activity(
        &agent_tool_call_info(),
        ToolCallRequest {
            run_id: progress.run_id.clone(),
            session_id: task.session_id.clone(),
            step: progress.step_base + step,
            call,
            memory_scope: task.memory_scope.clone(),
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

/// The memory snapshot of one segment, as an activity.
///
/// # Errors
///
/// Returns an error when no store is installed or the store cannot read.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_memory_snapshot(
    ctx: &ActivityContext,
    scope: MemoryScope,
) -> Result<String, String> {
    harness(ctx)?.memory_snapshot(scope).await
}

/// One report to the delivery, as an activity.
///
/// # Errors
///
/// Returns the delivery error. The loop records it and goes on.
#[activity(
    start_to_close = "2m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(2))
)]
pub async fn agent_deliver(ctx: &ActivityContext, report: Report) -> Result<(), String> {
    harness(ctx)?.deliver(report).await
}

/// The heartbeat precheck, as an activity.
///
/// # Errors
///
/// Returns an error only when no harness is installed.
#[activity(start_to_close = "1m", retry = RetryPolicy::fixed(1, Duration::ZERO))]
pub async fn agent_precheck(ctx: &ActivityContext, task: HeartbeatTask) -> Result<bool, String> {
    harness(ctx)?.precheck_tick(task).await
}

/// The workflows to register on a `HarvestBuilder`: `agent_loop` and
/// `agent_heartbeat`.
#[must_use]
pub fn workflows() -> Vec<WorkflowInfo> {
    vec![agent_loop_info(), agent_heartbeat_info()]
}

/// The activities to register on a `HarvestBuilder`.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    vec![
        agent_model_turn_info(),
        agent_tool_call_info(),
        agent_memory_snapshot_info(),
        agent_deliver_info(),
        agent_precheck_info(),
    ]
}
