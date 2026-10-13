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

use std::collections::BTreeSet;
use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;

use crate::approval::{Approval, Decision, approval_signal, await_decision};
use crate::bounds::{exceeds_bytes, json_len};
use crate::delivery::{Report, ReportSource, report_text};
use crate::followup::{self, FOLLOWUP_TOOL, Planned};
use crate::harness::{AgentHarness, CacheTenant, DEFAULT_TOOL_OUTPUT_LIMIT, truncate};
use crate::heartbeat::{HeartbeatTask, agent_heartbeat_info};
use crate::loop_guard::{LoopTracker, LoopVerdict, warning_note};
use crate::memory::MemoryScope;
use crate::mesh::{
    DELEGATE_TOOL, DelegateArgs, DelegationRecord, INVALID_NAME, Mandate, Peer, Refusal,
    delegate_definition, valid_name,
};
use crate::message::{ChatMessage, ChatRole, ContentPart, RunId, StopReason, TokenUsage, ToolCall};
use crate::policy::ToolDecision;
use crate::types::{
    AgentReport, AgentStop, AgentTask, ModelTurn, ModelTurnRequest, RunIdSource, ToolCallRequest,
    ToolOutcome, without_system,
};

/// The registered workflow name.
pub const WORKFLOW_NAME: &str = "agent_loop";

/// The state of one agent run, carried across its segments. Every field is
/// rebuilt from history on replay.
struct Progress {
    run_id: String,
    messages: Vec<ChatMessage>,
    /// Tokens of the whole run, earlier segments and delegations included.
    usage: TokenUsage,
    /// Tool rounds of this segment. `max_steps` bounds it.
    steps: u32,
    /// Tool rounds of the earlier segments. Tool steps add it, so they never
    /// repeat across segments.
    step_base: u32,
    /// Tool calls of the whole run.
    tool_calls: u32,
    /// Tool rounds of every agent under this one. They do not count against
    /// `max_steps`, which bounds each agent alone.
    delegated_steps: u32,
    /// Tool calls of every agent under this one.
    delegated_tool_calls: u32,
    last_text: String,
    /// The segment ended on a turn with no tool calls, so `last_text` is its
    /// answer.
    answered: bool,
    /// The loop guard history of the whole run.
    tracker: LoopTracker,
    /// The follow-up that this segment booked.
    followup: Option<Planned>,
    /// Follow-ups that ran before this segment.
    chain: u32,
}

impl Progress {
    /// A run with `run_id` that continues `messages`.
    fn new(run_id: String, messages: Vec<ChatMessage>) -> Self {
        Self {
            run_id,
            messages,
            usage: TokenUsage::default(),
            steps: 0,
            step_base: 0,
            tool_calls: 0,
            delegated_steps: 0,
            delegated_tool_calls: 0,
            last_text: String::new(),
            answered: false,
            tracker: LoopTracker::default(),
            followup: None,
            chain: 0,
        }
    }

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

    fn report(&self, stop: AgentStop, tree: &Tree) -> AgentReport {
        AgentReport {
            stop,
            text: self.last_text.clone(),
            steps_used: self
                .step_base
                .saturating_add(self.steps)
                .saturating_add(self.delegated_steps),
            tool_calls: self.tool_calls.saturating_add(self.delegated_tool_calls),
            usage: self.usage,
            messages: without_system(&self.messages),
            followups: self.chain,
            followup_dropped: false,
            delegations: tree.records.clone(),
            flow_labels: tree.exercised.clone(),
        }
    }
}

/// The state that every agent of one run tree shares. Every field is rebuilt
/// from history on replay.
///
/// The tree runs one step at a time inside one workflow, so a plain value is
/// enough. No two children can read it at once.
struct Tree {
    /// The run id of the root agent. A child run id adds an ordinal to it.
    root: String,
    /// Tool rounds of the whole tree. Approval names use it, so they are
    /// unique in the tree. With no delegation, it equals the round count of
    /// the root run, so the names stay as before.
    rounds: u32,
    /// Delegations that started in the tree.
    delegations: u32,
    /// The flow labels of the task, and of every call in the tree that the
    /// decision let run.
    exercised: BTreeSet<String>,
    /// Each delegation, in the order it ended.
    records: Vec<DelegationRecord>,
}

/// One agent of a run tree: its mandate, and the peers that its recorded
/// lookup found.
struct Agent {
    mandate: Mandate,
    peers: Vec<Peer>,
}

impl Agent {
    /// The `delegate` tool, when the agent has a peer to ask.
    fn delegate_tool(&self) -> Option<crate::message::ToolDefinition> {
        (!self.peers.is_empty()).then(|| delegate_definition(&self.peers))
    }
}

/// One agent run: its task and its place in the tree.
#[derive(Clone, Copy)]
struct Run<'a> {
    task: &'a AgentTask,
    agent: Option<&'a Agent>,
}

impl Run<'_> {
    /// The harness tools that the mandate permits. `None` means every tool.
    fn allowed_tools(&self) -> Option<BTreeSet<String>> {
        self.agent.and_then(|agent| agent.mandate.tools.clone())
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
/// prompt. The report covers the whole run. Its text is the answer of the
/// last segment.
///
/// A task with a [`Mandate`] that names peers gets the `delegate` tool. A
/// delegation runs the peer inline, as a sub-agent. See
/// [`mesh`](crate::mesh).
///
/// The run ends under a named [`AgentStop`] when a bound is reached. A bound
/// is a normal end, not an error.
///
/// # Errors
///
/// Returns an error when an activity fails for good, for example on a
/// non-retryable provider failure, or when the mandate is not valid.
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
    let run_id = match task.run_id_source {
        RunIdSource::ExecutionId => ctx.execution_id().to_string(),
        // A run recorded before the change keeps its recorded run id.
        RunIdSource::WorkflowId => ctx.workflow_id().to_owned(),
    };
    // A system message inside the history is dropped. Only `task.system`
    // reaches the model as the system prompt.
    let mut progress = Progress::new(run_id, without_system(&task.history));
    let mut tree = Tree {
        root: progress.run_id.clone(),
        rounds: 0,
        delegations: 0,
        exercised: task
            .flow_labels
            .iter()
            .chain(task.mandate.iter().flat_map(|mandate| &mandate.flow_labels))
            .cloned()
            .collect(),
        records: Vec::new(),
    };
    let agent = match &task.mandate {
        Some(mandate) => {
            mandate
                .validate()
                .map_err(|err| format!("the task mandate is not valid: {err}"))?;
            Some(discover(ctx, mandate).await?)
        }
        None => None,
    };
    let mut input = task.input.as_str();
    let mut delivered = 0_u32;
    loop {
        // No person is present when a follow-up wakes. It stays read-only
        // unless the app opts in.
        let woken = progress.chain > 0;
        let read_only = task.read_only
            || task
                .mandate
                .as_ref()
                .is_some_and(|mandate| mandate.read_only)
            || (woken
                && !task
                    .followups
                    .is_some_and(|settings| settings.allow_actions));
        let segment = AgentTask {
            read_only,
            unattended: task.unattended || woken,
            history: Vec::new(),
            ..task.clone()
        };
        let run = Run {
            task: &segment,
            agent: agent.as_ref(),
        };
        let stop = run_segment(ctx, run, &mut progress, &mut tree, input).await?;
        let mut report = progress.report(stop, &tree);
        if task.deliver {
            let source = if woken {
                ReportSource::Followup
            } else {
                source
            };
            let answer = if progress.answered {
                report.text.as_str()
            } else {
                ""
            };
            if let Some(text) = report_text(source, stop, answer) {
                let sent = Report {
                    source,
                    run_id: RunId::new(progress.run_id.clone()),
                    segment: progress.chain,
                    session_id: task.session_id.clone(),
                    text,
                    stop,
                };
                if deliver(ctx, sent).await? {
                    delivered = delivered.saturating_add(1);
                }
            }
        }
        // A follow-up runs only after a segment that completed.
        let next = match progress.followup.take() {
            Some(next) if stop == AgentStop::Completed => next,
            dropped => {
                report.followup_dropped = dropped.is_some();
                return Ok((report, delivered));
            }
        };
        ctx.timer(
            &format!("agent_followup:{}", progress.chain),
            next.delay_secs,
        )
        .await
        .map_err(|e| e.to_string())?;
        progress.chain = progress.chain.saturating_add(1);
        progress.step_base = progress.step_base.saturating_add(progress.steps);
        progress.steps = 0;
        progress.messages = report.messages;
        progress.last_text.clear();
        progress.answered = false;
        input = followup::WAKE_MESSAGE;
    }
}

/// Read the peers that `mandate` names, through one recorded activity.
///
/// A mandate with no peers needs no lookup, so it records nothing.
async fn discover(ctx: &WorkflowContext, mandate: &Mandate) -> Result<Agent, String> {
    let peers = if mandate.peers.is_empty() {
        Vec::new()
    } else {
        let names: Vec<String> = mandate.peers.iter().cloned().collect();
        ctx.execute_activity(&agent_discover_peers_info(), names)
            .await
            .map_err(|e| e.to_string())?
    };
    Ok(Agent {
        mandate: mandate.clone(),
        peers,
    })
}

/// Send one report.
///
/// A failed send never fails the run: the failure is recorded, so replay
/// gives the same answer. Returns `true` when the delivery took the report.
///
/// # Errors
///
/// Returns an engine error that is not a failed send, such as a cancel.
async fn deliver(ctx: &WorkflowContext, report: Report) -> Result<bool, String> {
    let sent: HarvestResult<()> = ctx.execute_activity(&agent_deliver_info(), report).await;
    match sent {
        Ok(()) => Ok(true),
        Err(
            HarvestError::ActivityFailed { .. }
            | HarvestError::Timeout { .. }
            | HarvestError::PayloadTooLarge { .. },
        ) => Ok(false),
        Err(other) => Err(other.to_string()),
    }
}

/// Run one segment: model turns and tool rounds until the model answers or a
/// bound ends it. Returns why the segment ended.
async fn run_segment(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    input: &str,
) -> Result<AgentStop, String> {
    let task = run.task;
    let system = system_prompt(ctx, task).await?;
    let mut messages = Vec::with_capacity(progress.messages.len() + 2);
    if let Some(system) = system {
        messages.push(ChatMessage::text(ChatRole::System, system));
    }
    messages.append(&mut progress.messages);
    messages.push(ChatMessage::text(ChatRole::User, input));
    progress.messages = messages;
    let extra_tools: Vec<_> = task
        .followups
        .map(|_| followup::definition())
        .into_iter()
        .chain(run.agent.and_then(Agent::delegate_tool))
        .collect();

    loop {
        // A delegation adds the child usage after a turn. A tree over its cap
        // makes no further call. A run with no delegation never starts a turn
        // over its cap, so this check changes nothing for it.
        if task
            .max_total_tokens
            .is_some_and(|max| progress.usage.total() > max)
        {
            return Ok(AgentStop::TokensExhausted);
        }
        let request = ModelTurnRequest {
            run_id: progress.run_id.clone(),
            session_id: task.session_id.clone(),
            steps_used: progress.steps,
            max_steps: task.max_steps,
            usage: progress.usage,
            messages: progress.messages.clone(),
            max_output_tokens: task.max_output_tokens,
            read_only: task.read_only,
            memory_scope: task.memory_tool_scope(),
            extra_tools: extra_tools.clone(),
            tenant: task.tenant.clone(),
            agent: run.agent.map(|agent| agent.mandate.holder().to_owned()),
            allowed_tools: run.allowed_tools(),
            flow_labels: tree.exercised.clone(),
        };
        // A request the engine would refuse is not sent. The refusal is not
        // retryable, so it would fail the run after earlier work was paid for.
        if exceeds_bytes(&request, task.request_cap()) {
            return Ok(AgentStop::TranscriptFull);
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
            progress.push_assistant(&turn);
            progress.answered = true;
            return Ok(if over_budget {
                AgentStop::TokensExhausted
            } else if turn.stop == StopReason::MaxTokens {
                AgentStop::OutputCapped
            } else {
                AgentStop::Completed
            });
        }
        // A turn with calls that ends here is NOT added to the transcript. A
        // tool call with no result would make the transcript invalid for the
        // next run that continues it.
        //
        // A turn cut at the output cap can hold a partial call. It never runs.
        if turn.stop == StopReason::MaxTokens {
            return Ok(AgentStop::OutputCapped);
        }
        if over_budget {
            return Ok(AgentStop::TokensExhausted);
        }
        if progress.steps >= task.max_steps {
            return Ok(AgentStop::StepsExhausted);
        }

        let slot = Slot {
            step: progress.steps,
            round: tree.rounds,
            position: 0,
            labels: None,
        };
        progress.steps = progress.steps.saturating_add(1);
        tree.rounds = tree.rounds.saturating_add(1);
        progress.push_assistant(&turn);

        match Box::pin(run_round(ctx, run, progress, tree, slot, &turn, calls)).await? {
            RoundEnd::Continue => {}
            RoundEnd::Full => return Ok(AgentStop::TranscriptFull),
            RoundEnd::Loop => return Ok(AgentStop::LoopDetected),
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

/// Where one call sits in its run and in the tree, with its recorded flow
/// labels.
#[derive(Debug, Clone, Copy)]
struct Slot<'a> {
    /// The tool round (0-based) of the segment.
    step: u32,
    /// The tool round of the whole tree. The approval name holds it.
    round: u32,
    /// The position of the call in its round.
    position: usize,
    /// The flow labels that the decision recorded for the call. `None` when
    /// the turn recorded no labels, because its worker had no flow rules.
    labels: Option<&'a BTreeSet<String>>,
}

impl Slot<'_> {
    /// Add the recorded labels of the call to the tree. The decision lets
    /// the call run, so its labels count.
    fn exercise(self, tree: &mut Tree) {
        if let Some(labels) = self.labels {
            tree.exercised.extend(labels.iter().cloned());
        }
    }
}

/// Answer every call of one round in ONE tool message.
///
/// The results can pass the request cap. No more calls run then, because the
/// results cannot fit any request. No more calls run after the loop guard
/// stops the run either. Each call still gets an answer, so the transcript
/// stays valid and shows what ran.
async fn run_round(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    turn: &ModelTurn,
    calls: Vec<ToolCall>,
) -> Result<RoundEnd, String> {
    let task = run.task;
    let cap = task.request_cap();
    let mut results = Vec::with_capacity(calls.len());
    let mut spent = 0_u64;
    let mut full = false;
    let mut looped = false;
    let mut stale = false;
    for (position, call) in calls.into_iter().enumerate() {
        progress.tool_calls = progress.tool_calls.saturating_add(1);
        let call_id = call.id.clone();
        let content = if full {
            ToolOutcome::error(NOT_RUN_FULL).content
        } else if looped {
            ToolOutcome::error(NOT_RUN_LOOP).content
        } else if stale {
            ToolOutcome::error(NOT_RUN_STALE).content
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
            let labels = turn.labels.get(position);
            // With flow rules, the turn decided each call against the labels
            // before the round. A delegation can add the labels of its child.
            // The calls after it then hold a stale decision, so they do not run.
            let expected: Option<BTreeSet<String>> =
                labels.map(|labels| tree.exercised.union(labels).cloned().collect());
            let name = call.name.clone();
            let arguments = call.arguments.clone();
            let at = Slot {
                position,
                labels,
                ..slot
            };
            let mut content = decide(ctx, run, progress, tree, at, call, decision)
                .await?
                .content;
            stale = expected.is_some_and(|expected| !tree.exercised.is_subset(&expected));
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

/// The answer to a call that did not run because a delegation earlier in the
/// round added flow labels after the decision.
const NOT_RUN_STALE: &str = "the call did not run: a delegation earlier in this turn \
     changed the flow labels, so its decision is stale; call it again to get a new decision";

/// Act on one recorded policy decision.
///
/// The recorded flow labels of the call join the tree labels when the
/// decision lets the call run.
async fn decide(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    call: ToolCall,
    decision: ToolDecision,
) -> Result<ToolOutcome, String> {
    match decision {
        ToolDecision::Allow => {
            slot.exercise(tree);
            run_tool(ctx, run, progress, tree, slot, call).await
        }
        ToolDecision::Deny { reason } => {
            Ok(ToolOutcome::error(&format!("denied by policy: {reason}")))
        }
        ToolDecision::RequireApproval { .. } => {
            gated_call(ctx, run, progress, tree, slot, call).await
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
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    mut call: ToolCall,
) -> Result<ToolOutcome, String> {
    let task = run.task;
    let decision = await_decision::<Approval>(
        ctx,
        &approval_signal(slot.round, slot.position, &call.id),
        Duration::from_secs(task.approval_timeout_secs),
    )
    .await?;
    match decision {
        Decision::Decided(Approval::Approve) => {
            slot.exercise(tree);
            run_tool(ctx, run, progress, tree, slot, call).await
        }
        Decision::Decided(Approval::Edit { arguments }) => {
            call.arguments = arguments;
            slot.exercise(tree);
            run_tool(ctx, run, progress, tree, slot, call).await
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
/// The follow-up tool and the delegate tool run in the workflow. Every other
/// call is a durable activity.
async fn run_tool(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    call: ToolCall,
) -> Result<ToolOutcome, String> {
    let task = run.task;
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
    // Under a mandate, the workflow owns the name `delegate`, even with no
    // peer to ask. Such a call gets a named refusal.
    if let Some(agent) = run.agent.filter(|_| call.name == DELEGATE_TOOL) {
        return delegate(ctx, run, agent, progress, tree, &call).await;
    }
    ctx.execute_activity(
        &agent_tool_call_info(),
        ToolCallRequest {
            run_id: progress.run_id.clone(),
            session_id: task.session_id.clone(),
            step: progress.step_base.saturating_add(slot.step),
            call,
            memory_scope: task.memory_tool_scope(),
            read_only: task.read_only,
            allowed_tools: run.allowed_tools(),
            labels: slot.labels.cloned(),
        },
    )
    .await
    .map_err(|e| e.to_string())
}

/// Run one `delegate` call: refuse it with a named verdict, or run the peer
/// inline as a sub-agent.
///
/// The child gets the peer prompt, a fresh transcript and the task text. It
/// sees nothing else of the parent. Its model calls and tool calls are
/// activities of this workflow, so replay sends none of them again. Only
/// its final answer returns, as the tool result.
async fn delegate(
    ctx: &WorkflowContext,
    run: Run<'_>,
    agent: &Agent,
    progress: &mut Progress,
    tree: &mut Tree,
    call: &ToolCall,
) -> Result<ToolOutcome, String> {
    let task = run.task;
    // serde also decodes a struct from a JSON array, so the arguments must
    // be an object to have the named fields `agent` and `task`.
    let admitted = Some(&call.arguments)
        .filter(|arguments| arguments.is_object())
        .and_then(|arguments| serde_json::from_value::<DelegateArgs>(arguments.clone()).ok())
        .ok_or((Refusal::InvalidArguments, String::new()))
        .and_then(|args| {
            admit(agent, task, progress, tree, &args.agent)
                .map(|child| (child, args.task))
                .map_err(|refusal| (refusal, args.agent))
        });
    let (child, child_input) = match admitted {
        Ok(admitted) => admitted,
        Err((refusal, name)) => {
            // The model wrote the name, so a record keeps it only when it is
            // a valid agent name.
            let shown = if valid_name(&name) {
                name.clone()
            } else {
                INVALID_NAME.to_owned()
            };
            let mut chain = agent.mandate.chain.clone();
            chain.push(shown);
            tree.records.push(DelegationRecord {
                chain,
                run_id: None,
                stop: None,
                refusal: Some(refusal),
                usage: TokenUsage::default(),
                steps_used: 0,
                tool_calls: 0,
            });
            return Ok(refused(refusal, &name));
        }
    };
    let Admitted {
        mandate,
        peer,
        max_total_tokens,
    } = child;
    tree.delegations = tree.delegations.saturating_add(1);
    let run_id = format!("{}:d{}", tree.root, tree.delegations);
    let read_only = task.read_only || mandate.read_only;
    let child_agent = discover(ctx, &mandate).await?;
    let child_task = AgentTask {
        input: child_input,
        system: peer.system.clone(),
        history: Vec::new(),
        session_id: task.session_id.clone(),
        max_steps: peer.max_steps,
        max_total_tokens,
        max_output_tokens: task.max_output_tokens,
        approval_timeout_secs: task.approval_timeout_secs,
        max_request_bytes: task.max_request_bytes,
        deliver: false,
        memory_scope: None,
        followups: None,
        loop_guard: task.loop_guard,
        read_only,
        unattended: task.unattended,
        unattended_memory_writes: false,
        run_id_source: task.run_id_source,
        tenant: task.tenant.clone(),
        mandate: Some(mandate),
        // The tree holds the labels, so the child task carries none.
        flow_labels: BTreeSet::new(),
    };
    let mut child = Progress::new(run_id.clone(), Vec::new());
    let child_run = Run {
        task: &child_task,
        agent: Some(&child_agent),
    };
    let stop = Box::pin(run_segment(
        ctx,
        child_run,
        &mut child,
        tree,
        &child_task.input,
    ))
    .await?;
    // The parent counts the whole subtree, as it does for usage.
    let steps = child.steps.saturating_add(child.delegated_steps);
    let tool_calls = child.tool_calls.saturating_add(child.delegated_tool_calls);
    progress.usage = progress.usage.saturating_add(child.usage);
    progress.delegated_steps = progress.delegated_steps.saturating_add(steps);
    progress.delegated_tool_calls = progress.delegated_tool_calls.saturating_add(tool_calls);
    tree.records.push(DelegationRecord {
        chain: child_agent.mandate.chain.clone(),
        run_id: Some(run_id),
        stop: Some(stop),
        refusal: None,
        usage: child.usage,
        steps_used: steps,
        tool_calls,
    });
    let outcome = serde_json::json!({
        "agent": peer.name,
        "stop": stop,
        "answer": truncate(&child.last_text, DEFAULT_TOOL_OUTPUT_LIMIT),
    })
    .to_string();
    Ok(ToolOutcome {
        content: outcome,
        is_error: stop != AgentStop::Completed,
    })
}

/// A delegation that passed every check.
struct Admitted<'a> {
    mandate: Mandate,
    peer: &'a Peer,
    max_total_tokens: Option<u32>,
}

/// Check one delegation to `name`. Returns the child mandate, the peer and
/// the child token cap.
///
/// The checks run in a fixed order: the mandate names the peer, the roster
/// knows it, the mandate allows the hop, and tokens are left.
fn admit<'a>(
    agent: &'a Agent,
    task: &AgentTask,
    progress: &Progress,
    tree: &Tree,
    name: &str,
) -> Result<Admitted<'a>, Refusal> {
    if !agent.mandate.peers.contains(name) {
        return Err(Refusal::NotPermitted);
    }
    let peer = agent
        .peers
        .iter()
        .find(|peer| peer.name == name)
        .ok_or(Refusal::UnknownPeer)?;
    let mandate = agent.mandate.delegate(peer, tree.delegations)?;
    let remainder = task
        .max_total_tokens
        .map(|max| max.saturating_sub(progress.usage.total()));
    let max_total_tokens = match (remainder, peer.max_total_tokens) {
        (Some(left), Some(own)) => Some(left.min(own)),
        (left, own) => left.or(own),
    };
    // A cap of zero would still let the child make one model call.
    if max_total_tokens == Some(0) {
        return Err(Refusal::BudgetExhausted);
    }
    Ok(Admitted {
        mandate,
        peer,
        max_total_tokens,
    })
}

/// The tool result of a refused delegation. The model reads the verdict.
fn refused(refusal: Refusal, peer: &str) -> ToolOutcome {
    ToolOutcome {
        content: serde_json::json!({
            "error": refusal.message(peer),
            "verdict": refusal,
        })
        .to_string(),
        is_error: true,
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
/// A rate limit, a transport fault, an outage or a timeout retries with
/// backoff. Any other failure fails the call at once.
///
/// A turn that calls the model records one call in the agent cost ledger
/// (issue #1996). `AgentModel::model_id` names the model, and
/// `AgentModel::cost_usd_micros` prices the call. A response cache hit calls
/// no model and spends no tokens, so it records no call.
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
    let harness = harness(ctx)?;
    let tenant = cache_tenant(ctx, harness, &request).await;
    let (turn, latency) = harness
        .model_turn_timed_in(request, tenant, |_content| {})
        .await?;
    if let Some(latency) = latency {
        harness.record_turn(ctx, &turn.usage, latency);
    }
    Ok(turn)
}

/// The tenant that scopes the response cache for one turn (issue #1998).
///
/// The activity reads the verified tenant of the run only when the harness
/// has a cache.
async fn cache_tenant(
    ctx: &ActivityContext,
    harness: &AgentHarness,
    request: &ModelTurnRequest,
) -> CacheTenant {
    if !harness.cache_active() {
        return CacheTenant::None;
    }
    let info = ctx.info();
    if shares_run_id(
        &request.run_id,
        &info.workflow_id,
        &info.execution_id.to_string(),
    ) {
        return CacheTenant::Skip;
    }
    // A local activity has no pool, so its read fails. The turn then skips
    // the cache.
    let read = tokio::time::timeout(harness.cache_budget(), ctx.run_tenant())
        .await
        .map_err(|_| "the read timed out".to_owned())
        .and_then(|read| read.map_err(|err| err.to_string()));
    resolve_tenant(read, request.tenant.as_deref())
}

/// Whether the run id of a turn is the workflow id, which a later run can
/// reuse.
///
/// A hit repeats the tool-call ids of the stored answer. Tools take the run
/// id, the step and the call id as an idempotency key. With a shared run
/// id, a hit could give a tool a key that an earlier run used, so the turn
/// skips the cache.
///
/// A delegated child has the run id `{root}:d{n}`. A later run of the same
/// workflow id gives its child the same id, so the rule covers the root part.
fn shares_run_id(run_id: &str, workflow_id: &str, execution_id: &str) -> bool {
    let root = child_root(run_id).unwrap_or(run_id);
    (run_id == workflow_id || root == workflow_id) && run_id != execution_id && root != execution_id
}

/// The root run id of a delegated child run id `{root}:d{n}`, or `None` for
/// any other run id.
fn child_root(run_id: &str) -> Option<&str> {
    let (root, ordinal) = run_id.rsplit_once(":d")?;
    (!ordinal.is_empty() && ordinal.bytes().all(|byte| byte.is_ascii_digit())).then_some(root)
}

/// Pick the cache tenant from the verified read and the task field.
///
/// The verified tenant wins over the task field. A caller bound to one
/// tenant writes the task, so the activity does not trust the field alone.
/// With no verified tenant, the task field applies as a declared tenant. A
/// failed read gives an unknown tenant, so the turn skips the cache.
fn resolve_tenant(read: Result<Option<String>, String>, task: Option<&str>) -> CacheTenant {
    match read {
        Ok(Some(verified)) => {
            if task.is_some_and(|task| task != verified) {
                tracing::warn!(
                    "AgentTask::tenant differs from the verified tenant; the cache uses the verified tenant"
                );
            }
            CacheTenant::Verified(verified)
        }
        Ok(None) => CacheTenant::declared(task),
        Err(err) => {
            tracing::warn!(error = %err, "the run tenant read failed; the turn skips the cache");
            CacheTenant::Skip
        }
    }
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

/// The peers of the roster that a mandate names, as an activity.
///
/// A run reads its peers once, at its start. Replay reads the record, so a
/// roster change cannot change a recorded run.
///
/// # Errors
///
/// Returns an error only when no harness is installed.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_discover_peers(
    ctx: &ActivityContext,
    names: Vec<String>,
) -> Result<Vec<Peer>, String> {
    Ok(harness(ctx)?.discover_peers(&names))
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
        agent_discover_peers_info(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_verified_tenant_wins_over_the_task_field() {
        let verified = || Ok(Some("acme".to_owned()));
        assert_eq!(
            resolve_tenant(verified(), Some("evil")),
            CacheTenant::Verified("acme".into())
        );
        assert_eq!(
            resolve_tenant(verified(), None),
            CacheTenant::Verified("acme".into())
        );
    }

    #[test]
    fn with_no_verified_tenant_the_task_field_is_declared() {
        assert_eq!(
            resolve_tenant(Ok(None), Some("a")),
            CacheTenant::Declared("a".into())
        );
        assert_eq!(resolve_tenant(Ok(None), None), CacheTenant::None);
    }

    #[test]
    fn a_run_id_from_the_workflow_id_skips_the_cache() {
        assert!(shares_run_id("daily", "daily", "0e3c"));
        assert!(!shares_run_id("0e3c", "daily", "0e3c"));
        // A test context has neither id.
        assert!(!shares_run_id("run-1", "", "00000000"));
    }

    #[test]
    fn a_child_of_a_workflow_id_run_skips_the_cache() {
        assert!(shares_run_id("daily:d1", "daily", "0e3c"));
        assert!(shares_run_id("daily:d12", "daily", "0e3c"));
        assert!(!shares_run_id("0e3c:d1", "daily", "0e3c"));
        // A workflow id that only looks like a child id keeps the old rule.
        assert!(shares_run_id("agent:daily", "agent:daily", "0e3c"));
        assert!(!shares_run_id("agent:daily", "agent", "0e3c"));
    }

    #[test]
    fn a_failed_read_skips_the_cache() {
        let failed = Err("the pool is down".to_owned());
        assert_eq!(resolve_tenant(failed, Some("a")), CacheTenant::Skip);
    }
}
