//! The agent loop as a workflow.
//!
//! The loop is ordinary Rust. Durability comes from the awaits:
//!
//! - [`agent_model_turn`] — one model call and the policy decisions.
//! - [`agent_tool_call`] — one tool call.
//! - [`agent_approval_notice`] — one notice before an approval wait.
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;

use crate::agent::{
    AGENT_WORKFLOW_NAME, AgentId, AgentOp, INTERRUPT_SIGNAL, PeerEnvelope, PeerReply, STEER_SIGNAL,
    Steer,
};
use crate::agent::{
    agent_info, agent_memory_count_info, agent_memory_reset_info, agent_profile_info,
    agent_profile_overlay_info, agent_transcript_count_info, agent_transcript_erase_info,
    agent_workspace_floor_info, agent_workspace_reset_info, agent_workspace_retain_info,
};
use crate::answer_check::{self, Checked};
use crate::approval::{Approval, Decision, approval_signal, await_decision, decode_decision};
use crate::bounds::{
    RoundBudget, full_round_resets, json_len, reaches_percent, reset_due, sibling_cap,
};
use crate::context_reset::{self, ProgressNotes, SAVE_PROGRESS_TOOL};
use crate::delivery::{ParkedCall, Report, ReportSource, cut_arguments, report_text};
use crate::draw::Draw;
use crate::fan_out_plan::{Fit, Step, fan_out_plan};
use crate::flow::Released;
use crate::followup::{self, FOLLOWUP_TOOL, Planned};
use crate::harness::{
    AgentHarness, CacheTenant, REASON_CUT_BYTES, TRUNCATED, cut_bytes, max_tool_result_bytes,
    truncate, truncate_flagged,
};
use crate::heartbeat::{HeartbeatTask, agent_heartbeat_info};
use crate::loop_guard::{LoopTracker, LoopVerdict, warning_note};
use crate::memory::{
    MEMORY_TOOL, MemoryBlock, MemoryOp, MemoryScope, MemorySnapshot, MemoryTool, apply_to,
    edit_result,
};
use crate::mesh::{
    DELEGATE_TOOL, DelegateArgs, DelegationRecord, INVALID_NAME, Mandate, Peer, Refusal,
    SerialReason, delegate_definition, sample_cap, valid_name,
};
use crate::mesh_control::{MESH_ATTR, MESH_BUDGET_EXCEEDED, MeshBudgetExceeded};
use crate::message::{
    ChatMessage, ChatRole, ContentPart, RunId, StopReason, TokenUsage, ToolCall, ToolDefinition,
};
use crate::policy::ToolDecision;
use crate::skill::{self, LOAD_SKILL_TOOL};
use crate::transcript::{PREFIX_MISSING, PrefixRef, Sent, TRANSCRIPT_DELTA_PATCH};
use crate::types::{
    AgentReport, AgentStop, AgentTask, MAX_PARALLEL_CHILDREN, ModelTurn, ModelTurnRequest,
    RunIdSource, ToolCallRequest, ToolOutcome, without_system,
};
use crate::workspace::WorkspaceMount;
use autumn_harvest::durable_promise::PromiseSettlement;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, EntityMessage};

/// The replay gate of the final-turn delivery and the raw follow-up length.
const FINAL_TURN_PATCH: &str = "agent_final_turn_rules";

/// The gate for `report_failure` on fan-out children (issue #2328). The flag is
/// part of the recorded child input. A history from before the gate replays
/// with the old input.
const CHILD_FAILURE_REPORT_PATCH: &str = "agent_child_failure_report";

/// The gate of the `sample` argument of `delegate`. A history from before
/// the gate refused such a call as `invalid_arguments`, so its replay still
/// does. The gate runs only for a call with a sample, so a plain call
/// records no marker.
const DELEGATE_SAMPLE_PATCH: &str = "agent_delegate_sample";

/// The gate of the `sample` argument in the schema of the `delegate` tool.
/// A history from before the gate recorded each model input with the old
/// schema, so its replay keeps it.
const DELEGATE_SAMPLE_SCHEMA_PATCH: &str = "agent_delegate_sample_schema";

/// The cut of a delegation answer in a history from before issue #2481.
///
/// Such a history recorded its peer lookup with no `answer_limit`. Its run
/// replays with this cut and with no `truncated` key. A new lookup always
/// holds a limit, so a new run never reads this value.
const LEGACY_ANSWER_LIMIT: usize = 8_000;

/// The gate of the rules for a tool round over the cap (issue #2461). The
/// loop asks it only where a new rule changes the branch: a round that
/// resets, and a stopped save round. A history recorded before the gate
/// holds no marker there, so it replays as it ran.
const RESET_ON_FULL_PATCH: &str = "agent_reset_on_full_round";

/// The gate of the approval notice. A history from before the gate holds no
/// notice before its approval waits, so its replay sends none. The loop asks
/// the gate only for a gated call, so each other run records no marker.
const APPROVAL_NOTICE_PATCH: &str = "agent_approval_notice";

/// The patch that gates the verdict of a durable peer with no reply (issue
/// #2484). It gates the model text, the record verdict and the decode of a
/// value that is not a reply. A history from before the gate keeps each of
/// them, so its replay holds.
///
/// A sent message asks the gate before its wait. A run that waits across
/// the upgrade then reads no marker, so it keeps the old shape. The strict
/// replayer of the test engine refuses a marker right after a fired wait
/// deadline. So the gate cannot come after the wait.
const REPLY_VERDICT_PATCH: &str = "agent_peer_reply_verdict";

/// A signal name that no client sends. A probe of it claims nothing.
///
/// The probe moves each signal that waits at the end of history into the
/// signal buffer. The patch gate reads a gate with only signals after it as a
/// recorded position, and takes the old branch. A decision left over from an
/// earlier wait would then hide the notice. After the probe, the gate sees the
/// real end of history.
const APPROVAL_NOTICE_PROBE: &str = "agent.approval_notice.probe";

/// The side effect that records whether the notice input fits the caps.
///
/// A schedule over a cap records nothing. The wait after it would then meet a
/// replay that expects the notice. The recorded answer keeps the choice the
/// same on each replay, also after a cap change.
const APPROVAL_NOTICE_FITS: &str = "agent.approval_notice.fits";

/// The registered workflow name.
pub const WORKFLOW_NAME: &str = "agent_loop";

/// The state of one agent run, carried across its segments. Every field is
/// rebuilt from history on replay.
pub(crate) struct Progress {
    pub(crate) run_id: String,
    pub(crate) messages: Vec<ChatMessage>,
    /// Tokens of the whole run, earlier segments and delegations included.
    pub(crate) usage: TokenUsage,
    /// The priced cost of the same calls, in millionths of a US dollar.
    pub(crate) cost_usd_micros: u64,
    /// Tool rounds of this segment. `max_steps` bounds it.
    pub(crate) steps: u32,
    /// Tool rounds of the earlier segments. Tool steps add it, so they never
    /// repeat across segments.
    pub(crate) step_base: u32,
    /// Tool calls of the whole run.
    pub(crate) tool_calls: u32,
    /// Tool rounds of every agent under this one. They do not count against
    /// `max_steps`, which bounds each agent alone.
    pub(crate) delegated_steps: u32,
    /// Tool calls of every agent under this one.
    pub(crate) delegated_tool_calls: u32,
    pub(crate) last_text: String,
    /// The text of the turn that ended the segment with no tool calls. It is
    /// `None` when the segment ended any other way.
    pub(crate) answer: Option<String>,
    /// The loop guard history of the whole run.
    pub(crate) tracker: LoopTracker,
    /// The follow-up that this segment booked.
    pub(crate) followup: Option<Planned>,
    /// Follow-ups that ran before this segment.
    pub(crate) chain: u32,
    /// The run records the final-turn marker. An older history does not, and
    /// replays under the rules it ran with.
    pub(crate) final_turn_rules: bool,
    /// The steer book of a durable agent segment. `None` takes no steer.
    pub(crate) steer: Option<SteerBook>,
    /// An interrupt ended the work of this segment (issue #2265).
    pub(crate) interrupted: bool,
    /// Send each turn as a delta on the last window (issue #2264).
    pub(crate) delta: bool,
    /// The last window that a turn sent, when `delta` is on.
    pub(crate) sent: Option<Sent>,
    /// The memory blocks of a durable agent with state memory (issue
    /// #2273). The workflow runs the `memory` tool on them.
    pub(crate) memory: Option<Vec<MemoryBlock>>,
    /// A mesh budget refused a turn. The period ends then, in epoch
    /// milliseconds (issue #2272).
    pub(crate) parked_until: Option<i64>,
    /// The workspace of the run and its head (issue #2274). A tool call that
    /// changes the workspace moves the head.
    pub(crate) workspace: Option<WorkspaceMount>,
    /// The notes and the context resets of the run (issue #2453).
    pub(crate) reset: ResetState,
    /// The steer messages of this segment. A reset keeps them, so a reset
    /// never drops a message from a person.
    pub(crate) steers: Vec<ChatMessage>,
    /// The admitted answer check of the run (issue #2442).
    pub(crate) check: Option<answer_check::ActiveCheck>,
    /// The reason of each answer that the check rejected, follow-ups
    /// included.
    pub(crate) rejections: Vec<String>,
}

/// The notes and the context resets of one run (issue #2453).
#[derive(Debug, Default)]
pub(crate) struct ResetState {
    /// The context resets of the run, follow-ups included.
    pub(crate) count: u32,
    /// The notes of the last `save_progress` call that passed its checks.
    pub(crate) notes: Option<ProgressNotes>,
    /// Show the notes after the input of each segment. Only a durable agent
    /// sets it, because its transcript holds no copy of the notes.
    pub(crate) restore: bool,
    /// The last turn was the save turn of a reset. The reset runs before
    /// the next request.
    pub(crate) pending: bool,
    /// The booking that woke this segment, with its result. Each reset of
    /// the segment keeps it, because the wake message points at it.
    pub(crate) woken_by: Option<[ChatMessage; 2]>,
    /// The tool round over the cap that made the next reset due, with its
    /// answers (issue #2461). The reset window keeps it, so the model sees
    /// which calls ran. The reset clears it.
    pub(crate) kept: Option<[ChatMessage; 2]>,
}

/// The steer messages of a durable agent (issue #2265).
///
/// A client sends a steer as a steer signal and as an op with one message
/// id. A running segment takes the signal, so the op is then a no-op. An op
/// that runs first marks its id, so its signal is then a no-op.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct SteerBook {
    /// Ids of steer signals that a segment took. Their ops are no-ops.
    pub steered: std::collections::VecDeque<String>,
    /// Ids of steer ops that ran. Their signals are no-ops.
    pub ran: std::collections::VecDeque<String>,
}

impl SteerBook {
    /// The most ids that each list keeps. A full list drops its oldest id.
    pub const MAX_IDS: usize = 256;

    /// Take the id `id` from `list`. Returns `true` when it was there.
    pub(crate) fn take(list: &mut std::collections::VecDeque<String>, id: &str) -> bool {
        list.iter()
            .position(|held| held == id)
            .and_then(|at| list.remove(at))
            .is_some()
    }

    /// Add `id` to `list`. A full list drops its oldest id.
    pub(crate) fn note(list: &mut std::collections::VecDeque<String>, id: String) {
        if list.len() >= Self::MAX_IDS {
            list.pop_front();
        }
        list.push_back(id);
    }
}

impl Progress {
    /// A run with `run_id` that continues `messages`.
    pub(crate) fn new(run_id: String, messages: Vec<ChatMessage>) -> Self {
        Self {
            run_id,
            messages,
            usage: TokenUsage::default(),
            cost_usd_micros: 0,
            steps: 0,
            step_base: 0,
            tool_calls: 0,
            delegated_steps: 0,
            delegated_tool_calls: 0,
            last_text: String::new(),
            answer: None,
            tracker: LoopTracker::default(),
            followup: None,
            chain: 0,
            final_turn_rules: false,
            steer: None,
            interrupted: false,
            delta: false,
            sent: None,
            memory: None,
            parked_until: None,
            workspace: None,
            reset: ResetState::default(),
            steers: Vec::new(),
            check: None,
            rejections: Vec::new(),
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

    /// The text of a segment to deliver. `last` is the last non-empty text.
    pub(crate) fn delivered_text<'a>(&'a self, last: &'a str) -> &'a str {
        match self.answer.as_deref() {
            Some(text) if self.final_turn_rules => text,
            // A history before the rules sent the last non-empty text.
            Some(_) => last,
            None => "",
        }
    }

    pub(crate) fn report(&self, stop: AgentStop, tree: &Tree) -> AgentReport {
        AgentReport {
            stop,
            text: self.last_text.clone(),
            steps_used: self
                .step_base
                .saturating_add(self.steps)
                .saturating_add(self.delegated_steps),
            tool_calls: self.tool_calls.saturating_add(self.delegated_tool_calls),
            usage: self.usage,
            cost_usd_micros: self.cost_usd_micros,
            messages: without_system(&self.messages),
            followups: self.chain,
            followup_dropped: false,
            delegations: tree.records.clone(),
            flow_labels: tree.exercised.clone(),
            run_id: Some(self.run_id.clone()),
            workspace: self.workspace.as_ref().and_then(|mount| mount.head.clone()),
            error: None,
            resets: self.reset.count,
            answer_rejections: self.rejections.clone(),
            parked_until_ms: self.parked_until,
        }
    }

    /// Add the cost of a call or of a child run.
    pub(crate) const fn add_cost(&mut self, cost_usd_micros: u64) {
        self.cost_usd_micros = self.cost_usd_micros.saturating_add(cost_usd_micros);
    }

    /// The report of a run that failed with `error`. It keeps what the run
    /// spent before it failed (issue #2328).
    fn failure(&self, error: String, tree: &Tree) -> AgentReport {
        AgentReport {
            error: Some(error),
            ..self.report(AgentStop::Failed, tree)
        }
    }
}

/// The state that every agent of one run tree shares. Every field is rebuilt
/// from history on replay.
///
/// The tree runs one step at a time inside one workflow, so a plain value is
/// enough. No two children can read it at once.
pub(crate) struct Tree {
    /// The run id of the root agent. A child run id adds an ordinal to it.
    pub(crate) root: String,
    /// Tool rounds of the whole tree. Approval names use it, so they are
    /// unique in the tree. With no delegation, it equals the round count of
    /// the root run, so the names stay as before.
    pub(crate) rounds: u32,
    /// Delegations that started in the tree.
    pub(crate) delegations: u32,
    /// The flow labels of the task, and of every call in the tree that the
    /// decision let run.
    pub(crate) exercised: BTreeSet<String>,
    /// The flow rules that a person released by an approval in this tree.
    /// Another run or a remote peer never adds one, so a carried label can
    /// never stand in for an approval.
    pub(crate) released: Released,
    /// Each delegation, in the order it ended.
    pub(crate) records: Vec<DelegationRecord>,
    /// The sender side of durable peer messages (issue #2269).
    pub(crate) outbox: Outbox,
}

/// The sender side of durable peer messages (issue #2269).
#[derive(Debug, Clone, Default)]
pub(crate) struct Outbox {
    /// The sender id that a receiver dedupes on.
    pub(crate) from: String,
    /// The workflow id of this agent.
    pub(crate) self_id: String,
    /// The workflow ids that wait on the op of this agent. A send to one of
    /// them, or to this agent, is a deadlock.
    pub(crate) waiting: Vec<String>,
    /// The number of the last message to each receiver, by workflow id. Each
    /// receiver counts from 1, so it sees no gaps.
    pub(crate) sent: BTreeMap<String, u64>,
    /// The mesh of the sender (issue #2272).
    pub(crate) mesh: Option<String>,
}

impl Outbox {
    /// Take the number of the next message to `target`.
    fn next_seq(&mut self, target: &str) -> u64 {
        let last = self.sent.entry(target.to_owned()).or_default();
        *last = last.saturating_add(1);
        *last
    }
}

/// One agent of a run tree: its mandate, and the peers that its recorded
/// lookup found.
#[derive(Debug)]
pub(crate) struct Member {
    mandate: Mandate,
    peers: Vec<Peer>,
    /// Whether the `delegate` tool may show the `sample` argument. The patch
    /// gate decides it once per member, so a history from before the gate
    /// keeps the old tool schema in each model input.
    sample_schema: bool,
}

impl Member {
    /// A member with `mandate` and no peer.
    pub(crate) const fn bare(mandate: Mandate) -> Self {
        Self {
            mandate,
            peers: Vec::new(),
            sample_schema: false,
        }
    }

    /// This member under `mandate`, which is never wider. It keeps only the
    /// peers that `mandate` names.
    pub(crate) fn narrowed(&self, mandate: Mandate) -> Self {
        let peers = self
            .peers
            .iter()
            .filter(|peer| mandate.peers.contains(&peer.name))
            .cloned()
            .collect();
        Self {
            mandate,
            peers,
            sample_schema: self.sample_schema,
        }
    }

    /// The mandate of the member.
    pub(crate) const fn mandate(&self) -> &Mandate {
        &self.mandate
    }

    /// The `delegate` tool, when the agent has a peer to ask.
    fn delegate_tool(&self) -> Option<crate::message::ToolDefinition> {
        let cap = self
            .sample_schema
            .then(|| sample_cap(&self.mandate, &self.peers))
            .flatten();
        (!self.peers.is_empty()).then(|| delegate_definition(&self.peers, cap))
    }
}

/// One agent run: its task and its place in the tree.
#[derive(Clone, Copy)]
pub(crate) struct Run<'a> {
    pub(crate) task: &'a AgentTask,
    pub(crate) agent: Option<&'a Member>,
    /// The run is a segment of a durable agent. It takes interrupts and
    /// records which tool calls opt in to them (issue #2265).
    pub(crate) live: bool,
}

impl Run<'_> {
    /// The harness tools that the mandate permits. `None` means every tool.
    fn allowed_tools(&self) -> Option<BTreeSet<String>> {
        self.agent.and_then(|agent| agent.mandate.tools.clone())
    }

    /// The tools that the workflow runs itself. The model sees them with the
    /// harness tools.
    fn extra_tools(&self) -> Vec<ToolDefinition> {
        self.task
            .followups
            .map(|_| followup::definition())
            .into_iter()
            .chain(self.agent.and_then(Member::delegate_tool))
            .chain(
                (self.task.memory_in_state && self.task.memory_writable())
                    .then(MemoryTool::definition),
            )
            .chain(
                self.task
                    .context_reset
                    .as_ref()
                    .map(context_reset::definition),
            )
            .collect()
    }
}

/// The request for the next model turn over `messages`.
///
/// The loop sends it, and a tool round measures it before it runs a call.
/// One builder makes the loop and the round measure the same fields.
fn next_request(
    run: Run<'_>,
    progress: &Progress,
    tree: &Tree,
    extra_tools: &[ToolDefinition],
    messages: Vec<ChatMessage>,
) -> ModelTurnRequest {
    let task = run.task;
    ModelTurnRequest {
        run_id: progress.run_id.clone(),
        session_id: task.session_id.clone(),
        steps_used: progress.steps,
        max_steps: task.max_steps,
        usage: progress.usage,
        messages,
        max_output_tokens: task.max_output_tokens,
        read_only: task.read_only,
        memory_scope: task.memory_tool_scope(),
        extra_tools: extra_tools.to_vec(),
        tenant: task.tenant.clone(),
        agent: run.agent.map(|agent| agent.mandate.holder().to_owned()),
        allowed_tools: run.allowed_tools(),
        flow_labels: tree.exercised.clone(),
        flow_released: tree.released.clone(),
        interruptible: run.live,
        mesh: task.mesh.clone(),
        stream: task.stream,
        prefix: None,
        remember: false,
        model: task.model.clone(),
        tool_guidance: task.tool_guidance.clone(),
        skills: !task.skills.is_empty(),
        allowed_effects: task.allowed_effects.clone(),
        draw: task.draw.as_ref().map(|draw| draw.id),
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
    // A draw with a root has the run id `{root}:s{n}` (issue #2440).
    let draw_run_id = match &task.draw {
        Some(draw) => draw.run_id()?,
        None => None,
    };
    let run_id = match (draw_run_id, task.run_id_source) {
        (Some(draw), _) => draw,
        (None, RunIdSource::ExecutionId) => ctx.execution_id().to_string(),
        // A run recorded before the change keeps its recorded run id.
        (None, RunIdSource::WorkflowId) => ctx.workflow_id().to_owned(),
    };
    // A system message inside the history is dropped. Only `task.system`
    // reaches the model as the system prompt.
    let mut progress = Progress::new(run_id, without_system(&task.history));
    progress.workspace.clone_from(&task.workspace);
    // Gate the final-turn rules. A run recorded before them replays as it ran.
    progress.final_turn_rules = ctx.patched(FINAL_TURN_PATCH);
    // A task recorded before the delta form has no flag, so it asks no gate.
    progress.delta = task.transcript_delta && ctx.patched(TRANSCRIPT_DELTA_PATCH);
    // A mesh budget sums the ledger rows of every run of the mesh, child
    // runs included. The upsert appends no event.
    if let Some(mesh) = &task.mesh
        && let Err(err) =
            ctx.upsert_search_attrs([(MESH_ATTR.to_owned(), Some(serde_json::json!(mesh)))])
    {
        tracing::warn!(error = %err, "the mesh search attribute was not set");
    }
    let mut tree = root_tree(ctx, task, &progress.run_id);
    let agent = root_member(ctx, task).await?;
    if let Some(failed) = Box::pin(admit_check(ctx, task, &mut progress, &tree)).await? {
        return Ok((failed, 0));
    }
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
            live: false,
        };
        let stop = match run_segment(ctx, run, &mut progress, &mut tree, input).await {
            Ok(stop) => stop,
            Err(error) if task.report_failure => {
                return Ok((progress.failure(error, &tree), delivered));
            }
            Err(error) => return Err(error),
        };
        let mut report = progress.report(stop, &tree);
        // A probe sends no report (issue #2378). Its answer is for the
        // operator, not for a person.
        if task.deliver && task.probe.is_none() {
            let source = if woken {
                ReportSource::Followup
            } else {
                source
            };
            let answer = progress.delivered_text(&report.text);
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
        progress.answer = None;
        input = followup::WAKE_MESSAGE;
    }
}

/// The tree of an `agent_loop` run: its root and the labels of the task.
fn root_tree(ctx: &WorkflowContext, task: &AgentTask, run_id: &str) -> Tree {
    Tree {
        root: run_id.to_owned(),
        rounds: 0,
        delegations: 0,
        exercised: task
            .flow_labels
            .iter()
            .chain(task.mandate.iter().flat_map(|mandate| &mandate.flow_labels))
            .cloned()
            .collect(),
        released: Released::new(),
        records: Vec::new(),
        outbox: Outbox {
            from: run_id.to_owned(),
            self_id: ctx.workflow_id().to_owned(),
            waiting: task.waiting.clone(),
            sent: BTreeMap::new(),
            mesh: None,
        },
    }
}

/// Read the peers that `mandate` names, through one recorded activity.
///
/// A mandate with no peers needs no lookup, so it records nothing.
pub(crate) async fn discover(ctx: &WorkflowContext, mandate: &Mandate) -> Result<Member, String> {
    let peers = if mandate.peers.is_empty() {
        Vec::new()
    } else {
        let names: Vec<String> = mandate.peers.iter().cloned().collect();
        ctx.execute_activity(&agent_discover_peers_info(), names)
            .await
            .map_err(|e| e.to_string())?
    };
    let mut member = Member {
        mandate: mandate.clone(),
        peers,
        sample_schema: false,
    };
    // The gate runs only for a member that can offer a strategy, so each
    // other run records no marker.
    if sample_cap(&member.mandate, &member.peers).is_some() {
        member.sample_schema = ctx.patched(DELEGATE_SAMPLE_SCHEMA_PATCH);
    }
    Ok(member)
}

/// Send one report.
///
/// A failed send never fails the run: the failure is recorded, so replay
/// gives the same answer. Returns `true` when the delivery took the report.
///
/// # Errors
///
/// Returns an engine error that is not a failed send, such as a cancel.
pub(crate) async fn deliver(ctx: &WorkflowContext, report: Report) -> Result<bool, String> {
    tolerate(ctx.execute_activity(&agent_deliver_info(), report).await)
}

/// Send the notice of one parked call.
///
/// A failed notice never fails the run and never releases the call. A failed
/// or timed-out send is recorded, so replay gives the same answer. An input
/// over the cap records nothing, so [`notice_fits`] records the check first.
///
/// # Errors
///
/// Returns an engine error that is not a failed send, such as a cancel.
async fn notify(ctx: &WorkflowContext, call: ParkedCall) -> Result<bool, String> {
    tolerate(
        ctx.execute_activity(&agent_approval_notice_info(), call)
            .await,
    )
}

/// Read the result of one send. A failed send is `false`. Any other engine
/// error goes on.
fn tolerate(sent: HarvestResult<()>) -> Result<bool, String> {
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

/// Build the window of a new segment: the system prompt, the earlier
/// messages and the input.
///
/// A new segment can rebuild the window, so this checks that the last sent
/// window is still its prefix (issue #2264).
async fn open_segment(
    ctx: &WorkflowContext,
    task: &AgentTask,
    progress: &mut Progress,
    input: &str,
) -> Result<BTreeSet<String>, String> {
    let (system, labels) = system_prompt(ctx, task).await?;
    let mut messages = Vec::with_capacity(progress.messages.len() + 2);
    if let Some(system) = system {
        messages.push(ChatMessage::text(ChatRole::System, system));
    }
    messages.append(&mut progress.messages);
    if task.context_reset.is_some() && input == followup::WAKE_MESSAGE {
        progress.reset.woken_by = booking(&messages);
    }
    // A beam step continues its history, which holds the user message.
    if !task.beam_step {
        messages.push(ChatMessage::text(ChatRole::User, input));
    }
    if progress.reset.restore
        && let Some(notes) = &progress.reset.notes
    {
        messages.extend(context_reset::restored(&notes.text));
    }
    progress.messages = messages;
    progress.steers.clear();
    // A save turn that ended its segment has no reset to run.
    progress.reset.pending = false;
    progress.reset.kept = None;
    if progress
        .sent
        .as_ref()
        .is_some_and(|sent| !sent.is_prefix_of(&progress.messages))
    {
        progress.sent = None;
    }
    Ok(labels)
}

/// Check the model of `run` against its mandate (issue #2271).
///
/// A delegation checks the model of its peer. This check covers a root task
/// and a durable agent.
fn check_model(run: Run<'_>) -> Result<(), String> {
    let model = run.task.model.as_deref();
    match run.agent {
        Some(agent) if !agent.mandate.allows_model(model) => Err(format!(
            "the mandate of {:?} forbids the model {:?}",
            agent.mandate.holder(),
            model.unwrap_or(crate::DEFAULT_MODEL)
        )),
        _ => Ok(()),
    }
}

/// Run one segment: model turns and tool rounds until the model answers or a
/// bound ends it. Returns why the segment ended.
pub(crate) async fn run_segment(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    input: &str,
) -> Result<AgentStop, String> {
    let task = run.task;
    check_model(run)?;
    // The run read the memory entries, so it takes their labels.
    let read = open_segment(ctx, task, progress, input).await?;
    tree.exercised.extend(read);
    let extra_tools = run.extra_tools();

    loop {
        // A durable agent takes an interrupt and each steer before a turn.
        if run.live {
            if take_interrupt(ctx)? {
                progress.interrupted = true;
                return Ok(AgentStop::Interrupted);
            }
            if let Some(book) = progress.steer.as_mut() {
                let steers = take_steers(ctx, book)?;
                progress.messages.extend(steers.iter().cloned());
                progress.steers.extend(steers);
            }
        }
        // A delegation adds the child usage after a turn. A tree over its cap
        // makes no further call. A run with no delegation never starts a turn
        // over its cap, so this check changes nothing for it.
        if task
            .max_total_tokens
            .is_some_and(|max| progress.usage.total() > max)
        {
            return Ok(AgentStop::TokensExhausted);
        }
        let cap = task.request_cap();
        let (request, len) = sized_request(run, progress, tree, &extra_tools, input);
        // A request the engine would refuse is not sent. The refusal is not
        // retryable, so it would fail the run after earlier work was paid for.
        //
        // A task recorded before the whole-request budget read a zero cap as a
        // limit of zero bytes. Its run ended here, so a replay must end here.
        let legacy_zero_cap = cap == 0 && !task.whole_request_budget;
        if legacy_zero_cap || (cap > 0 && len > cap) {
            return Ok(AgentStop::TranscriptFull);
        }
        let turn: ModelTurn = match send_turn(ctx, run.live, progress, request).await {
            Ok(Some(turn)) => turn,
            Ok(None) => {
                progress.interrupted = true;
                return Ok(AgentStop::Interrupted);
            }
            // A spent mesh budget parks a durable agent until the period
            // ends (issue #2272).
            Err(error) if error.activity_error_type() == Some(MESH_BUDGET_EXCEEDED) => {
                progress.parked_until = mesh_refusal_until(&error);
                return Ok(AgentStop::BudgetExceeded);
            }
            // A spent LLM budget is a bound, so the run ends normally.
            Err(error) if LlmBudgetExceeded::is_refusal(&error) => {
                return Ok(AgentStop::BudgetExceeded);
            }
            Err(error) => return Err(error.to_string()),
        };

        progress.usage = progress.usage.saturating_add(turn.usage);
        progress.add_cost(turn.cost_usd_micros.unwrap_or(0));
        // A run with an answer check publishes a final answer only after the
        // check (issue #2442). A run with no check publishes each turn here.
        if progress.check.is_none() || !turn.calls().is_empty() {
            publish_turn_text(ctx, run, progress, &turn)?;
        }
        // The activity drops a reply that does not fit the result cap. The
        // turn then holds no text and no calls, so the segment ends here.
        if turn.oversized {
            return Ok(AgentStop::ReplyTooLarge);
        }
        let over_budget = task
            .max_total_tokens
            .is_some_and(|max| progress.usage.total() > max);
        let calls = turn.calls();

        if calls.is_empty() {
            match Box::pin(answer_turn(ctx, run, progress, &turn, over_budget)).await? {
                Some(stop) => return Ok(stop),
                None => continue,
            }
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
            releases: None,
            interruptible: false,
        };
        progress.steps = progress.steps.saturating_add(1);
        tree.rounds = tree.rounds.saturating_add(1);
        let narration = progress.last_text.clone();
        progress.push_assistant(&turn);

        match Box::pin(run_round(ctx, run, progress, tree, slot, &turn, calls)).await? {
            // A beam step ends after its first tool round.
            RoundEnd::Continue if task.beam_step => return Ok(AgentStop::StepEnded),
            RoundEnd::Continue => {}
            RoundEnd::Full { stopped } => {
                if !full_round_goes_on(ctx, run, progress, tree, input, stopped, narration) {
                    return Ok(AgentStop::TranscriptFull);
                }
            }
            RoundEnd::Loop => return Ok(AgentStop::LoopDetected),
            RoundEnd::Interrupted => {
                // The text of the turn can claim work that never ran.
                progress.last_text = narration;
                return Ok(AgentStop::Interrupted);
            }
        }
    }
}

/// Read the policy of the answer check of `task` and admit the check
/// (issue #2442). It runs before the first model call.
///
/// Returns the failed report of a refused check under `report_failure`.
///
/// # Errors
///
/// Returns the refusal, or the error of the policy read.
async fn admit_check(
    ctx: &WorkflowContext,
    task: &AgentTask,
    progress: &mut Progress,
    tree: &Tree,
) -> Result<Option<AgentReport>, String> {
    match answer_check::admit(ctx, task).await {
        Ok(check) => {
            progress.check = check;
            Ok(None)
        }
        Err(error) if task.report_failure => Ok(Some(progress.failure(error, tree))),
        Err(error) => Err(error),
    }
}

/// End the segment on `turn`, a turn with no calls, and return its stop.
///
/// `None` asks the model again. That is the reply to a save turn (issue
/// #2453), or an answer that the check rejected within its cap (issue
/// #2442). Only an answer that completes is checked.
///
/// With a check, the loop publishes the durable text of the turn here. A
/// rejected or unchecked answer then never reaches the durable stream.
async fn answer_turn(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    turn: &ModelTurn,
    over_budget: bool,
) -> Result<Option<AgentStop>, String> {
    let checked = progress.check.is_some();
    if save_reply(progress, turn, over_budget) {
        if checked {
            publish_turn_text(ctx, run, progress, turn)?;
        }
        return Ok(None);
    }
    let narration = progress.last_text.clone();
    progress.push_assistant(turn);
    let answer = turn.text();
    let stop = answer_stop(turn, over_budget);
    if stop != AgentStop::Completed {
        if checked {
            publish_turn_text(ctx, run, progress, turn)?;
        }
        progress.answer = Some(answer);
        return Ok(Some(stop));
    }
    match answer_check::check(ctx, run.task, progress, &answer).await? {
        Checked::Passed => {
            if checked {
                publish_turn_text(ctx, run, progress, turn)?;
            }
            progress.answer = Some(answer);
            Ok(Some(stop))
        }
        // The report keeps the last answer. No answer is delivered.
        Checked::Stop(AgentStop::AnswerRejected) => Ok(Some(AgentStop::AnswerRejected)),
        // A rejected or unchecked answer is not the text of the report.
        Checked::Retry => {
            progress.last_text = narration;
            Ok(None)
        }
        Checked::Stop(stop) => {
            progress.last_text = narration;
            Ok(Some(stop))
        }
    }
}

/// How a segment ends on `turn`, a turn with no calls.
fn answer_stop(turn: &ModelTurn, over_budget: bool) -> AgentStop {
    if over_budget {
        AgentStop::TokensExhausted
    } else if turn.stop == StopReason::MaxTokens {
        AgentStop::OutputCapped
    } else {
        AgentStop::Completed
    }
}

/// Whether a turn with no calls is the text reply to a save turn (issue
/// #2453). Such a reply has not ended the task. The reset drops it and runs
/// before the next request. A bound that ends the run still ends it.
fn save_reply(progress: &Progress, turn: &ModelTurn, over_budget: bool) -> bool {
    progress.reset.pending && !over_budget && turn.stop != StopReason::MaxTokens
}

/// Whether a segment goes on after a tool round over the cap.
///
/// The reset of a save round runs next, with the notes it has. Any other
/// round goes on only when [`reset_on_full_round`] resets it. A round that
/// the loop guard or an interrupt also stopped never goes on, so the stop
/// holds. A save round before that rule went on, so the gate keeps that
/// history (issue #2461).
///
/// Not every call ran, so the text of the turn is no answer. It can claim
/// work that never ran. So `narration`, the text before the turn, comes back
/// unless the save round goes on.
fn full_round_goes_on(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &Tree,
    input: &str,
    stopped: bool,
    narration: String,
) -> bool {
    let save_round = progress.reset.pending;
    if save_round && !(stopped && ctx.patched(RESET_ON_FULL_PATCH)) {
        return true;
    }
    progress.last_text = narration;
    !save_round && !stopped && reset_on_full_round(ctx, run, progress, tree, input)
}

/// Whether a tool round over the cap resets, and does not end the run
/// (issue #2461).
///
/// The reset keeps the round with its answers. A call that ran shows its
/// result or `DROPPED_FULL`, and a call that did not run shows
/// `NOT_RUN_FULL`. So the model does not run a call with an effect again.
///
/// The round resets when a reset is left and the window after the reset,
/// with the notes of that time, falls under the threshold. Then the next
/// request takes the reset, with a save turn first when one fits. The check
/// reads only recorded data, so replay takes the same branch.
fn reset_on_full_round(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &Tree,
    input: &str,
) -> bool {
    let Some(setting) = run.task.context_reset else {
        return false;
    };
    // The round pushed its assistant turn, then its tool message.
    let [.., call, answers] = progress.messages.as_slice() else {
        return false;
    };
    progress.reset.kept = Some([call.clone(), answers.clone()]);
    let after = reset_window(run.task, progress, input);
    let after_len = json_len(&next_request(
        run,
        progress,
        tree,
        &run.extra_tools(),
        after,
    ));
    let resets = full_round_resets(
        progress.reset.count,
        setting.max_resets,
        after_len,
        run.task.request_cap(),
        setting.reset_at_percent,
    ) && ctx.patched(RESET_ON_FULL_PATCH);
    if !resets {
        progress.reset.kept = None;
    }
    resets
}

/// The next request and its size, after a context reset when one is due
/// (issue #2453).
///
/// A reset reads only the recorded window, the cap and the reset count, so
/// replay takes the same reset.
fn sized_request(
    run: Run<'_>,
    progress: &mut Progress,
    tree: &Tree,
    extra_tools: &[ToolDefinition],
    input: &str,
) -> (ModelTurnRequest, u64) {
    let request = next_request(run, progress, tree, extra_tools, progress.messages.clone());
    let len = json_len(&request);
    if run.task.context_reset.is_none() {
        return (request, len);
    }
    match plan_reset(run, progress, tree, extra_tools, input, len) {
        Some(window) => {
            progress.messages = window;
            let request = next_request(run, progress, tree, extra_tools, progress.messages.clone());
            let len = json_len(&request);
            (request, len)
        }
        None => (request, len),
    }
}

/// Plan the context reset before one request of `len` bytes (issue
/// #2453). Returns the new window, or `None` to send the window as it is.
///
/// A due reset first asks for a save turn. The loop sends the window with a
/// fixed notice, and only `save_progress` runs in that round. The model so
/// reads each result before the reset drops it. The reset runs before the
/// next request.
///
/// The save turn needs room under the cap for the notice and the notes.
/// Without room, the reset runs at once. A reset whose window, with the
/// notes of that time, still reaches the threshold frees no room. So it does
/// not run, because it would only spend the next reset.
///
/// A round over the cap that kept its answers makes the reset due at once
/// (issue #2461). [`reset_on_full_round`] already checked its room.
fn plan_reset(
    run: Run<'_>,
    progress: &mut Progress,
    tree: &Tree,
    extra_tools: &[ToolDefinition],
    input: &str,
    len: u64,
) -> Option<Vec<ChatMessage>> {
    let setting = run.task.context_reset?;
    let cap = run.task.request_cap();
    if std::mem::take(&mut progress.reset.pending) {
        return Some(reset_now(run.task, progress, input));
    }
    if progress.reset.kept.is_none() {
        if !reset_due(
            progress.reset.count,
            setting.max_resets,
            len,
            cap,
            setting.reset_at_percent,
        ) {
            return None;
        }
        let after = reset_window(run.task, progress, input);
        let after_len = json_len(&next_request(run, progress, tree, extra_tools, after));
        if reaches_percent(after_len, cap, setting.reset_at_percent) {
            return None;
        }
    }
    let mut noticed = progress.messages.clone();
    noticed.push(ChatMessage::text(
        ChatRole::User,
        context_reset::RESET_NOTICE,
    ));
    let noticed_len = json_len(&next_request(
        run,
        progress,
        tree,
        extra_tools,
        noticed.clone(),
    ));
    if noticed_len.saturating_add(u64::from(setting.max_notes_bytes)) > cap {
        return Some(reset_now(run.task, progress, input));
    }
    progress.reset.pending = true;
    Some(noticed)
}

/// Count one reset and return its window. The window does not start with
/// the last sent window, so the next turn goes in full.
fn reset_now(task: &AgentTask, progress: &mut Progress, input: &str) -> Vec<ChatMessage> {
    progress.reset.count = progress.reset.count.saturating_add(1);
    progress.sent = None;
    let window = reset_window(task, progress, input);
    // A later reset drops the kept round like any other turn.
    progress.reset.kept = None;
    window
}

/// The window after a reset (issue #2453).
///
/// It holds the system prompt and the task. In a segment that a follow-up
/// woke, the booking that woke it follows. The wake message points at that
/// booking, so the wake message comes next. The booking that this segment
/// made follows that. A kept round over the cap follows (issue #2461). Then
/// the notes follow, and last the steers.
///
/// The kept round can hold the booking of the segment. Then the booking
/// shows only in the round, so no call id comes twice.
fn reset_window(task: &AgentTask, progress: &Progress, input: &str) -> Vec<ChatMessage> {
    let messages = &progress.messages;
    let mut window = Vec::with_capacity(10 + progress.steers.len());
    window.extend(
        messages
            .first()
            .filter(|message| message.role == ChatRole::System)
            .cloned(),
    );
    window.push(ChatMessage::text(ChatRole::User, task.input.clone()));
    // The segment starts at its last input message.
    let start = messages
        .iter()
        .rposition(|message| {
            message.role == ChatRole::User
                && message.content == [ContentPart::Text(input.to_owned())]
        })
        .unwrap_or(0);
    if input == followup::WAKE_MESSAGE {
        window.extend(progress.reset.woken_by.iter().flatten().cloned());
    }
    if input != task.input {
        window.push(ChatMessage::text(ChatRole::User, input));
    }
    let kept = progress.reset.kept.as_ref();
    if progress.followup.is_some() {
        let own = booking(&messages[start..])
            .filter(|[call, _]| !kept.is_some_and(|[round, _]| holds_calls_of(round, call)));
        window.extend(own.into_iter().flatten());
    }
    window.extend(kept.into_iter().flatten().cloned());
    if let Some(notes) = &progress.reset.notes {
        window.extend(context_reset::restored(&notes.text));
    }
    window.extend(progress.steers.iter().cloned());
    window
}

/// The last `schedule_followup` call of `messages` that booked a follow-up,
/// alone in its message, and its result. A refused call books nothing, so
/// it does not count.
fn booking(messages: &[ChatMessage]) -> Option<[ChatMessage; 2]> {
    let calls = messages.iter().rev().flat_map(|message| {
        message
            .content
            .iter()
            .rev()
            .filter(move |_| message.role == ChatRole::Assistant)
    });
    let (call, result) = calls
        .filter_map(|part| match part {
            ContentPart::ToolCall { id, name, .. } if name == FOLLOWUP_TOOL => {
                result_of(messages, id).map(|result| (part, result))
            }
            _ => None,
        })
        .find(|(_, result)| booked(result))?;
    Some([
        ChatMessage {
            role: ChatRole::Assistant,
            content: vec![call.clone()],
        },
        ChatMessage {
            role: ChatRole::Tool,
            content: vec![result.clone()],
        },
    ])
}

/// Whether `message` holds every call part of `calls`.
fn holds_calls_of(message: &ChatMessage, calls: &ChatMessage) -> bool {
    calls
        .content
        .iter()
        .all(|part| message.content.contains(part))
}

/// The result part of the call `id` in `messages`.
fn result_of<'a>(messages: &'a [ChatMessage], id: &str) -> Option<&'a ContentPart> {
    messages.iter().find_map(|message| {
        message.content.iter().find(
            |part| matches!(part, ContentPart::ToolResult { tool_call_id, .. } if tool_call_id == id),
        )
    })
}

/// Whether a `schedule_followup` result booked a follow-up. The loop guard
/// can add a note after the JSON, so only the first value counts.
fn booked(result: &ContentPart) -> bool {
    let ContentPart::ToolResult { content, .. } = result else {
        return false;
    };
    serde_json::Deserializer::from_str(content)
        .into_iter::<serde_json::Value>()
        .next()
        .and_then(Result::ok)
        .is_some_and(|value| value["scheduled"] == true)
}

/// One model turn. A durable agent races it against an interrupt.
///
/// Returns `None` when an interrupt won. The race cancels the turn and
/// records the winner, so replay takes the same branch.
async fn model_turn(
    ctx: &WorkflowContext,
    live: bool,
    request: ModelTurnRequest,
) -> HarvestResult<Option<ModelTurn>> {
    if !live {
        return ctx
            .execute_activity(&agent_model_turn_info(), request)
            .await
            .map(Some);
    }
    let winner = ctx
        .race()
        .activity(&agent_model_turn_info(), request)
        .signal(INTERRUPT_SIGNAL)
        .run()
        .await?;
    if winner.index == 0 {
        winner.decode().map(Some)
    } else {
        Ok(None)
    }
}

/// Send one model turn: as a delta on the last window when the run allows
/// it, else in the full form (issue #2264).
///
/// A delta whose prefix the store does not hold fails before any model
/// call. The turn then goes again in the full form. Both outcomes are
/// recorded, so replay takes the same path.
async fn send_turn(
    ctx: &WorkflowContext,
    live: bool,
    progress: &mut Progress,
    mut request: ModelTurnRequest,
) -> HarvestResult<Option<ModelTurn>> {
    if !progress.delta {
        return model_turn(ctx, live, request).await;
    }
    request.remember = true;
    let window = std::mem::take(&mut request.messages);
    if let Some(sent) = progress
        .sent
        .clone()
        .filter(|sent| sent.len <= window.len())
    {
        let delta = ModelTurnRequest {
            prefix: Some(PrefixRef {
                digest: sent.digest.clone(),
                len: u32::try_from(sent.len).unwrap_or(u32::MAX),
            }),
            messages: window[sent.len..].to_vec(),
            ..request.clone()
        };
        match model_turn(ctx, live, delta).await {
            Err(error) if error.activity_error_type() == Some(PREFIX_MISSING) => {}
            other => {
                if matches!(other, Ok(Some(_))) {
                    progress.sent = Some(sent.after(&window));
                }
                return other;
            }
        }
    }
    let sent = Sent::of(&window);
    request.messages = window;
    let turn = model_turn(ctx, live, request).await;
    if matches!(turn, Ok(Some(_))) {
        progress.sent = Some(sent);
    }
    turn
}

/// Publish the text of a finished turn to the durable stream (issue #2268).
///
/// A reader that reconnects gets each whole answer once. The call claims its
/// offset on replay too, so a replay publishes nothing twice.
fn publish_turn_text(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &Progress,
    turn: &ModelTurn,
) -> Result<(), String> {
    let text = turn.text();
    if !run.task.stream || text.is_empty() {
        return Ok(());
    }
    let record = crate::live::TurnText {
        kind: "turn".to_owned(),
        run_id: progress.run_id.clone(),
        step: progress.steps,
        agent: run.agent.map(|agent| agent.mandate.holder().to_owned()),
        text,
    };
    ctx.publish_durable_progress(record)
        .map_err(|err| err.to_string())
}

/// The period end of a mesh budget refusal, from its recorded details.
fn mesh_refusal_until(error: &HarvestError) -> Option<i64> {
    match error {
        HarvestError::ActivityFailed {
            details: Some(details),
            ..
        } => serde_json::from_value::<MeshBudgetExceeded>(details.clone())
            .ok()
            .map(|refusal| refusal.until_ms),
        _ => None,
    }
}

/// Take one waiting interrupt. Returns `true` when one was waiting.
fn take_interrupt(ctx: &WorkflowContext) -> Result<bool, String> {
    Ok(ctx
        .try_wait_for_signal(INTERRUPT_SIGNAL)
        .map_err(|err| err.to_string())?
        .is_some())
}

/// Take each waiting steer as a user message for the transcript.
///
/// A steer whose op already ran is a no-op. A payload that does not decode
/// is dropped: it carries no message id to keep.
fn take_steers(ctx: &WorkflowContext, book: &mut SteerBook) -> Result<Vec<ChatMessage>, String> {
    let mut messages = Vec::new();
    for raw in ctx
        .drain_signals_raw(STEER_SIGNAL)
        .map_err(|err| err.to_string())?
    {
        let Ok(steer) = serde_json::from_value::<Steer>(raw) else {
            continue;
        };
        if SteerBook::take(&mut book.ran, &steer.message_id) {
            continue;
        }
        messages.push(ChatMessage::text(ChatRole::User, steer.text));
        SteerBook::note(&mut book.steered, steer.message_id);
    }
    Ok(messages)
}

/// Check the mandate of `task`. Then find the peers that the mandate names,
/// if it has one.
///
/// # Errors
///
/// Returns an error when the mandate is not valid, or when the peer lookup
/// fails.
async fn root_member(ctx: &WorkflowContext, task: &AgentTask) -> Result<Option<Member>, String> {
    let Some(mandate) = &task.mandate else {
        return Ok(None);
    };
    mandate
        .validate()
        .map_err(|err| format!("the task mandate is not valid: {err}"))?;
    Ok(Some(discover(ctx, mandate).await?))
}

/// The system prompt of one segment: the task prompt, the skill list and
/// the memory snapshot.
///
/// The snapshot is read once per segment, by an activity, so the prompt does
/// not change inside the segment and replay reads the recorded snapshot.
///
/// Returns the prompt and the flow labels of the memory entries that it
/// holds (issue #2272).
async fn system_prompt(
    ctx: &WorkflowContext,
    task: &AgentTask,
) -> Result<(Option<String>, BTreeSet<String>), String> {
    // A skill name and summary join the prompt, so each segment checks them
    // first. A durable agent runs its segments outside `drive` (issue #2373).
    if let Some((name, reason)) = skill::first_invalid(&task.skills) {
        return Err(format!(
            "the skill {:?} is not valid: {reason}",
            truncate(name, crate::mesh::MAX_NAME_LEN)
        ));
    }
    // The list holds names and summaries, never a body (issue #2373).
    let system = match (task.system.as_deref(), skill::listing(&task.skills)) {
        (Some(system), Some(list)) if !system.is_empty() => Some(format!("{system}\n\n{list}")),
        (_, Some(list)) => Some(list),
        (system, None) => system.map(str::to_owned),
    };
    let Some(scope) = &task.memory_scope else {
        return Ok((system, BTreeSet::new()));
    };
    let snapshot: MemorySnapshot = ctx
        .execute_activity(&agent_memory_snapshot_info(), scope.clone())
        .await
        .map_err(|e| e.to_string())?;
    let prompt = system.as_ref().map_or_else(
        || snapshot.text().to_owned(),
        |system| format!("{system}\n\n{}", snapshot.text()),
    );
    Ok((Some(prompt), snapshot.labels()))
}

/// How one tool round ended.
enum RoundEnd {
    /// Every call has an answer. Ask the model again.
    Continue,
    /// The next request would exceed the request cap. `stopped` is true when
    /// the loop guard or an interrupt also stopped the round.
    Full { stopped: bool },
    /// The loop guard stopped the run.
    Loop,
    /// An interrupt ended the round (issue #2265).
    Interrupted,
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
    /// The flow rules that an approval of the call releases. `None` when no
    /// rule of the deciding worker can be released.
    releases: Option<&'a Released>,
    /// An interrupt may cancel the call once it started (issue #2265).
    interruptible: bool,
}

impl Slot<'_> {
    /// Add the recorded labels of the call to the tree. The decision lets
    /// the call run, so its labels count.
    fn exercise(self, tree: &mut Tree) {
        if let Some(labels) = self.labels {
            tree.exercised.extend(labels.iter().cloned());
        }
    }

    /// Release the rules that the call tripped. Only a person's approval
    /// calls this.
    fn release(self, tree: &mut Tree) {
        if let Some(releases) = self.releases {
            tree.released.extend(releases.iter().cloned());
        }
    }
}

/// The budget of one tool round: the whole next request, with a placeholder
/// answer for each call.
///
/// The request holds the transcript, the assistant turn and every answer. A
/// round that measured the results alone would run a call whose result no
/// request can carry (issue #2040).
///
/// The request also holds the flow state. A call that runs adds its labels,
/// and an approval adds its releases. The recorded turn names both, so the
/// budget adds them for every call that can run.
fn round_budget(
    run: Run<'_>,
    progress: &Progress,
    tree: &Tree,
    turn: &ModelTurn,
    calls: &[ToolCall],
) -> RoundBudget {
    let cap = run.task.request_cap();
    // A task recorded before the whole-request budget measured the results
    // alone. A run in flight must replay as it ran. A zero cap is no cap.
    if !run.task.whole_request_budget || cap == 0 {
        return RoundBudget::results_only(cap);
    }
    let placeholder = placeholder();
    let answers = calls
        .iter()
        .map(|call| ContentPart::ToolResult {
            tool_call_id: call.id.clone(),
            content: placeholder.clone(),
        })
        .collect();
    let mut messages = progress.messages.clone();
    messages.push(ChatMessage {
        role: ChatRole::Tool,
        content: answers,
    });
    let mut request = next_request(run, progress, tree, &run.extra_tools(), messages);
    (request.flow_labels, request.flow_released) = flow_after(tree, turn);
    RoundBudget::new(cap, json_len(&request), json_len(&placeholder))
}

/// The flow state after every call of `turn` that can run.
///
/// It adds the labels of each call that the decision does not deny. It adds
/// the releases of each call that waits for an approval.
fn flow_after(tree: &Tree, turn: &ModelTurn) -> (BTreeSet<String>, Released) {
    let mut exercised = tree.exercised.clone();
    let mut released = tree.released.clone();
    for (position, decision) in turn.decisions.iter().enumerate() {
        if matches!(decision, ToolDecision::Deny { .. }) {
            continue;
        }
        exercised.extend(turn.labels.get(position).into_iter().flatten().cloned());
        if matches!(decision, ToolDecision::RequireApproval { .. }) {
            released.extend(turn.releases.get(position).into_iter().flatten().cloned());
        }
    }
    (exercised, released)
}

/// Answer every call of one round in ONE tool message.
///
/// The next request must fit the request cap. When a placeholder answer for
/// each call does not fit, no call runs. The model could not read a result.
///
/// The round drops a result that does not fit, and runs no more calls. The
/// run then ends, so the model never reads a later result. With a reset left
/// and room in the reset window, the run resets instead (issue #2461). A
/// round that the loop guard or an interrupt also stopped never resets. No
/// more calls run after the loop guard stops the run either. Each call still
/// gets an answer, so the transcript stays valid and shows what ran.
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
    let mut budget = round_budget(run, progress, tree, turn, &calls);
    let plan = round_plan(&calls, &turn.decisions, run.agent, task.parallel_children);
    let mut results = Vec::with_capacity(calls.len());
    let mut full = false;
    let mut looped = false;
    let mut stale = false;
    // The outcomes of the rest of a fan-out that ran (issue #2270).
    let mut fanned: VecDeque<ToolOutcome> = VecDeque::new();
    for (position, call) in calls.iter().enumerate() {
        progress.tool_calls = progress.tool_calls.saturating_add(1);
        let ran = fanned.pop_front();
        // A durable agent takes an interrupt before each call. No later call
        // of the round runs after one. A call of a fan-out ran already.
        if ran.is_none()
            && run.live
            && !progress.interrupted
            && !full
            && !looped
            && take_interrupt(ctx)?
        {
            progress.interrupted = true;
        }
        let mut guard = Settle {
            task,
            budget: &mut budget,
            full: &mut full,
            looped: &mut looped,
        };
        let content = if let Some(outcome) = ran {
            guard.settle(progress, call, outcome.content)
        } else if *guard.full {
            ToolOutcome::error(NOT_RUN_FULL).content
        } else if *guard.looped {
            ToolOutcome::error(NOT_RUN_LOOP).content
        } else if progress.interrupted {
            ToolOutcome::error(NOT_RUN_INTERRUPTED).content
        } else if stale {
            ToolOutcome::error(NOT_RUN_STALE).content
        } else if progress.reset.pending && call.name != SAVE_PROGRESS_TOOL {
            // The reset drops each result of this round unread, so only the
            // notes tool runs.
            ToolOutcome::error(NOT_RUN_RESET).content
        } else if !guard.budget.can_run() {
            // A placeholder for each call exceeds the cap. No result could
            // reach the model, so no call runs.
            *guard.full = true;
            ToolOutcome::error(NOT_RUN_FULL).content
        } else {
            let batch = match plan.get(position) {
                Some(&Step::Lead(len)) => Some(position..position + len),
                _ => None,
            };
            let span = batch.clone().unwrap_or(position..position + 1);
            // With flow rules, the turn decided each call against the labels
            // before the round. A delegation can add the labels of its child.
            // The calls after it then hold a stale decision, so they do not run.
            let expected = recorded_labels(turn, span.clone()).map(|labels| {
                tree.exercised
                    .union(&labels)
                    .cloned()
                    .collect::<BTreeSet<_>>()
            });
            let before = tree.records.len();
            let (outcome, rest) = act(
                ctx, run, progress, tree, slot, turn, &calls, position, batch,
            )
            .await?;
            if let Some(&Step::Alone(reason)) = plan.get(position) {
                note_serial(ctx, tree, before, call, reason);
            }
            fanned = rest;
            stale = expected.is_some_and(|expected| !tree.exercised.is_subset(&expected));
            let mut guard = Settle {
                task,
                budget: &mut budget,
                full: &mut full,
                looped: &mut looped,
            };
            guard.settle(progress, call, outcome.content)
        };
        results.push(ContentPart::ToolResult {
            tool_call_id: call.id.clone(),
            content,
        });
    }
    progress.messages.push(ChatMessage {
        role: ChatRole::Tool,
        content: results,
    });
    Ok(if full {
        RoundEnd::Full {
            stopped: looped || progress.interrupted,
        }
    } else if looped {
        RoundEnd::Loop
    } else if progress.interrupted {
        RoundEnd::Interrupted
    } else {
        RoundEnd::Continue
    })
}

/// Run the call at `position`, or the fan-out `batch` that starts there.
///
/// Returns the outcome of the call, and the outcomes of the rest of the
/// batch.
#[allow(clippy::too_many_arguments)]
async fn act(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    turn: &ModelTurn,
    calls: &[ToolCall],
    position: usize,
    batch: Option<std::ops::Range<usize>>,
) -> Result<(ToolOutcome, VecDeque<ToolOutcome>), String> {
    if let (Some(span), Some(agent)) = (batch, run.agent) {
        for at in span.clone() {
            Slot {
                position: at,
                labels: turn.labels.get(at),
                ..slot
            }
            .exercise(tree);
        }
        let mut outcomes: VecDeque<ToolOutcome> =
            fan_out(ctx, run, agent, progress, tree, &calls[span])
                .await?
                .into();
        let first = outcomes
            .pop_front()
            .unwrap_or_else(|| ToolOutcome::error(NOT_RUN_FULL));
        return Ok((first, outcomes));
    }
    // A recorded turn always holds one decision per call. A missing one
    // denies the call rather than guess.
    let decision = turn
        .decisions
        .get(position)
        .cloned()
        .unwrap_or_else(|| ToolDecision::Deny {
            reason: "no recorded decision".to_owned(),
        });
    let at = Slot {
        position,
        labels: turn.labels.get(position),
        releases: turn.releases.get(position),
        interruptible: turn.interruptible.get(position).copied().unwrap_or(false),
        ..slot
    };
    let call = calls[position].clone();
    let outcome = decide(ctx, run, progress, tree, at, call, decision).await?;
    Ok((outcome, VecDeque::new()))
}

/// The union of the recorded labels of the calls in `span`, or `None` when
/// the turn recorded no labels.
fn recorded_labels(turn: &ModelTurn, span: std::ops::Range<usize>) -> Option<BTreeSet<String>> {
    let mut union: Option<BTreeSet<String>> = None;
    for labels in span.filter_map(|at| turn.labels.get(at)) {
        union
            .get_or_insert_with(BTreeSet::new)
            .extend(labels.iter().cloned());
    }
    union
}

/// The checks that each answered call of a round passes: the loop guard and
/// the request cap.
struct Settle<'a, 'b> {
    task: &'a AgentTask,
    budget: &'b mut RoundBudget,
    full: &'b mut bool,
    looped: &'b mut bool,
}

impl Settle<'_, '_> {
    /// Record the call with the loop guard, then fit its result to the
    /// request cap. A result that does not fit is dropped.
    fn settle(&mut self, progress: &mut Progress, call: &ToolCall, mut content: String) -> String {
        match progress
            .tracker
            .record(&self.task.loop_guard, &call.name, &call.arguments, &content)
        {
            LoopVerdict::Ok => {}
            LoopVerdict::Warn(repeats) => content.push_str(&warning_note(&call.name, repeats)),
            LoopVerdict::Stop(_) => *self.looped = true,
        }
        if !*self.full && self.budget.admit(json_len(&content)) {
            content
        } else {
            *self.full = true;
            ToolOutcome::error(DROPPED_FULL).content
        }
    }
}

/// The fan-out plan of one round (issues #2270 and #2483).
///
/// A batch is two or more calls in a row. The policy allows each call, and
/// each call delegates to an inline peer. A batch holds at most `bound`
/// calls. A gated delegation, or one to a durable peer, runs alone, as
/// before. The step of each delegation that runs alone names why.
fn round_plan(
    calls: &[ToolCall],
    decisions: &[ToolDecision],
    agent: Option<&Member>,
    bound: u32,
) -> Vec<Step> {
    let bound = usize::try_from(bound.min(MAX_PARALLEL_CHILDREN)).unwrap_or(1);
    let Some(agent) = agent else {
        return vec![Step::Skip; calls.len()];
    };
    let fits: Vec<Fit> = calls
        .iter()
        .enumerate()
        .map(|(position, call)| fit(call, decisions.get(position), agent))
        .collect();
    fan_out_plan(&fits, bound)
}

/// What one call is to a fan-out. A call to a peer that the roster does not
/// name is refused, so the plan does not touch it. A person can edit the
/// target of a gated call, so a gated call is `Gated` whatever peer it names.
fn fit(call: &ToolCall, decision: Option<&ToolDecision>, agent: &Member) -> Fit {
    if call.name != DELEGATE_TOOL {
        return Fit::Other;
    }
    match decision {
        None | Some(ToolDecision::Deny { .. }) => return Fit::Other,
        Some(ToolDecision::RequireApproval { .. }) => return Fit::Gated,
        Some(ToolDecision::Allow) => {}
    }
    // A call with a sample never joins a batch. Before the patch gate it
    // did not decode, so an old history plans the same round.
    if holds_sample(&call.arguments) {
        return Fit::Other;
    }
    let Some(peer) = delegate_args(&call.arguments)
        .and_then(|args| agent.peers.iter().find(|peer| peer.name == args.agent))
    else {
        return Fit::Other;
    };
    if peer.agent.is_some() {
        Fit::Durable
    } else {
        Fit::Batchable
    }
}

/// Write `reason` on the record of a delegation that ran alone (issue
/// #2483).
///
/// The call pushes its own record last, after the records of its subtree.
/// A refused delegation did not run, so its record keeps no reason. This
/// includes a durable peer that refuses the message. A message with no
/// reply went out, so its record keeps its serial reason (issue #2484). A
/// person can also reject a gated call. It then pushes no record.
fn note_serial(
    ctx: &WorkflowContext,
    tree: &mut Tree,
    before: usize,
    call: &ToolCall,
    reason: SerialReason,
) {
    let Some(record) = tree.records.get_mut(before..).and_then(<[_]>::last_mut) else {
        return;
    };
    if record.refusal.is_some_and(|refusal| !refusal.unanswered()) {
        return;
    }
    record.serial = Some(reason);
    // Replay runs this code again. The record is the durable trace, so the
    // log line fires only once.
    if !ctx.is_replaying() {
        tracing::debug!(
            call_id = %call.id,
            chain = ?record.chain,
            reason = reason.code(),
            "a delegation ran alone"
        );
    }
}

/// The longest answer that the round writes for a call with no result.
///
/// The round budget reserves it for every call, so any fixed answer fits the
/// next request in its place.
fn placeholder() -> String {
    [
        DROPPED_FULL,
        NOT_RUN_FULL,
        NOT_RUN_LOOP,
        NOT_RUN_STALE,
        NOT_RUN_INTERRUPTED,
        NOT_RUN_RESET,
        INTERRUPTED_CALL,
        INTERRUPTED_APPROVAL,
    ]
    .into_iter()
    .map(|message| ToolOutcome::error(message).content)
    .max_by_key(json_len)
    .unwrap_or_default()
}

/// The answer to a call whose result would exceed the request cap.
const DROPPED_FULL: &str =
    "the call ran, but its result was dropped: the next request would exceed the request cap";

/// The answer to a call that did not run because the next request would
/// exceed the request cap.
const NOT_RUN_FULL: &str = "the call did not run: the next request would exceed the request cap";

/// The answer to a call that did not run because the loop guard stopped the
/// run.
const NOT_RUN_LOOP: &str = "the call did not run: the loop guard stopped the run";

/// The answer to a call that did not run because a person interrupted the op.
const NOT_RUN_INTERRUPTED: &str = "the call did not run: a person interrupted this op";

/// The answer to a call that an interrupt cancelled while it ran. Only a tool
/// that opts in can get it.
const INTERRUPTED_CALL: &str = "the call was cancelled: a person interrupted this op";

/// The answer to a gated call whose approval wait an interrupt ended.
const INTERRUPTED_APPROVAL: &str =
    "denied: a person interrupted this op while the call waited for an approval";

/// The answer to a call of a save turn other than `save_progress` (issue
/// #2453). The reset after the turn drops each result unread.
const NOT_RUN_RESET: &str =
    "the call did not run: your context resets now, so only save_progress runs in this turn";

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
        ToolDecision::RequireApproval { reason } => {
            gated_call(ctx, run, progress, tree, slot, call, reason).await
        }
    }
}

/// Wait for a decision on one gated call, then run it or refuse it.
///
/// The call first sends one notice. The wait then races the call's own
/// approval signal against a durable deadline. A missing decision denies the
/// call, so an unattended run keeps moving. A payload that is not an
/// `Approval` denies the call too. It never fails the run.
async fn gated_call(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    slot: Slot<'_>,
    mut call: ToolCall,
    reason: String,
) -> Result<ToolOutcome, String> {
    let task = run.task;
    let name = approval_signal(slot.round, slot.position, &call.id);
    let timeout = Duration::from_secs(task.approval_timeout_secs);
    // A probe run sends no notice. Nobody answers it.
    if task.probe.is_none() && notice_gate(ctx)? {
        let parked = parked_call(ctx, run, progress, &name, &call, reason);
        // The result changes nothing. Only a decision releases the call.
        if notice_fits(ctx, &parked)? {
            notify(ctx, parked).await?;
        }
    }
    let decision = if run.live {
        let Some(decision) = live_decision(ctx, &name, timeout).await? else {
            progress.interrupted = true;
            return Ok(ToolOutcome::error(INTERRUPTED_APPROVAL));
        };
        decision
    } else {
        await_decision::<Approval>(ctx, &name, timeout).await?
    };
    match decision {
        Decision::Decided(Approval::Approve) => {
            slot.exercise(tree);
            slot.release(tree);
            run_tool(ctx, run, progress, tree, slot, call).await
        }
        Decision::Decided(Approval::Edit { arguments }) => {
            call.arguments = arguments;
            slot.exercise(tree);
            slot.release(tree);
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

/// Ask the patch gate of the notice. See [`APPROVAL_NOTICE_PROBE`].
fn notice_gate(ctx: &WorkflowContext) -> Result<bool, String> {
    ctx.try_wait_for_signal(APPROVAL_NOTICE_PROBE)
        .map_err(|err| err.to_string())?;
    Ok(ctx.patched(APPROVAL_NOTICE_PATCH))
}

/// Record whether the notice input fits the caps. See
/// [`APPROVAL_NOTICE_FITS`].
fn notice_fits(ctx: &WorkflowContext, parked: &ParkedCall) -> Result<bool, String> {
    let input = serde_json::to_value(parked).map_err(|err| err.to_string())?;
    let name = agent_approval_notice_info().name;
    ctx.side_effect(APPROVAL_NOTICE_FITS, || {
        ctx.activity_input_fits(name, &input)
    })
    .map_err(|err| err.to_string())
}

/// The notice input of one gated call.
///
/// The arguments are cut to the tool result cap. The reason is cut to the
/// reason cut of a full turn. Each escaped byte of the reason grows the input
/// up to six times, so a small cut keeps the whole input under the cap.
fn parked_call(
    ctx: &WorkflowContext,
    run: Run<'_>,
    progress: &Progress,
    signal: &str,
    call: &ToolCall,
    reason: String,
) -> ParkedCall {
    let task = run.task;
    let max = max_tool_result_bytes(task.request_cap());
    let (arguments, arguments_truncated) = cut_arguments(call.arguments.clone(), max);
    // The workflow id of a durable agent is its agent id. An inline child of
    // the agent runs in the same workflow, so it names the agent too.
    let agent_id = (ctx.workflow_type() == AGENT_WORKFLOW_NAME)
        .then(|| AgentId::parse(ctx.workflow_id()).ok())
        .flatten();
    ParkedCall {
        run_id: RunId::new(progress.run_id.clone()),
        execution_id: ctx.execution_id(),
        agent_id,
        session_id: task.session_id.clone(),
        signal: signal.to_owned(),
        tool: call.name.clone(),
        reason: cut_bytes(reason, REASON_CUT_BYTES.min(max)),
        arguments,
        arguments_truncated,
        timeout_secs: task.approval_timeout_secs,
    }
}

/// Wait for a decision on one gated call of a durable agent, or an interrupt.
///
/// Returns `None` when an interrupt won the race.
async fn live_decision(
    ctx: &WorkflowContext,
    name: &str,
    timeout: Duration,
) -> Result<Option<Decision<Approval>>, String> {
    // A list query shows the wait. The upsert appends no event.
    let status = |name: &str| {
        let attrs = [("agent_status".to_owned(), Some(serde_json::json!(name)))];
        if let Err(err) = ctx.upsert_search_attrs(attrs) {
            tracing::warn!(error = %err, "the agent status was not set");
        }
    };
    status("waiting_approval");
    let winner = ctx
        .race()
        .signal(name)
        .signal(INTERRUPT_SIGNAL)
        .timer(timeout)
        .run()
        .await
        .map_err(|err| err.to_string())?;
    status("running");
    Ok(match winner.index {
        0 => Some(decode_decision(Some(winner.value))),
        1 => None,
        _ => Some(Decision::TimedOut),
    })
}

/// Run one tool call.
///
/// The follow-up tool, the notes tool and the delegate tool run in the
/// workflow. Every other call is a durable activity.
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
            progress.final_turn_rules,
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
    if let Some(settings) = task
        .context_reset
        .filter(|_| call.name == SAVE_PROGRESS_TOOL)
    {
        return Ok(save_notes(&settings, progress, tree, &call));
    }
    // State memory is edited in the workflow, so the edit commits with the
    // op (issue #2273).
    if call.name == MEMORY_TOOL
        && task.memory_in_state
        && let Some(blocks) = progress.memory.as_mut()
    {
        // The tool is not offered to a segment that may not write memory. A
        // call to it anyway gets an error, so no policy can let it through.
        if !task.memory_writable() {
            return Ok(ToolOutcome::error(
                "the memory is read-only in this segment",
            ));
        }
        return Ok(state_memory_edit(blocks, &call, &tree.exercised));
    }
    // Under a mandate, the workflow owns the name `delegate`, even with no
    // peer to ask. Such a call gets a named refusal.
    if let Some(agent) = run.agent.filter(|_| call.name == DELEGATE_TOOL) {
        return delegate(ctx, run, agent, progress, tree, &call).await;
    }
    // The body comes from the recorded task, so the activity needs no
    // lookup (issue #2373). A run with no skills offers no such tool.
    let wants_skill = call.name == LOAD_SKILL_TOOL && !task.skills.is_empty();
    // A memory note keeps the labels of the tree (issue #2272).
    let flow_labels = if call.name == MEMORY_TOOL {
        tree.exercised.clone()
    } else {
        BTreeSet::new()
    };
    let mut request = ToolCallRequest {
        run_id: progress.run_id.clone(),
        session_id: task.session_id.clone(),
        step: progress.step_base.saturating_add(slot.step),
        call,
        memory_scope: task.memory_tool_scope(),
        read_only: task.read_only,
        allowed_tools: run.allowed_tools(),
        labels: slot.labels.cloned(),
        flow_labels,
        workspace: progress.workspace.clone(),
        skill: None,
        allowed_effects: task.allowed_effects.clone(),
        probe_effects: task.probe.map(crate::types::Probe::effects),
    };
    if wants_skill {
        request.skill = Some(skill::lookup(
            &task.skills,
            &request.call.arguments,
            skill_room(task.request_cap(), &request),
        ));
    }
    // Only a tool that opts in races an interrupt. Any other call runs to
    // its end, because its side effect may be half done.
    if run.live && slot.interruptible {
        let winner = ctx
            .race()
            .activity(&agent_tool_call_info(), request)
            .signal(INTERRUPT_SIGNAL)
            .run()
            .await
            .map_err(|e| e.to_string())?;
        if winner.index == 0 {
            let outcome = winner.decode().map_err(|e| e.to_string())?;
            return Ok(adopt_workspace(progress, outcome));
        }
        progress.interrupted = true;
        return Ok(ToolOutcome::error(INTERRUPTED_CALL));
    }
    let outcome = call_tool(ctx, request).await?;
    Ok(adopt_workspace(progress, outcome))
}

/// The bytes that the request cap `cap` leaves for a skill body in
/// `request` (issue #2373). The envelope of the lookup takes a few more
/// bytes. A cap of zero is no cap, so the room is zero too.
fn skill_room(cap: u64, request: &ToolCallRequest) -> u64 {
    /// The JSON around the body: the field name, the tag and the cut mark.
    const LOOKUP_ENVELOPE_BYTES: u64 = 64;
    if cap == 0 {
        return 0;
    }
    // A full request leaves room 1, not 0, so the body never goes uncut.
    cap.saturating_sub(json_len(request))
        .saturating_sub(LOOKUP_ENVELOPE_BYTES)
        .max(1)
}

/// Run one `save_progress` call (issue #2453). The notes live in the
/// workflow, so replay rebuilds them from the recorded call. They keep the
/// flow labels of the tree at the time of the save.
fn save_notes(
    settings: &crate::ContextReset,
    progress: &mut Progress,
    tree: &Tree,
    call: &ToolCall,
) -> ToolOutcome {
    match context_reset::notes_of(&call.arguments, settings) {
        Ok(text) => {
            let outcome = ToolOutcome::ok(
                serde_json::json!({"saved": true, "bytes": text.len()}).to_string(),
            );
            progress.reset.notes = Some(ProgressNotes {
                text,
                labels: tree.exercised.clone(),
            });
            outcome
        }
        Err(reason) => ToolOutcome::error(&reason),
    }
}

/// The most runs of one tool call whose workspace store fails (issue
/// #2274).
const WORKSPACE_CALL_RUNS: u32 = 3;

/// Run one tool-call activity.
///
/// The activity itself runs once, so a side effect never runs twice. A
/// failed workspace store is the one exception: the call runs again from
/// the same head, up to [`WORKSPACE_CALL_RUNS`] times. Only a tool that
/// opts into the workspace touches the store, so no other tool runs again.
/// Only a recorded result moves the head, so the snapshot of a failed run
/// never becomes the head.
async fn call_tool(ctx: &WorkflowContext, request: ToolCallRequest) -> Result<ToolOutcome, String> {
    let mut runs = 1;
    loop {
        match ctx
            .execute_activity(&agent_tool_call_info(), request.clone())
            .await
        {
            Ok(outcome) => return Ok(outcome),
            Err(err)
                if runs < WORKSPACE_CALL_RUNS
                    && matches!(
                        err.activity_error_type(),
                        Some(
                            crate::harness::WORKSPACE_STORE_FAILED
                                | crate::harness::WORKSPACE_TIMEOUT
                        )
                    ) =>
            {
                runs += 1;
            }
            Err(err) => return Err(err.to_string()),
        }
    }
}

/// Move the workspace head to the snapshot that a recorded call made (issue
/// #2274). Only a recorded result moves it, so the snapshot of a stale
/// attempt never becomes the head.
fn adopt_workspace(progress: &mut Progress, outcome: ToolOutcome) -> ToolOutcome {
    if let (Some(mount), Some(head)) = (progress.workspace.as_mut(), &outcome.workspace) {
        mount.head = Some(head.clone());
    }
    outcome
}

/// Apply one `memory` call to state memory, with the labels of the tree.
fn state_memory_edit(
    blocks: &mut [MemoryBlock],
    call: &ToolCall,
    labels: &BTreeSet<String>,
) -> ToolOutcome {
    let edit = serde_json::from_value::<MemoryOp>(call.arguments.clone())
        .map_err(|err| format!("not a valid memory edit: {err}"))
        .and_then(|op| apply_to(blocks, &op, labels).map_err(|err| format!("{err}")));
    match edit {
        Ok(block) => ToolOutcome::ok(edit_result(&block).to_string()),
        Err(reason) => ToolOutcome::error(&reason),
    }
}

/// The arguments of a `delegate` call, or `None` when they are not exactly
/// `agent` and `task`.
///
/// serde also decodes a struct from a JSON array, so the arguments must be
/// an object to have the named fields.
fn delegate_args(arguments: &serde_json::Value) -> Option<DelegateArgs> {
    arguments
        .is_object()
        .then(|| serde_json::from_value(arguments.clone()).ok())
        .flatten()
}

/// Whether the arguments of a `delegate` call hold the key `sample`,
/// whatever its value. Before the patch gate, such a call never decoded.
fn holds_sample(arguments: &serde_json::Value) -> bool {
    arguments.get("sample").is_some()
}

/// The arguments of a `delegate` call that the workflow runs.
///
/// A call that holds the key `sample` passes the patch gate first, also
/// with a `null` value. A history from before the gate reads `None`, as it
/// did then.
fn sampled_args(ctx: &WorkflowContext, arguments: &serde_json::Value) -> Option<DelegateArgs> {
    if holds_sample(arguments) && !ctx.patched(DELEGATE_SAMPLE_PATCH) {
        return None;
    }
    delegate_args(arguments)
}

/// The units of the nested delegations that ran: one for each delegation,
/// plus its draw units.
fn nested_units(records: &[DelegationRecord]) -> u32 {
    records
        .iter()
        .filter(|record| record.run_id.is_some())
        .fold(0_u32, |units, record| {
            units.saturating_add(1).saturating_add(record.draws)
        })
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
    agent: &Member,
    progress: &mut Progress,
    tree: &mut Tree,
    call: &ToolCall,
) -> Result<ToolOutcome, String> {
    let task = run.task;
    let admitted = sampled_args(ctx, &call.arguments)
        .ok_or((Refusal::InvalidArguments, String::new()))
        .and_then(|args| {
            admit(agent, task, progress, tree, &args.agent)
                .map(|child| (child, args.task, args.sample))
                .map_err(|refusal| (refusal, args.agent))
        });
    let (child, child_input, sample) = match admitted {
        Ok(admitted) => admitted,
        Err((refusal, name)) => return Ok(refuse_delegation(agent, tree, refusal, &name)),
    };
    let Admitted {
        mandate,
        peer,
        max_total_tokens,
    } = child;
    if peer.agent.is_some() {
        let ask = Ask {
            text: child_input,
            sample,
        };
        return Box::pin(durable_peer(ctx, run, agent, progress, tree, peer, ask)).await;
    }
    // An inline peer is a role, not a kind, so no sampling policy admits
    // its draws.
    if sample.is_some() {
        let refusal = Refusal::StrategyNeedsDurablePeer;
        return Ok(refuse_delegation(agent, tree, refusal, &peer.name));
    }
    tree.delegations = tree.delegations.saturating_add(1);
    let run_id = format!("{}:d{}", tree.root, tree.delegations);
    let child_agent = discover(ctx, &mandate).await?;
    let child_task = child_task(task, peer, child_input, mandate, max_total_tokens);
    let mut child = Progress::new(run_id.clone(), Vec::new());
    child.delta = progress.delta;
    let child_run = Run {
        task: &child_task,
        agent: Some(&child_agent),
        live: run.live,
    };
    let ended = Box::pin(run_segment(
        ctx,
        child_run,
        &mut child,
        tree,
        &child_task.input,
    ))
    .await;
    // A failed child paid for its turns, so the parent counts them.
    if ended.is_err() {
        progress.usage = progress.usage.saturating_add(child.usage);
        progress.add_cost(child.cost_usd_micros);
    }
    let stop = ended?;
    // An interrupt that ended the child ends the round of the parent too.
    if child.interrupted {
        progress.interrupted = true;
    }
    // The parent counts the whole subtree, as it does for usage.
    let steps = child.steps.saturating_add(child.delegated_steps);
    let tool_calls = child.tool_calls.saturating_add(child.delegated_tool_calls);
    progress.usage = progress.usage.saturating_add(child.usage);
    progress.add_cost(child.cost_usd_micros);
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
        resets: child.reset.count,
        answer_truncated: false,
        serial: None,
        draws: 0,
        idempotency_key: None,
    });
    let mut outcome = serde_json::json!({
        "agent": peer.name,
        "stop": stop,
    });
    AnswerRoom::of(ctx, task).put(tree, peer, &mut outcome, &child.last_text);
    let outcome = outcome.to_string();
    Ok(ToolOutcome {
        content: outcome,
        is_error: stop != AgentStop::Completed,
        workspace: None,
    })
}

/// The room for one delegation answer in the next request of the parent
/// (issue #2481).
#[derive(Clone, Copy)]
struct AnswerRoom<'a> {
    ctx: &'a WorkflowContext,
    /// The byte share of one result under the request cap of the parent.
    max_bytes: usize,
}

impl<'a> AnswerRoom<'a> {
    /// The room under the request cap of `task`.
    ///
    /// A tool result gets the same share of its result cap. Without this
    /// cut, an answer over the cap drops whole, and the run ends as
    /// `transcript_full`.
    fn of(ctx: &'a WorkflowContext, task: &AgentTask) -> Self {
        Self {
            ctx,
            max_bytes: max_tool_result_bytes(task.request_cap()),
        }
    }

    /// Put the answer of `peer` into `result`, and report a cut.
    ///
    /// A cut sets `answer_truncated` on the last record of `tree`, which
    /// is the record of this delegation. The operator also gets a warning.
    fn put(self, tree: &mut Tree, peer: &Peer, result: &mut serde_json::Value, answer: &str) {
        if !put_answer(result, peer, answer, self.max_bytes) {
            return;
        }
        if let Some(record) = tree.records.last_mut() {
            record.answer_truncated = true;
        }
        self.ctx.log_warn(&format!(
            "the answer of the peer {:?} is cut: it has {} characters, and the limit is {} characters or {} bytes",
            peer.name,
            answer.chars().count(),
            peer.answer_limit.unwrap_or(usize::MAX),
            self.max_bytes,
        ));
    }
}

/// Put the answer of `peer` into a delegation result (issue #2481).
///
/// It cuts the answer to the character limit of the peer. It also cuts the
/// encoded result to `max_bytes`, as a tool result is cut. The encoded size
/// counts each escape, so the request escape stays inside the share. The
/// `truncated` key tells the parent model whether text dropped. It returns
/// `true` on a cut.
///
/// Put every other key into `result` first, so the cut measures them.
///
/// A peer lookup from before issue #2481 has no limit. Then the result
/// keeps the old cut and the old shape, and the function returns `false`.
/// So that run replays as it ran.
fn put_answer(result: &mut serde_json::Value, peer: &Peer, answer: &str, max_bytes: usize) -> bool {
    let Some(limit) = peer.answer_limit else {
        result["answer"] = truncate(answer, LEGACY_ANSWER_LIMIT).into();
        return false;
    };
    // `false` is the longer flag, so the frame holds for either value.
    result["answer"] = "".into();
    result["truncated"] = false.into();
    let frame = result.to_string().len();
    let (shown, mut cut) = truncate_flagged(answer, limit);
    let shown = if frame.saturating_add(escaped_len(&shown)) <= max_bytes {
        shown
    } else {
        cut = true;
        let room = max_bytes
            .saturating_sub(frame)
            .saturating_sub(escaped_len(TRUNCATED));
        format!("{}{TRUNCATED}", escaped_prefix(answer, limit, room))
    };
    result["answer"] = shown.into();
    result["truncated"] = cut.into();
    cut
}

/// The longest prefix of the first `limit` characters of `text` whose JSON
/// escape fits `room` bytes.
fn escaped_prefix(text: &str, limit: usize, room: usize) -> &str {
    let mut used = 0_usize;
    for (taken, (index, c)) in text.char_indices().enumerate() {
        used = used.saturating_add(escaped_char_len(c));
        if taken >= limit || used > room {
            return &text[..index];
        }
    }
    text
}

/// The bytes of `text` inside a JSON string, without the quotes.
fn escaped_len(text: &str) -> usize {
    text.chars().map(escaped_char_len).sum()
}

/// The bytes of `c` inside a JSON string, as `serde_json` writes it.
const fn escaped_char_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\u{8}' | '\t' | '\n' | '\u{c}' | '\r' => 2,
        '\0'..='\u{1f}' => 6,
        _ => c.len_utf8(),
    }
}

/// Record a refused delegation and answer the model.
fn refuse_delegation(agent: &Member, tree: &mut Tree, refusal: Refusal, name: &str) -> ToolOutcome {
    // The model wrote the name, so a record keeps it only when it is a valid
    // agent name.
    let shown = if valid_name(name) {
        name.to_owned()
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
        resets: 0,
        answer_truncated: false,
        serial: None,
        draws: 0,
        idempotency_key: None,
    });
    refused(refusal, name)
}

/// One delegation of a fan-out that passed the mandate checks.
struct Sibling<'a> {
    position: usize,
    mandate: Mandate,
    peer: &'a Peer,
    input: String,
}

/// Run a batch of delegations at the same time (issue #2270).
///
/// Each child runs as a child `agent_loop` run, with its own history. One
/// history cannot hold two children that each take many steps. Replay pairs
/// each command with the next recorded event, so two children could swap
/// their results. A child run holds one result for the parent.
///
/// The siblings split the token remainder and the delegation remainder of
/// the tree with [`sibling_cap`], so the batch never spends more than one
/// child could. Each child starts with the labels of the tree. Its labels
/// join the tree when the batch ends.
///
/// A failed child fails the batch. The reports of the other children still
/// join the parent first. The batch returns the first error in input order.
///
/// Returns one outcome per call, in order.
async fn fan_out(
    ctx: &WorkflowContext,
    run: Run<'_>,
    agent: &Member,
    progress: &mut Progress,
    tree: &mut Tree,
    calls: &[ToolCall],
) -> Result<Vec<ToolOutcome>, String> {
    let task = run.task;
    let mut outcomes: Vec<Option<ToolOutcome>> = vec![None; calls.len()];
    let mut siblings = Vec::new();
    for (position, call) in calls.iter().enumerate() {
        let taken = u32::try_from(siblings.len()).unwrap_or(u32::MAX);
        let used = tree.delegations.saturating_add(taken);
        let admitted = sampled_args(ctx, &call.arguments)
            .ok_or((Refusal::InvalidArguments, String::new()))
            .and_then(|args| {
                // A batch holds inline peers only, and an inline peer runs
                // no strategy.
                let refusal = args
                    .sample
                    .is_some()
                    .then_some(Refusal::StrategyNeedsDurablePeer);
                sibling(agent, used, &args.agent)
                    .and_then(|admitted| refusal.map_or(Ok(admitted), Err))
                    .map(|(mandate, peer)| Sibling {
                        position,
                        mandate,
                        peer,
                        input: args.task,
                    })
                    .map_err(|refusal| (refusal, args.agent))
            });
        match admitted {
            Ok(admitted) => siblings.push(admitted),
            Err((refusal, name)) => {
                outcomes[position] = Some(refuse_delegation(agent, tree, refusal, &name));
            }
        }
    }
    let count = u32::try_from(siblings.len()).unwrap_or(u32::MAX);
    let tokens_left = task
        .max_total_tokens
        .map(|max| max.saturating_sub(progress.usage.total()));
    let delegations_left = agent
        .mandate
        .max_delegations
        .saturating_sub(tree.delegations.saturating_add(count));
    // A child reports its failure, so the parent keeps its usage. The gate
    // runs only when a child starts. Replay reaches it at the same point
    // (issue #2328).
    let report_failure = !siblings.is_empty() && ctx.patched(CHILD_FAILURE_REPORT_PATCH);
    let mut children = Vec::new();
    for (index, sibling) in (0_u32..).zip(siblings) {
        let share = tokens_left.map(|left| sibling_cap(left, count, index));
        let cap = match (share, sibling.peer.max_total_tokens) {
            (Some(share), Some(own)) => Some(share.min(own)),
            (share, own) => share.or(own),
        };
        // A cap of zero would still let the child make one model call.
        if cap == Some(0) {
            let refusal =
                refuse_delegation(agent, tree, Refusal::BudgetExhausted, &sibling.peer.name);
            outcomes[sibling.position] = Some(refusal);
            continue;
        }
        let mut mandate = sibling.mandate;
        // The child run counts its own delegations from zero.
        mandate.max_delegations = sibling_cap(delegations_left, count, index);
        tree.delegations = tree.delegations.saturating_add(1);
        let mut child = child_task(task, sibling.peer, sibling.input, mandate, cap);
        child.run_id_source = RunIdSource::ExecutionId;
        child.report_failure = report_failure;
        child.flow_labels.clone_from(&tree.exercised);
        // The parent waits on the child run, so a send from the child back
        // up the chain is a deadlock. Only a durable agent can get such a
        // send, so only an agent id joins the chain.
        child.waiting.clone_from(&tree.outbox.waiting);
        if crate::agent::AgentId::parse(&tree.outbox.self_id).is_ok() {
            child.waiting.push(tree.outbox.self_id.clone());
        }
        children.push((sibling.position, sibling.peer, child));
    }
    // The engine fan-out records the children in order and waits for all
    // of them, so the result order never depends on completion order.
    let mut inputs = Vec::with_capacity(children.len());
    for (_, _, child) in &children {
        let input = serde_json::to_value(child).map_err(|err| err.to_string())?;
        inputs.push((WORKFLOW_NAME.to_owned(), input));
    }
    let ended = ctx
        .spawn_child_workflow_fan_out_collect_raw(inputs)
        .await
        .map_err(|err| err.to_string())?;
    // Each sibling ended, so every report joins the parent. The first
    // failure in input order fails the batch.
    let mut failed = None;
    let room = AnswerRoom::of(ctx, task);
    for ((position, peer, child), output) in children.into_iter().zip(ended) {
        match merge_ended(room, progress, tree, peer, child, output) {
            Ok(outcome) => outcomes[position] = Some(outcome),
            Err(err) => {
                failed.get_or_insert(err);
            }
        }
    }
    if let Some(err) = failed {
        return Err(err);
    }
    Ok(outcomes
        .into_iter()
        .map(|outcome| outcome.unwrap_or_else(|| ToolOutcome::error(NOT_RUN_FULL)))
        .collect())
}

/// Merge the end of one fan-out child into the parent.
///
/// A child that reported its failure still joins the parent. Then it counts
/// as a failure like any other (issue #2328).
fn merge_ended(
    room: AnswerRoom<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    peer: &Peer,
    child: AgentTask,
    output: Result<serde_json::Value, String>,
) -> Result<ToolOutcome, String> {
    let report = output.and_then(|value| {
        serde_json::from_value::<AgentReport>(value).map_err(|err| err.to_string())
    })?;
    let outcome = merge_child(room, progress, tree, peer, child, &report);
    if report.stop == AgentStop::Failed {
        return Err(report
            .error
            .unwrap_or_else(|| "the child run failed".to_owned()));
    }
    Ok(outcome)
}

/// The mandate checks of one sibling. Returns its mandate and its peer.
fn sibling<'a>(agent: &'a Member, used: u32, name: &str) -> Result<(Mandate, &'a Peer), Refusal> {
    if !agent.mandate.peers.contains(name) {
        return Err(Refusal::NotPermitted);
    }
    let peer = agent
        .peers
        .iter()
        .find(|peer| peer.name == name)
        .ok_or(Refusal::UnknownPeer)?;
    Ok((agent.mandate.delegate(peer, used)?, peer))
}

/// Add the report of one child run to the parent, and answer the model.
fn merge_child(
    room: AnswerRoom<'_>,
    progress: &mut Progress,
    tree: &mut Tree,
    peer: &Peer,
    child: AgentTask,
    report: &AgentReport,
) -> ToolOutcome {
    progress.usage = progress.usage.saturating_add(report.usage);
    progress.add_cost(report.cost_usd_micros);
    progress.delegated_steps = progress.delegated_steps.saturating_add(report.steps_used);
    progress.delegated_tool_calls = progress
        .delegated_tool_calls
        .saturating_add(report.tool_calls);
    tree.exercised.extend(report.flow_labels.iter().cloned());
    tree.delegations = tree
        .delegations
        .saturating_add(nested_units(&report.delegations));
    tree.records.extend(report.delegations.iter().cloned());
    tree.records.push(DelegationRecord {
        chain: child
            .mandate
            .map(|mandate| mandate.chain)
            .unwrap_or_default(),
        run_id: report.run_id.clone(),
        stop: Some(report.stop),
        refusal: None,
        usage: report.usage,
        steps_used: report.steps_used,
        tool_calls: report.tool_calls,
        resets: report.resets,
        answer_truncated: false,
        serial: None,
        draws: 0,
        idempotency_key: None,
    });
    let mut outcome = serde_json::json!({
        "agent": peer.name,
        "stop": report.stop,
    });
    room.put(tree, peer, &mut outcome, &report.text);
    let outcome = outcome.to_string();
    ToolOutcome {
        content: outcome,
        is_error: report.stop != AgentStop::Completed,
        workspace: None,
    }
}

/// Send one task to a durable peer and wait for its reply (issue #2269).
///
/// The send is recorded with the key `<from>:<seq>`, so replay does not send
/// it again. The reply is a durable promise. The caller races it against the
/// reply deadline, and a durable agent also races it against an interrupt.
async fn durable_peer(
    ctx: &WorkflowContext,
    run: Run<'_>,
    agent: &Member,
    progress: &mut Progress,
    tree: &mut Tree,
    peer: &Peer,
    ask: Ask,
) -> Result<ToolOutcome, String> {
    let target = peer.agent.as_deref().and_then(|id| AgentId::parse(id).ok());
    let Some(target) = target else {
        return Ok(refuse(agent, tree, Refusal::PeerUnavailable, &peer.name));
    };
    // A durable peer runs its own op with its own effects. A mandate cannot
    // carry an effect set, so a narrowed run never asks one (issue #2439).
    // A probe never asks one for the same reason (issue #2378).
    if run.task.allowed_effects.is_some() || run.task.probe.is_some() {
        return Ok(refuse(agent, tree, Refusal::NotPermitted, &peer.name));
    }
    let target_id = target.as_str();
    if target_id == tree.outbox.self_id || tree.outbox.waiting.iter().any(|id| id == target_id) {
        return Ok(refuse(agent, tree, Refusal::Deadlock, &peer.name));
    }
    let mut mandate = match agent
        .mandate
        .outbound(peer, tree.delegations, &tree.exercised)
    {
        Ok(mandate) => mandate,
        Err(refusal) => return Ok(refuse(agent, tree, refusal, &peer.name)),
    };
    // A read-only segment sends a read-only mandate, as an inline child
    // inherits it (issue #2319).
    mandate.read_only |= run.task.read_only;
    // The caller admits the draws of a strategy under its own tree before
    // it sends anything. The cap of the peer role narrows its cap.
    let draws = match &ask.sample {
        Some(sample) => {
            let strategy = &sample.strategy;
            let admitted = agent.mandate.admit_delegated_draws(
                peer.max_draws,
                strategy.draws(),
                strategy.counts_fan_out(),
                tree.delegations,
            );
            match admitted {
                Ok(units) => units,
                Err(refusal) => return Ok(refuse(agent, tree, refusal, &peer.name)),
            }
        }
        None => 0,
    };
    tree.delegations = tree.delegations.saturating_add(1);
    let seq = tree.outbox.next_seq(target_id);
    let mut promise = ctx.new_promise().map_err(|err| err.to_string())?;
    let mut waiting = tree.outbox.waiting.clone();
    waiting.push(tree.outbox.self_id.clone());
    let envelope = PeerEnvelope {
        from: tree.outbox.from.clone(),
        sender: agent.mandate.holder().to_owned(),
        seq,
        text: ask.text,
        mandate: mandate.clone(),
        flow_labels: tree.exercised.clone(),
        reply_to: Some(promise.id().to_string()),
        waiting,
        mesh: tree.outbox.mesh.clone(),
        sample: ask.sample,
    };
    let key = format!("{}:{seq}", envelope.from);
    let sent = ctx
        .signal_external_workflow_by_id_with_idempotency(
            AGENT_WORKFLOW_NAME,
            target_id,
            ENTITY_OP_SIGNAL,
            EntityMessage::op(AgentOp::PeerMessage(envelope)),
            key.clone(),
        )
        .await;
    match sent {
        // The draw units count once the message is out. A refused send
        // runs no draw, and its record keeps no unit.
        Ok(()) => tree.delegations = tree.delegations.saturating_add(draws),
        Err(HarvestError::ExternalSignalFailed { .. }) => {
            return Ok(refuse(agent, tree, Refusal::PeerUnavailable, &peer.name));
        }
        Err(other) => return Err(other.to_string()),
    }
    let verdict_text = ctx.patched(REPLY_VERDICT_PATCH);
    let timeout = Duration::from_secs(
        peer.reply_timeout_secs
            .unwrap_or(crate::mesh::DEFAULT_REPLY_TIMEOUT_SECS),
    );
    let wait = ReplyWait {
        live: run.live,
        timeout,
        verdict_text,
    };
    let outcome = await_reply(ctx, wait, progress, &mut promise).await?;
    let mut chain = mandate.chain;
    if chain.last().is_none_or(|last| *last != peer.name) {
        chain.push(peer.name.clone());
    }
    let room = AnswerRoom::of(ctx, run.task);
    let sent = PeerSent {
        target_id,
        chain,
        draws,
        key,
        verdict_text,
    };
    Ok(peer_outcome(room, tree, peer, sent, outcome))
}

/// What a `delegate` call asks a durable peer.
struct Ask {
    /// The task text.
    text: String,
    /// The strategy, if the call asks for one.
    sample: Option<crate::sampling::OpSample>,
}

/// One message that a caller sent to a durable peer.
struct PeerSent<'a> {
    /// The workflow id of the peer.
    target_id: &'a str,
    /// The chain of the record.
    chain: Vec<String>,
    /// The draw units that the caller counted.
    draws: u32,
    /// The idempotency key of the message.
    key: String,
    /// The value of [`REPLY_VERDICT_PATCH`] for this message.
    verdict_text: bool,
}

/// The most characters of a no-reply cause that the model reads. A caller
/// outside the run writes the reason of a rejection, so the cause has a
/// bound.
const MAX_NO_REPLY_CAUSE_CHARS: usize = 256;

/// Why a durable peer sent no reply (issue #2484).
#[derive(Debug, Clone, PartialEq, Eq)]
enum NoReply {
    /// The deadline passed.
    Timeout(Duration),
    /// A caller rejected the reply promise, with this reason.
    Rejected(String),
    /// A person interrupted the wait.
    Interrupted,
}

impl NoReply {
    /// The verdict of the cause.
    const fn verdict(&self) -> Refusal {
        match self {
            Self::Timeout(_) => Refusal::ReplyTimeout,
            Self::Rejected(_) => Refusal::ReplyRejected,
            Self::Interrupted => Refusal::ReplyInterrupted,
        }
    }

    /// The cause in words.
    fn cause(&self) -> String {
        match self {
            Self::Timeout(timeout) => {
                format!("no reply arrived within {}s", timeout.as_secs())
            }
            Self::Rejected(reason) => reason.clone(),
            Self::Interrupted => {
                "a person interrupted this op while it waited for the reply".to_owned()
            }
        }
    }

    /// The error text that the model reads: the verdict text, then the cause
    /// cut to [`MAX_NO_REPLY_CAUSE_CHARS`].
    fn error(&self, peer: &str) -> String {
        let cause = crate::harness::truncate(&self.cause(), MAX_NO_REPLY_CAUSE_CHARS);
        format!("{}: {cause}", self.verdict().message(peer))
    }

    /// The error text of a history from before [`REPLY_VERDICT_PATCH`]. It
    /// must not change, so it keeps the whole cause.
    fn legacy(&self, peer: &str) -> String {
        format!(
            "the durable agent {peer:?} gave no answer: {}",
            self.cause()
        )
    }
}

/// Record one exchange with a durable peer and answer the model.
fn peer_outcome(
    room: AnswerRoom<'_>,
    tree: &mut Tree,
    peer: &Peer,
    sent: PeerSent<'_>,
    outcome: Result<PeerReply, NoReply>,
) -> ToolOutcome {
    let PeerSent {
        target_id,
        chain,
        draws,
        key,
        verdict_text,
    } = sent;
    match outcome {
        Ok(reply) => {
            // The answer enters this context, so the labels of the peer count.
            tree.exercised.extend(reply.flow_labels.iter().cloned());
            tree.records.push(DelegationRecord {
                chain,
                run_id: Some(target_id.to_owned()),
                stop: reply.stop,
                refusal: reply.refusal,
                usage: TokenUsage::new(u32::try_from(reply.tokens).unwrap_or(u32::MAX), 0),
                steps_used: 0,
                tool_calls: 0,
                resets: 0,
                answer_truncated: false,
                serial: None,
                draws,
                idempotency_key: Some(key),
            });
            if let Some(refusal) = reply.refusal {
                refused(refusal, &peer.name)
            } else {
                // The keys keep their old order, also under the
                // `preserve_order` feature of `serde_json`.
                // The answer key comes first, then `error`, so the cut
                // measures the error too.
                let mut content = serde_json::json!({
                    "agent": peer.name,
                    "stop": reply.stop,
                });
                content["answer"] = "".into();
                content["error"] = serde_json::json!(reply.error);
                room.put(tree, peer, &mut content, &reply.answer);
                let content = content.to_string();
                ToolOutcome {
                    content,
                    is_error: reply.stop != Some(AgentStop::Completed),
                    workspace: None,
                }
            }
        }
        Err(missing) => {
            // The message went out, so the record keeps the run id. A
            // parent then still counts its unit and its draws. A history
            // from before the gate keeps its old record with no verdict.
            tree.records.push(DelegationRecord {
                chain,
                run_id: Some(target_id.to_owned()),
                stop: None,
                refusal: verdict_text.then(|| missing.verdict()),
                usage: TokenUsage::default(),
                steps_used: 0,
                tool_calls: 0,
                resets: 0,
                answer_truncated: false,
                serial: None,
                draws,
                idempotency_key: Some(key),
            });
            if verdict_text {
                ToolOutcome {
                    content: serde_json::json!({
                        "error": missing.error(&peer.name),
                        "verdict": missing.verdict(),
                    })
                    .to_string(),
                    is_error: true,
                    workspace: None,
                }
            } else {
                ToolOutcome::error(&missing.legacy(&peer.name))
            }
        }
    }
}

/// How one caller waits for the reply of a durable peer.
#[derive(Debug, Clone, Copy)]
struct ReplyWait {
    /// `true` for a durable agent. It also races the wait against an
    /// interrupt.
    live: bool,
    /// The reply deadline.
    timeout: Duration,
    /// The value of [`REPLY_VERDICT_PATCH`] for this message.
    verdict_text: bool,
}

/// Wait for the reply of a durable peer until its deadline.
///
/// A durable agent also races the wait against an interrupt. Returns the
/// reply, or why there is none.
async fn await_reply(
    ctx: &WorkflowContext,
    wait: ReplyWait,
    progress: &mut Progress,
    promise: &mut autumn_harvest::durable_promise::DurablePromise<'_>,
) -> Result<Result<PeerReply, NoReply>, String> {
    let ReplyWait {
        live,
        timeout,
        verdict_text,
    } = wait;
    if !live {
        let settled = promise
            .wait_timeout::<serde_json::Value>(timeout)
            .await
            .map_err(|err| err.to_string())?;
        return match settled {
            None => Ok(Err(NoReply::Timeout(timeout))),
            Some(Ok(value)) => decode_reply(value, verdict_text),
            Some(Err(rejected)) => Ok(Err(NoReply::Rejected(rejected.error))),
        };
    }
    let winner = ctx
        .race()
        .signal(&promise.id().signal_name())
        .signal(INTERRUPT_SIGNAL)
        .timer(timeout)
        .run()
        .await
        .map_err(|err| err.to_string())?;
    Ok(match winner.index {
        0 => match PromiseSettlement::decode::<serde_json::Value>(winner.value)
            .map_err(|err| err.to_string())?
        {
            Ok(value) => decode_reply(value, verdict_text)?,
            Err(rejected) => Err(NoReply::Rejected(rejected.error)),
        },
        1 => {
            progress.interrupted = true;
            Err(NoReply::Interrupted)
        }
        _ => Err(NoReply::Timeout(timeout)),
    })
}

/// Decode the value of a resolved reply.
///
/// A caller outside the run can resolve the promise with any value. With
/// the gate, a value that is not a reply is a rejected reply, so the record
/// keeps its verdict and its key (issue #2484). Without the gate, it fails
/// the call with the old error text.
fn decode_reply(
    value: serde_json::Value,
    verdict_text: bool,
) -> Result<Result<PeerReply, NoReply>, String> {
    match serde_json::from_value::<PeerReply>(value) {
        Ok(reply) => Ok(Ok(reply)),
        Err(err) if verdict_text => Ok(Err(NoReply::Rejected(format!(
            "the reply does not decode: {err}"
        )))),
        Err(err) => Err(HarvestError::from(err).to_string()),
    }
}

/// Record a refused delegation to `name` and answer the model.
fn refuse(agent: &Member, tree: &mut Tree, refusal: Refusal, name: &str) -> ToolOutcome {
    let mut chain = agent.mandate.chain.clone();
    chain.push(name.to_owned());
    tree.records.push(DelegationRecord {
        chain,
        run_id: None,
        stop: None,
        refusal: Some(refusal),
        usage: TokenUsage::default(),
        steps_used: 0,
        tool_calls: 0,
        resets: 0,
        answer_truncated: false,
        serial: None,
        draws: 0,
        idempotency_key: None,
    });
    refused(refusal, name)
}

/// The task of a delegated child. It keeps the bounds of the parent task and
/// takes the prompt and the limits of the peer.
///
/// Every field is named, so a new field of [`AgentTask`] must be placed here.
fn child_task(
    task: &AgentTask,
    peer: &Peer,
    input: String,
    mandate: Mandate,
    max_total_tokens: Option<u32>,
) -> AgentTask {
    AgentTask {
        input,
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
        read_only: task.read_only || mandate.read_only,
        unattended: task.unattended,
        unattended_memory_writes: false,
        run_id_source: task.run_id_source,
        whole_request_budget: task.whole_request_budget,
        tenant: task.tenant.clone(),
        mandate: Some(mandate),
        // The tree holds the labels, so the child task carries none.
        flow_labels: BTreeSet::new(),
        mesh: task.mesh.clone(),
        stream: task.stream,
        // A child never edits the memory of its parent.
        memory_in_state: false,
        transcript_delta: task.transcript_delta,
        parallel_children: task.parallel_children,
        model: peer.model.clone().or_else(|| task.model.clone()),
        // A child has no workspace, so siblings never race on one head
        // (issue #2274).
        workspace: None,
        waiting: Vec::new(),
        // Only a fan-out sets it, after its gate.
        report_failure: false,
        // A child keeps the setting. Its new run holds no notes of the
        // parent (issue #2453).
        context_reset: task.context_reset,
        // The peer has its own prompt, so the guidance and the skills of the
        // parent do not apply to it (issue #2373).
        tool_guidance: std::collections::BTreeMap::new(),
        skills: std::collections::BTreeMap::new(),
        // A child never runs a wider effect than its parent (issue #2439).
        allowed_effects: task.allowed_effects.clone(),
        // A child is part of the draw of its parent, so its requests keep
        // the draw id. It has its own run id, so it drops the root.
        draw: task.draw.as_ref().map(Draw::inherited),
        // Only the answer of the root run is checked (issue #2442).
        answer_check: None,
        // A child of a probe is a probe with the same effects (issue #2378).
        probe: task.probe,
        // A peer is another agent, so it is not of the kind of the parent.
        kind: None,
        // A child runs its whole task. Only the root of a step stops early.
        beam_step: false,
    }
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
    agent: &'a Member,
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
        workspace: None,
    }
}

/// The installed harness, or an error that names the fix.
pub(crate) fn harness(ctx: &ActivityContext) -> Result<&AgentHarness, String> {
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
/// The turn first runs the LLM budget check of its workflow type (issue
/// #1997). A spent budget refuses the turn before the model call, and the
/// loop ends under `budget_exceeded`.
///
/// A turn that calls the model records one call in the agent cost ledger
/// (issue #1996). `AgentModel::model_id` names the model, and
/// `AgentModel::cost_usd_micros` prices the call. A response cache hit calls
/// no model and spends no tokens, so it records no call.
///
/// A reply over the result cap does not fail the activity. The activity cuts
/// the decision reasons. When the reply still does not fit, it returns a turn
/// with `oversized` set, and the loop ends under `reply_too_large`.
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
    harness.check_budget(|| ctx.check_llm_budget()).await?;
    let tenant = cache_tenant(ctx, harness, &request).await;
    let model = request.model.clone();
    // Live text goes to the progress channel of the run. Nothing is stored.
    let publish = |event: &crate::live::LiveEvent| {
        if let Err(err) = ctx.publish_progress(event) {
            tracing::debug!(error = %err, "a live chunk was dropped");
        }
    };
    let (turn, latency) = harness
        .model_turn_timed_in(
            request,
            tenant,
            |_content| {},
            Some(&publish),
            autumn_harvest::chrono::Utc::now(),
        )
        .await?;
    if let Some(latency) = latency {
        harness.record_turn(ctx, model.as_deref(), &turn.usage, latency);
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
/// A delegated child has the run id `{root}:d{n}`, and a draw has
/// `{root}:s{n}` (issue #2440). A draw can delegate, so the suffixes can
/// chain. A later run of the same workflow id gives its children the same
/// ids, so the rule covers each root part of the chain.
fn shares_run_id(run_id: &str, workflow_id: &str, execution_id: &str) -> bool {
    let chain: Vec<&str> = std::iter::successors(Some(run_id), |id| child_root(id)).collect();
    chain.contains(&workflow_id) && !chain.contains(&execution_id)
}

/// The parent run id of a child run id `{root}:d{n}` or `{root}:s{n}`, or
/// `None` for any other run id.
fn child_root(run_id: &str) -> Option<&str> {
    let (root, suffix) = run_id.rsplit_once(':')?;
    let ordinal = suffix
        .strip_prefix('d')
        .or_else(|| suffix.strip_prefix('s'))?;
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
) -> Result<MemorySnapshot, String> {
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

/// The notice of one parked call, as an activity.
///
/// # Errors
///
/// Returns the delivery error. The loop records it and waits as before.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_approval_notice(ctx: &ActivityContext, call: ParkedCall) -> Result<(), String> {
    harness(ctx)?.notify_approval(call).await
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

/// The workflows to register on a `HarvestBuilder`: `agent_loop`,
/// `agent_heartbeat`, the durable `agent`, the strategy runs `agent_draws`,
/// `agent_sample` and `agent_sweep`, and the pass `agent_consolidate`.
///
/// With the `eval` feature, it also holds the refine run `agent_refine`
/// (issue #2380).
#[must_use]
pub fn workflows() -> Vec<WorkflowInfo> {
    #[allow(unused_mut)]
    let mut list = vec![
        agent_loop_info(),
        agent_heartbeat_info(),
        agent_info(),
        crate::sampling::agent_draws_info(),
        crate::sampling::agent_sample_info(),
        crate::sampling::agent_sweep_info(),
        crate::consolidate::agent_consolidate_info(),
        crate::consolidate_all::agent_consolidate_all_info(),
    ];
    #[cfg(feature = "eval")]
    list.push(crate::refine::agent_refine_info());
    list
}

/// The activities to register on a `HarvestBuilder`.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    #[allow(unused_mut)]
    let mut list = vec![
        agent_model_turn_info(),
        agent_tool_call_info(),
        agent_memory_snapshot_info(),
        agent_deliver_info(),
        agent_approval_notice_info(),
        agent_precheck_info(),
        agent_discover_peers_info(),
        agent_profile_info(),
        agent_profile_overlay_info(),
        agent_memory_reset_info(),
        agent_memory_count_info(),
        agent_transcript_count_info(),
        agent_transcript_erase_info(),
        agent_workspace_retain_info(),
        agent_workspace_reset_info(),
        agent_workspace_floor_info(),
        crate::refine::agent_refine_propose_info(),
        crate::sampling::agent_sampling_policy_info(),
        crate::refine::agent_refine_policy_info(),
        crate::score::agent_score_info(),
        crate::answer_check::agent_answer_check_info(),
        crate::score::agent_sample_score_info(),
        crate::sampling::agent_sweep_items_info(),
        crate::sampling::agent_vote_key_info(),
        crate::consolidate::agent_consolidate_read_info(),
        crate::consolidate::agent_consolidate_turn_info(),
        crate::consolidate::agent_consolidate_write_info(),
        crate::consolidate_all::agent_consolidate_due_info(),
        crate::consolidate::agent_episode_append_info(),
        crate::consolidate::agent_episode_reset_info(),
        crate::refine::agent_refine_lesson_info(),
        crate::refine::agent_lesson_match_info(),
        crate::refine::agent_lesson_erase_info(),
    ];
    #[cfg(feature = "eval")]
    list.extend([
        crate::refine::agent_refine_start_info(),
        crate::refine::agent_refine_probe_info(),
        crate::refine::agent_refine_compose_info(),
        crate::refine::agent_refine_select_info(),
        crate::refine::agent_refine_adopt_info(),
        crate::refine::agent_refine_decide_info(),
    ]);
    list
}

/// Register the agent workflows, their activities and `harness` on a
/// worker builder.
///
/// It needs no backend feature. Each activity reads the harness with
/// `ActivityContext::state`, so it runs on each backend that gives the
/// builder state to an `#[activity]` body: the Postgres worker and the
/// SQLite driver of `HarvestPlugin`. A worker with no harness fails each
/// call as `AgentHarnessMissing`. Call it once per builder.
#[must_use]
pub fn register(
    builder: autumn_harvest::builder::HarvestBuilder,
    harness: AgentHarness,
) -> autumn_harvest::builder::HarvestBuilder {
    builder
        .workflows(workflows())
        .activities(activities())
        .state(harness)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    /// The batching of one round before issue #2483, at the `ToolCall`
    /// level. Returns `(start, len)` pairs.
    fn old_batches(
        calls: &[ToolCall],
        decisions: &[ToolDecision],
        agent: Option<&Member>,
        bound: u32,
    ) -> Vec<(usize, usize)> {
        let bound = usize::try_from(bound.min(MAX_PARALLEL_CHILDREN)).unwrap_or(1);
        let Some(agent) = agent else {
            return Vec::new();
        };
        let batchable = |position: usize| {
            let call = &calls[position];
            call.name == DELEGATE_TOOL
                && matches!(decisions.get(position), Some(ToolDecision::Allow))
                && delegate_args(&call.arguments)
                    .and_then(|args| agent.peers.iter().find(|peer| peer.name == args.agent))
                    .is_some_and(|peer| peer.agent.is_none())
        };
        let mut batches = Vec::new();
        let mut start = 0;
        while start < calls.len() {
            if !batchable(start) {
                start += 1;
                continue;
            }
            let mut end = start + 1;
            while end < calls.len() && end - start < bound && batchable(end) {
                end += 1;
            }
            if end - start >= 2 {
                batches.push((start, end - start));
            }
            start = end;
        }
        batches
    }

    /// A call of a round: a delegation to an inline, a durable or an
    /// unknown peer, a delegation with bad arguments, or another tool.
    fn call() -> impl Strategy<Value = ToolCall> {
        let delegate = |agent: &str| ToolCall {
            id: "c".into(),
            name: DELEGATE_TOOL.into(),
            arguments: json!({"agent": agent, "task": "t"}),
        };
        prop_oneof![
            Just(delegate("inline")),
            Just(delegate("durable")),
            Just(delegate("unknown")),
            Just(ToolCall {
                id: "c".into(),
                name: DELEGATE_TOOL.into(),
                arguments: json!({"agent": "inline"}),
            }),
            Just(ToolCall {
                id: "c".into(),
                name: "note".into(),
                arguments: json!({}),
            }),
        ]
    }

    fn decision() -> impl Strategy<Value = ToolDecision> {
        prop_oneof![
            Just(ToolDecision::Allow),
            Just(ToolDecision::RequireApproval { reason: "r".into() }),
            Just(ToolDecision::Deny { reason: "d".into() }),
        ]
    }

    proptest! {
        /// The plan batches each round exactly as the code before issue
        /// #2483 did, so a recorded run replays the same. A round may hold
        /// fewer decisions than calls.
        #[test]
        fn a_round_plan_batches_as_before(
            calls in prop::collection::vec(call(), 0..12),
            decisions in prop::collection::vec(decision(), 0..12),
            bound in 0u32..20,
            mandated in any::<bool>(),
        ) {
            let id = crate::agent::AgentId::new("peer", "p1").unwrap();
            let agent = Member {
                mandate: Mandate::new("root"),
                peers: vec![
                    Peer::new("inline", "Inline."),
                    Peer::agent("durable", "Durable.", &id),
                ],
                sample_schema: false,
            };
            let agent = mandated.then_some(&agent);
            let plan = round_plan(&calls, &decisions, agent, bound);
            let leads: Vec<(usize, usize)> = plan
                .iter()
                .enumerate()
                .filter_map(|(at, step)| match step {
                    Step::Lead(len) => Some((at, *len)),
                    _ => None,
                })
                .collect();
            prop_assert_eq!(plan.len(), calls.len());
            prop_assert_eq!(leads, old_batches(&calls, &decisions, agent, bound));
        }
    }

    // Issue #2481: a peer lookup recorded before the limit has no
    // `answer_limit`. Its run replays with the old cut and the old shape.
    #[test]
    fn a_legacy_lookup_keeps_the_old_cut_and_shape() {
        let peer: Peer = serde_json::from_value(serde_json::json!({
            "name": "researcher",
            "description": "R.",
            "max_steps": 8,
        }))
        .unwrap();
        assert_eq!(peer.answer_limit, None);
        assert_eq!(LEGACY_ANSWER_LIMIT, 8_000, "old histories pin this value");
        let mut result = serde_json::json!({"agent": "researcher", "stop": "completed"});
        let cut = put_answer(&mut result, &peer, &"x".repeat(8_050), 10);
        assert!(!cut, "a legacy run sets no record flag");
        let want = serde_json::json!({
            "agent": "researcher",
            "stop": "completed",
            "answer": format!("{}…[truncated]", "x".repeat(8_000)),
        });
        assert_eq!(result, want);
    }

    // Issue #2481: a resolved lookup cuts at its limit and marks the cut.
    #[test]
    fn a_resolved_lookup_marks_the_cut() {
        let peer = Peer::new("researcher", "R.").answer_limit(3);
        let mut result = serde_json::json!({"agent": "researcher"});
        assert!(put_answer(&mut result, &peer, "abcd", usize::MAX));
        assert_eq!(result["answer"], "abc…[truncated]");
        assert_eq!(result["truncated"], true);
        let mut result = serde_json::json!({"agent": "researcher"});
        assert!(!put_answer(&mut result, &peer, "abc", usize::MAX));
        assert_eq!(result["answer"], "abc");
        assert_eq!(result["truncated"], false);
    }

    // Issue #2481: the byte share also cuts, on a char boundary.
    #[test]
    fn a_resolved_lookup_cuts_to_the_byte_share() {
        let peer = Peer::new("researcher", "R.").answer_limit(usize::MAX);
        let mut result = serde_json::json!({"agent": "a"});
        // The frame `{"agent":"a","answer":"","truncated":false}` is 43
        // bytes. One "é" (2 bytes) and the mark (14 bytes) fill 59 bytes.
        assert!(put_answer(&mut result, &peer, &"é".repeat(20), 59));
        assert_eq!(result["answer"], "é…[truncated]");
        assert_eq!(result["truncated"], true);
        assert!(result.to_string().len() <= 59);
        let mut result = serde_json::json!({"agent": "a"});
        assert!(!put_answer(&mut result, &peer, "éééé", 100));
        assert_eq!(result["answer"], "éééé");
        assert_eq!(result["truncated"], false);
    }

    // Issue #2481: the cut measures the encoded result, so an escaped
    // character counts with its escape.
    #[test]
    fn an_escaped_answer_cuts_to_the_encoded_size() {
        let peer = Peer::new("researcher", "R.").answer_limit(usize::MAX);
        let mut result = serde_json::json!({"agent": "a"});
        assert!(put_answer(&mut result, &peer, &"\0".repeat(100), 100));
        assert!(result.to_string().len() <= 100, "{result}");
        assert!(result["answer"].as_str().unwrap().ends_with("…[truncated]"));
    }

    // The escape length matches `serde_json` for each ASCII character and a
    // few wider ones.
    #[test]
    fn the_escape_length_matches_serde_json() {
        let wide = ['é', '漢', '🎉', '\u{7f}', '\u{2028}'];
        for c in (0_u8..=0x7f).map(char::from).chain(wide) {
            let encoded = serde_json::to_string(&c.to_string()).unwrap();
            assert_eq!(escaped_char_len(c), encoded.len() - 2, "{c:?}");
        }
    }

    // A fixed list of answers with each kind of escape, for the bound below.
    fn escape_cases() -> Vec<String> {
        let all: String = (0_u8..=0x7f).map(char::from).collect();
        vec![
            all.repeat(3),
            "\"\\\n\t\0é漢🎉".repeat(20),
            "plain text ".repeat(40),
            String::new(),
        ]
    }

    // Issue #2481: each encoded result fits a share that can hold the frame
    // and the mark. A kept answer is a prefix of the original.
    #[test]
    fn each_encoded_result_fits_its_share() {
        let peer = Peer::new("researcher", "R.").answer_limit(usize::MAX);
        // The frame `{"agent":"a","answer":"","stop":"completed","truncated":false}`
        // is 62 bytes. The mark adds 14.
        for answer in escape_cases() {
            for max_bytes in [76, 77, 81, 100, 257, 1_000] {
                let mut result = serde_json::json!({"agent": "a", "stop": "completed"});
                let cut = put_answer(&mut result, &peer, &answer, max_bytes);
                assert!(
                    result.to_string().len() <= max_bytes,
                    "{max_bytes}: {result}"
                );
                let shown = result["answer"].as_str().unwrap();
                let kept = shown.strip_suffix("…[truncated]").unwrap_or(shown);
                assert!(answer.starts_with(kept), "{max_bytes}");
                assert_eq!(cut, kept.len() < answer.len(), "{max_bytes}");
            }
        }
    }

    // A share too small for the frame and the mark keeps the mark only. The
    // parent model still sees the cut.
    #[test]
    fn a_tiny_share_keeps_the_mark_only() {
        let peer = Peer::new("researcher", "R.").answer_limit(usize::MAX);
        let mut result = serde_json::json!({"agent": "a"});
        assert!(put_answer(&mut result, &peer, "abc", 10));
        assert_eq!(result["answer"], "…[truncated]");
        assert_eq!(result["truncated"], true);
    }

    #[test]
    fn the_flow_state_after_a_turn_skips_denied_calls() {
        let labels = |names: &[&str]| -> BTreeSet<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        // A released rule is a set of labels.
        let rule = |names: &[&str]| -> Released { std::iter::once(labels(names)).collect() };
        let tree = Tree {
            root: "r".into(),
            rounds: 0,
            delegations: 0,
            exercised: labels(&["old"]),
            released: Released::new(),
            records: Vec::new(),
            outbox: Outbox::default(),
        };
        let ask = ToolDecision::RequireApproval {
            reason: "ask".into(),
        };
        let deny = ToolDecision::Deny {
            reason: "no".into(),
        };
        let turn = ModelTurn {
            cost_usd_micros: None,
            content: Vec::new(),
            stop: StopReason::ToolUse,
            usage: TokenUsage::default(),
            decisions: vec![ToolDecision::Allow, deny, ask],
            cache_hit: false,
            labels: vec![labels(&["a"]), labels(&["denied"]), labels(&["c"])],
            releases: vec![rule(&["x"]), rule(&["denied"]), rule(&["y"])],
            oversized: false,
            interruptible: Vec::new(),
        };
        let (exercised, released) = flow_after(&tree, &turn);
        assert_eq!(
            exercised,
            labels(&["old", "a", "c"]),
            "a denied call adds none"
        );
        assert_eq!(released, rule(&["y"]), "only a call that asks can release");
    }

    #[test]
    fn the_placeholder_is_as_long_as_every_fixed_answer() {
        // The round reserves one placeholder per call. Any fixed answer that
        // replaces it must not grow the next request.
        let reserved = json_len(&placeholder());
        for message in [
            DROPPED_FULL,
            NOT_RUN_FULL,
            NOT_RUN_LOOP,
            NOT_RUN_STALE,
            NOT_RUN_INTERRUPTED,
            NOT_RUN_RESET,
            INTERRUPTED_CALL,
            INTERRUPTED_APPROVAL,
        ] {
            let answer = ToolOutcome::error(message).content;
            assert!(json_len(&answer) <= reserved, "{message}");
        }
        // The round budget of a recorded run holds this length. A longer
        // fixed answer would change how that run replays.
        assert_eq!(
            reserved,
            json_len(&ToolOutcome::error(NOT_RUN_STALE).content)
        );
    }

    /// A tree with no labels, for a reset test.
    fn bare_tree() -> Tree {
        Tree {
            root: "r".into(),
            rounds: 0,
            delegations: 0,
            exercised: BTreeSet::new(),
            released: Released::new(),
            records: Vec::new(),
            outbox: Outbox::default(),
        }
    }

    /// A run of a task, a window and the request size of that window.
    fn reset_fixture() -> (AgentTask, Progress, u64) {
        let setting = crate::ContextReset::new(1, 50).max_notes_bytes(100);
        let task = AgentTask::new("audit").context_reset(setting);
        let progress = Progress::new(
            "r".into(),
            vec![
                ChatMessage::text(ChatRole::User, "audit"),
                ChatMessage::text(ChatRole::Assistant, "x".repeat(2_000)),
            ],
        );
        let run = Run {
            task: &task,
            agent: None,
            live: false,
        };
        let request = next_request(
            run,
            &progress,
            &bare_tree(),
            &run.extra_tools(),
            progress.messages.clone(),
        );
        let len = json_len(&request);
        (task, progress, len)
    }

    fn plan(task: &AgentTask, progress: &mut Progress, len: u64) -> Option<Vec<ChatMessage>> {
        let run = Run {
            task,
            agent: None,
            live: false,
        };
        plan_reset(
            run,
            progress,
            &bare_tree(),
            &run.extra_tools(),
            "audit",
            len,
        )
    }

    #[test]
    fn a_save_turn_comes_first_and_the_reset_after_it() {
        let (mut task, mut progress, len) = reset_fixture();
        task.max_request_bytes = Some(len + 1_000);
        let noticed = plan(&task, &mut progress, len).expect("a save turn");
        assert!(progress.reset.pending);
        assert_eq!(progress.reset.count, 0, "the save turn spends no reset");
        let last = noticed.last().unwrap();
        assert_eq!(
            last.content,
            vec![ContentPart::Text(context_reset::RESET_NOTICE.to_owned())]
        );
        assert_eq!(noticed.len(), progress.messages.len() + 1);

        progress.sent = Some(crate::transcript::Sent::of(&progress.messages));
        let window = plan(&task, &mut progress, len).expect("the reset");
        assert!(!progress.reset.pending);
        assert_eq!(progress.reset.count, 1);
        assert!(progress.sent.is_none(), "the next turn goes in full");
        assert_eq!(window, vec![ChatMessage::text(ChatRole::User, "audit")]);
    }

    #[test]
    fn with_no_room_for_a_save_turn_the_reset_runs_at_once() {
        let (mut task, mut progress, len) = reset_fixture();
        task.max_request_bytes = Some(len);
        task.context_reset = Some(crate::ContextReset::new(1, 100).max_notes_bytes(100));
        let window = plan(&task, &mut progress, len).expect("the reset");
        assert!(!progress.reset.pending);
        assert_eq!(progress.reset.count, 1);
        assert_eq!(window, vec![ChatMessage::text(ChatRole::User, "audit")]);
    }

    #[test]
    fn the_save_turn_needs_room_for_the_notes() {
        let (mut task, mut progress, len) = reset_fixture();
        // The notice fits, but the notes of a save turn would not.
        task.max_request_bytes = Some(len + 1_000);
        task.context_reset = Some(crate::ContextReset::new(1, 50).max_notes_bytes(5_000));
        let window = plan(&task, &mut progress, len).expect("the reset");
        assert!(!progress.reset.pending, "no save turn");
        assert_eq!(progress.reset.count, 1);
        assert_eq!(window, vec![ChatMessage::text(ChatRole::User, "audit")]);
    }

    #[test]
    fn under_the_threshold_or_with_no_reset_left_nothing_changes() {
        let (mut task, mut progress, len) = reset_fixture();
        task.max_request_bytes = Some(len * 3);
        assert!(plan(&task, &mut progress, len).is_none(), "under 50 %");
        task.max_request_bytes = Some(len + 1_000);
        progress.reset.count = 1;
        assert!(plan(&task, &mut progress, len).is_none(), "no reset left");
        assert!(!progress.reset.pending);
    }

    #[test]
    fn a_kept_round_makes_the_reset_due_under_the_threshold() {
        let (mut task, mut progress, len) = reset_fixture();
        task.max_request_bytes = Some(len * 3);
        let round = [
            assistant(vec![followup_call("h1")]),
            tool(vec![result("h1", "dropped")]),
        ];
        progress.reset.kept = Some(round.clone());
        let noticed = plan(&task, &mut progress, len).expect("a save turn");
        assert!(
            progress.reset.pending,
            "under 50 %, the kept round asks a save turn"
        );
        assert_eq!(noticed.len(), progress.messages.len() + 1);
        let window = plan(&task, &mut progress, len).expect("the reset");
        assert_eq!(progress.reset.count, 1);
        assert!(progress.reset.kept.is_none(), "the reset clears the round");
        let mut expected = vec![ChatMessage::text(ChatRole::User, "audit")];
        expected.extend(round);
        assert_eq!(window, expected);
    }

    #[test]
    fn a_booking_in_the_kept_round_shows_once() {
        let booked = r#"{"in_minutes":1,"scheduled":true}"#;
        let task = AgentTask::new("audit");
        let round = [
            assistant(vec![followup_call("f1"), followup_call("h1")]),
            tool(vec![result("f1", booked), result("h1", "dropped")]),
        ];
        let mut messages = vec![ChatMessage::text(ChatRole::User, "audit")];
        messages.extend(round.clone());
        let mut progress = Progress::new("r".into(), messages);
        progress.followup = Some(Planned { delay_secs: 60 });
        progress.reset.kept = Some(round.clone());
        let window = reset_window(&task, &progress, "audit");
        let mut expected = vec![ChatMessage::text(ChatRole::User, "audit")];
        expected.extend(round);
        assert_eq!(window, expected);
        // A round with no booking keeps the booking of the segment apart.
        progress.reset.kept = Some([
            assistant(vec![followup_call("h2")]),
            tool(vec![result("h2", "dropped")]),
        ]);
        let ids: Vec<String> = reset_window(&task, &progress, "audit")
            .iter()
            .map(|message| match &message.content[0] {
                ContentPart::Text(text) => text.clone(),
                ContentPart::ToolCall { id, .. }
                | ContentPart::ToolResult {
                    tool_call_id: id, ..
                } => id.clone(),
            })
            .collect();
        assert_eq!(
            ids,
            ["audit", "f1", "f1", "h2", "h2"],
            "the booking comes first"
        );
    }

    fn followup_call(id: &str) -> ContentPart {
        ContentPart::ToolCall {
            id: id.into(),
            name: FOLLOWUP_TOOL.into(),
            arguments: serde_json::json!({"prompt": id, "delay_minutes": 1}),
        }
    }

    fn result(id: &str, content: &str) -> ContentPart {
        ContentPart::ToolResult {
            tool_call_id: id.into(),
            content: content.into(),
        }
    }

    fn assistant(content: Vec<ContentPart>) -> ChatMessage {
        ChatMessage {
            role: ChatRole::Assistant,
            content,
        }
    }

    fn tool(content: Vec<ContentPart>) -> ChatMessage {
        ChatMessage {
            role: ChatRole::Tool,
            content,
        }
    }

    #[test]
    fn a_booking_is_the_last_call_that_booked() {
        let booked = r#"{"in_minutes":1,"scheduled":true}"#;
        let refused = ToolOutcome::error("a follow-up is already scheduled").content;
        let messages = vec![
            ChatMessage::text(ChatRole::User, "task"),
            assistant(vec![
                ContentPart::Text("booking".into()),
                ContentPart::ToolCall {
                    id: "a".into(),
                    name: "lookup".into(),
                    arguments: serde_json::json!({}),
                },
                followup_call("f1"),
            ]),
            // The loop guard can add a note after the JSON.
            tool(vec![
                result("a", "ok"),
                result("f1", &format!("{booked}\n[note]")),
            ]),
            assistant(vec![followup_call("f2")]),
            tool(vec![result("f2", &refused)]),
            assistant(vec![followup_call("f3")]),
            tool(vec![result(
                "f3",
                &ToolOutcome::error(NOT_RUN_RESET).content,
            )]),
        ];
        let [call, answer] = booking(&messages).expect("the booking");
        assert_eq!(call.content, vec![followup_call("f1")]);
        assert_eq!(answer.content.len(), 1);
        assert!(booked_part(&answer.content[0]));
        assert!(booking(&messages[..2]).is_none(), "a call with no result");
        assert!(
            booking(&messages[3..]).is_none(),
            "refused calls book nothing"
        );
    }

    fn booked_part(part: &ContentPart) -> bool {
        booked(part)
    }

    #[test]
    fn a_woken_window_keeps_the_booking_that_woke_it() {
        let booked = r#"{"in_minutes":1,"scheduled":true}"#;
        let task = AgentTask::new("audit").system("s");
        let mut progress = Progress::new(
            "r".into(),
            vec![
                ChatMessage::text(ChatRole::System, "s"),
                ChatMessage::text(ChatRole::User, "audit"),
                assistant(vec![followup_call("f1")]),
                tool(vec![result("f1", booked)]),
                ChatMessage::text(ChatRole::User, followup::WAKE_MESSAGE),
                // This segment books again.
                assistant(vec![followup_call("f2")]),
                tool(vec![result("f2", booked)]),
            ],
        );
        progress.followup = Some(Planned { delay_secs: 60 });
        progress.reset.woken_by = booking(&progress.messages[..4]);
        let window = reset_window(&task, &progress, followup::WAKE_MESSAGE);
        let ids: Vec<String> = window
            .iter()
            .map(|message| match &message.content[0] {
                ContentPart::Text(text) => text.chars().take(5).collect(),
                ContentPart::ToolCall { id, .. }
                | ContentPart::ToolResult {
                    tool_call_id: id, ..
                } => id.clone(),
            })
            .collect();
        assert_eq!(ids, ["s", "audit", "f1", "f1", "[Sche", "f2", "f2"]);
        // A durable follow-up op has the wake message as its task.
        let durable = AgentTask::new(followup::WAKE_MESSAGE);
        progress.followup = None;
        let window = reset_window(&durable, &progress, followup::WAKE_MESSAGE);
        assert_eq!(
            window.len(),
            4,
            "the system prompt, the task and the booking"
        );
        assert_eq!(window[2].content, vec![followup_call("f1")]);
        // A second reset of the op still keeps the booking.
        progress.messages = window;
        let again = reset_window(&durable, &progress, followup::WAKE_MESSAGE);
        assert_eq!(again, progress.messages);
    }

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
    fn a_draw_of_a_workflow_id_run_skips_the_cache() {
        assert!(shares_run_id("daily:s0", "daily", "0e3c"));
        assert!(shares_run_id("daily:s1:d2", "daily", "0e3c"));
        assert!(!shares_run_id("0e3c:s1", "daily", "0e3c"));
        assert!(!shares_run_id("0e3c:s1:d2", "daily", "0e3c"));
        // A draw that `run_draws` starts has a recorded UUID as its root, so
        // the child sees no shared id.
        assert!(!shares_run_id(
            "01890a5d-ac96-774b-bcce-b302099a8057:s0",
            "child-wf",
            "child-exec"
        ));
        // Only a numeric suffix marks a child.
        assert!(!shares_run_id("daily:sx", "daily", "0e3c"));
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

    fn durable() -> Peer {
        Peer::agent(
            "researcher",
            "Finds facts.",
            &AgentId::new("research", "r1").unwrap(),
        )
    }

    /// Whether the `delegate` tool of a member shows the `sample` argument.
    fn shows_sample(mandate: Mandate, peers: Vec<Peer>) -> bool {
        let member = Member {
            mandate,
            peers,
            sample_schema: true,
        };
        let tool = member.delegate_tool().unwrap();
        tool.input_schema["properties"].get("sample").is_some()
    }

    #[test]
    fn the_delegate_tool_shows_sample_only_under_a_cap_of_two_and_a_durable_peer() {
        let inline = Peer::new("helper", "Helps.");
        assert!(shows_sample(
            Mandate::new("a").max_draws(2),
            vec![inline.clone(), durable()]
        ));
        assert!(!shows_sample(
            Mandate::new("a").max_draws(1),
            vec![durable()]
        ));
        assert!(!shows_sample(Mandate::new("a"), vec![durable()]));
        assert!(!shows_sample(Mandate::new("a").max_draws(3), vec![inline]));
        // The cap of a durable peer role counts, met with the mandate cap.
        let capped = |max| Peer {
            max_draws: Some(max),
            ..durable()
        };
        assert!(shows_sample(Mandate::new("a"), vec![capped(3)]));
        assert!(!shows_sample(
            Mandate::new("a").max_draws(5),
            vec![capped(1)]
        ));
        let mut member = Member {
            mandate: Mandate::new("a").max_draws(4),
            peers: vec![durable()],
            sample_schema: false,
        };
        let old = member.delegate_tool().unwrap();
        assert!(
            old.input_schema["properties"].get("sample").is_none(),
            "a member before the gate keeps the old schema"
        );
        member.sample_schema = true;
        let tool = member.delegate_tool().unwrap();
        assert_eq!(tool.input_schema["additionalProperties"], json!(false));
        let sample = &tool.input_schema["properties"]["sample"];
        assert!(sample["description"].as_str().unwrap().contains('4'));
        assert_eq!(tool.input_schema["required"], json!(["agent", "task"]));
    }

    #[test]
    fn delegate_args_take_an_optional_sample() {
        let plain = delegate_args(&json!({"agent": "a", "task": "t"})).unwrap();
        assert_eq!(plain.sample, None);
        let sampled = delegate_args(&json!({
            "agent": "a",
            "task": "t",
            "sample": {"strategy": {"best_of_n": {"n": 3}}, "scorer": {"id": "table"}},
        }))
        .unwrap();
        assert_eq!(sampled.sample.unwrap().strategy.draws(), 3);
        let policy_key = json!({
            "agent": "a",
            "task": "t",
            "sample": {"strategy": "single", "max_draws": 9},
        });
        assert_eq!(delegate_args(&policy_key), None);
        assert_eq!(
            delegate_args(&json!({"agent": "a", "task": "t", "n": 3})),
            None
        );
    }

    #[test]
    fn a_call_that_holds_a_sample_key_never_joins_a_batch() {
        let member = Member {
            mandate: Mandate::new("a").peers(["helper"]).max_draws(3),
            peers: vec![Peer::new("helper", "Helps.")],
            sample_schema: true,
        };
        let delegate = |arguments: serde_json::Value| ToolCall {
            id: "c".into(),
            name: DELEGATE_TOOL.into(),
            arguments,
        };
        let allow = ToolDecision::Allow;
        let plain = delegate(json!({"agent": "helper", "task": "t"}));
        assert_eq!(fit(&plain, Some(&allow), &member), Fit::Batchable);
        // Before the patch gate, each of these failed to decode. So an old
        // history plans the same round.
        for sample in [json!(null), json!({"strategy": "single"})] {
            let call = delegate(json!({"agent": "helper", "task": "t", "sample": sample}));
            assert!(holds_sample(&call.arguments));
            assert_eq!(fit(&call, Some(&allow), &member), Fit::Other);
        }
        assert!(!holds_sample(&json!({"agent": "helper", "task": "t"})));
    }

    fn ran(draws: u32) -> DelegationRecord {
        DelegationRecord {
            chain: vec!["a".into(), "b".into()],
            run_id: Some("r".into()),
            stop: Some(AgentStop::Completed),
            refusal: None,
            usage: TokenUsage::default(),
            steps_used: 0,
            tool_calls: 0,
            resets: 0,
            answer_truncated: false,
            serial: None,
            draws,
            idempotency_key: None,
        }
    }

    #[test]
    fn a_parent_counts_each_nested_delegation_and_its_draw_units() {
        let refused = DelegationRecord {
            run_id: None,
            refusal: Some(Refusal::DrawsExceeded),
            ..ran(0)
        };
        assert_eq!(nested_units(&[ran(0), ran(3), refused]), 1 + 4);
        assert_eq!(nested_units(&[]), 0);
        assert_eq!(nested_units(&[ran(u32::MAX), ran(1)]), u32::MAX);
    }

    // Issue #2484: the message of an unanswered delegation went out, so
    // its unit and its draws still count.
    #[test]
    fn an_unanswered_delegation_still_counts_its_units() {
        let unanswered = DelegationRecord {
            stop: None,
            refusal: Some(Refusal::ReplyTimeout),
            idempotency_key: Some("r:1".into()),
            ..ran(2)
        };
        assert_eq!(nested_units(&[unanswered]), 1 + 2);
    }

    // Issue #2484: each cause of a missing reply has its own verdict.
    #[test]
    fn each_missing_reply_has_its_verdict_and_its_cause() {
        let timeout = NoReply::Timeout(Duration::from_secs(60));
        let rejected = NoReply::Rejected("the operator said no".into());
        let interrupted = NoReply::Interrupted;
        assert_eq!(timeout.verdict(), Refusal::ReplyTimeout);
        assert_eq!(rejected.verdict(), Refusal::ReplyRejected);
        assert_eq!(interrupted.verdict(), Refusal::ReplyInterrupted);
        for cause in [&timeout, &rejected, &interrupted] {
            assert!(cause.verdict().unanswered());
            let text = cause.error("researcher");
            assert!(
                text.starts_with(&cause.verdict().message("researcher")),
                "{text}"
            );
        }
        assert!(timeout.error("researcher").contains("60s"));
        assert!(
            rejected
                .error("researcher")
                .contains("the operator said no")
        );
    }

    // Issue #2484: a caller outside the run writes a rejection reason, so
    // the model reads a bounded cause. The legacy text keeps it whole.
    #[test]
    fn a_long_rejection_reason_is_cut_for_the_model() {
        let long = NoReply::Rejected("x".repeat(10_000));
        let text = long.error("researcher");
        let message = Refusal::ReplyRejected.message("researcher");
        let cause = text.strip_prefix(&format!("{message}: ")).unwrap();
        assert!(cause.starts_with(&"x".repeat(256)), "{cause}");
        assert!(!cause.contains(&"x".repeat(257)), "{cause}");
        assert!(long.legacy("researcher").ends_with(&"x".repeat(10_000)));
    }

    // Issue #2484: a history from before the gate keeps its old text.
    #[test]
    fn the_legacy_text_of_each_missing_reply_is_unchanged() {
        assert_eq!(
            NoReply::Timeout(Duration::from_secs(60)).legacy("researcher"),
            "the durable agent \"researcher\" gave no answer: no reply arrived within 60s"
        );
        assert_eq!(
            NoReply::Rejected("no".into()).legacy("researcher"),
            "the durable agent \"researcher\" gave no answer: no"
        );
        assert_eq!(
            NoReply::Interrupted.legacy("researcher"),
            "the durable agent \"researcher\" gave no answer: \
             a person interrupted this op while it waited for the reply"
        );
    }
}
