//! Replay-as-evaluation (issue #2001).
//!
//! [`evaluate`] re-drives a recorded `agent_loop` run with a candidate model or
//! prompt. It reports where the decisions of the two runs diverge.
//!
//! # How it runs
//!
//! The harness runs the real `agent_loop` body in the in-memory test engine.
//! It writes nothing to a database. Each activity resolves as follows:
//!
//! - `agent_model_turn` asks the candidate model, then the candidate policy.
//!   These are the only live calls. A retryable failure retries under the
//!   retry policy of the activity, as on a worker. A turn over the worker
//!   result cap is cut or dropped, as on a worker with no payload store. The
//!   response cache is skipped, so every turn asks the candidate.
//! - `agent_tool_call` returns the recorded outcome of the same call. A call
//!   with no recorded outcome gets an error stub. No tool runs.
//! - `agent_memory_snapshot` returns the recorded snapshot, or an empty one.
//! - `agent_discover_peers` returns the recorded peers for the same names, or
//!   none. The candidate needs no roster.
//! - A tool, snapshot or peer-lookup activity that failed the source fails
//!   the candidate at the same activity.
//! - `agent_deliver` is a stub. No report leaves the harness.
//! - Any other activity has no mock, so it fails the candidate run.
//!
//! The harness sends again each approval for a wait that the source opened,
//! when it arrived before the deadline. It drops every other signal. A late or
//! unsolicited approval therefore cannot release a call.
//!
//! # The fork rules
//!
//! An evaluation is an in-memory fork at the first event. It follows the fork
//! rules of issue #2000:
//!
//! - The harness reads a borrowed slice, so the source stays unchanged.
//! - The harness accepts a completed source.
//! - Each effect gets a recorded result or a stub. No option turns on live
//!   effects.
//! - The harness always refuses an erased source.
//!
//! The harness also refuses a source that has not ended, and a source that
//! was cancelled or timed out. Such a source has no recorded end to compare
//! against.
//!
//! # Matching
//!
//! A recorded tool outcome answers a call with the same step, tool name and
//! arguments. A model gives new call ids on each call, so the key holds no id.
//! In a delegation run, the call must also come from the same agent of the
//! tree. An agent is its lane, the child run id suffix such as `:d1`, with
//! its name. The lane alone is a delegation ordinal, so another peer can hold
//! it.
//!
//! A child run of a fan-out takes the result of the first unused recorded
//! child with the same delegation chain and task text. With no such child,
//! the candidate fails with [`NO_CHILD_RUN`].
//!
//! A candidate call that equals the recorded call at the same turn and
//! position, of the same agent, takes the recorded id. The ids change before
//! the policy runs. Approval signal names hold the id, so the recorded
//! approvals stay valid. Any other candidate call gets a new id with the
//! prefix `eval_`. A recorded approval therefore never releases it.
//!
//! # The diff
//!
//! The harness compares the decisions of each model turn: the tool calls, their
//! arguments, the policy decisions and the stop reason. Two final answers with
//! the same stop reason and different text get [`Verdict::Reworded`]. That is
//! not a divergence. The harness also compares how the two runs end.
//!
//! The replay debugger (issue #949) compares commands. A new prompt changes
//! every request, so a command diff always stops at the first model turn. This
//! diff compares the decisions instead.
//!
//! # Cost
//!
//! Each candidate turn is a live model call. [`Candidate::max_turns`] caps
//! them. The default cap is the recorded turn count plus
//! [`DEFAULT_EXTRA_TURNS`].
//!
//! Each engine cycle replays the history so far. So the time of an
//! evaluation grows with the square of the run length. Evaluate long runs
//! offline, not on a request path.
//!
//! # Runtime
//!
//! The test engine resolves activities synchronously. The model call is
//! async, so the harness blocks in place. Blocking in place needs a
//! multi-thread Tokio runtime.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use autumn_harvest::erase::is_erasure_tombstone;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::parse_typed_payload;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::types::ActivityExecId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::harness::{AgentHarness, fit_turn};
use crate::memory::MemorySnapshot;
use crate::mesh::Peer;
use crate::message::{ContentPart, StopReason, ToolCall};
use crate::policy::ToolDecision;
use crate::types::{
    AgentReport, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
};
use crate::workflow::{
    WORKFLOW_NAME, agent_deliver_info, agent_discover_peers_info, agent_loop_info,
    agent_memory_snapshot_info, agent_model_turn_info, agent_tool_call_info,
};

/// The error text of the stub outcome. A candidate call with no recorded
/// outcome gets it.
pub const NOT_RUN: &str =
    "not run: the evaluation has no recorded output for this call, and it runs no tool";

/// The error of a child run of a fan-out that no recorded child answers
/// (issue #2270). The candidate run fails with it.
pub const NO_CHILD_RUN: &str =
    "not run: the source has no child run with this delegation chain and task";

/// The default number of live turns past the recorded turn count.
pub const DEFAULT_EXTRA_TURNS: usize = 8;

/// The failure of a model turn past the turn cap.
const TURN_CAP_ERROR: &str = "the evaluation reached its turn cap";

/// The prefix of a candidate call id that the harness replaced.
const EVAL_ID_PREFIX: &str = "eval_";

/// The prefix of the deadline timer of a signal wait.
///
/// The engine names the timer `__signal_timeout:{seq}:{signal}`. The test
/// `a_late_approval_is_not_delivered_and_new_ids_take_the_recorded_ids`
/// fails when the name changes.
const SIGNAL_TIMEOUT_PREFIX: &str = "__signal_timeout:";

/// The candidate: a harness with a live model, and an optional new prompt.
///
/// The harness supplies the model, the temperature, the tool definitions and
/// the policy. The model and the policy run live, so use a policy with no
/// side effects. The tools, the memory store and the delivery never run.
#[derive(Debug, Clone)]
pub struct Candidate {
    harness: Arc<AgentHarness>,
    system: Option<String>,
    max_turns: Option<usize>,
    model: Option<String>,
    agent_models: BTreeMap<String, String>,
}

impl Candidate {
    /// A candidate that uses `harness` for every model turn.
    #[must_use]
    pub fn new(harness: AgentHarness) -> Self {
        Self::shared(Arc::new(harness))
    }

    /// A candidate that uses a shared `harness`.
    #[must_use]
    pub const fn shared(harness: Arc<AgentHarness>) -> Self {
        Self {
            harness,
            system: None,
            max_turns: None,
            model: None,
            agent_models: BTreeMap::new(),
        }
    }

    /// Ask the harness model `name` for every model turn (issue #2271).
    ///
    /// Without it, each turn asks the model that its recorded input names.
    #[must_use]
    pub fn model(mut self, name: impl Into<String>) -> Self {
        self.model = Some(name.into());
        self
    }

    /// Ask the harness model `name` for each turn of the agent `agent` of a
    /// delegation tree. It wins over [`model`](Self::model) for that agent.
    #[must_use]
    pub fn agent_model(mut self, agent: impl Into<String>, name: impl Into<String>) -> Self {
        self.agent_models.insert(agent.into(), name.into());
        self
    }

    /// The model name that this candidate sets for a turn of `agent`, if any.
    fn model_for(&self, agent: Option<&str>) -> Option<&String> {
        agent
            .and_then(|agent| self.agent_models.get(agent))
            .or(self.model.as_ref())
    }

    /// Replace the system prompt of the recorded task.
    #[must_use]
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system = Some(prompt.into());
        self
    }

    /// Cap the live model turns at `max_turns`.
    ///
    /// The turn past the cap fails the candidate run, and the report sets
    /// [`Evaluation::turn_cap_reached`].
    #[must_use]
    pub const fn max_turns(mut self, max_turns: usize) -> Self {
        self.max_turns = Some(max_turns);
        self
    }
}

/// Why an evaluation cannot start or finish.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EvalError {
    /// The history is not an `agent_loop` run, or its input does not decode
    /// as an `AgentTask`. An encoded input gives this error.
    #[error("the history is not an agent run: {0}")]
    NotAnAgentRun(String),
    /// The history holds an erasure tombstone. An evaluation never forks an
    /// erased source.
    #[error("the source run was erased, so it cannot be evaluated")]
    ErasedSource,
    /// The source history has no terminal event.
    #[error("the source run has not ended, so it has no recorded frontier")]
    InFlightSource,
    /// The source was cancelled or timed out. An outside event cut it, so its
    /// last turn is not a recorded end. The text says what ended it.
    #[error("the source run was interrupted ({0}), so it has no recorded end")]
    InterruptedSource(String),
    /// A recorded or candidate payload does not decode.
    #[error("a payload does not decode: {0}")]
    Undecodable(String),
    /// The current runtime cannot block in place.
    #[error("evaluation needs a multi-thread Tokio runtime")]
    MultiThreadRuntimeRequired,
}

/// One side of a comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The recorded run.
    Recorded,
    /// The candidate run.
    Candidate,
}

/// How two decisions of one turn differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "side", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Divergence {
    /// One side answers and the other calls tools.
    Shape,
    /// The tool names, the arguments or the number of calls differ.
    Calls,
    /// The calls agree, but a policy decision differs.
    Policy,
    /// The calls and decisions agree, but the stop reasons differ, or only
    /// one side dropped its reply as too large.
    Stop,
    /// Only one side has this turn. The value names the side that lacks it.
    Missing(Side),
}

/// The verdict on one turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", content = "divergence", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Verdict {
    /// The two decisions agree.
    Same,
    /// Both sides answer with the same stop reason and different text.
    Reworded,
    /// The decisions differ.
    Diverged(Divergence),
}

/// One tool call of a turn and its policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallDecision {
    /// The tool name.
    pub name: String,
    /// The tool arguments.
    pub arguments: Value,
    /// The policy decision.
    #[serde(flatten)]
    pub decision: ToolDecision,
}

/// The decision of one model turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnDecision {
    /// The tool calls, in order.
    pub calls: Vec<CallDecision>,
    /// The text of the turn.
    pub text: String,
    /// Why the model stopped.
    pub stop: StopReason,
    /// The reply exceeded the result cap and the worker dropped it (issue
    /// #2040). Such a turn holds no text and no calls.
    #[serde(default, skip_serializing_if = "crate::types::is_false")]
    pub dropped: bool,
}

impl TurnDecision {
    fn of(turn: &ModelTurn) -> Self {
        let calls = turn
            .calls()
            .into_iter()
            .enumerate()
            .map(|(position, call)| CallDecision {
                name: call.name,
                arguments: call.arguments,
                // The loop denies a call with no recorded decision. The diff
                // reads it the same way.
                decision: turn.decisions.get(position).cloned().unwrap_or_else(|| {
                    ToolDecision::Deny {
                        reason: "no recorded decision".to_owned(),
                    }
                }),
            })
            .collect();
        Self {
            calls,
            text: turn.text(),
            stop: turn.stop,
            dropped: turn.oversized,
        }
    }
}

/// The comparison of one turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnDiff {
    /// The turn index, from 0, across every segment of the run.
    pub turn: usize,
    /// The recorded decision, or `None` when the recorded run has no such
    /// turn.
    pub recorded: Option<TurnDecision>,
    /// The candidate decision, or `None` when the candidate run has no such
    /// turn.
    pub candidate: Option<TurnDecision>,
    /// How the two decisions compare.
    pub verdict: Verdict,
}

/// How a run ended.
// A box would change the public shape of `Completed`. The report is the
// common case, so its size is no waste.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "end", content = "detail")]
#[non_exhaustive]
pub enum RunEnd {
    /// The run completed with this report.
    Completed(AgentReport),
    /// The run failed. The text says why.
    Failed(String),
}

/// The result of one evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evaluation {
    /// One comparison per model turn.
    pub turns: Vec<TurnDiff>,
    /// The index of the first divergent turn, or `None` when no turn
    /// diverges.
    pub first_divergence: Option<usize>,
    /// How the recorded run ended.
    pub recorded: RunEnd,
    /// How the candidate run ended.
    pub candidate: RunEnd,
    /// `true` when the two runs end in different ways.
    ///
    /// Two completed runs differ when their [`AgentStop`](crate::AgentStop)
    /// values differ. A completed run and a failed run always differ. Two
    /// failed runs do not differ.
    pub end_diverged: bool,
    /// The number of candidate tool calls that got a recorded outcome.
    pub replayed_tool_calls: u32,
    /// The number of candidate tool calls that got the stub outcome.
    pub stubbed_tool_calls: u32,
    /// `true` when the turn cap stopped the candidate run.
    pub turn_cap_reached: bool,
}

impl Evaluation {
    /// `true` when a turn diverges or the run ends differ.
    #[must_use]
    pub const fn diverged(&self) -> bool {
        self.first_divergence.is_some() || self.end_diverged
    }
}

/// Evaluate `candidate` against the recorded `history`.
///
/// The candidate model and policy answer every model turn live. Every other
/// effect is recorded or stubbed. See the [module documentation](self).
///
/// A failure of the candidate model is not an error. It shows as
/// [`RunEnd::Failed`] in [`Evaluation::candidate`].
///
/// Decode payload-store references and encrypted payloads before the
/// evaluation.
///
/// # Errors
///
/// Returns an [`EvalError`] when the history cannot be evaluated, when a
/// payload does not decode, or when the runtime cannot block in place. A
/// refused history costs no model call.
pub async fn evaluate(
    history: &[WorkflowEvent],
    candidate: &Candidate,
) -> Result<Evaluation, EvalError> {
    let recording = Recording::read(history)?;
    let handle = multi_thread_handle()?;

    let mut task = recording.task.clone();
    if let Some(system) = &candidate.system {
        task.system = Some(system.clone());
    }
    let input = serde_json::to_value(&task).map_err(|e| EvalError::Undecodable(e.to_string()))?;
    let max_turns = candidate
        .max_turns
        .unwrap_or_else(|| recording.turns.len().saturating_add(DEFAULT_EXTRA_TURNS));

    let session = Arc::new(Mutex::new(Session {
        recorded_calls: recording.turns.iter().map(ModelTurn::calls).collect(),
        recorded_agents: recording.turn_agents.clone(),
        candidate_names: HashMap::new(),
        turn: 0,
        max_turns,
        cap_reached: false,
        tools: recording.tools.clone(),
        snapshots: recording.snapshots.iter().cloned().collect(),
        peers: recording.peers.clone(),
        root: None,
        replayed: 0,
        stubbed: 0,
    }));

    let model = model_mock(&session, candidate, &recording, handle);
    let tool = {
        let session = Arc::clone(&session);
        move |input: Value| -> Result<Value, String> {
            let request: ToolCallRequest = decode(input)?;
            let outcome = lock(&session).answer(&request)?;
            encode(&outcome)
        }
    };
    let snapshot = {
        let session = Arc::clone(&session);
        move |_scope: Value| -> Result<Value, String> {
            let snapshot = lock(&session).snapshots.pop_front();
            encode(&snapshot.unwrap_or_else(|| Ok(MemorySnapshot::Text(String::new())))?)
        }
    };

    let peers = {
        let session = Arc::clone(&session);
        move |names: Value| -> Result<Value, String> { encode(&lock(&session).lookup(&names)?) }
    };

    // A child run of a fan-out takes the result of the first unused recorded
    // child that it matches. No child runs live.
    let children = {
        let recorded = Mutex::new(recording.children.clone());
        move |task: Value| -> Result<Value, String> {
            let result = lock(&recorded)
                .iter_mut()
                .find(|child| !child.used && child.answers(&task))
                .and_then(|child| {
                    child.used = true;
                    child.result.clone()
                });
            result.unwrap_or_else(|| Err(NO_CHILD_RUN.to_owned()))
        }
    };

    let mut env = engine()
        .mock_child_workflow(WORKFLOW_NAME, children)
        .mock_activity(agent_discover_peers_info().name, peers)
        .mock_activity(agent_model_turn_info().name, model)
        .mock_activity(agent_tool_call_info().name, tool)
        .mock_activity(agent_memory_snapshot_info().name, snapshot)
        .mock_activity(agent_deliver_info().name, |_report| Ok(Value::Null));
    for (name, payload) in &recording.signals {
        env = env.queue_signal(name.clone(), payload.clone());
    }
    let outcome = env.run(agent_loop_info().handler, input).await;

    let candidate_turns = model_turns(outcome.events())?;
    let candidate_end = match outcome.result {
        Ok(output) => RunEnd::Completed(decode(output).map_err(EvalError::Undecodable)?),
        Err(error) => RunEnd::Failed(error),
    };
    let (replayed, stubbed, cap_reached) = {
        let session = lock(&session);
        (session.replayed, session.stubbed, session.cap_reached)
    };

    let turns = diff_turns(&recording.turns, &candidate_turns);
    let first_divergence = turns
        .iter()
        .position(|turn| matches!(turn.verdict, Verdict::Diverged(_)));
    let end_diverged = ends_differ(&recording.end, &candidate_end);
    Ok(Evaluation {
        turns,
        first_divergence,
        recorded: recording.end,
        candidate: candidate_end,
        end_diverged,
        replayed_tool_calls: replayed,
        stubbed_tool_calls: stubbed,
        turn_cap_reached: cap_reached,
    })
}

/// Two candidates on one source, such as two models for one agent (issue
/// #2271).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    /// The first candidate against the source.
    pub first: Evaluation,
    /// The second candidate against the source.
    pub second: Evaluation,
    /// One comparison per model turn of the two candidates.
    ///
    /// In each [`TurnDiff`], `recorded` holds the decision of the first
    /// candidate, and `candidate` holds the decision of the second.
    /// [`Side::Recorded`] names the first candidate.
    pub turns: Vec<TurnDiff>,
    /// The index of the first turn where the candidates diverge.
    pub first_divergence: Option<usize>,
    /// `true` when the two candidates end in different ways, by the rule of
    /// [`Evaluation::end_diverged`].
    pub end_diverged: bool,
}

impl Comparison {
    /// `true` when a turn diverges or the run ends differ.
    #[must_use]
    pub const fn diverged(&self) -> bool {
        self.first_divergence.is_some() || self.end_diverged
    }
}

/// Evaluate `first` and `second` against one recorded `history`, then
/// compare the two candidates turn by turn (issue #2271).
///
/// Give both candidates one harness with two named models, and set
/// [`Candidate::model`] or [`Candidate::agent_model`] on each. The two
/// evaluations run one after the other, each with its own live model calls.
///
/// # Errors
///
/// The errors of [`evaluate`]. A refused history costs no model call.
pub async fn compare(
    history: &[WorkflowEvent],
    first: &Candidate,
    second: &Candidate,
) -> Result<Comparison, EvalError> {
    let first = evaluate(history, first).await?;
    let second = evaluate(history, second).await?;
    let decision = |evaluation: &Evaluation, turn: usize| {
        evaluation
            .turns
            .get(turn)
            .and_then(|diff| diff.candidate.clone())
    };
    let turns: Vec<TurnDiff> = (0..first.turns.len().max(second.turns.len()))
        .filter_map(|turn| {
            let (one, two) = (decision(&first, turn), decision(&second, turn));
            // A turn that only the source has is not a candidate turn.
            (one.is_some() || two.is_some()).then(|| TurnDiff {
                turn,
                verdict: verdict(one.as_ref(), two.as_ref()),
                recorded: one,
                candidate: two,
            })
        })
        .collect();
    let first_divergence = turns
        .iter()
        .position(|turn| matches!(turn.verdict, Verdict::Diverged(_)));
    let end_diverged = ends_differ(&first.candidate, &second.candidate);
    Ok(Comparison {
        first,
        second,
        turns,
        first_divergence,
        end_diverged,
    })
}

/// What the source run recorded.
struct Recording {
    task: AgentTask,
    /// The run id of the first recorded model request.
    run_id: Option<String>,
    turns: Vec<ModelTurn>,
    tools: Vec<RecordedTool>,
    /// The recorded snapshots, and the error of a snapshot that failed for
    /// good.
    snapshots: Vec<Result<MemorySnapshot, String>>,
    /// Signals for waits that the source opened, received in time, in order.
    signals: Vec<(String, Value)>,
    /// The recorded peer lookups: the names asked and the peers found.
    peers: Vec<RecordedPeers>,
    /// The agent of each recorded model turn, in order.
    turn_agents: Vec<AgentKey>,
    /// The child runs of the fan-outs, in start order (issue #2270).
    children: Vec<RecordedChild>,
    end: RunEnd,
}

/// One recorded peer lookup.
#[derive(Clone)]
struct RecordedPeers {
    names: Value,
    /// The peers found, or the error of a lookup that failed for good.
    peers: Result<Vec<Peer>, String>,
    /// A recorded lookup answers one candidate lookup at most.
    used: bool,
}

/// One recorded tool call and its outcome.
#[derive(Clone)]
struct RecordedTool {
    /// The agent of the tree that made the call.
    agent: AgentKey,
    step: u32,
    name: String,
    arguments: Value,
    /// The recorded outcome, or the error of a call that failed for good.
    outcome: Result<ToolOutcome, String>,
    /// A recorded outcome answers one candidate call at most.
    used: bool,
}

impl Recording {
    fn read(history: &[WorkflowEvent]) -> Result<Self, EvalError> {
        let task = task_of(history)?;
        let model_name = agent_model_turn_info().name;
        let mut scheduled: HashMap<ActivityExecId, (&str, &Value)> = HashMap::new();
        let mut run_id = None;
        let mut effects = Effects::default();
        let mut opened: HashSet<&str> = HashSet::new();
        let mut timed_out: HashSet<&str> = HashSet::new();
        let mut signals = Vec::new();
        let mut children = Children::default();
        let mut end = None;
        // The last tool or snapshot failure with no later completion. A
        // failed source can end on it.
        let mut last_failure: Option<(ActivityExecId, String)> = None;
        for event in history {
            if matches!(event, WorkflowEvent::ActivityCompleted { .. }) {
                last_failure = None;
            }
            if children.read(event) {
                continue;
            }
            match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name,
                    input,
                    ..
                } => {
                    if run_id.is_none() && name == model_name {
                        let request: ModelTurnRequest =
                            decode(input.clone()).map_err(EvalError::Undecodable)?;
                        run_id = Some(request.run_id);
                    }
                    scheduled.insert(*activity_id, (name.as_str(), input));
                }
                WorkflowEvent::ActivityFailed {
                    activity_id, error, ..
                } => last_failure = Some((*activity_id, error.clone())),
                WorkflowEvent::ActivityTimedOut {
                    activity_id,
                    timeout_type,
                } => {
                    last_failure = Some((*activity_id, format!("timed out: {timeout_type:?}")));
                }
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output,
                } => {
                    let scheduled = scheduled.get(activity_id).copied();
                    effects.completed(scheduled, output, run_id.as_deref())?;
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => {
                    if let Some(signal) = deadline_signal(timer_id.as_str()) {
                        opened.insert(signal);
                    }
                }
                WorkflowEvent::TimerFired { timer_id } => {
                    if let Some(signal) = deadline_signal(timer_id.as_str()) {
                        timed_out.insert(signal);
                    }
                }
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    // The deadline fired first, so the source never read
                    // this signal.
                    if !timed_out.contains(signal_name.as_str()) {
                        signals.push((signal_name.clone(), payload.clone()));
                    }
                }
                // A redrive reopens a failed run. A later terminal event ends
                // it again.
                WorkflowEvent::WorkflowRedriven { .. } => {
                    end = None;
                    last_failure = None;
                }
                other => {
                    if let Some(found) = run_end(other)? {
                        end = Some(found);
                    }
                }
            }
        }
        // A source that failed on a tool, snapshot or peer-lookup activity
        // replays that failure, so the candidate fails at the same activity.
        if let (Some(RunEnd::Failed(_)), Some((activity_id, error))) = (&end, last_failure) {
            let scheduled = scheduled.get(&activity_id).copied();
            effects.failed(scheduled, error, run_id.as_deref())?;
        }
        // Only a signal for a wait that the source opened is sent again. A
        // wait opens with its deadline timer. A signal that arrived before
        // its wait opened leaves no timer, so it is dropped too. The candidate
        // then times out, which shows as a divergence.
        signals.retain(|(name, _)| opened.contains(name.as_str()));
        let turns = model_turns(history)?;
        let turn_agents = model_turn_agents(history, run_id.as_deref())?;
        name_tool_agents(&mut effects.tools, &turn_agents);
        Ok(Self {
            task,
            run_id,
            turns,
            tools: effects.tools,
            snapshots: effects.snapshots,
            signals,
            peers: effects.peers,
            turn_agents,
            children: children.results,
            end: end.ok_or(EvalError::InFlightSource)?,
        })
    }
}

/// Name the agent of each recorded tool call from the model turns of its
/// lane. One lane holds one agent in a run, so its model turns name it.
fn name_tool_agents(tools: &mut [RecordedTool], turn_agents: &[AgentKey]) {
    let names: HashMap<&str, &Option<String>> = turn_agents
        .iter()
        .map(|agent| (agent.lane.as_str(), &agent.name))
        .collect();
    for tool in tools {
        tool.agent.name = names
            .get(tool.agent.lane.as_str())
            .and_then(|name| (*name).clone());
    }
}

/// One child run of a fan-out of a source run (issue #2270).
#[derive(Clone)]
struct RecordedChild {
    /// The child task, as history records it.
    input: Value,
    /// The result, or `None` for a child that has no result yet.
    result: Option<Result<Value, String>>,
    /// Whether a candidate child took the result. Each result answers one
    /// child.
    used: bool,
}

impl RecordedChild {
    /// Whether this child answers the candidate child `task`.
    ///
    /// The two must have the same delegation chain and task text. The chain
    /// names the delegating agents and the peer. The token share, the model
    /// and the flow labels can differ.
    fn answers(&self, task: &Value) -> bool {
        ["/input", "/mandate/chain"]
            .iter()
            .all(|at| self.input.pointer(at) == task.pointer(at))
    }
}

/// The child runs of the fan-outs of a source run (issue #2270).
#[derive(Default)]
struct Children {
    /// Each child run, in start order.
    results: Vec<RecordedChild>,
    /// The place of each child run in `results`, by child id.
    index: HashMap<String, usize>,
}

impl Children {
    /// Read one event. Returns `true` when it is a child-run event.
    fn read(&mut self, event: &WorkflowEvent) -> bool {
        let (child_id, result) = match event {
            WorkflowEvent::ChildWorkflowStarted {
                child_id, input, ..
            } => {
                self.index.insert(child_id.to_string(), self.results.len());
                self.results.push(RecordedChild {
                    input: input.clone(),
                    result: None,
                    used: false,
                });
                return true;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, output } => {
                (child_id, Ok(output.clone()))
            }
            WorkflowEvent::ChildWorkflowFailed {
                child_id, error, ..
            } => (child_id, Err(error.clone())),
            _ => return false,
        };
        if let Some(&at) = self.index.get(&child_id.to_string()) {
            self.results[at].result = Some(result);
        }
        true
    }
}

/// The recorded results of the effect activities of a source run.
#[derive(Default)]
struct Effects {
    tools: Vec<RecordedTool>,
    /// The recorded snapshots, and the error of a snapshot that failed for
    /// good.
    snapshots: Vec<Result<MemorySnapshot, String>>,
    peers: Vec<RecordedPeers>,
}

impl Effects {
    /// Record the `output` of the `scheduled` activity, when it is a tool
    /// call, a snapshot or a peer lookup. `root` is the run id of the root
    /// agent.
    fn completed(
        &mut self,
        scheduled: Option<(&str, &Value)>,
        output: &Value,
        root: Option<&str>,
    ) -> Result<(), EvalError> {
        let Some((name, input)) = scheduled else {
            return Ok(());
        };
        if name == agent_tool_call_info().name {
            let outcome = decode(output.clone()).map_err(EvalError::Undecodable)?;
            self.tools.push(recorded_tool(input, Ok(outcome), root)?);
        } else if name == agent_discover_peers_info().name {
            self.peers.push(RecordedPeers {
                names: input.clone(),
                peers: Ok(decode(output.clone()).map_err(EvalError::Undecodable)?),
                used: false,
            });
        } else if name == agent_memory_snapshot_info().name {
            let snapshot = decode(output.clone()).map_err(EvalError::Undecodable)?;
            self.snapshots.push(Ok(snapshot));
        }
        Ok(())
    }

    /// Record the terminal failure of the `scheduled` activity, when it is a
    /// tool call, a snapshot or a peer lookup.
    fn failed(
        &mut self,
        scheduled: Option<(&str, &Value)>,
        error: String,
        root: Option<&str>,
    ) -> Result<(), EvalError> {
        match scheduled {
            Some((name, input)) if name == agent_tool_call_info().name => {
                self.tools.push(recorded_tool(input, Err(error), root)?);
            }
            Some((name, _)) if name == agent_memory_snapshot_info().name => {
                self.snapshots.push(Err(error));
            }
            Some((name, input)) if name == agent_discover_peers_info().name => {
                self.peers.push(RecordedPeers {
                    names: input.clone(),
                    peers: Err(error),
                    used: false,
                });
            }
            _ => {}
        }
        Ok(())
    }
}

/// The task of an agent run history.
///
/// # Errors
///
/// Returns [`EvalError::ErasedSource`] for an erased history, and
/// [`EvalError::NotAnAgentRun`] for any history that is not an agent run.
fn task_of(history: &[WorkflowEvent]) -> Result<AgentTask, EvalError> {
    let Some(WorkflowEvent::WorkflowStarted { input, .. }) = history.first() else {
        return Err(EvalError::NotAnAgentRun(
            "the history does not start with WorkflowStarted".to_owned(),
        ));
    };
    // The erasure check runs first. An erased payload must not reach a
    // decoder or a prompt.
    if history.iter().any(holds_tombstone) {
        return Err(EvalError::ErasedSource);
    }
    serde_json::from_value(input.clone())
        .map_err(|e| EvalError::NotAnAgentRun(format!("the input is not an AgentTask: {e}")))
}

/// The recorded tool call of the tool-call activity `input`, with `outcome`.
/// `root` is the run id of the root agent.
fn recorded_tool(
    input: &Value,
    outcome: Result<ToolOutcome, String>,
    root: Option<&str>,
) -> Result<RecordedTool, EvalError> {
    let request: ToolCallRequest = decode(input.clone()).map_err(EvalError::Undecodable)?;
    Ok(RecordedTool {
        // The name comes from the model turns of the lane, after the read.
        agent: AgentKey {
            lane: lane_of(&request.run_id, root).to_owned(),
            name: None,
        },
        step: request.step,
        name: request.call.name,
        arguments: request.call.arguments,
        outcome,
        used: false,
    })
}

/// The lane of `run_id` under the root run id `root`: the suffix that names
/// one agent of a delegation tree. The root agent, and a run id that does not
/// start with `root`, have the empty lane.
fn lane_of<'a>(run_id: &'a str, root: Option<&str>) -> &'a str {
    root.and_then(|root| run_id.strip_prefix(root))
        .unwrap_or_default()
}

/// One agent of a delegation tree.
///
/// The lane is a delegation ordinal, so two runs can give the same lane to
/// different peers. The name tells them apart.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentKey {
    /// The run id suffix after the root run id. The root agent has an empty
    /// lane.
    lane: String,
    /// The agent that holds the mandate, or `None` for a run with no
    /// mandate.
    name: Option<String>,
}

/// How a terminal event ends a run, or `None` for another event.
///
/// # Errors
///
/// Returns [`EvalError::InterruptedSource`] for a cancel or a timeout.
fn run_end(event: &WorkflowEvent) -> Result<Option<RunEnd>, EvalError> {
    Ok(match event {
        WorkflowEvent::WorkflowCompleted { output } => Some(RunEnd::Completed(
            decode(output.clone()).map_err(EvalError::Undecodable)?,
        )),
        WorkflowEvent::WorkflowFailed { error, .. } => Some(RunEnd::Failed(error.clone())),
        // An outside event cut the run, so its last turn is not an end.
        WorkflowEvent::WorkflowCancelled { reason } => {
            return Err(EvalError::InterruptedSource(format!("cancelled: {reason}")));
        }
        WorkflowEvent::WorkflowExecutionTimedOut { .. } => {
            return Err(EvalError::InterruptedSource("timed out".to_owned()));
        }
        _ => None,
    })
}

/// The mutable state that the activity mocks share.
struct Session {
    /// The tool calls of each recorded turn, for id alignment.
    recorded_calls: Vec<Vec<ToolCall>>,
    /// The agent of each recorded turn. A candidate turn of another agent
    /// takes no recorded id.
    recorded_agents: Vec<AgentKey>,
    /// The name of the agent in each candidate lane, from its model turns.
    candidate_names: HashMap<String, Option<String>>,
    /// The index of the next candidate turn.
    turn: usize,
    max_turns: usize,
    cap_reached: bool,
    tools: Vec<RecordedTool>,
    snapshots: VecDeque<Result<MemorySnapshot, String>>,
    peers: Vec<RecordedPeers>,
    /// The run id of the candidate root agent: the run id of its first model
    /// request.
    root: Option<String>,
    replayed: u32,
    stubbed: u32,
}

impl Session {
    /// Claim the next candidate turn, of `agent`. Returns its index and the
    /// recorded calls of the same turn.
    ///
    /// A recorded turn of another agent gives no calls. Its call ids could
    /// otherwise match an approval that a person gave to another agent.
    ///
    /// # Errors
    ///
    /// Returns [`TURN_CAP_ERROR`] when the turn cap is reached.
    fn next_turn(&mut self, agent: &AgentKey) -> Result<(usize, Vec<ToolCall>), String> {
        if self.turn >= self.max_turns {
            self.cap_reached = true;
            return Err(TURN_CAP_ERROR.to_owned());
        }
        let index = self.turn;
        self.turn += 1;
        let same_agent = self
            .recorded_agents
            .get(index)
            .is_none_or(|recorded| recorded == agent);
        let recorded = if same_agent {
            self.recorded_calls.get(index).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };
        Ok((index, recorded))
    }

    /// The recorded outcome of the same call, or the stub outcome.
    ///
    /// # Errors
    ///
    /// Returns the recorded error of a call that failed for good.
    fn answer(&mut self, request: &ToolCallRequest) -> Result<ToolOutcome, String> {
        let lane = lane_of(&request.run_id, self.root.as_deref()).to_owned();
        let name = self.candidate_names.get(&lane).cloned().flatten();
        let agent = AgentKey { lane, name };
        let recorded = self.tools.iter_mut().find(|tool| {
            !tool.used
                && tool.agent == agent
                && tool.step == request.step
                && tool.name == request.call.name
                && tool.arguments == request.call.arguments
        });
        if let Some(tool) = recorded {
            tool.used = true;
            self.replayed = self.replayed.saturating_add(1);
            tool.outcome.clone()
        } else {
            self.stubbed = self.stubbed.saturating_add(1);
            Ok(ToolOutcome::error(NOT_RUN))
        }
    }

    /// The recorded peers for the same `names`, or none.
    ///
    /// # Errors
    ///
    /// Returns the recorded error of a lookup that failed the source.
    fn lookup(&mut self, names: &Value) -> Result<Vec<Peer>, String> {
        self.peers
            .iter_mut()
            .find(|recorded| !recorded.used && recorded.names == *names)
            .map_or_else(
                || Ok(Vec::new()),
                |recorded| {
                    recorded.used = true;
                    recorded.peers.clone()
                },
            )
    }
}

/// Align the call ids in the content of candidate turn `index` with the
/// recorded turn.
///
/// A call that equals the recorded call at the same position takes the
/// recorded id. Any other call gets a new `eval_` id that no recorded call
/// holds, so no recorded approval name can match it.
fn align_call_ids(
    content: &mut [ContentPart],
    recorded: &[ToolCall],
    taken: &HashSet<String>,
    index: usize,
) {
    let calls = content.iter_mut().filter_map(|part| match part {
        ContentPart::ToolCall {
            id,
            name,
            arguments,
        } => Some((id, name, arguments)),
        _ => None,
    });
    for (position, (id, name, arguments)) in calls.enumerate() {
        match recorded.get(position) {
            Some(original) if *name == original.name && *arguments == original.arguments => {
                id.clone_from(&original.id);
            }
            _ => {
                let base = format!("{EVAL_ID_PREFIX}{index}_{position}_{id}");
                let mut fresh = base.clone();
                let mut suffix = 0_u32;
                while taken.contains(&fresh) {
                    suffix = suffix.saturating_add(1);
                    fresh = format!("{base}_{suffix}");
                }
                *id = fresh;
            }
        }
    }
}

/// The test engine that runs the candidate.
///
/// Each sequential await costs one engine cycle, and one turn can hold any
/// number of tool calls. The engine cap guards against an endless loop. The
/// turn cap already bounds this loop, so the harness lifts the cap.
fn engine() -> WorkflowTestEnv {
    WorkflowTestEnv::new()
        .with_max_iterations(usize::MAX)
        .with_workflow_name(WORKFLOW_NAME)
}

/// The mock of the model activity: the live candidate turn.
fn model_mock(
    session: &Arc<Mutex<Session>>,
    candidate: &Candidate,
    recording: &Recording,
    handle: tokio::runtime::Handle,
) -> impl Fn(Value) -> Result<Value, String> + Send + Sync + 'static {
    let session = Arc::clone(session);
    let harness = Arc::clone(&candidate.harness);
    let models = candidate.clone();
    let run_id = recording.run_id.clone();
    let taken: HashSet<String> = recording
        .turns
        .iter()
        .flat_map(ModelTurn::calls)
        .map(|call| call.id)
        .collect();
    let retry = agent_model_turn_info()
        .default_retry_policy
        .unwrap_or_default();
    move |input: Value| -> Result<Value, String> {
        let mut request: ModelTurnRequest = decode(input)?;
        if let Some(name) = models.model_for(request.agent.as_deref()) {
            request.model = Some(name.clone());
        }
        let (index, recorded) = {
            let mut session = lock(&session);
            // The root agent makes the first model request.
            let root = session.root.get_or_insert_with(|| request.run_id.clone());
            let lane = lane_of(&request.run_id, Some(root.as_str())).to_owned();
            let agent = AgentKey {
                lane: lane.clone(),
                name: request.agent.clone(),
            };
            session
                .candidate_names
                .insert(lane.clone(), request.agent.clone());
            // The test engine has its own ids. The policy sees the recorded
            // run id instead, with the lane of the same agent.
            if let Some(run_id) = &run_id {
                request.run_id = format!("{run_id}{lane}");
            }
            session.next_turn(&agent)?
        };
        let turn = live_turn(
            &handle,
            &harness,
            &request,
            (&recorded, &taken, index),
            &retry,
        )?;
        encode(&turn)
    }
}

/// Ask the candidate for turn `index`. The call ids change before the policy
/// runs, so the policy sees the ids that the transcript records.
///
/// The test engine does not retry an activity. So this function applies the
/// retry policy of the model activity, as a worker does.
///
/// # Errors
///
/// Returns the final failure payload of the model turn.
fn live_turn(
    handle: &tokio::runtime::Handle,
    harness: &AgentHarness,
    request: &ModelTurnRequest,
    (recorded, taken, index): (&[ToolCall], &HashSet<String>, usize),
    retry: &RetryPolicy,
) -> Result<ModelTurn, String> {
    // The cap applies after the call, to the turn as a worker records it. So
    // the harness must not cut the turn with the aligned ids.
    let result_cap = harness.result_cap();
    let harness = &harness.clone().max_result_bytes(0);
    let mut attempt = 1;
    loop {
        // The provider ids, before the alignment changes them.
        let mut provider_ids = Vec::new();
        let align = |content: &mut Vec<ContentPart>| {
            provider_ids = call_ids(content);
            align_call_ids(content, recorded, taken, index);
        };
        let result = tokio::task::block_in_place(|| {
            handle.block_on(harness.model_turn_with(request.clone(), align))
        });
        let payload = match result {
            Ok(turn) => return Ok(fit_result_cap(turn, &provider_ids, result_cap)),
            Err(payload) => payload,
        };
        let delay = retry_delay(&payload, retry, attempt).ok_or(payload)?;
        tokio::task::block_in_place(|| handle.block_on(tokio::time::sleep(delay)));
        attempt += 1;
    }
}

/// The ids of the tool calls in `content`, in order.
fn call_ids(content: &[ContentPart]) -> Vec<String> {
    content
        .iter()
        .filter_map(|part| match part {
            ContentPart::ToolCall { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect()
}

/// Fit a turn to the worker result cap, as a worker with no payload store
/// does: cut the decision reasons, or drop the reply.
///
/// A worker records the turn with the provider ids. So the fit measures the
/// turn with `provider_ids`, not with the aligned ids.
fn fit_result_cap(turn: ModelTurn, provider_ids: &[String], cap: u64) -> ModelTurn {
    let mut as_recorded = turn.clone();
    let parts = as_recorded
        .content
        .iter_mut()
        .filter_map(|part| match part {
            ContentPart::ToolCall { id, .. } => Some(id),
            _ => None,
        });
    for (id, provider_id) in parts.zip(provider_ids) {
        id.clone_from(provider_id);
    }
    let fitted = fit_turn(as_recorded, cap);
    if fitted.oversized {
        return turn.into_oversized();
    }
    ModelTurn {
        decisions: fitted.decisions,
        ..turn
    }
}

/// The wait before attempt `attempt + 1` of a failed model turn, or `None`
/// when the failure is final.
fn retry_delay(payload: &str, policy: &RetryPolicy, attempt: u32) -> Option<Duration> {
    let typed = parse_typed_payload(payload);
    let final_failure = typed.as_ref().is_some_and(|failure| failure.non_retryable)
        || policy.is_non_retryable(
            typed.as_ref().map(|failure| failure.error_type.as_str()),
            payload,
        );
    if final_failure {
        return None;
    }
    policy.next_delay(attempt)
}

/// The completed model turns of a history, in order.
fn model_turns(history: &[WorkflowEvent]) -> Result<Vec<ModelTurn>, EvalError> {
    let model_name = agent_model_turn_info().name;
    let mut scheduled: HashSet<ActivityExecId> = HashSet::new();
    let mut turns = Vec::new();
    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == model_name => {
                scheduled.insert(*activity_id);
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } if scheduled.contains(activity_id) => {
                turns.push(decode(output.clone()).map_err(EvalError::Undecodable)?);
            }
            _ => {}
        }
    }
    Ok(turns)
}

/// The agent of each completed model turn of a history, in order. `root` is
/// the run id of the root agent.
fn model_turn_agents(
    history: &[WorkflowEvent],
    root: Option<&str>,
) -> Result<Vec<AgentKey>, EvalError> {
    let model_name = agent_model_turn_info().name;
    let mut scheduled: HashMap<ActivityExecId, AgentKey> = HashMap::new();
    let mut agents = Vec::new();
    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input,
                ..
            } if name == model_name => {
                let request: ModelTurnRequest =
                    decode(input.clone()).map_err(EvalError::Undecodable)?;
                let agent = AgentKey {
                    lane: lane_of(&request.run_id, root).to_owned(),
                    name: request.agent,
                };
                scheduled.insert(*activity_id, agent);
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. } => {
                if let Some(agent) = scheduled.remove(activity_id) {
                    agents.push(agent);
                }
            }
            _ => {}
        }
    }
    Ok(agents)
}

/// Compare the two runs turn by turn.
fn diff_turns(recorded: &[ModelTurn], candidate: &[ModelTurn]) -> Vec<TurnDiff> {
    (0..recorded.len().max(candidate.len()))
        .map(|turn| {
            let recorded = recorded.get(turn).map(TurnDecision::of);
            let candidate = candidate.get(turn).map(TurnDecision::of);
            let verdict = verdict(recorded.as_ref(), candidate.as_ref());
            TurnDiff {
                turn,
                recorded,
                candidate,
                verdict,
            }
        })
        .collect()
}

/// The verdict on one pair of decisions.
fn verdict(recorded: Option<&TurnDecision>, candidate: Option<&TurnDecision>) -> Verdict {
    let (recorded, candidate) = match (recorded, candidate) {
        (Some(recorded), Some(candidate)) => (recorded, candidate),
        (Some(_), None) => return Verdict::Diverged(Divergence::Missing(Side::Candidate)),
        (None, _) => return Verdict::Diverged(Divergence::Missing(Side::Recorded)),
    };
    let calls_of = |decision: &TurnDecision| -> Vec<(String, Value)> {
        decision
            .calls
            .iter()
            .map(|call| (call.name.clone(), call.arguments.clone()))
            .collect()
    };
    let decisions_of = |decision: &TurnDecision| -> Vec<ToolDecision> {
        decision
            .calls
            .iter()
            .map(|call| call.decision.clone())
            .collect()
    };
    if recorded.calls.is_empty() != candidate.calls.is_empty() {
        Verdict::Diverged(Divergence::Shape)
    } else if calls_of(recorded) != calls_of(candidate) {
        Verdict::Diverged(Divergence::Calls)
    } else if decisions_of(recorded) != decisions_of(candidate) {
        Verdict::Diverged(Divergence::Policy)
    } else if recorded.dropped != candidate.dropped || recorded.stop != candidate.stop {
        Verdict::Diverged(Divergence::Stop)
    } else if recorded.calls.is_empty() && recorded.text != candidate.text {
        Verdict::Reworded
    } else {
        Verdict::Same
    }
}

/// `true` when the two runs end in different ways.
fn ends_differ(recorded: &RunEnd, candidate: &RunEnd) -> bool {
    match (recorded, candidate) {
        (RunEnd::Completed(recorded), RunEnd::Completed(candidate)) => {
            recorded.stop != candidate.stop
        }
        (RunEnd::Failed(_), RunEnd::Failed(_)) => false,
        _ => true,
    }
}

/// The signal of a signal-wait deadline timer, or `None` for another timer.
fn deadline_signal(timer_id: &str) -> Option<&str> {
    let rest = timer_id.strip_prefix(SIGNAL_TIMEOUT_PREFIX)?;
    let (seq, signal) = rest.split_once(':')?;
    seq.parse::<u64>().ok()?;
    Some(signal)
}

/// `true` when any payload of `event` holds an erasure tombstone.
fn holds_tombstone(event: &WorkflowEvent) -> bool {
    fn walk(value: &Value) -> bool {
        is_erasure_tombstone(value)
            || match value {
                Value::Array(items) => items.iter().any(walk),
                Value::Object(map) => map.values().any(walk),
                _ => false,
            }
    }
    // An event that does not serialize cannot be checked, so it counts as
    // erased. The check fails closed.
    serde_json::to_value(event).map_or(true, |value| walk(&value))
}

/// The current Tokio runtime, when it can block in place.
fn multi_thread_handle() -> Result<tokio::runtime::Handle, EvalError> {
    let handle =
        tokio::runtime::Handle::try_current().map_err(|_| EvalError::MultiThreadRuntimeRequired)?;
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
        return Err(EvalError::MultiThreadRuntimeRequired);
    }
    Ok(handle)
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}

fn encode<T: Serialize>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

/// Lock `mutex`. A poisoned lock still holds valid counts, so the harness
/// keeps going.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::TokenUsage;
    use serde_json::json;

    fn decision(calls: &[(&str, Value)], text: &str, stop: StopReason) -> TurnDecision {
        TurnDecision {
            calls: calls
                .iter()
                .map(|(name, arguments)| CallDecision {
                    name: (*name).to_owned(),
                    arguments: arguments.clone(),
                    decision: ToolDecision::Allow,
                })
                .collect(),
            text: text.to_owned(),
            stop,
            dropped: false,
        }
    }

    fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
        }
    }

    fn turn_of(calls: &[ToolCall]) -> ModelTurn {
        ModelTurn {
            content: calls
                .iter()
                .map(|call| ContentPart::ToolCall {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                })
                .collect(),
            stop: StopReason::ToolUse,
            usage: TokenUsage::default(),
            decisions: Vec::new(),
            cache_hit: false,
            labels: Vec::new(),
            releases: Vec::new(),
            oversized: false,
            interruptible: Vec::new(),
        }
    }

    #[test]
    fn a_verdict_names_each_kind_of_difference() {
        let call = decision(&[("a", json!(1))], "", StopReason::ToolUse);
        let other_call = decision(&[("b", json!(1))], "", StopReason::ToolUse);
        let answer = decision(&[], "hi", StopReason::EndTurn);
        let reworded = decision(&[], "hello", StopReason::EndTurn);
        let capped = decision(&[], "hi", StopReason::MaxTokens);
        let mut denied = call.clone();
        denied.calls[0].decision = ToolDecision::Deny {
            reason: "no".into(),
        };

        assert_eq!(verdict(Some(&call), Some(&call)), Verdict::Same);
        assert_eq!(verdict(Some(&answer), Some(&reworded)), Verdict::Reworded);
        assert_eq!(
            verdict(Some(&call), Some(&answer)),
            Verdict::Diverged(Divergence::Shape)
        );
        assert_eq!(
            verdict(Some(&call), Some(&other_call)),
            Verdict::Diverged(Divergence::Calls)
        );
        assert_eq!(
            verdict(Some(&call), Some(&denied)),
            Verdict::Diverged(Divergence::Policy)
        );
        assert_eq!(
            verdict(Some(&answer), Some(&capped)),
            Verdict::Diverged(Divergence::Stop)
        );
        assert_eq!(
            verdict(Some(&answer), None),
            Verdict::Diverged(Divergence::Missing(Side::Candidate))
        );
        assert_eq!(
            verdict(None, Some(&answer)),
            Verdict::Diverged(Divergence::Missing(Side::Recorded))
        );
    }

    #[test]
    fn the_text_of_a_tool_turn_is_not_compared() {
        let before = decision(&[("a", json!(1))], "Let me look.", StopReason::ToolUse);
        let after = decision(&[("a", json!(1))], "Checking.", StopReason::ToolUse);
        assert_eq!(verdict(Some(&before), Some(&after)), Verdict::Same);
    }

    #[test]
    fn a_call_with_no_recorded_decision_reads_as_denied() {
        let turn = turn_of(&[call("c1", "a", json!(1))]);
        let decision = TurnDecision::of(&turn);
        assert!(matches!(
            decision.calls[0].decision,
            ToolDecision::Deny { .. }
        ));
    }

    #[test]
    fn only_a_retryable_failure_waits_for_another_attempt() {
        use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
        let policy = RetryPolicy::exponential(3, Duration::from_millis(10));
        let retryable = ActivityFailure::retryable("RateLimited", "slow").into_error_payload();
        let fatal = ActivityFailure::non_retryable("Config", "bad key").into_error_payload();
        assert!(retry_delay(&retryable, &policy, 1).is_some());
        assert!(
            retry_delay(&retryable, &policy, 3).is_none(),
            "the attempts run out"
        );
        assert!(retry_delay(&fatal, &policy, 1).is_none());
    }

    #[test]
    fn a_deadline_timer_names_its_signal() {
        assert_eq!(
            deadline_signal("__signal_timeout:3:tool_approval:0:0:w1"),
            Some("tool_approval:0:0:w1")
        );
        assert_eq!(deadline_signal("agent_followup:0"), None);
        assert_eq!(deadline_signal("__signal_timeout:x:name"), None);
    }

    #[test]
    fn a_nested_tombstone_counts_as_erased() {
        let event = WorkflowEvent::SignalReceived {
            signal_name: "s".into(),
            payload: json!({"a": [{"_harvest_erased": true}]}),
        };
        assert!(holds_tombstone(&event));
        let clean = WorkflowEvent::SignalReceived {
            signal_name: "s".into(),
            payload: json!({"_harvest_erased": false}),
        };
        assert!(!holds_tombstone(&clean));
    }

    #[test]
    fn only_an_equal_call_takes_the_recorded_id() {
        let recorded = vec![call("r1", "a", json!(1)), call("r2", "b", json!(2))];
        let mut turn = turn_of(&[call("c1", "a", json!(1)), call("c2", "b", json!(3))]);
        align_call_ids(&mut turn.content, &recorded, &HashSet::new(), 4);
        let ids: Vec<String> = turn.calls().into_iter().map(|call| call.id).collect();
        assert_eq!(ids, vec!["r1".to_owned(), "eval_4_1_c2".to_owned()]);
    }

    #[test]
    fn a_new_id_never_equals_a_recorded_id() {
        let recorded = vec![call("eval_0_0_c1", "a", json!(1))];
        let taken: HashSet<String> = recorded.iter().map(|call| call.id.clone()).collect();
        let mut turn = turn_of(&[call("c1", "b", json!(2))]);
        align_call_ids(&mut turn.content, &recorded, &taken, 0);
        let id = turn.calls()[0].id.clone();
        assert!(id.starts_with("eval_0_0_c1"), "{id}");
        assert_ne!(id, "eval_0_0_c1");
    }

    #[test]
    fn the_result_cap_cuts_a_long_reason_before_it_drops_the_reply() {
        let mut turn = turn_of(&[call("c1", "a", json!(1))]);
        turn.decisions = vec![ToolDecision::Deny {
            reason: "r".repeat(5_000),
        }];
        let fitted = fit_result_cap(turn.clone(), &["c1".to_owned()], 2_048);
        assert!(!fitted.oversized);
        assert_eq!(fitted.content, turn.content);
        let [ToolDecision::Deny { reason }] = fitted.decisions.as_slice() else {
            panic!("one cut reason: {:?}", fitted.decisions);
        };
        assert!(reason.len() <= 256, "{}", reason.len());
    }

    #[test]
    fn a_dropped_reply_is_a_divergence() {
        let answer = decision(&[], "done", StopReason::EndTurn);
        let dropped = TurnDecision {
            text: String::new(),
            dropped: true,
            ..answer.clone()
        };
        assert_eq!(
            verdict(Some(&answer), Some(&dropped)),
            Verdict::Diverged(Divergence::Stop)
        );
        assert_eq!(
            verdict(Some(&dropped), Some(&answer)),
            Verdict::Diverged(Divergence::Stop)
        );
        assert_eq!(verdict(Some(&dropped), Some(&dropped)), Verdict::Same);
        // The flag is left out at its default, so a recorded diff encodes as before.
        assert!(
            serde_json::to_value(&answer)
                .unwrap()
                .get("dropped")
                .is_none()
        );
    }

    #[test]
    fn the_result_cap_measures_the_provider_ids() {
        let mut turn = turn_of(&[call("c1", "a", json!(1))]);
        let fits = crate::bounds::json_len(&turn);
        align_call_ids(&mut turn.content, &[], &HashSet::new(), 0);
        let original = vec!["c1".to_owned()];
        // The aligned ids are longer, so only the provider form fits.
        let fitted = fit_result_cap(turn.clone(), &original, fits);
        assert!(!fitted.oversized);
        assert_eq!(fitted.content, turn.content, "the aligned ids stay");
        assert!(fit_result_cap(turn.clone(), &original, fits - 1).oversized);
        assert!(
            !fit_result_cap(turn, &original, 0).oversized,
            "a zero cap is no cap"
        );
    }

    #[test]
    fn the_engine_does_not_cap_the_cycles_of_a_turn() {
        assert_eq!(engine().max_iterations(), usize::MAX);
    }

    #[test]
    fn the_report_shapes_use_named_tags() {
        assert_eq!(json!(Verdict::Same), json!({"verdict": "same"}));
        assert_eq!(
            json!(Verdict::Diverged(Divergence::Missing(Side::Recorded))),
            json!({"verdict": "diverged", "divergence": {"kind": "missing", "side": "recorded"}})
        );
        let call = CallDecision {
            name: "a".into(),
            arguments: json!(1),
            decision: ToolDecision::Deny {
                reason: "no".into(),
            },
        };
        let value = json!(call);
        assert_eq!(
            value,
            json!({"name": "a", "arguments": 1, "decision": "deny", "reason": "no"})
        );
        let back: CallDecision = serde_json::from_value(value).unwrap();
        assert_eq!(back, call);
    }

    #[test]
    fn two_ends_differ_by_kind_and_stop() {
        let failed = RunEnd::Failed("x".into());
        assert!(!ends_differ(&failed, &RunEnd::Failed("y".into())));
        let report = |stop| {
            RunEnd::Completed(AgentReport {
                stop,
                text: String::new(),
                steps_used: 0,
                tool_calls: 0,
                usage: TokenUsage::default(),
                messages: Vec::new(),
                followups: 0,
                followup_dropped: false,
                delegations: Vec::new(),
                flow_labels: std::collections::BTreeSet::new(),
                run_id: None,
                workspace: None,
                error: None,
                resets: 0,
            })
        };
        let done = report(crate::AgentStop::Completed);
        assert!(!ends_differ(&done, &done.clone()));
        assert!(ends_differ(
            &done,
            &report(crate::AgentStop::TokensExhausted)
        ));
        assert!(ends_differ(&done, &failed));
        assert!(ends_differ(&failed, &done));
    }
}
