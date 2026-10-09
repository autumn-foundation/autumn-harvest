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
//!   retry policy of the activity, as on a worker.
//! - `agent_tool_call` returns the recorded outcome of the same call. A call
//!   with no recorded outcome gets an error stub. No tool runs.
//! - `agent_memory_snapshot` returns the recorded snapshot, or an empty one.
//! - `agent_deliver` is a stub. No report leaves the harness.
//! - Any other activity has no mock, so it fails the candidate run.
//!
//! The harness sends again each signal that the source received before its
//! deadline. Approvals are such signals. A signal that arrived after its
//! deadline is dropped, so a late approval cannot release a call.
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
//! The harness also refuses a source that has not ended. Such a source has no
//! recorded frontier to compare against.
//!
//! # Matching
//!
//! A recorded tool outcome answers a call with the same step, tool name and
//! arguments. A model gives new call ids on each call, so the key holds no id.
//!
//! A candidate call that equals the recorded call at the same turn and
//! position takes the recorded id. The ids change before the policy runs.
//! Approval signal names hold the id, so the recorded approvals stay valid. Any other candidate call gets a new id with
//! the prefix `eval_`. A recorded approval therefore never releases it.
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
//! # Runtime
//!
//! The test engine resolves activities synchronously. The model call is
//! async, so the harness blocks in place. Blocking in place needs a
//! multi-thread Tokio runtime.

use std::collections::{HashMap, HashSet, VecDeque};
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

use crate::harness::AgentHarness;
use crate::message::{ContentPart, StopReason, ToolCall};
use crate::policy::ToolDecision;
use crate::types::{
    AgentReport, AgentTask, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome,
};
use crate::workflow::{
    WORKFLOW_NAME, agent_deliver_info, agent_loop_info, agent_memory_snapshot_info,
    agent_model_turn_info, agent_tool_call_info,
};

/// The error text of the stub outcome. A candidate call with no recorded
/// outcome gets it.
pub const NOT_RUN: &str =
    "not run: the evaluation has no recorded output for this call, and it runs no tool";

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
        }
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
    /// The calls and decisions agree, but the stop reasons differ.
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "end", content = "detail")]
#[non_exhaustive]
pub enum RunEnd {
    /// The run completed with this report.
    Completed(AgentReport),
    /// The run failed, or the engine cancelled it or timed it out. The text
    /// says why.
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
        turn: 0,
        max_turns,
        cap_reached: false,
        tools: recording.tools.clone(),
        snapshots: recording.snapshots.iter().cloned().collect(),
        replayed: 0,
        stubbed: 0,
    }));

    let model = {
        let session = Arc::clone(&session);
        let harness = Arc::clone(&candidate.harness);
        let run_id = recording.run_id.clone();
        let retry = agent_model_turn_info()
            .default_retry_policy
            .unwrap_or_default();
        move |input: Value| -> Result<Value, String> {
            let mut request: ModelTurnRequest = decode(input)?;
            // The test engine has its own ids. The policy sees the recorded
            // run id instead.
            if let Some(run_id) = &run_id {
                request.run_id.clone_from(run_id);
            }
            let (index, recorded) = lock(&session).next_turn()?;
            let turn = live_turn(&handle, &harness, &request, &recorded, index, &retry)?;
            encode(&turn)
        }
    };
    let tool = {
        let session = Arc::clone(&session);
        move |input: Value| -> Result<Value, String> {
            let request: ToolCallRequest = decode(input)?;
            let outcome = lock(&session).answer(&request);
            encode(&outcome)
        }
    };
    let snapshot = {
        let session = Arc::clone(&session);
        move |_scope: Value| -> Result<Value, String> {
            let snapshot = lock(&session).snapshots.pop_front().unwrap_or_default();
            encode(&snapshot)
        }
    };

    let mut env = WorkflowTestEnv::new()
        .with_workflow_name(WORKFLOW_NAME)
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

/// What the source run recorded.
struct Recording {
    task: AgentTask,
    /// The run id of the first recorded model request.
    run_id: Option<String>,
    turns: Vec<ModelTurn>,
    tools: Vec<RecordedTool>,
    snapshots: Vec<String>,
    /// Signals that the source received in time, in order.
    signals: Vec<(String, Value)>,
    end: RunEnd,
}

/// One recorded tool call and its outcome.
#[derive(Clone)]
struct RecordedTool {
    step: u32,
    name: String,
    arguments: Value,
    outcome: ToolOutcome,
    /// A recorded outcome answers one candidate call at most.
    used: bool,
}

impl Recording {
    fn read(history: &[WorkflowEvent]) -> Result<Self, EvalError> {
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
        let task: AgentTask = serde_json::from_value(input.clone())
            .map_err(|e| EvalError::NotAnAgentRun(format!("the input is not an AgentTask: {e}")))?;

        let model_name = agent_model_turn_info().name;
        let tool_name = agent_tool_call_info().name;
        let snapshot_name = agent_memory_snapshot_info().name;
        let mut scheduled: HashMap<ActivityExecId, (&str, &Value)> = HashMap::new();
        let mut run_id = None;
        let mut tools = Vec::new();
        let mut snapshots = Vec::new();
        let mut timed_out: HashSet<&str> = HashSet::new();
        let mut signals = Vec::new();
        let mut end = None;
        for event in history {
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
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output,
                } => match scheduled.get(activity_id) {
                    Some((name, input)) if *name == tool_name => {
                        let request: ToolCallRequest =
                            decode((*input).clone()).map_err(EvalError::Undecodable)?;
                        tools.push(RecordedTool {
                            step: request.step,
                            name: request.call.name,
                            arguments: request.call.arguments,
                            outcome: decode(output.clone()).map_err(EvalError::Undecodable)?,
                            used: false,
                        });
                    }
                    Some((name, _)) if *name == snapshot_name => {
                        snapshots.push(decode(output.clone()).map_err(EvalError::Undecodable)?);
                    }
                    _ => {}
                },
                WorkflowEvent::TimerFired { timer_id } => {
                    if let Some(signal) = timed_out_signal(timer_id.as_str()) {
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
                WorkflowEvent::WorkflowCompleted { output } => {
                    end = Some(RunEnd::Completed(
                        decode(output.clone()).map_err(EvalError::Undecodable)?,
                    ));
                }
                WorkflowEvent::WorkflowFailed { error, .. } => {
                    end = Some(RunEnd::Failed(error.clone()));
                }
                WorkflowEvent::WorkflowCancelled { reason } => {
                    end = Some(RunEnd::Failed(format!("cancelled: {reason}")));
                }
                WorkflowEvent::WorkflowExecutionTimedOut { .. } => {
                    end = Some(RunEnd::Failed("timed out".to_owned()));
                }
                _ => {}
            }
        }
        Ok(Self {
            task,
            run_id,
            turns: model_turns(history)?,
            tools,
            snapshots,
            signals,
            end: end.ok_or(EvalError::InFlightSource)?,
        })
    }
}

/// The mutable state that the activity mocks share.
struct Session {
    /// The tool calls of each recorded turn, for id alignment.
    recorded_calls: Vec<Vec<ToolCall>>,
    /// The index of the next candidate turn.
    turn: usize,
    max_turns: usize,
    cap_reached: bool,
    tools: Vec<RecordedTool>,
    snapshots: VecDeque<String>,
    replayed: u32,
    stubbed: u32,
}

impl Session {
    /// Claim the next candidate turn. Returns its index and the recorded
    /// calls of the same turn.
    ///
    /// # Errors
    ///
    /// Returns [`TURN_CAP_ERROR`] when the turn cap is reached.
    fn next_turn(&mut self) -> Result<(usize, Vec<ToolCall>), String> {
        if self.turn >= self.max_turns {
            self.cap_reached = true;
            return Err(TURN_CAP_ERROR.to_owned());
        }
        let index = self.turn;
        self.turn += 1;
        let recorded = self.recorded_calls.get(index).cloned().unwrap_or_default();
        Ok((index, recorded))
    }

    /// The recorded outcome of the same call, or the stub outcome.
    fn answer(&mut self, request: &ToolCallRequest) -> ToolOutcome {
        let recorded = self.tools.iter_mut().find(|tool| {
            !tool.used
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
            ToolOutcome::error(NOT_RUN)
        }
    }
}

/// Align the call ids in the content of candidate turn `index` with the
/// recorded turn.
///
/// A call that equals the recorded call at the same position takes the
/// recorded id. Any other call gets a new `eval_` id, so no recorded approval
/// name can match it.
fn align_call_ids(content: &mut [ContentPart], recorded: &[ToolCall], index: usize) {
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
            _ => *id = format!("{EVAL_ID_PREFIX}{index}_{position}_{id}"),
        }
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
    recorded: &[ToolCall],
    index: usize,
    retry: &RetryPolicy,
) -> Result<ModelTurn, String> {
    let mut attempt = 1;
    loop {
        let align = |content: &mut Vec<ContentPart>| align_call_ids(content, recorded, index);
        let result = tokio::task::block_in_place(|| {
            handle.block_on(harness.model_turn_with(request.clone(), align))
        });
        let payload = match result {
            Ok(turn) => return Ok(turn),
            Err(payload) => payload,
        };
        let delay = retry_delay(&payload, retry, attempt).ok_or(payload)?;
        tokio::task::block_in_place(|| handle.block_on(tokio::time::sleep(delay)));
        attempt += 1;
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
    } else if recorded.stop != candidate.stop {
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
fn timed_out_signal(timer_id: &str) -> Option<&str> {
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
            timed_out_signal("__signal_timeout:3:tool_approval:0:0:w1"),
            Some("tool_approval:0:0:w1")
        );
        assert_eq!(timed_out_signal("agent_followup:0"), None);
        assert_eq!(timed_out_signal("__signal_timeout:x:name"), None);
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
        align_call_ids(&mut turn.content, &recorded, 4);
        let ids: Vec<String> = turn.calls().into_iter().map(|call| call.id).collect();
        assert_eq!(ids, vec!["r1".to_owned(), "eval_4_1_c2".to_owned()]);
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
