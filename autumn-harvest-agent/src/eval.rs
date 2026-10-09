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
//! - `agent_model_turn` asks the candidate model. This is the only live call.
//! - `agent_tool_call` returns the recorded outcome of the same call. A call
//!   with no recorded outcome gets an error stub. No tool runs.
//! - `agent_memory_snapshot` returns the recorded snapshot, or an empty one.
//! - `agent_deliver` is a stub. No report leaves the harness.
//! - Any other activity has no mock, so it fails the candidate run.
//!
//! The harness sends each approval again that the source received in time.
//! It drops an approval that arrived after its deadline, so that approval
//! cannot release a call.
//!
//! # The fork rules
//!
//! An evaluation is an in-memory fork at the first event. It follows the fork
//! rules of issue #2000:
//!
//! - The source stays unchanged. The harness reads a borrowed slice.
//! - A completed source is accepted.
//! - Effects are recorded or stubbed. There is no opt-in for live effects.
//! - An erased source is always refused.
//!
//! # Matching
//!
//! A recorded tool outcome answers a call with the same step, tool name and
//! arguments. Each model call gives new call ids, so the key has no id.
//!
//! A candidate call that equals the recorded call at the same turn and
//! position takes the recorded id. Approval signal names hold the id, so the
//! recorded approvals stay valid.
//!
//! # The diff
//!
//! The harness compares the decisions of each model turn: the tool calls, their
//! arguments, the policy decisions and the stop reason. Two final answers with
//! other words are [`Verdict::Reworded`], not a divergence.
//!
//! The replay debugger (#949) compares commands. A new prompt changes every
//! request, so a command diff always stops at the first model turn. This diff
//! compares the decisions instead.
//!
//! # Runtime
//!
//! The test engine resolves activities synchronously. The model call is
//! async, so the harness blocks in place. That needs a multi-thread Tokio
//! runtime.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use autumn_harvest::erase::is_erasure_tombstone;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::types::ActivityExecId;
use serde::Serialize;
use serde::de::DeserializeOwned;
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

/// The answer to a candidate call that has no recorded outcome.
pub const NOT_RUN: &str =
    "not run: the evaluation has no recorded output for this call, and it runs no tool";

/// The prefix of the deadline timer of a signal wait.
///
/// The engine names the timer `__signal_timeout:{seq}:{signal}`. The test
/// `a_late_approval_is_not_delivered_and_new_ids_take_the_recorded_ids`
/// fails when the name changes.
const SIGNAL_TIMEOUT_PREFIX: &str = "__signal_timeout:";

/// The candidate: a harness with a live model, and an optional new prompt.
///
/// The harness supplies the model, the temperature, the tool definitions and
/// the policy. Its tools never run.
#[derive(Debug, Clone)]
pub struct Candidate {
    harness: Arc<AgentHarness>,
    system: Option<String>,
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
        }
    }

    /// Replace the system prompt of the recorded task.
    #[must_use]
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system = Some(prompt.into());
        self
    }
}

/// Why an evaluation cannot start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EvalError {
    /// The history is not an `agent_loop` run.
    #[error("the history is not an agent run: {0}")]
    NotAnAgentRun(String),
    /// The history holds an erasure tombstone. An evaluation never forks an
    /// erased source.
    #[error("the source run was erased, so it cannot be evaluated")]
    ErasedSource,
    /// A recorded payload does not decode. A history with payload-store
    /// references or encrypted payloads needs decoding before evaluation.
    #[error("a recorded payload does not decode: {0}")]
    Undecodable(String),
    /// The current runtime cannot block in place.
    #[error("evaluation needs a multi-thread Tokio runtime")]
    MultiThreadRuntimeRequired,
}

/// One side of a comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The recorded run.
    Recorded,
    /// The candidate run.
    Candidate,
}

/// How two decisions of one turn differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The two decisions agree.
    Same,
    /// Both sides answer with the same stop reason, in other words.
    Reworded,
    /// The decisions differ.
    Diverged(Divergence),
}

/// One tool call of a turn and its policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CallDecision {
    /// The tool name.
    pub name: String,
    /// The tool arguments.
    pub arguments: Value,
    /// The policy decision.
    pub decision: ToolDecision,
}

/// The decision of one model turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TurnDiff {
    /// The turn index, from 0, across every segment of the run.
    pub turn: usize,
    /// The recorded decision.
    pub recorded: Option<TurnDecision>,
    /// The candidate decision.
    pub candidate: Option<TurnDecision>,
    /// The verdict.
    pub verdict: Verdict,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "end", content = "detail")]
pub enum RunEnd {
    /// The run completed with this report.
    Completed(AgentReport),
    /// The run failed, was cancelled or timed out. The text says why.
    Failed(String),
    /// The history ends before the run does.
    InFlight,
}

/// The result of one evaluation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Evaluation {
    /// One comparison per model turn.
    pub turns: Vec<TurnDiff>,
    /// The index of the first divergent turn.
    pub first_divergence: Option<usize>,
    /// How the recorded run ended.
    pub recorded: RunEnd,
    /// How the candidate run ended.
    pub candidate: RunEnd,
    /// Candidate tool calls that got a recorded output.
    pub replayed_tool_calls: u32,
    /// Candidate tool calls that got an error stub.
    pub stubbed_tool_calls: u32,
}

impl Evaluation {
    /// `true` when a turn diverges.
    #[must_use]
    pub const fn diverged(&self) -> bool {
        self.first_divergence.is_some()
    }
}

/// Evaluate `candidate` against the recorded `history`.
///
/// The candidate model answers every model turn live. Every other effect is
/// recorded or stubbed. See the [module documentation](self).
///
/// A failure of the candidate model is not an error. It shows as
/// [`RunEnd::Failed`] in [`Evaluation::candidate`].
///
/// # Errors
///
/// Returns an [`EvalError`] when the evaluation cannot start: the history is
/// not an agent run, it was erased, a payload does not decode, or the runtime
/// is current-thread. No model call runs in these cases.
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

    let session = Arc::new(Mutex::new(Session {
        recorded_calls: recording.turns.iter().map(ModelTurn::calls).collect(),
        turn: 0,
        tools: recording.tools.clone(),
        snapshots: recording.snapshots.iter().cloned().collect(),
        replayed: 0,
        stubbed: 0,
    }));

    let model = {
        let session = Arc::clone(&session);
        let harness = Arc::clone(&candidate.harness);
        move |input: Value| -> Result<Value, String> {
            let request: ModelTurnRequest = decode(input)?;
            let mut turn =
                tokio::task::block_in_place(|| handle.block_on(harness.model_turn(request)))?;
            let recorded = {
                let mut session = lock(&session);
                let recorded = session.recorded_calls.get(session.turn).cloned();
                session.turn += 1;
                recorded
            };
            if let Some(recorded) = recorded {
                adopt_recorded_ids(&mut turn, &recorded);
            }
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
    let (replayed, stubbed) = {
        let session = lock(&session);
        (session.replayed, session.stubbed)
    };

    let turns = diff_turns(&recording.turns, &candidate_turns);
    let first_divergence = turns
        .iter()
        .position(|turn| matches!(turn.verdict, Verdict::Diverged(_)));
    Ok(Evaluation {
        turns,
        first_divergence,
        recorded: recording.end,
        candidate: candidate_end,
        replayed_tool_calls: replayed,
        stubbed_tool_calls: stubbed,
    })
}

/// What the source run recorded.
struct Recording {
    task: AgentTask,
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

        let tool_name = agent_tool_call_info().name;
        let snapshot_name = agent_memory_snapshot_info().name;
        let mut scheduled: HashMap<ActivityExecId, (&str, &Value)> = HashMap::new();
        let mut tools = Vec::new();
        let mut snapshots = Vec::new();
        let mut timed_out: HashSet<&str> = HashSet::new();
        let mut signals = Vec::new();
        let mut end = RunEnd::InFlight;
        for event in history {
            match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name,
                    input,
                    ..
                } => {
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
                    end =
                        RunEnd::Completed(decode(output.clone()).map_err(EvalError::Undecodable)?);
                }
                WorkflowEvent::WorkflowFailed { error, .. } => {
                    end = RunEnd::Failed(error.clone());
                }
                WorkflowEvent::WorkflowCancelled { reason } => {
                    end = RunEnd::Failed(format!("cancelled: {reason}"));
                }
                WorkflowEvent::WorkflowExecutionTimedOut { .. } => {
                    end = RunEnd::Failed("timed out".to_owned());
                }
                _ => {}
            }
        }
        Ok(Self {
            task,
            turns: model_turns(history)?,
            tools,
            snapshots,
            signals,
            end,
        })
    }
}

/// The mutable state that the activity mocks share.
struct Session {
    /// The tool calls of each recorded turn, for id adoption.
    recorded_calls: Vec<Vec<ToolCall>>,
    /// The index of the next candidate turn.
    turn: usize,
    tools: Vec<RecordedTool>,
    snapshots: VecDeque<String>,
    replayed: u32,
    stubbed: u32,
}

impl Session {
    /// The recorded outcome of the same call, or an error stub.
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

/// Give each candidate call the id of the recorded call at the same position,
/// when the two calls have the same name and arguments.
fn adopt_recorded_ids(turn: &mut ModelTurn, recorded: &[ToolCall]) {
    let calls = turn.content.iter_mut().filter_map(|part| match part {
        ContentPart::ToolCall {
            id,
            name,
            arguments,
        } => Some((id, name, arguments)),
        _ => None,
    });
    for ((id, name, arguments), original) in calls.zip(recorded) {
        if *name == original.name && *arguments == original.arguments {
            id.clone_from(&original.id);
        }
    }
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
        let recorded = vec![
            ToolCall {
                id: "r1".into(),
                name: "a".into(),
                arguments: json!(1),
            },
            ToolCall {
                id: "r2".into(),
                name: "b".into(),
                arguments: json!(2),
            },
        ];
        let mut turn = ModelTurn {
            content: vec![
                ContentPart::ToolCall {
                    id: "c1".into(),
                    name: "a".into(),
                    arguments: json!(1),
                },
                ContentPart::ToolCall {
                    id: "c2".into(),
                    name: "b".into(),
                    arguments: json!(3),
                },
            ],
            stop: StopReason::ToolUse,
            usage: crate::message::TokenUsage::default(),
            decisions: Vec::new(),
        };
        adopt_recorded_ids(&mut turn, &recorded);
        let ids: Vec<String> = turn.calls().into_iter().map(|call| call.id).collect();
        assert_eq!(ids, vec!["r1".to_owned(), "c2".to_owned()]);
    }
}
