//! Replay-as-evaluation (issue #2001).

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use serde::Serialize;
use serde_json::Value;

use crate::harness::AgentHarness;
use crate::message::StopReason;
use crate::policy::ToolDecision;
use crate::types::AgentReport;

/// The candidate: a harness with a live model, and an optional new prompt.
#[derive(Debug, Clone)]
pub struct Candidate {
    harness: Arc<AgentHarness>,
    system: Option<String>,
}

impl Candidate {
    /// A candidate that uses `harness` for every model turn.
    #[must_use]
    pub fn new(harness: AgentHarness) -> Self {
        Self {
            harness: Arc::new(harness),
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
    /// The history holds an erasure tombstone.
    #[error("the source run was erased")]
    ErasedSource,
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
    /// Both sides answer, but the stop reasons differ.
    Stop,
    /// Only the other side has this turn.
    Missing(Side),
}

/// The verdict on one turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The two decisions agree.
    Same,
    /// Both sides answer with the same stop reason in other words.
    Reworded,
    /// The decisions differ.
    Diverged(Divergence),
}

/// One tool call of a turn and its policy decision.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CallDecision {
    /// The tool name.
    pub name: String,
    /// The tool arguments.
    pub arguments: Value,
    /// The policy decision.
    pub decision: ToolDecision,
}

/// The decision of one model turn.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnDecision {
    /// The tool calls, in order.
    pub calls: Vec<CallDecision>,
    /// The text of the turn.
    pub text: String,
    /// Why the model stopped.
    pub stop: StopReason,
}

/// The comparison of one turn.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TurnDiff {
    /// The turn index, from 0.
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
    /// The run failed with this error.
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
/// # Errors
///
/// Returns an [`EvalError`] when the evaluation cannot start.
#[allow(clippy::unused_async)]
pub async fn evaluate(
    history: &[WorkflowEvent],
    candidate: &Candidate,
) -> Result<Evaluation, EvalError> {
    let _ = (history, candidate);
    Err(EvalError::NotAnAgentRun("unimplemented".to_owned()))
}
