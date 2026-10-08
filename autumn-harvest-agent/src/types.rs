//! The payloads the loop records in history.
//!
//! This crate owns these types. Each one holds only plugin-agent types that
//! the agent crate already persists: `ChatMessage`, `ContentPart`, `ToolCall`,
//! `TokenUsage`, `ToolDecision` and `SessionId`. A change to a plugin-agent
//! type with no serde contract therefore cannot alter a stored payload.
//!
//! Replay reads these payloads back. Add a field only with `#[serde(default)]`,
//! so that a history written before the field still decodes.

use std::time::Duration;

use autumn_plugin_agent::{
    ChatMessage, ChatRole, ContentPart, SessionId, StopReason, TokenUsage, ToolCall, ToolDecision,
};
use serde::{Deserialize, Serialize};

/// The default bound on tool rounds.
pub const DEFAULT_MAX_STEPS: u32 = 8;

/// The default time a gated call waits for a decision: one hour.
pub const DEFAULT_APPROVAL_TIMEOUT_SECS: u64 = 3_600;

/// The input of one agent run: the workflow input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTask {
    /// The user message that starts this run.
    pub input: String,
    /// The system prompt, if any.
    #[serde(default)]
    pub system: Option<String>,
    /// Earlier turns to continue, without the system prompt.
    #[serde(default)]
    pub history: Vec<ChatMessage>,
    /// The session this run belongs to. Tools and the policy see it.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// The bound on tool rounds. The model may answer once more after it.
    pub max_steps: u32,
    /// The bound on provider-reported tokens for the whole run.
    #[serde(default)]
    pub max_total_tokens: Option<u32>,
    /// The output cap of one model call.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    /// How long a gated call waits for a decision before it is denied.
    pub approval_timeout_secs: u64,
    /// The activity-input cap of the workers, in bytes. Set it when the
    /// workers use a cap other than the engine default. The loop stops with
    /// `TranscriptFull` before it sends a request over this size.
    #[serde(default)]
    pub max_request_bytes: Option<u64>,
}

impl AgentTask {
    /// A task with the default bounds.
    #[must_use]
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
            system: None,
            history: Vec::new(),
            session_id: None,
            max_steps: DEFAULT_MAX_STEPS,
            max_total_tokens: None,
            max_output_tokens: None,
            approval_timeout_secs: DEFAULT_APPROVAL_TIMEOUT_SECS,
            max_request_bytes: None,
        }
    }

    /// Set the system prompt.
    #[must_use]
    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Continue from earlier turns.
    #[must_use]
    pub fn history(mut self, history: Vec<ChatMessage>) -> Self {
        self.history = history;
        self
    }

    /// Bind the run to a session.
    #[must_use]
    pub fn session(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Set the bound on tool rounds.
    #[must_use]
    pub const fn max_steps(mut self, max_steps: u32) -> Self {
        self.max_steps = max_steps;
        self
    }

    /// Set the bound on provider-reported tokens for the run.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }

    /// Set the output cap of one model call.
    #[must_use]
    pub const fn max_output_tokens(mut self, max: u32) -> Self {
        self.max_output_tokens = Some(max);
        self
    }

    /// Set how long a gated call waits for a decision.
    ///
    /// The value rounds up to whole seconds, so a short wait never becomes
    /// an immediate deny.
    #[must_use]
    pub const fn approval_timeout(mut self, timeout: Duration) -> Self {
        let partial = if timeout.subsec_nanos() > 0 { 1 } else { 0 };
        self.approval_timeout_secs = timeout.as_secs().saturating_add(partial);
        self
    }

    /// Set the activity-input cap of the workers, in bytes.
    #[must_use]
    pub const fn max_request_bytes(mut self, bytes: u64) -> Self {
        self.max_request_bytes = Some(bytes);
        self
    }

    /// The request cap the loop enforces.
    #[must_use]
    pub fn request_cap(&self) -> u64 {
        self.max_request_bytes
            .unwrap_or(autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES)
    }
}

/// The input of one model-turn activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelTurnRequest {
    /// The workflow id. Tools and the policy see it as the run id.
    pub run_id: String,
    /// The session the run belongs to.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// Tool rounds used before this turn.
    pub steps_used: u32,
    /// The run's bound on tool rounds.
    pub max_steps: u32,
    /// Tokens spent before this turn.
    pub usage: TokenUsage,
    /// The whole conversation so far, the system prompt first.
    pub messages: Vec<ChatMessage>,
    /// The output cap of this call.
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
}

/// Why the model stopped one turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStop {
    /// The model finished its turn.
    EndTurn,
    /// The model asked for tool calls.
    ToolUse,
    /// The turn hit the output cap.
    MaxTokens,
    /// The provider sent a reason the client does not know.
    Unknown,
}

impl From<StopReason> for TurnStop {
    fn from(reason: StopReason) -> Self {
        match reason {
            StopReason::EndTurn => Self::EndTurn,
            StopReason::ToolUse => Self::ToolUse,
            StopReason::MaxTokens => Self::MaxTokens,
            StopReason::Unknown => Self::Unknown,
        }
    }
}

/// The recorded result of one model-turn activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelTurn {
    /// The assistant content, as the client returned it.
    pub content: Vec<ContentPart>,
    /// Why the model stopped.
    pub stop: TurnStop,
    /// Provider-reported tokens for this call.
    pub usage: TokenUsage,
    /// The policy decision for each tool call of [`content`](Self::content),
    /// in order. Replay reads it and does not ask the policy again.
    ///
    /// The calls themselves are read from `content`, so the arguments are
    /// recorded once.
    pub decisions: Vec<ToolDecision>,
}

impl ModelTurn {
    /// The tool calls of the turn, in order.
    #[must_use]
    pub fn calls(&self) -> Vec<ToolCall> {
        self.content
            .iter()
            .filter_map(|part| match part {
                ContentPart::ToolCall {
                    id,
                    name,
                    arguments,
                } => Some(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                }),
                ContentPart::Text(_) | ContentPart::ToolResult { .. } => None,
            })
            .collect()
    }

    /// The text parts of the turn, joined.
    #[must_use]
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text(text) => Some(text.as_str()),
                ContentPart::ToolCall { .. } | ContentPart::ToolResult { .. } => None,
            })
            .collect()
    }
}

/// The input of one tool-call activity.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRequest {
    /// The workflow id.
    pub run_id: String,
    /// The session the run belongs to.
    #[serde(default)]
    pub session_id: Option<SessionId>,
    /// The tool round (0-based) that issued the call.
    pub step: u32,
    /// The call to run, with the arguments a reviewer may have edited.
    pub call: ToolCall,
}

/// The recorded result of one tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOutcome {
    /// The text the model receives as the tool result.
    pub content: String,
    /// `true` when the call failed, or was denied, rejected, or not decided.
    pub is_error: bool,
}

impl ToolOutcome {
    /// A successful result.
    #[must_use]
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
        }
    }

    /// An error result. The model reads the message and can choose again.
    #[must_use]
    pub fn error(message: &str) -> Self {
        Self {
            content: serde_json::json!({ "error": message }).to_string(),
            is_error: true,
        }
    }
}

/// Why a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStop {
    /// The model gave a final answer.
    Completed,
    /// The last turn hit the output cap. The answer is cut short.
    OutputCapped,
    /// The model asked for tools after the last permitted tool round.
    StepsExhausted,
    /// The run spent more tokens than `max_total_tokens`.
    TokensExhausted,
    /// The next request was too large to record. It was not sent.
    TranscriptFull,
}

/// The workflow output: how the run ended and what it produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentReport {
    /// Why the run ended.
    pub stop: AgentStop,
    /// The last text the model produced.
    pub text: String,
    /// Tool rounds used.
    pub steps_used: u32,
    /// Tool calls answered, whether they ran or not. A round cut at the
    /// request cap answers its remaining calls with an error.
    pub tool_calls: u32,
    /// Provider-reported tokens for the whole run.
    pub usage: TokenUsage,
    /// The transcript without the system prompt. Pass it as the history of the
    /// next run to continue the conversation.
    pub messages: Vec<ChatMessage>,
}

/// The transcript without system messages.
pub(crate) fn without_system(messages: &[ChatMessage]) -> Vec<ChatMessage> {
    messages
        .iter()
        .filter(|message| message.role != ChatRole::System)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_task_without_optional_fields_decodes() {
        let task: AgentTask = serde_json::from_value(json!({
            "input": "hi",
            "max_steps": 2,
            "approval_timeout_secs": 5,
        }))
        .unwrap();
        assert_eq!(task.input, "hi");
        assert_eq!(task.history, Vec::new());
        assert_eq!(task.max_total_tokens, None);
    }

    #[test]
    fn the_builder_sets_every_bound() {
        let task = AgentTask::new("go")
            .system("s")
            .session(SessionId::new("s1"))
            .max_steps(3)
            .max_total_tokens(10)
            .max_output_tokens(5)
            .approval_timeout(Duration::from_secs(7));
        assert_eq!(task.system.as_deref(), Some("s"));
        assert_eq!(task.max_steps, 3);
        assert_eq!(task.max_total_tokens, Some(10));
        assert_eq!(task.max_output_tokens, Some(5));
        assert_eq!(task.approval_timeout_secs, 7);
        assert!(task.session_id.is_some());
    }

    #[test]
    fn a_short_approval_timeout_rounds_up() {
        let task = AgentTask::new("go").approval_timeout(Duration::from_millis(500));
        assert_eq!(task.approval_timeout_secs, 1);
        let task = AgentTask::new("go").approval_timeout(Duration::from_secs(2));
        assert_eq!(task.approval_timeout_secs, 2);
    }

    #[test]
    fn the_request_cap_defaults_to_the_engine_cap() {
        let task = AgentTask::new("go");
        assert_eq!(
            task.request_cap(),
            autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES
        );
        assert_eq!(task.max_request_bytes(10).request_cap(), 10);
    }

    #[test]
    fn stop_names_are_stable_in_history() {
        assert_eq!(json!(AgentStop::TranscriptFull), json!("transcript_full"));
        assert_eq!(json!(TurnStop::ToolUse), json!("tool_use"));
        assert_eq!(TurnStop::from(StopReason::MaxTokens), TurnStop::MaxTokens);
        assert_eq!(TurnStop::from(StopReason::Unknown), TurnStop::Unknown);
    }

    #[test]
    fn an_error_outcome_is_json_the_model_can_read() {
        let outcome = ToolOutcome::error("no \"quotes\" lost");
        assert!(outcome.is_error);
        let value: serde_json::Value = serde_json::from_str(&outcome.content).unwrap();
        assert_eq!(value["error"], "no \"quotes\" lost");
    }

    #[test]
    fn turn_text_joins_only_text_parts() {
        let turn = ModelTurn {
            content: vec![
                ContentPart::Text("a".into()),
                ContentPart::ToolCall {
                    id: "c".into(),
                    name: "t".into(),
                    arguments: json!({}),
                },
                ContentPart::Text("b".into()),
            ],
            stop: TurnStop::ToolUse,
            usage: TokenUsage::default(),
            decisions: vec![ToolDecision::Allow],
        };
        assert_eq!(turn.text(), "ab");
        let calls = turn.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "c");
    }
}
