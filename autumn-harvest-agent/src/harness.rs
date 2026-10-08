//! The activity bodies: one model call and one tool call.
//!
//! [`AgentHarness`] holds the plugin-agent parts that do real work: the
//! [`LlmClient`], the [`Tool`]s, and the [`ToolPolicy`]. The engine runs its
//! two methods as activities, so each result is recorded once and replayed
//! after that.

use std::sync::Arc;

use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_plugin_agent::hooks::RunInfo;
use autumn_plugin_agent::policy::AllowAll;
use autumn_plugin_agent::{
    AgentError, ChatRequest, ContentPart, ErrorKind, LlmClient, RunId, Tool, ToolCall, ToolContext,
    ToolPolicy,
};

use crate::types::{GatedCall, ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome};

/// The default cap on one tool result, in characters. It matches the
/// plugin-agent loop.
pub const DEFAULT_TOOL_OUTPUT_LIMIT: usize = 8_000;

/// The model, the tools, and the policy behind the two agent activities.
///
/// Install it once per worker. On the Postgres engine, pass it to
/// `HarvestBuilder::state`. On SQLite, pass it to `sqlite::register`.
#[derive(Debug, Clone)]
pub struct AgentHarness {
    client: Arc<dyn LlmClient>,
    tools: Vec<Arc<dyn Tool>>,
    policy: Arc<dyn ToolPolicy>,
    temperature: Option<f32>,
    tool_output_limit: usize,
}

impl AgentHarness {
    /// A harness with no tools and the [`AllowAll`] policy.
    #[must_use]
    pub fn new(client: Arc<dyn LlmClient>) -> Self {
        Self {
            client,
            tools: Vec::new(),
            policy: Arc::new(AllowAll),
            temperature: None,
            tool_output_limit: DEFAULT_TOOL_OUTPUT_LIMIT,
        }
    }

    /// Add one tool.
    #[must_use]
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Add several tools.
    #[must_use]
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }

    /// Set the policy that decides on each tool call.
    #[must_use]
    pub fn policy(mut self, policy: Arc<dyn ToolPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Set the sampling temperature of every model call.
    #[must_use]
    pub const fn temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the cap on one tool result, in characters.
    #[must_use]
    pub const fn tool_output_limit(mut self, limit: usize) -> Self {
        self.tool_output_limit = limit;
        self
    }

    /// Run one model call, then ask the policy about each tool call.
    ///
    /// The decisions are part of the result. Replay reads them back, so a
    /// policy that reads changing state cannot change a recorded run.
    ///
    /// # Errors
    ///
    /// Returns an activity error payload. A rate limit or a transport fault is
    /// retryable. Every other provider failure is not, because a retry cannot
    /// fix it.
    pub async fn model_turn(&self, request: ModelTurnRequest) -> Result<ModelTurn, String> {
        let chat = ChatRequest {
            messages: request.messages,
            tools: self.tools.iter().map(|tool| tool.definition()).collect(),
            max_tokens: request.max_output_tokens,
            temperature: self.temperature,
        };
        let response = self
            .client
            .chat(&chat)
            .await
            .map_err(|err| model_failure(&err))?;
        let info = RunInfo {
            run_id: RunId::new(request.run_id),
            session_id: request.session_id,
            steps_used: request.steps_used,
            max_steps: request.max_steps,
            usage: request.usage.saturating_add(response.usage),
        };
        let mut calls = Vec::new();
        for part in &response.content {
            if let ContentPart::ToolCall {
                id,
                name,
                arguments,
            } = part
            {
                let call = ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                };
                let decision = self.policy.decide(&call, self.find(name), &info).await;
                calls.push(GatedCall { call, decision });
            }
        }
        Ok(ModelTurn {
            content: response.content,
            stop: response.stop_reason.into(),
            usage: response.usage,
            calls,
        })
    }

    /// Run one tool call.
    ///
    /// A tool failure or an unknown tool name is a result the model reads,
    /// not an activity error. The run keeps going.
    ///
    /// # Errors
    ///
    /// This method returns `Ok` for every tool outcome. The `Result` matches
    /// the activity signature.
    pub async fn tool_call(&self, request: ToolCallRequest) -> Result<ToolOutcome, String> {
        let call = request.call;
        let Some(tool) = self.find(&call.name) else {
            return Ok(ToolOutcome::error(&format!("unknown tool {:?}", call.name)));
        };
        let ctx = ToolContext {
            run_id: RunId::new(request.run_id),
            call_id: call.id,
            session_id: request.session_id,
            step: request.step,
        };
        Ok(match tool.execute(call.arguments, &ctx).await {
            Ok(output) => ToolOutcome::ok(fit_result(truncate(
                &output.to_string(),
                self.tool_output_limit,
            ))),
            Err(err) => {
                tracing::warn!(tool = %call.name, error = %err, "agent tool call failed");
                ToolOutcome::error(err.message())
            }
        })
    }

    fn find(&self, name: &str) -> Option<&dyn Tool> {
        self.tools
            .iter()
            .find(|tool| tool.name() == name)
            .map(AsRef::as_ref)
    }
}

/// Map a provider failure to an activity error payload.
///
/// Only a rate limit and a transport fault produced no answer that a retry
/// cannot fix. Everything else fails at once.
fn model_failure(err: &AgentError) -> String {
    let kind = format!("{:?}", err.kind());
    let message = err.to_string();
    let failure = match err.kind() {
        ErrorKind::RateLimited | ErrorKind::Transport => ActivityFailure::retryable(kind, message),
        _ => ActivityFailure::non_retryable(kind, message),
    };
    failure.into_error_payload()
}

/// The most bytes of tool output one result keeps.
///
/// The engine refuses an activity result over
/// [`DEFAULT_MAX_ACTIVITY_RESULT_BYTES`]. That refusal fails the run, so a
/// large result must be cut before it is recorded. JSON escapes one byte into
/// at most six (`\u0000`), so a sixth of the cap always fits.
#[allow(
    clippy::cast_possible_truncation,
    reason = "a sixth of 2 MiB fits every usize"
)]
pub const MAX_TOOL_RESULT_BYTES: usize = (DEFAULT_MAX_ACTIVITY_RESULT_BYTES / 6) as usize;

/// Cut `text` so that its recorded outcome fits the result cap.
fn fit_result(text: String) -> String {
    if text.len() <= MAX_TOOL_RESULT_BYTES {
        return text;
    }
    let mut end = MAX_TOOL_RESULT_BYTES - TRUNCATED.len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATED}", &text[..end])
}

/// The mark at the end of a cut result.
const TRUNCATED: &str = "…[truncated]";

/// Cut `text` to `limit` characters and mark the cut.
fn truncate(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        None => text.to_owned(),
        Some((end, _)) => format!("{}{TRUNCATED}", &text[..end]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_kinds_are_marked_retryable() {
        for (kind, retryable) in [
            (ErrorKind::RateLimited, true),
            (ErrorKind::Transport, true),
            (ErrorKind::Authentication, false),
            (ErrorKind::Provider, false),
            (ErrorKind::Config, false),
            (ErrorKind::Decode, false),
            (ErrorKind::Budget, false),
        ] {
            let payload = model_failure(&AgentError::new(kind, "boom"));
            let failure = autumn_harvest::failure::parse_typed_payload(&payload).unwrap();
            assert_eq!(failure.non_retryable, !retryable, "{kind:?}");
            assert!(failure.message.contains("boom"));
        }
    }

    #[test]
    fn a_result_of_any_content_fits_the_result_cap() {
        for unit in ["x", "\u{0}", "é", "\""] {
            let text = unit.repeat(usize::try_from(DEFAULT_MAX_ACTIVITY_RESULT_BYTES).unwrap());
            let outcome = ToolOutcome::ok(fit_result(text));
            assert!(
                crate::bounds::json_len(&outcome) <= DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                "{unit:?}"
            );
            assert!(outcome.content.ends_with(TRUNCATED));
        }
        assert_eq!(fit_result("short".into()), "short");
    }

    #[test]
    fn truncate_cuts_on_a_char_boundary() {
        assert_eq!(truncate("abc", 3), "abc");
        assert_eq!(truncate("ééé", 2), "éé…[truncated]");
        assert_eq!(truncate("", 0), "");
    }
}
