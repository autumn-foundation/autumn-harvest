//! The activity bodies: one model call and one tool call.
//!
//! [`AgentHarness`] holds the parts that do real work: the
//! [`AgentModel`], the [`Tool`]s, and the [`ToolPolicy`]. The engine runs its
//! two methods as activities, so each result is recorded once and replayed
//! after that.

use std::sync::Arc;
use std::time::Duration;

use crate::error::AgentError;
use crate::message::{RunId, TokenUsage};
use crate::model::{AgentModel, ChatRequest};
use crate::policy::{AllowAll, RunInfo, ToolDecision, ToolPolicy};
use crate::tool::{Tool, ToolContext};
use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::llm_budget::LlmUsage;

use crate::types::{ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome};

/// The default cap on one tool result, in characters.
pub const DEFAULT_TOOL_OUTPUT_LIMIT: usize = 8_000;

/// The default time budget of one model call: 13 minutes.
///
/// With [`DEFAULT_POLICY_TIMEOUT`], it stays below the 15-minute
/// `start_to_close` of `agent_model_turn`. A slow call therefore ends as a
/// retryable failure that the harness reports. It does not end as an engine
/// timeout, which SQLite reports only after the body returns.
pub const DEFAULT_MODEL_TIMEOUT: Duration = Duration::from_secs(13 * 60);

/// The default time budget of one tool call: 9 minutes.
///
/// It is below the 10-minute `start_to_close` of `agent_tool_call`. A slow
/// tool therefore gives the model an error result, and the run goes on.
pub const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(9 * 60);

/// The default time budget of the policy for all tool calls of one turn: one
/// minute.
///
/// A policy can wait on I/O. Without a budget, a stalled policy would hang
/// the model turn, and SQLite cannot end the turn from outside.
pub const DEFAULT_POLICY_TIMEOUT: Duration = Duration::from_secs(60);

/// The bytes that the `ToolOutcome` JSON adds around its content.
const OUTCOME_ENVELOPE_BYTES: u64 = 64;

/// The mark at the end of a cut result.
const TRUNCATED: &str = "…[truncated]";

/// The model, the tools, and the policy behind the two agent activities.
///
/// Install it once per worker. On the Postgres engine, pass it to
/// `HarvestBuilder::state`. On SQLite, pass it to `sqlite::register`.
#[derive(Debug, Clone)]
pub struct AgentHarness {
    client: Arc<dyn AgentModel>,
    tools: Vec<Arc<dyn Tool>>,
    policy: Arc<dyn ToolPolicy>,
    temperature: Option<f32>,
    tool_output_limit: usize,
    max_result_bytes: u64,
    model_timeout: Duration,
    tool_timeout: Duration,
    policy_timeout: Duration,
}

impl AgentHarness {
    /// A harness with no tools and the [`AllowAll`] policy.
    #[must_use]
    pub fn new(client: Arc<dyn AgentModel>) -> Self {
        Self {
            client,
            tools: Vec::new(),
            policy: Arc::new(AllowAll),
            temperature: None,
            tool_output_limit: DEFAULT_TOOL_OUTPUT_LIMIT,
            max_result_bytes: DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            policy_timeout: DEFAULT_POLICY_TIMEOUT,
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

    /// Set the activity-result cap of the workers, in bytes. Set it when the
    /// workers use a cap other than the engine default.
    #[must_use]
    pub const fn max_result_bytes(mut self, bytes: u64) -> Self {
        self.max_result_bytes = bytes;
        self
    }

    /// Set the time budget of one model call.
    #[must_use]
    pub const fn model_timeout(mut self, timeout: Duration) -> Self {
        self.model_timeout = timeout;
        self
    }

    /// Set the time budget of one tool call.
    #[must_use]
    pub const fn tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = timeout;
        self
    }

    /// Set the time budget of the policy for all tool calls of one turn. A
    /// call that has no decision when the budget ends is denied.
    #[must_use]
    pub const fn policy_timeout(mut self, timeout: Duration) -> Self {
        self.policy_timeout = timeout;
        self
    }

    /// Run one model call, then ask the policy about each tool call.
    ///
    /// The decisions are part of the result. Replay reads them back, so a
    /// policy that reads changing state cannot change a recorded run.
    ///
    /// # Errors
    ///
    /// Returns an activity error payload. A failure that a retry can fix is
    /// retryable: a rate limit, a transport fault, a provider outage, or the
    /// time budget. Every other failure is not.
    pub async fn model_turn(&self, request: ModelTurnRequest) -> Result<ModelTurn, String> {
        self.timed_model_turn(request).await.map(|(turn, _)| turn)
    }

    /// [`Self::model_turn`], with the time that the model call took.
    pub(crate) async fn timed_model_turn(
        &self,
        request: ModelTurnRequest,
    ) -> Result<(ModelTurn, Duration), String> {
        let chat = ChatRequest {
            messages: request.messages,
            tools: self.tools.iter().map(|tool| tool.definition()).collect(),
            max_tokens: request.max_output_tokens,
            temperature: self.temperature,
        };
        let started = std::time::Instant::now();
        let response = match tokio::time::timeout(self.model_timeout, self.client.chat(&chat)).await
        {
            Ok(response) => response.map_err(|err| model_failure(&err))?,
            Err(_) => {
                return Err(ActivityFailure::retryable(
                    "ModelTimeout",
                    format!("the model did not answer within {:?}", self.model_timeout),
                )
                .into_error_payload());
            }
        };
        let latency = started.elapsed();
        let mut turn = ModelTurn {
            content: response.content,
            stop: response.stop_reason,
            usage: response.usage,
            decisions: Vec::new(),
        };
        let info = RunInfo {
            run_id: RunId::new(request.run_id),
            session_id: request.session_id,
            steps_used: request.steps_used,
            max_steps: request.max_steps,
            usage: request.usage.saturating_add(response.usage),
        };
        // One deadline bounds the whole decision phase. A per-call budget
        // would let many stalled calls add up past `start_to_close`.
        let deadline = tokio::time::Instant::now() + self.policy_timeout;
        for call in turn.calls() {
            let decide = self.policy.decide(&call, self.find(&call.name), &info);
            // A stalled policy denies the call. That fails closed, and the
            // model reads why.
            let decision = tokio::time::timeout_at(deadline, decide)
                .await
                .unwrap_or_else(|_| ToolDecision::Deny {
                    reason: format!("the policy did not decide within {:?}", self.policy_timeout),
                });
            turn.decisions.push(decision);
        }
        Ok((turn, latency))
    }

    /// Run one tool call.
    ///
    /// A tool failure, an unknown tool name, or a call over its time budget
    /// is a result the model reads, not an activity error. The run goes on.
    ///
    /// # Errors
    ///
    /// This method returns `Ok` for every tool outcome. The `Result` matches
    /// the activity signature.
    pub async fn tool_call(&self, request: ToolCallRequest) -> Result<ToolOutcome, String> {
        let call = request.call;
        let Some(tool) = self.find(&call.name) else {
            return Ok(self.error_outcome(&format!("unknown tool {:?}", call.name)));
        };
        let ctx = ToolContext {
            run_id: RunId::new(request.run_id),
            call_id: call.id,
            session_id: request.session_id,
            step: request.step,
        };
        let execution = tool.execute(call.arguments, &ctx);
        Ok(
            match tokio::time::timeout(self.tool_timeout, execution).await {
                Ok(Ok(output)) => ToolOutcome::ok(fit_result(
                    truncate(&output.to_string(), self.tool_output_limit),
                    self.max_result_bytes,
                )),
                Ok(Err(err)) => {
                    tracing::warn!(tool = %call.name, error = %err, "agent tool call failed");
                    self.error_outcome(err.message())
                }
                Err(_) => self.error_outcome(&format!(
                    "the tool did not finish within {:?}",
                    self.tool_timeout
                )),
            },
        )
    }

    /// An error result whose recorded outcome fits the result cap.
    ///
    /// The message is escaped twice: once into the error JSON, and once as
    /// the outcome content. One byte can grow to twelve, so the message keeps
    /// a twelfth of the cap.
    fn error_outcome(&self, message: &str) -> ToolOutcome {
        let message = truncate(message, self.tool_output_limit);
        let cap = self.max_result_bytes.saturating_sub(OUTCOME_ENVELOPE_BYTES);
        let max = usize::try_from(cap / 12).unwrap_or(usize::MAX);
        ToolOutcome::error(&cut_bytes(message, max))
    }

    /// The ledger usage of one model call (issue #1997).
    ///
    /// `input_tokens` already counts the cached prompt tokens, so the cache
    /// counts are not added again.
    pub(crate) fn ledger_usage(&self, usage: &TokenUsage, latency: Duration) -> LlmUsage {
        LlmUsage::new(
            self.client.model_id(),
            u64::from(usage.input_tokens),
            u64::from(usage.output_tokens),
        )
        .with_cost_micros(self.client.cost_micros(usage))
        .with_latency(latency)
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
/// A rate limit, a transport fault, and a provider outage produced no answer.
/// A retry can succeed, so they retry. Every other kind fails at once.
fn model_failure(err: &AgentError) -> String {
    let kind = format!("{:?}", err.kind());
    let message = err.to_string();
    let failure = if err.kind().is_retryable() {
        ActivityFailure::retryable(kind, message)
    } else {
        ActivityFailure::non_retryable(kind, message)
    };
    failure.into_error_payload()
}

/// The most bytes of tool output one result keeps under `cap`.
///
/// JSON escapes one byte into at most six (`\u0000`). A sixth of the cap,
/// less the outcome envelope, therefore always fits.
#[must_use]
pub fn max_tool_result_bytes(cap: u64) -> usize {
    usize::try_from(cap.saturating_sub(OUTCOME_ENVELOPE_BYTES) / 6).unwrap_or(usize::MAX)
}

/// Cut `text` so that its recorded outcome fits the result cap.
fn fit_result(text: String, cap: u64) -> String {
    cut_bytes(text, max_tool_result_bytes(cap))
}

/// Cut `text` to at most `max` bytes on a char boundary, and mark the cut.
fn cut_bytes(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max.saturating_sub(TRUNCATED.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATED}", &text[..end])
}

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
    use crate::bounds::json_len;
    use crate::error::ErrorKind;

    /// A model with a fixed id and a price of one micro per token.
    #[derive(Debug)]
    struct PricedModel;

    impl AgentModel for PricedModel {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> crate::model::BoxFuture<'a, Result<crate::model::ChatResponse, AgentError>> {
            Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "unused")) })
        }

        #[allow(clippy::unnecessary_literal_bound)]
        fn model_id(&self) -> &str {
            "priced-1"
        }

        fn cost_micros(&self, usage: &TokenUsage) -> u64 {
            u64::from(usage.total())
        }
    }

    #[test]
    fn the_ledger_usage_reads_the_model_id_the_cost_and_the_latency() {
        let harness = AgentHarness::new(Arc::new(PricedModel));
        let usage = TokenUsage {
            input_tokens: 30,
            output_tokens: 12,
            cache_read_tokens: 20,
            cache_write_tokens: 0,
        };
        let ledger = harness.ledger_usage(&usage, Duration::from_millis(1_500));
        assert_eq!(ledger.model, "priced-1");
        // The input count already holds the cached tokens.
        assert_eq!((ledger.input_tokens, ledger.output_tokens), (30, 12));
        assert_eq!(ledger.cost_micros, 42);
        assert_eq!(ledger.latency_ms, 1_500);
    }

    #[test]
    fn a_model_with_the_defaults_records_an_unknown_id_and_no_cost() {
        let model = PricedModelDefaults;
        assert_eq!(model.model_id(), "unknown");
        assert_eq!(model.cost_micros(&TokenUsage::new(5, 5)), 0);
    }

    #[derive(Debug)]
    struct PricedModelDefaults;

    impl AgentModel for PricedModelDefaults {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> crate::model::BoxFuture<'a, Result<crate::model::ChatResponse, AgentError>> {
            Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "unused")) })
        }
    }

    #[test]
    fn retryable_kinds_are_marked_retryable() {
        for (kind, retryable) in [
            (ErrorKind::RateLimited, true),
            (ErrorKind::Transport, true),
            (ErrorKind::Unavailable, true),
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
        let cap = DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
        let max = max_tool_result_bytes(cap);
        for unit in ["x", "\u{0}", "é", "\""] {
            // At the limit, one byte under it, and far over it.
            for len in [max - 1, max, max + 1, 2 * max] {
                let text: String = unit.repeat(len).chars().take(len).collect();
                let outcome = ToolOutcome::ok(fit_result(text, cap));
                assert!(json_len(&outcome) <= cap, "{unit:?} {len}");
            }
        }
        assert_eq!(fit_result("short".into(), cap), "short");
        assert!(fit_result("x".repeat(100), 300).ends_with(TRUNCATED));
    }

    #[test]
    fn truncate_cuts_on_a_char_boundary() {
        assert_eq!(truncate("abc", 3), "abc");
        assert_eq!(truncate("ééé", 2), "éé…[truncated]");
        assert_eq!(truncate("", 0), "");
    }
}
