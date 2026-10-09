//! The activity bodies: model calls, tool calls, and the always-on hooks.
//!
//! [`AgentHarness`] holds the parts that do real work: the
//! [`AgentModel`], the [`Tool`]s, the [`ToolPolicy`], the memory store, the
//! delivery and the precheck. The engine runs its methods as activities, so
//! each result is recorded once and replayed after that.

use std::sync::Arc;
use std::time::Duration;

use crate::delivery::{Delivery, LogDelivery, Report};
use crate::error::AgentError;
use crate::heartbeat::{HeartbeatTask, Precheck};
use crate::memory::{MEMORY_TOOL, MemoryScope, MemoryStore, MemoryTool, render_snapshot};
use crate::message::{RunId, ToolDefinition};
use crate::model::{AgentModel, BoxFuture, ChatRequest};
use crate::policy::{AllowAll, RunInfo, Strictest, ToolDecision, ToolPolicy, ToolRules};
use crate::tool::{Tool, ToolContext, ToolEffect};
use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

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

/// The default time budget of one memory read, delivery or precheck: 30
/// seconds.
///
/// It is below the one-minute `start_to_close` of the shortest of those
/// activities. SQLite cannot end a stalled call from outside.
pub const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

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
    hook_timeout: Duration,
    memory: Option<Arc<dyn MemoryStore>>,
    delivery: Arc<dyn Delivery>,
    precheck: Option<Arc<dyn Precheck>>,
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
            hook_timeout: DEFAULT_HOOK_TIMEOUT,
            memory: None,
            delivery: Arc::new(LogDelivery),
            precheck: None,
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

    /// Set the time budget of one memory read, delivery or precheck.
    #[must_use]
    pub const fn hook_timeout(mut self, timeout: Duration) -> Self {
        self.hook_timeout = timeout;
        self
    }

    /// Set the memory store. A run with a memory scope reads a snapshot from
    /// it and gets the `memory` tool.
    #[must_use]
    pub fn memory(mut self, store: Arc<dyn MemoryStore>) -> Self {
        self.memory = Some(store);
        self
    }

    /// Set where reports go. The default is [`LogDelivery`].
    #[must_use]
    pub fn delivery(mut self, delivery: Arc<dyn Delivery>) -> Self {
        self.delivery = delivery;
        self
    }

    /// Set the cheap check that can skip a heartbeat before any model call.
    #[must_use]
    pub fn precheck(mut self, precheck: Arc<dyn Precheck>) -> Self {
        self.precheck = Some(precheck);
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
        let builtins = self.builtins(
            request.memory_scope.as_ref(),
            request.extra_tools.iter().cloned(),
        );
        let tools: Vec<_> = self.visible(&builtins).map(Tool::definition).collect();
        let chat = ChatRequest {
            messages: request.messages,
            tools,
            max_tokens: request.max_output_tokens,
            temperature: self.temperature,
        };
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
        let read_only: Arc<dyn ToolPolicy>;
        let policy: &dyn ToolPolicy = if request.read_only {
            // The read-only rules go first. Their deny ends the decision, so
            // the app policy is not asked about a call that cannot run.
            read_only = Arc::new(Strictest::new(vec![
                Arc::new(ToolRules::read_only()),
                Arc::clone(&self.policy),
            ]));
            read_only.as_ref()
        } else {
            self.policy.as_ref()
        };
        for call in turn.calls() {
            let tool = self
                .visible(&builtins)
                .find(|tool| tool.name() == call.name);
            let decide = policy.decide(&call, tool, &info);
            // A stalled policy denies the call. That fails closed, and the
            // model reads why.
            let decision = tokio::time::timeout_at(deadline, decide)
                .await
                .unwrap_or_else(|_| ToolDecision::Deny {
                    reason: format!("the policy did not decide within {:?}", self.policy_timeout),
                });
            turn.decisions.push(decision);
        }
        Ok(turn)
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
        let builtins = self.builtins(request.memory_scope.as_ref(), std::iter::empty());
        let Some(tool) = self
            .visible(&builtins)
            .find(|tool| tool.name() == call.name)
        else {
            return Ok(self.error_outcome(&format!("unknown tool {:?}", call.name)));
        };
        // A second check of the read-only rule. The decision and the call can
        // run on workers with different tool lists, for example in a deploy.
        if request.read_only && tool.effect() >= ToolEffect::Write {
            return Ok(self.error_outcome("this run may only read data"));
        }
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

    /// Load the memory of `scope` and render it as the frozen snapshot.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error when no store is installed, and the
    /// store error otherwise.
    pub async fn memory_snapshot(&self, scope: MemoryScope) -> Result<String, String> {
        let Some(store) = &self.memory else {
            return Err(ActivityFailure::non_retryable(
                "MemoryStoreMissing",
                "the run has a memory scope, but no memory store is installed: call AgentHarness::memory",
            )
            .into_error_payload());
        };
        let blocks = tokio::time::timeout(self.hook_timeout, store.load(&scope))
            .await
            .map_err(|_| self.hook_timed_out("MemoryTimeout", "the memory store"))?
            .map_err(|err| model_failure(&err))?;
        Ok(fit_result(render_snapshot(&blocks), self.max_result_bytes))
    }

    /// Send one report to the delivery.
    ///
    /// # Errors
    ///
    /// Returns the delivery error. A retryable kind retries, and so does a
    /// send over its time budget.
    pub async fn deliver(&self, report: Report) -> Result<(), String> {
        tokio::time::timeout(self.hook_timeout, self.delivery.deliver(&report))
            .await
            .map_err(|_| self.hook_timed_out("DeliveryTimeout", "the delivery"))?
            .map_err(|err| model_failure(&err))
    }

    /// Ask the precheck whether a heartbeat tick should run. With no
    /// precheck, every tick runs. A precheck over its time budget skips the
    /// tick.
    ///
    /// # Errors
    ///
    /// This method returns `Ok` for every answer. The `Result` matches the
    /// activity signature.
    pub async fn precheck_tick(&self, task: HeartbeatTask) -> Result<bool, String> {
        let Some(precheck) = &self.precheck else {
            return Ok(true);
        };
        let answer = tokio::time::timeout(self.hook_timeout, precheck.should_run(&task)).await;
        Ok(answer.unwrap_or_else(|_| {
            tracing::warn!(
                budget = ?self.hook_timeout,
                "heartbeat precheck timed out; the tick is skipped"
            );
            false
        }))
    }

    fn hook_timed_out(&self, kind: &str, what: &str) -> String {
        ActivityFailure::retryable(
            kind,
            format!("{what} did not answer within {:?}", self.hook_timeout),
        )
        .into_error_payload()
    }

    /// The tools that the crate provides for one call: the memory tool, and
    /// a stand-in for each tool the workflow handles itself.
    fn builtins(
        &self,
        scope: Option<&MemoryScope>,
        extra: impl Iterator<Item = ToolDefinition>,
    ) -> Vec<Arc<dyn Tool>> {
        let memory =
            self.memory.as_ref().zip(scope).map(|(store, scope)| {
                Arc::new(MemoryTool::new(Arc::clone(store), scope.clone())) as _
            });
        memory
            .into_iter()
            .chain(extra.map(|definition| Arc::new(WorkflowTool(definition)) as _))
            .collect()
    }

    /// The built-in tools, then each app tool whose name no built-in takes.
    ///
    /// A built-in wins a name clash. Two tools with one name would make many
    /// providers refuse the request.
    fn visible<'a>(&'a self, builtins: &'a [Arc<dyn Tool>]) -> impl Iterator<Item = &'a dyn Tool> {
        // With a store installed, `memory` stays reserved even when this run
        // gets no memory tool. An app tool of that name could otherwise slip
        // past the memory-write opt-in.
        let reserved = self.memory.is_some();
        let taken = move |name: &str| {
            (reserved && name == MEMORY_TOOL) || builtins.iter().any(|tool| tool.name() == name)
        };
        builtins.iter().map(AsRef::as_ref).chain(
            self.tools
                .iter()
                .map(AsRef::as_ref)
                .filter(move |tool| !taken(tool.name())),
        )
    }
}

/// A tool that the workflow runs itself, such as `schedule_followup`.
///
/// The policy sees it as a known tool with the `Internal` effect. The harness
/// never runs it.
#[derive(Debug)]
struct WorkflowTool(ToolDefinition);

impl Tool for WorkflowTool {
    fn name(&self) -> &str {
        &self.0.name
    }

    fn description(&self) -> &str {
        &self.0.description
    }

    fn input_schema(&self) -> serde_json::Value {
        self.0.input_schema.clone()
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::Internal
    }

    fn execute<'a>(
        &'a self,
        _input: serde_json::Value,
        _ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        let err = AgentError::new(
            crate::error::ErrorKind::Tool,
            format!("{} runs in the workflow, not in the harness", self.0.name),
        );
        Box::pin(std::future::ready(Err(err)))
    }

    fn definition(&self) -> ToolDefinition {
        self.0.clone()
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
