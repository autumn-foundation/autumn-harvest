//! Tools: the functions an agent may call.

use std::future::Future;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::AgentError;
use crate::message::{RunId, SessionId, ToolDefinition};
use crate::model::BoxFuture;

/// What a tool can change. A policy uses it to gate calls.
///
/// The variants go from least to most impact, so
/// `effect <= ToolEffect::Internal` reads as "safe for an unattended run".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// Reads data. Changes nothing.
    ReadOnly,
    /// Changes only the agent's own private state.
    Internal,
    /// Changes data that other people can see.
    Write,
    /// Acts outside the app: mail, a third-party API, money.
    External,
}

/// The facts a tool gets with each call.
///
/// `run_id`, `step` and `call_id` together are an idempotency key. A call
/// that was in flight at a crash runs again, and the key lets the tool see
/// that. The step is in the key because a provider can reuse a call id in a
/// later step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolContext {
    /// The run that made the call.
    pub run_id: RunId,
    /// The provider id of this call.
    pub call_id: String,
    /// The session of the run, if any.
    pub session_id: Option<SessionId>,
    /// The tool round (0-based) that made the call, counted across
    /// follow-up segments.
    pub step: u32,
}

/// A function the agent may call.
pub trait Tool: Send + Sync + std::fmt::Debug {
    /// The name the model calls. Use `snake_case`.
    fn name(&self) -> &str;

    /// What the tool does, for the model.
    fn description(&self) -> &str;

    /// The JSON Schema of the input object.
    fn input_schema(&self) -> serde_json::Value;

    /// Run the tool on decoded JSON arguments.
    fn execute<'a>(
        &'a self,
        input: serde_json::Value,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>>;

    /// What the tool can change. The default is [`ToolEffect::Write`]: a tool
    /// gets less trust only when it says so.
    fn effect(&self) -> ToolEffect {
        ToolEffect::Write
    }

    /// The metadata the model sees.
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().to_owned(),
            description: self.description().to_owned(),
            input_schema: self.input_schema(),
        }
    }
}

type ToolFn = Box<
    dyn Fn(
            serde_json::Value,
            ToolContext,
        ) -> BoxFuture<'static, Result<serde_json::Value, AgentError>>
        + Send
        + Sync,
>;

/// A [`Tool`] made from an async function.
pub struct FnTool {
    name: String,
    description: String,
    schema: serde_json::Value,
    effect: ToolEffect,
    run: ToolFn,
}

impl std::fmt::Debug for FnTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnTool")
            .field("name", &self.name)
            .field("effect", &self.effect)
            .finish_non_exhaustive()
    }
}

impl FnTool {
    /// Wrap an async function of the decoded arguments.
    pub fn new<F, Fut>(
        name: impl Into<String>,
        description: impl Into<String>,
        schema: serde_json::Value,
        run: F,
    ) -> Self
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, AgentError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            description: description.into(),
            schema,
            effect: ToolEffect::Write,
            run: Box::new(move |input, _ctx| Box::pin(run(input))),
        }
    }

    /// Wrap an async function that also gets the [`ToolContext`], for
    /// example to use the idempotency key.
    pub fn with_context<F, Fut>(
        name: impl Into<String>,
        description: impl Into<String>,
        schema: serde_json::Value,
        run: F,
    ) -> Self
    where
        F: Fn(serde_json::Value, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, AgentError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            description: description.into(),
            schema,
            effect: ToolEffect::Write,
            run: Box::new(move |input, ctx| Box::pin(run(input, ctx))),
        }
    }

    /// Say what the tool can change. The default is [`ToolEffect::Write`].
    #[must_use]
    pub const fn effect(mut self, effect: ToolEffect) -> Self {
        self.effect = effect;
        self
    }

    /// Wrap the tool in an `Arc` for a harness.
    #[must_use]
    pub fn shared(self) -> Arc<dyn Tool> {
        Arc::new(self)
    }
}

impl Tool for FnTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> serde_json::Value {
        self.schema.clone()
    }

    fn execute<'a>(
        &'a self,
        input: serde_json::Value,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        (self.run)(input, ctx.clone())
    }

    fn effect(&self) -> ToolEffect {
        self.effect
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn a_context_tool_sees_its_idempotency_key() {
        let tool =
            FnTool::with_context(
                "key",
                "Echo the key.",
                json!({}),
                |_input, ctx| async move {
                    Ok(json!(format!("{}/{}", ctx.run_id.as_str(), ctx.call_id)))
                },
            )
            .effect(ToolEffect::ReadOnly);
        let ctx = ToolContext {
            run_id: RunId::new("r"),
            call_id: "c".into(),
            session_id: None,
            step: 0,
        };
        assert_eq!(tool.execute(json!({}), &ctx).await.unwrap(), json!("r/c"));
        assert_eq!(Tool::effect(&tool), ToolEffect::ReadOnly);
        assert_eq!(tool.definition().name, "key");
        assert!(format!("{tool:?}").contains("ReadOnly"));
    }

    #[test]
    fn effects_order_from_least_to_most_impact() {
        assert!(ToolEffect::ReadOnly < ToolEffect::Internal);
        assert!(ToolEffect::Internal < ToolEffect::Write);
        assert!(ToolEffect::Write < ToolEffect::External);
    }
}
