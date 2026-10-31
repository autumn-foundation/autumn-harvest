//! The model contract: one chat request in, one chat response out.
//!
//! The adapter ships no HTTP client. An app implements [`AgentModel`] for
//! its provider, or bridges a framework it already uses. The loop calls it
//! from the `agent_model_turn` activity, so each answer is recorded once.

use std::future::Future;
use std::pin::Pin;

use crate::error::AgentError;
use crate::message::{ChatMessage, ContentPart, StopReason, TokenUsage, ToolDefinition};

/// A boxed, sendable future, as the object-safe traits return it.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One chat request.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatRequest {
    /// The whole conversation, the system prompt first.
    pub messages: Vec<ChatMessage>,
    /// The tools the model may call.
    pub tools: Vec<ToolDefinition>,
    /// The output cap of this call.
    pub max_tokens: Option<u32>,
    /// The sampling temperature.
    pub temperature: Option<f32>,
}

/// One chat response.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatResponse {
    /// The text parts and tool calls of the answer.
    pub content: Vec<ContentPart>,
    /// Why the model stopped.
    pub stop_reason: StopReason,
    /// Provider-reported tokens for this call.
    pub usage: TokenUsage,
}

/// A chat model with tool calls.
///
/// Object-safe, so a harness holds it as `Arc<dyn AgentModel>`.
///
/// Return an [`AgentError`] whose kind says whether a retry can succeed.
/// `RateLimited`, `Transport` and `Unavailable` retry with backoff. Every
/// other kind fails the call at once.
///
/// The LLM budgets (issue #1997) count `input_tokens` plus `output_tokens` of
/// [`ChatResponse::usage`]. Count the whole prompt in `input_tokens`, cached
/// tokens included, even when the provider reports them apart.
pub trait AgentModel: Send + Sync + std::fmt::Debug {
    /// Send one request and decode the answer.
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>>;

    /// The id of the model that answers, for example `acme-chat-2026-01`.
    ///
    /// The [`ResponseCache`](crate::ResponseCache) puts it in each key. A
    /// model with no id is never cached. The default is `None`.
    ///
    /// The id must change whenever the answer can change for the same
    /// request. Use a pinned model version, not an alias. Put each client
    /// setting that [`ChatRequest`] does not hold, such as `top_p`, in the id.
    ///
    /// The agent cost ledger records the id in clear (issue #1996), so do
    /// not put PII in it. The ledger takes a token of at most 200 bytes:
    /// ASCII letters, digits and `._:/@+-`. It records `None` or any other
    /// id as [`UNKNOWN_MODEL_ID`].
    fn model_id(&self) -> Option<&str> {
        None
    }

    /// The cost of one call, in millionths of a US dollar (issue #1996).
    ///
    /// Return `None` when the price is not known. The ledger then counts the
    /// call as unpriced. The cost budgets (issue #1997) sum this value, so an
    /// unpriced call adds no cost.
    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        let _ = usage;
        None
    }

    /// Send one request, and pass each piece of answer text to `on_delta`
    /// while the model writes it (issue #2268).
    ///
    /// The deltas are a preview. The response is the answer that counts, so
    /// return the whole answer, as [`chat`](Self::chat) does. The default
    /// calls `chat` and passes no delta. A provider with a streaming API
    /// overrides it.
    ///
    /// Pass `""` at each provider event that has no text, for example a
    /// thinking delta. The harness then sends the text that waits.
    fn chat_stream<'a>(
        &'a self,
        request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let _ = on_delta;
        self.chat(request)
    }
}

/// The ledger model id of a model that does not name itself.
pub const UNKNOWN_MODEL_ID: &str = "unknown";
