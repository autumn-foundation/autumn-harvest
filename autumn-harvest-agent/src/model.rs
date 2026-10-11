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
pub trait AgentModel: Send + Sync + std::fmt::Debug {
    /// Send one request and decode the answer.
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>>;

    /// The model id that the agent cost ledger records (issue #1996).
    ///
    /// The ledger stores the id in clear, so do not put PII in it. It must be
    /// a token of at most 200 bytes: ASCII letters, digits and `._:/@+-`. The
    /// ledger records any other id as [`UNKNOWN_MODEL_ID`].
    fn model_id(&self) -> &str {
        UNKNOWN_MODEL_ID
    }

    /// The cost of one call, in millionths of a US dollar (issue #1996).
    ///
    /// Return `None` when the price is not known. The ledger then counts the
    /// call as unpriced.
    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        let _ = usage;
        None
    }
}

/// The ledger model id of a model that does not name itself.
pub const UNKNOWN_MODEL_ID: &str = "unknown";
