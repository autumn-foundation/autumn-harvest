//! A Claude model over the Anthropic Messages API (the `anthropic` feature).
//!
//! The module has three layers:
//!
//! - [`wire`] holds the rules with no I/O: the constants, the settings of
//!   each model, the error table, the usage counters and the prices.
//! - [`Client`] sends one request body and returns the message. It reads
//!   the body under a cap and refuses bytes that are not UTF-8.
//! - [`ClaudeModel`] implements [`AgentModel`](crate::AgentModel) on top.
//!
//! An app that keeps its own transcript format can use [`Client`] and
//! [`wire`] alone. An app on the agent loop uses [`ClaudeModel`].
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use autumn_harvest_agent::AgentHarness;
//! use autumn_harvest_agent::anthropic::ClaudeModel;
//!
//! # fn demo() -> Result<(), autumn_harvest_agent::AgentError> {
//! let key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_default();
//! let model = ClaudeModel::new(&key)?.model("claude-sonnet-5-5");
//! let harness = AgentHarness::new(Arc::new(model));
//! # let _ = harness;
//! # Ok(())
//! # }
//! ```

mod chat;
mod client;
mod model;
mod sse;
pub mod wire;

pub use chat::{
    TurnStore, parse_response, replayable, request_body, streaming_body, turn_context, turn_key,
    wire_messages,
};
pub use client::{
    ApiError, BodyError, CONNECT_TIMEOUT, Client, MAX_BODY_BYTES, REQUEST_TIMEOUT, decode_body,
    http_client, push_capped,
};
pub use model::ClaudeModel;
/// The HTTP client crate, so an app can build a client for [`Client::http`].
pub use reqwest;
pub use sse::{SseFrames, StreamAssembler};
pub use wire::{
    API_VERSION, DEFAULT_MAX_TOKENS, DEFAULT_MODEL, EFFORT, ENDPOINT, FALLBACK_BETA, Price,
    cost_usd_micros, error_kind, model_id, price, usage,
};
