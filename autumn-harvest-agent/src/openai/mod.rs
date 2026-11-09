//! A model over any OpenAI-compatible Chat Completions API (the `openai`
//! feature).
//!
//! The base URL picks the server. [`OPENAI_BASE_URL`],
//! [`OPENROUTER_BASE_URL`] and [`OLLAMA_BASE_URL`] name three. vLLM, LM
//! Studio, a `LiteLLM` proxy and many hosted providers speak the same API.
//!
//! The module has three layers, as the `anthropic` module has:
//!
//! - [`wire`] maps a [`ChatRequest`](crate::ChatRequest) to a body and a
//!   body to a [`ChatResponse`](crate::ChatResponse). It does no I/O.
//! - [`Client`] sends one body over the shared
//!   [`Transport`](crate::provider::Transport).
//! - [`OpenAiModel`] implements [`AgentModel`](crate::AgentModel) on top.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use autumn_harvest_agent::AgentHarness;
//! use autumn_harvest_agent::openai::{OPENROUTER_BASE_URL, OpenAiModel};
//!
//! # fn demo() -> Result<(), autumn_harvest_agent::AgentError> {
//! // Reads `OPENROUTER_API_KEY`, the key variable of this host.
//! let model = OpenAiModel::with_env_key(OPENROUTER_BASE_URL, "openai/gpt-5-mini")?
//!     .header("X-Title", "My agent")?;
//! let harness = AgentHarness::new(Arc::new(model));
//! # let _ = harness;
//! # Ok(())
//! # }
//! ```

mod client;
mod model;
mod stream;
pub mod wire;

pub use client::Client;
pub use model::OpenAiModel;
pub use stream::ChunkAssembler;
pub use wire::{
    OLLAMA_BASE_URL, OPENAI_BASE_URL, OPENAI_KEY_ENV, OPENROUTER_BASE_URL, OPENROUTER_KEY_ENV,
    Settings, TokenField, endpoint, key_env, parse_response, request_body, usage, wire_messages,
};
