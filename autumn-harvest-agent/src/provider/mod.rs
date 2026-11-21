//! The shared layer of the provider models.
//!
//! It holds the HTTP transport, the error type, the body cap, the status
//! table, the Server-Sent Events framing and the price.
//!
//! The `anthropic` and `openai` features each add a model on top. Both use
//! this one HTTP path, so a fix to it reaches both.

mod http;
mod price;
mod sse;
#[cfg(test)]
pub(crate) mod testing;

pub use http::{
    ApiError, BodyError, CONNECT_TIMEOUT, MAX_BODY_BYTES, REQUEST_TIMEOUT, Transport, decode_body,
    error_kind, http_client, printable, push_capped, secret_header,
};
pub use price::Price;
/// The HTTP client crate, so an app can build a client for a model.
pub use reqwest;
pub use sse::SseFrames;
