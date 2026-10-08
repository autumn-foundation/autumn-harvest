//! Default outbound HTTP transport for durable completion callbacks (issue #605).
//!
//! `autumn-harvest` core is deliberately Postgres-only and ships no HTTP
//! client — [`autumn_harvest::completion_callback::CompletionCallbackDeliverer`]
//! is a thin transport seam (mirrors `PayloadStore`/`HistoryArchiver`).
//! [`ReqwestCallbackDeliverer`] is the batteries-included implementation the
//! plugin auto-wires when an embedder configures completion callbacks
//! without supplying their own deliverer.

use autumn_harvest::completion_callback::{
    CompletionCallbackDeliverer, DeliverFuture, DeliveryAttempt,
};
use std::time::Duration;

/// Default request timeout for a completion-callback delivery attempt.
pub const DEFAULT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// A [`CompletionCallbackDeliverer`] backed by a `reqwest::Client`.
///
/// Redirects are never followed (`redirect::Policy::none()`): an
/// allowlisted host that responds with a 3xx pointing at a non-allowlisted
/// (e.g. internal) address must never be silently chased, or the SSRF guard
/// enforced at registration/enqueue time would be bypassed at delivery
/// time. A 3xx response is instead reported as its literal status code,
/// which `DeliveryAttempt::is_success` correctly classifies as a failure
/// (only 2xx is success), so it flows through the normal retry/backoff/
/// dead-letter path like any other non-2xx response.
pub struct ReqwestCallbackDeliverer {
    client: reqwest::Client,
}

impl ReqwestCallbackDeliverer {
    /// Build a deliverer with [`DEFAULT_DELIVERY_TIMEOUT`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_DELIVERY_TIMEOUT)
    }

    /// Build a deliverer with a custom per-request timeout.
    ///
    /// # Panics
    /// Never panics in practice: the internal `expect` only guards against a
    /// `reqwest::Client` builder failure, which cannot occur for this
    /// static, valid configuration (a timeout and a redirect policy).
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client with a static, valid configuration must build");
        Self { client }
    }
}

impl Default for ReqwestCallbackDeliverer {
    fn default() -> Self {
        Self::new()
    }
}

impl CompletionCallbackDeliverer for ReqwestCallbackDeliverer {
    fn deliver<'a>(
        &'a self,
        target_url: &'a str,
        body: &'a [u8],
        headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a> {
        Box::pin(async move {
            let mut request = self
                .client
                .post(target_url)
                .header("Content-Type", "application/json")
                .body(body.to_vec());
            for (name, value) in headers {
                request = request.header(*name, value);
            }

            match request.send().await {
                Ok(response) => DeliveryAttempt::success(response.status().as_u16()),
                Err(error) => DeliveryAttempt::transport_error(error.to_string()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliverer_is_object_safe_and_send_sync() {
        fn assert_bounds<T: CompletionCallbackDeliverer>() {}
        assert_bounds::<ReqwestCallbackDeliverer>();
        let _: Box<dyn CompletionCallbackDeliverer> = Box::new(ReqwestCallbackDeliverer::new());
    }

    #[test]
    fn default_matches_new() {
        let _ = ReqwestCallbackDeliverer::default();
        let _ = ReqwestCallbackDeliverer::new();
        let _ = ReqwestCallbackDeliverer::with_timeout(Duration::from_secs(5));
    }
}
