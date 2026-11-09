//! One POST to a Chat Completions endpoint, over the shared transport.

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName};
use serde_json::{Value, json};

use super::stream::ChunkAssembler;
use super::wire;
use crate::provider::{self, ApiError, REQUEST_TIMEOUT, Transport};
use crate::{AgentError, ErrorKind};

/// A Chat Completions client: a base URL, an optional key, extra headers, an
/// HTTP client and a body cap.
///
/// The transport is [`Transport`]. It follows no redirect, reads each body
/// under a cap and refuses bytes that are not UTF-8.
#[derive(Clone, Debug)]
pub struct Client {
    transport: Transport,
}

impl Client {
    /// A client for the server at `base_url`, with no key.
    ///
    /// # Errors
    ///
    /// A `Config` error when `base_url` is not an `http` or `https` URL, or
    /// when the HTTP client cannot be built.
    pub fn new(base_url: &str) -> Result<Self, AgentError> {
        Self::with_http(base_url, provider::http_client(REQUEST_TIMEOUT)?)
    }

    /// A client for `base_url` that sends each request through `http`.
    ///
    /// # Errors
    ///
    /// A `Config` error when `base_url` is not an `http` or `https` URL.
    pub fn with_http(base_url: &str, http: reqwest::Client) -> Result<Self, AgentError> {
        Ok(Self {
            transport: Transport::new(http, wire::endpoint(base_url)?),
        })
    }

    /// Send `key` as `Authorization: Bearer {key}` on each request.
    ///
    /// # Errors
    ///
    /// A `Config` error when the key is blank, or holds a byte that a header
    /// cannot carry.
    pub fn api_key(self, key: &str) -> Result<Self, AgentError> {
        if key.trim().is_empty() {
            return Err(AgentError::new(ErrorKind::Config, "the API key is blank"));
        }
        let value = provider::secret_header(&format!("Bearer {key}"), "API key")?;
        Ok(Self {
            transport: self.transport.header(AUTHORIZATION, value),
        })
    }

    /// Send `value` as the header `name` on each request, for example the
    /// `HTTP-Referer` and `X-Title` of `OpenRouter`.
    ///
    /// # Errors
    ///
    /// A `Config` error when the name or the value is not valid.
    pub fn header(self, name: &str, value: &str) -> Result<Self, AgentError> {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            AgentError::new(
                ErrorKind::Config,
                format!("{} is not a header name", provider::printable(name)),
            )
        })?;
        // A header can carry a secret too, so its value stays out of logs.
        let value = provider::secret_header(value, "header value")?;
        Ok(Self {
            transport: self.transport.header(name, value),
        })
    }

    /// Read at most `cap` bytes of each response body.
    #[must_use]
    pub fn max_body_bytes(mut self, cap: usize) -> Self {
        self.transport = self.transport.max_body_bytes(cap);
        self
    }

    /// The Chat Completions endpoint.
    #[must_use]
    pub fn url(&self) -> &str {
        self.transport.url()
    }

    /// Send one request `body` and return the response body.
    ///
    /// # Errors
    ///
    /// The errors of [`Transport::json`]. A `2xx` body with an `error`
    /// object is the error of its code. A body with no `choices` array is
    /// `Decode`.
    pub async fn create(&self, body: &Value) -> Result<Value, ApiError> {
        let (status, answer) = self.transport.json(body, &HeaderMap::new()).await?;
        check_answer(answer, status)
    }

    /// Send one request `body` as a stream, and return a body with the shape
    /// of a [`create`](Self::create) answer.
    ///
    /// With `usage`, the request asks for the usage in a last chunk.
    ///
    /// # Errors
    ///
    /// As [`create`](Self::create). A body that is not an object is a
    /// `Config` error. A stream that breaks off is `Transport`.
    pub async fn stream(
        &self,
        body: Value,
        usage: bool,
        on_delta: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<Value, ApiError> {
        let Value::Object(mut map) = body else {
            return Err(ApiError::new(
                ErrorKind::Config,
                None,
                "the request body is not a JSON object",
            ));
        };
        map.insert("stream".to_owned(), json!(true));
        if usage {
            map.insert("stream_options".to_owned(), json!({"include_usage": true}));
        }
        let body = Value::Object(map);
        let mut chunks = ChunkAssembler::default();
        let status = self
            .transport
            .events(&body, &HeaderMap::new(), |data| {
                chunks.event(data, on_delta)
            })
            .await?;
        let answer = chunks
            .finish()
            .map_err(|e| ApiError::new(e.kind(), Some(status), e.message()))?;
        check_answer(answer, status)
    }
}

/// Refuse a `2xx` body that holds an error or no choice.
///
/// Some servers, `OpenRouter` among them, answer `200` with an `error`
/// object. That body is the error of its code.
fn check_answer(answer: Value, status: u16) -> Result<Value, ApiError> {
    if answer.get("error").is_some_and(|error| !error.is_null()) {
        let error = wire::error_of(&answer["error"]);
        return Err(ApiError::new(error.kind(), Some(status), error.message()));
    }
    if answer["choices"].is_array() {
        Ok(answer)
    } else {
        Err(ApiError::new(
            ErrorKind::Decode,
            Some(status),
            "its response is not a chat completion: `choices` is required",
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde_json::json;

    use super::*;
    use crate::provider::testing::{first, serve};

    fn reply() -> Value {
        json!({"choices": [{"finish_reason": "stop",
            "message": {"role": "assistant", "content": "hi"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}})
    }

    fn client(url: &str) -> Client {
        let base = url.trim_end_matches("/v1/endpoint");
        Client::new(base).unwrap()
    }

    #[tokio::test]
    async fn a_request_goes_to_the_endpoint_with_the_key_and_the_headers() {
        let (url, seen) = serve(200, reply().to_string().as_bytes()).await;
        let base = url.trim_end_matches("/endpoint");
        let client = Client::new(base)
            .unwrap()
            .api_key("sk-or-test")
            .unwrap()
            .header("HTTP-Referer", "https://example.test")
            .unwrap()
            .header("X-Title", "Desk")
            .unwrap();
        assert!(client.url().ends_with("/v1/chat/completions"));
        // The test server answers any path, so the endpoint is checked
        // above and the answer here.
        let body = client.create(&json!({"model": "m"})).await.unwrap();
        assert_eq!(body["choices"][0]["message"]["content"], "hi");
        let head = first(&seen);
        assert!(head.starts_with("post /v1/chat/completions"), "{head}");
        assert!(head.contains("authorization: bearer sk-or-test"), "{head}");
        assert!(
            head.contains("http-referer: https://example.test"),
            "{head}"
        );
        assert!(head.contains("x-title: desk"), "{head}");
    }

    #[tokio::test]
    async fn a_request_with_no_key_sends_no_authorization() {
        let (url, seen) = serve(200, reply().to_string().as_bytes()).await;
        client(&url).create(&json!({})).await.unwrap();
        assert!(!first(&seen).contains("authorization"));
    }

    #[tokio::test]
    async fn a_429_or_a_5xx_is_retryable_and_a_400_is_not() {
        for (status, retryable) in [(429, true), (503, true), (400, false)] {
            let (url, _) = serve(status, br#"{"error":{"message":"x"}}"#).await;
            let error = client(&url).create(&json!({})).await.unwrap_err();
            assert_eq!(error.status(), Some(status));
            assert_eq!(error.kind().is_retryable(), retryable, "{status}");
        }
    }

    #[tokio::test]
    async fn an_error_in_a_2xx_body_is_the_error_of_its_code() {
        let (url, _) = serve(200, br#"{"error":{"code":502,"message":"upstream"}}"#).await;
        let error = client(&url).create(&json!({})).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(error.status(), Some(200));
        let (url, _) = serve(200, br#"{"id":"x"}"#).await;
        let error = client(&url).create(&json!({})).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
    }

    #[tokio::test]
    async fn a_stream_asks_for_usage_and_returns_the_body() {
        let wire = [
            json!({"choices": [{"delta": {"content": "a"}, "finish_reason": "stop"}]}).to_string(),
            json!({"choices": [], "usage": {"prompt_tokens": 2, "completion_tokens": 1}})
                .to_string(),
            "[DONE]".to_owned(),
        ]
        .iter()
        .fold(String::new(), |all, data| all + "data: " + data + "\n\n");
        let (url, seen) = serve(200, wire.as_bytes()).await;
        let body = client(&url)
            .stream(json!({"model": "m"}), true, &|_| {})
            .await
            .unwrap();
        assert_eq!(body["choices"][0]["message"]["content"], "a");
        assert_eq!(body["usage"]["prompt_tokens"], 2);
        let request = first(&seen);
        assert!(request.contains(r#""stream":true"#), "{request}");
        assert!(request.contains(r#""include_usage":true"#), "{request}");
    }

    #[tokio::test]
    async fn a_stream_with_no_usage_asks_for_none() {
        let wire =
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let (url, seen) = serve(200, wire.as_bytes()).await;
        client(&url)
            .stream(json!({}), false, &|_| {})
            .await
            .unwrap();
        assert!(!first(&seen).contains("include_usage"));
        let error = client(&url)
            .stream(json!([1]), true, &|_| {})
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
    }

    #[test]
    fn a_bad_base_url_key_or_header_is_refused() {
        assert_eq!(
            Client::new("localhost:1").unwrap_err().kind(),
            ErrorKind::Config
        );
        let ok = Client::new("http://localhost:1/v1").unwrap();
        assert!(ok.clone().api_key("sk-a\nb").is_err());
        assert!(ok.clone().api_key(" ").is_err());
        assert!(ok.clone().header("bad name", "v").is_err());
        assert!(ok.header("x-ok", "v\nw").is_err());
    }

    #[test]
    fn the_key_stays_out_of_debug_output() {
        let client = Client::new("http://localhost:1/v1")
            .unwrap()
            .api_key("sk-secret")
            .unwrap();
        let shown = format!("{client:?}");
        assert!(!shown.contains("sk-secret"), "{shown}");
    }
}
