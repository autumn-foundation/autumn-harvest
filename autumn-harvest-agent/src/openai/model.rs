//! [`OpenAiModel`]: the [`AgentModel`] of a Chat Completions server.

use super::client::Client;
use super::wire::{self, Settings, TokenField};
use crate::provider::Price;
use crate::{AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse, TokenUsage};

/// The key in `value`, or `None` when it is unset or blank.
fn env_key(value: Option<String>) -> Option<String> {
    value
        .map(|key| key.trim().to_owned())
        .filter(|key| !key.is_empty())
}

/// A model behind any OpenAI-compatible Chat Completions API.
///
/// The base URL picks the server: `OpenAI`, `OpenRouter`, Ollama, vLLM, LM
/// Studio, a `LiteLLM` proxy or another server with the same API.
#[derive(Debug, Clone)]
pub struct OpenAiModel {
    client: Client,
    model: String,
    id: String,
    settings: Settings,
    price: Option<Price>,
}

impl OpenAiModel {
    /// `model` at the server at `base_url`, with no key.
    ///
    /// # Errors
    ///
    /// A `Config` error when `base_url` is not an `http` or `https` URL, or
    /// when the HTTP client cannot be built.
    pub fn new(base_url: &str, model: impl Into<String>) -> Result<Self, AgentError> {
        Ok(Self::with_client(Client::new(base_url)?, model))
    }

    /// `model` at `base_url`, with the key of a known host from the
    /// environment.
    ///
    /// The variable is the one that [`key_env`](wire::key_env) names:
    /// `OPENAI_API_KEY` for `OpenAI` and `OPENROUTER_API_KEY` for
    /// `OpenRouter`. Another host gets no key, so a key of one provider never
    /// reaches another. Name a variable with [`with_key_from`](Self::with_key_from).
    ///
    /// # Errors
    ///
    /// The errors of [`new`](Self::new) and of [`api_key`](Self::api_key).
    pub fn with_env_key(base_url: &str, model: impl Into<String>) -> Result<Self, AgentError> {
        match wire::key_env(base_url) {
            Some(var) => Self::with_key_from(base_url, model, var),
            None => Self::new(base_url, model),
        }
    }

    /// `model` at `base_url`, with the key from the environment variable
    /// `var` when it is set.
    ///
    /// The key comes from the environment, not from an argument. The
    /// arguments of a process are readable by each user of the host. A
    /// blank or unset variable means no key.
    ///
    /// # Errors
    ///
    /// The errors of [`new`](Self::new) and of [`api_key`](Self::api_key).
    pub fn with_key_from(
        base_url: &str,
        model: impl Into<String>,
        var: &str,
    ) -> Result<Self, AgentError> {
        let model = Self::new(base_url, model)?;
        match env_key(std::env::var(var).ok()) {
            Some(key) => model.api_key(&key),
            None => Ok(model),
        }
    }

    /// `model` behind `client`. The id names the host of the client.
    #[must_use]
    pub fn with_client(client: Client, model: impl Into<String>) -> Self {
        let model = model.into();
        // The endpoint parsed when the client was built, so the host is there.
        let host = wire::host(client.url()).unwrap_or_default();
        // The `OpenAI` models refuse `max_tokens` now. Others take it.
        let token_field = if host == "api.openai.com" {
            TokenField::MaxCompletionTokens
        } else {
            TokenField::MaxTokens
        };
        Self {
            id: format!("{model}@{host}"),
            client,
            model,
            settings: Settings {
                token_field,
                ..Settings::default()
            },
            price: None,
        }
    }

    /// Send `key` as a bearer key on each request.
    ///
    /// # Errors
    ///
    /// The errors of [`Client::api_key`].
    pub fn api_key(mut self, key: &str) -> Result<Self, AgentError> {
        self.client = self.client.api_key(key)?;
        Ok(self)
    }

    /// Send `value` as the header `name` on each request.
    ///
    /// # Errors
    ///
    /// The errors of [`Client::header`].
    pub fn header(mut self, name: &str, value: &str) -> Result<Self, AgentError> {
        self.client = self.client.header(name, value)?;
        Ok(self)
    }

    /// Report `id` in place of `{model}@{host}`, for the response cache and
    /// the cost ledger.
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Price each call at `price`. The default is unpriced.
    #[must_use]
    pub const fn price(mut self, price: Price) -> Self {
        self.price = Some(price);
        self
    }

    /// Send the output cap as `max_completion_tokens`, as the `OpenAI`
    /// reasoning models need.
    #[must_use]
    pub const fn max_completion_tokens(mut self, on: bool) -> Self {
        self.settings.token_field = if on {
            TokenField::MaxCompletionTokens
        } else {
            TokenField::MaxTokens
        };
        self
    }

    /// Send the draw number as `seed`, or do not. The default is to send it.
    #[must_use]
    pub const fn seed(mut self, on: bool) -> Self {
        self.settings.seed = on;
        self
    }

    /// Ask a stream for its usage, or do not. The default is to ask. Turn it
    /// off for a server that refuses `stream_options`.
    #[must_use]
    pub const fn stream_usage(mut self, on: bool) -> Self {
        self.settings.stream_usage = on;
        self
    }
}

impl AgentModel for OpenAiModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = wire::request_body(request, &self.model, self.settings);
            let answer = self.client.create(&body).await?;
            wire::parse_response(&answer)
        })
    }

    fn chat_stream<'a>(
        &'a self,
        request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = wire::request_body(request, &self.model, self.settings);
            let answer = self
                .client
                .stream(body, self.settings.stream_usage, on_delta)
                .await?;
            wire::parse_response(&answer)
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some(&self.id)
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        self.price.map(|price| price.cost_usd_micros(usage))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest::llm_ledger::LlmCall;
    use serde_json::json;

    use super::*;
    use crate::openai::{OLLAMA_BASE_URL, OPENROUTER_BASE_URL};
    use crate::provider::testing::{first, head, serve_each};
    use crate::{ChatMessage, ChatRole, ContentPart, ErrorKind, StopReason};

    /// A server that answers each request with the next `(status, body)`.
    async fn serve(answers: Vec<(u16, String)>) -> (String, crate::provider::testing::Seen) {
        let answers = answers
            .into_iter()
            .map(|(status, body)| (head(status, body.len()), body.into_bytes()))
            .collect();
        let (url, seen) = serve_each(answers).await;
        (url.trim_end_matches("/endpoint").to_owned(), seen)
    }

    fn ask() -> ChatRequest {
        ChatRequest {
            messages: vec![ChatMessage::text(ChatRole::User, "Hi.")],
            tools: Vec::new(),
            max_tokens: None,
            temperature: None,
            draw: None,
        }
    }

    #[tokio::test]
    async fn a_chat_maps_the_answer_and_its_usage() {
        let answer = json!({"choices": [{"finish_reason": "stop",
            "message": {"role": "assistant", "content": "Hello."}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 10,
                      "prompt_tokens_details": {"cached_tokens": 40}}});
        let (url, seen) = serve(vec![(200, answer.to_string())]).await;
        let model = OpenAiModel::new(&url, "llama3.1:8b").unwrap().price(Price {
            input: 150,
            output: 600,
            cache_read: 75,
            cache_write: 150,
        });
        let response = model.chat(&ask()).await.unwrap();
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        assert_eq!(response.content, [ContentPart::Text("Hello.".into())]);
        assert_eq!(response.usage.input_tokens, 100);
        assert_eq!(response.usage.cache_read_tokens, 40);
        // 60 at 150, 40 at 75 and 10 at 600 nanodollars: 18 millionths.
        assert_eq!(model.cost_usd_micros(&response.usage), Some(18));
        let request = first(&seen);
        assert!(request.contains(r#""model":"llama3.1:8b""#), "{request}");
        assert!(!request.contains(r#""stream""#), "{request}");
    }

    #[tokio::test]
    async fn a_chat_that_meets_an_overload_or_a_bad_request_says_which() {
        let (url, _) = serve(vec![(503, "{}".into()), (400, "{}".into())]).await;
        let model = OpenAiModel::new(&url, "m").unwrap();
        let busy = model.chat(&ask()).await.unwrap_err();
        assert_eq!(busy.kind(), ErrorKind::Unavailable);
        let bad = model.chat(&ask()).await.unwrap_err();
        assert!(!bad.kind().is_retryable());
    }

    #[tokio::test]
    async fn a_streamed_chat_passes_each_delta_and_maps_the_answer() {
        let wire = [
            json!({"choices": [{"delta": {"content": "Hel"}}]}).to_string(),
            json!({"choices": [{"delta": {"content": "lo"}, "finish_reason": "stop"}]}).to_string(),
            json!({"choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 2}})
                .to_string(),
            "[DONE]".to_owned(),
        ]
        .iter()
        .fold(String::new(), |all, data| all + "data: " + data + "\n\n");
        let (url, _) = serve(vec![(200, wire)]).await;
        let model = OpenAiModel::new(&url, "m").unwrap();
        let seen = std::sync::Mutex::new(String::new());
        let on_delta = |piece: &str| seen.lock().unwrap().push_str(piece);
        let response = model.chat_stream(&ask(), &on_delta).await.unwrap();
        assert_eq!(seen.into_inner().unwrap(), "Hello");
        assert_eq!(response.content, [ContentPart::Text("Hello".into())]);
        assert_eq!(
            (response.usage.input_tokens, response.usage.output_tokens),
            (5, 2)
        );
    }

    #[tokio::test]
    async fn the_settings_reach_the_request() {
        let answer = json!({"choices": [{"finish_reason": "stop",
            "message": {"role": "assistant", "content": "x"}}]});
        let (url, seen) = serve(vec![(200, answer.to_string())]).await;
        let mut request = ask();
        request.max_tokens = Some(64);
        request.draw = Some(crate::DrawId::new(2));
        OpenAiModel::new(&url, "o4")
            .unwrap()
            .max_completion_tokens(true)
            .seed(false)
            .chat(&request)
            .await
            .unwrap();
        let body = first(&seen);
        assert!(body.contains(r#""max_completion_tokens":64"#), "{body}");
        assert!(!body.contains("seed"), "{body}");
    }

    #[test]
    fn the_id_names_the_model_and_the_host_and_passes_the_ledger() {
        let ollama = OpenAiModel::new(OLLAMA_BASE_URL, "llama3.1:8b").unwrap();
        assert_eq!(ollama.model_id(), Some("llama3.1:8b@localhost:11434"));
        let router = OpenAiModel::new(OPENROUTER_BASE_URL, "openai/gpt-5-mini").unwrap();
        assert_eq!(router.model_id(), Some("openai/gpt-5-mini@openrouter.ai"));
        for model in [&ollama, &router] {
            LlmCall::new(model.model_id().unwrap(), 1, 1)
                .validate()
                .unwrap();
        }
        assert_eq!(router.id("mine").model_id(), Some("mine"));
        assert_eq!(ollama.cost_usd_micros(&TokenUsage::new(9, 9)), None);
    }

    #[test]
    fn a_key_variable_reaches_only_its_own_host() {
        // No known variable for a local or unknown host: no key at all.
        assert!(wire::key_env(OLLAMA_BASE_URL).is_none());
        assert!(wire::key_env("https://litellm.example.test/v1").is_none());
        assert_eq!(
            wire::key_env(OPENROUTER_BASE_URL),
            Some("OPENROUTER_API_KEY")
        );
    }

    #[test]
    fn a_blank_or_unset_key_variable_means_no_key() {
        assert_eq!(env_key(None), None);
        assert_eq!(env_key(Some("  ".into())), None);
        assert_eq!(env_key(Some(" sk-a \n".into())), Some("sk-a".into()));
    }

    #[test]
    fn the_openai_host_sends_the_cap_in_the_field_its_models_need() {
        let mut request = ask();
        request.max_tokens = Some(8);
        let openai = OpenAiModel::new(crate::openai::OPENAI_BASE_URL, "gpt-5-mini").unwrap();
        let body = wire::request_body(&request, "gpt-5-mini", openai.settings);
        assert_eq!(body["max_completion_tokens"], 8);
        let local = OpenAiModel::new(OLLAMA_BASE_URL, "m").unwrap();
        let body = wire::request_body(&request, "m", local.settings);
        assert_eq!(body["max_tokens"], 8);
    }

    #[test]
    fn the_key_stays_out_of_debug_output() {
        let model = OpenAiModel::new(OPENROUTER_BASE_URL, "m")
            .unwrap()
            .api_key("sk-or-secret")
            .unwrap();
        assert!(!format!("{model:?}").contains("sk-or-secret"));
    }
}
