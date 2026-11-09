//! [`ClaudeModel`]: the [`AgentModel`] of the Messages API.

use std::sync::Arc;

use serde_json::Value;

use super::chat::{self, TurnStore, Turns};
use super::client::Client;
use super::wire::{self, DEFAULT_MODEL};
use crate::{AgentError, AgentModel, BoxFuture, ChatRequest, ChatResponse, TokenUsage};

/// A Claude model over the Messages API.
///
/// The default model is [`DEFAULT_MODEL`] with adaptive thinking, effort
/// [`EFFORT`](super::EFFORT), automatic prompt caching and the server-side
/// refusal fallback. `chat_stream` streams the same request over
/// Server-Sent Events.
///
/// [`model_id`](AgentModel::model_id) names the model and its settings in a
/// form that the cost ledger accepts.
/// [`cost_usd_micros`](AgentModel::cost_usd_micros) prices a call from
/// [`price`](super::price).
#[derive(Debug, Clone)]
pub struct ClaudeModel {
    client: Client,
    model: String,
    fallbacks: bool,
    /// The model and every setting that changes its answers.
    id: String,
    turns: Turns,
}

impl ClaudeModel {
    /// A model with `api_key` and the default model. The raw turns live in
    /// memory only.
    ///
    /// # Errors
    ///
    /// A `Config` error when [`Client::new`] refuses the key.
    pub fn new(api_key: &str) -> Result<Self, AgentError> {
        Ok(Self::with_client(Client::new(api_key)?))
    }

    /// A model that sends each request through `client`.
    #[must_use]
    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            model: DEFAULT_MODEL.into(),
            fallbacks: true,
            id: wire::model_id(DEFAULT_MODEL, true),
            turns: Turns::default(),
        }
    }

    /// Use `model` in place of the default.
    ///
    /// The model gets a memory copy of its own. A thinking block belongs to
    /// the model that wrote it, so a clone on another model does not see the
    /// turns of this one.
    #[must_use]
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self.id = wire::model_id(&self.model, self.fallbacks);
        self.turns = self.turns.fresh();
        self
    }

    /// Ask for the server-side refusal fallback, or do not. The default is
    /// to ask. A fallback answers one turn on another model.
    #[must_use]
    pub fn fallbacks(mut self, on: bool) -> Self {
        self.fallbacks = on;
        self.id = wire::model_id(&self.model, on);
        self
    }

    /// Keep the raw turns in `store` too, so a restarted process can replay
    /// them.
    #[must_use]
    pub fn turn_store(mut self, store: Arc<dyn TurnStore>) -> Self {
        self.turns.store = Some(store);
        self
    }

    /// The JSON body of one request, with the raw turns replayed.
    fn body(&self, request: &ChatRequest) -> Value {
        chat::request_body(request, &self.model, self.fallbacks, |before, visible| {
            self.turns.get(before, visible)
        })
    }

    /// Map a whole message body, and keep its raw turn for a replay.
    fn finish(&self, request: &ChatRequest, body: &Value) -> Result<ChatResponse, AgentError> {
        let response = chat::parse_response(body)?;
        if let Some((key, raw)) = chat::replayable(&chat::turn_context(request), body) {
            self.turns.put(key, raw)?;
        }
        Ok(response)
    }
}

impl AgentModel for ClaudeModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = self.client.create(&self.body(request)).await?;
            self.finish(request, &body)
        })
    }

    fn chat_stream<'a>(
        &'a self,
        request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let body = self.client.stream(self.body(request), on_delta).await?;
            self.finish(request, &body)
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some(&self.id)
    }

    /// The price of the configured model. A turn that a refusal fallback
    /// answers is billed at the rates of the fallback model. This price does
    /// not see that model, so it reads the rates of the configured one.
    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        wire::cost_usd_micros(&self.model, usage)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest::llm_ledger::LlmCall;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{ChatMessage, ChatRole, ContentPart, ErrorKind, StopReason};

    /// A server that answers each request with the next `(status, body)`.
    async fn serve(answers: Vec<(u16, String)>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/messages", listener.local_addr().unwrap());
        tokio::spawn(async move {
            for (status, body) in answers {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 8192];
                // Read the head, then the body that its length names.
                loop {
                    let read = socket.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    let text = String::from_utf8_lossy(&request).to_lowercase();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .and_then(|n| n.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let head = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(body.as_bytes()).await.unwrap();
            }
        });
        url
    }

    fn model(url: &str) -> ClaudeModel {
        ClaudeModel::with_client(Client::new("sk-ant-test").unwrap().endpoint(url))
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
        let answer = json!({"type": "message", "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "Hello."}],
            "usage": {"input_tokens": 10, "output_tokens": 2, "cache_read_input_tokens": 90}});
        let url = serve(vec![(200, answer.to_string())]).await;
        let model = model(&url);
        let response = model.chat(&ask()).await.unwrap();
        assert_eq!(response.stop_reason, StopReason::EndTurn);
        assert_eq!(response.content, [ContentPart::Text("Hello.".into())]);
        assert_eq!(response.usage.input_tokens, 100);
        assert_eq!(response.usage.cache_read_tokens, 90);
        // 10 uncached at 4000, 90 cached at 200, 2 out at 20000 nanos.
        assert_eq!(model.cost_usd_micros(&response.usage), Some(98));
    }

    #[tokio::test]
    async fn a_chat_that_meets_an_overload_or_a_bad_request_says_which() {
        let url = serve(vec![(529, "{}".into()), (400, "{}".into())]).await;
        let model = model(&url);
        let overloaded = model.chat(&ask()).await.unwrap_err();
        assert_eq!(overloaded.kind(), ErrorKind::Unavailable);
        assert!(overloaded.kind().is_retryable());
        let bad = model.chat(&ask()).await.unwrap_err();
        assert_eq!(bad.kind(), ErrorKind::Provider);
        assert!(!bad.kind().is_retryable());
    }

    #[tokio::test]
    async fn a_streamed_chat_passes_each_delta_and_maps_the_message() {
        let events = [
            json!({"type": "message_start", "message": {"type": "message", "role": "assistant",
                "content": [], "stop_reason": null, "usage": {"input_tokens": 5, "output_tokens": 1}}}),
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": "Hel"}}),
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": "lo"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
                   "usage": {"output_tokens": 3}}),
            json!({"type": "message_stop"}),
        ];
        let wire = events.iter().fold(String::new(), |wire, e| {
            wire + "data: " + &e.to_string() + "\n\n"
        });
        let url = serve(vec![(200, wire)]).await;
        let seen = std::sync::Mutex::new(String::new());
        let on_delta = |piece: &str| seen.lock().unwrap().push_str(piece);
        let response = model(&url).chat_stream(&ask(), &on_delta).await.unwrap();
        assert_eq!(seen.into_inner().unwrap(), "Hello");
        assert_eq!(response.content, [ContentPart::Text("Hello".into())]);
        assert_eq!(
            (response.usage.input_tokens, response.usage.output_tokens),
            (5, 3)
        );
    }

    #[test]
    fn the_model_reports_a_ledger_id_and_a_price() {
        let model = ClaudeModel::new("sk-ant-test").unwrap();
        for model in [
            model.clone(),
            model.clone().fallbacks(false),
            model.model("claude-haiku-5-5"),
        ] {
            let id = model.model_id().unwrap();
            let usage = TokenUsage::new(1_000, 100);
            let cost = model.cost_usd_micros(&usage).unwrap();
            LlmCall::new(id, 1_000, 100)
                .with_cost_usd_micros(cost)
                .validate()
                .unwrap_or_else(|e| panic!("{id}: {e}"));
        }
    }

    #[test]
    fn a_new_setting_changes_the_id() {
        let base = ClaudeModel::new("sk-ant-test").unwrap();
        let ids = [
            base.model_id().unwrap().to_owned(),
            base.clone().fallbacks(false).model_id().unwrap().to_owned(),
            base.model("claude-sonnet-5-5")
                .model_id()
                .unwrap()
                .to_owned(),
        ];
        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
    }

    #[test]
    fn the_key_stays_out_of_debug_output() {
        let shown = format!("{:?}", ClaudeModel::new("sk-ant-secret").unwrap());
        assert!(!shown.contains("sk-ant-secret"), "{shown}");
    }
}
