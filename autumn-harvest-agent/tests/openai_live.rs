//! A live check of `OpenAiModel` against a real server. It is ignored by
//! default, because it needs a server and can cost money.
//!
//! Run it against any OpenAI-compatible server:
//!
//! ```sh
//! OPENAI_LIVE_BASE_URL=http://localhost:11434/v1 OPENAI_LIVE_MODEL=qwen2.5:7b \
//!   cargo test -p autumn-harvest-agent --features openai --test openai_live -- --ignored
//! ```
//!
//! `OPENAI_API_KEY` is sent when it is set, as `OpenAiModel::with_env_key`
//! sends it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use autumn_harvest_agent::openai::OpenAiModel;
use autumn_harvest_agent::{AgentModel, ChatMessage, ChatRequest, ChatRole, ErrorKind};

fn live() -> (String, String) {
    let base = std::env::var("OPENAI_LIVE_BASE_URL").expect("set OPENAI_LIVE_BASE_URL");
    let model = std::env::var("OPENAI_LIVE_MODEL").expect("set OPENAI_LIVE_MODEL");
    (base, model)
}

fn ask() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage::text(
            ChatRole::User,
            "Answer with the one word: ready",
        )],
        tools: Vec::new(),
        max_tokens: Some(16),
        temperature: None,
        draw: None,
    }
}

#[tokio::test]
#[ignore = "needs a live OpenAI-compatible server"]
async fn a_live_chat_and_stream_answer_with_usage() {
    let (base, model) = live();
    let model = OpenAiModel::with_env_key(&base, model).unwrap();
    let plain = model.chat(&ask()).await.expect("the chat answers");
    assert!(!plain.content.is_empty(), "{plain:?}");
    assert!(plain.usage.input_tokens > 0, "{plain:?}");
    let pieces = std::sync::Mutex::new(String::new());
    let on_delta = |piece: &str| pieces.lock().unwrap().push_str(piece);
    let streamed = model
        .chat_stream(&ask(), &on_delta)
        .await
        .expect("the stream answers");
    assert!(!pieces.into_inner().unwrap().is_empty(), "{streamed:?}");
}

#[tokio::test]
#[ignore = "needs a live OpenAI-compatible server"]
async fn a_live_server_refuses_an_unknown_model_without_a_retry() {
    let (base, _) = live();
    let model = OpenAiModel::with_env_key(&base, "no-such-model-for-this-check").unwrap();
    let error = model.chat(&ask()).await.unwrap_err();
    assert!(!error.kind().is_retryable(), "{error}");
    assert_ne!(error.kind(), ErrorKind::Decode, "{error}");
    println!("{error}");
}
