//! The model-turn span names the acting agent (mesh AC14).
//!
//! This test has its own binary because `set_default` covers one thread only.
//! Tracing caches the interest of each span callsite for the whole process. In
//! the `mesh` binary, a parallel test on a thread with no subscriber could
//! register `agent_model_turn` first and cache it as never enabled. The span
//! was then never built, and the test saw no fields.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use autumn_harvest_agent::{AgentHarness, ChatRole, ModelTurnRequest, TokenUsage};
use common::{ScriptedModel, answer};

fn turn_request() -> ModelTurnRequest {
    ModelTurnRequest {
        run_id: "r".into(),
        session_id: None,
        steps_used: 0,
        max_steps: 8,
        usage: TokenUsage::default(),
        messages: vec![autumn_harvest_agent::ChatMessage::text(
            ChatRole::User,
            "go",
        )],
        max_output_tokens: None,
        read_only: false,
        memory_scope: None,
        extra_tools: Vec::new(),
        tenant: None,
        agent: None,
        allowed_tools: None,
        flow_labels: BTreeSet::new(),
        flow_released: autumn_harvest_agent::Released::new(),
        interruptible: false,
        model: None,
        stream: false,
        mesh: None,
        prefix: None,
        remember: false,
    }
}

/// A tracing subscriber that keeps the name and the fields of each span.
#[derive(Clone, Default)]
struct Spans(Arc<std::sync::Mutex<Vec<(String, String, String)>>>);

struct Fields<'a>(&'a mut Vec<(String, String)>);

impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }
}

impl Spans {
    fn keep(&self, span: &str, record: &dyn Fn(&mut Fields<'_>)) {
        let mut fields = Vec::new();
        record(&mut Fields(&mut fields));
        let mut kept = self.0.lock().unwrap();
        for (name, value) in fields {
            kept.push((span.to_owned(), name, value));
        }
    }
}

impl tracing::Subscriber for Spans {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::always()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let name = span.metadata().name();
        self.keep(name, &|fields| span.record(fields));
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        self.keep("agent_model_turn", &|fields| values.record(fields));
    }

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

// AC14: the model-turn span names the acting agent.
#[tokio::test]
async fn the_model_turn_span_names_the_agent() {
    let spans = Spans::default();
    let _guard = tracing::subscriber::set_default(spans.clone());
    let harness = AgentHarness::new(ScriptedModel::new(vec![answer("hi", 1)]));
    let request = ModelTurnRequest {
        agent: Some("researcher".into()),
        ..turn_request()
    };
    harness.model_turn(request).await.unwrap();
    let kept = spans.0.lock().unwrap().clone();
    assert!(
        kept.contains(&(
            "agent_model_turn".to_owned(),
            "gen_ai.agent.name".to_owned(),
            "researcher".to_owned()
        )),
        "{kept:?}"
    );
}
