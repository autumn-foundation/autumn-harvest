//! Shared fixtures: a scripted model, counted tools, and a SQLite runtime.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_wraps
)]

use std::collections::VecDeque;
use std::future::Future;
#[cfg(feature = "sqlite")]
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest_agent::AgentReport;
#[cfg(feature = "sqlite")]
use autumn_harvest_agent::{AgentHarness, sqlite};
#[cfg(feature = "sqlite")]
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use autumn_plugin_agent::hooks::RunInfo;
use autumn_plugin_agent::{
    AgentError, ChatRequest, ChatResponse, ContentPart, ErrorKind, FnTool, LlmClient, StopReason,
    TokenUsage, Tool, ToolCall, ToolDecision, ToolEffect, ToolPolicy,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};

/// One scripted model reply.
pub type Reply = Result<ChatResponse, AgentError>;

/// A model that answers from a fixed script and counts its calls.
#[derive(Debug, Default)]
pub struct ScriptedModel {
    replies: Mutex<VecDeque<Reply>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ChatRequest>>,
}

impl ScriptedModel {
    pub fn new(replies: Vec<Reply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into()),
            ..Self::default()
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> Vec<ChatRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl LlmClient for ScriptedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(AgentError::new(ErrorKind::Provider, "script ran out")));
        Box::pin(async move { reply })
    }

    fn list_models(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, AgentError>> + Send + '_>> {
        Box::pin(async { Ok(vec!["scripted".to_owned()]) })
    }

    fn provider_name(&self) -> &'static str {
        "scripted"
    }
}

/// A final text answer.
pub fn answer(text: &str, tokens: u32) -> Reply {
    Ok(ChatResponse {
        content: vec![ContentPart::Text(text.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(tokens, 0),
    })
}

/// A turn that asks for tool calls.
pub fn calls(list: &[(&str, &str, Value)], tokens: u32) -> Reply {
    Ok(ChatResponse {
        content: list
            .iter()
            .map(|(id, name, arguments)| ContentPart::ToolCall {
                id: (*id).to_owned(),
                name: (*name).to_owned(),
                arguments: arguments.clone(),
            })
            .collect(),
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(tokens, 0),
    })
}

/// A tool that records the arguments of every run.
#[derive(Debug, Default)]
pub struct Recorder {
    runs: Mutex<Vec<Value>>,
}

impl Recorder {
    pub fn runs(&self) -> Vec<Value> {
        self.runs.lock().unwrap().clone()
    }
}

/// A tool named `name` that records its runs and echoes its input.
pub fn recorded_tool(name: &str, effect: ToolEffect, recorder: &Arc<Recorder>) -> Arc<dyn Tool> {
    let recorder = Arc::clone(recorder);
    FnTool::new(
        name,
        "A test tool.",
        json!({"type": "object"}),
        move |input: Value| {
            let recorder = Arc::clone(&recorder);
            async move {
                recorder.runs.lock().unwrap().push(input.clone());
                Ok(json!({"echo": input}))
            }
        },
    )
    .effect(effect)
    .shared()
}

/// A policy that answers one fixed decision and counts how often it is asked.
#[derive(Debug)]
pub struct CountingPolicy {
    decision: ToolDecision,
    asked: AtomicUsize,
}

impl CountingPolicy {
    pub fn new(decision: ToolDecision) -> Arc<Self> {
        Arc::new(Self {
            decision,
            asked: AtomicUsize::new(0),
        })
    }

    pub fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}

impl ToolPolicy for CountingPolicy {
    fn decide<'a>(
        &'a self,
        _call: &'a ToolCall,
        _tool: Option<&'a dyn Tool>,
        _info: &'a RunInfo,
    ) -> BoxFuture<'a, ToolDecision> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        let decision = self.decision.clone();
        Box::pin(async move { decision })
    }
}

#[cfg(feature = "sqlite")]
/// Open `db` and register the agent loop against `harness`.
pub fn runtime(db: &Path, harness: AgentHarness) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    sqlite::register(&mut rt, Arc::new(harness));
    rt
}

#[cfg(feature = "sqlite")]
/// Decode the report of a completed run.
pub fn report(state: RunState) -> AgentReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).expect("the report decodes"),
        other => panic!("expected a completed run, got {other:?}"),
    }
}

/// The text of every tool result the transcript carries, in order.
pub fn tool_results(report: &AgentReport) -> Vec<String> {
    report
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}
