//! The recorded overlay read suite (issue #2372).
//!
//! Each test runs on a [`Backend`]. `overlay_read.rs` runs the suite on
//! `SQLite`, and `overlay_read_postgres.rs` runs it on a Postgres worker.
//! The design is in `docs/design/DESIGN-2372.md`.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::testing::{ReplayReport, ReplayStatus};
use autumn_harvest_agent::approval::approval_signal;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentState, BoxFuture,
    ChatMessage, ChatRequest, ChatResponse, ChatRole, ContentPart, FnTool, OverlayVersion,
    ProfileOverlay, ProfileOverlayStore, Provenance, Rule, StopReason, TokenUsage, ToolEffect,
    ToolRules,
};
use serde_json::json;

/// The system text of the first version.
pub const PROMPT_V1: &str = "Prompt of version one.";

/// The system text of the second version.
pub const PROMPT_V2: &str = "Prompt of version two.";

/// The system text of the bare profile.
pub const PROMPT_BASE: &str = "Prompt of the bare profile.";

/// The id of the gated tool call.
pub const CALL_ID: &str = "w1";

/// A model that records the system prompt of each request.
///
/// A user message that starts with `write` gets one call of the `write`
/// tool. A tool result gets the answer `written`. Any other message gets
/// the answer `ok`.
#[derive(Debug, Default)]
pub struct PromptModel {
    prompts: Mutex<Vec<String>>,
    calls: AtomicUsize,
}

impl PromptModel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The system prompt of each request, in order.
    pub fn prompts(&self) -> Vec<String> {
        self.prompts.lock().unwrap().clone()
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn text_of(message: &ChatMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn reply(content: Vec<ContentPart>, stop_reason: StopReason) -> ChatResponse {
    ChatResponse {
        content,
        stop_reason,
        usage: TokenUsage::new(1, 1),
    }
}

impl AgentModel for PromptModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let system = request
            .messages
            .iter()
            .find(|message| message.role == ChatRole::System)
            .map(text_of)
            .unwrap_or_default();
        self.prompts.lock().unwrap().push(system);
        let last = request.messages.last();
        let answer = match last {
            Some(message) if message.role == ChatRole::Tool => reply(
                vec![ContentPart::Text("written".to_owned())],
                StopReason::EndTurn,
            ),
            Some(message) if text_of(message).starts_with("write") => reply(
                vec![ContentPart::ToolCall {
                    id: CALL_ID.to_owned(),
                    name: "write".to_owned(),
                    arguments: json!({}),
                }],
                StopReason::ToolUse,
            ),
            _ => reply(
                vec![ContentPart::Text("ok".to_owned())],
                StopReason::EndTurn,
            ),
        };
        Box::pin(async move { Ok(answer) })
    }
}

/// One way to run a durable agent: an engine and an overlay store.
#[allow(async_fn_in_trait)]
pub trait Backend {
    /// The agent kind of this test. A shared database needs a new kind for
    /// each test, so no test sees the versions of another.
    fn kind(&self) -> String;

    /// The overlay store of the backend, with `validator`.
    fn store(&self, validator: Arc<AgentHarness>) -> Arc<dyn ProfileOverlayStore>;

    /// Serve the agents with `harness`. A second call replaces the harness,
    /// as a deploy does.
    async fn serve(&mut self, harness: AgentHarness);

    /// Send `op` to the agent `id`.
    async fn send(&mut self, id: &AgentId, op: &AgentOp);

    /// Run the agent `id` until its state passes `done`. Returns the state.
    async fn settle(
        &mut self,
        id: &AgentId,
        done: &(dyn Fn(&AgentState) -> bool + Sync),
    ) -> AgentState;

    /// Run the agent `id` until it waits on the approval `signal`.
    async fn wait_approval(&mut self, id: &AgentId, signal: &str);

    /// Approve the call that `signal` names.
    async fn approve(&mut self, id: &AgentId, signal: &str);

    /// A strict replay of each recorded run of the agent `id`.
    async fn replay(&mut self, id: &AgentId) -> Vec<ReplayReport>;

    /// The state that a new process rebuilds from history, with `harness`.
    async fn rebuilt(&mut self, id: &AgentId, harness: AgentHarness) -> AgentState;

    /// The workflow ids whose `agent_profile_version` is `version`.
    async fn listed(&mut self, version: &str) -> Vec<String>;
}

/// The base harness of `kind`: `model`, a gated `write` tool, and a
/// profile with [`PROMPT_BASE`].
pub fn base_harness(kind: &str, model: Arc<PromptModel>) -> AgentHarness {
    let write = FnTool::new("write", "Write.", json!({"type": "object"}), |_| async {
        Ok(json!("saved"))
    })
    .effect(ToolEffect::Write)
    .shared();
    AgentHarness::new(model)
        .tool(write)
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .agent(AgentProfile::new(kind).system(PROMPT_BASE))
}

/// `base` with a store of `backend` that validates with `base`.
pub fn with_store<B: Backend>(
    backend: &B,
    base: AgentHarness,
) -> (AgentHarness, Arc<dyn ProfileOverlayStore>) {
    let store = backend.store(Arc::new(base.clone()));
    (base.overlay_store(Arc::clone(&store)), store)
}

/// An overlay that sets the system prompt to `system`.
pub fn prompt(system: &str) -> ProfileOverlay {
    ProfileOverlay {
        system: Some(system.to_owned()),
        ..ProfileOverlay::default()
    }
}

/// Put `overlay` for `kind` and make it active over `expected`.
pub async fn adopt(
    store: &dyn ProfileOverlayStore,
    kind: &str,
    overlay: &ProfileOverlay,
    expected: Option<OverlayVersion>,
) -> OverlayVersion {
    let version = store
        .put(kind, overlay, Provenance::operator("ops"))
        .await
        .expect("put");
    store
        .activate(kind, version, expected)
        .await
        .expect("activate");
    version
}

fn agent(kind: &str, name: &str) -> AgentId {
    AgentId::new(kind, name).unwrap()
}

/// The approval signal of the first gated call of a new agent.
pub fn first_approval() -> String {
    approval_signal(1, 0, CALL_ID)
}

fn segments(n: u64) -> impl Fn(&AgentState) -> bool + Sync {
    move |state: &AgentState| state.segments >= n
}

fn failures(n: u64) -> impl Fn(&AgentState) -> bool + Sync {
    move |state: &AgentState| state.failures >= n
}

fn assert_all(prompts: &[String], text: &str) {
    assert!(!prompts.is_empty(), "the model was asked");
    for prompt in prompts {
        assert!(prompt.contains(text), "{prompt:?} holds {text:?}");
    }
}

/// AC 1: run op 1 under v1, activate v2, then replay op 1. The replay
/// raises no non-determinism error, and the rebuilt transcript is the
/// original one.
pub async fn replay_is_exact<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model.clone()));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    backend.serve(harness).await;
    let id = agent(&kind, "replay");
    backend.send(&id, &AgentOp::message("hello")).await;
    let original = backend.settle(&id, &segments(1)).await;
    assert_eq!(original.failures, 0, "{:?}", original.last_error);
    assert_all(&model.prompts(), PROMPT_V1);

    adopt(store.as_ref(), &kind, &prompt(PROMPT_V2), Some(v1)).await;
    for report in backend.replay(&id).await {
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    // A new process with v2 active, and a model that must not be asked.
    let fresh = PromptModel::new();
    let (harness, _store) = with_store(backend, base_harness(&kind, fresh.clone()));
    let rebuilt = backend.rebuilt(&id, harness).await;
    assert_eq!(rebuilt.transcript, original.transcript);
    assert_eq!(rebuilt.profile_version, Some(v1));
    assert_eq!(fresh.calls(), 0, "the replay asks no model");
}

/// AC 2: op 1 sees v1's text, and op 2 sees v2's text.
pub async fn the_next_op_sees_v2<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model.clone()));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    backend.serve(harness).await;
    let id = agent(&kind, "next");

    backend.send(&id, &AgentOp::message("one")).await;
    let state = backend.settle(&id, &segments(1)).await;
    assert_eq!(state.profile_version, Some(v1));
    assert_eq!(model.prompts().len(), 1);
    assert_all(&model.prompts(), PROMPT_V1);

    let v2 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V2), Some(v1)).await;
    backend.send(&id, &AgentOp::message("two")).await;
    let state = backend.settle(&id, &segments(2)).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.profile_version, Some(v2));
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[1].contains(PROMPT_V2), "{prompts:?}");
    assert!(!prompts[1].contains(PROMPT_V1), "{prompts:?}");
}

/// AC 3: an activation while op 1 waits on an approval changes no turn
/// of op 1. The next op sees it.
pub async fn a_mid_op_activation_is_ignored<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model.clone()));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    backend.serve(harness).await;
    let id = agent(&kind, "mid-op");

    backend.send(&id, &AgentOp::message("write it")).await;
    let signal = first_approval();
    backend.wait_approval(&id, &signal).await;
    let v2 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V2), Some(v1)).await;
    backend.approve(&id, &signal).await;
    let state = backend.settle(&id, &segments(1)).await;
    assert_eq!(state.last_answer.as_deref(), Some("written"));
    assert_eq!(state.profile_version, Some(v1));
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 2, "two turns: the call and the answer");
    assert_all(&prompts, PROMPT_V1);

    backend.send(&id, &AgentOp::message("again")).await;
    let state = backend.settle(&id, &segments(2)).await;
    assert_eq!(state.profile_version, Some(v2));
    assert!(model.prompts()[2].contains(PROMPT_V2));
}

/// AC 4: `agent_profile_version = 'v2'` lists the agent after op 2, and
/// not after op 1.
pub async fn the_search_attribute_follows_the_op<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    backend.serve(harness).await;
    let id = agent(&kind, "listed");

    backend.send(&id, &AgentOp::message("one")).await;
    backend.settle(&id, &segments(1)).await;
    let v2 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V2), Some(v1)).await;
    assert_eq!(v2.to_string(), "v2");
    let named = |ids: &[String]| ids.iter().any(|listed| listed == id.as_str());
    assert!(named(&backend.listed("v1").await), "op 1 ran on v1");
    assert!(
        !named(&backend.listed("v2").await),
        "an activation alone does not list the agent"
    );

    backend.send(&id, &AgentOp::message("two")).await;
    backend.settle(&id, &segments(2)).await;
    assert!(named(&backend.listed("v2").await), "op 2 ran on v2");
    assert!(!named(&backend.listed("v1").await));
}

/// AC 5: a stored version that no longer validates fails the op. The error
/// names the version, and the agent does not run on the bare profile.
/// The harness of the new deploy no longer holds the model of the version.
pub async fn bad_stored_data_fails_safe<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let named = PromptModel::new();
    let base = |model: Arc<PromptModel>| {
        let harness = base_harness(&kind, model.clone());
        let profile = harness.agent_profile(&kind).unwrap().model("m2");
        harness.agent(profile)
    };
    let old = base(named.clone()).model_named("m2", named.clone());
    let (_harness, store) = with_store(backend, old);
    let overlay = ProfileOverlay {
        model: Some("m2".to_owned()),
        ..prompt(PROMPT_V1)
    };
    let v1 = adopt(store.as_ref(), &kind, &overlay, None).await;

    // The new deploy removed the model `m2`.
    let fallback = PromptModel::new();
    let (harness, _store) = with_store(backend, base(fallback.clone()));
    backend.serve(harness).await;
    let id = agent(&kind, "bad");
    backend.send(&id, &AgentOp::message("hello")).await;
    let state = backend.settle(&id, &failures(1)).await;
    let error = state.last_error.clone().unwrap_or_default();
    assert!(
        error.contains(&v1.to_string()) && error.contains("m2"),
        "the error names the version and the model: {error}"
    );
    assert_eq!(state.profile_version, None);
    assert_eq!(state.transcript, Vec::new(), "the op kept nothing");
    assert_eq!(fallback.calls() + named.calls(), 0, "no model ran");
}
