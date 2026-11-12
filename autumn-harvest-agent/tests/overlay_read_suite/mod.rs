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

use autumn_harvest::event::WorkflowEvent;
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

const fn reply(content: Vec<ContentPart>, stop_reason: StopReason) -> ChatResponse {
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

    /// Delete the agent `id`, and run it until its run ends.
    async fn delete(&mut self, id: &AgentId);

    /// A strict replay of each recorded run of the agent `id`. The replay
    /// holds `harness` as state, so a read outside history would see it.
    async fn replay(&mut self, id: &AgentId, harness: AgentHarness) -> Vec<ReplayReport>;

    /// Every recorded event of the agent `id`, oldest run first.
    async fn history(&mut self, id: &AgentId) -> Vec<WorkflowEvent>;

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
        .activate(kind, version, expected, Provenance::operator("ops"))
        .await
        .expect("activate");
    version
}

fn agent(kind: &str, name: &str) -> AgentId {
    AgentId::new(kind, name).unwrap()
}

/// The approval signal of the first gated call of a new agent.
pub fn first_approval() -> String {
    approval_signal(0, 0, CALL_ID)
}

/// The input and the result of each recorded activity named `name`.
pub fn recorded(
    events: &[WorkflowEvent],
    name: &str,
) -> Vec<(serde_json::Value, serde_json::Value)> {
    let mut inputs = std::collections::HashMap::new();
    let mut out = Vec::new();
    for event in events {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: scheduled,
                input,
                ..
            } if scheduled == name => {
                inputs.insert(*activity_id, input.clone());
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } => {
                if let Some(input) = inputs.remove(activity_id) {
                    out.push((input, output.clone()));
                }
            }
            _ => {}
        }
    }
    out
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

/// AC 1: run op 1 under v1, activate v2, then replay op 1. The rebuilt
/// transcript is the original one, and the replay raises no
/// non-determinism error.
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

    // A new process with v2 active, and a model that must not be asked.
    let fresh = PromptModel::new();
    let (harness, _store) = with_store(backend, base_harness(&kind, fresh.clone()));
    let rebuilt = backend.rebuilt(&id, harness).await;
    assert_eq!(rebuilt.transcript, original.transcript);
    assert_eq!(rebuilt.profile_version, Some(v1));
    assert_eq!(fresh.calls(), 0, "the replay asks no model");

    // History holds the op read and the turn that used it.
    let events = backend.history(&id).await;
    let reads = recorded(&events, "agent_profile_overlay");
    assert_eq!(reads.len(), 1, "one op read");
    assert_eq!(reads[0].1["overlay_version"], json!(v1.get()));
    let turns = recorded(&events, "agent_model_turn");
    assert_eq!(turns.len(), 1);
    assert!(turns[0].0.to_string().contains(PROMPT_V1), "{}", turns[0].0);

    // A strict replay needs a whole history, so the run ends first. The
    // delete op runs no segment, so it reads no profile. The replay holds
    // the v2 harness, so a read outside history would change the turn input.
    backend.delete(&id).await;
    let (harness, _store) = with_store(backend, base_harness(&kind, PromptModel::new()));
    for report in backend.replay(&id, harness).await {
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }
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
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 3);
    assert!(prompts[2].contains(PROMPT_V2), "{prompts:?}");
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
    assert!(listed_under(backend, "v1", &id).await, "op 1 ran on v1");
    assert!(
        !named(&backend.listed("v2").await),
        "an activation alone does not list the agent"
    );

    backend.send(&id, &AgentOp::message("two")).await;
    backend.settle(&id, &segments(2)).await;
    assert!(listed_under(backend, "v2", &id).await, "op 2 ran on v2");
    assert!(!named(&backend.listed("v1").await));
}

/// Whether the agent `id` lists under `version` within a few seconds.
///
/// `settle` reads the state that a query replays. The agent writes its
/// search attributes in a later step of the same op. So a listing read at
/// once can run ahead of that write.
async fn listed_under<B: Backend>(backend: &mut B, version: &str, id: &AgentId) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if backend
            .listed(version)
            .await
            .iter()
            .any(|listed| listed == id.as_str())
        {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// AC 5: a stored version that no longer validates fails the op. The error
/// names the version, and the agent does not run on the bare profile.
/// The harness of the new deploy no longer holds the model of the version.
pub async fn bad_stored_data_fails_safe<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let named = PromptModel::new();
    let base = |model: Arc<PromptModel>| {
        let harness = base_harness(&kind, model);
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
        error.contains(&format!("the overlay {v1}")) && error.contains("m2"),
        "the error names the version and the model: {error}"
    );
    assert_eq!(state.profile_version, None);
    assert_eq!(state.transcript, Vec::new(), "the op kept nothing");
    assert_eq!(fallback.calls() + named.calls(), 0, "no model ran");
}

/// AC 5, second case: the bare profile could run here. A deploy lowers
/// `max_steps` below the stored version, so the version no longer
/// validates. The op still fails, and no model runs.
pub async fn a_narrowed_deploy_fails_safe<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let old = PromptModel::new();
    let wide = base_harness(&kind, old.clone());
    let profile = wide.agent_profile(&kind).unwrap().max_steps(20);
    let (_harness, store) = with_store(backend, wide.agent(profile.clone()));
    let overlay = ProfileOverlay {
        max_steps: Some(15),
        ..prompt(PROMPT_V1)
    };
    let v1 = adopt(store.as_ref(), &kind, &overlay, None).await;

    let fallback = PromptModel::new();
    let narrow = base_harness(&kind, fallback.clone());
    let narrow = narrow.agent(profile.max_steps(10));
    let (harness, _store) = with_store(backend, narrow);
    backend.serve(harness).await;
    let id = agent(&kind, "narrowed");
    backend.send(&id, &AgentOp::message("hello")).await;
    let state = backend.settle(&id, &failures(1)).await;
    let error = state.last_error.unwrap_or_default();
    assert!(
        error.contains(&format!("the overlay {v1}")) && error.contains("max_steps"),
        "the error names the version and the bound: {error}"
    );
    assert_eq!(fallback.calls() + old.calls(), 0, "no model ran bare");
}

/// Issue #2374: `rollback` after v1 then v2 makes v1 active. The next op
/// sends v1's prompt to the model.
pub async fn a_rollback_restores_v1<B: Backend>(backend: &mut B) {
    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model.clone()));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    let v2 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V2), Some(v1)).await;
    backend.serve(harness).await;
    let id = agent(&kind, "rollback");

    backend.send(&id, &AgentOp::message("one")).await;
    let state = backend.settle(&id, &segments(1)).await;
    assert_eq!(state.profile_version, Some(v2));

    let back = store
        .rollback(&kind, Provenance::operator("oncall"))
        .await
        .expect("rollback");
    assert_eq!((back.from, back.to), (v2, v1));
    backend.send(&id, &AgentOp::message("two")).await;
    let state = backend.settle(&id, &segments(2)).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.profile_version, Some(v1));
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[0].contains(PROMPT_V2), "{prompts:?}");
    assert!(prompts[1].contains(PROMPT_V1), "{prompts:?}");
    assert!(!prompts[1].contains(PROMPT_V2), "{prompts:?}");
}

/// Issue #2394: `ceilings.max_steps = 40`, over a profile of 20, lets an
/// overlay with 30 steps run. The op reads the policy with the overlay.
/// After a reset, the next op refuses that version and runs no model.
pub async fn a_policy_ceiling_reaches_the_op_read<B: Backend>(backend: &mut B) {
    use autumn_harvest_agent::RefinePolicy;

    let kind = backend.kind();
    let model = PromptModel::new();
    let base = base_harness(&kind, model.clone());
    let profile = base.agent_profile(&kind).unwrap().max_steps(20);
    let (harness, store) = with_store(backend, base.agent(profile));
    let wide: RefinePolicy =
        serde_json::from_value(json!({"ceilings": {"max_steps": 40}})).unwrap();
    store
        .set_policy(&kind, &wide, None, Provenance::operator("ops"))
        .await
        .expect("set");
    let overlay = ProfileOverlay {
        max_steps: Some(30),
        ..prompt(PROMPT_V1)
    };
    let v1 = adopt(store.as_ref(), &kind, &overlay, None).await;
    backend.serve(harness).await;
    let id = agent(&kind, "ceiling");

    backend.send(&id, &AgentOp::message("one")).await;
    let state = backend.settle(&id, &segments(1)).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.profile_version, Some(v1));
    assert_all(&model.prompts(), PROMPT_V1);

    store
        .reset_policy(&kind, Provenance::operator("ops"))
        .await
        .expect("reset");
    backend.send(&id, &AgentOp::message("two")).await;
    let state = backend.settle(&id, &failures(1)).await;
    let error = state.last_error.unwrap_or_default();
    assert!(
        error.contains(&format!("the overlay {v1}")) && error.contains("max_steps"),
        "the error names the version and the bound: {error}"
    );
    assert_eq!(model.calls(), 1, "no model ran after the reset");
}

/// Issue #2394: `rollback` after an auto adoption restores the previous
/// version, and the next op sees its prompt.
pub async fn a_rollback_after_an_auto_adoption_restores_v1<B: Backend>(backend: &mut B) {
    use autumn_harvest::ExecutionId;
    use autumn_harvest_agent::{AutoAdoptOutcome, RefinePolicy};

    let kind = backend.kind();
    let model = PromptModel::new();
    let (harness, store) = with_store(backend, base_harness(&kind, model.clone()));
    let v1 = adopt(store.as_ref(), &kind, &prompt(PROMPT_V1), None).await;
    let allow: RefinePolicy =
        serde_json::from_value(json!({"auto_adopt": {"parts": ["system"]}})).unwrap();
    store
        .set_policy(&kind, &allow, None, Provenance::operator("ops"))
        .await
        .expect("set");
    let run = ExecutionId::new();
    let outcome = store
        .auto_adopt(&kind, &prompt(PROMPT_V2), run, Some(v1), &allow)
        .await
        .expect("auto adopt");
    let AutoAdoptOutcome::Adopted(v2) = outcome else {
        panic!("want Adopted, got {outcome:?}");
    };
    backend.serve(harness).await;
    let id = agent(&kind, "auto-rollback");

    backend.send(&id, &AgentOp::message("one")).await;
    let state = backend.settle(&id, &segments(1)).await;
    assert_eq!(state.profile_version, Some(v2));

    let back = store
        .rollback(&kind, Provenance::operator("oncall"))
        .await
        .expect("rollback");
    assert_eq!((back.from, back.to), (v2, v1));
    backend.send(&id, &AgentOp::message("two")).await;
    let state = backend.settle(&id, &segments(2)).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.profile_version, Some(v1));
    let prompts = model.prompts();
    assert_eq!(prompts.len(), 2);
    assert!(prompts[0].contains(PROMPT_V2), "{prompts:?}");
    assert!(prompts[1].contains(PROMPT_V1), "{prompts:?}");
}
