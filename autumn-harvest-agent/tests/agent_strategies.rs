//! Strategies in a durable agent and in a mesh, on SQLite.
//!
//! - An op that carries an `OpSample` runs its draws through the core of
//!   `agent_sample`, and the agent keeps the selected answer.
//! - The sampling policy of the kind still applies to the op.
//! - `Mandate::max_draws` and the fan-out of a mandate bound the draws.
//! - Each draw carries the mesh, so a mesh budget stops the next draw.

#![cfg(feature = "sqlite")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::context_reset::{ContextReset, SAVE_PROGRESS_TOOL};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, OpSample, SampleInput, SampleStrategy, SamplingPolicy,
    SamplingPolicyStore, ScorerId, ScorerRef, Scorers, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentState, AgentStatus,
    ChatMessage, ChatRequest, ChatRole, ContentPart, FiniteF64, Mandate, MeshBudget, PeerEnvelope,
    ScoreContext, ScoreInput, Scorer, Verdict, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionId, RunState, SqliteReader, SqliteRuntime};
use common::{TryFnModel, answer, runtime};
use futures::FutureExt as _;

const KIND: &str = "desk";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn desk(name: &str) -> AgentId {
    AgentId::new(KIND, name).unwrap()
}

/// A policy that allows best-of-N with up to `max` draws.
fn best_of(max: u32) -> SamplingPolicy {
    SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: max,
        ..SamplingPolicy::default()
    }
}

fn store_with(policy: &SamplingPolicy) -> Arc<InMemorySamplingPolicyStore> {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    store
        .set(KIND, policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    store
}

/// A scorer that gives each listed answer its score, and rejects others.
#[derive(Debug)]
struct Table(Vec<(&'static str, f64)>);

impl Scorer for Table {
    fn id(&self) -> &str {
        "table"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let found = self
            .0
            .iter()
            .find(|(answer, _)| *answer == input.answer.trim());
        let verdict = match found {
            Some((_, score)) => Verdict::score(*score),
            None => Ok(Verdict::reject(format!("no score for {:?}", input.answer))),
        };
        Box::pin(std::future::ready(verdict))
    }
}

fn letters() -> Table {
    Table(vec![("a", 0.2), ("b", 0.9), ("c", 0.5)])
}

fn best_of_three() -> OpSample {
    OpSample::new(
        SampleStrategy::best_of_n(3),
        ScorerRef::deterministic("table").unwrap(),
    )
}

/// A model that answers draw `n` with `answers[n % len]` and a plain turn
/// with `plain`. Each answer costs 5 tokens.
fn by_draw(answers: &'static [&'static str], plain: &'static str) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| {
        request.draw.map_or_else(
            || answer(plain, 5),
            |draw| answer(answers[draw.get() as usize % answers.len()], 5),
        )
    })
}

fn draw_calls(model: &TryFnModel) -> usize {
    model
        .requests()
        .iter()
        .filter(|request| request.draw.is_some())
        .count()
}

fn harness(
    model: Arc<dyn AgentModel>,
    profile: AgentProfile,
    policy: &SamplingPolicy,
) -> AgentHarness {
    AgentHarness::new(model)
        .agent(profile)
        .scorer(Arc::new(letters()))
        .sampling_policies(store_with(policy))
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, agent));
    async move { query.await.unwrap().unwrap() }
}

async fn send(rt: &mut SqliteRuntime, agent: &AgentId, op: &AgentOp) {
    sqlite::send(rt, agent, op, None).unwrap();
    sqlite::drive(rt, agent).await.unwrap();
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

fn texts(messages: &[ChatMessage], role: ChatRole) -> Vec<String> {
    messages
        .iter()
        .filter(|message| message.role == role)
        .map(text_of)
        .collect()
}

// ── An op with a strategy ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_best_of_three_op_runs_three_draws_and_keeps_the_selected_answer() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"], "hello");
    let profile = AgentProfile::new(KIND).system("Be brief.");
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("one");

    send(&mut rt, &agent, &AgentOp::message("Say hello.")).await;
    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;

    assert_eq!(draw_calls(&model), 3, "one model call for each draw");
    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.last_answer.as_deref(), Some("b"));
    assert_eq!(state.tokens_used, 5 + 15, "the agent counts each draw");
    assert_eq!(
        texts(&state.transcript, ChatRole::User),
        ["Say hello.", "Pick a letter."]
    );
    assert_eq!(
        texts(&state.transcript, ChatRole::Assistant),
        ["hello", "b"],
        "the transcript keeps the selected answer only"
    );
    // Each draw sees the conversation of the agent.
    for request in model.requests().iter().filter(|r| r.draw.is_some()) {
        let users = texts(&request.messages, ChatRole::User);
        assert_eq!(users, ["Say hello.", "Pick a letter."], "{users:?}");
    }

    // The next op continues from the selected answer.
    send(&mut rt, &agent, &AgentOp::message("Thanks.")).await;
    let last = model.requests().last().cloned().unwrap();
    assert!(last.draw.is_none());
    assert_eq!(
        texts(&last.messages, ChatRole::Assistant),
        ["hello", "b"],
        "the plain op reads the selected answer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_strategy_that_the_policy_does_not_list_is_refused_and_the_agent_stays_live() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Rejection]),
        max_draws: 3,
        ..SamplingPolicy::default()
    };
    let mut rt = runtime(
        &db,
        harness(model.clone(), AgentProfile::new(KIND), &policy),
    );
    let agent = desk("two");

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;
    assert!(
        model.requests().is_empty(),
        "no model call before a refusal"
    );
    let refused = state(&rt, &agent).await;
    assert_eq!(refused.failures, 1);
    let error = refused.last_error.clone().unwrap();
    assert!(error.contains("strategy_not_allowed"), "{error}");
    assert!(refused.transcript.is_empty(), "a refusal keeps no turn");

    send(&mut rt, &agent, &AgentOp::message("Hello?")).await;
    let live = state(&rt, &agent).await;
    assert_eq!(live.failures, 1, "the next op runs");
    assert_eq!(live.last_answer.as_deref(), Some("plain"));
    assert_eq!(model.requests().len(), 1);
}

// ── The mandate bounds the draws ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_mandate_with_two_draws_refuses_best_of_three() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).max_draws(2));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("three");

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;
    assert!(model.requests().is_empty(), "no draw runs");
    let error = state(&rt, &agent).await.last_error.unwrap();
    assert!(error.contains("draws_exceeded"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_draw_counts_against_the_fan_out_of_the_mandate() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).max_delegations(2));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("four");

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;
    assert!(model.requests().is_empty(), "no draw runs");
    let error = state(&rt, &agent).await.last_error.unwrap();
    assert!(error.contains("fan_out_exhausted"), "{error}");
}

/// The payloads of the external signals that `agent` sent.
fn sent_signals(rt: &SqliteRuntime, agent: &AgentId) -> Vec<serde_json::Value> {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ExternalSignalRequested { payload, .. } => Some(payload),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_under_a_mandate_with_two_draws_gets_draws_exceeded() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("five");

    let envelope = PeerEnvelope {
        from: "triage/x@life".into(),
        sender: "triage".into(),
        seq: 1,
        text: "Pick a letter.".into(),
        mandate: Mandate::new("triage").max_draws(2),
        flow_labels: BTreeSet::new(),
        reply_to: Some(format!("{}/reply-1", ExecutionId::new())),
        waiting: Vec::new(),
        mesh: None,
        sample: Some(best_of_three()),
    };
    send(&mut rt, &agent, &AgentOp::peer(envelope)).await;

    assert!(model.requests().is_empty(), "no draw runs");
    let replies = sent_signals(&rt, &agent);
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert_eq!(replies[0]["outcome"], "resolved");
    assert_eq!(replies[0]["value"]["refusal"], "draws_exceeded");
    let kept = state(&rt, &agent).await.transcript;
    assert!(kept.is_empty(), "a refusal keeps no turn: {kept:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_op_with_room_for_its_draws_replies_with_the_selected_answer() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("six");

    let envelope = PeerEnvelope {
        from: "triage/x@life".into(),
        sender: "triage".into(),
        seq: 1,
        text: "Pick a letter.".into(),
        mandate: Mandate::new("triage").max_draws(3).max_delegations(5),
        flow_labels: BTreeSet::from(["untrusted_input".to_owned()]),
        reply_to: Some(format!("{}/reply-1", ExecutionId::new())),
        waiting: Vec::new(),
        mesh: None,
        sample: Some(best_of_three()),
    };
    send(&mut rt, &agent, &AgentOp::peer(envelope)).await;

    assert_eq!(draw_calls(&model), 3);
    let replies = sent_signals(&rt, &agent);
    assert_eq!(replies.len(), 1, "{replies:?}");
    let reply = &replies[0]["value"];
    assert_eq!(reply["answer"], "b");
    assert_eq!(reply["stop"], "completed");
    assert_eq!(reply["tokens"], 15);
    assert!(reply.get("refusal").is_none_or(serde_json::Value::is_null));
    let labels = reply["flow_labels"].as_array().unwrap();
    assert!(labels.iter().any(|label| label == "untrusted_input"));
}

// ── The mesh budget ─────────────────────────────────────────────────────────

/// The ledger rows of every run in `mesh`.
fn mesh_ledger_rows(db: &Path, mesh: &str) -> usize {
    let reader = SqliteReader::open(db).unwrap();
    let filter = ExecutionFilter::new().search_attribute(
        autumn_harvest_agent::mesh_control::MESH_ATTR,
        serde_json::json!(mesh),
    );
    let runs: std::collections::HashSet<ExecutionId> = reader
        .list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .collect();
    let since = autumn_harvest::chrono::DateTime::<autumn_harvest::chrono::Utc>::MIN_UTC;
    let until = autumn_harvest::chrono::DateTime::<autumn_harvest::chrono::Utc>::MAX_UTC;
    reader
        .llm_ledger_between(since, until)
        .unwrap()
        .iter()
        .filter(|row| runs.contains(&row.exec_id))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mesh_budget_that_admits_two_draws_stops_the_third() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"], "plain");
    let profile = AgentProfile::new(KIND).mesh("support");
    let harness = harness(model.clone(), profile, &best_of(3))
        .mesh_budget(MeshBudget::daily("support").max_tokens(10))
        .mesh_spend(Arc::new(sqlite::LedgerSpend::open(&db)));
    let mut rt = runtime(&db, harness);
    let agent = desk("seven");

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three().concurrency(1));
    send(&mut rt, &agent, &op).await;

    assert_eq!(draw_calls(&model), 2, "the budget stops the third draw");
    assert_eq!(
        mesh_ledger_rows(&db, "support"),
        2,
        "the ledger shows 2 draws"
    );
    let state = state(&rt, &agent).await;
    assert_eq!(state.tokens_used, 10);
    assert_eq!(state.last_answer.as_deref(), Some("b"), "the best of two");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spent_mesh_budget_parks_the_agent_and_holds_the_strategy_op() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request: &ChatRequest| {
        answer(if request.draw.is_some() { "b" } else { "spend" }, 12)
    });
    let profile = AgentProfile::new(KIND).mesh("support");
    let harness = harness(model.clone(), profile, &best_of(3))
        .mesh_budget(MeshBudget::daily("support").max_tokens(10))
        .mesh_spend(Arc::new(sqlite::LedgerSpend::open(&db)));
    let mut rt = runtime(&db, harness);
    let agent = desk("eight");

    send(&mut rt, &agent, &AgentOp::message("spend")).await;
    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;

    assert_eq!(draw_calls(&model), 0, "the budget refuses each draw");
    let state = state(&rt, &agent).await;
    assert!(
        matches!(state.status, AgentStatus::Parked { .. }),
        "{:?}",
        state.status
    );
    assert_eq!(state.failures, 0, "a refusal of the mesh is not a failure");
    assert_eq!(state.held.len(), 1, "the op waits for the next period");
    assert_eq!(state.held[0], op);
    assert_eq!(
        texts(&state.transcript, ChatRole::User),
        ["spend"],
        "the transcript stays as it was"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejection_op_stops_at_the_first_pass() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"], "plain");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Rejection]),
        max_draws: 3,
        ..SamplingPolicy::default()
    };
    let mut rt = runtime(
        &db,
        harness(model.clone(), AgentProfile::new(KIND), &policy),
    );
    let agent = desk("nine");

    let strategy = SampleStrategy::rejection(3, FiniteF64::new(0.8).unwrap());
    let sample = OpSample::new(strategy, ScorerRef::deterministic("table").unwrap());
    send(
        &mut rt,
        &agent,
        &AgentOp::message("Pick.").with_sample(sample),
    )
    .await;

    assert_eq!(draw_calls(&model), 2, "draw 1 passes, so draw 2 never runs");
    assert_eq!(state(&rt, &agent).await.last_answer.as_deref(), Some("b"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_op_that_the_policy_refuses_replies_with_the_error() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND));
    let mut rt = runtime(
        &db,
        harness(model.clone(), profile, &SamplingPolicy::default()),
    );
    let agent = desk("ten");

    let envelope = PeerEnvelope {
        from: "triage/x@life".into(),
        sender: "triage".into(),
        seq: 1,
        text: "Pick a letter.".into(),
        mandate: Mandate::new("triage"),
        flow_labels: BTreeSet::new(),
        reply_to: Some(format!("{}/reply-1", ExecutionId::new())),
        waiting: Vec::new(),
        mesh: None,
        sample: Some(best_of_three()),
    };
    send(&mut rt, &agent, &AgentOp::peer(envelope)).await;

    assert!(model.requests().is_empty(), "no draw runs");
    let replies = sent_signals(&rt, &agent);
    assert_eq!(replies.len(), 1, "{replies:?}");
    let reply = &replies[0]["value"];
    assert!(reply["refusal"].is_null(), "{reply}");
    let error = reply["error"].as_str().unwrap();
    assert!(error.contains("strategy_not_allowed"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_judge_model_that_the_mandate_forbids_is_refused() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 3,
        scorers: Scorers::Only(BTreeSet::from([ScorerId::new("table").unwrap()])),
        judge_models: BTreeSet::from(["big".to_owned()]),
        ..SamplingPolicy::default()
    };
    let mandate = Mandate::new(KIND).models([autumn_harvest_agent::DEFAULT_MODEL]);
    let profile = AgentProfile::new(KIND).mandate(mandate);
    let mut rt = runtime(&db, harness(model.clone(), profile, &policy));
    let agent = desk("eleven");

    let judge = ScorerRef::judge("table", "big").unwrap();
    let sample = OpSample::new(SampleStrategy::best_of_n(2), judge);
    send(
        &mut rt,
        &agent,
        &AgentOp::message("Pick.").with_sample(sample),
    )
    .await;

    assert!(model.requests().is_empty(), "no draw runs");
    let error = state(&rt, &agent).await.last_error.unwrap();
    assert!(error.contains("model_forbidden"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_sample_refuses_draws_over_the_profile_mandate() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a"], "plain");
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).max_draws(2));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));

    let input = SampleInput::new(
        KIND,
        "Pick.",
        SampleStrategy::best_of_n(3),
        ScorerRef::deterministic("table").unwrap(),
    );
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    match rt.run_until_blocked(exec).await.unwrap() {
        RunState::Failed(error) => assert!(error.contains("draws_exceeded"), "{error}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(model.requests().is_empty(), "no draw runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transcript_too_large_for_a_draw_is_refused_before_any_draw() {
    let (_dir, db) = fresh_db();
    let big: &'static str = Box::leak("x".repeat(1_200_000).into_boxed_str());
    let model = TryFnModel::new(move |request: &ChatRequest| {
        answer(if request.draw.is_some() { "a" } else { big }, 5)
    });
    let mut rt = runtime(
        &db,
        harness(model.clone(), AgentProfile::new(KIND), &best_of(3)),
    );
    let agent = desk("twelve");

    send(&mut rt, &agent, &AgentOp::message("one")).await;
    send(&mut rt, &agent, &AgentOp::message("two")).await;
    assert_eq!(state(&rt, &agent).await.failures, 0);
    let op = AgentOp::message("Pick.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;

    assert_eq!(draw_calls(&model), 0, "no draw runs");
    let state = state(&rt, &agent).await;
    let error = state.last_error.unwrap();
    assert!(error.contains("too large"), "{error}");
    assert_eq!(state.failures, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draw_of_a_strategy_op_gets_no_context_reset() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["a", "b", "c"], "plain");
    let policy = SamplingPolicy {
        draw_effects: BTreeSet::from([
            autumn_harvest_agent::ToolEffect::ReadOnly,
            autumn_harvest_agent::ToolEffect::Internal,
        ]),
        ..best_of(3)
    };
    let profile = AgentProfile::new(KIND).context_reset(ContextReset::new(1, 90));
    let mut rt = runtime(&db, harness(model.clone(), profile, &policy));
    let agent = desk("thirteen");

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;

    assert_eq!(draw_calls(&model), 3);
    for request in model.requests().iter().filter(|r| r.draw.is_some()) {
        assert!(
            request
                .tools
                .iter()
                .all(|tool| tool.name != SAVE_PROGRESS_TOOL),
            "a draw keeps no progress notes"
        );
    }
    let state = state(&rt, &agent).await;
    assert_eq!(state.last_answer.as_deref(), Some("b"));
    assert!(state.notes.is_none(), "{:?}", state.notes);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_beam_op_checks_max_draws_but_not_the_fan_out() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["b"], "plain");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Beam]),
        max_draws: 8,
        ..SamplingPolicy::default()
    };
    // Three steps under one delegation: a beam keeps its own count.
    let roomy = Mandate::new(KIND).max_delegations(1).max_draws(3);
    let profile = AgentProfile::new(KIND).mandate(roomy);
    let mut rt = runtime(&db, harness(model.clone(), profile, &policy));
    let agent = desk("fourteen");
    let beam = OpSample::new(
        SampleStrategy::beam(1, 1, 3),
        ScorerRef::deterministic("table").unwrap(),
    );
    send(
        &mut rt,
        &agent,
        &AgentOp::message("Pick.").with_sample(beam),
    )
    .await;
    let roomy_state = state(&rt, &agent).await;
    assert_eq!(roomy_state.failures, 0, "{:?}", roomy_state.last_error);
    assert_eq!(roomy_state.last_answer.as_deref(), Some("b"));

    // The same beam passes a cap of two draws.
    let (_dir2, db2) = fresh_db();
    let model = by_draw(&["b"], "plain");
    let tight = Mandate::new(KIND).max_draws(2);
    let profile = AgentProfile::new(KIND).mandate(tight);
    let mut rt = runtime(&db2, harness(model.clone(), profile, &policy));
    let beam = OpSample::new(
        SampleStrategy::beam(1, 1, 3),
        ScorerRef::deterministic("table").unwrap(),
    );
    send(
        &mut rt,
        &agent,
        &AgentOp::message("Pick.").with_sample(beam),
    )
    .await;
    assert!(model.requests().is_empty(), "no step runs");
    let error = state(&rt, &agent).await.last_error.unwrap_or_default();
    assert!(error.contains("draws_exceeded"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draw_reads_the_progress_notes_of_the_agent() {
    let (_dir, db) = fresh_db();
    let model = TryFnModel::new(|request: &ChatRequest| {
        if let Some(draw) = request.draw {
            return answer(["a", "b", "c"][draw.get() as usize % 3], 5);
        }
        let saved = request
            .messages
            .iter()
            .any(|message| message.role == ChatRole::Tool);
        if saved {
            answer("saved", 5)
        } else {
            common::calls(
                &[(
                    "s1",
                    SAVE_PROGRESS_TOOL,
                    serde_json::json!({"notes": "customer id 42"}),
                )],
                5,
            )
        }
    });
    let profile = AgentProfile::new(KIND)
        .window(4)
        .context_reset(ContextReset::new(1, 90));
    let mut rt = runtime(&db, harness(model.clone(), profile, &best_of(3)));
    let agent = desk("fifteen");

    send(&mut rt, &agent, &AgentOp::message("Remember my id.")).await;
    send(&mut rt, &agent, &AgentOp::message("Hi.")).await;
    let window = serde_json::json!(state(&rt, &agent).await.transcript).to_string();
    assert!(
        !window.contains("customer id 42"),
        "the window dropped op 1, so only the notes hold the id: {window}"
    );
    let before = state(&rt, &agent).await.notes;
    assert_eq!(
        before.as_ref().map(|notes| notes.text.as_str()),
        Some("customer id 42")
    );
    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
    send(&mut rt, &agent, &op).await;

    let draws: Vec<ChatRequest> = model
        .requests()
        .into_iter()
        .filter(|request| request.draw.is_some())
        .collect();
    assert_eq!(draws.len(), 3);
    for request in &draws {
        let text = serde_json::json!(request.messages).to_string();
        assert!(
            text.contains("customer id 42"),
            "a draw reads the notes: {text}"
        );
    }
    let after = state(&rt, &agent).await;
    assert_eq!(after.failures, 0, "{:?}", after.last_error);
    assert_eq!(after.notes, before, "a draw changes no notes");
    assert_eq!(after.last_answer.as_deref(), Some("b"));
    let kept = serde_json::json!(after.transcript).to_string();
    assert!(
        !kept.contains(autumn_harvest_agent::context_reset::RESTORED_CALL_ID),
        "the transcript keeps no restored copy: {kept}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_tier_of_a_cascade_op_counts_against_the_fan_out() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["b"], "plain");
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Cascade]),
        max_draws: 2,
        cascade_models: BTreeSet::from(["cheap".to_owned(), "strong".to_owned()]),
        ..SamplingPolicy::default()
    };
    let profile = AgentProfile::new(KIND).mandate(Mandate::new(KIND).max_delegations(1));
    let harness = AgentHarness::new(model.clone())
        .model_named("cheap", model.clone())
        .model_named("strong", model.clone())
        .agent(profile)
        .scorer(Arc::new(letters()))
        .sampling_policies(store_with(&policy));
    let mut rt = runtime(&db, harness);
    let agent = desk("seventeen");

    let pass = FiniteF64::new(0.8).unwrap();
    let cascade = SampleStrategy::cascade([
        autumn_harvest_agent::sampling::Tier::new("cheap", pass),
        autumn_harvest_agent::sampling::Tier::new("strong", pass),
    ]);
    let sample = OpSample::new(cascade, ScorerRef::deterministic("table").unwrap());
    send(
        &mut rt,
        &agent,
        &AgentOp::message("Pick.").with_sample(sample),
    )
    .await;

    assert!(model.requests().is_empty(), "no tier runs");
    let error = state(&rt, &agent).await.last_error.unwrap_or_default();
    assert!(error.contains("fan_out_exhausted"), "{error}");
}

/// A peer message that asks for a cascade under `mandate`.
fn cascade_envelope(mandate: Mandate, models: &[&str]) -> PeerEnvelope {
    let pass = FiniteF64::new(0.8).unwrap();
    let tiers = models
        .iter()
        .map(|model| autumn_harvest_agent::sampling::Tier::new(*model, pass));
    let sample = OpSample::new(
        SampleStrategy::cascade(tiers),
        ScorerRef::deterministic("table").unwrap(),
    );
    PeerEnvelope {
        from: "triage/x@life".into(),
        sender: "triage".into(),
        seq: 1,
        text: "Pick a letter.".into(),
        mandate,
        flow_labels: BTreeSet::new(),
        reply_to: Some(format!("{}/reply-1", ExecutionId::new())),
        waiting: Vec::new(),
        mesh: None,
        sample: Some(sample),
    }
}

fn cascade_harness(model: &Arc<TryFnModel>) -> AgentHarness {
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Cascade]),
        max_draws: 2,
        cascade_models: BTreeSet::from(["cheap".to_owned(), "strong".to_owned()]),
        ..SamplingPolicy::default()
    };
    AgentHarness::new(model.clone())
        .model_named("cheap", model.clone())
        .model_named("strong", model.clone())
        .agent(AgentProfile::new(KIND).mandate(Mandate::new(KIND)))
        .scorer(Arc::new(letters()))
        .sampling_policies(store_with(&policy))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_peer_op_checks_the_tier_models_not_the_profile_model() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["b"], "plain");
    let mut rt = runtime(&db, cascade_harness(&model));
    let agent = desk("eighteen");

    // The caller allows the cheap tier only, not the default profile model.
    let mandate = Mandate::new("triage").models(["cheap"]);
    let envelope = cascade_envelope(mandate, &["cheap", "strong"]);
    send(&mut rt, &agent, &AgentOp::peer(envelope)).await;

    let replies = sent_signals(&rt, &agent);
    assert_eq!(replies.len(), 1, "{replies:?}");
    let reply = &replies[0]["value"];
    assert!(reply["refusal"].is_null(), "{reply}");
    assert_eq!(reply["answer"], "b");
    assert_eq!(draw_calls(&model), 1, "only the cheap tier runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_peer_op_with_no_allowed_tier_is_refused() {
    let (_dir, db) = fresh_db();
    let model = by_draw(&["b"], "plain");
    let mut rt = runtime(&db, cascade_harness(&model));
    let agent = desk("nineteen");

    let mandate = Mandate::new("triage").models(["other"]);
    let envelope = cascade_envelope(mandate, &["cheap", "strong"]);
    send(&mut rt, &agent, &AgentOp::peer(envelope)).await;

    assert!(model.requests().is_empty(), "no tier runs");
    let replies = sent_signals(&rt, &agent);
    assert_eq!(replies[0]["value"]["refusal"], "model_forbidden");
}
