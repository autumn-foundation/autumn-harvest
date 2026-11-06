//! Strategies in a durable agent and in a mesh, on SQLite.
//!
//! - An op that carries an `OpSample` runs its draws through the core of
//!   `agent_sample`, and the agent keeps the selected answer.
//! - The sampling policy of the kind still applies to the op.
//! - `Mandate::max_draws` and the fan-out of a mandate bound the draws.
//! - Each draw carries the mesh, so a mesh budget stops the next draw.

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
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, OpSample, SampleStrategy, SamplingPolicy, SamplingPolicyStore,
    ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentState, ChatMessage,
    ChatRequest, ChatRole, ContentPart, Mandate, MeshBudget, PeerEnvelope, ScoreContext,
    ScoreInput, Scorer, Verdict, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionId, SqliteReader, SqliteRuntime};
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

/// A model that answers draw `n` with `answers[n]` and a plain turn with
/// `plain`. Each answer costs 5 tokens.
fn by_draw(answers: &'static [&'static str], plain: &'static str) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| match request.draw {
        Some(draw) => answer(answers[draw.get() as usize % answers.len()], 5),
        None => answer(plain, 5),
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

async fn state(rt: &SqliteRuntime, agent: &AgentId) -> AgentState {
    sqlite::state(rt, agent).await.unwrap().unwrap()
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
    let profile = AgentProfile::new(KIND);
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
    assert!(state(&rt, &agent).await.transcript.is_empty());
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

    let op = AgentOp::message("Pick a letter.").with_sample(best_of_three());
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
