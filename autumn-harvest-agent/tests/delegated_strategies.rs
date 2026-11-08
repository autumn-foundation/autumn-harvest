//! A delegating model asks a durable peer for a strategy, on SQLite.
//!
//! - The `delegate` tool takes an optional `sample`, an `OpSample`.
//! - The caller checks the draws against its own `max_draws` and its
//!   fan-out before it sends anything.
//! - An inline peer refuses a strategy, because no sampling policy covers
//!   it.

#![cfg(feature = "sqlite")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::mesh::{DELEGATE_TOOL, DelegationRecord};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, OpSample, SampleStrategy, SamplingPolicy, SamplingPolicyStore,
    ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentOp, AgentProfile, AgentReport, AgentTask, ChatRequest,
    ChatRole, ContentPart, Control, Mandate, Peer, PeerEnvelope, ScoreContext, ScoreInput, Scorer,
    Verdict, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{TryFnModel, answer, calls, report, runtime};
use futures::FutureExt as _;
use serde_json::{Value, json};

const RECEIVER_KIND: &str = "research";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn research() -> AgentId {
    AgentId::new(RECEIVER_KIND, "r1").unwrap()
}

/// A scorer that gives each listed answer its score, and rejects others.
#[derive(Debug)]
struct Table;

impl Scorer for Table {
    fn id(&self) -> &str {
        "table"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let verdict = match input.answer.trim() {
            "a" => Verdict::score(0.2),
            "b" => Verdict::score(0.9),
            "c" => Verdict::score(0.5),
            other => Ok(Verdict::reject(format!("no score for {other:?}"))),
        };
        Box::pin(std::future::ready(verdict))
    }
}

fn best_of_three() -> OpSample {
    OpSample::new(
        SampleStrategy::best_of_n(3),
        ScorerRef::deterministic("table").unwrap(),
    )
}

/// The policy of the receiver kind: best-of-N with up to 3 draws.
fn policies() -> Arc<InMemorySamplingPolicyStore> {
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 3,
        ..SamplingPolicy::default()
    };
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    store
        .set(RECEIVER_KIND, &policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    store
}

/// The text of the first user message of a request.
fn first_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == ChatRole::User)
        .and_then(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// The text of each tool result of a request.
fn tool_results(request: &ChatRequest) -> Vec<String> {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

/// A model in which the caller makes `delegations` in one turn, then
/// repeats the tool results. Draw `n` answers `a`, `b` or `c`. Each other
/// run, a peer or an inline child, answers `plain`.
fn mesh_model(delegations: Vec<Value>) -> Arc<TryFnModel> {
    TryFnModel::new(move |request: &ChatRequest| {
        if let Some(draw) = request.draw {
            return answer(["a", "b", "c"][draw.get() as usize % 3], 5);
        }
        if first_user(request) != "go" {
            return answer("plain", 5);
        }
        let results = tool_results(request);
        if results.is_empty() {
            let list: Vec<(String, &str, Value)> = delegations
                .iter()
                .enumerate()
                .map(|(i, arguments)| (format!("d{i}"), DELEGATE_TOOL, arguments.clone()))
                .collect();
            let list: Vec<(&str, &str, Value)> = list
                .iter()
                .map(|(id, name, arguments)| (id.as_str(), *name, arguments.clone()))
                .collect();
            calls(&list, 1)
        } else {
            answer(&results.join("\n"), 1)
        }
    })
}

/// A `delegate` call to `agent` with `sample`.
fn ask(agent: &str, sample: Option<&OpSample>) -> Value {
    let mut arguments = json!({"agent": agent, "task": "Pick a letter."});
    if let Some(sample) = sample {
        arguments["sample"] = serde_json::to_value(sample).unwrap();
    }
    arguments
}

fn harness(model: &Arc<TryFnModel>) -> AgentHarness {
    AgentHarness::new(model.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research()))
        .peer(Peer::new("helper", "Helps inline."))
        .agent(AgentProfile::new(RECEIVER_KIND).mandate(Mandate::new("researcher")))
        .scorer(Arc::new(Table))
        .sampling_policies(policies())
}

/// Start the durable receiver, so a peer can reach it.
async fn start(rt: &mut SqliteRuntime) {
    sqlite::send(rt, &research(), &AgentOp::Control(Control::Resume), None).unwrap();
    sqlite::drive(rt, &research()).await.unwrap();
}

/// Run one `agent_loop` caller under `mandate` to its end. The receiver
/// runs each message that the caller sends.
async fn run_caller(rt: &mut SqliteRuntime, task: AgentTask) -> AgentReport {
    let exec = sqlite::start(rt, &task).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    sqlite::drive(rt, &research()).await.unwrap();
    let ended = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(ended, RunState::Completed(_)), "{ended:?}");
    report(ended)
}

fn caller(mandate: Mandate) -> AgentTask {
    AgentTask::new("go").mandate(mandate)
}

fn draw_calls(model: &TryFnModel) -> usize {
    model
        .requests()
        .iter()
        .filter(|request| request.draw.is_some())
        .count()
}

/// The peer messages in the history of the receiver.
fn peer_messages(rt: &SqliteRuntime) -> Vec<PeerEnvelope> {
    let exec = sqlite::current_run(rt, &research()).unwrap().unwrap();
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::SignalReceived { payload, .. }
                if payload["op"]["type"] == "peer_message" =>
            {
                serde_json::from_value(payload["op"].clone()).ok()
            }
            _ => None,
        })
        .collect()
}

/// The tool results that the caller read.
fn caller_results(report: &AgentReport) -> Vec<Value> {
    common::tool_results(report)
        .iter()
        .map(|text| serde_json::from_str(text).unwrap())
        .collect()
}

fn refusal_code(record: &DelegationRecord) -> Value {
    serde_json::to_value(record.refusal).unwrap()
}

/// The `sample` property of the `delegate` tool in the first caller
/// request, if the tool shows one.
fn sample_property(model: &TryFnModel) -> Option<Value> {
    let first = model
        .requests()
        .into_iter()
        .find(|request| request.draw.is_none())
        .expect("the caller asked the model");
    let tool = first
        .tools
        .iter()
        .find(|tool| tool.name == DELEGATE_TOOL)
        .expect("the caller sees the delegate tool");
    tool.input_schema["properties"].get("sample").cloned()
}

// ── A durable peer ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_best_of_three_runs_three_draws_and_answers_with_the_selected_one() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage").peers(["researcher"]).max_draws(3);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert_eq!(draw_calls(&model), 3, "the receiver runs 3 draws");
    let results = caller_results(&done);
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(
        results[0]["answer"], "b",
        "the caller reads the selected answer"
    );
    assert_eq!(results[0]["stop"], "completed");
    let received = peer_messages(&rt);
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].sample, Some(best_of_three()));
    assert_eq!(received[0].mandate.max_draws, Some(3));
    let record = done.delegations.last().unwrap();
    assert_eq!(record.refusal, None);
    assert_eq!(record.draws, 3, "the record keeps the draw units");
    assert!(
        sample_property(&model).is_some(),
        "a cap of 3 shows the sample argument"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_best_of_three_under_two_draws_is_refused_before_any_draw() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage").peers(["researcher"]).max_draws(2);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert_eq!(draw_calls(&model), 0, "no draw runs");
    assert!(peer_messages(&rt).is_empty(), "nothing is sent");
    let results = caller_results(&done);
    assert_eq!(results[0]["verdict"], "draws_exceeded", "{results:?}");
    let record = done.delegations.last().unwrap();
    assert_eq!(refusal_code(record), "draws_exceeded");
    assert_eq!(record.run_id, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_best_of_three_needs_four_units_of_fan_out() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage")
        .peers(["researcher"])
        .max_draws(3)
        .max_delegations(3);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert_eq!(draw_calls(&model), 0, "no draw runs");
    assert!(peer_messages(&rt).is_empty(), "nothing is sent");
    let results = caller_results(&done);
    assert_eq!(results[0]["verdict"], "fan_out_exhausted", "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn four_units_of_fan_out_admit_a_delegated_best_of_three() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage")
        .peers(["researcher"])
        .max_draws(3)
        .max_delegations(4);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert_eq!(draw_calls(&model), 3);
    assert_eq!(caller_results(&done)[0]["answer"], "b");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_with_no_draw_cap_sees_no_sample_and_gets_one_draw() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let done = run_caller(
        &mut rt,
        caller(Mandate::new("triage").peers(["researcher"])),
    )
    .await;

    assert!(
        sample_property(&model).is_none(),
        "no cap hides the argument"
    );
    assert_eq!(draw_calls(&model), 0, "no draw runs");
    assert_eq!(caller_results(&done)[0]["verdict"], "draws_exceeded");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cap_of_one_draw_hides_the_sample_argument() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("researcher", None)]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage").peers(["researcher"]).max_draws(1);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert!(sample_property(&model).is_none());
    assert_eq!(caller_results(&done)[0]["answer"], "plain");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_field_in_the_sample_is_invalid_arguments() {
    let (_dir, db) = fresh_db();
    let mut arguments = ask("researcher", Some(&best_of_three()));
    arguments["sample"]["max_draws"] = json!(9);
    let model = mesh_model(vec![arguments]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage").peers(["researcher"]).max_draws(3);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert!(peer_messages(&rt).is_empty(), "nothing is sent");
    assert_eq!(caller_results(&done)[0]["verdict"], "invalid_arguments");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_policy_that_refuses_the_strategy_answers_with_an_error() {
    let (_dir, db) = fresh_db();
    let vote = OpSample::vote(3, autumn_harvest_agent::sampling::AnswerKey::Trimmed);
    let model = mesh_model(vec![ask("researcher", Some(&vote))]);
    let mut rt = runtime(&db, harness(&model));
    start(&mut rt).await;
    let mandate = Mandate::new("triage").peers(["researcher"]).max_draws(3);
    let done = run_caller(&mut rt, caller(mandate)).await;

    assert_eq!(draw_calls(&model), 0, "the receiver policy lists no vote");
    let results = caller_results(&done);
    assert!(
        results[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("vote")),
        "{results:?}"
    );
}

// ── An inline peer ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_delegated_strategy_to_an_inline_peer_is_refused() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![ask("helper", Some(&best_of_three()))]);
    let mut rt = runtime(&db, harness(&model));
    let mandate = Mandate::new("triage").peers(["helper"]).max_draws(3);
    let exec = sqlite::start(&mut rt, &caller(mandate)).unwrap();
    let done = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(model.requests().len(), 2, "the helper never runs");
    let results = caller_results(&done);
    assert_eq!(results[0]["verdict"], "strategy_needs_durable_peer");
    let record = done.delegations.last().unwrap();
    assert_eq!(refusal_code(record), "strategy_needs_durable_peer");
    assert_eq!(record.chain, ["triage", "helper"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_refuses_each_call_with_a_strategy_and_runs_the_rest() {
    let (_dir, db) = fresh_db();
    let model = mesh_model(vec![
        ask("helper", Some(&best_of_three())),
        ask("helper", None),
    ]);
    let mut rt = runtime(&db, harness(&model));
    let mandate = Mandate::new("triage").peers(["helper"]).max_draws(3);
    let task = caller(mandate).parallel_children(2);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let done = report(rt.run_until_blocked(exec).await.unwrap());

    let results = caller_results(&done);
    assert_eq!(results.len(), 2, "{results:?}");
    assert_eq!(results[0]["verdict"], "strategy_needs_durable_peer");
    assert_eq!(results[1]["stop"], "completed", "{results:?}");
    assert_eq!(draw_calls(&model), 0);
}
