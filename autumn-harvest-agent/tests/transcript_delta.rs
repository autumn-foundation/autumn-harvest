//! Each model turn records only the messages that it adds (issue #2264).
//!
//! Before this change each turn recorded the whole transcript, so a run's
//! history grew with the square of its turn count. A turn now records a
//! reference to the prefix and the messages after it. The model-turn
//! activity rebuilds the window from a `TranscriptStore`. On a miss, the
//! turn fails before any model call, and the workflow sends the full form.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::eval::{Candidate, evaluate};
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentOp, AgentProfile, AgentStop, AgentTask, ChatMessage,
    InMemoryTranscriptStore, ModelTurnRequest, ToolEffect, TranscriptStore, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, SqliteRuntime};
use common::{MemStore, Reply, ScriptedModel, answer, calls, report, response_cache, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

const TURNS: usize = 50;

/// A script of `TURNS` model turns: a lookup each turn, then an answer.
fn script() -> Vec<Reply> {
    let mut replies: Vec<Reply> = (0..TURNS - 1)
        .map(|n| calls(&[(&format!("c{n}"), "lookup", json!({"page": n}))], 10))
        .collect();
    replies.push(answer("all pages read", 10));
    replies
}

/// A lookup tool whose result is about 1 KB of text.
fn harness(model: Arc<ScriptedModel>) -> AgentHarness {
    let lookup = autumn_harvest_agent::FnTool::new(
        "lookup",
        "Read one page.",
        json!({"type": "object"}),
        |args| async move {
            let page = args["page"].as_u64().unwrap_or(0);
            Ok(json!(format!("page {page}: {}", "lorem ipsum ".repeat(85))))
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    AgentHarness::new(model).tool(lookup)
}

fn task(delta: bool) -> AgentTask {
    AgentTask::new("read every page")
        .system("You read pages.")
        .max_steps(u32::try_from(TURNS).unwrap())
        .transcript_delta(delta)
}

/// Run `task` to its end. Returns the execution, the report and the size
/// of the stored history in bytes.
async fn run(
    rt: &mut SqliteRuntime,
    task: &AgentTask,
) -> (ExecutionId, autumn_harvest_agent::AgentReport, usize) {
    let exec = sqlite::start(rt, task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    let bytes = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .map(|event| serde_json::to_vec(event).unwrap().len())
        .sum();
    (exec, report, bytes)
}

fn model_inputs(rt: &SqliteRuntime, exec: ExecutionId) -> Vec<ModelTurnRequest> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(serde_json::from_value(input).unwrap())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_50_turn_run_stores_at_least_80_percent_less_history() {
    let (_dir, db) = fresh_db();
    let full_model = ScriptedModel::new(script());
    let mut rt = runtime(&db, harness(full_model.clone()));
    let (_, full_report, full) = run(&mut rt, &task(false)).await;

    let (_dir2, db2) = fresh_db();
    let delta_model = ScriptedModel::new(script());
    let mut rt2 = runtime(&db2, harness(delta_model.clone()));
    let (exec, delta_report, delta) = run(&mut rt2, &task(true)).await;

    println!("history bytes: full {full}, delta {delta}");
    assert_eq!(full_report.stop, AgentStop::Completed);
    assert_eq!(delta_report.text, full_report.text);
    assert!(
        delta * 5 <= full,
        "full {full}, delta {delta}: less than 80% saved"
    );
    // The model read the same windows in both runs.
    assert_eq!(delta_model.requests(), full_model.requests());
    // The first turn sends the full form. Each later turn sends a delta.
    let inputs = model_inputs(&rt2, exec);
    assert!(inputs[0].prefix.is_none());
    assert!(inputs[1..].iter().all(|input| input.prefix.is_some()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_form_history_still_replays() {
    // A history from before the change has no `transcript_delta` in its
    // task, and every turn holds the whole transcript.
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(ScriptedModel::new(script())));
    let (exec, _, _) = run(&mut rt, &task(false)).await;
    let mut events = rt.load_history(exec).unwrap();
    if let Some(WorkflowEvent::WorkflowStarted { input, .. }) = events.first_mut() {
        input.as_object_mut().unwrap().remove("transcript_delta");
    }
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delta_history_replays_strictly() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(ScriptedModel::new(script())));
    let (exec, _, _) = run(&mut rt, &task(true)).await;
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

/// A store that never holds anything, like a cold worker.
#[derive(Debug, Default)]
struct ColdStore;

impl TranscriptStore for ColdStore {
    fn get(&self, _digest: &str) -> Option<Vec<ChatMessage>> {
        None
    }

    fn put(&self, _digest: &str, _window: &[ChatMessage]) {}
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cold_store_falls_back_to_the_full_form() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"page": 1}))], 1),
        calls(&[("c2", "lookup", json!({"page": 2}))], 1),
        answer("done", 1),
    ]);
    let cold = harness(model.clone()).transcript_store(Arc::new(ColdStore));
    let mut rt = runtime(&db, cold);
    let (exec, report, _) = run(&mut rt, &task(true)).await;

    assert_eq!(report.text, "done");
    assert_eq!(model.calls(), 3, "a miss calls no model");
    let inputs = model_inputs(&rt, exec);
    // Each delta misses, so the turn goes again in the full form.
    assert_eq!(inputs.len(), 5);
    assert!(inputs[0].prefix.is_none());
    assert!(inputs[1].prefix.is_some() && inputs[2].prefix.is_none());
    assert!(inputs[3].prefix.is_some() && inputs[4].prefix.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cache_key_is_the_same_in_both_forms() {
    // A full-form run fills the cache. A delta run of the same script then
    // reads every turn from it, so the two forms give the same keys.
    let (_dir, db) = fresh_db();
    let store = Arc::new(MemStore::default());
    let cache = response_cache(&store);
    let named = |replies| ScriptedModel::named("model-1", replies);
    let first = named(script());
    let mut rt = runtime(&db, harness(first.clone()).response_cache(cache.clone()));
    run(&mut rt, &task(false)).await;
    assert_eq!(first.calls(), TURNS);

    let second = named(Vec::new());
    drop(rt);
    let mut rt = runtime(&db, harness(second.clone()).response_cache(cache));
    let (_, report, _) = run(&mut rt, &task(true)).await;
    assert_eq!(report.text, "all pages read");
    assert_eq!(second.calls(), 0, "every turn is a cache hit");
}

#[tokio::test(flavor = "multi_thread")]
async fn eval_gives_the_same_result_for_a_delta_source() {
    let (_dir, db) = fresh_db();
    let mut rt = runtime(&db, harness(ScriptedModel::new(script())));
    let (exec, _, _) = run(&mut rt, &task(true)).await;
    let source = rt.load_history(exec).unwrap();

    let candidate = Candidate::new(harness(ScriptedModel::new(script())));
    let evaluation = evaluate(&source, &candidate).await.unwrap();
    assert!(!evaluation.diverged(), "{evaluation:#?}");
    assert_eq!(evaluation.turns.len(), TURNS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_agent_sends_deltas_across_its_ops() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("one", 1), answer("two", 1), answer("three", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("desk"));
    let mut rt = runtime(&db, harness);
    let desk = AgentId::new("desk", "d1").unwrap();
    for text in ["a", "b", "c"] {
        sqlite::send(&mut rt, &desk, &AgentOp::message(text), None).unwrap();
        sqlite::drive(&mut rt, &desk).await.unwrap();
    }
    let exec = sqlite::current_run(&rt, &desk).unwrap().unwrap();
    let inputs = model_inputs(&rt, exec);
    assert_eq!(inputs.len(), 3);
    assert!(inputs[0].prefix.is_none());
    // A later op starts with the transcript of the earlier ones.
    assert!(inputs[1..].iter().all(|input| input.prefix.is_some()));
    // The model still read every earlier message.
    let last = model.requests().pop().unwrap();
    assert_eq!(last.messages.len(), 5, "a, one, b, two, c");
}

#[test]
fn the_in_memory_store_has_a_byte_bound() {
    let store = InMemoryTranscriptStore::with_max_bytes(2_000);
    let window = |text: &str| {
        vec![ChatMessage::text(
            autumn_harvest_agent::ChatRole::User,
            text,
        )]
    };
    store.put("a", &window(&"x".repeat(900)));
    store.put("b", &window(&"y".repeat(900)));
    store.put("c", &window(&"z".repeat(900)));
    assert!(store.get("a").is_none(), "the oldest window goes first");
    assert!(store.get("c").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trimmed_transcript_is_sent_in_the_full_form() {
    // A window of two messages drops the oldest turns between ops. The old
    // window is then no prefix, so a delta on it would show the model a
    // wrong transcript.
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("one", 1), answer("two", 1), answer("three", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("desk").window(2));
    let mut rt = runtime(&db, harness);
    let desk = AgentId::new("desk", "d2").unwrap();
    for text in ["a", "b", "c"] {
        sqlite::send(&mut rt, &desk, &AgentOp::message(text), None).unwrap();
        sqlite::drive(&mut rt, &desk).await.unwrap();
    }
    let texts: Vec<Vec<String>> = model
        .requests()
        .iter()
        .map(|request| {
            request
                .messages
                .iter()
                .flat_map(|message| message.content.iter())
                .filter_map(|part| match part {
                    autumn_harvest_agent::ContentPart::Text(text) => Some(text.clone()),
                    _ => None,
                })
                .collect()
        })
        .collect();
    assert_eq!(
        texts,
        [vec!["a"], vec!["a", "one", "b"], vec!["b", "two", "c"]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_request_cap_still_measures_the_whole_window() {
    // A store can miss, and then the turn goes in the full form. So the cap
    // measures the whole window in both forms, and both runs end alike.
    let mut ends = Vec::new();
    for delta in [false, true] {
        let (_dir, db) = fresh_db();
        let model = ScriptedModel::new(script());
        let mut rt = runtime(&db, harness(model.clone()));
        let (_, report, _) = run(&mut rt, &task(delta).max_request_bytes(20_000)).await;
        ends.push((report.stop, model.calls()));
    }
    assert_eq!(ends[0].0, AgentStop::TranscriptFull);
    assert_eq!(ends[0], ends[1]);
}
