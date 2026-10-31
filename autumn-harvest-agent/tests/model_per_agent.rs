//! A model per agent and per peer (issue #2271).
//!
//! The harness holds named models. A task, a peer, a profile or a heartbeat
//! names its model. The model-turn activity input records the name, so replay
//! and the response cache stay exact.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::{
    AgentHarness, AgentHeartbeat, AgentId, AgentOp, AgentProfile, AgentTask, DEFAULT_MODEL,
    Mandate, Peer, Refusal, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{MemStore, Reply, ScriptedModel, answer, calls, report, response_cache, runtime};
use serde_json::{Value, json};

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// A turn that delegates `task` to `agent`.
fn delegate(agent: &str, task: &str) -> Reply {
    calls(
        &[("d1", DELEGATE_TOOL, json!({"agent": agent, "task": task}))],
        3,
    )
}

/// The `model` field of each recorded model-turn input, in order.
fn turn_models(rt: &SqliteRuntime, exec: autumn_harvest_sqlite::ExecutionId) -> Vec<Value> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(input.get("model").cloned().unwrap_or(Value::Null))
            }
            _ => None,
        })
        .collect()
}

/// Run `task` to its end and return the execution and its end state.
async fn run(
    rt: &mut SqliteRuntime,
    task: &AgentTask,
) -> (autumn_harvest_sqlite::ExecutionId, RunState) {
    let exec = sqlite::start(rt, task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    (exec, state)
}

fn triage_mandate() -> Mandate {
    Mandate::new("triage").peers(["planner"])
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parent_and_a_child_on_different_models_record_both_ids() {
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named(
        "cheap-1",
        vec![delegate("planner", "plan it"), answer("the plan is set", 3)],
    );
    let strong = ScriptedModel::named("strong-1", vec![answer("step one, step two", 7)]);
    let harness = AgentHarness::new(cheap.clone())
        .model_named("strong", strong.clone())
        .peer(Peer::new("planner", "Plans.").model("strong"));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("triage this").mandate(triage_mandate());
    let (exec, state) = run(&mut rt, &task).await;

    assert_eq!(report(state).text, "the plan is set");
    assert_eq!((cheap.calls(), strong.calls()), (2, 1));
    let models: Vec<String> = rt
        .llm_ledger(exec)
        .unwrap()
        .into_iter()
        .map(|row| row.model)
        .collect();
    assert_eq!(models, ["cheap-1", "strong-1", "cheap-1"]);
    // The input of each turn names its model, so replay asks the same one.
    assert_eq!(
        turn_models(&rt, exec),
        [Value::Null, json!("strong"), Value::Null]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_with_no_model_keeps_the_model_of_its_parent() {
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named("cheap-1", vec![]);
    let strong = ScriptedModel::named(
        "strong-1",
        vec![
            delegate("planner", "plan it"),
            answer("child", 2),
            answer("root", 2),
        ],
    );
    let harness = AgentHarness::new(cheap.clone())
        .model_named("strong", strong.clone())
        .peer(Peer::new("planner", "Plans."));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go")
        .model("strong")
        .mandate(triage_mandate());
    let (exec, state) = run(&mut rt, &task).await;

    assert_eq!(report(state).text, "root");
    assert_eq!((cheap.calls(), strong.calls()), (0, 3));
    assert_eq!(turn_models(&rt, exec), vec![json!("strong"); 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_response_cache_never_serves_one_models_answer_to_another() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(MemStore::default());
    let a = ScriptedModel::named("model-a", vec![answer("from a", 2)]);
    let b = ScriptedModel::named("model-b", vec![answer("from b", 2)]);
    let harness = AgentHarness::new(a.clone())
        .model_named("b", b.clone())
        .response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let on_a = AgentTask::new("the same question");
    let on_b = AgentTask::new("the same question").model("b");

    let (_, first) = run(&mut rt, &on_a).await;
    assert_eq!(report(first).text, "from a");
    let (_, second) = run(&mut rt, &on_b).await;
    assert_eq!(report(second).text, "from b", "b gets its own answer");
    assert_eq!(b.calls(), 1);
    // The cache still serves model a its own answer.
    let (_, third) = run(&mut rt, &on_a).await;
    assert_eq!(report(third).text, "from a");
    assert_eq!(a.calls(), 1, "a cache hit");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mandate_that_forbids_a_model_refuses_the_delegation() {
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named(
        "cheap-1",
        vec![delegate("planner", "plan it"), answer("no planner", 3)],
    );
    let strong = ScriptedModel::named("strong-1", vec![]);
    let harness = AgentHarness::new(cheap.clone())
        .model_named("strong", strong.clone())
        .peer(Peer::new("planner", "Plans.").model("strong"));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("triage this").mandate(triage_mandate().models([DEFAULT_MODEL]));
    let (_, state) = run(&mut rt, &task).await;

    let report = report(state);
    assert_eq!(report.text, "no planner");
    assert_eq!(strong.calls(), 0, "the forbidden model never runs");
    assert_eq!(report.delegations.len(), 1);
    assert_eq!(report.delegations[0].refusal, Some(Refusal::ModelForbidden));
    let refused = &cheap.requests()[1];
    let text = serde_json::to_string(&refused.messages).unwrap();
    assert!(text.contains("model_forbidden"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_root_task_on_a_forbidden_model_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named("cheap-1", vec![answer("never", 1)]);
    let strong = ScriptedModel::named("strong-1", vec![answer("never", 1)]);
    let harness = AgentHarness::new(cheap.clone()).model_named("strong", strong.clone());
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("go")
        .model("strong")
        .mandate(Mandate::new("triage").models([DEFAULT_MODEL]));
    let (_, state) = run(&mut rt, &task).await;

    let RunState::Failed(error) = state else {
        panic!("the run fails: {state:?}");
    };
    assert!(error.contains("forbids the model"), "{error}");
    assert_eq!((cheap.calls(), strong.calls()), (0, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_model_fails_the_turn_without_a_retry() {
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named("cheap-1", vec![answer("never", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(cheap.clone()));
    let (exec, state) = run(&mut rt, &AgentTask::new("go").model("missing")).await;

    let RunState::Failed(error) = state else {
        panic!("the run fails: {state:?}");
    };
    assert!(error.contains("missing"), "{error}");
    assert_eq!(cheap.calls(), 0, "no fallback to the default model");
    let attempts = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WorkflowEvent::ActivityScheduled { name, .. } if name == "agent_model_turn"))
        .count();
    assert_eq!(attempts, 1, "a configuration error is not retried");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_profile_and_its_heartbeat_name_their_models() {
    // The recorded clock of a wake is the wall clock, so this test lets one
    // real second pass.
    let (_dir, db) = fresh_db();
    let cheap = ScriptedModel::named("cheap-1", vec![answer("HEARTBEAT_OK", 1)]);
    let strong = ScriptedModel::named("strong-1", vec![answer("hello", 1)]);
    let beat = AgentHeartbeat::every(std::time::Duration::from_secs(1)).model(DEFAULT_MODEL);
    let harness = AgentHarness::new(cheap.clone())
        .model_named("strong", strong.clone())
        .agent(AgentProfile::new("desk").model("strong").heartbeat(beat));
    let mut rt = runtime(&db, harness);
    let desk = AgentId::new("desk", "d1").unwrap();
    sqlite::send(&mut rt, &desk, &AgentOp::message("hi"), None).unwrap();
    sqlite::drive(&mut rt, &desk).await.unwrap();
    assert_eq!(
        (cheap.calls(), strong.calls()),
        (0, 1),
        "a message runs on the profile model"
    );

    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    sqlite::drive(&mut rt, &desk).await.unwrap();
    assert_eq!(
        (cheap.calls(), strong.calls()),
        (1, 1),
        "a tick runs on its own model"
    );
}
