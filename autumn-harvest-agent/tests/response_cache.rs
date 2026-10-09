//! The cross-run response cache on the SQLite backend (issue #1998).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{
    AgentHarness, AgentStop, AgentTask, CacheScope, ModelTurn, TokenUsage, ToolDecision,
    ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, SqliteRuntime};
use common::{
    CountingPolicy, MemStore, Recorder, ScriptedModel, answer, calls, recorded_tool, report,
    response_cache, runtime,
};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// Run `task` to its end. Returns the execution id and the answer.
async fn run(rt: &mut SqliteRuntime, task: &AgentTask) -> (ExecutionId, String) {
    let exec = sqlite::start(rt, task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());
    assert_eq!(report.stop, AgentStop::Completed);
    (exec, report.text)
}

/// Each recorded model turn of `exec`, in order.
fn turns(rt: &SqliteRuntime, exec: ExecutionId) -> Vec<ModelTurn> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { output, .. } => {
                serde_json::from_value::<ModelTurn>(output).ok()
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_run_is_served_from_the_cache_and_recorded() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("hello", 10)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi").system("Be brief.");

    let (first, text) = run(&mut rt, &task).await;
    assert_eq!(text, "hello");
    let (second, text) = run(&mut rt, &task).await;
    assert_eq!(text, "hello");
    assert_eq!(model.calls(), 1, "the second run must not call the model");

    let recorded = turns(&rt, first);
    assert_eq!(recorded.len(), 1);
    assert!(!recorded[0].cache_hit);
    assert_eq!(recorded[0].usage, TokenUsage::new(10, 0));

    let served = turns(&rt, second);
    assert_eq!(served.len(), 1, "the hit is recorded as a model turn");
    assert!(served[0].cache_hit);
    assert_eq!(served[0].usage, TokenUsage::default());
    assert_eq!(served[0].content, recorded[0].content);
    assert_eq!(served[0].stop, recorded[0].stop);
}

/// Strict replay of a history with a hit succeeds. The replayer runs no
/// activity, so the cache cannot change what replay sees.
#[tokio::test(flavor = "multi_thread")]
async fn strict_replay_of_a_hit_succeeds() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("hello", 10)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi").tenant("acme");
    run(&mut rt, &task).await;
    let (second, _) = run(&mut rt, &task).await;
    assert!(turns(&rt, second)[0].cache_hit);

    let history = rt.load_history(second).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(second)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay:?}"
    );
    assert_eq!(model.calls(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenants_do_not_share_an_entry_by_default() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named(
        "m-1",
        vec![
            answer("for a", 1),
            answer("for b", 1),
            answer("for no tenant", 1),
        ],
    );
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("the same prompt");

    let (_, a) = run(&mut rt, &task.clone().tenant("a")).await;
    let (_, b) = run(&mut rt, &task.clone().tenant("b")).await;
    let (_, none) = run(&mut rt, &task).await;
    let (_, a_again) = run(&mut rt, &task.clone().tenant("a")).await;

    assert_eq!(
        [a.as_str(), b.as_str(), none.as_str(), a_again.as_str()],
        ["for a", "for b", "for no tenant", "for a"]
    );
    assert_eq!(model.calls(), 3, "only the same tenant shares an entry");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_with_no_id_is_never_cached() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("one", 1), answer("two", 1)]);
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi");

    run(&mut rt, &task).await;
    run(&mut rt, &task).await;

    assert_eq!(model.calls(), 2);
    assert_eq!(store.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broken_store_falls_back_to_the_model() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("one", 1), answer("two", 1)]);
    let store = MemStore::broken();
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi");

    let (_, one) = run(&mut rt, &task).await;
    let (_, two) = run(&mut rt, &task).await;

    assert_eq!((one.as_str(), two.as_str()), ("one", "two"));
    assert_eq!(model.calls(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_cache_read_falls_back_to_the_model() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("one", 1), answer("two", 1)]);
    let store = MemStore::broken_reads();
    let harness = AgentHarness::new(model.clone()).response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("hi");

    let (_, one) = run(&mut rt, &task).await;
    let (second, two) = run(&mut rt, &task).await;

    assert_eq!((one.as_str(), two.as_str()), ("one", "two"));
    assert_eq!(store.gets(), 1, "the second run read the cache and failed");
    assert!(!turns(&rt, second)[0].cache_hit);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shared_scope_serves_every_tenant() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::named("m-1", vec![answer("public", 1)]);
    let store = Arc::new(MemStore::default());
    let cache = response_cache(&store).scope(CacheScope::Shared);
    let harness = AgentHarness::new(model.clone()).response_cache(cache);
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("what is 2 + 2?");

    let (_, a) = run(&mut rt, &task.clone().tenant("a")).await;
    let (_, b) = run(&mut rt, &task.clone().tenant("b")).await;

    assert_eq!((a.as_str(), b.as_str()), ("public", "public"));
    assert_eq!(model.calls(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hit_still_asks_the_policy_and_runs_the_tool() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let policy = CountingPolicy::new(ToolDecision::Allow);
    let model = ScriptedModel::named(
        "m-1",
        vec![
            calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
            answer("done", 5),
        ],
    );
    let store = Arc::new(MemStore::default());
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .policy(policy.clone())
        .response_cache(response_cache(&store));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("look it up");

    let (_, first) = run(&mut rt, &task).await;
    let (second, again) = run(&mut rt, &task).await;

    assert_eq!((first.as_str(), again.as_str()), ("done", "done"));
    assert_eq!(model.calls(), 2, "both turns of the second run are hits");
    assert_eq!(policy.asked(), 2, "a hit does not skip the policy");
    assert_eq!(recorder.runs().len(), 2, "a hit does not skip the tool");
    assert!(turns(&rt, second).iter().all(|turn| turn.cache_hit));
}
