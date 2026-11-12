//! The profile reload of a running agent on the `SQLite` backend.
//!
//! `Control::ReloadProfile` reads the profile of the kind again, through
//! the recorded activity `agent_profile`. A good read ends the run at a
//! checkpoint, so the next op runs on the profile of the next run.
//! `tests/overlay_read_suite` runs the same check on Postgres. The design
//! is in `docs/design/DESIGN-2658.md`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest_agent::{
    AgentHarness, AgentHeartbeat, AgentId, AgentOp, AgentProfile, AgentState, ChatRequest,
    ChatRole, ContentPart, Control, FnTool, agent_profile_info, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionSummary, SqliteRuntime};
use common::{ScriptedModel, answer, runtime};
use serde_json::json;

const OLD: &str = "You are the old prompt.";
const NEW: &str = "You are the new prompt.";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(kind: &str, name: &str) -> AgentId {
    AgentId::new(kind, name).unwrap()
}

fn send(rt: &mut SqliteRuntime, agent: &AgentId, op: &AgentOp) {
    sqlite::send(rt, agent, op, None).unwrap();
}

fn say(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) {
    send(rt, agent, &AgentOp::message(text));
}

fn reload(rt: &mut SqliteRuntime, agent: &AgentId) {
    send(rt, agent, &AgentOp::Control(Control::ReloadProfile));
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, agent));
    async move { query.await.unwrap().expect("the agent runs") }
}

/// A model with `n` answers.
fn model(n: usize) -> Arc<ScriptedModel> {
    ScriptedModel::new((0..n).map(|_| answer("ok", 1)).collect())
}

/// The system text of `request`.
fn system(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::System)
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// The number of recorded `agent_profile` reads of the agent, over all
/// of its runs.
fn profile_reads(rt: &SqliteRuntime, agent: &AgentId) -> usize {
    let filter = ExecutionFilter::new()
        .workflow_name("agent")
        .workflow_id(agent.as_str());
    rt.list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .filter_map(ExecutionSummary::execution_id)
        .flat_map(|exec| rt.load_history(exec).unwrap())
        .filter(|event| {
            matches!(
                event,
                WorkflowEvent::ActivityScheduled { name, .. } if name == "agent_profile"
            )
        })
        .count()
}

/// The repro of the issue: a restart with a new prompt. With no reload,
/// the model still gets the old prompt, also with one op per run. After a
/// reload, it gets the new one.
async fn restart_then_reload(max_ops: Option<u64>) {
    let (_dir, db) = fresh_db();
    let agent = id("kind", "mark");
    let profile = |system: &str| {
        let profile = AgentProfile::new("kind").system(system);
        match max_ops {
            Some(n) => profile.max_ops_per_run(n),
            None => profile,
        }
    };
    let first = model(1);
    let mut rt = runtime(&db, AgentHarness::new(first.clone()).agent(profile(OLD)));
    say(&mut rt, &agent, "one");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(system(&first.requests()[0]), OLD);
    drop(rt);

    let second = model(2);
    let mut rt = runtime(&db, AgentHarness::new(second.clone()).agent(profile(NEW)));
    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(system(&second.requests()[0]), OLD, "the run keeps its read");

    reload(&mut rt, &agent);
    say(&mut rt, &agent, "three");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let requests = second.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(system(&requests[1]), NEW);
    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.segments, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_and_a_reload_reach_the_new_prompt() {
    restart_then_reload(None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_reaches_the_new_prompt_with_one_op_per_run() {
    restart_then_reload(Some(1)).await;
}

/// A reload with no restart. A new `register` replaces the harness of a
/// live runtime. The new window and the new tool guidance reach the op.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_reaches_the_window_and_the_tool_guidance() {
    let (_dir, db) = fresh_db();
    let agent = id("support", "acct-1");
    let lookup = FnTool::new("lookup", "Look up.", json!({"type": "object"}), |_| async {
        Ok(json!("found"))
    })
    .shared();
    let harness = |model: Arc<ScriptedModel>, profile: AgentProfile| {
        AgentHarness::new(model)
            .tool(Arc::clone(&lookup))
            .agent(profile)
    };
    let first = model(2);
    let mut rt = runtime(&db, harness(first.clone(), AgentProfile::new("support")));
    say(&mut rt, &agent, "one");
    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(state(&rt, &agent).await.transcript.len(), 4);

    let second = model(1);
    let narrow = AgentProfile::new("support")
        .window(2)
        .tool_guidance("lookup", "Look up the order first.");
    sqlite::register(&mut rt, Arc::new(harness(second.clone(), narrow)));
    reload(&mut rt, &agent);
    say(&mut rt, &agent, "three");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let requests = second.requests();
    assert_eq!(requests.len(), 1);
    let tool = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "lookup")
        .expect("the tool is offered");
    assert_eq!(tool.description, "Look up the order first.");
    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert_eq!(state.transcript.len(), 2, "the new window holds");
}

/// A failed reload keeps the current profile. The op records the error,
/// and the next op runs on the old profile.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_reload_keeps_the_profile() {
    let (_dir, db) = fresh_db();
    let agent = id("support", "acct-2");
    let model = model(2);
    let profile = AgentProfile::new("support").system(OLD);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).agent(profile));
    say(&mut rt, &agent, "one");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    rt.register_activity_with_body(&agent_profile_info(), |_| {
        Err(
            ActivityFailure::non_retryable("StoreDown", "the profile store is down")
                .into_error_payload(),
        )
    });
    reload(&mut rt, &agent);
    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 1);
    let error = state.last_error.unwrap_or_default();
    assert!(error.contains("the profile store is down"), "{error}");
    assert_eq!(state.segments, 2, "the next op still runs");
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(system(&requests[1]), OLD);
}

/// A kind with no profile after a deploy also keeps the current profile.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_of_a_removed_kind_keeps_the_profile() {
    let (_dir, db) = fresh_db();
    let agent = id("support", "acct-3");
    let first = model(1);
    let profile = AgentProfile::new("support").system(OLD);
    let mut rt = runtime(&db, AgentHarness::new(first).agent(profile));
    say(&mut rt, &agent, "one");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let second = model(1);
    let other = AgentProfile::new("billing").system(NEW);
    sqlite::register(
        &mut rt,
        Arc::new(AgentHarness::new(second.clone()).agent(other)),
    );
    reload(&mut rt, &agent);
    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 1);
    let error = state.last_error.unwrap_or_default();
    assert!(error.contains("no agent profile"), "{error}");
    assert_eq!(system(&second.requests()[0]), OLD);
}

/// A reload applies at once while the agent is paused. The held op runs
/// on the new profile at the resume.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_while_paused_reaches_the_held_ops() {
    let (_dir, db) = fresh_db();
    let agent = id("support", "acct-4");
    let first = model(0);
    let old = AgentProfile::new("support").system(OLD);
    let mut rt = runtime(&db, AgentHarness::new(first.clone()).agent(old));
    send(
        &mut rt,
        &agent,
        &AgentOp::Control(Control::Pause { reason: None }),
    );
    say(&mut rt, &agent, "held");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(state(&rt, &agent).await.held.len(), 1);

    let second = model(1);
    let new = AgentProfile::new("support").system(NEW);
    sqlite::register(
        &mut rt,
        Arc::new(AgentHarness::new(second.clone()).agent(new)),
    );
    reload(&mut rt, &agent);
    send(&mut rt, &agent, &AgentOp::Control(Control::Resume));
    sqlite::drive(&mut rt, &agent).await.unwrap();

    assert_eq!(first.calls(), 0);
    let requests = second.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(system(&requests[0]), NEW);
}

/// The reload op books the heartbeat of the new profile. So the schedule
/// of that op already uses the reloaded profile.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_books_the_new_heartbeat() {
    let (_dir, db) = fresh_db();
    let agent = id("support", "acct-5");
    let mut rt = runtime(
        &db,
        AgentHarness::new(model(1)).agent(AgentProfile::new("support")),
    );
    say(&mut rt, &agent, "one");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(state(&rt, &agent).await.next_heartbeat_ms, None);

    let beat = AgentHeartbeat::every(Duration::from_secs(3600));
    let profile = AgentProfile::new("support").heartbeat(beat);
    sqlite::register(
        &mut rt,
        Arc::new(AgentHarness::new(model(0)).agent(profile)),
    );
    reload(&mut rt, &agent);
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
    assert!(
        state.next_heartbeat_ms.is_some(),
        "the new heartbeat is booked"
    );
}

/// `sqlite::reload_profiles` reloads each running agent of the kind, and
/// no agent of another kind. A kind that only starts with the same text is
/// another kind.
#[tokio::test(flavor = "multi_thread")]
async fn reload_profiles_reaches_each_agent_of_the_kind() {
    let (_dir, db) = fresh_db();
    let a = id("support", "a");
    let b = id("support", "b");
    let near = id("support-desk", "c");
    let harness = |model: Arc<ScriptedModel>, system: &str| {
        AgentHarness::new(model)
            .agent(AgentProfile::new("support").system(system))
            .agent(AgentProfile::new("support-desk").system(system))
    };
    let mut rt = runtime(&db, harness(model(3), OLD));
    for agent in [&a, &b, &near] {
        say(&mut rt, agent, "one");
        sqlite::drive(&mut rt, agent).await.unwrap();
    }

    sqlite::register(&mut rt, Arc::new(harness(model(0), NEW)));
    let reloaded = sqlite::reload_profiles(&mut rt, "support").unwrap();
    assert_eq!(reloaded, vec![a.clone(), b.clone()]);
    for agent in [&a, &b, &near] {
        sqlite::drive(&mut rt, agent).await.unwrap();
    }
    // The first run read, the reload read, and the read of the next run.
    assert_eq!(profile_reads(&rt, &a), 3);
    assert_eq!(profile_reads(&rt, &b), 3);
    assert_eq!(profile_reads(&rt, &near), 1, "another kind");
    assert_eq!(
        sqlite::reload_profiles(&mut rt, "nobody").unwrap(),
        Vec::<AgentId>::new()
    );
}
