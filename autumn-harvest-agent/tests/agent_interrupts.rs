//! Mid-turn messages to a durable agent: queue, steer and interrupt (issue
//! #2265).
//!
//! The SQLite tests hold an op in flight with a model call that fails once.
//! Its retry waits on a backoff, so a test can send a message while the call
//! runs. The simulator tests deliver an interrupt at every task boundary.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, EntityMessage};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowTestEnv;
use autumn_harvest::testing::sim::SimOutcome;
use autumn_harvest::{always, sometimes};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentOp, AgentProfile, AgentState, AgentStop, ChatRole,
    ContentPart, ErrorKind, INTERRUPT_SIGNAL, ModelTurn, ModelTurnRequest, StopReason, TokenUsage,
    ToolDecision, ToolEffect, ToolOutcome, Urgency, agent_info, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, ScriptedModel, answer, calls, recorded_tool, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(name: &str) -> AgentId {
    AgentId::new("support", name).unwrap()
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().expect("the agent runs") }
}

/// A model error that the activity retries after a backoff.
fn rate_limited() -> common::Reply {
    Err(AgentError::new(ErrorKind::RateLimited, "slow down"))
}

/// The user texts of `state`, in order.
fn user_texts(state: &AgentState) -> Vec<String> {
    state
        .transcript
        .iter()
        .filter(|message| message.role == ChatRole::User)
        .filter_map(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// Every event of the running run of `agent`.
fn history(rt: &SqliteRuntime, agent: &AgentId) -> Vec<WorkflowEvent> {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    rt.load_history(exec).unwrap()
}

/// Drive the agent with its clock `secs` ahead, so a retry backoff ends.
async fn drive_later(rt: &mut SqliteRuntime, agent: &AgentId, secs: i64) -> RunState {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    let later = chrono::Utc::now() + chrono::Duration::seconds(secs);
    rt.run_until_blocked_as_of(exec, later).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupt_during_a_model_call_cancels_it_and_records_it() {
    let (_dir, db) = fresh_db();
    // The first call fails, so the turn waits on its retry backoff.
    let model = ScriptedModel::new(vec![rate_limited(), answer("the new answer", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("int-1");
    sqlite::send(&mut rt, &agent, &AgentOp::message("a long question"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(model.calls(), 1, "the first attempt ran and failed");

    let stop = AgentOp::urgent("stop, answer this instead", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &agent, &stop, Some("m-2")).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let events = history(&rt, &agent);
    let cancelled = events.iter().any(|event| {
        matches!(event, WorkflowEvent::ActivityFailed { error, .. } if error.contains("lost race"))
    });
    assert!(cancelled, "the race cancelled the model turn");
    let state = state(&rt, &agent).await;
    assert_eq!(model.calls(), 2, "the cancelled turn never ran again");
    assert_eq!(state.last_stop, Some(AgentStop::Completed));
    assert_eq!(state.last_answer.as_deref(), Some("the new answer"));
    assert_eq!(state.segments, 2);
    assert_eq!(
        user_texts(&state),
        ["a long question", "stop, answer this instead"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_history_replays() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![rate_limited(), answer("after", 1)]);
    let harness = AgentHarness::new(model).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("int-3");
    sqlite::send(&mut rt, &agent, &AgentOp::message("before"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let stop = AgentOp::urgent("now this", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &agent, &stop, Some("m-2")).unwrap();
    sqlite::delete(&mut rt, &agent, None).unwrap();
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let report = autumn_harvest::testing::WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_workflow_id(agent.as_str())
        .replay_from_events(rt.load_history(exec).unwrap())
        .await;
    assert!(
        matches!(
            report.status,
            autumn_harvest::testing::ReplayStatus::ReplaySucceeded
        ),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupt_ends_an_approval_wait_as_a_rejection() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({}))], 1),
        answer("dropped the write", 1),
        answer("on it", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            autumn_harvest_agent::ToolRules::new()
                .effect(ToolEffect::Write, autumn_harvest_agent::Rule::Ask),
        ))
        .agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("int-4");
    sqlite::send(&mut rt, &agent, &AgentOp::message("write it"), None).unwrap();
    let waiting = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(waiting, Some(RunState::WaitingSignal(_))),
        "{waiting:?}"
    );
    let stop = AgentOp::urgent("never mind", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &agent, &stop, Some("m-2")).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();

    assert!(recorder.runs().is_empty(), "the gated write never ran");
    let state = state(&rt, &agent).await;
    let results: Vec<String> = state
        .transcript
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].contains("interrupted"), "{results:?}");
    assert_eq!(state.last_answer.as_deref(), Some("dropped the write"));
    assert_eq!(model.calls(), 2, "the interrupted op asks no more turns");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_interrupt_does_not_stop_the_next_op() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("one", 1), answer("two", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("int-2");
    sqlite::send(&mut rt, &agent, &AgentOp::message("first"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    // Nothing runs, so the interrupt has nothing to stop.
    let next = AgentOp::urgent("second", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &agent, &next, Some("m-2")).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(state.last_stop, Some(AgentStop::Completed));
    assert_eq!(state.last_answer.as_deref(), Some("two"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_steer_joins_the_running_op_and_its_own_op_does_nothing() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        rate_limited(),
        calls(&[("c1", "lookup", json!({}))], 1),
        answer("checked both", 1),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("steer-1");
    sqlite::send(&mut rt, &agent, &AgentOp::message("check the order"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let steer = AgentOp::urgent("also check the invoice", "s-1", Urgency::Steer);
    sqlite::send(&mut rt, &agent, &steer, Some("s-1")).unwrap();
    // The backoff ends. The turn calls a tool, and the steer joins before the
    // next turn.
    drive_later(&mut rt, &agent, 60).await;
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let requests = model.requests();
    let last: Vec<String> = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::User)
        .filter_map(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(last, ["check the order", "also check the invoice"]);
    let state = state(&rt, &agent).await;
    assert_eq!(state.segments, 1, "the steer op ran no segment of its own");
    assert_eq!(model.calls(), 3);
    assert!(state.steer.steered.is_empty() && state.steer.ran.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_steer_with_no_op_running_runs_once_as_a_message() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("first", 1), answer("second", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("steer-2");
    sqlite::send(&mut rt, &agent, &AgentOp::message("hello"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let steer = AgentOp::urgent("one more thing", "s-2", Urgency::Steer);
    sqlite::send(&mut rt, &agent, &steer, Some("s-2")).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(user_texts(&state), ["hello", "one more thing"]);
    assert_eq!(model.calls(), 2);
    assert!(state.steer.steered.is_empty() && state.steer.ran.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_acknowledged_before_a_crash_is_handled_once() {
    let (_dir, db) = fresh_db();
    let agent = id("ack-1");
    let first = ScriptedModel::new(vec![]);
    let mut rt = runtime(
        &db,
        AgentHarness::new(first.clone()).agent(AgentProfile::new("support")),
    );
    let op = AgentOp::message_with_id("keep me", "m-1");
    let sent = sqlite::send(&mut rt, &agent, &op, Some("m-1")).unwrap();
    assert!(sent.signal_delivered, "the send is acknowledged");
    // The process stops before the agent runs.
    drop(rt);

    let second = ScriptedModel::new(vec![answer("kept", 1)]);
    let mut rt = runtime(
        &db,
        AgentHarness::new(second.clone()).agent(AgentProfile::new("support")),
    );
    // A client retry after the crash sends the same message again.
    let again = sqlite::send(&mut rt, &agent, &op, Some("m-1")).unwrap();
    assert!(!again.signal_delivered, "the key dedupes the retry");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(user_texts(&state), ["keep me"]);
    assert_eq!((first.calls(), second.calls()), (0, 1));
}

// ---------------------------------------------------------------------------
// The simulator: an interrupt lands at a seed-chosen task.
// ---------------------------------------------------------------------------

const SEEDS: SeedPlan = SeedPlan {
    first: 0,
    count: 96,
};

/// A model turn that calls the tool `name`, and marks whether the call may
/// be interrupted.
fn tool_turn(name: &str, interruptible: bool) -> ModelTurn {
    ModelTurn {
        content: vec![ContentPart::ToolCall {
            id: "c1".into(),
            name: name.into(),
            arguments: json!({}),
        }],
        stop: StopReason::ToolUse,
        usage: TokenUsage::new(1, 1),
        decisions: vec![ToolDecision::Allow],
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: vec![interruptible],
    }
}

fn final_turn() -> ModelTurn {
    ModelTurn {
        content: vec![ContentPart::Text("done".into())],
        stop: StopReason::EndTurn,
        usage: TokenUsage::new(1, 1),
        decisions: Vec::new(),
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
    }
}

/// No-op resume ops that go before the interrupt. Each boundary lands some
/// of them, so the interrupt tends to land later in the first op.
const NOISE: usize = 6;

/// A simulation that sends the noise, one interrupt, and a last message.
///
/// The last op drains an interrupt that arrived too late to stop anything.
/// Every signal is then read, so a strict replay of each history holds.
fn with_interrupt(env: &WorkflowTestEnv) -> autumn_harvest::testing::sim::Simulation<'_> {
    let resume = json!(EntityMessage::op(AgentOp::Control(
        autumn_harvest_agent::Control::Resume
    )));
    let last = json!(EntityMessage::op(AgentOp::message("bye")));
    (0..NOISE)
        .fold(env.simulation().seeds(SEEDS), |sim, _| {
            sim.signal(ENTITY_OP_SIGNAL, resume.clone())
        })
        .signal(INTERRUPT_SIGNAL, json!({}))
        .signal(ENTITY_OP_SIGNAL, last)
}

/// An agent env whose one op calls the tool `name` once.
fn sim_env(name: &'static str, interruptible: bool) -> WorkflowTestEnv {
    let op = json!(EntityMessage::op(AgentOp::message("work")));
    WorkflowTestEnv::new()
        .with_workflow_id("support/sim")
        .queue_signal(ENTITY_OP_SIGNAL, op)
        .mock_activity("agent_profile", |_| {
            Ok(serde_json::to_value(AgentProfile::new("support")).unwrap())
        })
        .mock_activity("agent_model_turn", move |input| {
            let request: ModelTurnRequest = serde_json::from_value(input).unwrap();
            let answered = request
                .messages
                .iter()
                .any(|message| message.role == ChatRole::Tool);
            let turn = if answered {
                final_turn()
            } else {
                tool_turn(name, interruptible)
            };
            Ok(serde_json::to_value(turn).unwrap())
        })
        .mock_activity("agent_tool_call", |_| {
            Ok(serde_json::to_value(ToolOutcome::ok("tool done")).unwrap())
        })
}

/// The id of the tool activity, and the positions of its schedule, its end
/// and the interrupt signal.
struct ToolWindow {
    scheduled: Option<usize>,
    ended: Option<usize>,
    cancelled: bool,
    interrupt: Option<usize>,
}

fn tool_window(events: &[WorkflowEvent]) -> ToolWindow {
    let mut window = ToolWindow {
        scheduled: None,
        ended: None,
        cancelled: false,
        interrupt: None,
    };
    let mut tool_id = None;
    for (at, event) in events.iter().enumerate() {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == "agent_tool_call" => {
                tool_id = Some(*activity_id);
                window.scheduled = Some(at);
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
                if Some(*activity_id) == tool_id =>
            {
                window.ended = Some(at);
            }
            WorkflowEvent::ActivityFailed {
                activity_id, error, ..
            } if Some(*activity_id) == tool_id => {
                window.ended = Some(at);
                window.cancelled = error.contains("lost race");
            }
            WorkflowEvent::SignalReceived { signal_name, .. }
                if signal_name == INTERRUPT_SIGNAL =>
            {
                window.interrupt = Some(at);
            }
            _ => {}
        }
    }
    window
}

/// The interrupt landed after the tool started and before it ended.
const fn lands_in_flight(window: &ToolWindow) -> bool {
    match (window.scheduled, window.ended, window.interrupt) {
        (Some(start), Some(end), Some(signal)) => start < signal && signal < end,
        (Some(start), None, Some(signal)) => start < signal,
        _ => false,
    }
}

fn check_write_tool(run: &SimOutcome) {
    let window = tool_window(run.events());
    always!(!window.cancelled, "a write tool is never cancelled halfway");
    if window.scheduled.is_some() {
        always!(
            window.ended.is_some(),
            "a started write tool runs to its end"
        );
    }
    sometimes!(
        lands_in_flight(&window),
        "an interrupt lands while the write tool runs"
    );
}

fn check_opted_in_tool(run: &SimOutcome) {
    let window = tool_window(run.events());
    sometimes!(window.cancelled, "an interrupt cancels an opted-in tool");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_tool_is_never_interrupted_halfway() {
    let env = sim_env("write", false);
    let report = with_interrupt(&env)
        .require_reached("an interrupt lands while the write tool runs")
        .sweep(agent_info().handler, json!({}), check_write_tool)
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(report.seeds > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_opted_in_tool_can_be_interrupted() {
    let env = sim_env("search", true);
    let report = with_interrupt(&env)
        .require_reached("an interrupt cancels an opted-in tool")
        .sweep(agent_info().handler, json!({}), check_opted_in_tool)
        .await
        .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(report.seeds > 0);
}
