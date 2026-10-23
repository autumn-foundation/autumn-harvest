//! Heartbeats and follow-ups of durable agents, delivered through the inbox
//! (issue #2266).
//!
//! The SQLite tests move the drive clock with `run_until_blocked_as_of`. A
//! wake timer fires when the drive clock reaches it. The simulator test lets
//! a tick and a message race at every task boundary.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::delivery::{Delivery, Report};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentModel, AgentOp, AgentProfile,
    AgentState, BoxFuture, ChatRequest, ChatResponse, ContentPart, Control, Followups,
    HeartbeatSetting, ModelTurnRequest, StopReason, TokenUsage, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, RunState, SqliteRuntime};
use common::{Recorder, ScriptedModel, answer, calls, recorded_tool, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(name: &str) -> AgentId {
    AgentId::new("watch", name).unwrap()
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().expect("the agent runs") }
}

/// Drive the agent at the drive clock `at`, through each checkpoint.
async fn drive_at(rt: &mut SqliteRuntime, agent: &AgentId, at: DateTime<Utc>) -> RunState {
    let mut exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    loop {
        match rt.run_until_blocked_as_of(exec, at).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return other,
        }
    }
}

/// A model that answers every request with one fixed text.
#[derive(Debug)]
struct FixedModel {
    text: &'static str,
    calls: AtomicUsize,
}

impl FixedModel {
    fn new(text: &'static str) -> Arc<Self> {
        Arc::new(Self {
            text,
            calls: AtomicUsize::new(0),
        })
    }
}

impl AgentModel for FixedModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = self.text;
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(text.to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(1, 1),
            })
        })
    }
}

/// A delivery that keeps each report.
#[derive(Debug, Default)]
struct Inbox {
    reports: Mutex<Vec<Report>>,
}

impl Delivery for Inbox {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.reports.lock().unwrap().push(report.clone());
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_thousand_agents_tick_on_their_own_intervals() {
    const AGENTS: u64 = 1_000;
    const PASSES: i64 = 6;
    let (_dir, db) = fresh_db();
    let model = FixedModel::new("HEARTBEAT_OK");
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("watch"));
    let mut rt = runtime(&db, harness);
    let start = Utc::now();
    let mut agents = Vec::new();
    for n in 0..AGENTS {
        let agent = id(&format!("a{n}"));
        let every = 60 + n;
        let setting = HeartbeatSetting::Custom {
            heartbeat: AgentHeartbeat::every(Duration::from_secs(every)),
        };
        let op = AgentOp::Control(Control::SetHeartbeat { setting });
        let exec = sqlite::send(&mut rt, &agent, &op, None).unwrap().exec_id;
        rt.run_until_blocked_as_of(exec, start).await.unwrap();
        agents.push((exec, every));
    }
    // Each pass moves the drive clock one minute and wakes each agent once.
    // The half-minute offset keeps each deadline clear of a pass.
    for pass in 1..=PASSES {
        let at = start + chrono::Duration::seconds(60 * pass + 30);
        for (exec, _) in &agents {
            rt.run_until_blocked_as_of(*exec, at).await.unwrap();
        }
    }
    for (n, (exec, every)) in agents.iter().enumerate() {
        let every = i64::try_from(*every).unwrap();
        // The first tick is due `every` after the start. Each later tick is
        // due `every` after the pass that ran the one before.
        let step = (every + 59) / 60;
        let first = (every - 30 + 59) / 60;
        let want = if first > PASSES {
            0
        } else {
            1 + (PASSES - first) / step
        };
        let value = rt
            .query(
                *exec,
                autumn_harvest::entity::ENTITY_STATE_QUERY,
                json!(null),
            )
            .await
            .unwrap();
        let state: AgentState = serde_json::from_value(value).unwrap();
        assert_eq!(
            state.ticks,
            u64::try_from(want).unwrap(),
            "agent a{n} every {every}s"
        );
        assert!(state.transcript.is_empty(), "a silent tick leaves no turn");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unattended_tick_cannot_run_a_write_tool() {
    for allow in [false, true] {
        let (_dir, db) = fresh_db();
        let recorder = Arc::new(Recorder::default());
        let model = ScriptedModel::new(vec![
            calls(&[("w1", "write", json!({}))], 1),
            answer("HEARTBEAT_OK", 1),
        ]);
        let mut beat = AgentHeartbeat::every(Duration::from_secs(60));
        if allow {
            beat = beat.allow_actions();
        }
        let harness = AgentHarness::new(model)
            .tool(recorded_tool("write", ToolEffect::Write, &recorder))
            .agent(AgentProfile::new("watch").heartbeat(beat));
        let mut rt = runtime(&db, harness);
        let agent = id("rw");
        let start = Utc::now();
        sqlite::send(&mut rt, &agent, &AgentOp::Control(Control::Resume), None).unwrap();
        drive_at(&mut rt, &agent, start).await;
        drive_at(&mut rt, &agent, start + chrono::Duration::seconds(90)).await;
        let state = state(&rt, &agent).await;
        assert_eq!(state.ticks, 1);
        assert_eq!(
            recorder.runs().len(),
            usize::from(allow),
            "allow_actions = {allow}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_during_a_wait_ticks_once_and_reports_once() {
    let (_dir, db) = fresh_db();
    let delivery = Arc::new(Inbox::default());
    let agent = id("restart");
    let harness = |delivery: &Arc<Inbox>| {
        AgentHarness::new(FixedModel::new("The disk is 95% full."))
            .delivery(delivery.clone())
            .agent(
                AgentProfile::new("watch")
                    .deliver()
                    .heartbeat(AgentHeartbeat::every(Duration::from_secs(60))),
            )
    };
    let start = Utc::now();
    let mut rt = runtime(&db, harness(&delivery));
    sqlite::send(&mut rt, &agent, &AgentOp::Control(Control::Resume), None).unwrap();
    drive_at(&mut rt, &agent, start).await;
    // The process stops while the agent waits on its timer.
    drop(rt);

    let later = start + chrono::Duration::seconds(90);
    let mut rt = runtime(&db, harness(&delivery));
    drive_at(&mut rt, &agent, later).await;
    // A second drive and a second restart at the same time change nothing.
    drive_at(&mut rt, &agent, later).await;
    drop(rt);
    let mut rt = runtime(&db, harness(&delivery));
    drive_at(&mut rt, &agent, later).await;

    assert_eq!(state(&rt, &agent).await.ticks, 1);
    let reports = delivery.reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert_eq!(
        reports[0].source,
        autumn_harvest_agent::ReportSource::Heartbeat
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_agent_skips_ticks_and_catches_up_once() {
    // The recorded clock of a wake is the wall clock, so this test lets one
    // real second pass.
    let (_dir, db) = fresh_db();
    let model = FixedModel::new("HEARTBEAT_OK");
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new("watch").heartbeat(AgentHeartbeat::every(Duration::from_secs(1))));
    let mut rt = runtime(&db, harness);
    let agent = id("paused");
    let pause = AgentOp::Control(Control::Pause { reason: None });
    sqlite::send(&mut rt, &agent, &pause, None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(
        state(&rt, &agent).await.ticks,
        0,
        "a paused agent skips ticks"
    );
    sqlite::send(&mut rt, &agent, &AgentOp::Control(Control::Resume), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(
        state(&rt, &agent).await.ticks,
        1,
        "one catch-up tick, not a pile"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_followup_chain_cap_holds_across_checkpoints() {
    let (_dir, db) = fresh_db();
    let book = || {
        calls(
            &[(
                "f",
                "schedule_followup",
                json!({"prompt": "check again", "delay_minutes": 1}),
            )],
            1,
        )
    };
    let model = ScriptedModel::new(vec![
        book(),
        answer("booked", 1),
        book(),
        answer("booked again", 1),
        book(),
        answer("the cap is reached", 1),
    ]);
    let profile = AgentProfile::new("watch")
        .followups(Followups::new(Duration::from_secs(3_600)).max_chain(2))
        // Each op ends its run, so each follow-up runs after a checkpoint.
        .max_ops_per_run(1);
    let harness = AgentHarness::new(model.clone()).agent(profile);
    let mut rt = runtime(&db, harness);
    let agent = id("chain");
    let start = Utc::now();
    sqlite::send(&mut rt, &agent, &AgentOp::message("watch the queue"), None).unwrap();
    drive_at(&mut rt, &agent, start).await;
    for minutes in [2, 4, 6, 8] {
        drive_at(&mut rt, &agent, start + chrono::Duration::minutes(minutes)).await;
    }
    let state = state(&rt, &agent).await;
    assert_eq!(state.chain, 2, "two follow-ups ran");
    assert_eq!(state.followup_due_ms, None, "the third booking was refused");
    assert_eq!(model.calls(), 6);
    let refused = state
        .transcript
        .iter()
        .flat_map(|message| message.content.iter())
        .any(|part| matches!(part, ContentPart::ToolResult { content, .. } if content.contains("cap of 2")));
    assert!(refused, "the model read the refusal");
    let runs = rt
        .list_executions(
            &ExecutionFilter::new()
                .workflow_name("agent")
                .workflow_id(agent.as_str()),
        )
        .unwrap()
        .executions
        .len();
    assert!(runs >= 3, "the chain crossed checkpoints: {runs} runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_next_wake_is_listed() {
    let (_dir, db) = fresh_db();
    let harness = AgentHarness::new(FixedModel::new("ok")).agent(
        AgentProfile::new("watch").heartbeat(AgentHeartbeat::every(Duration::from_secs(300))),
    );
    let mut rt = runtime(&db, harness);
    let agent = id("listed");
    sqlite::send(&mut rt, &agent, &AgentOp::message("hi"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    let page = rt
        .list_executions(
            &ExecutionFilter::new()
                .workflow_name("agent")
                .state("RUNNING")
                .search_attribute("agent_status", json!("idle"))
                .search_attribute("agent_wake_reason", json!("heartbeat")),
        )
        .unwrap();
    assert_eq!(page.executions.len(), 1);
    let attrs = page.executions[0]
        .search_attributes
        .value()
        .unwrap()
        .clone();
    assert_eq!(attrs["agent_kind"], json!("watch"));
    assert_eq!(
        attrs["agent_next_wake_at"],
        json!(state.next_heartbeat_ms.unwrap())
    );
}

// ---------------------------------------------------------------------------
// A tick and a message never overlap.
// ---------------------------------------------------------------------------

/// The segment number of each model turn of the agent, in history order.
fn turn_segments(rt: &SqliteRuntime, agent: &AgentId) -> Vec<(u64, bool)> {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    rt.load_history(exec)
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                let request: ModelTurnRequest = serde_json::from_value(input.clone()).ok()?;
                let segment = request.run_id.rsplit_once(":s")?.1.parse().ok()?;
                let tick = input.to_string().contains("HEARTBEAT_OK");
                Some((segment, tick))
            }
            _ => None,
        })
        .collect()
}

/// A heartbeat agent whose ticks and messages each run two turns: a tool
/// call, then an answer.
fn overlap_agent(
    script: Vec<common::Reply>,
) -> (tempfile::TempDir, SqliteRuntime, Arc<ScriptedModel>) {
    let (dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(script);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("look", ToolEffect::ReadOnly, &recorder))
        .agent(AgentProfile::new("watch").heartbeat(AgentHeartbeat::every(Duration::from_secs(1))));
    let rt = runtime(&db, harness);
    (dir, rt, model)
}

fn look() -> common::Reply {
    calls(&[("l", "look", json!({}))], 1)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_waits_for_a_tick_that_runs() {
    // A wake is due by the recorded wall clock, so the test lets real time
    // pass.
    let (_dir, mut rt, _model) = overlap_agent(vec![
        answer("ready", 1),
        look(),
        answer("HEARTBEAT_OK", 1),
        look(),
        answer("the reply", 1),
    ]);
    let agent = id("overlap-1");
    sqlite::send(&mut rt, &agent, &AgentOp::message("start"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    // The tick is due, and a message waits. The tick runs first and whole.
    sqlite::send(&mut rt, &agent, &AgentOp::message("hello"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let turns = turn_segments(&rt, &agent);
    assert_eq!(
        turns,
        [(0, false), (1, true), (1, true), (2, false), (2, false)],
        "the tick turns all come before the message turns"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tick_waits_for_a_message_that_runs() {
    let (_dir, mut rt, _model) = overlap_agent(vec![
        answer("ready", 1),
        // The first turn of the message fails once, so the op waits on a
        // backoff of two seconds while the tick falls due.
        Err(AgentError::new(
            autumn_harvest_agent::ErrorKind::RateLimited,
            "slow down",
        )),
        look(),
        answer("the reply", 1),
        look(),
        answer("HEARTBEAT_OK", 1),
    ]);
    let agent = id("overlap-2");
    sqlite::send(&mut rt, &agent, &AgentOp::message("start"), None).unwrap();
    sqlite::send(&mut rt, &agent, &AgentOp::message("hello"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let turns = turn_segments(&rt, &agent);
    let message: Vec<usize> = (0..turns.len())
        .filter(|&at| turns[at] == (1, false))
        .collect();
    let tick: Vec<usize> = (0..turns.len()).filter(|&at| turns[at].1).collect();
    assert!(!message.is_empty() && !tick.is_empty(), "{turns:?}");
    assert!(
        message.iter().max() < tick.iter().min(),
        "the message turns all come before the tick turns: {turns:?}"
    );
}
