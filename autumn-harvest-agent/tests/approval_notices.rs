//! The approval notice on the SQLite backend.
//!
//! A gated call sends one notice before it parks. The notice names the
//! signal that `decide` takes. The tests cover the guarantees of
//! `docs/design/DESIGN-2603.md` section 8:
//!
//! - one notice for each parked call, also after a restart;
//! - a retry keeps the dedupe key;
//! - a failed notice never fails the run and never releases the call;
//! - a history from before the notice replays as it ran.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::Utc;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::delivery::{ApprovalNotice, Delivery, Report};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentOp, AgentProfile, AgentState, AgentStop, AgentTask,
    Approval, BoxFuture, ErrorKind, Rule, SessionId, ToolEffect, ToolRules, agent_info,
    agent_loop_info, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, ScriptedModel, answer, calls, recorded_tool, runtime};
use serde_json::{Value, json};

/// The name of the notice activity.
const NOTICE_ACTIVITY: &str = "agent_approval_notice";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(name: &str) -> AgentId {
    AgentId::new("support", name).unwrap()
}

/// A delivery that keeps each notice. It fails the first `fail` sends with
/// `kind`.
#[derive(Debug)]
struct Notices {
    seen: Mutex<Vec<ApprovalNotice>>,
    fail: Mutex<usize>,
    kind: ErrorKind,
}

impl Notices {
    fn new() -> Arc<Self> {
        Self::failing(0, ErrorKind::Unavailable)
    }

    fn failing(fail: usize, kind: ErrorKind) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            fail: Mutex::new(fail),
            kind,
        })
    }

    fn seen(&self) -> Vec<ApprovalNotice> {
        self.seen.lock().unwrap().clone()
    }
}

impl Delivery for Notices {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn notify_approval<'a>(
        &'a self,
        notice: &'a ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.seen.lock().unwrap().push(notice.clone());
        let failed = {
            let mut left = self.fail.lock().unwrap();
            let failed = *left > 0;
            *left = left.saturating_sub(1);
            failed
        };
        let err = AgentError::new(self.kind, "the chat API is down");
        Box::pin(async move { if failed { Err(err) } else { Ok(()) } })
    }
}

/// A harness whose `write` tool needs an approval.
fn harness(
    model: Arc<ScriptedModel>,
    recorder: &Arc<Recorder>,
    notices: &Arc<Notices>,
) -> AgentHarness {
    AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .delivery(Arc::clone(notices) as Arc<dyn Delivery>)
        .agent(AgentProfile::new("support").approval_timeout(Duration::from_secs(600)))
}

fn script() -> Vec<common::Reply> {
    vec![
        calls(&[("w1", "write", json!({"n": 1}))], 1),
        answer("written", 1),
    ]
}

/// Drive `agent` until it blocks on no retry timer. A retry backs off on a
/// durable timer, so each pass moves the clock a few seconds on. The approval
/// deadline is minutes away, so it never fires here.
async fn settle(rt: &mut SqliteRuntime, agent: &AgentId) -> Option<RunState> {
    let mut state = sqlite::drive(rt, agent).await.unwrap();
    for pass in 1..=20 {
        if !matches!(state, Some(RunState::WaitingTimer)) {
            break;
        }
        let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
        let at = Utc::now() + autumn_harvest::chrono::Duration::seconds(pass * 5);
        rt.run_until_blocked_as_of(exec, at).await.unwrap();
        state = sqlite::drive(rt, agent).await.unwrap();
    }
    state
}

/// Send `text` to `agent`, drive it, and return the signal it waits on.
async fn park(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) -> String {
    sqlite::send(rt, agent, &AgentOp::message(text), None).unwrap();
    match settle(rt, agent).await {
        Some(RunState::WaitingSignal(signal)) if signal.starts_with("tool_approval:") => signal,
        other => panic!("the op waits on its approval, got {other:?}"),
    }
}

async fn state(rt: &SqliteRuntime, agent: &AgentId) -> AgentState {
    sqlite::state(rt, agent)
        .await
        .unwrap()
        .expect("the agent runs")
}

/// Every event of the current run of `agent`.
fn history(rt: &SqliteRuntime, agent: &AgentId) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    (exec, rt.load_history(exec).unwrap())
}

/// The inputs of each scheduled `name` activity of `events`.
fn scheduled(events: &[WorkflowEvent], name: &str) -> Vec<Value> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                name: scheduled,
                input,
                ..
            } if scheduled == name => Some(input.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gated_call_on_an_agent_sends_one_notice_that_decide_accepts() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let model = ScriptedModel::new(script());
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let agent = id("acct-1");
    let before = Utc::now();

    let signal = park(&mut rt, &agent, "save it").await;
    let after = Utc::now();
    let seen = notices.seen();
    assert_eq!(seen.len(), 1, "one notice for one parked call");
    let notice = &seen[0];
    assert_eq!(notice.call.signal, signal);
    assert_eq!(notice.call.tool, "write");
    assert_eq!(notice.call.arguments, json!({"n": 1}));
    assert!(!notice.call.arguments_truncated);
    assert_eq!(notice.call.agent_id.as_ref(), Some(&agent));
    assert_eq!(notice.call.timeout_secs, 600);
    assert!(!notice.call.reason.is_empty(), "the policy reason");
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    assert_eq!(notice.call.execution_id, exec);
    assert_eq!(
        notice.key(),
        format!("{}:{signal}", notice.call.run_id.as_str())
    );
    let window = autumn_harvest::chrono::Duration::seconds(600);
    assert!(notice.deadline >= before + window, "{notice:?}");
    assert!(notice.deadline <= after + window, "{notice:?}");
    assert!(recorder.runs().is_empty(), "the notice releases nothing");

    sqlite::decide(
        &mut rt,
        notice.call.execution_id,
        &notice.call.signal,
        &Approval::Approve,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(recorder.runs(), vec![json!({"n": 1})]);
    assert_eq!(notices.seen().len(), 1, "no second notice");
    let state = state(&rt, &agent).await;
    assert_eq!(state.last_answer.as_deref(), Some("written"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_during_the_wait_sends_no_second_notice() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let first = Notices::new();
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &first),
    );
    let agent = id("acct-2");
    let signal = park(&mut rt, &agent, "save it").await;
    assert_eq!(first.seen().len(), 1);
    drop(rt);

    // A new process with a new delivery. The recorded notice is not sent again.
    let second = Notices::new();
    let mut rest = script();
    rest.drain(..1);
    let mut rt = runtime(&db, harness(ScriptedModel::new(rest), &recorder, &second));
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(second.seen().is_empty(), "{:?}", second.seen());
    assert_eq!(recorder.runs().len(), 1);
    assert_eq!(
        state(&rt, &agent).await.last_stop,
        Some(AgentStop::Completed)
    );

    // Replay reads a run that ended, so delete the agent first.
    let (exec, _) = history(&rt, &agent);
    sqlite::delete(&mut rt, &agent, None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let events = rt.load_history(exec).unwrap();
    assert_eq!(scheduled(&events, NOTICE_ACTIVITY).len(), 1);
    let replay = WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_execution_id(exec)
        .with_workflow_id(agent.as_str())
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_notice_keeps_its_key() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let notices = Notices::failing(1, ErrorKind::Unavailable);
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &notices),
    );
    let agent = id("acct-3");
    park(&mut rt, &agent, "save it").await;

    let seen = notices.seen();
    assert_eq!(seen.len(), 2, "the activity retried once");
    assert_eq!(seen[0].key(), seen[1].key(), "a retry keeps the key");
    let (_exec, events) = history(&rt, &agent);
    assert_eq!(scheduled(&events, NOTICE_ACTIVITY).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_notice_never_fails_the_run_or_releases_the_call() {
    for kind in [ErrorKind::Provider, ErrorKind::Unavailable] {
        let (_dir, db) = fresh_db();
        let recorder = Arc::new(Recorder::default());
        let notices = Notices::failing(usize::MAX, kind);
        let model = ScriptedModel::new(vec![
            calls(&[("w1", "write", json!({"n": 1}))], 1),
            answer("not written", 1),
            calls(&[("w2", "write", json!({"n": 2}))], 1),
            answer("written", 1),
        ]);
        let mut rt = runtime(&db, harness(model, &recorder, &notices));
        let agent = id("acct-4");

        // The failed notice still parks the call. Only a decision moves it.
        let signal = park(&mut rt, &agent, "save one").await;
        assert!(!notices.seen().is_empty(), "{kind:?}");
        assert!(recorder.runs().is_empty(), "{kind:?}: nothing ran");
        let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
        let reject = Approval::Reject {
            reason: "no".into(),
        };
        sqlite::decide(&mut rt, exec, &signal, &reject).unwrap();
        settle(&mut rt, &agent).await;
        assert!(
            recorder.runs().is_empty(),
            "{kind:?}: a rejection runs nothing"
        );
        let after = state(&rt, &agent).await;
        assert_eq!(after.last_stop, Some(AgentStop::Completed), "{kind:?}");
        assert_eq!(after.last_answer.as_deref(), Some("not written"));

        // The next gated call parks again, and an approval releases it.
        let signal = park(&mut rt, &agent, "save two").await;
        assert!(recorder.runs().is_empty(), "{kind:?}");
        let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
        sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
        settle(&mut rt, &agent).await;
        assert_eq!(recorder.runs(), vec![json!({"n": 2})], "{kind:?}");
        let after = state(&rt, &agent).await;
        assert_eq!(after.last_answer.as_deref(), Some("written"), "{kind:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_loop_run_sends_a_notice_with_no_agent_id() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &notices),
    );
    let session = SessionId::new("chat-7");
    let task = AgentTask::new("save it")
        .session(session.clone())
        .approval_timeout(Duration::from_secs(90));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on its approval");
    };

    let seen = notices.seen();
    assert_eq!(seen.len(), 1);
    let notice = &seen[0].call;
    assert_eq!(notice.agent_id, None);
    assert_eq!(notice.session_id.as_ref(), Some(&session));
    assert_eq!(notice.run_id.as_str(), exec.to_string());
    assert_eq!(notice.execution_id, exec);
    assert_eq!(notice.signal, signal);
    assert_eq!(notice.timeout_secs, 90);

    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_eq!(notices.seen().len(), 1);
    assert_eq!(recorder.runs().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn large_arguments_are_cut_to_the_result_cap() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let big = "x".repeat(4_000);
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": big}))], 1),
        answer("written", 1),
    ]);
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let task = AgentTask::new("save it").max_request_bytes(12_000);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let RunState::WaitingSignal(_) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on its approval");
    };

    let notice = notices.seen().remove(0).call;
    assert!(notice.arguments_truncated);
    let Value::String(cut) = &notice.arguments else {
        panic!("a cut argument object is text: {:?}", notice.arguments);
    };
    assert!(cut.ends_with("…[truncated]"), "{cut}");
    assert!(cut.len() < 4_000, "{}", cut.len());
}

/// A history that `trunk-dev` recorded at fa41510, before the approval
/// notice. It holds an `agent_loop` run and an `agent` run. Each parks a
/// gated call, takes an approval and runs the call. The agent is then
/// deleted, so its run ends.
const PRE_NOTICE_HISTORY: &str = include_str!("fixtures/pre_notice_history.json");

#[tokio::test(flavor = "multi_thread")]
async fn a_history_from_before_the_notice_replays() {
    assert!(!PRE_NOTICE_HISTORY.contains(NOTICE_ACTIVITY));
    let doc: Value = serde_json::from_str(PRE_NOTICE_HISTORY).unwrap();

    let run = &doc["agent_loop"];
    let exec: ExecutionId = serde_json::from_value(run["execution_id"].clone()).unwrap();
    let events: Vec<WorkflowEvent> = serde_json::from_value(run["events"].clone()).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );

    let run = &doc["agent"];
    let exec: ExecutionId = serde_json::from_value(run["execution_id"].clone()).unwrap();
    let events: Vec<WorkflowEvent> = serde_json::from_value(run["events"].clone()).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_execution_id(exec)
        .with_workflow_id(run["workflow_id"].as_str().unwrap())
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_history_with_a_notice_replays() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &notices),
    );
    let exec = sqlite::start(&mut rt, &AgentTask::new("save it")).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on its approval");
    };
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let events = rt.load_history(exec).unwrap();
    assert_eq!(scheduled(&events, NOTICE_ACTIVITY).len(), 1);
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
async fn an_interrupt_during_the_notice_ends_the_wait_as_a_rejection() {
    use autumn_harvest_agent::{ContentPart, Urgency};

    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let notices = Notices::failing(1, ErrorKind::Unavailable);
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"n": 1}))], 1),
        answer("dropped the write", 1),
        answer("on it", 1),
    ]);
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let agent = id("acct-5");
    sqlite::send(&mut rt, &agent, &AgentOp::message("save it"), None).unwrap();
    // The first notice fails, so the activity backs off on a timer.
    let waiting = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(waiting, Some(RunState::WaitingTimer)),
        "{waiting:?}"
    );

    let stop = AgentOp::urgent("never mind", "m-2", Urgency::Interrupt);
    sqlite::send(&mut rt, &agent, &stop, Some("m-2")).unwrap();
    settle(&mut rt, &agent).await;

    assert_eq!(notices.seen().len(), 2, "the notice ran to its end");
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
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].contains("interrupted"), "{results:?}");
    assert_eq!(state.last_answer.as_deref(), Some("on it"));
}
