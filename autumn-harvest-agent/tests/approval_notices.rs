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
/// durable timer, so each pass moves the clock a few seconds on. The clock
/// moves at most 100 seconds. Use it only with an approval timeout above that,
/// so the deadline never fires here.
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

/// The state of `agent`. The query holds no borrow of the runtime.
fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = Box::pin(sqlite::state(rt, agent));
    async move { query.await.unwrap().expect("the agent runs") }
}

/// Every event of the current run of `agent`.
fn history(rt: &SqliteRuntime, agent: &AgentId) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    (exec, rt.load_history(exec).unwrap())
}

/// Delete `agent`, so its current run ends, then replay that run strictly.
async fn assert_replays(rt: &mut SqliteRuntime, agent: &AgentId) -> Vec<WorkflowEvent> {
    let exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    sqlite::delete(rt, agent, None).unwrap();
    sqlite::drive(rt, agent).await.unwrap();
    let events = rt.load_history(exec).unwrap();
    let replay = WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_execution_id(exec)
        .with_workflow_id(agent.as_str())
        .replay_from_events(events.clone())
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
    events
}

/// The number of failed attempts of `name` that `events` record.
fn failed(events: &[WorkflowEvent], name: &str) -> usize {
    let ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: scheduled,
                ..
            } if scheduled == name => Some(*activity_id),
            _ => None,
        })
        .collect();
    events
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::ActivityFailed { activity_id, .. }
                if ids.contains(activity_id))
        })
        .count()
}

/// The text of each tool result in the transcript of `state`.
fn tool_results(state: &AgentState) -> Vec<String> {
    state
        .transcript
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            autumn_harvest_agent::ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
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
    assert_eq!(notice.call.reason, "write needs approval before it runs");
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    assert_eq!(notice.call.execution_id, exec);
    assert_eq!(
        notice.key(),
        format!("{}:{exec}:{signal}", notice.call.run_id.as_str())
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
    assert_eq!(seen[0].call, seen[1].call, "a retry sends the same call");
    let (_exec, events) = history(&rt, &agent);
    assert_eq!(scheduled(&events, NOTICE_ACTIVITY).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_before_the_notice_is_recorded_keeps_its_key() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let first = Notices::failing(1, ErrorKind::Unavailable);
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &first),
    );
    let agent = id("acct-6");
    sqlite::send(&mut rt, &agent, &AgentOp::message("save it"), None).unwrap();
    // The first attempt fails. The process stops before a retry records it.
    let waiting = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(waiting, Some(RunState::WaitingTimer)),
        "{waiting:?}"
    );
    drop(rt);

    let second = Notices::new();
    let mut rest = script();
    rest.drain(..1);
    let mut rt = runtime(&db, harness(ScriptedModel::new(rest), &recorder, &second));
    let parked = settle(&mut rt, &agent).await;
    assert!(
        matches!(&parked, Some(RunState::WaitingSignal(signal)) if signal.starts_with("tool_approval:")),
        "{parked:?}"
    );
    assert_eq!(second.seen().len(), 1, "the retry sends the notice once");
    assert_eq!(second.seen()[0].key(), first.seen()[0].key());
    let (_exec, events) = history(&rt, &agent);
    assert_eq!(scheduled(&events, NOTICE_ACTIVITY).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_notice_never_fails_the_run_or_releases_the_call() {
    // A refusal does not retry. An outage tries three times.
    for (kind, attempts) in [(ErrorKind::Provider, 1), (ErrorKind::Unavailable, 3)] {
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
        assert_eq!(notices.seen().len(), attempts, "{kind:?}");
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

        // Each failure is recorded, and the run replays over it.
        let events = assert_replays(&mut rt, &agent).await;
        // A retry records no event. Each notice records one final failure.
        assert_eq!(failed(&events, NOTICE_ACTIVITY), 2, "{kind:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_notice_and_no_decision_deny_the_call() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let notices = Notices::failing(usize::MAX, ErrorKind::Provider);
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"n": 1}))], 1),
        answer("no write", 1),
    ]);
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let agent = id("acct-7");
    park(&mut rt, &agent, "save it").await;

    // Nobody decides. The 600 s deadline passes.
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    let late = Utc::now() + autumn_harvest::chrono::Duration::seconds(601);
    rt.run_until_blocked_as_of(exec, late).await.unwrap();

    assert!(recorder.runs().is_empty(), "a timeout runs nothing");
    let after = state(&rt, &agent).await;
    assert_eq!(after.last_answer.as_deref(), Some("no write"));
    let results = tool_results(&after);
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        results[0].contains("no approval arrived within 600s"),
        "{results:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_gated_calls_send_two_notices_and_each_decision_releases_one() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let model = ScriptedModel::new(vec![
        calls(
            &[
                ("w1", "write", json!({"n": 1})),
                ("w2", "write", json!({"n": 2})),
            ],
            1,
        ),
        answer("one written", 1),
    ]);
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let agent = id("acct-8");
    let first = park(&mut rt, &agent, "save both").await;

    let seen = notices.seen();
    assert_eq!(seen.len(), 1, "the second call parks after the first");
    assert_eq!(seen[0].call.signal, first);
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    sqlite::decide(&mut rt, exec, &first, &Approval::Approve).unwrap();
    let second = match settle(&mut rt, &agent).await {
        Some(RunState::WaitingSignal(signal)) => signal,
        other => panic!("the second call parks, got {other:?}"),
    };

    let seen = notices.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].call.signal, second);
    assert_ne!(seen[0].key(), seen[1].key());
    assert_eq!(recorder.runs(), vec![json!({"n": 1})]);
    let reject = Approval::Reject {
        reason: "no".into(),
    };
    sqlite::decide(&mut rt, exec, &second, &reject).unwrap();
    settle(&mut rt, &agent).await;
    assert_eq!(recorder.runs(), vec![json!({"n": 1})], "only one ran");
    assert_eq!(notices.seen().len(), 2);
    assert_replays(&mut rt, &agent).await;
}

/// A policy that asks for an approval with a long reason.
#[derive(Debug)]
struct LongReason;

impl autumn_harvest_agent::ToolPolicy for LongReason {
    fn decide<'a>(
        &'a self,
        _call: &'a autumn_harvest_agent::ToolCall,
        _tool: Option<&'a dyn autumn_harvest_agent::Tool>,
        _info: &'a autumn_harvest_agent::RunInfo,
    ) -> BoxFuture<'a, autumn_harvest_agent::ToolDecision> {
        let reason = "r".repeat(1_000);
        Box::pin(async move { autumn_harvest_agent::ToolDecision::RequireApproval { reason } })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_reason_is_cut() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let harness = AgentHarness::new(ScriptedModel::new(script()))
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(LongReason))
        .delivery(Arc::clone(&notices) as Arc<dyn Delivery>);
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("save it")).unwrap();
    let RunState::WaitingSignal(_) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the run waits on its approval");
    };
    let reason = notices.seen().remove(0).call.reason;
    assert!(reason.len() <= 256, "{}", reason.len());
    assert!(reason.ends_with("…[truncated]"), "{reason}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_decision_during_the_notice_releases_an_agent_loop_call() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let notices = Notices::failing(1, ErrorKind::Unavailable);
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &notices),
    );
    let exec = sqlite::start(&mut rt, &AgentTask::new("save it")).unwrap();
    let start = Utc::now();
    let state = rt.run_until_blocked_as_of(exec, start).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");

    // The app decides from the first send, before the wait opens.
    let signal = notices.seen()[0].call.signal.clone();
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let mut state = state;
    for pass in 1..=10 {
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
        let at = start + autumn_harvest::chrono::Duration::seconds(pass * 5);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
    }
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(recorder.runs(), vec![json!({"n": 1})]);

    let events = rt.load_history(exec).unwrap();
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
async fn a_decision_during_the_notice_releases_an_agent_call() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let notices = Notices::failing(1, ErrorKind::Unavailable);
    let mut rt = runtime(
        &db,
        harness(ScriptedModel::new(script()), &recorder, &notices),
    );
    let agent = id("acct-9");
    sqlite::send(&mut rt, &agent, &AgentOp::message("save it"), None).unwrap();
    let waiting = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(waiting, Some(RunState::WaitingTimer)),
        "{waiting:?}"
    );

    // The decision lands before the approval race opens.
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    let signal = notices.seen()[0].call.signal.clone();
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    settle(&mut rt, &agent).await;

    assert_eq!(recorder.runs(), vec![json!({"n": 1})]);
    assert_eq!(
        state(&rt, &agent).await.last_answer.as_deref(),
        Some("written")
    );
    assert_replays(&mut rt, &agent).await;
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
    let max = autumn_harvest_agent::harness::max_tool_result_bytes(12_000);
    assert!(cut.len() <= max, "{} > {max}", cut.len());
}

/// A history that `trunk-dev` recorded at fa41510, before the approval
/// notice. It holds an `agent_loop` run and an `agent` run. Each parks a
/// gated call, takes an approval and runs the call. The agent is then
/// deleted, so its run ends.
const PRE_NOTICE_HISTORY: &str = include_str!("fixtures/pre_notice_history.json");

#[tokio::test(flavor = "multi_thread")]
async fn a_history_from_before_the_notice_replays() {
    assert!(!PRE_NOTICE_HISTORY.contains(NOTICE_ACTIVITY));
    assert!(!PRE_NOTICE_HISTORY.contains("patch:agent_approval_notice"));
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
    ]);
    let mut rt = runtime(&db, harness(model.clone(), &recorder, &notices));
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
    assert_eq!(state.last_answer.as_deref(), Some("dropped the write"));
    assert_eq!(model.calls(), 2, "the interrupted op asks no more turns");
    // The interrupt landed before the approval race opened. It replays.
    assert_replays(&mut rt, &agent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leftover_decision_does_not_hide_the_next_notice() {
    let (_dir, db) = fresh_db();
    let (recorder, notices) = (Arc::new(Recorder::default()), Notices::new());
    let model = ScriptedModel::new(vec![
        calls(
            &[
                ("w1", "write", json!({"n": 1})),
                ("w2", "write", json!({"n": 2})),
            ],
            1,
        ),
        answer("done", 1),
    ]);
    let mut rt = runtime(&db, harness(model, &recorder, &notices));
    let exec = sqlite::start(&mut rt, &AgentTask::new("save both")).unwrap();
    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the first call parks");
    };

    // Two reviewers answer the first call. The rejection runs no tool, so
    // the second call parks with the extra answer still unread.
    let reject = Approval::Reject {
        reason: "no".into(),
    };
    sqlite::decide(&mut rt, exec, &first, &reject).unwrap();
    sqlite::decide(&mut rt, exec, &first, &reject).unwrap();
    let RunState::WaitingSignal(second) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the second call parks");
    };

    let seen = notices.seen();
    assert_eq!(seen.len(), 2, "each parked call sends a notice");
    assert_eq!(seen[1].call.signal, second);
    sqlite::decide(&mut rt, exec, &second, &Approval::Approve).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_eq!(recorder.runs(), vec![json!({"n": 2})]);
    // No strict replay here. The extra answer stays unread, and a strict
    // replay reports an unread decision. See `approval.rs`.
}
