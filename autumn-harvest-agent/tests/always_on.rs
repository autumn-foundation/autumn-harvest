//! Always-on primitives on the SQLite backend: delivery, heartbeats,
//! follow-ups, memory and the loop guard.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest_agent::delivery::{Delivery, HEARTBEAT_OK, Report, ReportSource};
use autumn_harvest_agent::followup::{FOLLOWUP_TOOL, Followups};
use autumn_harvest_agent::heartbeat::{HeartbeatReport, HeartbeatTask, Precheck};
use autumn_harvest_agent::loop_guard::LoopGuard;
use autumn_harvest_agent::memory::{InMemoryMemoryStore, MEMORY_TOOL, MemoryScope, MemoryStore};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentStop, AgentTask, BoxFuture, ChatRole, ToolEffect, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{
    Recorder, ScriptedModel, answer, calls, recorded_tool, report, runtime, tool_results,
};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn later(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(seconds)
}

/// A delivery channel that keeps every report.
#[derive(Debug, Default)]
struct Inbox(Mutex<Vec<Report>>);

impl Inbox {
    fn reports(&self) -> Vec<Report> {
        self.0.lock().unwrap().clone()
    }
}

impl Delivery for Inbox {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.lock().unwrap().push(report.clone());
        Box::pin(async { Ok(()) })
    }
}

/// A precheck with a fixed answer.
#[derive(Debug)]
struct Fixed(bool);

impl Precheck for Fixed {
    fn should_run<'a>(&'a self, _task: &'a HeartbeatTask) -> BoxFuture<'a, bool> {
        let answer = self.0;
        Box::pin(async move { answer })
    }
}

fn heartbeat_report(state: RunState) -> HeartbeatReport {
    match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed heartbeat, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delivered_run_reaches_the_delivery_once() {
    let (_dir, db) = fresh_db();
    let inbox = Arc::new(Inbox::default());
    let model = ScriptedModel::new(vec![answer("all done", 1)]);
    let harness = AgentHarness::new(model).delivery(inbox.clone());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("go").deliver()).unwrap();
    let _ = report(rt.run_until_blocked(exec).await.unwrap());

    let reports = inbox.reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].source, ReportSource::Run);
    assert_eq!(reports[0].text, "all done");
    assert_eq!(reports[0].stop, AgentStop::Completed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_without_deliver_sends_nothing() {
    let (_dir, db) = fresh_db();
    let inbox = Arc::new(Inbox::default());
    let model = ScriptedModel::new(vec![answer("quiet", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model).delivery(inbox.clone()));

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let _ = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(inbox.reports(), Vec::<Report>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_heartbeat_is_not_delivered() {
    let (_dir, db) = fresh_db();
    let inbox = Arc::new(Inbox::default());
    let model = ScriptedModel::new(vec![answer(HEARTBEAT_OK, 1)]);
    let mut rt = runtime(
        &db,
        AgentHarness::new(model.clone()).delivery(inbox.clone()),
    );

    let exec = sqlite::start_heartbeat(&mut rt, &HeartbeatTask::new()).unwrap();
    let outcome = heartbeat_report(rt.run_until_blocked(exec).await.unwrap());

    assert!(!outcome.skipped);
    assert!(!outcome.delivered);
    assert_eq!(inbox.reports(), Vec::<Report>::new());
    let prompt = &model.requests()[0].messages;
    assert!(
        prompt
            .iter()
            .any(|m| m.role == ChatRole::User && format!("{m:?}").contains(HEARTBEAT_OK)),
        "the default prompt names the silent answer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_noteworthy_heartbeat_is_delivered_and_stays_read_only() {
    let (_dir, db) = fresh_db();
    let inbox = Arc::new(Inbox::default());
    let writes = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w", "write", json!({}))], 1),
        answer("The disk is 95% full.", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .delivery(inbox.clone());
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start_heartbeat(&mut rt, &HeartbeatTask::new()).unwrap();
    let outcome = heartbeat_report(rt.run_until_blocked(exec).await.unwrap());

    assert!(outcome.delivered);
    assert_eq!(writes.runs(), Vec::<serde_json::Value>::new(), "read-only");
    let reports = inbox.reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].source, ReportSource::Heartbeat);
    assert_eq!(reports[0].text, "The disk is 95% full.");
    let run = outcome.report.unwrap();
    assert!(tool_results(&run)[0].contains("may only read"));
}

#[tokio::test(flavor = "multi_thread")]
async fn allow_actions_lifts_the_read_only_rule() {
    let (_dir, db) = fresh_db();
    let writes = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w", "write", json!({"n": 1}))], 1),
        answer(HEARTBEAT_OK, 1),
    ]);
    let harness = AgentHarness::new(model).tool(recorded_tool("write", ToolEffect::Write, &writes));
    let mut rt = runtime(&db, harness);

    let task = HeartbeatTask::new().allow_actions();
    let exec = sqlite::start_heartbeat(&mut rt, &task).unwrap();
    let _ = heartbeat_report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(writes.runs(), vec![json!({"n": 1})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_precheck_that_says_no_skips_the_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("never", 1)]);
    let harness = AgentHarness::new(model.clone()).precheck(Arc::new(Fixed(false)));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start_heartbeat(&mut rt, &HeartbeatTask::new()).unwrap();
    let outcome = heartbeat_report(rt.run_until_blocked(exec).await.unwrap());

    assert!(outcome.skipped);
    assert!(outcome.report.is_none());
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_followup_waits_on_a_durable_timer_then_runs_in_the_same_conversation() {
    let (_dir, db) = fresh_db();
    let inbox = Arc::new(Inbox::default());
    let model = ScriptedModel::new(vec![
        calls(
            &[(
                "f",
                FOLLOWUP_TOOL,
                json!({"prompt": "check the deploy", "delay_minutes": 20}),
            )],
            1,
        ),
        answer("I will check back in 20 minutes.", 1),
        answer("The deploy is green.", 1),
    ]);
    let harness = AgentHarness::new(model.clone()).delivery(inbox.clone());
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("watch the deploy")
        .deliver()
        .followups(Followups::new(Duration::from_secs(3_600)));
    let exec = sqlite::start(&mut rt, &task).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    assert_eq!(model.calls(), 2);
    assert_eq!(
        inbox.reports().len(),
        1,
        "the first answer is delivered now"
    );
    assert!(
        model.requests()[0]
            .tools
            .iter()
            .any(|t| t.name == FOLLOWUP_TOOL),
        "the model sees the follow-up tool"
    );

    let state = rt
        .run_until_blocked_as_of(exec, later(25 * 60))
        .await
        .unwrap();
    let run = report(state);
    assert_eq!(run.text, "The deploy is green.");
    assert_eq!(run.followups, 1);
    let reports = inbox.reports();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[1].source, ReportSource::Followup);

    // The follow-up continues the same conversation.
    let third = &model.requests()[2].messages;
    assert!(format!("{third:?}").contains("watch the deploy"));
    assert!(format!("{:?}", third.last().unwrap()).contains("check the deploy"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_followup_over_the_max_delay_is_refused() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(
            &[(
                "f",
                FOLLOWUP_TOOL,
                json!({"prompt": "later", "delay_minutes": 120}),
            )],
            1,
        ),
        answer("ok", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model));

    let task = AgentTask::new("go").followups(Followups::new(Duration::from_secs(3_600)));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let run = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(run.followups, 0);
    assert!(
        tool_results(&run)[0].contains("at most 60 minutes"),
        "{:?}",
        tool_results(&run)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_chain_cap_stops_an_agent_waking_itself_forever() {
    let (_dir, db) = fresh_db();
    let again = || {
        calls(
            &[(
                "f",
                FOLLOWUP_TOOL,
                json!({"prompt": "again", "delay_minutes": 1}),
            )],
            1,
        )
    };
    let model = ScriptedModel::new(vec![again(), answer("one", 1), again(), answer("two", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model));

    let task =
        AgentTask::new("go").followups(Followups::new(Duration::from_secs(600)).max_chain(1));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    let run = report(rt.run_until_blocked_as_of(exec, later(120)).await.unwrap());

    assert_eq!(run.followups, 1);
    assert_eq!(run.text, "two");
    assert!(
        tool_results(&run)
            .iter()
            .any(|r| r.contains("no more follow-ups")),
        "{:?}",
        tool_results(&run)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_is_a_frozen_snapshot_and_the_tool_writes_through() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(InMemoryMemoryStore::new());
    let scope = MemoryScope::new("user-1");
    let model = ScriptedModel::new(vec![
        calls(
            &[(
                "m",
                MEMORY_TOOL,
                json!({"action": "add", "block": "user", "text": "Prefers tea."}),
            )],
            1,
        ),
        answer("noted", 1),
    ]);
    let harness = AgentHarness::new(model.clone()).memory(store.clone());
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("remember I like tea")
        .system("You are helpful.")
        .memory(scope.clone());
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let run = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(run.stop, AgentStop::Completed);
    let blocks = store.load(&scope).await.unwrap();
    let user = blocks.iter().find(|b| b.label == "user").unwrap();
    assert_eq!(user.entries, vec!["Prefers tea.".to_owned()]);

    let requests = model.requests();
    let first = format!("{:?}", requests[0].messages[0]);
    let second = format!("{:?}", requests[1].messages[0]);
    assert!(first.contains("You are helpful."));
    assert!(first.contains("user"), "the snapshot names its blocks");
    assert_eq!(first, second, "the prompt does not change mid-run");
    assert!(requests[0].tools.iter().any(|t| t.name == MEMORY_TOOL));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_identical_call_warns_then_stops() {
    let (_dir, db) = fresh_db();
    let reads = Arc::new(Recorder::default());
    let same = || calls(&[("r", "read", json!({"q": 1}))], 1);
    let model = ScriptedModel::new(vec![same(), same(), same(), answer("never", 1)]);
    let harness =
        AgentHarness::new(model.clone()).tool(recorded_tool("read", ToolEffect::ReadOnly, &reads));
    let mut rt = runtime(&db, harness);

    let guard = LoopGuard {
        warn_after: 2,
        stop_after: 3,
        window: 10,
    };
    let exec = sqlite::start(&mut rt, &AgentTask::new("go").loop_guard(guard)).unwrap();
    let run = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(run.stop, AgentStop::LoopDetected);
    assert_eq!(model.calls(), 3);
    let results = tool_results(&run);
    assert!(!results[0].contains("repeated"));
    assert!(results[1].contains("repeated"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn no_call_runs_after_the_guard_stops_the_round() {
    let (_dir, db) = fresh_db();
    let reads = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![calls(
        &[
            ("a", "read", json!({"q": 1})),
            ("b", "read", json!({"q": 1})),
            ("c", "read", json!({"q": 2})),
        ],
        1,
    )]);
    let harness =
        AgentHarness::new(model).tool(recorded_tool("read", ToolEffect::ReadOnly, &reads));
    let mut rt = runtime(&db, harness);

    let guard = LoopGuard {
        warn_after: 2,
        stop_after: 2,
        window: 10,
    };
    let exec = sqlite::start(&mut rt, &AgentTask::new("go").loop_guard(guard)).unwrap();
    let run = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(run.stop, AgentStop::LoopDetected);
    assert_eq!(reads.runs().len(), 2, "the third call never runs");
    let results = tool_results(&run);
    assert_eq!(results.len(), 3, "every call still has an answer");
    assert!(results[2].contains("loop guard stopped"), "{results:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_during_the_followup_wait_resends_nothing() {
    let (_dir, db) = fresh_db();
    let script = || {
        vec![
            calls(
                &[(
                    "f",
                    FOLLOWUP_TOOL,
                    json!({"prompt": "check again", "delay_minutes": 5}),
                )],
                1,
            ),
            answer("Checking back soon.", 1),
            answer("All clear.", 1),
        ]
    };
    let task = AgentTask::new("watch")
        .deliver()
        .followups(Followups::new(Duration::from_secs(600)));

    // Process one: run to the timer, then "crash".
    let first_inbox = Arc::new(Inbox::default());
    let first = ScriptedModel::new(script());
    let exec = {
        let mut rt = runtime(
            &db,
            AgentHarness::new(first.clone()).delivery(first_inbox.clone()),
        );
        let exec = sqlite::start(&mut rt, &task).unwrap();
        let state = rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
        exec
    };

    // Process two: the second segment answers from its own third turn.
    let second_inbox = Arc::new(Inbox::default());
    let second = ScriptedModel::new(script()[2..].to_vec());
    let mut rt = runtime(
        &db,
        AgentHarness::new(second.clone()).delivery(second_inbox.clone()),
    );
    let run = report(
        rt.run_until_blocked_as_of(exec, later(10 * 60))
            .await
            .unwrap(),
    );

    assert_eq!(run.text, "All clear.");
    assert_eq!(first.calls(), 2);
    assert_eq!(second.calls(), 1, "replay never asks the model again");
    assert_eq!(first_inbox.reports().len(), 1);
    let resent = second_inbox.reports();
    assert_eq!(resent.len(), 1, "the first report is not sent again");
    assert_eq!(resent[0].source, ReportSource::Followup);
}
