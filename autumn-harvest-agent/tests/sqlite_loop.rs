//! The agent loop on the SQLite backend: tools, policy, approvals, budgets.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest_agent::{AgentError, Approval, ChatRole, ErrorKind, ToolDecision, ToolEffect};
use autumn_harvest_agent::{AgentHarness, AgentStop, AgentTask, approval, sqlite};
use autumn_harvest_agent::{Rule, ToolRules};
use autumn_harvest_sqlite::RunState;
use common::{
    CountingPolicy, Recorder, ScriptedModel, answer, calls, recorded_tool, report, runtime,
    tool_results,
};
use serde_json::json;

/// A wall-clock instant `seconds` from now, to fire a durable timer early.
fn later(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(seconds)
}

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_final_answer_completes_the_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("42", 10)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));

    let exec = sqlite::start(&mut rt, &AgentTask::new("the answer?").system("Be brief.")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "42");
    assert_eq!(report.steps_used, 0);
    assert_eq!(report.usage.input_tokens, 10);
    assert_eq!(model.calls(), 1);
    let request = &model.requests()[0];
    assert_eq!(request.messages[0].role, ChatRole::System);
    assert_eq!(request.messages[1].role, ChatRole::User);
    assert!(
        report.messages.iter().all(|m| m.role != ChatRole::System),
        "the report transcript leaves out the system prompt"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_allowed_tool_runs_once_and_its_result_reaches_the_model() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer("done", 5),
    ]);
    let harness = AgentHarness::new(model.clone()).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("look it up")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.steps_used, 1);
    assert_eq!(report.tool_calls, 1);
    assert_eq!(report.usage.input_tokens, 10);
    assert_eq!(recorder.runs(), vec![json!({"q": "x"})]);
    assert_eq!(model.calls(), 2);
    assert_eq!(
        model.requests()[0].tools.len(),
        1,
        "the model sees the tool"
    );
    let results = tool_results(&report);
    assert_eq!(results.len(), 1);
    assert!(
        results[0].contains("echo"),
        "unexpected result: {}",
        results[0]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_tool_is_an_error_result_not_a_failed_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "nope", json!({}))], 1),
        answer("ok", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model));

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert!(tool_results(&report)[0].contains("unknown tool"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_denied_call_never_runs_and_the_model_sees_why() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "wipe", json!({}))], 1),
        answer("ok", 1),
    ]);
    let rules = ToolRules::new().tool("wipe", Rule::Deny("never wipe".into()));
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("wipe", ToolEffect::Write, &recorder))
        .policy(Arc::new(rules));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(
        recorder.runs(),
        Vec::<serde_json::Value>::new(),
        "a denied call must not run"
    );
    let results = tool_results(&report);
    assert!(
        results[0].contains("denied by policy: never wipe"),
        "{results:?}"
    );
}

/// Drive to the approval wait of one gated `write` call.
async fn to_approval(
    decision: Option<Approval>,
    advance: Option<i64>,
) -> (autumn_harvest_agent::AgentReport, Arc<Recorder>) {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"path": "a"}))], 1),
        answer("finished", 1),
    ]);
    let rules = ToolRules::new().effect(ToolEffect::Write, Rule::Ask);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(rules));
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("write it").approval_timeout(Duration::from_secs(60));
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::WaitingSignal(signal) = state else {
        panic!("expected an approval wait, got {state:?}");
    };
    assert_eq!(approval::approval_call_id(&signal), Some("w1"));
    assert_eq!(
        recorder.runs(),
        Vec::<serde_json::Value>::new(),
        "nothing runs before a decision"
    );

    if let Some(decision) = decision {
        sqlite::decide(&mut rt, exec, &signal, &decision).unwrap();
    }
    let state = match advance {
        Some(seconds) => rt.run_until_blocked_as_of(exec, later(seconds)).await,
        None => rt.run_until_blocked(exec).await,
    }
    .unwrap();
    (report(state), recorder)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_approved_call_runs_as_the_model_asked() {
    let (report, recorder) = to_approval(Some(Approval::Approve), None).await;
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(recorder.runs(), vec![json!({"path": "a"})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_edited_call_runs_with_the_reviewer_arguments() {
    let edit = Approval::Edit {
        arguments: json!({"path": "b"}),
    };
    let (_, recorder) = to_approval(Some(edit), None).await;
    assert_eq!(recorder.runs(), vec![json!({"path": "b"})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_call_never_runs_and_the_model_sees_the_reason() {
    let reject = Approval::Reject {
        reason: "not today".into(),
    };
    let (report, recorder) = to_approval(Some(reject), None).await;
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
    assert!(tool_results(&report)[0].contains("rejected this call: not today"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missed_deadline_denies_the_call() {
    let (report, recorder) = to_approval(None, Some(120)).await;
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
    assert!(
        tool_results(&report)[0].contains("no approval arrived within 60s"),
        "{:?}",
        tool_results(&report)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_step_budget_stops_before_another_tool_round() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("a", "lookup", json!({}))], 1),
        calls(&[("b", "lookup", json!({}))], 1),
        calls(&[("c", "lookup", json!({}))], 1),
    ]);
    let harness = AgentHarness::new(model.clone()).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("loop").max_steps(2)).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::StepsExhausted);
    assert_eq!(report.steps_used, 2);
    assert_eq!(recorder.runs().len(), 2, "the third round never runs");
    assert_eq!(model.calls(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_token_budget_stops_the_run_before_its_tools() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("a", "lookup", json!({}))], 100),
        calls(&[("b", "lookup", json!({}))], 100),
    ]);
    let harness =
        AgentHarness::new(model).tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder));
    let mut rt = runtime(&db, harness);

    let task = AgentTask::new("spend").max_total_tokens(150);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TokensExhausted);
    assert_eq!(report.usage.input_tokens, 200);
    assert_eq!(recorder.runs().len(), 1, "the over-budget round never runs");
}

/// A read-only tool that returns `bytes` bytes, more than one result keeps.
fn big_tool(bytes: usize) -> Arc<dyn autumn_harvest_agent::Tool> {
    autumn_harvest_agent::FnTool::new(
        "big",
        "Returns a large result.",
        json!({"type": "object"}),
        move |_input| async move { Ok(json!("x".repeat(bytes))) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

/// `n` calls to the `big` tool in one turn.
fn big_calls(n: usize) -> common::Reply {
    let ids: Vec<String> = (0..n).map(|i| format!("b{i}")).collect();
    let list: Vec<(&str, &str, serde_json::Value)> = ids
        .iter()
        .map(|id| (id.as_str(), "big", json!({})))
        .collect();
    calls(&list, 1)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_result_over_the_result_cap_is_cut_and_the_run_goes_on() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![big_calls(1), answer("fine", 1)]);
    let harness = AgentHarness::new(model)
        .tool(big_tool(3 * 1024 * 1024))
        .tool_output_limit(usize::MAX);
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("fetch").loop_guard(no_guard())).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    let result = &tool_results(&report)[0];
    assert!(
        result.len()
            <= autumn_harvest_agent::harness::max_tool_result_bytes(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES
            )
    );
    assert!(result.ends_with("…[truncated]"));
}

/// The cap tests repeat one call on purpose. The loop guard would stop them
/// first.
const fn no_guard() -> autumn_harvest_agent::loop_guard::LoopGuard {
    autumn_harvest_agent::loop_guard::LoopGuard::disabled()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_round_whose_results_pass_the_cap_ends_the_run_mid_batch() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![big_calls(8), answer("unreachable", 1)]);
    let harness = AgentHarness::new(model.clone())
        .tool(big_tool(3 * 1024 * 1024))
        .tool_output_limit(usize::MAX);
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("fetch").loop_guard(no_guard())).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TranscriptFull);
    assert_eq!(model.calls(), 1);
    // Every call gets an answer, so the transcript stays valid and shows
    // which calls ran. The call that passed the cap ran, but its result is
    // dropped. The calls after it never ran.
    let results = tool_results(&report);
    assert_eq!(results.len(), 8);
    assert_eq!(report.tool_calls, 8);
    let dropped = results
        .iter()
        .position(|r| r.contains("its result was dropped"))
        .expect("one result is dropped");
    assert!(dropped > 0 && dropped < 7, "{dropped}");
    assert!(
        results[..dropped]
            .iter()
            .all(|r| r.ends_with("…[truncated]"))
    );
    assert!(
        results[dropped + 1..]
            .iter()
            .all(|r| r.contains("did not run"))
    );
    assert_eq!(report.messages.last().unwrap().role, ChatRole::Tool);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transcript_too_large_to_record_ends_the_run_before_the_next_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![big_calls(4), big_calls(3), answer("unreachable", 1)]);
    let harness = AgentHarness::new(model.clone())
        .tool(big_tool(3 * 1024 * 1024))
        .tool_output_limit(usize::MAX);
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("fill").loop_guard(no_guard())).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TranscriptFull);
    assert_eq!(model.calls(), 2, "the oversized request is never sent");
    assert_eq!(report.steps_used, 2);
    assert_eq!(tool_results(&report).len(), 7);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_output_capped_turn_ends_the_run_and_drops_its_calls() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let mut capped = calls(&[("a", "lookup", json!({}))], 1).unwrap();
    capped.stop_reason = autumn_harvest_agent::StopReason::MaxTokens;
    let model = ScriptedModel::new(vec![Ok(capped)]);
    let harness =
        AgentHarness::new(model).tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::OutputCapped);
    assert!(
        recorder.runs().is_empty(),
        "a cut turn may hold a partial call"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rate_limit_retries_and_an_auth_failure_does_not() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        Err(AgentError::new(ErrorKind::RateLimited, "slow down")),
        answer("after retry", 1),
    ]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));
    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    // The retry waits on its backoff timer.
    for _ in 0..10 {
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
        state = rt.run_until_blocked_as_of(exec, later(600)).await.unwrap();
    }
    assert_eq!(report(state).text, "after retry");
    assert_eq!(model.calls(), 2);

    let (_dir2, db2) = fresh_db();
    let model = ScriptedModel::new(vec![
        Err(AgentError::new(ErrorKind::Authentication, "bad key")),
        answer("never", 1),
    ]);
    let mut rt = runtime(&db2, AgentHarness::new(model.clone()));
    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref e) if e.contains("bad key")),
        "{state:?}"
    );
    assert_eq!(model.calls(), 1, "an auth failure is not retried");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_replays_recorded_turns_and_recorded_policy_decisions() {
    let (_dir, db) = fresh_db();
    let script = || {
        vec![
            calls(&[("w1", "write", json!({"path": "a"}))], 1),
            answer("finished", 1),
        ]
    };
    let ask = || CountingPolicy::new(ToolDecision::RequireApproval { reason: "r".into() });

    let first_model = ScriptedModel::new(script());
    let first_policy = ask();
    let first_tools = Arc::new(Recorder::default());
    let (exec, signal) = {
        let harness = AgentHarness::new(first_model.clone())
            .tool(recorded_tool("write", ToolEffect::Write, &first_tools))
            .policy(first_policy.clone());
        let mut rt = runtime(&db, harness);
        let exec = sqlite::start(&mut rt, &AgentTask::new("write")).unwrap();
        let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
            panic!("expected an approval wait");
        };
        (exec, signal)
    };
    assert_eq!(first_model.calls(), 1);
    assert_eq!(first_policy.asked(), 1);

    // A new runtime on the same file: the first turn and its decision replay.
    let mut second_script = script();
    second_script.drain(..1);
    let second_model = ScriptedModel::new(second_script);
    let second_policy = ask();
    let second_tools = Arc::new(Recorder::default());
    let harness = AgentHarness::new(second_model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &second_tools))
        .policy(second_policy.clone());
    let mut rt = runtime(&db, harness);
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.text, "finished");
    assert_eq!(
        second_model.calls(),
        1,
        "the recorded turn is not sent again"
    );
    assert_eq!(
        second_policy.asked(),
        0,
        "the recorded decision is not asked again"
    );
    assert_eq!(second_tools.runs().len(), 1);
    assert_eq!(first_tools.runs(), Vec::<serde_json::Value>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_decision_for_one_wait_never_releases_another() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    // The model reuses the call id `w` in two rounds.
    let model = ScriptedModel::new(vec![
        calls(&[("w", "write", json!({"n": 1}))], 1),
        calls(&[("w", "write", json!({"n": 2}))], 1),
        answer("done", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ));
    let mut rt = runtime(&db, harness);
    let task = AgentTask::new("twice").approval_timeout(Duration::from_secs(60));
    let exec = sqlite::start(&mut rt, &task).unwrap();

    let RunState::WaitingSignal(first) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("expected the first wait");
    };
    // The first wait times out. Its decision then arrives late.
    let state = rt.run_until_blocked_as_of(exec, later(120)).await.unwrap();
    let RunState::WaitingSignal(second) = state else {
        panic!("expected the second wait, got {state:?}");
    };
    assert_ne!(first, second, "each wait has its own name");
    sqlite::decide(&mut rt, exec, &first, &Approval::Approve).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref name) if *name == second),
        "the late decision must not release the second call: {state:?}"
    );
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_decision_denies_the_call_and_the_run_goes_on() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({}))], 1),
        answer("finished", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ));
    let mut rt = runtime(&db, harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("write")).unwrap();
    let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("expected an approval wait");
    };

    // The daemon's decision shape, not an `Approval`.
    rt.send_signal(exec, &signal, json!({"approved": true}))
        .unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(recorder.runs(), Vec::<serde_json::Value>::new());
    assert!(
        tool_results(&report)[0].contains("not a readable approval"),
        "{:?}",
        tool_results(&report)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lower_request_cap_stops_the_run_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("never", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));

    let task = AgentTask::new("x".repeat(1_000)).max_request_bytes(512);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.stop, AgentStop::TranscriptFull);
    assert_eq!(
        model.calls(),
        0,
        "a request over the worker cap is never sent"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_system_message_in_the_history_never_reaches_the_model() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("ok", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));

    let history = vec![
        autumn_harvest_agent::ChatMessage::text(ChatRole::System, "old prompt"),
        autumn_harvest_agent::ChatMessage::text(ChatRole::User, "earlier"),
    ];
    let task = AgentTask::new("now").system("new prompt").history(history);
    let exec = sqlite::start(&mut rt, &task).unwrap();
    let _ = report(rt.run_until_blocked(exec).await.unwrap());

    let sent = &model.requests()[0].messages;
    let systems = sent.iter().filter(|m| m.role == ChatRole::System).count();
    assert_eq!(systems, 1);
    assert_eq!(sent[0].role, ChatRole::System);
    assert_eq!(sent.len(), 3, "system, earlier, now");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_tool_is_an_error_result_not_a_failed_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("s", "slow", json!({}))], 1),
        answer("moved on", 1),
    ]);
    let slow = autumn_harvest_agent::FnTool::new(
        "slow",
        "Never finishes in time.",
        json!({"type": "object"}),
        |_input| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!("late"))
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    let harness = AgentHarness::new(model)
        .tool(slow)
        .tool_timeout(Duration::from_millis(50));
    let mut rt = runtime(&db, harness);

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let report = report(rt.run_until_blocked(exec).await.unwrap());

    assert_eq!(report.text, "moved on");
    assert!(tool_results(&report)[0].contains("did not finish within"));
}

/// `block_in_place` panics on a current-thread runtime. The activity fails
/// at once with a typed, non-retryable reason instead.
#[tokio::test(flavor = "current_thread")]
async fn a_current_thread_runtime_fails_the_call_and_names_the_fix() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("never", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()));

    let exec = sqlite::start(&mut rt, &AgentTask::new("go")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(ref e) if e.contains("multi-thread")),
        "{state:?}"
    );
    assert_eq!(model.calls(), 0);
}
