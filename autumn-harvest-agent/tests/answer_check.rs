//! The answer check in the agent loop (issue #2442).
//!
//! Each test maps to one acceptance line of the issue. The SQLite tests
//! count the model calls and the scorer calls. The crash test runs its own
//! binary again as a child process, as `crash_resume.rs` does. The budget
//! test mocks the activities in the test engine.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::IntoActivityErrorString as _;
use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::quota::QuotaResource;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, MAX_ANSWER_RETRIES, SamplingPolicy, SamplingPolicyStore, ScorerRef,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentReport, AgentStop, AgentTask,
    AnswerCheck, ChatMessage, ChatRequest, ChatResponse, ChatRole, ContentPart, Delivery,
    ModelTurn, Report, ScoreContext, ScoreInput, Scorer, StopReason, TokenUsage, Verdict,
    agent_loop_info, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};
use common::{Reply, ScriptedModel, answer, report, runtime};
use futures::FutureExt as _;
use serde_json::{Value, json};

const KIND: &str = "writer";
const SCORER: &str = "json_object";

const DB_ENV: &str = "ANSWER_CHECK_CRASH_DB";
const LOG_ENV: &str = "ANSWER_CHECK_CRASH_LOG";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn log(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

/// A scorer that passes an answer that is a JSON object. It counts each
/// call, and logs it when it has a log.
#[derive(Debug, Default)]
struct JsonObject {
    calls: AtomicUsize,
    log: Option<(PathBuf, &'static str)>,
}

impl JsonObject {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Scorer for JsonObject {
    fn id(&self) -> &str {
        SCORER
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((path, who)) = &self.log {
            log(path, &format!("{who} scorer"));
        }
        let verdict = match serde_json::from_str::<Value>(&input.answer) {
            Ok(Value::Object(_)) => Verdict::score(1.0),
            _ => Ok(Verdict::reject(format!(
                "the answer {:?} is not a JSON object",
                input.answer
            ))),
        };
        Box::pin(async move { verdict })
    }
}

fn store_with(max_answer_retries: u32) -> Arc<InMemorySamplingPolicyStore> {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    let policy = SamplingPolicy {
        max_answer_retries,
        ..SamplingPolicy::default()
    };
    store
        .set(KIND, &policy, "ops@example.com")
        .now_or_never()
        .expect("the in-memory store is ready at once")
        .expect("the policy is valid");
    store
}

fn check(retries: u32) -> AnswerCheck {
    AnswerCheck::new(KIND, ScorerRef::deterministic(SCORER).unwrap()).retries(retries)
}

fn task(retries: u32) -> AgentTask {
    AgentTask::new("Give the result as a JSON object.").answer_check(check(retries))
}

/// A harness with a profile of [`KIND`], the scorer, and a policy that
/// allows `cap` retries.
fn harness(model: Arc<dyn AgentModel>, scorer: &Arc<JsonObject>, cap: u32) -> AgentHarness {
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::clone(scorer) as Arc<dyn Scorer>)
        .sampling_policies(store_with(cap))
}

async fn drive(rt: &mut SqliteRuntime, task: &AgentTask) -> (ExecutionId, RunState) {
    let exec = sqlite::start(rt, task).expect("the run starts");
    let state = rt.run_until_blocked(exec).await.expect("the run drives");
    (exec, state)
}

/// The text of the last user message of `request`.
fn last_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role == autumn_harvest_agent::ChatRole::User)
        .map(text_of)
        .unwrap_or_default()
}

fn text_of(message: &ChatMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// Replay the history of `exec` and expect no nondeterminism.
async fn replays(events: Vec<WorkflowEvent>, exec: ExecutionId) {
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

// ── AC 1: a valid first answer ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_valid_first_answer_costs_one_model_call_and_one_scorer_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer(r#"{"ok": true}"#, 5)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let (exec, state) = drive(&mut rt, &task(2)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, r#"{"ok": true}"#);
    assert_eq!(report.answer_rejections, Vec::<String>::new());
    assert_eq!(model.calls(), 1);
    assert_eq!(scorer.calls(), 1);
    replays(rt.load_history(exec).unwrap(), exec).await;
}

// ── AC 2: an invalid answer, then a valid one ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_answer_is_retried_with_the_reason() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("not json", 5), answer(r#"{"n": 1}"#, 5)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let (exec, state) = drive(&mut rt, &task(2)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, r#"{"n": 1}"#);
    assert_eq!(model.calls(), 2);
    assert_eq!(scorer.calls(), 2);
    let reason = r#"the answer "not json" is not a JSON object"#;
    assert_eq!(report.answer_rejections, [reason]);
    let second = &model.requests()[1];
    assert!(
        last_user(second).contains(reason),
        "the retry names the reason: {}",
        last_user(second)
    );
    // The rejected answer stays in the transcript, before the reason.
    let messages = &second.messages;
    let rejected = &messages[messages.len() - 2];
    assert_eq!(rejected.role, ChatRole::Assistant);
    assert_eq!(text_of(rejected), "not json");
    assert_eq!(report.usage.total(), 10, "the retry is in the usage");
    replays(rt.load_history(exec).unwrap(), exec).await;
}

// ── AC 3: the cap ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn three_rejects_with_a_cap_of_two_end_as_answer_rejected() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        answer("a", 1),
        answer("b", 1),
        answer("c", 1),
        answer(r#"{"never": "sent"}"#, 1),
    ]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let (exec, state) = drive(&mut rt, &task(2)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::AnswerRejected);
    assert_eq!(model.calls(), 3);
    assert_eq!(scorer.calls(), 3);
    assert_eq!(report.answer_rejections.len(), 3);
    assert!(report.answer_rejections[2].contains("\"c\""));
    assert_eq!(report.text, "c", "the report keeps the last answer");
    assert_eq!(
        serde_json::to_value(AgentStop::AnswerRejected).unwrap(),
        json!("answer_rejected")
    );
    replays(rt.load_history(exec).unwrap(), exec).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn with_no_retries_one_reject_ends_the_run() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 1), answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    // No store: every default, so the cap is 0.
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::clone(&scorer) as Arc<dyn Scorer>);
    let mut rt = runtime(&db, harness);
    let (_exec, state) = drive(&mut rt, &task(0)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::AnswerRejected);
    assert_eq!(model.calls(), 1);
    assert_eq!(scorer.calls(), 1);
}

// ── AC 4: a crash mid-retry ─────────────────────────────────────────────────

fn crash_script() -> Vec<Reply> {
    vec![answer("not json", 1), answer(r#"{"ok": 1}"#, 1)]
}

/// A model that logs each call. With `abort_on` set, it aborts the process
/// inside that call, after the log.
#[derive(Debug)]
struct LoggedModel {
    inner: Arc<ScriptedModel>,
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
}

impl AgentModel for LoggedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        log(&self.log_path, &format!("{} model", self.who));
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }
}

fn logged(
    replies: Vec<Reply>,
    log_path: &Path,
    who: &'static str,
    abort_on: Option<usize>,
) -> (Arc<LoggedModel>, Arc<JsonObject>) {
    let model = Arc::new(LoggedModel {
        inner: ScriptedModel::new(replies),
        log_path: log_path.to_path_buf(),
        who,
        abort_on,
    });
    let scorer = Arc::new(JsonObject {
        calls: AtomicUsize::new(0),
        log: Some((log_path.to_path_buf(), who)),
    });
    (model, scorer)
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes_in_the_retry() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let (db, log_path) = (PathBuf::from(db), PathBuf::from(log_path));
    let (model, scorer) = logged(crash_script(), &log_path, "child", Some(2));
    let mut rt = runtime(&db, harness(model, &scorer, 1));
    let exec = sqlite::start(&mut rt, &task(1)).unwrap();
    std::fs::write(db.with_extension("exec"), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_mid_retry_repeats_no_model_call_and_no_scorer_call() {
    let (_dir, db) = fresh_db();
    let log_path = db.with_extension("log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_runs_until_it_crashes_in_the_retry",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    let before = std::fs::read_to_string(&log_path).unwrap();
    assert_eq!(
        before.lines().collect::<Vec<_>>(),
        ["child model", "child scorer", "child model"],
        "the child dies inside the retry model call"
    );

    // The parent knows only the retry turn. A policy of 0 retries shows
    // that the recorded policy read is used, not a new one.
    let mut rest = crash_script();
    rest.drain(..1);
    let (model, scorer) = logged(rest, &log_path, "parent", None);
    let mut rt = runtime(&db, harness(model, &scorer, 0));
    let exec: ExecutionId = std::fs::read_to_string(db.with_extension("exec"))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let report = report(state);
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.answer_rejections.len(), 1);
    let parent: Vec<String> = std::fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("parent"))
        .map(str::to_owned)
        .collect();
    // The first model call and its verdict completed before the crash.
    // Neither runs again. The retry call in flight is sent again.
    assert_eq!(parent, ["parent model", "parent scorer"]);
    replays(rt.load_history(exec).unwrap(), exec).await;
}

// ── AC 5: retries count against the token and budget limits ─────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_past_the_token_limit_ends_as_tokens_exhausted() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 10), answer("b", 10), answer("{}", 10)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 5));
    let (_exec, state) = drive(&mut rt, &task(5).max_total_tokens(15)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::TokensExhausted);
    assert_eq!(
        model.calls(),
        2,
        "the retry is the call that spends past the cap"
    );
    assert_eq!(scorer.calls(), 1, "an answer past the cap is not scored");
    assert_eq!(report.usage.total(), 20);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_answer_past_the_token_limit_is_not_scored() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 10), answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 5));
    let (_exec, state) = drive(&mut rt, &task(5).max_total_tokens(9)).await;
    // The first answer passes the cap, so it is not scored.
    assert_eq!(report(state).stop, AgentStop::TokensExhausted);
    assert_eq!(model.calls(), 1);
    assert_eq!(scorer.calls(), 0);
}

fn turn(text: &str) -> Value {
    serde_json::to_value(ModelTurn {
        content: vec![ContentPart::Text(text.to_owned())],
        stop: StopReason::EndTurn,
        usage: TokenUsage::new(5, 0),
        decisions: Vec::new(),
        cache_hit: false,
        labels: Vec::new(),
        releases: Vec::new(),
        oversized: false,
        interruptible: Vec::new(),
        cost_usd_micros: None,
    })
    .unwrap()
}

fn budget_refusal() -> String {
    LlmBudgetExceeded {
        resource: QuotaResource::TenantLlmTokens,
        limit: 100,
        current: 120,
    }
    .into_failure()
    .into_error_payload()
}

fn admitted(cap: u32) -> Value {
    json!({"admission": "admitted", "cap": cap, "revision": 1})
}

#[tokio::test]
async fn a_retry_refused_by_the_llm_budget_ends_as_budget_exceeded() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("agent_answer_check", vec![Ok(admitted(3))])
        .mock_activity_retries("agent_model_turn", vec![Ok(turn("not json"))])
        .mock_activity_retries("agent_model_turn", vec![Err(budget_refusal())])
        .mock_activity_retries("agent_score", vec![Ok(json!({"reject": "not JSON"}))])
        .run(
            agent_loop_info().handler,
            serde_json::to_value(task(3).tenant("acme")).unwrap(),
        )
        .await;
    let report: AgentReport = serde_json::from_value(outcome.result.unwrap()).unwrap();
    assert_eq!(report.stop, AgentStop::BudgetExceeded);
    assert_eq!(report.answer_rejections, ["not JSON"]);
    assert_eq!(
        report.text, "",
        "a bound after a retry does not report the rejected answer"
    );
}

#[tokio::test]
async fn a_judge_refused_by_the_llm_budget_ends_as_budget_exceeded() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("agent_answer_check", vec![Ok(admitted(3))])
        .mock_activity_retries("agent_model_turn", vec![Ok(turn("{}"))])
        .mock_activity_retries("agent_score", vec![Err(budget_refusal())])
        .run(
            agent_loop_info().handler,
            serde_json::to_value(task(3).tenant("acme")).unwrap(),
        )
        .await;
    let report: AgentReport = serde_json::from_value(outcome.result.unwrap()).unwrap();
    assert_eq!(report.stop, AgentStop::BudgetExceeded);
}

// ── AC 6: a task recorded before the change ─────────────────────────────────

/// A history that `trunk-dev` recorded before draws and before the answer
/// check: one tool round and one answer.
const OLD_HISTORY: &str = include_str!("fixtures/pre_draw_history.json");

#[tokio::test(flavor = "multi_thread")]
async fn a_history_from_before_the_check_decodes_and_replays() {
    assert!(!OLD_HISTORY.contains("answer_check"));
    let doc: Value = serde_json::from_str(OLD_HISTORY).unwrap();
    let exec: ExecutionId = serde_json::from_value(doc["execution_id"].clone()).unwrap();
    let events: Vec<WorkflowEvent> = serde_json::from_value(doc["events"].clone()).unwrap();
    let task: AgentTask = match &events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            serde_json::from_value(input.clone()).unwrap()
        }
        other => panic!("expected a start, got {other:?}"),
    };
    assert_eq!(task.answer_check, None);
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
async fn a_task_with_no_check_records_no_check_and_no_policy_read() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("plain", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let (exec, state) = drive(&mut rt, &AgentTask::new("hi")).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(scorer.calls(), 0);
    assert!(
        serde_json::to_value(&report)
            .unwrap()
            .get("answer_rejections")
            .is_none()
    );
    let history = rt.load_history(exec).unwrap();
    let names: Vec<&str> = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(names, ["agent_model_turn"]);
    let input = serde_json::to_value(AgentTask::new("hi")).unwrap();
    assert!(input.get("answer_check").is_none());
}

// ── AC 7: more retries than the policy allows ───────────────────────────────

fn refused(state: &RunState, code: &str) {
    match state {
        RunState::Failed(error) => assert!(error.contains(code), "{error}"),
        other => panic!("expected a refused run, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn more_retries_than_the_policy_allows_fail_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 1));
    let (_exec, state) = drive(&mut rt, &task(2)).await;
    refused(&state, "too_many_answer_retries");
    assert_eq!(model.calls(), 0);
    assert_eq!(scorer.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_check_with_report_failure_ends_as_failed() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 0));
    let mut task = task(1);
    task.report_failure = true;
    let (_exec, state) = drive(&mut rt, &task).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::Failed);
    assert!(
        report
            .error
            .as_deref()
            .is_some_and(|error| error.contains("too_many_answer_retries")),
        "{:?}",
        report.error
    );
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_judge_outside_the_policy_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let judge = AnswerCheck::new(KIND, ScorerRef::judge(SCORER, "big").unwrap());
    let (_exec, state) = drive(&mut rt, &AgentTask::new("x").answer_check(judge)).await;
    refused(&state, "scorer_not_allowed");
    assert_eq!(model.calls(), 0);
}

#[test]
fn the_task_cannot_carry_the_policy_cap() {
    let mut value = serde_json::to_value(task(1)).unwrap();
    value["max_answer_retries"] = json!(MAX_ANSWER_RETRIES);
    assert!(serde_json::from_value::<AgentTask>(value).is_err());
    let mut value = serde_json::to_value(task(1)).unwrap();
    value["answer_check"]["max_retries"] = json!(3);
    assert!(
        serde_json::from_value::<AgentTask>(value).is_err(),
        "an unknown check key fails closed"
    );
    let value = serde_json::to_value(task(1)).unwrap();
    assert_eq!(
        value["answer_check"],
        json!({"kind": KIND, "scorer": {"id": SCORER}, "retries": 1})
    );
    let back: AgentTask = serde_json::from_value(value).unwrap();
    assert_eq!(back.answer_check, Some(check(1)));
}

// ── Review findings: admission, judges, delivery ────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_scorer_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND))
        .sampling_policies(store_with(2));
    let mut rt = runtime(&db, harness);
    let (_exec, state) = drive(&mut rt, &task(1)).await;
    refused(&state, "unknown_scorer");
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_with_no_profile_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let other = AnswerCheck::new("nobody", ScorerRef::deterministic(SCORER).unwrap());
    let (_exec, state) = drive(&mut rt, &AgentTask::new("x").answer_check(other)).await;
    refused(&state, "unknown_kind");
    let bad = AnswerCheck::new("Not A Kind", ScorerRef::deterministic(SCORER).unwrap());
    let (_exec, state) = drive(&mut rt, &AgentTask::new("x").answer_check(bad)).await;
    refused(&state, "invalid_kind");
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_of_another_tenant_fails_before_any_model_call() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1), answer("{}", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(KIND).tenant("acme"))
        .scorer(Arc::clone(&scorer) as Arc<dyn Scorer>)
        .sampling_policies(store_with(2));
    let mut rt = runtime(&db, harness);
    let (_exec, state) = drive(&mut rt, &task(1).tenant("globex")).await;
    refused(&state, "other_tenant");
    let (_exec, state) = drive(&mut rt, &task(1)).await;
    refused(&state, "other_tenant");
    assert_eq!(model.calls(), 0);
    let (_exec, state) = drive(&mut rt, &task(1).tenant("acme")).await;
    assert_eq!(report(state).stop, AgentStop::Completed);
}

/// A scorer that asks the judge model `big`, whatever the check says.
#[derive(Debug)]
struct Sneaky;

impl Scorer for Sneaky {
    fn id(&self) -> &str {
        SCORER
    }

    fn score<'a>(
        &'a self,
        _input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: Vec::new(),
                tools: Vec::new(),
                max_tokens: None,
                temperature: None,
                draw: None,
            };
            // The context refuses the call before the model runs.
            match cx.chat(Some("big"), &request).await {
                Ok(_) => Verdict::score(1.0),
                Err(err) => Ok(Verdict::reject(err.to_string())),
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scorer_cannot_call_a_judge_that_the_check_did_not_admit() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("{}", 1)]);
    let big = ScriptedModel::new(vec![answer("1.0", 1)]);
    let harness = AgentHarness::new(model.clone())
        .model_named("big", big.clone())
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(Sneaky))
        .sampling_policies(store_with(2));
    let mut rt = runtime(&db, harness);
    let (_exec, state) = drive(&mut rt, &task(0)).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::AnswerRejected);
    assert!(
        report.answer_rejections[0].contains("did not admit the judge model"),
        "{:?}",
        report.answer_rejections
    );
    assert_eq!(big.calls(), 0, "the judge model is never called");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_output_capped_answer_is_not_scored() {
    let (_dir, db) = fresh_db();
    let capped = Ok(ChatResponse {
        content: vec![ContentPart::Text("{\"cut".to_owned())],
        stop_reason: StopReason::MaxTokens,
        usage: TokenUsage::new(1, 0),
    });
    let model = ScriptedModel::new(vec![capped]);
    let scorer = Arc::new(JsonObject::default());
    let mut rt = runtime(&db, harness(model.clone(), &scorer, 2));
    let (_exec, state) = drive(&mut rt, &task(2)).await;
    assert_eq!(report(state).stop, AgentStop::OutputCapped);
    assert_eq!(scorer.calls(), 0);
}

/// A delivery that keeps each report.
#[derive(Debug, Default)]
struct Kept(Mutex<Vec<Report>>);

impl Delivery for Kept {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.lock().unwrap().push(report.clone());
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_answer_is_never_delivered() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("secret one", 1), answer("secret two", 1)]);
    let scorer = Arc::new(JsonObject::default());
    let kept = Arc::new(Kept::default());
    let harness =
        harness(model.clone(), &scorer, 1).delivery(Arc::clone(&kept) as Arc<dyn Delivery>);
    let mut rt = runtime(&db, harness);
    let mut task = task(1);
    task.deliver = true;
    let (_exec, state) = drive(&mut rt, &task).await;
    let report = report(state);
    assert_eq!(report.stop, AgentStop::AnswerRejected);
    assert_eq!(
        report.text, "secret two",
        "the app still reads the last answer"
    );
    let sent = kept.0.lock().unwrap().clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].text, "The agent stopped early: answer_rejected.");
}
