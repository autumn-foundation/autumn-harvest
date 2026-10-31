//! The durable agent on the SQLite backend (issue #2263).
//!
//! Each test drives the `agent` workflow through its inbox. The tests cover
//! the guarantees of `docs/design/DESIGN-2262.md` section 1. One op runs at a time, no op
//! is lost, replay asks no model twice, and labels survive a checkpoint.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentOp, AgentProfile, AgentState, AgentStatus,
    AgentStop, Approval, BoxFuture, ChatRole, Control, FlowRules, FnTool, InMemoryMemoryStore,
    MemoryScope, MemoryStore, MeshBudget, MeshSpend, ModelTurnRequest, Rule, Spend, ToolEffect,
    agent_info, agent_profile_info, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn id(name: &str) -> AgentId {
    AgentId::new("support", name).unwrap()
}

fn say(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) {
    sqlite::send(rt, agent, &AgentOp::message(text), None).unwrap();
}

fn control(rt: &mut SqliteRuntime, agent: &AgentId, control: Control) {
    sqlite::send(rt, agent, &AgentOp::Control(control), None).unwrap();
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().expect("the agent runs") }
}

/// The text of each message of `messages`, with its role.
fn texts(messages: &[autumn_harvest_agent::ChatMessage]) -> Vec<(ChatRole, String)> {
    messages
        .iter()
        .map(|message| {
            let text = message
                .content
                .iter()
                .filter_map(|part| match part {
                    autumn_harvest_agent::ContentPart::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            (message.role, text)
        })
        .collect()
}

/// Every event of every run of the agent, oldest run first.
fn agent_history(rt: &SqliteRuntime, agent: &AgentId) -> Vec<Vec<WorkflowEvent>> {
    let filter = autumn_harvest_sqlite::ExecutionFilter::new()
        .workflow_name("agent")
        .workflow_id(agent.as_str());
    let mut runs: Vec<_> = rt
        .list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .collect();
    runs.reverse();
    runs.into_iter()
        .map(|exec| rt.load_history(exec).unwrap())
        .collect()
}

/// The inputs of every recorded model turn of the agent, in order.
fn turn_requests(rt: &SqliteRuntime, agent: &AgentId) -> Vec<ModelTurnRequest> {
    agent_history(rt, agent)
        .into_iter()
        .flatten()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == "agent_model_turn" => {
                Some(serde_json::from_value(input).unwrap())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn two_messages_run_one_at_a_time_in_order() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("reply one", 1), answer("reply two", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-42");

    // Both messages wait in the inbox before the agent runs.
    say(&mut rt, &agent, "one");
    say(&mut rt, &agent, "two");
    let blocked = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(blocked, Some(RunState::WaitingSignal(_))),
        "{blocked:?}"
    );

    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        texts(&requests[1].messages),
        vec![
            (ChatRole::User, "one".to_owned()),
            (ChatRole::Assistant, "reply one".to_owned()),
            (ChatRole::User, "two".to_owned()),
        ],
        "the second op sees the first reply"
    );
    let state = state(&rt, &agent).await;
    assert_eq!(texts(&state.transcript).len(), 4);
    assert_eq!(state.segments, 2);
    assert_eq!(state.last_answer.as_deref(), Some("reply two"));
    assert_eq!(state.tokens_used, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_at_an_approval_asks_no_recorded_model_call_again() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let mut script = vec![
        calls(&[("w1", "write", json!({"n": 1}))], 1),
        answer("written", 1),
    ];
    let first = ScriptedModel::new(script.clone());
    let profile = AgentProfile::new("support");
    let harness = |model: Arc<ScriptedModel>| {
        AgentHarness::new(model)
            .tool(recorded_tool("write", ToolEffect::Write, &recorder))
            .policy(Arc::new(
                autumn_harvest_agent::ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
            ))
            .agent(profile.clone())
    };
    let agent = id("acct-1");
    let mut rt = runtime(&db, harness(first.clone()));
    say(&mut rt, &agent, "save it");
    let waiting = sqlite::drive(&mut rt, &agent).await.unwrap();
    let Some(RunState::WaitingSignal(signal)) = waiting else {
        panic!("the op waits on its approval, got {waiting:?}");
    };
    assert!(signal.starts_with("tool_approval:"), "{signal}");
    assert_eq!(first.calls(), 1);
    drop(rt);

    // A new process: a model with only the turns that have not run.
    script.drain(..1);
    let second = ScriptedModel::new(script);
    let mut rt = runtime(&db, harness(second.clone()));
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(
        second.calls(),
        1,
        "the recorded first turn is not asked again"
    );
    assert_eq!(recorder.runs().len(), 1);
    let state = state(&rt, &agent).await;
    assert_eq!(state.last_answer.as_deref(), Some("written"));
    assert_eq!(state.last_stop, Some(AgentStop::Completed));
}

#[tokio::test(flavor = "multi_thread")]
async fn flow_labels_survive_a_checkpoint() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 1),
        answer("a stranger wrote", 1),
        answer("noted", 1),
    ]);
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail from a stranger")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let harness = AgentHarness::new(model.clone())
        .tool(inbox)
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ))
        // One op per run, so the second op runs after a checkpoint.
        .agent(AgentProfile::new("support").max_ops_per_run(1));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-2");
    say(&mut rt, &agent, "read the inbox");
    say(&mut rt, &agent, "what now?");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let runs = agent_history(&rt, &agent);
    assert!(runs.len() >= 2, "the agent took a checkpoint");
    let requests = turn_requests(&rt, &agent);
    let last = requests.last().unwrap();
    assert!(
        last.flow_labels.contains(labels::UNTRUSTED_INPUT),
        "the turn after the checkpoint carries the label: {:?}",
        last.flow_labels
    );
    let state = state(&rt, &agent).await;
    assert!(state.flow_labels.contains(labels::UNTRUSTED_INPUT));
    assert_eq!(
        texts(&state.transcript).len(),
        6,
        "the transcript survives too"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_paused_agent_holds_work_and_runs_it_in_order_on_resume() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("r1", 1), answer("r2", 1), answer("r3", 1)]);
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-3");
    control(
        &mut rt,
        &agent,
        Control::Pause {
            reason: Some("maintenance".into()),
        },
    );
    say(&mut rt, &agent, "m1");
    say(&mut rt, &agent, "m2");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert_eq!(model.calls(), 0, "a paused agent runs no work");
    let paused = state(&rt, &agent).await;
    assert_eq!(
        paused.status,
        AgentStatus::Paused {
            reason: Some("maintenance".into())
        }
    );
    assert_eq!(paused.held.len(), 2);

    control(&mut rt, &agent, Control::Resume);
    say(&mut rt, &agent, "m3");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(state.status, AgentStatus::Open);
    assert!(state.held.is_empty());
    let users: Vec<String> = texts(&state.transcript)
        .into_iter()
        .filter(|(role, _)| *role == ChatRole::User)
        .map(|(_, text)| text)
        .collect();
    assert_eq!(users, ["m1", "m2", "m3"], "held work runs first, in order");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_op_keeps_the_transcript_and_the_labels() {
    let (_dir, db) = fresh_db();
    // The second turn fails for good: the script has a provider error.
    let model = ScriptedModel::new(vec![
        answer("hello", 1),
        calls(&[("c1", "inbox", json!({}))], 1),
        Err(autumn_harvest_agent::AgentError::new(
            autumn_harvest_agent::ErrorKind::Provider,
            "the provider refused",
        )),
    ]);
    let inbox = FnTool::new(
        "inbox",
        "Read mail.",
        json!({"type": "object"}),
        |_| async { Ok(json!("mail")) },
    )
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let harness = AgentHarness::new(model.clone())
        .tool(inbox)
        .flow_rules(
            FlowRules::new().forbid([labels::UNTRUSTED_INPUT, "effect:external"], Rule::Ask),
        )
        .agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-4");
    say(&mut rt, &agent, "hi");
    say(&mut rt, &agent, "read the inbox");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 1);
    assert!(state.last_error.unwrap().contains("the provider refused"));
    assert_eq!(
        texts(&state.transcript),
        vec![
            (ChatRole::User, "hi".to_owned()),
            (ChatRole::Assistant, "hello".to_owned()),
        ],
        "the failed op leaves no half turn"
    );
    assert!(
        state.flow_labels.contains(labels::UNTRUSTED_INPUT),
        "the inbox ran, so its taint stays"
    );
    assert_eq!(state.tokens_used, 2, "the paid turns stay counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn approval_names_never_repeat_across_ops() {
    let (_dir, db) = fresh_db();
    let recorder = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("same", "write", json!({}))], 1),
        answer("one", 1),
        calls(&[("same", "write", json!({}))], 1),
        answer("two", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &recorder))
        .policy(Arc::new(
            autumn_harvest_agent::ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-5");
    let mut names = Vec::new();
    for text in ["first", "second"] {
        say(&mut rt, &agent, text);
        let Some(RunState::WaitingSignal(signal)) = sqlite::drive(&mut rt, &agent).await.unwrap()
        else {
            panic!("each op waits on an approval");
        };
        let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
        sqlite::decide(&mut rt, exec, &signal, &Approval::Approve).unwrap();
        sqlite::drive(&mut rt, &agent).await.unwrap();
        names.push(signal);
    }
    assert_ne!(
        names[0], names[1],
        "the same call id in a later op gets a new name"
    );
    assert_eq!(recorder.runs().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_kind_fails_each_op_and_keeps_the_agent() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![]);
    let harness = AgentHarness::new(model.clone());
    let mut rt = runtime(&db, harness);
    let agent = id("nobody");
    say(&mut rt, &agent, "hello?");
    let blocked = sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        matches!(blocked, Some(RunState::WaitingSignal(_))),
        "{blocked:?}"
    );
    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 1);
    assert!(state.last_error.unwrap().contains("no agent profile"));
    assert_eq!(model.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_ends_the_life_and_a_new_send_starts_another() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("a", 1), answer("b", 1)]);
    let harness = AgentHarness::new(model).agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-6");
    say(&mut rt, &agent, "one");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let first = state(&rt, &agent).await.incarnation;
    assert_ne!(first, String::new());

    sqlite::delete(&mut rt, &agent, None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    assert!(
        sqlite::current_run(&rt, &agent).unwrap().is_none(),
        "the run ended"
    );

    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let second = state(&rt, &agent).await;
    assert_ne!(second.incarnation, first, "a new life gets new run ids");
    assert_eq!(
        texts(&second.transcript).len(),
        2,
        "a new life starts empty"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_memory_clears_the_scope_of_the_agent() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(InMemoryMemoryStore::new());
    let model = ScriptedModel::new(vec![
        calls(
            &[(
                "m1",
                "memory",
                json!({"action": "add", "block": "memory", "text": "likes tea"}),
            )],
            1,
        ),
        answer("noted", 1),
    ]);
    let harness = AgentHarness::new(model)
        .memory(store.clone())
        .agent(AgentProfile::new("support").memory(AgentMemory::Store));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-7");
    let scope = MemoryScope::new("agent:support/acct-7");
    say(&mut rt, &agent, "remember tea");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let blocks = store.load(&scope).await.unwrap();
    assert!(
        blocks.iter().any(|b| b.entries == ["likes tea"]),
        "{blocks:?}"
    );

    control(&mut rt, &agent, Control::ResetMemory);
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let blocks = store.load(&scope).await.unwrap();
    assert!(blocks.iter().all(|b| b.entries.is_empty()), "{blocks:?}");
    assert_eq!(state(&rt, &agent).await.failures, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_agent_history_replays() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "lookup", json!({}))], 1),
        answer("found", 1),
        answer("bye", 1),
    ]);
    let lookup = FnTool::new("lookup", "Look up.", json!({"type": "object"}), |_| async {
        Ok(json!("data"))
    })
    .effect(ToolEffect::ReadOnly)
    .shared();
    let harness = AgentHarness::new(model)
        .tool(lookup)
        .agent(AgentProfile::new("support"));
    let mut rt = runtime(&db, harness);
    let agent = id("acct-8");
    say(&mut rt, &agent, "look");
    say(&mut rt, &agent, "thanks");
    sqlite::delete(&mut rt, &agent, None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let runs = agent_history(&rt, &agent);
    assert_eq!(runs.len(), 1);
    let report = WorkflowReplayer::new()
        .register(vec![agent_info()])
        .with_workflow_id(agent.as_str())
        .replay_from_events(runs[0].clone())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_profile_lookup_keeps_the_run_and_its_state() {
    let (_dir, db) = fresh_db();
    let agent = id("acct-9");
    // One op per run, so the next run starts from a checkpoint.
    let profile = AgentProfile::new("support").max_ops_per_run(1);
    let model = ScriptedModel::new(vec![answer("first", 1)]);
    let mut rt = runtime(&db, AgentHarness::new(model).agent(profile.clone()));
    say(&mut rt, &agent, "one");
    let exec = sqlite::current_run(&rt, &agent).unwrap().unwrap();
    let RunState::ContinuedAsNew(next) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("the op ends its run");
    };
    drop(rt);

    // A new process whose profile lookup fails, as with no harness.
    let model = ScriptedModel::new(vec![]);
    let mut rt = runtime(&db, AgentHarness::new(model.clone()).agent(profile));
    rt.register_activity_with_body(&agent_profile_info(), |_| {
        Err(
            ActivityFailure::non_retryable("AgentHarnessMissing", "no AgentHarness is installed")
                .into_error_payload(),
        )
    });
    let blocked = rt.run_until_blocked(next).await.unwrap();
    assert!(
        matches!(blocked, RunState::WaitingSignal(_)),
        "the run waits for work: {blocked:?}"
    );
    say(&mut rt, &agent, "two");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let state = state(&rt, &agent).await;
    assert_eq!(state.failures, 1);
    let error = state.last_error.unwrap_or_default();
    assert!(
        error.contains("agent_profile") && error.contains("no AgentHarness"),
        "the error names the lookup: {error}"
    );
    assert_eq!(model.calls(), 0);
    assert_eq!(state.last_answer.as_deref(), Some("first"));
    assert_eq!(
        texts(&state.transcript).len(),
        2,
        "the carried transcript survives"
    );
}

/// A spend source that reports a spent budget. A read past `max_reads`
/// fails, so the turn waits on a retry backoff. An op that loops stops
/// there, and the test can count its reads.
#[derive(Debug)]
struct SpentBudget {
    reads: AtomicUsize,
    max_reads: usize,
}

impl SpentBudget {
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl MeshSpend for SpentBudget {
    fn spent<'a>(
        &'a self,
        _mesh: &'a str,
        _since: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Spend, AgentError>> {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        let spend = if read <= self.max_reads {
            Ok(Spend {
                tokens: 1_000,
                cost_usd_micros: 0,
            })
        } else {
            Err(AgentError::new(
                autumn_harvest_agent::ErrorKind::Unavailable,
                "the ledger is down",
            ))
        };
        Box::pin(async move { spend })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_under_a_spent_budget_holds_the_message_and_does_not_loop() {
    let (_dir, db) = fresh_db();
    let spend = Arc::new(SpentBudget {
        reads: AtomicUsize::new(0),
        max_reads: 20,
    });
    let model = ScriptedModel::new(vec![answer("ran", 1)]);
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new("support").mesh("help"))
        .mesh_budget(MeshBudget::daily("help").max_tokens(10))
        .mesh_spend(spend.clone());
    let mut rt = runtime(&db, harness);
    let agent = id("acct-10");
    say(&mut rt, &agent, "hello");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let parked = state(&rt, &agent).await;
    assert!(
        matches!(parked.status, AgentStatus::Parked { .. }),
        "{:?}",
        parked.status
    );
    assert_eq!(spend.reads(), 1);

    control(&mut rt, &agent, Control::Resume);
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert!(
        matches!(state.status, AgentStatus::Parked { .. }),
        "the budget is still spent: {:?}",
        state.status
    );
    let held: Vec<String> = state
        .held
        .iter()
        .filter_map(|op| match op {
            AgentOp::UserMessage(message) => Some(message.text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(held, ["hello"], "the message waits, first in line");
    assert_eq!(spend.reads(), 2, "the resume tries the message once");
    assert_eq!(model.calls(), 0);
}

// ---------------------------------------------------------------------------
// A process crash inside a model call.
// ---------------------------------------------------------------------------

const DB_ENV: &str = "DURABLE_AGENT_CRASH_DB";
const LOG_ENV: &str = "DURABLE_AGENT_CRASH_LOG";

fn log(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

/// A model that logs each call. With `abort_on`, it aborts the process inside
/// that call, after the log line.
#[derive(Debug)]
struct LoggedModel {
    inner: Arc<ScriptedModel>,
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
}

impl autumn_harvest_agent::AgentModel for LoggedModel {
    fn chat<'a>(
        &'a self,
        request: &'a autumn_harvest_agent::ChatRequest,
    ) -> autumn_harvest_agent::BoxFuture<
        'a,
        Result<autumn_harvest_agent::ChatResponse, autumn_harvest_agent::AgentError>,
    > {
        log(&self.log_path, &format!("{} model", self.who));
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }
}

fn crash_script() -> Vec<Reply> {
    vec![
        calls(&[("c1", "step", json!({}))], 1),
        answer("first done", 1),
        answer("second done", 1),
    ]
}

fn crash_harness(
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
    skip: usize,
) -> AgentHarness {
    let mut script = crash_script();
    script.drain(..skip);
    let model = Arc::new(LoggedModel {
        inner: ScriptedModel::new(script),
        log_path: log_path.clone(),
        who,
        abort_on,
    });
    let step = FnTool::new("step", "One step.", json!({"type": "object"}), move |_| {
        let log_path = log_path.clone();
        async move {
            log(&log_path, &format!("{who} tool"));
            Ok(json!("ok"))
        }
    })
    .effect(ToolEffect::ReadOnly)
    .shared();
    AgentHarness::new(model)
        .tool(step)
        .agent(AgentProfile::new("support"))
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_agent_runs_until_it_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    // Abort inside the third model call: the first turn of the second op.
    let mut rt = runtime(Path::new(&db), crash_harness(log_path, "child", Some(3), 0));
    let agent = id("crash");
    say(&mut rt, &agent, "first");
    say(&mut rt, &agent, "second");
    let _ = sqlite::drive(&mut rt, &agent).await;
    panic!("the child must abort inside the model call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_inside_a_model_call_reruns_only_that_call() {
    if std::env::var_os(DB_ENV).is_some() {
        return;
    }
    let (dir, db) = fresh_db();
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_agent_runs_until_it_crashes",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child aborted");

    // The parent finishes the run. Its model skips the two recorded turns.
    let mut rt = runtime(&db, crash_harness(log_path.clone(), "parent", None, 2));
    let agent = id("crash");
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let state = state(&rt, &agent).await;
    assert_eq!(state.last_answer.as_deref(), Some("second done"));

    let lines: Vec<String> = std::fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(
        lines,
        [
            "child model",
            "child tool",
            "child model",
            "child model",
            "parent model",
        ],
        "only the call in flight at the crash runs again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn profile_tool_guidance_reaches_the_model_and_the_history() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![answer("ok", 1)]);
    let recorder = Arc::new(Recorder::default());
    let mut profile = AgentProfile::new("support");
    profile
        .tool_guidance
        .insert("lookup".to_owned(), "Look up one order.".to_owned());
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder))
        .tool(recorded_tool("other", ToolEffect::ReadOnly, &recorder))
        .agent(profile);
    let mut rt = runtime(&db, harness);
    let agent = id("guided");

    say(&mut rt, &agent, "where is it?");
    sqlite::drive(&mut rt, &agent).await.unwrap();

    let tools = &model.requests()[0].tools;
    let text = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .map(|tool| tool.description.clone())
    };
    assert_eq!(text("lookup").as_deref(), Some("Look up one order."));
    assert_eq!(text("other").as_deref(), Some("A test tool."));
    let requests = turn_requests(&rt, &agent);
    assert_eq!(
        requests[0].tool_guidance.get("lookup").map(String::as_str),
        Some("Look up one order.")
    );
}
