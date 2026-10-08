//! A process crash in the middle of the loop. The resumed run repeats no
//! completed model call and no completed tool call.
//!
//! Each test runs its own binary again as a child process. The child drives
//! one run and calls `abort` inside the second tool call, or inside the second
//! model call. The parent then opens the same database and finishes the run.
//! Both processes append to one log, so the test can count every call across
//! the crash.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::{AgentHarness, AgentReport, AgentStop, AgentTask, sqlite};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use autumn_plugin_agent::{FnTool, Tool, ToolEffect};
use common::{Reply, ScriptedModel, answer, calls, report, runtime};
use serde_json::{Value, json};

const DB_ENV: &str = "AGENT_CRASH_DB";
const LOG_ENV: &str = "AGENT_CRASH_LOG";
const AT_ENV: &str = "AGENT_CRASH_AT";

/// The full script: two tool rounds, then an answer.
fn script() -> Vec<Reply> {
    vec![
        calls(&[("c1", "step", json!({"n": 1}))], 1),
        calls(&[("c2", "step", json!({"n": 2, "crash": true}))], 1),
        answer("done", 1),
    ]
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

/// A model that logs each call, then answers from `replies`. With
/// `abort_on` set, it aborts the process inside that call, after the log.
fn logged_model(
    replies: Vec<Reply>,
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
) -> Arc<LoggedModel> {
    Arc::new(LoggedModel {
        inner: ScriptedModel::new(replies),
        log_path,
        who,
        abort_on,
    })
}

#[derive(Debug)]
struct LoggedModel {
    inner: Arc<ScriptedModel>,
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<usize>,
}

impl autumn_plugin_agent::LlmClient for LoggedModel {
    fn chat<'a>(
        &'a self,
        request: &'a autumn_plugin_agent::ChatRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        autumn_plugin_agent::ChatResponse,
                        autumn_plugin_agent::AgentError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        log(&self.log_path, &format!("{} model", self.who));
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }

    fn list_models(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<String>, autumn_plugin_agent::AgentError>>
                + Send
                + '_,
        >,
    > {
        self.inner.list_models()
    }

    fn provider_name(&self) -> &'static str {
        "logged"
    }
}

/// The `step` tool. It logs each run. In the child it aborts on a call that
/// asks for a crash, after it logged the start.
fn step_tool(log_path: PathBuf, who: &'static str, crash: bool) -> Arc<dyn Tool> {
    FnTool::new(
        "step",
        "One step of work.",
        json!({"type": "object"}),
        move |input: Value| {
            let log_path = log_path.clone();
            async move {
                log(&log_path, &format!("{who} tool {}", input["n"]));
                if crash && input["crash"] == json!(true) {
                    std::process::abort();
                }
                Ok(json!({"ok": input["n"]}))
            }
        },
    )
    .effect(ToolEffect::ReadOnly)
    .shared()
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes() {
    let (Some(db), Some(log_path), Some(at)) = (
        std::env::var_os(DB_ENV),
        std::env::var_os(LOG_ENV),
        std::env::var_os(AT_ENV),
    ) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let in_model = at == "model";
    let model = logged_model(script(), log_path.clone(), "child", in_model.then_some(2));
    let harness = AgentHarness::new(model).tool(step_tool(log_path, "child", !in_model));
    let mut rt = runtime(Path::new(&db), harness);
    let exec = sqlite::start(&mut rt, &AgentTask::new("work")).unwrap();
    std::fs::write(exec_file(Path::new(&db)), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

/// Run the child until it aborts at `at`. Returns the database, the log, and
/// the temporary directory that holds them.
fn crash_child(at: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_runs_until_it_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .env(AT_ENV, at)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    (dir, db, log_path)
}

/// Finish the crashed run in this process with `rest` as the model script.
/// Returns the report and the lines this process logged.
async fn resume(db: &Path, log_path: &Path, rest: Vec<Reply>) -> (AgentReport, Vec<String>) {
    let harness = AgentHarness::new(logged_model(rest, log_path.to_path_buf(), "parent", None))
        .tool(step_tool(log_path.to_path_buf(), "parent", false));
    let mut rt = runtime(db, harness);
    let exec: ExecutionId = std::fs::read_to_string(exec_file(db))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let parent = std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("parent"))
        .map(str::to_owned)
        .collect();
    (report(state), parent)
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_inside_a_model_call_resends_only_that_call() {
    let (_dir, db, log_path) = crash_child("model");
    let before = std::fs::read_to_string(&log_path).unwrap();
    assert_eq!(
        before.lines().collect::<Vec<_>>(),
        ["child model", "child tool 1", "child model"],
        "the child dies inside the second model call"
    );

    // The parent knows the turn in flight and the one after it.
    let mut rest = script();
    rest.drain(..1);
    let (report, parent) = resume(&db, &log_path, rest).await;

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "done");
    // The first model call and the first tool call completed before the
    // crash. Neither runs again. The model call in flight is sent again.
    assert_eq!(parent, ["parent model", "parent tool 2", "parent model"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_mid_loop_resumes_without_rerunning_completed_calls() {
    let (_dir, db, log_path) = crash_child("tool");
    let before = std::fs::read_to_string(&log_path).unwrap();
    assert_eq!(
        before.lines().collect::<Vec<_>>(),
        ["child model", "child tool 1", "child model", "child tool 2"],
        "the child dies inside the second tool call"
    );

    // The parent knows only the turn the child never reached.
    let mut rest = script();
    rest.drain(..2);
    let (report, parent) = resume(&db, &log_path, rest).await;

    assert_eq!(report.stop, AgentStop::Completed);
    assert_eq!(report.text, "done");
    assert_eq!(report.steps_used, 2);
    // Two model calls and the first tool call completed before the crash.
    // None of them runs again. The tool call that was in flight runs again,
    // because activity execution is at-least-once.
    assert_eq!(parent, ["parent tool 2", "parent model"]);
}

/// The file where the child records its execution id.
fn exec_file(db: &Path) -> PathBuf {
    db.with_extension("exec")
}
