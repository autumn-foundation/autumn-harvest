//! The `agent_score` activity on SQLite (issue #2441).
//!
//! - A crash after a score replays the recorded verdict. Replay does not
//!   call the scorer again.
//! - A NaN score fails the activity with `ErrorKind::Decode`.
//! - The tokens of a judge scorer go to the cost ledger of the run that
//!   scores.
//!
//! The crash test runs its own binary again as a child process. The child
//! scores once, then aborts inside the next step. The parent opens the
//! same database and finishes the run. Both processes append each scorer
//! call to one log, so the test counts every call across the crash.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]
// The `#[workflow]` macro reads its input through a binding.
#![allow(clippy::used_underscore_binding)]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentStop, ChatMessage, ChatRequest, ChatResponse,
    ChatRole, ContentPart, ErrorKind, ScoreContext, ScoreInput, ScoreRequest, Scorer, StopReason,
    TokenUsage, Verdict, agent_score_info, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};

const DB_ENV: &str = "SCORE_CRASH_DB";
const LOG_ENV: &str = "SCORE_CRASH_LOG";

/// Scores with each scorer id in turn. Returns each verdict. An activity
/// failure returns its error type.
#[workflow]
async fn score_each(ctx: &WorkflowContext, scorers: Vec<String>) -> Result<Vec<Verdict>, String> {
    let mut verdicts = Vec::new();
    for scorer in scorers {
        let input = ScoreInput::new("Capital of France?", "Paris", AgentStop::Completed);
        let verdict: Verdict = ctx
            .execute_activity(&agent_score_info(), ScoreRequest::new(scorer, input))
            .await
            .map_err(|err| {
                err.activity_error_type()
                    .map_or_else(|| err.to_string(), str::to_owned)
            })?;
        verdicts.push(verdict);
    }
    Ok(verdicts)
}

#[derive(Debug)]
struct NoModel;

impl AgentModel for NoModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "no model")) })
    }
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

/// A scorer that logs and counts each call. With `abort` set, it aborts
/// the process inside the call, after the log.
#[derive(Debug)]
struct Counting {
    id: &'static str,
    value: f64,
    log_path: PathBuf,
    who: &'static str,
    abort: bool,
    calls: AtomicUsize,
}

impl Scorer for Counting {
    fn id(&self) -> &str {
        self.id
    }

    fn score<'a>(
        &'a self,
        _input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        log(&self.log_path, &format!("{} {}", self.who, self.id));
        if self.abort {
            std::process::abort();
        }
        Box::pin(async move { Verdict::score(self.value) })
    }
}

fn counting(
    id: &'static str,
    value: f64,
    log_path: &Path,
    who: &'static str,
    abort: bool,
) -> Arc<Counting> {
    Arc::new(Counting {
        id,
        value,
        log_path: log_path.to_path_buf(),
        who,
        abort,
        calls: AtomicUsize::new(0),
    })
}

/// A runtime with the `score_each` workflow and the agent activities.
fn runtime(db: &Path, harness: AgentHarness) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    rt.register_workflow(&score_each_info());
    sqlite::register(&mut rt, Arc::new(harness));
    rt
}

fn scorers(ids: &[&str]) -> serde_json::Value {
    serde_json::to_value(ids).unwrap()
}

fn exec_file(db: &Path) -> PathBuf {
    db.with_extension("exec")
}

fn lines(log_path: &Path, who: &str) -> Vec<String> {
    std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with(who))
        .map(str::to_owned)
        .collect()
}

fn completed_verdicts(events: &[WorkflowEvent]) -> Vec<Verdict> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { output, .. } => {
                Some(serde_json::from_value(output.clone()).unwrap())
            }
            _ => None,
        })
        .collect()
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_scores_then_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let db = PathBuf::from(db);
    let log_path = PathBuf::from(log_path);
    let harness = AgentHarness::new(Arc::new(NoModel))
        .scorer(counting("first", 0.5, &log_path, "child", false))
        .scorer(counting("next", 0.9, &log_path, "child", true));
    let mut rt = runtime(&db, harness);
    let exec = rt
        .start_workflow("score_each", scorers(&["first", "next"]))
        .unwrap();
    std::fs::write(exec_file(&db), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_score_replays_the_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("score.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_scores_then_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    assert_eq!(
        lines(&log_path, "child"),
        ["child first", "child next"],
        "the child dies in the step after the first score"
    );

    // The parent finishes the run with fresh scorers. Its first scorer
    // gives another value, so the output shows where the verdict came from.
    let first = counting("first", 0.1, &log_path, "parent", false);
    let next = counting("next", 0.9, &log_path, "parent", false);
    let harness = AgentHarness::new(Arc::new(NoModel))
        .scorer(Arc::clone(&first) as Arc<dyn Scorer>)
        .scorer(Arc::clone(&next) as Arc<dyn Scorer>);
    let mut rt = runtime(&db, harness);
    let exec: ExecutionId = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(value) = state else {
        panic!("{state:?}");
    };
    // Replay reads the recorded verdict, so the first scorer stays at the
    // one call of the child.
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(lines(&log_path, "parent"), ["parent next"]);
    let all_first = std::fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .filter(|line| line.ends_with(" first"))
        .count();
    assert_eq!(all_first, 1, "the first scorer ran once across the crash");

    let verdicts: Vec<Verdict> = serde_json::from_value(value).unwrap();
    assert_eq!(
        verdicts,
        [Verdict::score(0.5).unwrap(), Verdict::score(0.9).unwrap()]
    );

    // History holds one verdict per score.
    let events = rt.load_history(exec).unwrap();
    assert_eq!(completed_verdicts(&events), verdicts);

    // Replay rebuilds the run from the recorded verdicts alone. The
    // replayer holds no scorer, so none can run again.
    let report = WorkflowReplayer::new()
        .register(vec![score_each_info()])
        .replay_from_events(events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
    assert_eq!(
        std::fs::read_to_string(&log_path).unwrap().lines().count(),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nan_score_fails_the_run_with_decode() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("calls.log");
    let nan = counting("nan", f64::NAN, &log_path, "run", false);
    let harness = AgentHarness::new(Arc::new(NoModel)).scorer(Arc::clone(&nan) as Arc<dyn Scorer>);
    let mut rt = runtime(&dir.path().join("score.db"), harness);
    let exec = rt.start_workflow("score_each", scorers(&["nan"])).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Failed(error) = state else {
        panic!("{state:?}");
    };
    assert_eq!(error, "Decode");
    // Decode is not retryable, so the scorer ran once.
    assert_eq!(nan.calls.load(Ordering::SeqCst), 1);
}

/// A judge model that answers `0.8` and reports its usage.
#[derive(Debug)]
struct JudgeModel {
    calls: AtomicUsize,
}

impl AgentModel for JudgeModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ChatResponse {
                content: vec![ContentPart::Text("0.8".into())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(120, 3),
            })
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some("judge-2026")
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.input_tokens + usage.output_tokens))
    }
}

/// A judge scorer: two judge calls, then the mean of the two replies.
#[derive(Debug)]
struct TwoJudges;

impl Scorer for TwoJudges {
    fn id(&self) -> &str {
        "judge"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                tools: Vec::new(),
                max_tokens: Some(8),
                temperature: None,
            };
            let mut sum = 0.0;
            for model in [None, Some("judge")] {
                let reply = cx.chat(model, &request).await?;
                let Some(ContentPart::Text(text)) = reply.content.first() else {
                    return Ok(Verdict::reject("the judge gave no text"));
                };
                sum += text.parse::<f64>().unwrap_or(f64::NAN);
            }
            Verdict::score(sum / 2.0)
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn judge_tokens_land_in_the_ledger_of_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(JudgeModel {
        calls: AtomicUsize::new(0),
    });
    let harness = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
        .model_named("judge", Arc::clone(&model) as Arc<dyn AgentModel>)
        .scorer(Arc::new(TwoJudges));
    let mut rt = runtime(&dir.path().join("score.db"), harness);
    let exec = rt
        .start_workflow("score_each", scorers(&["judge"]))
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(value) = state else {
        panic!("{state:?}");
    };
    let verdicts: Vec<Verdict> = serde_json::from_value(value).unwrap();
    assert_eq!(verdicts, [Verdict::score(0.8).unwrap()]);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);

    let rows = rt.llm_ledger(exec).unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.exec_id, exec);
        assert_eq!(row.activity_name, "agent_score");
        assert_eq!(row.call_index, u32::try_from(index).unwrap());
        assert_eq!(row.model, "judge-2026");
        assert_eq!(row.input_tokens, 120);
        assert_eq!(row.output_tokens, 3);
        assert_eq!(row.cost_usd_micros, Some(123));
    }

    // A second drive of the finished run calls no judge.
    drop(rt);
    let again = Arc::new(JudgeModel {
        calls: AtomicUsize::new(0),
    });
    let harness = AgentHarness::new(Arc::clone(&again) as Arc<dyn AgentModel>)
        .model_named("judge", Arc::clone(&again) as Arc<dyn AgentModel>)
        .scorer(Arc::new(TwoJudges));
    let mut rt = runtime(&dir.path().join("score.db"), harness);
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_eq!(again.calls.load(Ordering::SeqCst), 0);
    assert_eq!(rt.llm_ledger(exec).unwrap().len(), 2);
}
