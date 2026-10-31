//! A worker crash in the middle of a proposer stage (issue #2379).
//!
//! A test workflow runs an INV stage, then a DOM stage. The child process
//! aborts inside the second attempt of the DOM stage. The parent opens the
//! same database and finishes the run. Both processes append each proposer
//! call to one log, so the test counts every call across the crash.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
    AgentError, AgentHarness, AgentModel, AgentProfile, Ceilings, ChatRequest, ChatResponse,
    Dimension, ErrorKind, Evidence, OverlayProposer, ProfileOverlay, ProposalAttempts,
    ProposeContext, ProposeRequest, ProposerReply, Stage, StageOutcome, agent_refine_propose_info,
    sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};

const DB_ENV: &str = "REFINE_CRASH_DB";
const LOG_ENV: &str = "REFINE_CRASH_LOG";

/// Runs an INV stage, then a DOM stage, each as one activity.
#[workflow]
async fn two_stages(ctx: &WorkflowContext, request: ProposeRequest) -> Result<Vec<Stage>, String> {
    let mut stages = Vec::new();
    for dimension in [Dimension::Inv, Dimension::Dom] {
        let stage: Stage = ctx
            .execute_activity(
                &agent_refine_propose_info(),
                ProposeRequest {
                    dimension,
                    ..request.clone()
                },
            )
            .await
            .map_err(|err| err.to_string())?;
        stages.push(stage);
    }
    Ok(stages)
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

/// A proposer that logs each call. Attempt 1 is past the ceilings, and
/// attempt 2 is valid. With `abort_dom` set, it aborts the process inside
/// attempt 2 of the DOM stage, after the log.
#[derive(Debug)]
struct LoggedProposer {
    log_path: PathBuf,
    who: &'static str,
    abort_dom: bool,
    calls: AtomicUsize,
}

impl OverlayProposer for LoggedProposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        log(
            &self.log_path,
            &format!("{} {} {}", self.who, context.dimension, context.attempt),
        );
        if self.abort_dom && context.dimension == Dimension::Dom && context.attempt == 2 {
            std::process::abort();
        }
        let reply = if context.attempt == 1 {
            ProfileOverlay {
                max_steps: Some(99),
                ..ProfileOverlay::default()
            }
        } else {
            proposal(context.dimension)
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

fn proposal(dimension: Dimension) -> ProfileOverlay {
    ProfileOverlay {
        system: Some(format!("A {dimension} edit.")),
        ..ProfileOverlay::default()
    }
}

fn request() -> ProposeRequest {
    let profile = AgentProfile::new("support").max_steps(20);
    ProposeRequest {
        dimension: Dimension::Inv,
        evidence: Evidence::text("run 1 skipped the check"),
        ceilings: Ceilings::from_profile(&profile),
        profile,
        current: ProfileOverlay::default(),
        attempts: ProposalAttempts::default(),
    }
}

fn runtime(db: &Path, proposer: Arc<LoggedProposer>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    rt.register_workflow(&two_stages_info());
    let harness = AgentHarness::new(Arc::new(NoModel)).proposer(proposer);
    sqlite::register(&mut rt, Arc::new(harness));
    rt
}

fn proposer(log_path: &Path, who: &'static str, abort_dom: bool) -> Arc<LoggedProposer> {
    Arc::new(LoggedProposer {
        log_path: log_path.to_path_buf(),
        who,
        abort_dom,
        calls: AtomicUsize::new(0),
    })
}

fn exec_file(db: &Path) -> PathBuf {
    db.with_extension("exec")
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let db = PathBuf::from(db);
    let mut rt = runtime(&db, proposer(Path::new(&log_path), "child", true));
    let exec = rt
        .start_workflow("two_stages", serde_json::to_value(request()).unwrap())
        .unwrap();
    std::fs::write(exec_file(&db), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

fn lines(log_path: &Path, who: &str) -> Vec<String> {
    std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with(who))
        .map(str::to_owned)
        .collect()
}

fn completed_stages(events: &[WorkflowEvent]) -> Vec<Stage> {
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

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_mid_retry_records_one_result_and_never_asks_a_completed_stage() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("refine.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_runs_until_it_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    assert_eq!(
        lines(&log_path, "child"),
        ["child INV 1", "child INV 2", "child DOM 1", "child DOM 2"],
        "the child dies inside attempt 2 of the DOM stage"
    );

    // The parent finishes the run.
    let parent = proposer(&log_path, "parent", false);
    let mut rt = runtime(&db, Arc::clone(&parent));
    let exec: ExecutionId = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(value) = state else {
        panic!("{state:?}");
    };
    // The INV stage completed before the crash, so it is never asked
    // again. The DOM stage in flight starts again at attempt 1.
    assert_eq!(lines(&log_path, "parent"), ["parent DOM 1", "parent DOM 2"]);
    assert_eq!(parent.calls.load(Ordering::SeqCst), 2);

    let stages: Vec<Stage> = serde_json::from_value(value).unwrap();
    assert_eq!(stages.len(), 2);
    for (stage, dimension) in stages.iter().zip([Dimension::Inv, Dimension::Dom]) {
        assert_eq!(stage.dimension, dimension);
        assert_eq!(stage.attempts, 2);
        assert_eq!(stage.outcome, StageOutcome::Proposed(proposal(dimension)));
    }

    // History holds exactly one result per stage.
    let events = rt.load_history(exec).unwrap();
    assert_eq!(completed_stages(&events), stages);

    // Replay reads the recorded stages and asks no proposer.
    let report = WorkflowReplayer::new()
        .register(vec![two_stages_info()])
        .replay_from_events(events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report}"
    );
    assert_eq!(parent.calls.load(Ordering::SeqCst), 2);
    assert_eq!(lines(&log_path, "parent").len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_completed_run_drives_again_with_no_proposer_call() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("refine.db");
    let log_path = dir.path().join("calls.log");
    let first = proposer(&log_path, "first", false);
    let mut rt = runtime(&db, Arc::clone(&first));
    let exec = rt
        .start_workflow("two_stages", serde_json::to_value(request()).unwrap())
        .unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_eq!(first.calls.load(Ordering::SeqCst), 4);
    drop(rt);

    let again = proposer(&log_path, "again", false);
    let mut rt = runtime(&db, Arc::clone(&again));
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_eq!(again.calls.load(Ordering::SeqCst), 0);
    assert_eq!(completed_stages(&rt.load_history(exec).unwrap()).len(), 2);
}
