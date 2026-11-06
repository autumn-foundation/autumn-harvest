//! A process crash in the middle of a model cascade.
//!
//! The test runs its own binary again as a child process. The child drives
//! a two-tier cascade. Tier 1 asks the cheap model, and the score is below
//! the tier threshold. The child aborts inside the call of tier 2. The
//! parent opens the same database and finishes the run. Both processes
//! append to one log, so the test counts each model call across the crash.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport, SampleStrategy,
    SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy, Tier,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, DrawId,
    ExactMatch, FiniteF64, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use common::{answer, runtime};
use futures::FutureExt as _;

const DB_ENV: &str = "SAMPLE_CASCADE_CRASH_DB";
const LOG_ENV: &str = "SAMPLE_CASCADE_CRASH_LOG";
const KIND: &str = "researcher";

fn log(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

/// A model that logs each call, then answers `text`. With `abort` set, it
/// aborts the process inside the call, after the log.
#[derive(Debug)]
struct LoggedModel {
    log_path: PathBuf,
    line: String,
    text: &'static str,
    abort: bool,
}

impl AgentModel for LoggedModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        log(&self.log_path, &self.line);
        if self.abort {
            std::process::abort();
        }
        Box::pin(std::future::ready(answer(self.text, 5)))
    }
}

fn harness(log_path: &Path, who: &str, abort_in_strong: bool) -> AgentHarness {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Cascade]),
        max_draws: 2,
        cascade_models: BTreeSet::from(["cheap".to_owned(), "strong".to_owned()]),
        ..SamplingPolicy::default()
    };
    store
        .set(KIND, &policy, "ops@example.com")
        .now_or_never()
        .unwrap()
        .unwrap();
    let model = |name: &str, text, abort| {
        Arc::new(LoggedModel {
            log_path: log_path.to_path_buf(),
            line: format!("{who} {name}"),
            text,
            abort,
        })
    };
    AgentHarness::new(model("default", "x", false))
        .model_named("cheap", model("cheap", "a", false))
        .model_named("strong", model("strong", "b", abort_in_strong))
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(ExactMatch::new("b")))
        .sampling_policies(store)
}

fn input() -> SampleInput {
    let tier = |model: &str| Tier::new(model, FiniteF64::ONE);
    SampleInput::new(
        KIND,
        "Pick a letter.",
        SampleStrategy::cascade([tier("cheap"), tier("strong")]),
        ScorerRef::deterministic(ExactMatch::ID).unwrap(),
    )
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_tier_two_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let mut rt = runtime(Path::new(&db), harness(&log_path, "child", true));
    let exec = sqlite::start_sample(&mut rt, &input()).unwrap();
    std::fs::write(exec_file(Path::new(&db)), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

fn exec_file(db: &Path) -> PathBuf {
    db.with_extension("exec")
}

fn lines(log_path: &Path) -> Vec<String> {
    std::fs::read_to_string(log_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_tier_one_resumes_at_tier_two() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_runs_until_tier_two_crashes",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    assert_eq!(
        lines(&log_path),
        ["child cheap", "child strong"],
        "tier 1 ended and was scored, and the child died inside tier 2"
    );

    let mut rt = runtime(&db, harness(&log_path, "parent", false));
    let exec: ExecutionId = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let report: SampleReport = match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed run, got {other:?}"),
    };

    let all = lines(&log_path);
    let parent: Vec<&String> = all
        .iter()
        .filter(|line| line.starts_with("parent"))
        .collect();
    assert_eq!(parent, ["parent strong"], "only tier 2 runs again");
    let cheap = all.iter().filter(|line| line.ends_with("cheap")).count();
    assert_eq!(cheap, 1, "tier 1 is never called again");
    assert_eq!(report.draws.len(), 2);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
}
