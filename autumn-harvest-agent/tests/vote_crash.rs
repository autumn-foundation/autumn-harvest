//! A process crash in the middle of a vote.
//!
//! The test runs its own binary again as a child process. The child drives
//! a vote of 5 draws, one draw at a time. Draws 0 and 1 vote for `a`. The
//! child aborts inside the model call of draw 2. The parent opens the same
//! database and finishes the run. Draw 2 votes for `a` too. With the two
//! recorded votes, `a` then has 3 votes and 2 draws are left, so the run
//! stops. Both processes append to one log, so the test counts each model
//! call across the crash.
//!
//! A second pair crashes a vote with an app key. The parent has no key
//! function, so the resumed draw fails its key, and the run stops with
//! `score_failed`. No draw casts a silent empty vote.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    AnswerKey, AnswerKeyFn, InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport,
    SampleStop, SamplingPolicy, SamplingPolicyStore, Strategy, VoteCount,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, DrawId, Verdict,
    sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use common::{answer, runtime};
use futures::FutureExt as _;

const DB_ENV: &str = "VOTE_CRASH_DB";
const LOG_ENV: &str = "VOTE_CRASH_LOG";
const KEY_DB_ENV: &str = "VOTE_KEY_CRASH_DB";
const KEY_LOG_ENV: &str = "VOTE_KEY_CRASH_LOG";
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

/// A model that logs each call with its draw, then answers `"a"` for
/// draws 0 to 2 and `"b"` for each later draw. With `abort_on` set, it
/// aborts the process inside the call of that draw, after the log.
#[derive(Debug)]
struct LoggedModel {
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<u32>,
}

impl AgentModel for LoggedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let draw = request.draw.map_or(u32::MAX, DrawId::get);
        log(&self.log_path, &format!("{} model s{draw}", self.who));
        if self.abort_on == Some(draw) {
            std::process::abort();
        }
        let text = if draw <= 2 { "a" } else { "b" };
        Box::pin(std::future::ready(answer(text, 5)))
    }
}

/// A key function that lower-cases the trimmed answer.
#[derive(Debug)]
struct Lower;

impl AnswerKeyFn for Lower {
    fn id(&self) -> &'static str {
        "lower"
    }

    fn key(&self, answer: &str) -> Option<String> {
        Some(answer.trim().to_lowercase())
    }
}

fn harness(log_path: &Path, who: &'static str, abort_on: Option<u32>) -> AgentHarness {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::Vote]),
        max_draws: 5,
        ..SamplingPolicy::default()
    };
    store
        .set(KIND, &policy, "ops@example.com")
        .now_or_never()
        .unwrap()
        .unwrap();
    let model = Arc::new(LoggedModel {
        log_path: log_path.to_path_buf(),
        who,
        abort_on,
    });
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .sampling_policies(store)
}

/// The child half. It does nothing unless the parent set the environment.
/// The child has the key function `lower`, and dies inside draw 1.
#[tokio::test(flavor = "multi_thread")]
async fn child_with_an_app_key_runs_until_draw_one_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(KEY_DB_ENV), std::env::var_os(KEY_LOG_ENV))
    else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let child = harness(&log_path, "child", Some(1)).answer_key(Arc::new(Lower));
    let mut rt = runtime(Path::new(&db), child);
    let input =
        SampleInput::vote(KIND, "Pick a letter.", 5, AnswerKey::custom("lower")).concurrency(1);
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
    std::fs::write(exec_file(Path::new(&db)), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the run ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_without_the_app_key_fails_the_key_and_not_silently() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_with_an_app_key_runs_until_draw_one_crashes",
            "--nocapture",
        ])
        .env(KEY_DB_ENV, &db)
        .env(KEY_LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");

    // The parent has no key function `lower`.
    let mut rt = runtime(&db, harness(&log_path, "parent", None));
    let exec: ExecutionId = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    // Each failed attempt backs off on a durable timer before the retry.
    let mut state = rt.run_until_blocked(exec).await.unwrap();
    for minute in 1..=10 {
        if !matches!(state, RunState::WaitingTimer) {
            break;
        }
        let at = chrono::Utc::now() + chrono::Duration::minutes(minute);
        state = rt.run_until_blocked_as_of(exec, at).await.unwrap();
    }
    let report: SampleReport = match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed run, got {other:?}"),
    };
    assert_eq!(report.stopped, SampleStop::ScoreFailed);
    assert_eq!(
        report.draws.len(),
        2,
        "the run starts no draw after the failure"
    );
    assert_eq!(
        report.draws[0].key.as_deref(),
        Some("a"),
        "the recorded key"
    );
    assert_eq!(report.draws[1].key, None);
    let Verdict::Reject(reason) = &report.draws[1].verdict else {
        panic!("draw 1 must get a reject");
    };
    assert!(reason.contains("answer key failed"), "{reason}");
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_draw_two_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let mut rt = runtime(Path::new(&db), harness(&log_path, "child", Some(2)));
    let input = SampleInput::vote(KIND, "Pick a letter.", 5, AnswerKey::Trimmed).concurrency(1);
    let exec = sqlite::start_sample(&mut rt, &input).unwrap();
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
async fn a_crash_in_a_vote_resumes_with_the_recorded_tally() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_runs_until_draw_two_crashes",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    assert_eq!(
        lines(&log_path),
        ["child model s0", "child model s1", "child model s2"],
        "draws 0 and 1 ended, and the child died inside draw 2"
    );

    let mut rt = runtime(&db, harness(&log_path, "parent", None));
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
    assert_eq!(parent, ["parent model s2"], "only draw 2 runs again");
    for draw in ["s0", "s1"] {
        let calls = all.iter().filter(|line| line.ends_with(draw)).count();
        assert_eq!(
            calls, 1,
            "the model counter of {draw} keeps its first value"
        );
    }
    assert_eq!(report.draws.len(), 3, "draws 3 and 4 never start");
    assert_eq!(report.stopped, SampleStop::Settled);
    assert_eq!(report.tally, [VoteCount::new("a", 3, DrawId::new(0))]);
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(0)
        }
    );
}
