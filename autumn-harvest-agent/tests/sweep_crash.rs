//! A process crash in the middle of an `agent_sweep` run.
//!
//! The test runs its own binary again as a child process. The child drives
//! a sweep of 5 items, one item at a time. It aborts inside the model call
//! of item 4, after items 1 to 3 completed. The parent opens the same
//! database and finishes the sweep. Both processes append to one log, so
//! the test counts each model call across the crash.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    SampleStrategy, ScorerRef, SweepInput, SweepItem, SweepItemStatus, SweepItems, SweepReport,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ContentPart,
    ExactMatch, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use common::{answer, runtime};

const DB_ENV: &str = "SWEEP_CRASH_DB";
const LOG_ENV: &str = "SWEEP_CRASH_LOG";
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

/// The item input of `request`, such as `item 4`.
fn item_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .find_map(|part| match part {
            ContentPart::Text(text) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// A model that logs each call with its item, then answers `ok`. With
/// `abort_on` set, it aborts the process inside the call of that item,
/// after the log.
#[derive(Debug)]
struct LoggedModel {
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<&'static str>,
}

impl AgentModel for LoggedModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let item = item_of(request);
        log(&self.log_path, &format!("{} model {item}", self.who));
        if self.abort_on == Some(item.as_str()) {
            std::process::abort();
        }
        Box::pin(std::future::ready(answer("ok", 5)))
    }
}

fn harness(log_path: &Path, who: &'static str, abort_on: Option<&'static str>) -> AgentHarness {
    let model = Arc::new(LoggedModel {
        log_path: log_path.to_path_buf(),
        who,
        abort_on,
    });
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .scorer(Arc::new(ExactMatch::per_item()))
}

fn input() -> SweepInput {
    let items = (1..=5)
        .map(|n| SweepItem::new(format!("item {n}")).expected("ok"))
        .collect();
    SweepInput::new(
        KIND,
        SweepItems::inline(items),
        SampleStrategy::single(),
        ScorerRef::deterministic(ExactMatch::ID).unwrap(),
    )
    .concurrency(1)
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_item_four_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let mut rt = runtime(Path::new(&db), harness(&log_path, "child", Some("item 4")));
    let exec = sqlite::start_sweep(&mut rt, &input()).unwrap();
    std::fs::write(exec_file(Path::new(&db)), exec.to_string()).unwrap();
    let _ = rt.run_until_blocked(exec).await;
    panic!("the child must abort before the sweep ends");
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
async fn a_crash_after_item_three_resumes_at_item_four() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_runs_until_item_four_crashes",
            "--nocapture",
        ])
        .env(DB_ENV, &db)
        .env(LOG_ENV, &log_path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");
    assert_eq!(
        lines(&log_path),
        [
            "child model item 1",
            "child model item 2",
            "child model item 3",
            "child model item 4",
        ],
        "items 1 to 3 completed, and the child died inside item 4"
    );

    let mut rt = runtime(&db, harness(&log_path, "parent", None));
    let exec: ExecutionId = std::fs::read_to_string(exec_file(&db))
        .unwrap()
        .parse()
        .unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    let report: SweepReport = match state {
        RunState::Completed(value) => serde_json::from_value(value).unwrap(),
        other => panic!("expected a completed sweep, got {other:?}"),
    };

    let all = lines(&log_path);
    let parent: Vec<&String> = all
        .iter()
        .filter(|line| line.starts_with("parent"))
        .collect();
    assert_eq!(
        parent,
        ["parent model item 4", "parent model item 5"],
        "the sweep resumes at item 4"
    );
    for item in ["item 1", "item 2", "item 3"] {
        let calls = all.iter().filter(|line| line.ends_with(item)).count();
        assert_eq!(
            calls, 1,
            "the model counter of {item} keeps its first value"
        );
    }
    assert_eq!(report.rows.len(), 5);
    assert!(
        report
            .rows
            .iter()
            .all(|row| row.status == SweepItemStatus::Completed && row.accepted)
    );
}
