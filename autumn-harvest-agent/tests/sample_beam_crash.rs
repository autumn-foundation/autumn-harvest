//! A process crash in the second round of a beam run.
//!
//! The test runs its own binary again as a child process. The child drives
//! a beam of width 2 and expansion 2, one step at a time. It aborts inside
//! the model call of draw 3, the second step of the second round. The
//! parent opens the same database and finishes the run. Both processes
//! append to one log, so the test counts each model call and each score.
//! The prose counts rounds from 1. `BeamStep::round` counts from 0.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]

mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, SampleInput, SampleOutcome, SampleReport, SampleStrategy,
    SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ContentPart,
    DrawId, FnTool, ScoreContext, ScoreInput, Scorer, ToolEffect, Verdict, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState};
use common::{answer, calls, runtime};
use futures::FutureExt as _;
use serde_json::json;

const DB_ENV: &str = "BEAM_CRASH_DB";
const LOG_ENV: &str = "BEAM_CRASH_LOG";
const KIND: &str = "walker";

fn log(path: &Path, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
    file.sync_all().unwrap();
}

fn path_of<'a>(contents: impl Iterator<Item = &'a str>) -> String {
    contents.map(|content| content.trim_matches('"')).collect()
}

/// A model that logs each call with its draw, walks two steps, then
/// answers its path. An even draw walks `L`, and an odd draw walks `R`.
/// With `abort_on` set, it aborts the process inside that call.
#[derive(Debug)]
struct LoggedWalker {
    log_path: PathBuf,
    who: &'static str,
    abort_on: Option<u32>,
}

impl AgentModel for LoggedWalker {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let draw = request.draw.map_or(u32::MAX, DrawId::get);
        log(&self.log_path, &format!("{} model s{draw}", self.who));
        if self.abort_on == Some(draw) {
            std::process::abort();
        }
        let done: Vec<&str> = request
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(|part| match part {
                ContentPart::ToolResult { content, .. } => Some(content.as_str()),
                _ => None,
            })
            .collect();
        let reply = if done.len() >= 2 {
            answer(&path_of(done.into_iter()), 5)
        } else {
            let dir = if draw.is_multiple_of(2) { "L" } else { "R" };
            calls(&[(&format!("c{draw}"), "walk", json!({"dir": dir}))], 5)
        };
        Box::pin(std::future::ready(reply))
    }
}

/// A step scorer that logs each score and prefers the path `RL`.
#[derive(Debug)]
struct LoggedPaths {
    log_path: PathBuf,
    who: &'static str,
}

impl Scorer for LoggedPaths {
    fn id(&self) -> &str {
        "paths"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        let path = path_of(input.tools.iter().map(|tool| tool.content.as_str()));
        log(&self.log_path, &format!("{} score {path}", self.who));
        let score = match path.as_str() {
            "R" => 0.6,
            "L" => 0.4,
            "RL" => 0.9,
            "RR" => 0.5,
            "LL" => 0.3,
            _ => 0.2,
        };
        Box::pin(std::future::ready(Verdict::score(score)))
    }
}

fn harness(log_path: &Path, who: &'static str, abort_on: Option<u32>) -> AgentHarness {
    let store = Arc::new(InMemorySamplingPolicyStore::new());
    let policy = SamplingPolicy {
        strategies: std::collections::BTreeSet::from([Strategy::Beam]),
        max_draws: 12,
        // One step at a time, so the log order is fixed.
        max_concurrent_draws: 1,
        ..SamplingPolicy::default()
    };
    store
        .set(KIND, &policy, "ops@example.com")
        .now_or_never()
        .unwrap()
        .unwrap();
    let model = Arc::new(LoggedWalker {
        log_path: log_path.to_path_buf(),
        who,
        abort_on,
    });
    let walk = FnTool::new(
        "walk",
        "Take one step.",
        json!({"type": "object"}),
        |input: serde_json::Value| async move { Ok(input["dir"].clone()) },
    )
    .effect(ToolEffect::ReadOnly)
    .shared();
    AgentHarness::new(model)
        .agent(AgentProfile::new(KIND))
        .tool(walk)
        .scorer(Arc::new(LoggedPaths {
            log_path: log_path.to_path_buf(),
            who,
        }))
        .sampling_policies(store)
}

fn input() -> SampleInput {
    SampleInput::new(
        KIND,
        "Find the path.",
        SampleStrategy::beam(2, 2, 3),
        ScorerRef::deterministic("paths").unwrap(),
    )
}

/// The child half. It does nothing unless the parent set the environment.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_round_two_crashes() {
    let (Some(db), Some(log_path)) = (std::env::var_os(DB_ENV), std::env::var_os(LOG_ENV)) else {
        return;
    };
    let log_path = PathBuf::from(log_path);
    let mut rt = runtime(Path::new(&db), harness(&log_path, "child", Some(3)));
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
async fn a_crash_in_round_two_resumes_in_round_two() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let log_path = dir.path().join("calls.log");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_runs_until_round_two_crashes",
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
            "child model s0",
            "child score L",
            "child model s1",
            "child score R",
            "child model s2",
            "child score RL",
            "child model s3",
        ],
        "round 1 ended, and the child died inside the second step of round 2"
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
    let parent: Vec<&str> = all
        .iter()
        .filter(|line| line.starts_with("parent"))
        .map(String::as_str)
        .collect();
    let parent_models: Vec<&str> = parent
        .iter()
        .filter(|line| line.contains(" model "))
        .copied()
        .collect();
    assert_eq!(
        parent_models,
        (3..10)
            .map(|n| format!("parent model s{n}"))
            .collect::<Vec<_>>(),
        "the parent resumes in round 2, at the step in flight"
    );
    for draw in ["s0", "s1", "s2"] {
        let calls = all.iter().filter(|line| line.ends_with(draw)).count();
        assert_eq!(calls, 1, "the model counter of {draw} keeps its value");
    }
    for path in ["L", "R"] {
        assert!(
            !parent.contains(&format!("parent score {path}").as_str()),
            "history holds the round 1 verdict of {path}, so the parent never scores it"
        );
    }
    // Draw 2 has the path RL. Its verdict is in history, so the parent
    // scores RL only for the 2 complete beams of the third round.
    let rl = parent
        .iter()
        .filter(|line| **line == "parent score RL")
        .count();
    assert_eq!(rl, 2);
    assert_eq!(report.draws.len(), 10);
    let first_round: Vec<Option<f64>> = report.draws[..2]
        .iter()
        .map(|draw| {
            draw.verdict
                .as_score()
                .map(autumn_harvest_agent::FiniteF64::get)
        })
        .collect();
    assert_eq!(
        first_round,
        [Some(0.4), Some(0.6)],
        "the round 1 verdicts come from history"
    );
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(6)
        }
    );
    assert_eq!(report.selected.unwrap().text, "RL");
}
