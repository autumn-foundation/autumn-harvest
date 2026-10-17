//! Shared fixtures: a lab, a database and a runtime with the scripted model.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::AgentHarness;
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionId, SqliteRuntime};
use autoresearch::agent::{self, HarnessParts};
use autoresearch::lab::Lab;
use autoresearch::runtime::{self, Pending, Step};
use autoresearch::scripted::ScriptedResearcher;
use autoresearch::workflows::{SessionConfig, SessionReport};
use serde_json::Value;

/// A temporary directory with one or more labs and one database.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub labs: Vec<Lab>,
    pub db: PathBuf,
    pub state: PathBuf,
}

impl Fixture {
    /// One lab.
    pub fn new() -> Self {
        Self::with_labs(1)
    }

    /// `count` labs that share one database.
    pub fn with_labs(count: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let labs = (0..count)
            .map(|i| Lab::init(dir.path().join(format!("lab{i}"))).unwrap())
            .collect();
        let state = dir.path().join("state");
        let db = dir.path().join("harvest.db");
        Self {
            dir,
            labs,
            db,
            state,
        }
    }

    pub fn lab(&self) -> &Lab {
        &self.labs[0]
    }

    pub fn parts(&self) -> HarnessParts {
        HarnessParts {
            labs: self.labs.clone(),
            state_dir: self.state.clone(),
        }
    }

    pub fn harness(&self, model: &Arc<ScriptedResearcher>) -> AgentHarness {
        agent::harness(model.clone(), &self.parts()).unwrap()
    }

    /// Open the database and register everything, as a process start does.
    pub fn open(&self, model: &Arc<ScriptedResearcher>) -> SqliteRuntime {
        let mut rt = SqliteRuntime::open(&self.db).unwrap();
        runtime::register(&mut rt, Arc::new(self.harness(model)));
        rt
    }

    /// A short, deterministic session on lab `i`.
    pub fn config_for(&self, i: usize, tag: &str, experiments: u32) -> SessionConfig {
        let mut config = SessionConfig::new(self.labs[i].root().display().to_string(), tag);
        config.max_experiments = experiments;
        config.explore_percent = 0;
        config.report = false;
        config.agent.approval_timeout_secs = 120;
        config
    }

    pub fn config(&self, tag: &str, experiments: u32) -> SessionConfig {
        self.config_for(0, tag, experiments)
    }
}

/// Drive `exec` to its end. Panics on an approval wait or a failure.
pub async fn run_to_end(rt: &mut SqliteRuntime, mut exec: ExecutionId) -> SessionReport {
    for _ in 0..10_000 {
        match runtime::drive(rt, exec).await.unwrap() {
            Step::Done(report) => return report,
            Step::Continued(next) => exec = next,
            Step::Waiting => tokio::time::sleep(Duration::from_millis(20)).await,
            Step::Approval(pending) => panic!("unexpected approval wait: {pending:?}"),
            Step::Failed(error) => panic!("the session failed: {error}"),
        }
    }
    panic!("the session did not end")
}

/// Drive `exec` to its next approval wait.
pub async fn run_to_approval(rt: &mut SqliteRuntime, exec: ExecutionId) -> Pending {
    for _ in 0..10_000 {
        match runtime::drive(rt, exec).await.unwrap() {
            Step::Approval(pending) => return pending,
            Step::Waiting => tokio::time::sleep(Duration::from_millis(20)).await,
            other => panic!("expected an approval wait, got {other:?}"),
        }
    }
    panic!("no approval wait")
}

/// Every execution of `workflow`, oldest first.
pub fn runs_of(rt: &SqliteRuntime, workflow: &str) -> Vec<ExecutionId> {
    let mut runs: Vec<ExecutionId> = rt
        .list_executions(&ExecutionFilter::new().workflow_name(workflow).limit(1000))
        .unwrap()
        .executions
        .iter()
        .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .collect();
    runs.reverse();
    runs
}

/// The input that started `exec`.
pub fn input_of(rt: &SqliteRuntime, exec: ExecutionId) -> Value {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowStarted { input, .. } => Some(input),
            _ => None,
        })
        .unwrap()
}

/// The names of the activities that `exec` scheduled, in order.
pub fn activities_of(rt: &SqliteRuntime, exec: ExecutionId) -> Vec<String> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, .. }
            | WorkflowEvent::LocalActivityScheduled { name, .. } => Some(name),
            _ => None,
        })
        .collect()
}

/// The payloads of every signal named `name` that `exec` received.
pub fn signals_of(rt: &SqliteRuntime, exec: ExecutionId, name: &str) -> Vec<Value> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } if signal_name == name => Some(payload),
            _ => None,
        })
        .collect()
}
