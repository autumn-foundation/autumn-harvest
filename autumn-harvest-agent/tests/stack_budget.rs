//! The stack need of an agent workflow in a debug build (issue #2752).
//!
//! `SqliteRuntime` polls a workflow on the caller thread. A debug build makes
//! a large frame for each poll. The Windows main thread has 1 MiB of stack, so
//! an agent op overflowed it.
//!
//! Each test runs one walk on a thread with a fixed stack of
//! [`STACK_BUDGET`] bytes. An overflow aborts the process, so a child process
//! runs the walk. The parent test checks that the child completed.
//!
//! Measure the need of a walk with the child alone, for example:
//!
//! ```sh
//! AGENT_STACK_PROBE=agent AGENT_STACK_PROBE_BYTES=262144 \
//!   cargo test -p autumn-harvest-agent --features sqlite,eval \
//!   --test stack_budget -- probe_child --exact --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::process::Command;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    Charge, ComposeContext, OverlayComposer, REFINE_STATUS_ATTR, RefineDecision, RefineInput,
    RefineMode, RefineReport,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentReport, AgentTask,
    ChatRequest, ChatResponse, ChatRole, ContentPart, Dimension, InMemoryMemoryStore,
    InMemoryOverlayStore, OverlayProposer, ProfileOverlay, ProfileOverlayStore, ProposeContext,
    ProposerReply, RefineStatus, StopReason, TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};

/// The stack of the thread that runs one walk.
///
/// The Windows main thread has 1 MiB. A Linux debug build of the deepest walk
/// needs about 370 KiB, test frames included. Windows makes larger frames, so
/// its budget is larger. Each budget leaves at least a quarter of that main
/// thread free.
const STACK_BUDGET: usize = if cfg!(windows) {
    768 * 1024
} else {
    512 * 1024
};

/// The walk that the child runs. A parent test sets it.
const PROBE_ENV: &str = "AGENT_STACK_PROBE";

/// An optional stack size for the child, in bytes. Use it to measure a walk.
const BYTES_ENV: &str = "AGENT_STACK_PROBE_BYTES";

/// The line that the child prints when its walk completes.
const DONE: &str = "STACK_PROBE:DONE";

const KIND: &str = "support";
const BASE: &str = "You are a support agent. Answer in one sentence.";
const RULE: &str = "A refund over 100 USD needs a manager.";
const MAX_STEPS: u32 = 4;
const TASK: &str = "Refund 250 USD for order 17.";
const PASS: &str = "A manager must approve this refund.";
const MISS: &str = "The refund is done.";

fn system_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::System)
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// A scripted model. It follows the rule only when its prompt holds the rule.
#[derive(Debug)]
struct SupportModel;

impl AgentModel for SupportModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let answer = if system_of(request).contains(RULE) {
            PASS
        } else {
            MISS
        };
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(answer.to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(40, 8),
            })
        })
    }
}

/// A scripted proposer. The DOM stage adds the rule. The INV stage keeps its
/// base.
#[derive(Debug)]
struct RuleProposer;

impl OverlayProposer for RuleProposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(200, 30)));
        let reply = match context.dimension {
            Dimension::Inv => context.current.clone(),
            Dimension::Dom => {
                let from = context
                    .current
                    .system
                    .as_deref()
                    .or(context.profile.system.as_deref())
                    .unwrap_or_default();
                ProfileOverlay {
                    system: Some(format!("{from}\n{RULE}")),
                    ..context.current.clone()
                }
            }
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// A scripted composer. It keeps the last probed candidate.
#[derive(Debug)]
struct LastComposer;

impl OverlayComposer for LastComposer {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(150, 20)));
        let reply = context
            .candidates
            .last()
            .map_or_else(|| context.current.clone(), |c| c.overlay.clone());
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// A runtime on a new database file, with the scripted pieces. The runtime
/// drops before the directory.
struct Walk {
    rt: SqliteRuntime,
    _dir: tempfile::TempDir,
}

impl Walk {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("stack.db");
        let base = AgentHarness::new(Arc::new(SupportModel) as Arc<dyn AgentModel>)
            .agent(AgentProfile::new(KIND).system(BASE).max_steps(MAX_STEPS));
        let store = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
        let harness = base
            .overlay_store(store as Arc<dyn ProfileOverlayStore>)
            .memory(Arc::new(InMemoryMemoryStore::new()))
            .proposer(Arc::new(RuleProposer))
            .composer(Arc::new(LastComposer))
            .refine_sources(Arc::new(sqlite::Sources::open(&db)));
        let mut rt = SqliteRuntime::open(&db).unwrap();
        sqlite::register(&mut rt, Arc::new(harness));
        Self { rt, _dir: dir }
    }

    /// One recorded `agent_loop` run of the kind.
    async fn agent_loop(&mut self) -> ExecutionId {
        let mut task = AgentTask::new(TASK).kind(KIND).max_steps(MAX_STEPS);
        task.system = Some(BASE.to_owned());
        let exec = sqlite::start(&mut self.rt, &task).unwrap();
        match self.rt.run_until_blocked(exec).await.unwrap() {
            RunState::Completed(value) => {
                let report: AgentReport = serde_json::from_value(value).unwrap();
                assert_eq!(report.text, MISS);
            }
            other => panic!("the agent_loop run did not complete: {other:?}"),
        }
        exec
    }

    /// One op of a durable agent of the kind.
    async fn agent_op(&mut self) {
        let id = AgentId::new(KIND, "customer").unwrap();
        sqlite::send(&mut self.rt, &id, &AgentOp::message(TASK), Some("op")).unwrap();
        sqlite::drive(&mut self.rt, &id).await.unwrap();
        let state = sqlite::state(&self.rt, &id).await.unwrap().unwrap();
        assert!(
            state.last_answer.is_some(),
            "the op gave no answer: {state:?}"
        );
    }

    /// One refine run from `source`, approved at the gate.
    async fn refine(&mut self, source: ExecutionId) {
        let input = RefineInput::new(KIND, vec![source], 1, RefineMode::Sequential, 7);
        let exec = sqlite::start_refine(&mut self.rt, &input).unwrap();
        let state = self.rt.run_until_blocked(exec).await.unwrap();
        assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");
        let description = self.rt.describe(exec).unwrap();
        let status = description
            .summary
            .search_attributes
            .value()
            .and_then(|attrs| attrs.get(REFINE_STATUS_ATTR)?.as_str().map(str::to_owned));
        let waiting = RefineStatus::AwaitingApproval.as_str();
        assert_eq!(status.as_deref(), Some(waiting));
        sqlite::decide_refine(&mut self.rt, exec, &RefineDecision::Approve).unwrap();
        match self.rt.run_until_blocked(exec).await.unwrap() {
            RunState::Completed(value) => {
                let report: RefineReport = serde_json::from_value(value).unwrap();
                assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
            }
            other => panic!("the refine run did not end: {other:?}"),
        }
    }
}

/// Run the walk `name`.
async fn walk(name: &str) {
    let mut walk = Walk::new();
    match name {
        "agent_loop" => {
            walk.agent_loop().await;
        }
        "agent" => walk.agent_op().await,
        "agent_refine" => {
            let source = walk.agent_loop().await;
            walk.refine(source).await;
            walk.agent_op().await;
        }
        other => panic!("no walk named {other:?}"),
    }
}

/// Run the walk `name` in a child process. Panic when it does not complete.
fn assert_fits(name: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["probe_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_ENV, name)
        .env_remove(BYTES_ENV)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(DONE),
        "the walk {name:?} did not complete on a stack of {STACK_BUDGET} bytes \
         ({}):\n{stdout}\n{stderr}",
        output.status,
    );
}

/// The child. It does nothing unless a parent test names a walk.
#[test]
fn probe_child() {
    let Ok(name) = std::env::var(PROBE_ENV) else {
        return;
    };
    let bytes = std::env::var(BYTES_ENV)
        .ok()
        .map_or(STACK_BUDGET, |bytes| bytes.parse().unwrap());
    let walker = std::thread::Builder::new()
        .name("stack-probe".to_owned())
        .stack_size(bytes)
        .spawn(move || {
            // The SQLite runtime needs a multi-thread runtime. `block_on`
            // still polls the walk on this thread.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(walk(&name));
        })
        .unwrap();
    walker.join().unwrap();
    println!("{DONE}");
}

#[test]
fn an_agent_loop_run_fits_the_stack_budget() {
    assert_fits("agent_loop");
}

#[test]
fn a_durable_agent_op_fits_the_stack_budget() {
    assert_fits("agent");
}

#[test]
fn the_refine_walk_fits_the_stack_budget() {
    assert_fits("agent_refine");
}
