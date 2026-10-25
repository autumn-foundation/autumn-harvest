//! The lifecycle of a durable agent (issue #2267).
//!
//! An idle agent is a row in the database and nothing more. Each test drops
//! the runtime that ran the agent and opens a fresh one on the same file.
//! The fresh runtime must serve the next message and the next wake.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentModel, AgentOp, AgentProfile,
    AgentState, BoxFuture, ChatRequest, ChatResponse, ChatRole, ContentPart, Control, StopReason,
    TokenUsage, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::runtime;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn state(rt: &SqliteRuntime, agent: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, agent);
    async move { query.await.unwrap().expect("the agent runs") }
}

/// Drive the agent at the drive clock `at`, through each checkpoint.
async fn drive_at(rt: &mut SqliteRuntime, agent: &AgentId, at: DateTime<Utc>) -> RunState {
    let mut exec = sqlite::current_run(rt, agent).unwrap().unwrap();
    loop {
        match rt.run_until_blocked_as_of(exec, at).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return other,
        }
    }
}

/// A model that answers every request with one fixed text, and counts.
#[derive(Debug)]
struct FixedModel {
    text: &'static str,
    calls: AtomicUsize,
}

impl FixedModel {
    fn new(text: &'static str) -> Arc<Self> {
        Arc::new(Self {
            text,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AgentModel for FixedModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = self.text;
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(text.to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(1, 1),
            })
        })
    }
}

fn support(model: Arc<FixedModel>) -> AgentHarness {
    AgentHarness::new(model).agent(AgentProfile::new("support"))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_agent_needs_no_runtime_and_wakes_on_a_message() {
    let (_dir, db) = fresh_db();
    let agent = AgentId::new("support", "ada").unwrap();
    let first = FixedModel::new("first answer");
    {
        let mut rt = runtime(&db, support(first.clone()));
        sqlite::send(&mut rt, &agent, &AgentOp::message("hello"), None).unwrap();
        sqlite::drive(&mut rt, &agent).await.unwrap();
        let idle = state(&rt, &agent).await;
        assert_eq!(idle.status.name(), "idle");
        assert_eq!(idle.segments, 1);
        // An idle agent has no runnable work. It holds no worker time.
        assert!(!rt.poll_once().await.unwrap(), "an idle agent runs nothing");
    }

    // The runtime and its process memory are gone. A fresh runtime with a
    // fresh model serves the next message.
    let second = FixedModel::new("second answer");
    let mut rt = runtime(&db, support(second.clone()));
    sqlite::send(&mut rt, &agent, &AgentOp::message("again"), None).unwrap();
    sqlite::drive(&mut rt, &agent).await.unwrap();
    let woke = state(&rt, &agent).await;
    assert_eq!(woke.segments, 2);
    assert_eq!(woke.last_answer.as_deref(), Some("second answer"));
    assert_eq!((first.calls(), second.calls()), (1, 1));
    let said: Vec<&str> = woke
        .transcript
        .iter()
        .filter(|m| m.role == ChatRole::User)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        said,
        ["hello", "again"],
        "the transcript survives the restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_agent_wakes_on_its_timer_in_a_fresh_runtime() {
    let (_dir, db) = fresh_db();
    let agent = AgentId::new("watch", "disk").unwrap();
    let harness = |model: Arc<FixedModel>| {
        AgentHarness::new(model).agent(
            AgentProfile::new("watch").heartbeat(AgentHeartbeat::every(Duration::from_secs(60))),
        )
    };
    let start = Utc::now();
    let first = FixedModel::new("HEARTBEAT_OK");
    let booked = {
        let mut rt = runtime(&db, harness(first.clone()));
        sqlite::send(&mut rt, &agent, &AgentOp::Control(Control::Resume), None).unwrap();
        drive_at(&mut rt, &agent, start).await;
        let idle = state(&rt, &agent).await;
        assert_eq!(idle.ticks, 0);
        idle.next_heartbeat_ms.expect("a tick is booked")
    };
    assert!(booked > start.timestamp_millis());

    let second = FixedModel::new("HEARTBEAT_OK");
    let mut rt = runtime(&db, harness(second.clone()));
    drive_at(&mut rt, &agent, start + chrono::Duration::seconds(90)).await;
    let woke = state(&rt, &agent).await;
    assert_eq!(woke.ticks, 1, "the booked tick ran in the fresh runtime");
    assert_eq!((first.calls(), second.calls()), (0, 1));
    assert!(
        woke.next_heartbeat_ms.is_some_and(|next| next > booked),
        "the next tick is booked after this one"
    );
}
