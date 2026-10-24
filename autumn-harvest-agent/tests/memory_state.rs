//! Agent memory as entity state (issue #2273).
//!
//! With `AgentMemory::State`, the memory blocks live in the agent state. The
//! workflow runs the `memory` tool itself, so an edit commits with the op
//! snapshot. A crash replays the op, and the edit happens once. No store and
//! no edit key are involved.
//!
//! The crash test runs this binary again as a child process. The child
//! aborts inside the model call after the memory edit.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::chrono::Utc;
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest_agent::memory::MEMORY_TOOL;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentMemory, AgentModel, AgentOp,
    AgentProfile, AgentState, ChatRequest, ChatResponse, ChatRole, ContentPart, Control, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;
use common::{Reply, ScriptedModel, answer, calls, runtime};
use serde_json::json;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

fn agent() -> AgentId {
    AgentId::new("desk", "d1").unwrap()
}

fn remember(text: &str) -> Reply {
    calls(
        &[(
            "m1",
            MEMORY_TOOL,
            json!({"action": "add", "block": "memory", "text": text}),
        )],
        1,
    )
}

/// A model that aborts the process inside call `abort_on`.
#[derive(Debug)]
struct AbortingModel {
    inner: Arc<ScriptedModel>,
    abort_on: Option<usize>,
}

impl AgentModel for AbortingModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> autumn_harvest_agent::BoxFuture<'a, Result<ChatResponse, AgentError>> {
        if self.abort_on == Some(self.inner.calls() + 1) {
            std::process::abort();
        }
        self.inner.chat(request)
    }
}

fn harness(model: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(model).agent(AgentProfile::new("desk").memory(AgentMemory::State))
}

fn state_of(rt: &SqliteRuntime, id: &AgentId) -> impl Future<Output = AgentState> + Send + 'static {
    let query = sqlite::state(rt, id);
    async move { query.await.unwrap().unwrap() }
}

fn entries(state: &AgentState) -> Vec<String> {
    state
        .memory
        .iter()
        .find(|block| block.label == "memory")
        .map(|block| block.entries.clone())
        .unwrap_or_default()
}

/// The text of the system message of each model request.
fn systems(model: &ScriptedModel) -> Vec<String> {
    model
        .requests()
        .iter()
        .map(|request| {
            request
                .messages
                .iter()
                .find(|message| message.role == ChatRole::System)
                .and_then(|message| match message.content.first() {
                    Some(ContentPart::Text(text)) => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_memory_edit_commits_with_the_op_and_shows_in_the_next_op() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![
        remember("likes tea"),
        answer("noted", 1),
        answer("tea", 1),
    ]);
    let mut rt = runtime(&db, harness(model.clone()));
    sqlite::send(&mut rt, &agent(), &AgentOp::message("I like tea"), None).unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    assert_eq!(entries(&state_of(&rt, &agent()).await), ["likes tea"]);

    sqlite::send(
        &mut rt,
        &agent(),
        &AgentOp::message("what do I like?"),
        None,
    )
    .unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    let systems = systems(&model);
    assert!(systems[2].contains("likes tea"), "{}", systems[2]);
    // No memory activity runs: the workflow owns the edit.
    let exec = sqlite::current_run(&rt, &agent()).unwrap().unwrap();
    let memory_activities = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(event, WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_tool_call" || name == "agent_memory_snapshot")
        })
        .count();
    assert_eq!(memory_activities, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_reads_are_stable_within_a_segment() {
    // The system prompt of an op is fixed when the op starts, so a provider
    // prompt cache keeps its prefix.
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![remember("likes tea"), answer("noted", 1)]);
    let mut rt = runtime(&db, harness(model.clone()));
    sqlite::send(&mut rt, &agent(), &AgentOp::message("I like tea"), None).unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    let systems = systems(&model);
    assert_eq!(systems.len(), 2);
    assert_eq!(systems[0], systems[1], "the edit shows from the next op");
    assert!(!systems[1].contains("likes tea"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_memory_is_in_the_entity_snapshot() {
    // Erasure and codec rotation cover the snapshot events and the
    // `harvest_entity_state` projection (issue #2259). Memory in the state
    // is therefore covered with no code of its own.
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![remember("likes tea"), answer("noted", 1)]);
    let mut rt = runtime(&db, harness(model));
    sqlite::send(&mut rt, &agent(), &AgentOp::message("I like tea"), None).unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    let exec = sqlite::current_run(&rt, &agent()).unwrap().unwrap();
    let snapshots: Vec<String> = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                value,
                ..
            } => Some(value.to_string()),
            _ => None,
        })
        .collect();
    assert!(snapshots.last().unwrap().contains("likes tea"));
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_memory_clears_the_state_memory() {
    let (_dir, db) = fresh_db();
    let model = ScriptedModel::new(vec![remember("likes tea"), answer("noted", 1)]);
    let mut rt = runtime(&db, harness(model));
    sqlite::send(&mut rt, &agent(), &AgentOp::message("I like tea"), None).unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    let reset = AgentOp::Control(Control::ResetMemory);
    sqlite::send(&mut rt, &agent(), &reset, None).unwrap();
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    assert_eq!(
        entries(&state_of(&rt, &agent()).await),
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tick_without_memory_writes_cannot_edit_the_state_memory() {
    for allow in [false, true] {
        let (_dir, db) = fresh_db();
        let model = ScriptedModel::new(vec![
            remember("a note from a tick"),
            answer("HEARTBEAT_OK", 1),
        ]);
        // The tick may act, so only the memory opt-in gates the edit.
        let mut beat = AgentHeartbeat::every(Duration::from_secs(60)).allow_actions();
        beat.allow_memory_writes = allow;
        let profile = AgentProfile::new("desk")
            .memory(AgentMemory::State)
            .heartbeat(beat);
        let mut rt = runtime(&db, AgentHarness::new(model.clone()).agent(profile));
        let start = Utc::now();
        sqlite::send(&mut rt, &agent(), &AgentOp::Control(Control::Resume), None).unwrap();
        let exec = sqlite::current_run(&rt, &agent()).unwrap().unwrap();
        rt.run_until_blocked_as_of(exec, start).await.unwrap();
        let later = start + chrono::Duration::seconds(90);
        rt.run_until_blocked_as_of(exec, later).await.unwrap();

        let state = state_of(&rt, &agent()).await;
        assert_eq!(state.ticks, 1);
        let want: Vec<String> = if allow {
            vec!["a note from a tick".to_owned()]
        } else {
            Vec::new()
        };
        assert_eq!(entries(&state), want, "allow_memory_writes = {allow}");
        if !allow {
            let result = serde_json::to_string(&model.requests()[1].messages).unwrap();
            assert!(result.contains("read-only"), "{result}");
        }
    }
}

// ---------------------------------------------------------------------------
// A crash between the memory call and the commit of the op.
// ---------------------------------------------------------------------------

const DB_ENV: &str = "AGENT_MEMORY_CRASH_DB";

fn script() -> Vec<Reply> {
    vec![remember("likes tea"), answer("noted", 1)]
}

/// The child half of the crash test. It aborts inside the second model call,
/// after the memory edit and before the op commits.
#[tokio::test(flavor = "multi_thread")]
async fn child_runs_until_it_crashes() {
    let Some(db) = std::env::var_os(DB_ENV) else {
        return;
    };
    let model = Arc::new(AbortingModel {
        inner: ScriptedModel::new(script()),
        abort_on: Some(2),
    });
    let mut rt = runtime(Path::new(&db), harness(model));
    sqlite::send(&mut rt, &agent(), &AgentOp::message("I like tea"), None).unwrap();
    let _ = sqlite::drive(&mut rt, &agent()).await;
    panic!("the child must abort before the op ends");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_the_memory_call_keeps_memory_and_history_consistent() {
    let (_dir, db) = fresh_db();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_runs_until_it_crashes", "--nocapture"])
        .env(DB_ENV, &db)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must crash, not finish");

    // A new process replays the op. The memory call runs again from the
    // recorded turn, and the edit lands once, with the op.
    let mut rest = script();
    rest.drain(..1);
    let model = ScriptedModel::new(rest);
    let mut rt = runtime(&db, harness(model.clone()));
    sqlite::drive(&mut rt, &agent()).await.unwrap();
    let state = state_of(&rt, &agent()).await;
    assert_eq!(entries(&state), ["likes tea"], "one edit, no edit key");
    assert_eq!(state.last_answer.as_deref(), Some("noted"));
    assert_eq!(model.calls(), 1, "the recorded turn is not sent again");
}
