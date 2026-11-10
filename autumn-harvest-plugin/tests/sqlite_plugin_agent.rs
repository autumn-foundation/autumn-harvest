//! The durable agent under `HarvestPlugin` on SQLite.
//!
//! The app registers the agent with `HarvestPlugin::agent`. It runs no
//! `SqliteRuntime` of its own. Each call goes through the `HarvestSqlite`
//! handle of the plugin. The overlay read suite of the agent crate runs here
//! as a third backend.

#![cfg(feature = "sqlite")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "../../autumn-harvest-agent/tests/overlay_read_suite/mod.rs"]
mod overlay_read_suite;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayReport, WorkflowReplayer};
use autumn_harvest::{ExecutionId, WorkflowIdReusePolicy};
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentHarness, AgentId, AgentOp, AgentProfile, AgentState,
    AgentStop, Approval, ChatRole, ContentPart, ProfileOverlayStore, SqliteOverlayStore, Urgency,
    agent_info,
};
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{
    ExecutionFilter, ExecutionSummary, HarvestSqlite, SignalWithStartParams, agent,
};
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestClient};
use overlay_read_suite::{Backend, PromptModel};
use serde_json::{Value, json};

/// How long a poll waits before the test fails.
const LIMIT: Duration = Duration::from_secs(20);

fn config(dir: &Path) -> AutumnConfig {
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.join("app.db").display()));
    config
}

/// Boot an app on `dir` that hosts the agent with `harness`.
fn boot(dir: &Path, harness: AgentHarness) -> TestClient {
    TestApp::new()
        .config(config(dir))
        .profile("dev")
        .plugin(HarvestPlugin::new().agent(harness).api("/harvest"))
        .build()
}

fn handle_of(client: &TestClient) -> HarvestSqlite {
    client
        .state()
        .extension::<HarvestSqlite>()
        .expect("the startup hook installs the handle")
        .as_ref()
        .clone()
}

/// An app on SQLite, with the overlay store in a file of its own.
struct Plugin {
    dir: tempfile::TempDir,
    overlays: PathBuf,
    app: Option<(TestClient, HarvestSqlite)>,
}

impl Plugin {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let overlays = dir.path().join("overlays.db");
        Self {
            dir,
            overlays,
            app: None,
        }
    }

    const fn handle(&self) -> &HarvestSqlite {
        &self.app.as_ref().expect("serve first").1
    }

    /// Stop the driver, as the shutdown hook does. The writer lock is then
    /// free for the next app.
    async fn stop(&mut self) {
        if let Some((_client, handle)) = self.app.take() {
            handle.shutdown().await.expect("the driver stops");
        }
    }

    /// Every run of the agent `id`, oldest first.
    async fn runs(&self, id: &AgentId) -> Vec<ExecutionId> {
        let filter = ExecutionFilter::new()
            .workflow_name(AGENT_WORKFLOW_NAME)
            .workflow_id(id.as_str());
        let page = self.handle().list(filter).await.expect("list");
        let mut runs: Vec<_> = page
            .executions
            .iter()
            .filter_map(ExecutionSummary::execution_id)
            .collect();
        runs.reverse();
        runs
    }

    async fn state(&self, id: &AgentId) -> Option<AgentState> {
        agent::state(self.handle(), id)
            .await
            .expect("the state reads")
    }
}

impl Backend for Plugin {
    fn kind(&self) -> String {
        "support".to_owned()
    }

    fn store(&self, validator: Arc<AgentHarness>) -> Arc<dyn ProfileOverlayStore> {
        Arc::new(SqliteOverlayStore::open(&self.overlays, validator).expect("the store opens"))
    }

    async fn serve(&mut self, harness: AgentHarness) {
        self.stop().await;
        let client = boot(self.dir.path(), harness);
        let handle = handle_of(&client);
        self.app = Some((client, handle));
    }

    async fn send(&mut self, id: &AgentId, op: &AgentOp) {
        agent::send(self.handle(), id, op, None)
            .await
            .expect("the op is sent");
    }

    async fn settle(
        &mut self,
        id: &AgentId,
        done: &(dyn Fn(&AgentState) -> bool + Sync),
    ) -> AgentState {
        let deadline = Instant::now() + LIMIT;
        loop {
            if let Some(state) = self.state(id).await
                && done(&state)
            {
                return state;
            }
            assert!(Instant::now() < deadline, "the agent {id} did not settle");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_approval(&mut self, id: &AgentId, _signal: &str) {
        let filter = ExecutionFilter::new()
            .workflow_name(AGENT_WORKFLOW_NAME)
            .workflow_id(id.as_str())
            .state("RUNNING")
            .search_attribute("agent_status", json!("waiting_approval"));
        let deadline = Instant::now() + LIMIT;
        while self
            .handle()
            .list(filter.clone())
            .await
            .expect("list")
            .executions
            .is_empty()
        {
            assert!(Instant::now() < deadline, "the agent {id} never waited");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn approve(&mut self, id: &AgentId, signal: &str) {
        let delivered = agent::decide(self.handle(), id, signal, &Approval::Approve, None)
            .await
            .expect("the decision is sent");
        assert!(delivered, "the decision is new");
    }

    async fn delete(&mut self, id: &AgentId) {
        agent::delete(self.handle(), id, None)
            .await
            .expect("the delete is sent");
        let deadline = Instant::now() + LIMIT;
        while agent::current_run(self.handle(), id)
            .await
            .expect("the run reads")
            .is_some()
        {
            assert!(Instant::now() < deadline, "the run of {id} did not end");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn history(&mut self, id: &AgentId) -> Vec<WorkflowEvent> {
        let mut events = Vec::new();
        for exec in self.runs(id).await {
            events.extend(self.handle().history(exec).await.expect("history"));
        }
        events
    }

    async fn replay(&mut self, id: &AgentId, harness: AgentHarness) -> Vec<ReplayReport> {
        let mut reports = Vec::new();
        for exec in self.runs(id).await {
            let events = self.handle().history(exec).await.expect("history");
            let report = WorkflowReplayer::new()
                .register(vec![agent_info()])
                .with_state(harness.clone())
                .with_workflow_id(id.as_str())
                .replay_from_events(events)
                .await;
            reports.push(report);
        }
        assert!(!reports.is_empty(), "the agent has a run");
        reports
    }

    async fn rebuilt(&mut self, id: &AgentId, harness: AgentHarness) -> AgentState {
        // A new app: the query replays the run from the file.
        self.serve(harness).await;
        self.state(id).await.expect("a run")
    }

    async fn listed(&mut self, version: &str) -> Vec<String> {
        let filter = ExecutionFilter::new()
            .workflow_name(AGENT_WORKFLOW_NAME)
            .search_attribute("agent_profile_version", json!(version));
        self.handle()
            .list(filter)
            .await
            .expect("list")
            .executions
            .iter()
            .filter_map(|summary| summary.workflow_id.value().cloned())
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_is_exact() {
    overlay_read_suite::replay_is_exact(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_next_op_sees_v2() {
    overlay_read_suite::the_next_op_sees_v2(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mid_op_activation_is_ignored() {
    overlay_read_suite::a_mid_op_activation_is_ignored(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_search_attribute_follows_the_op() {
    overlay_read_suite::the_search_attribute_follows_the_op(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_stored_data_fails_safe() {
    overlay_read_suite::bad_stored_data_fails_safe(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_narrowed_deploy_fails_safe() {
    overlay_read_suite::a_narrowed_deploy_fails_safe(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_restores_v1() {
    overlay_read_suite::a_rollback_restores_v1(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_ceiling_reaches_the_op_read() {
    overlay_read_suite::a_policy_ceiling_reaches_the_op_read(&mut Plugin::new()).await;
}

/// The text of each message, with its role.
fn texts(state: &AgentState) -> Vec<(ChatRole, String)> {
    state
        .transcript
        .iter()
        .map(|message| {
            let text = message
                .content
                .iter()
                .filter_map(|part| match part {
                    ContentPart::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            (message.role, text)
        })
        .collect()
}

/// The path of the issue: a raw signal-with-start, one op with a model
/// turn, and the state through a query. The management API sees the run.
#[tokio::test(flavor = "multi_thread")]
async fn an_entity_agent_runs_through_signal_with_start() {
    let dir = tempfile::tempdir().unwrap();
    let model = PromptModel::new();
    let harness = AgentHarness::new(model.clone()).agent(AgentProfile::new("support"));
    let client = boot(dir.path(), harness);
    let handle = handle_of(&client);
    let id = AgentId::new("support", "acct-1").unwrap();

    let outcome = handle
        .signal_with_start(SignalWithStartParams {
            workflow_name: AGENT_WORKFLOW_NAME.to_owned(),
            workflow_id: id.as_str().to_owned(),
            input: json!({}),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            signal_name: ENTITY_OP_SIGNAL.to_owned(),
            signal_payload: Agent::payload(&AgentOp::message("hello")),
            idempotency_key: Some("m-1".to_owned()),
        })
        .await
        .unwrap();
    assert!(outcome.started_fresh, "the signal starts the agent");
    let exec = outcome.exec_id;

    let deadline = Instant::now() + LIMIT;
    let state: AgentState = loop {
        let value: Value = handle
            .query(exec, ENTITY_STATE_QUERY, Value::Null)
            .await
            .unwrap();
        let state: AgentState = serde_json::from_value(value).unwrap();
        if state.segments >= 1 {
            break state;
        }
        assert!(Instant::now() < deadline, "the op did not run");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(model.calls(), 1, "one model turn");
    assert_eq!(state.last_answer.as_deref(), Some("ok"));
    assert_eq!(state.last_stop, Some(AgentStop::Completed));
    assert_eq!(
        texts(&state),
        vec![
            (ChatRole::User, "hello".to_owned()),
            (ChatRole::Assistant, "ok".to_owned()),
        ]
    );

    // The turn is a recorded activity of the run.
    let events = handle.history(exec).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, WorkflowEvent::ActivityCompleted { .. })),
        "the turn completed"
    );

    // The management API of the plugin lists the agent.
    let listed: Value = client.get("/harvest/workflows").send().await.json();
    assert!(
        listed.to_string().contains(id.as_str()),
        "the API lists the agent: {listed}"
    );
}

/// An interrupt reaches the op that waits on its approval, as the `sqlite`
/// module of the agent crate does.
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupt_ends_an_approval_wait() {
    let mut backend = Plugin::new();
    let model = PromptModel::new();
    backend
        .serve(overlay_read_suite::base_harness("support", model.clone()))
        .await;
    let id = AgentId::new("support", "interrupt").unwrap();
    backend.send(&id, &AgentOp::message("write it")).await;
    backend
        .wait_approval(&id, &overlay_read_suite::first_approval())
        .await;

    let stop = AgentOp::urgent("never mind", "m-2", Urgency::Interrupt);
    agent::send(backend.handle(), &id, &stop, Some("m-2"))
        .await
        .unwrap();
    let state = backend
        .settle(&id, &|state: &AgentState| state.segments >= 2)
        .await;
    let results: Vec<String> = state
        .transcript
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].contains("interrupted"), "{results:?}");
}

/// A send with no running run reads no run, and the state reads `None`.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_agent_has_no_run_and_no_state() {
    let mut backend = Plugin::new();
    backend
        .serve(AgentHarness::new(PromptModel::new()).agent(AgentProfile::new("support")))
        .await;
    let id = AgentId::new("support", "nobody").unwrap();
    assert_eq!(
        agent::current_run(backend.handle(), &id).await.unwrap(),
        None
    );
    assert!(backend.state(&id).await.is_none());
}
