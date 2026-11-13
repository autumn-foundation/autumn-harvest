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
use autumn_harvest::handle::{WorkflowResult, WorkflowResultState};
use autumn_harvest::testing::{ReplayReport, WorkflowReplayer};
use autumn_harvest::{ExecutionId, WorkflowIdReusePolicy};
use autumn_harvest_agent::sampling::{
    InMemorySamplingPolicyStore, SamplingPolicy, SamplingPolicyStore, ScorerRef,
};
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentError, AgentHarness, AgentId, AgentOp, AgentProfile,
    AgentState, AgentStop, AgentTask, AnswerCheck, Approval, BoxFuture, ChatRole, ContentPart,
    ProfileOverlayStore, ScoreContext, ScoreInput, Scorer, SqliteOverlayStore, Urgency, Verdict,
    WORKFLOW_NAME, agent_info,
};
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{
    ExecutionFilter, ExecutionSummary, HarvestSqlite, HarvestSqliteError, SignalWithStartParams,
    SqliteError, StartOptions, agent,
};
use autumn_web::config::AutumnConfig;
use autumn_web::test::{TestApp, TestClient};
use overlay_read_suite::{Backend, PromptModel};
use serde_json::{Value, json};

/// How long a poll waits before the test fails.
const LIMIT: Duration = Duration::from_secs(30);

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

    /// The state of the agent `id`, or `None` while it is not ready.
    async fn state(&self, id: &AgentId) -> Option<AgentState> {
        match agent::state(self.handle(), id).await {
            Ok(state) => state,
            // The agent reads its profile before it registers the query.
            Err(HarvestSqliteError::Sqlite(SqliteError::QueryNotFound { .. })) => None,
            Err(error) => panic!("the state reads: {error}"),
        }
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

    async fn reload_kind(&mut self, kind: &str) -> Vec<AgentId> {
        agent::reload_profiles(self.handle(), kind)
            .await
            .expect("the reloads are sent")
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
        let key = format!("decide:{signal}");
        let decide = || agent::decide(self.handle(), id, signal, &Approval::Approve, Some(&key));
        assert!(decide().await.expect("the decision is sent"), "a new key");
        assert!(
            !decide().await.expect("the retry is sent"),
            "a retry of the key is not delivered again"
        );
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

#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_after_an_auto_adoption_restores_v1() {
    overlay_read_suite::a_rollback_after_an_auto_adoption_restores_v1(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_reads_the_new_profile() {
    overlay_read_suite::a_reload_reads_the_new_profile(&mut Plugin::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_keeps_the_active_overlay() {
    overlay_read_suite::a_reload_keeps_the_active_overlay(&mut Plugin::new()).await;
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
        // The agent reads its profile before it registers the query. Until
        // the first decision records that read, the query has no handler.
        match handle.query(exec, ENTITY_STATE_QUERY, Value::Null).await {
            Ok(value) => {
                let state: AgentState = serde_json::from_value(value).unwrap();
                if state.segments >= 1 {
                    break state;
                }
            }
            Err(HarvestSqliteError::Sqlite(SqliteError::QueryNotFound { .. })) => {}
            Err(error) => panic!("the state query failed: {error}"),
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
    let turns = overlay_read_suite::recorded(&events, "agent_model_turn");
    assert_eq!(turns.len(), 1, "one recorded model turn");

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

/// An agent with no op has no run, and its state reads `None`.
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

/// A client retry of an op with the same key runs the op once. So does a
/// retry of an interrupt.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_key_delivers_the_op_once() {
    let mut backend = Plugin::new();
    let model = PromptModel::new();
    backend
        .serve(overlay_read_suite::base_harness("support", model.clone()))
        .await;
    let id = AgentId::new("support", "retry").unwrap();
    let hello = AgentOp::message("hello");
    let first = agent::send(backend.handle(), &id, &hello, Some("m-1"))
        .await
        .unwrap();
    assert!(first.signal_delivered);
    let again = agent::send(backend.handle(), &id, &hello, Some("m-1"))
        .await
        .unwrap();
    assert!(!again.signal_delivered, "the retry is a duplicate");
    backend
        .settle(&id, &|state: &AgentState| state.segments >= 1)
        .await;

    backend.send(&id, &AgentOp::message("write it")).await;
    backend
        .wait_approval(&id, &overlay_read_suite::first_approval())
        .await;
    let stop = AgentOp::urgent("never mind", "m-2", Urgency::Interrupt);
    for _ in 0..2 {
        agent::send(backend.handle(), &id, &stop, Some("m-2"))
            .await
            .unwrap();
    }
    let state = backend
        .settle(&id, &|state: &AgentState| state.segments >= 3)
        .await;
    let users = texts(&state)
        .into_iter()
        .filter(|(role, _text)| *role == ChatRole::User)
        .count();
    assert_eq!(users, 3, "hello, write it and never mind, each once");
    assert_eq!(state.failures, 0, "{:?}", state.last_error);
}

/// A scorer that passes each answer.
#[derive(Debug)]
struct PassAll;

impl Scorer for PassAll {
    fn id(&self) -> &'static str {
        "pass_all"
    }

    fn score<'a>(
        &'a self,
        _input: &'a ScoreInput,
        _cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(async { Verdict::score(1.0) })
    }
}

/// Run one `agent_loop` task through the handle, and return its result.
async fn run_task(handle: &HarvestSqlite, task: &AgentTask) -> WorkflowResult {
    let started = handle
        .start(
            WORKFLOW_NAME,
            serde_json::to_value(task).unwrap(),
            StartOptions::default(),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + LIMIT;
    loop {
        let result = handle.result(started.exec_id).await.unwrap();
        if result.state != WorkflowResultState::Running {
            return result;
        }
        assert!(Instant::now() < deadline, "the task did not end");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// SQLite keeps no run tenant, so the answer check admits the tenant that
/// the task declares, as the `sqlite` module of the agent crate does.
#[tokio::test(flavor = "multi_thread")]
async fn the_answer_check_reads_the_task_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let policies = Arc::new(InMemorySamplingPolicyStore::new());
    let policy = SamplingPolicy {
        max_answer_retries: 2,
        ..SamplingPolicy::default()
    };
    policies.set("writer", &policy, "ops").await.unwrap();
    let harness = AgentHarness::new(PromptModel::new())
        .agent(AgentProfile::new("writer").tenant("acme"))
        .scorer(Arc::new(PassAll) as Arc<dyn Scorer>)
        .sampling_policies(policies);
    let client = boot(dir.path(), harness);
    let handle = handle_of(&client);
    let check =
        AnswerCheck::new("writer", ScorerRef::deterministic("pass_all").unwrap()).retries(1);
    let task = |tenant: &str| {
        AgentTask::new("Answer.")
            .answer_check(check.clone())
            .tenant(tenant)
    };

    let own = run_task(&handle, &task("acme")).await;
    assert_eq!(own.state, WorkflowResultState::Completed, "{:?}", own.error);
    let other = run_task(&handle, &task("globex")).await;
    assert_eq!(other.state, WorkflowResultState::Failed);
    let error = other.error.unwrap_or_default();
    assert!(error.contains("other_tenant"), "{error}");
}
