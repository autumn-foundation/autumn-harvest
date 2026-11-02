//! The recorded overlay read on the `SQLite` backend (issue #2372).
//!
//! `overlay_read_postgres.rs` runs the same suite on a Postgres worker.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_read_suite;

use std::path::PathBuf;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayReport, WorkflowReplayer};
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentOp, AgentState, Approval, ProfileOverlayStore, SqliteOverlayStore,
    agent_info, sqlite,
};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionSummary, RunState, SqliteRuntime};
use overlay_read_suite::Backend;
use serde_json::json;

/// A `SQLite` file that holds the runs and the overlay store.
struct Sqlite {
    _dir: tempfile::TempDir,
    db: PathBuf,
    rt: Option<SqliteRuntime>,
}

impl Sqlite {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let db = dir.path().join("agent.db");
        Self {
            _dir: dir,
            db,
            rt: None,
        }
    }

    fn rt(&mut self) -> &mut SqliteRuntime {
        self.rt.as_mut().expect("serve first")
    }

    fn open(&self, harness: AgentHarness) -> SqliteRuntime {
        let mut rt = SqliteRuntime::open(&self.db).expect("the database opens");
        sqlite::register(&mut rt, Arc::new(harness));
        rt
    }

    fn runs(&mut self, id: &AgentId) -> Vec<autumn_harvest::ExecutionId> {
        let filter = ExecutionFilter::new()
            .workflow_name("agent")
            .workflow_id(id.as_str());
        let mut runs: Vec<_> = self
            .rt()
            .list_executions(&filter)
            .unwrap()
            .executions
            .iter()
            .filter_map(ExecutionSummary::execution_id)
            .collect();
        runs.reverse();
        runs
    }
}

impl Backend for Sqlite {
    fn kind(&self) -> String {
        "support".to_owned()
    }

    fn store(&self, validator: Arc<AgentHarness>) -> Arc<dyn ProfileOverlayStore> {
        Arc::new(SqliteOverlayStore::open(&self.db, validator).expect("the store opens"))
    }

    async fn serve(&mut self, harness: AgentHarness) {
        self.rt = None;
        self.rt = Some(self.open(harness));
    }

    async fn send(&mut self, id: &AgentId, op: &AgentOp) {
        sqlite::send(self.rt(), id, op, None).unwrap();
    }

    async fn settle(
        &mut self,
        id: &AgentId,
        done: &(dyn Fn(&AgentState) -> bool + Sync),
    ) -> AgentState {
        sqlite::drive(self.rt(), id).await.unwrap();
        let state = sqlite::state(self.rt(), id).await.unwrap().expect("a run");
        assert!(done(&state), "the agent settled early: {state:?}");
        state
    }

    async fn wait_approval(&mut self, id: &AgentId, signal: &str) {
        let blocked = sqlite::drive(self.rt(), id).await.unwrap();
        let Some(RunState::WaitingSignal(waits)) = blocked else {
            panic!("the op waits on its approval, got {blocked:?}");
        };
        assert_eq!(waits, signal);
    }

    async fn approve(&mut self, id: &AgentId, signal: &str) {
        let exec = sqlite::current_run(self.rt(), id).unwrap().expect("a run");
        sqlite::decide(self.rt(), exec, signal, &Approval::Approve).unwrap();
    }

    async fn delete(&mut self, id: &AgentId) {
        sqlite::delete(self.rt(), id, None).unwrap();
        let ended = sqlite::drive(self.rt(), id).await.unwrap();
        assert!(
            matches!(ended, Some(RunState::Completed(_))),
            "the run ended: {ended:?}"
        );
    }

    async fn history(&mut self, id: &AgentId) -> Vec<WorkflowEvent> {
        let runs = self.runs(id);
        runs.into_iter()
            .flat_map(|exec| self.rt().load_history(exec).unwrap())
            .collect()
    }

    async fn replay(&mut self, id: &AgentId, harness: AgentHarness) -> Vec<ReplayReport> {
        let mut reports = Vec::new();
        for exec in self.runs(id) {
            let events = self.rt().load_history(exec).unwrap();
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
        // A new process: the query replays the run from the file.
        self.rt = None;
        let rt = self.open(harness);
        let state = sqlite::state(&rt, id).await.unwrap().expect("a run");
        self.rt = Some(rt);
        state
    }

    async fn listed(&mut self, version: &str) -> Vec<String> {
        let filter = ExecutionFilter::new()
            .workflow_name("agent")
            .search_attribute("agent_profile_version", json!(version));
        self.rt()
            .list_executions(&filter)
            .unwrap()
            .executions
            .iter()
            .filter_map(|summary| summary.workflow_id.value().cloned())
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_is_exact() {
    overlay_read_suite::replay_is_exact(&mut Sqlite::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_next_op_sees_v2() {
    overlay_read_suite::the_next_op_sees_v2(&mut Sqlite::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mid_op_activation_is_ignored() {
    overlay_read_suite::a_mid_op_activation_is_ignored(&mut Sqlite::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_search_attribute_follows_the_op() {
    overlay_read_suite::the_search_attribute_follows_the_op(&mut Sqlite::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_stored_data_fails_safe() {
    overlay_read_suite::bad_stored_data_fails_safe(&mut Sqlite::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_narrowed_deploy_fails_safe() {
    overlay_read_suite::a_narrowed_deploy_fails_safe(&mut Sqlite::new()).await;
}

/// Every search attribute that an agent publishes is named in the
/// "Search attributes" paragraph of `docs/agent-adapter.md` section 15.
/// The agent sets each one: a tenant, a mesh, a heartbeat and a version.
#[tokio::test(flavor = "multi_thread")]
async fn every_published_attribute_is_documented() {
    use std::time::Duration;

    use autumn_harvest_agent::{AgentHeartbeat, AgentProfile, PROFILE_VERSION_ATTR};

    let mut backend = Sqlite::new();
    let model = overlay_read_suite::PromptModel::new();
    let profile = AgentProfile::new("support")
        .tenant("acme")
        .mesh("ops")
        .heartbeat(AgentHeartbeat::every(Duration::from_secs(300)));
    let base = AgentHarness::new(model).agent(profile);
    let (harness, store) = overlay_read_suite::with_store(&backend, base);
    let prompt = overlay_read_suite::prompt(overlay_read_suite::PROMPT_V1);
    overlay_read_suite::adopt(store.as_ref(), "support", &prompt, None).await;
    backend.serve(harness).await;
    let id = AgentId::new("support", "documented").unwrap();
    backend.send(&id, &AgentOp::message("hello")).await;
    backend
        .settle(&id, &|state: &AgentState| state.segments == 1)
        .await;

    let page = backend
        .rt()
        .list_executions(&ExecutionFilter::new().workflow_id(id.as_str()))
        .unwrap();
    let attrs = page.executions[0]
        .search_attributes
        .value()
        .unwrap()
        .clone();
    assert!(attrs.contains_key(PROFILE_VERSION_ATTR), "{attrs:?}");
    let doc = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/agent-adapter.md"),
    )
    .unwrap();
    let start = doc.find("**Search attributes.**").expect("the paragraph");
    let paragraph = &doc[start..start + doc[start..].find("\n\n").unwrap()];
    for key in attrs.keys() {
        assert!(
            paragraph.contains(&format!("`{key}`")),
            "docs/agent-adapter.md does not name {key}"
        );
    }
}
