//! The recorded overlay read on a Postgres worker (issue #2372).
//!
//! `overlay_read.rs` runs the same suite on `SQLite`. Here a Postgres
//! worker runs the agent, and `PostgresOverlayStore` holds the versions.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! The agent activities run on the `default` queue, so run the tests one
//! at a time. CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_read_suite;

use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::testing::{ReplayReport, WorkflowReplayer};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ExecutionId, HarvestBuilder, ShardId, WorkerConfig, migrate};
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentHarness, AgentId, AgentOp, AgentState, Approval, Control,
    PostgresOverlayStore, ProfileOverlayStore, agent_info, workflow,
};
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use overlay_read_suite::Backend;
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// How long a wait on the worker may take.
const LIMIT: Duration = Duration::from_secs(30);

/// A migrated database, and the container that serves it, if any.
async fn database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    (url, container)
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(16)
        .build()
        .expect("pool")
}

/// A name that no earlier test or run used, so a reused database works.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{prefix}-{nanos}")
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    text: String,
}

/// A Postgres database, one worker and one queue.
struct Pg {
    url: String,
    pool: DbPool,
    queue: String,
    kind: String,
    worker: Option<(Arc<Worker>, tokio::task::JoinHandle<()>)>,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Pg {
    async fn new() -> Self {
        let (url, container) = database().await;
        Self {
            pool: pool(&url),
            url,
            queue: unique("overlay-read"),
            kind: unique("k"),
            worker: None,
            _container: container,
        }
    }

    async fn conn(&self) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect")
    }

    fn stop(&mut self) {
        if let Some((worker, handle)) = self.worker.take() {
            worker.shutdown();
            handle.abort();
        }
    }

    /// The running run of the agent `id`.
    async fn current(&self, id: &AgentId) -> Option<ExecutionId> {
        let rows: Vec<TextRow> = diesel::sql_query(
            "SELECT id::text AS text FROM harvest_workflow_executions \
              WHERE workflow_name = 'agent' AND workflow_id = $1 AND state = 'RUNNING'",
        )
        .bind::<Text, _>(id.as_str())
        .load(&mut self.conn().await)
        .await
        .expect("load runs");
        // A slice pattern: `first()` would resolve to `RunQueryDsl::first`.
        match rows.as_slice() {
            [row, ..] => Some(row.text.parse().expect("an execution id")),
            [] => None,
        }
    }

    /// The last committed state of the agent `id`.
    async fn snapshot(&self, id: &AgentId) -> Option<AgentState> {
        let row = autumn_harvest::entity_state::read(
            &mut self.conn().await,
            &PayloadCodecs::default(),
            AGENT_WORKFLOW_NAME,
            id.as_str(),
        )
        .await
        .expect("read the projection")?;
        let state = row.snapshot.query_answer(ENTITY_STATE_QUERY)?;
        Some(serde_json::from_value(state).expect("the state decodes"))
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Backend for Pg {
    fn kind(&self) -> String {
        self.kind.clone()
    }

    fn store(&self, validator: Arc<AgentHarness>) -> Arc<dyn ProfileOverlayStore> {
        Arc::new(PostgresOverlayStore::new(self.pool.clone(), validator))
    }

    async fn serve(&mut self, harness: AgentHarness) {
        self.stop();
        let mut config = WorkerConfig::default().with_queues([self.queue.as_str(), "default"]);
        config.max_concurrent_workflows = 4;
        config.max_concurrent_activities = 4;
        let (registry, _dags, _schedules, config) = HarvestBuilder::new()
            .workflows(workflow::workflows())
            .activities(workflow::activities())
            .state(harness)
            .worker(config)
            .build()
            .into_worker_parts();
        let mut runtime = WorkerRuntimeConfig::from(config);
        runtime.poll_interval = Duration::from_millis(25);
        let worker = Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"));
        let handle = tokio::spawn({
            let worker = Arc::clone(&worker);
            let pool = self.pool.clone();
            async move {
                let _ = worker.run(&pool).await;
            }
        });
        self.worker = Some((worker, handle));
    }

    async fn send(&mut self, id: &AgentId, op: &AgentOp) {
        use autumn_harvest::WorkflowIdReusePolicy;
        use autumn_harvest::execution::{
            SignalWithStartParams, signal_with_start_workflow_execution,
        };
        signal_with_start_workflow_execution(
            &mut self.conn().await,
            SignalWithStartParams {
                workflow_name: AGENT_WORKFLOW_NAME,
                workflow_id: id.as_str(),
                exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
                input: json!({}),
                parent_id: None,
                queue_name: &self.queue,
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                chain_execution_timeout: None,
                max_workflow_chain_timeout_ceiling: None,
                concurrency_key: None,
                concurrency_limit: None,
                concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                signal_name: ENTITY_OP_SIGNAL,
                signal_payload: Agent::payload(op),
                idempotency_key: None,
                max_workflow_input_bytes: 0,
                max_signal_payload_bytes: 0,
                owner: None,
                runbook_url: None,
                severity: None,
                context_headers: None,
                sla: None,
                reject_fresh_if_debounced: false,
                workflow_retry_policy: None,
                max_workflow_attempts_ceiling: None,
                workflow_info: None,
                start_source_override: None,
                start_source_ref_override: None,
                tenant: None,
            },
        )
        .await
        .expect("signal-with-start");
    }

    async fn settle(
        &mut self,
        id: &AgentId,
        done: &(dyn Fn(&AgentState) -> bool + Sync),
    ) -> AgentState {
        let deadline = Instant::now() + LIMIT;
        loop {
            if let Some(state) = self.snapshot(id).await
                && done(&state)
            {
                return state;
            }
            assert!(Instant::now() < deadline, "the agent {id} did not settle");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_approval(&mut self, id: &AgentId, _signal: &str) {
        let deadline = Instant::now() + LIMIT;
        loop {
            let rows: Vec<TextRow> = diesel::sql_query(
                "SELECT coalesce(search_attrs ->> 'agent_status', '') AS text \
                   FROM harvest_workflow_executions \
                  WHERE workflow_name = 'agent' AND workflow_id = $1 AND state = 'RUNNING'",
            )
            .bind::<Text, _>(id.as_str())
            .load(&mut self.conn().await)
            .await
            .expect("load the status");
            if rows.iter().any(|row| row.text == "waiting_approval") {
                return;
            }
            assert!(Instant::now() < deadline, "the agent {id} never waited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn approve(&mut self, id: &AgentId, signal: &str) {
        let exec = self.current(id).await.expect("a run");
        autumn_harvest::signal::send_signal(
            &mut self.conn().await,
            exec,
            signal,
            serde_json::to_value(Approval::Approve).unwrap(),
        )
        .await
        .expect("approve");
    }

    async fn delete(&mut self, id: &AgentId) {
        let exec = self.current(id).await.expect("a run");
        autumn_harvest::signal::send_signal(
            &mut self.conn().await,
            exec,
            ENTITY_OP_SIGNAL,
            Agent::delete_payload(),
        )
        .await
        .expect("delete");
        let deadline = Instant::now() + LIMIT;
        while self.current(id).await.is_some() {
            assert!(Instant::now() < deadline, "the run of {id} did not end");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn replay(&mut self, id: &AgentId) -> Vec<ReplayReport> {
        let rows: Vec<TextRow> = diesel::sql_query(
            "SELECT id::text AS text FROM harvest_workflow_executions \
              WHERE workflow_name = 'agent' AND workflow_id = $1 ORDER BY started_at",
        )
        .bind::<Text, _>(id.as_str())
        .load(&mut self.conn().await)
        .await
        .expect("load runs");
        let mut reports = Vec::new();
        for row in rows {
            let report = WorkflowReplayer::new()
                .register(vec![agent_info()])
                .replay_from_db(
                    &mut self.conn().await,
                    row.text.parse().expect("an execution id"),
                )
                .await
                .expect("replay");
            reports.push(report);
        }
        assert!(!reports.is_empty(), "the agent has a run");
        reports
    }

    async fn rebuilt(&mut self, id: &AgentId, harness: AgentHarness) -> AgentState {
        // A new worker holds no run in memory, so it replays the run to
        // take the next op. A resume of an open agent changes nothing.
        let before = self.snapshot(id).await.expect("a state");
        self.serve(harness).await;
        self.send(id, &AgentOp::Control(Control::Resume)).await;
        let last = before.last_active_ms;
        self.settle(id, &move |state: &AgentState| state.last_active_ms > last)
            .await
    }

    async fn listed(&mut self, version: &str) -> Vec<String> {
        // The shape that the `filter` grammar compiles for
        // `attrs.agent_profile_version = '<version>'`.
        let rows: Vec<TextRow> = diesel::sql_query(
            "SELECT workflow_id AS text FROM harvest_workflow_executions \
              WHERE workflow_name = 'agent' AND search_attrs @> $1",
        )
        .bind::<diesel::sql_types::Jsonb, _>(json!({ "agent_profile_version": version }))
        .load(&mut self.conn().await)
        .await
        .expect("list");
        rows.into_iter().map(|row| row.text).collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_is_exact() {
    overlay_read_suite::replay_is_exact(&mut Pg::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_op_sees_v2() {
    overlay_read_suite::the_next_op_sees_v2(&mut Pg::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mid_op_activation_is_ignored() {
    overlay_read_suite::a_mid_op_activation_is_ignored(&mut Pg::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_search_attribute_follows_the_op() {
    overlay_read_suite::the_search_attribute_follows_the_op(&mut Pg::new().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_stored_data_fails_safe() {
    overlay_read_suite::bad_stored_data_fails_safe(&mut Pg::new().await).await;
}
