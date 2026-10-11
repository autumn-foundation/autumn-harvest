//! Remote task calls on a real Postgres (issue #2006).
//!
//! - A workflow calls a remote MCP task and suspends. The worker and the
//!   poller stop. A new worker and a new poller finish the run.
//! - A tool result with `isError: true` completes the run. It does not retry.
//! - A failed remote task fails the run.
//! - The first settlement wins. A later one returns `false`.
//!
//! Each test that runs a worker also replays the recorded history. When
//! `HARVEST_TEST_DATABASE_URL` is set, it is an admin URL and each test gets
//! a fresh database. Otherwise a testcontainer starts.
#![cfg(feature = "db")]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{SharedState, WorkflowContext};
use autumn_harvest::external_task::{self, ExternalHandoffFilters};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::remote_task::{
    self, RemoteFuture, RemoteTaskCall, RemoteTaskError, RemoteTaskHandle, RemoteTaskOutcome,
    RemoteTaskPoller, RemoteTaskRequest, RemoteTaskStart, RemoteTaskState, RemoteTaskTransport,
    RemoteTasks,
};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{
    ExecutionId, ExternalActivityToken, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_execution_from_url, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const WAIT: Duration = Duration::from_secs(30);

// ── Setup ────────────────────────────────────────────────────────────────

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

fn with_db_name(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// Returns the URL of a fresh, migrated database.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut admin = connect(&admin_url).await;
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("remote_task_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        let url = with_db_name(&admin_url, &db);
        let mut conn = connect(&url).await;
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

// ── A remote server that outlives the workers ────────────────────────────

/// The remote server. It keeps its tasks across Harvest restarts.
#[derive(Default)]
struct FakeServer {
    /// Task id by start key. A repeated key returns the same task.
    by_key: Mutex<HashMap<String, String>>,
    states: Mutex<HashMap<String, RemoteTaskState>>,
    starts: Mutex<u32>,
    gets: Mutex<u32>,
}

impl FakeServer {
    fn set(&self, task_id: &str, state: RemoteTaskState) {
        self.states
            .lock()
            .expect("states")
            .insert(task_id.to_string(), state);
    }

    fn starts(&self) -> u32 {
        *self.starts.lock().expect("starts")
    }

    fn gets(&self) -> u32 {
        *self.gets.lock().expect("gets")
    }
}

/// A client of [`FakeServer`]. Each worker gets its own client.
struct FakeTransport(Arc<FakeServer>);

impl RemoteTaskTransport for FakeTransport {
    fn start<'a>(
        &'a self,
        request: &'a RemoteTaskRequest,
        idempotency_key: &'a str,
    ) -> RemoteFuture<'a, Result<RemoteTaskStart, RemoteTaskError>> {
        Box::pin(async move {
            *self.0.starts.lock().expect("starts") += 1;
            let mut by_key = self.0.by_key.lock().expect("keys");
            let next = by_key.len();
            let task_id = by_key
                .entry(idempotency_key.to_string())
                .or_insert_with(|| format!("task-{next}"))
                .clone();
            self.0
                .states
                .lock()
                .expect("states")
                .entry(task_id.clone())
                .or_insert(RemoteTaskState::Working);
            Ok(RemoteTaskStart::Task(RemoteTaskHandle {
                server: request.server.clone(),
                protocol: request.protocol,
                task_id,
            }))
        })
    }

    fn get<'a>(
        &'a self,
        handle: &'a RemoteTaskHandle,
    ) -> RemoteFuture<'a, Result<RemoteTaskState, RemoteTaskError>> {
        Box::pin(async move {
            *self.0.gets.lock().expect("gets") += 1;
            self.0
                .states
                .lock()
                .expect("states")
                .get(&handle.task_id)
                .cloned()
                .ok_or_else(|| RemoteTaskError {
                    message: "Task not found".into(),
                    retryable: false,
                })
        })
    }
}

// ── Workflow ─────────────────────────────────────────────────────────────

const WORKFLOW: &str = "remote_report_wf";

/// Calls the remote `export` tool and returns its outcome.
fn remote_report_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let call = RemoteTaskCall::mcp("reports", "export", input, Duration::from_secs(600));
        let outcome = remote_task::call(ctx, &call)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_value(outcome).map_err(|e| e.to_string())
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WORKFLOW,
        module: "remote_task_db_tests",
        handler: remote_report_workflow,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn shared_state(remote: RemoteTasks) -> SharedState {
    let mut map: autumn_harvest::context::SharedStateMap = HashMap::new();
    map.insert(std::any::TypeId::of::<RemoteTasks>(), Box::new(remote));
    Arc::new(map)
}

/// One Harvest process: a worker and a poller with their own client.
struct Process {
    worker: Arc<autumn_harvest::worker::Worker>,
    handle: tokio::task::JoinHandle<()>,
    poller: RemoteTaskPoller,
    pool: autumn_harvest::worker::DbPool,
}

impl Process {
    fn start(url: &str, name: &str, server: &Arc<FakeServer>) -> Self {
        let remote = RemoteTasks::new(FakeTransport(Arc::clone(server)));
        let registry = Arc::new(HandlerRegistry::with_state(
            vec![workflow_info()],
            remote_task::activities(),
            shared_state(remote.clone()),
        ));
        let worker = build_runtime_worker(name, 4, 4, registry);
        let pool = build_test_pool(url);
        let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
        Self {
            worker,
            handle,
            poller: RemoteTaskPoller::new(&remote),
            pool,
        }
    }

    /// Stop the worker. Its pool closes, so it writes nothing more.
    async fn stop(self) {
        self.handle.abort();
        let _ = self.handle.await;
        self.worker.shutdown();
        self.pool.close();
    }
}

async fn start_run(url: &str, workflow_id: &str, input: Value) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let mut conn = connect(url).await;
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: WORKFLOW,
            workflow_id,
            exec_id,
            input: input.into(),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            tenant: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");
    exec_id
}

/// Waits until the run has a pending remote-task token, and returns it.
async fn wait_for_token(url: &str, exec_id: ExecutionId) -> ExternalActivityToken {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let mut conn = connect(url).await;
        let filters = ExternalHandoffFilters {
            execution_id: Some(exec_id),
            ..ExternalHandoffFilters::default()
        };
        let rows = external_task::list_external_handoffs(&mut conn, &filters)
            .await
            .expect("list handoffs");
        if let Some(row) = rows.into_iter().next() {
            assert_eq!(row.activity_name, remote_task::AWAIT_ACTIVITY);
            return row.token;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the run never parked on a remote task token"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until a poll settles a token.
async fn poll_until_settled(url: &str, poller: &RemoteTaskPoller) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let mut conn = connect(url).await;
        let report = poller.poll_once(&mut conn).await.expect("poll");
        if report.settled > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no poll settled a token"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn task_state(url: &str, token: ExternalActivityToken) -> String {
    let mut conn = connect(url).await;
    external_task::find_by_token(&mut conn, token)
        .await
        .expect("find token")
        .expect("token row")
        .state
}

async fn assert_replays_clean(url: &str, exec_id: ExecutionId) {
    let mut conn = connect(url).await;
    let history = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    let report = WorkflowReplayer::new()
        .register_fn(WORKFLOW, remote_report_workflow)
        .replay_from_events(history.events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the recorded history of {exec_id} must replay clean:\n{report}"
    );
}

// ── Tests ────────────────────────────────────────────────────────────────

/// AC1: a workflow calls a remote MCP task and suspends. Worker A and its
/// poller stop while the task runs. The remote task then ends with
/// `isError: true`. Worker B and a new poller settle the token and finish
/// the run. The remote task starts once. The outcome is `Ok` with
/// `is_error`, and the history replays clean.
#[tokio::test]
async fn a_remote_mcp_task_survives_a_worker_restart() {
    let (url, _container) = setup().await;
    let server = Arc::new(FakeServer::default());

    let a = Process::start(&url, "remote-task-a", &server);
    let exec_id = start_run(&url, "report-1", json!({"year": 2026})).await;
    let token = wait_for_token(&url, exec_id).await;

    // The task still runs, so a poll settles nothing.
    let mut conn = connect(&url).await;
    let report = a.poller.poll_once(&mut conn).await.expect("poll");
    assert_eq!(report.polled, 1);
    assert_eq!(report.settled, 0);
    assert_eq!(task_state(&url, token).await, "PENDING");
    a.stop().await;

    // The remote task ends while no Harvest process runs.
    let tool = json!({"content": [{"type": "text", "text": "no data for 2026"}], "isError": true});
    server.set(
        "task-0",
        RemoteTaskState::Completed(RemoteTaskOutcome {
            result: tool.clone(),
            is_error: true,
        }),
    );
    assert_eq!(
        load_execution_from_url(&url, exec_id).await.state,
        "RUNNING",
        "nothing settles the token while no process runs"
    );

    let b = Process::start(&url, "remote-task-b", &server);
    poll_until_settled(&url, &b.poller).await;
    let done = wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
    let outcome: RemoteTaskOutcome =
        serde_json::from_value(done.output.expect("output")).expect("decode outcome");
    assert_eq!(
        outcome,
        RemoteTaskOutcome {
            result: tool,
            is_error: true,
        }
    );
    assert_eq!(task_state(&url, token).await, "COMPLETED");
    assert_eq!(
        server.starts(),
        1,
        "the restart does not start the task again"
    );
    b.stop().await;

    assert_replays_clean(&url, exec_id).await;
}

/// A failed remote task fails the run with the remote message.
#[tokio::test]
async fn a_failed_remote_task_fails_the_run() {
    let (url, _container) = setup().await;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(&url, "remote-task-fail", &server);

    let exec_id = start_run(&url, "report-2", json!({})).await;
    let token = wait_for_token(&url, exec_id).await;
    server.set("task-0", RemoteTaskState::Failed("export crashed".into()));
    poll_until_settled(&url, &p.poller).await;

    let done = wait_for_execution_state_with_timeout(&url, exec_id, "FAILED", WAIT).await;
    let error = done.error.expect("error");
    assert!(error.contains("export crashed"), "got {error}");
    assert_eq!(task_state(&url, token).await, "FAILED");
    p.stop().await;

    assert_replays_clean(&url, exec_id).await;
}

/// AC2 alignment: the first settlement wins. A later one returns `false`,
/// as the durable promise resolvers of #1985 do. A state that has not ended
/// settles nothing.
#[tokio::test]
async fn the_first_settlement_wins() {
    let (url, _container) = setup().await;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(&url, "remote-task-settle", &server);

    let exec_id = start_run(&url, "report-3", json!({})).await;
    let token = wait_for_token(&url, exec_id).await;
    let codecs = PayloadCodecs::default();
    let mut conn = connect(&url).await;

    let working = remote_task::resolve(&mut conn, token, &RemoteTaskState::Working, &codecs)
        .await
        .expect("resolve working");
    assert!(!working, "a working task settles nothing");
    assert_eq!(task_state(&url, token).await, "PENDING");

    let done = RemoteTaskState::Completed(RemoteTaskOutcome {
        result: json!({"content": [], "isError": false}),
        is_error: false,
    });
    let first = remote_task::resolve(&mut conn, token, &done, &codecs)
        .await
        .expect("first");
    let second = remote_task::resolve(
        &mut conn,
        token,
        &RemoteTaskState::Failed("late".into()),
        &codecs,
    )
    .await
    .expect("second");
    assert!(first, "the first settlement wins");
    assert!(!second, "a later settlement is a no-op");

    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
    assert_eq!(task_state(&url, token).await, "COMPLETED");
    p.stop().await;
}

/// The poller reads each pending handle from history, one page at a time.
/// A push relay can use the same list to find a token.
#[tokio::test]
async fn pending_handles_page_in_token_order() {
    let (url, _container) = setup().await;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(&url, "remote-task-page", &server);

    let one = start_run(&url, "report-4", json!({})).await;
    let two = start_run(&url, "report-5", json!({})).await;
    let mut tokens = vec![
        wait_for_token(&url, one).await,
        wait_for_token(&url, two).await,
    ];
    tokens.sort_by_key(ExternalActivityToken::as_uuid);
    let codecs = PayloadCodecs::default();
    let mut conn = connect(&url).await;

    let first = remote_task::pending_remote_tasks(&mut conn, &codecs, None, 1)
        .await
        .expect("page 1");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].token, tokens[0]);
    assert_eq!(first[0].handle.server, "reports");
    let second = remote_task::pending_remote_tasks(&mut conn, &codecs, Some(tokens[0]), 1)
        .await
        .expect("page 2");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].token, tokens[1]);
    let mut ids = vec![
        first[0].handle.task_id.clone(),
        second[0].handle.task_id.clone(),
    ];
    ids.sort();
    assert_eq!(ids, vec!["task-0", "task-1"]);

    // A small batch still reaches every token: the cursor wraps.
    let poller = RemoteTaskPoller::new(&RemoteTasks::new(FakeTransport(Arc::clone(&server))))
        .with_batch_size(1);
    let before = server.gets();
    poller.poll_once(&mut conn).await.expect("poll 1");
    poller.poll_once(&mut conn).await.expect("poll 2");
    poller.poll_once(&mut conn).await.expect("poll 3");
    assert_eq!(server.gets() - before, 3);
    p.stop().await;
}
