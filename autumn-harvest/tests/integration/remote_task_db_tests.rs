//! Remote task calls on a real Postgres (issue #2006).
//!
//! - A workflow calls a remote MCP task and suspends. The worker and the
//!   poller stop. A new worker and a new poller finish the run.
//! - A tool result with `isError: true` completes the run. It does not retry.
//! - A failed remote task fails the run.
//! - The first settlement wins. A later one returns `false`.
//! - The poller reads live runs only, backs off a failing read, caps the
//!   result and pages through every token.
//!
//! Each test that runs a worker also replays the recorded history. When
//! `HARVEST_TEST_DATABASE_URL` is set, it is an admin URL and each test gets
//! a throwaway database. Otherwise a testcontainer starts.
#![cfg(feature = "db")]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
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
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_execution_from_url, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};
use crate::throwaway_db::ThrowawayDb;

type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const WAIT: Duration = Duration::from_secs(30);

// ── Setup ────────────────────────────────────────────────────────────────

/// A fresh, migrated database. The guards drop it at the end of the test.
struct Db {
    url: String,
    _throwaway: Option<ThrowawayDb>,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn setup() -> Db {
    if let Some(db) = ThrowawayDb::create("remote_task").await {
        return Db {
            url: db.url(),
            _throwaway: Some(db),
            _container: None,
        };
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
    Db {
        url,
        _throwaway: None,
        _container: Some(container),
    }
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
    /// The task id of each read, in order.
    reads: Mutex<Vec<String>>,
}

impl FakeServer {
    fn set(&self, task_id: &str, state: RemoteTaskState) {
        self.states
            .lock()
            .expect("states")
            .insert(task_id.to_string(), state);
    }

    fn forget(&self, task_id: &str) {
        self.states.lock().expect("states").remove(task_id);
    }

    fn starts(&self) -> u32 {
        *self.starts.lock().expect("starts")
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().expect("reads").clone()
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
            let task_id = {
                let mut by_key = self.0.by_key.lock().expect("keys");
                let next = by_key.len();
                by_key
                    .entry(idempotency_key.to_string())
                    .or_insert_with(|| format!("task-{next}"))
                    .clone()
            };
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
            self.0
                .reads
                .lock()
                .expect("reads")
                .push(handle.task_id.clone());
            self.0
                .states
                .lock()
                .expect("states")
                .get(&handle.task_id)
                .cloned()
                .ok_or_else(|| RemoteTaskError::non_retryable("Task not found"))
        })
    }
}

// ── Workflow ─────────────────────────────────────────────────────────────

const WORKFLOW: &str = "remote_report_wf";

/// Calls the remote `export` tool and returns its outcome.
fn remote_report_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let call = RemoteTaskCall::mcp("reports", "export", input, Duration::from_secs(600));
        let outcome = ctx
            .call_remote_task(&call)
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
    pool: DbPool,
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
async fn poll_until_settled(pool: &DbPool, poller: &RemoteTaskPoller) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let report = poller.poll_once(pool).await.expect("poll");
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
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());

    let a = Process::start(url, "remote-task-a", &server);
    let exec_id = start_run(url, "report-1", json!({"year": 2026})).await;
    let token = wait_for_token(url, exec_id).await;

    // The task still runs, so a poll settles nothing.
    let report = a.poller.poll_once(&a.pool).await.expect("poll");
    assert_eq!(report.polled, 1);
    assert_eq!(report.settled, 0);
    assert_eq!(task_state(url, token).await, "PENDING");
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
        load_execution_from_url(url, exec_id).await.state,
        "RUNNING",
        "nothing settles the token while no process runs"
    );

    let b = Process::start(url, "remote-task-b", &server);
    poll_until_settled(&b.pool, &b.poller).await;
    let done = wait_for_execution_state_with_timeout(url, exec_id, "COMPLETED", WAIT).await;
    let outcome: RemoteTaskOutcome =
        serde_json::from_value(done.output.expect("output")).expect("decode outcome");
    assert_eq!(
        outcome,
        RemoteTaskOutcome {
            result: tool,
            is_error: true,
        }
    );
    assert_eq!(task_state(url, token).await, "COMPLETED");
    assert_eq!(
        server.starts(),
        1,
        "the restart does not start the task again"
    );
    b.stop().await;

    assert_replays_clean(url, exec_id).await;
}

/// A failed remote task fails the run with the remote message.
#[tokio::test]
async fn a_failed_remote_task_fails_the_run() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-fail", &server);

    let exec_id = start_run(url, "report-2", json!({})).await;
    let token = wait_for_token(url, exec_id).await;
    server.set("task-0", RemoteTaskState::Failed("export crashed".into()));
    poll_until_settled(&p.pool, &p.poller).await;

    let done = wait_for_execution_state_with_timeout(url, exec_id, "FAILED", WAIT).await;
    let error = done.error.expect("error");
    assert!(error.contains("export crashed"), "got {error}");
    assert_eq!(task_state(url, token).await, "FAILED");
    p.stop().await;

    assert_replays_clean(url, exec_id).await;
}

/// AC2 alignment: the first settlement wins. A later one returns `false`,
/// as the durable promise resolvers of #1985 do. A state that has not ended
/// settles nothing.
#[tokio::test]
async fn the_first_settlement_wins() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-settle", &server);

    let exec_id = start_run(url, "report-3", json!({})).await;
    let token = wait_for_token(url, exec_id).await;
    let codecs = PayloadCodecs::default();
    let mut conn = connect(url).await;

    let working = remote_task::resolve(&mut conn, token, &RemoteTaskState::Working, &codecs)
        .await
        .expect("resolve working");
    assert!(!working, "a working task settles nothing");
    assert_eq!(task_state(url, token).await, "PENDING");

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

    wait_for_execution_state_with_timeout(url, exec_id, "COMPLETED", WAIT).await;
    assert_eq!(task_state(url, token).await, "COMPLETED");
    p.stop().await;
}

/// The poller reads each pending handle from history, one page at a time.
/// A small batch still reaches every token: the cursor advances, then
/// wraps.
#[tokio::test]
async fn pending_handles_page_in_token_order() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-page", &server);

    let one = start_run(url, "report-4", json!({})).await;
    let two = start_run(url, "report-5", json!({})).await;
    let mut tokens = [
        wait_for_token(url, one).await,
        wait_for_token(url, two).await,
    ];
    tokens.sort_by_key(ExternalActivityToken::as_uuid);
    let codecs = PayloadCodecs::default();
    let mut conn = connect(url).await;

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
    let first_id = first[0].handle.task_id.clone();
    let second_id = second[0].handle.task_id.clone();

    let poller = RemoteTaskPoller::new(&RemoteTasks::new(FakeTransport(Arc::clone(&server))))
        .with_batch_size(1);
    let before = server.reads().len();
    for _ in 0..3 {
        poller.poll_once(&p.pool).await.expect("poll");
    }
    assert_eq!(
        server.reads()[before..],
        [first_id.clone(), second_id, first_id],
        "the cursor advances, then wraps"
    );
    p.stop().await;
}

/// The poller skips a run that has ended. A late remote result does not
/// write after the terminal event.
#[tokio::test]
async fn the_poller_skips_a_run_that_has_ended() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-ended", &server);

    let exec_id = start_run(url, "report-6", json!({})).await;
    let token = wait_for_token(url, exec_id).await;
    let mut conn = connect(url).await;
    autumn_harvest::execution::terminate_workflow_execution(
        &mut conn,
        exec_id,
        "operator stop",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("terminate");
    server.set(
        "task-0",
        RemoteTaskState::Completed(RemoteTaskOutcome {
            result: json!({"content": []}),
            is_error: false,
        }),
    );

    let report = p.poller.poll_once(&p.pool).await.expect("poll");
    assert_eq!(report.polled, 0, "an ended run is not polled");
    let settled = remote_task::resolve(
        &mut conn,
        token,
        &RemoteTaskState::Failed("late".into()),
        &PayloadCodecs::default(),
    )
    .await
    .expect("resolve");
    assert!(!settled, "a push for an ended run settles nothing");
    assert_eq!(task_state(url, token).await, "PENDING");
    p.stop().await;
}

/// A failing remote read backs off: the next poll skips the token. The
/// token stays pending.
#[tokio::test]
async fn a_failing_read_backs_off() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-backoff", &server);

    let exec_id = start_run(url, "report-7", json!({})).await;
    let token = wait_for_token(url, exec_id).await;
    server.forget("task-0");

    let first = p.poller.poll_once(&p.pool).await.expect("poll 1");
    assert_eq!((first.polled, first.errors), (1, 1));
    let second = p.poller.poll_once(&p.pool).await.expect("poll 2");
    assert_eq!(second.polled, 0, "the failing token backs off");
    assert_eq!(task_state(url, token).await, "PENDING");
    p.stop().await;
}

/// A remote result over the limit fails the run. It does not reach history.
#[tokio::test]
async fn a_result_over_the_limit_fails_the_run() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-cap", &server);

    let exec_id = start_run(url, "report-8", json!({})).await;
    wait_for_token(url, exec_id).await;
    server.set(
        "task-0",
        RemoteTaskState::Completed(RemoteTaskOutcome {
            result: json!({"content": [{"type": "text", "text": "x".repeat(4096)}]}),
            is_error: false,
        }),
    );
    let poller = RemoteTaskPoller::new(&RemoteTasks::new(FakeTransport(Arc::clone(&server))))
        .with_max_result_bytes(1024);
    poll_until_settled(&p.pool, &poller).await;

    let done = wait_for_execution_state_with_timeout(url, exec_id, "FAILED", WAIT).await;
    let error = done.error.expect("error");
    assert!(error.contains("the limit is 1024"), "got {error}");
    p.stop().await;
}

/// A cap of 0 means no cap, as on the worker result paths.
#[tokio::test]
async fn a_zero_result_cap_means_no_cap() {
    let db = setup().await;
    let url = &db.url;
    let server = Arc::new(FakeServer::default());
    let p = Process::start(url, "remote-task-nocap", &server);

    let exec_id = start_run(url, "report-9", json!({})).await;
    wait_for_token(url, exec_id).await;
    server.set(
        "task-0",
        RemoteTaskState::Completed(RemoteTaskOutcome {
            result: json!({"content": [{"type": "text", "text": "x".repeat(4096)}]}),
            is_error: false,
        }),
    );
    let poller = RemoteTaskPoller::new(&RemoteTasks::new(FakeTransport(Arc::clone(&server))))
        .with_max_result_bytes(0);
    poll_until_settled(&p.pool, &poller).await;

    wait_for_execution_state_with_timeout(url, exec_id, "COMPLETED", WAIT).await;
    p.stop().await;
}
