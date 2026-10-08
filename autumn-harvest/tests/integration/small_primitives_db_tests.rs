//! DB tests for the issue #1985 primitives.
//!
//! - `OverlapPolicy::AllowAll` starts a run while another run is active.
//! - `ctx.wait_for_signal_matching` completes only on a matching payload.
//! - A durable promise settles once and replays clean.
//!
//! Each test that runs a worker also replays the recorded history with
//! `WorkflowReplayer`. When `HARVEST_TEST_DATABASE_URL` is set, it is an admin
//! URL and each test gets a fresh database. Otherwise a testcontainer starts.
#![cfg(feature = "db")]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::durable_promise::{self, PromiseId};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{
    ExecutionId, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{
    DagCatalog, SchedulerMonitor, StartSource, StartWorkflowParams,
    start_or_load_workflow_execution, tick_once,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_execution_from_url, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

const MODULE: &str = "small_primitives_db_tests";
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
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("small_prim_{}_{}", std::process::id(), n);
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

// ── Handlers ─────────────────────────────────────────────────────────────

/// Waits for the signal `finish`, so two scheduled runs overlap.
fn overlap_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("finish")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Waits for the order with id 42.
fn order_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal_matching("order", |p| p["id"] == 42)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Creates a promise, publishes its token through an activity and waits.
fn promise_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let promise = ctx.new_promise().map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("publish_token", json!(promise.id().to_string()), "default")
            .await
            .map_err(|e| e.to_string())?;
        match promise.wait::<Value>().await.map_err(|e| e.to_string())? {
            Ok(value) => Ok(json!({ "resolved": value })),
            Err(rejected) => Ok(json!({ "rejected": rejected.error })),
        }
    })
}

/// Waits for `go`, then waits on the named promise `approval`.
fn late_wait_promise_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        let promise = ctx.promise("approval").map_err(|e| e.to_string())?;
        match promise.wait::<Value>().await.map_err(|e| e.to_string())? {
            Ok(value) => Ok(json!({ "resolved": value })),
            Err(rejected) => Ok(json!({ "rejected": rejected.error })),
        }
    })
}

/// Settles the promise named by `input.token` from another workflow.
fn resolver_workflow(ctx: &WorkflowContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let id: PromiseId = input["token"]
            .as_str()
            .ok_or("token must be a string")?
            .parse()
            .map_err(|e: autumn_harvest::durable_promise::PromiseIdError| e.to_string())?;
        if input["reject"].as_bool() == Some(true) {
            ctx.reject_promise(&id, "rejected by a workflow").await
        } else {
            ctx.resolve_promise(&id, "resolved by a workflow").await
        }
        .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Tokens that `publish_token` received, in arrival order.
static PUBLISHED: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn publish_token_activity(_ctx: &ActivityContext, input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let token = input.as_str().ok_or("token must be a string")?.to_string();
        PUBLISHED.lock().expect("lock").push(token);
        Ok(Value::Null)
    })
}

fn wf(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: MODULE,
        handler,
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

fn act(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: MODULE,
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            wf("overlap_wf", overlap_workflow),
            wf("order_wf", order_workflow),
            wf("promise_wf", promise_workflow),
            wf("late_wait_promise_wf", late_wait_promise_workflow),
            wf("resolver_wf", resolver_workflow),
        ],
        vec![act("publish_token", publish_token_activity)],
    ))
}

/// Runs a worker for the duration of `body`.
async fn with_worker<F, Fut, T>(url: &str, body: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let worker = build_runtime_worker("small-primitives-worker", 4, 4, registry());
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    let out = body().await;
    worker.shutdown();
    let _ = handle.await;
    out
}

async fn start(url: &str, workflow_name: &str, workflow_id: &str) -> ExecutionId {
    start_full(url, workflow_name, workflow_id, None, Value::Null).await
}

async fn start_with_schedule(
    url: &str,
    workflow_name: &str,
    workflow_id: &str,
    schedule_id: Option<Uuid>,
) -> ExecutionId {
    start_full(url, workflow_name, workflow_id, schedule_id, Value::Null).await
}

async fn start_full(
    url: &str,
    workflow_name: &str,
    workflow_id: &str,
    schedule_id: Option<Uuid>,
    input: Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let mut conn = connect(url).await;
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
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
            context_headers: None,
            sla: None,
            schedule_id,
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

async fn send_signal(url: &str, exec_id: ExecutionId, name: &str, payload: Value) {
    let mut conn = connect(url).await;
    autumn_harvest::signal::send_signal(&mut conn, exec_id, name, payload)
        .await
        .expect("send signal");
}

/// Replays the stored history of `exec_id` with `handler`.
///
/// `replay_from_events` replays under a new execution id. A promise token
/// holds an execution id, so this also proves that replay reads the token
/// from history.
async fn assert_replays_clean(
    url: &str,
    exec_id: ExecutionId,
    name: &str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) {
    let mut conn = connect(url).await;
    let history = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    let report = WorkflowReplayer::new()
        .register_fn(name, handler)
        .replay_from_events(history.events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the recorded history of {exec_id} must replay clean:\n{report}"
    );
}

// ── Schedule helpers ─────────────────────────────────────────────────────

/// Inserts a due interval schedule with `max_active_runs = 1`.
async fn insert_schedule(url: &str, wf_name: &str, overlap_policy: &str, catchup: bool) -> Uuid {
    let lag = if catchup { 185 } else { 5 };
    insert_schedule_with(url, wf_name, overlap_policy, catchup, "interval:60", lag).await
}

/// Inserts a due schedule whose first slot is `lag_secs` in the past.
async fn insert_schedule_with(
    url: &str,
    wf_name: &str,
    overlap_policy: &str,
    catchup: bool,
    schedule_expr: &str,
    lag_secs: i64,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let mut conn = connect(url).await;
    let id = Uuid::new_v4();
    let lag = lag_secs;
    diesel::insert_into(dsl::harvest_schedules)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq(schedule_expr),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(catchup),
            dsl::max_active_runs.eq(1),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(lag)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq(overlap_policy),
            dsl::buffered_runs.eq(json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(&mut conn)
        .await
        .expect("insert schedule");
    id
}

/// Makes the schedule due again.
async fn make_due(url: &str, schedule_id: Uuid) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let mut conn = connect(url).await;
    diesel::update(dsl::harvest_schedules.find(schedule_id))
        .set(dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(5)))
        .execute(&mut conn)
        .await
        .expect("make schedule due");
}

async fn tick(url: &str) {
    tick_once(
        build_test_pool(url),
        registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");
}

#[derive(QueryableByName)]
struct ExecRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

async fn executions(url: &str, wf_name: &str) -> Vec<ExecRow> {
    let mut conn = connect(url).await;
    diesel::sql_query(
        "SELECT id, workflow_id, state FROM harvest_workflow_executions \
         WHERE workflow_name = $1 ORDER BY started_at",
    )
    .bind::<diesel::sql_types::Text, _>(wf_name)
    .load::<ExecRow>(&mut conn)
    .await
    .expect("load executions")
}

async fn buffered_count(url: &str, schedule_id: Uuid) -> i32 {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        n: i32,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(
        "SELECT jsonb_array_length(buffered_runs) AS n FROM harvest_schedules WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(schedule_id)
    .get_result::<Row>(&mut conn)
    .await
    .expect("buffered count")
    .n
}

// ── AllowAll ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_starts_a_run_while_another_is_running() {
    let (url, _c) = setup().await;
    let schedule_id = insert_schedule(&url, "overlap_wf", "allow_all", false).await;
    start_with_schedule(&url, "overlap_wf", "already-running", Some(schedule_id)).await;

    tick(&url).await;

    let runs = executions(&url, "overlap_wf").await;
    assert_eq!(runs.len(), 2, "AllowAll must start a second run");
    assert!(runs.iter().all(|r| r.state == "RUNNING"));
    assert_ne!(runs[0].workflow_id, runs[1].workflow_id);
    assert_eq!(buffered_count(&url, schedule_id).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_does_not_start_a_run_while_another_is_running() {
    let (url, _c) = setup().await;
    let schedule_id = insert_schedule(&url, "overlap_wf", "skip", false).await;
    start_with_schedule(&url, "overlap_wf", "already-running", Some(schedule_id)).await;

    tick(&url).await;

    assert_eq!(executions(&url, "overlap_wf").await.len(), 1);
}

/// Starts a manual trigger through the DAG trigger path.
async fn manual_trigger(url: &str, name: &str) -> autumn_harvest::HarvestResult<ExecutionId> {
    autumn_harvest::scheduler::trigger_unified_dag(
        build_test_pool(url),
        name,
        None,
        autumn_harvest::types::ShardId::new(0),
        "default",
        None,
        None,
        None,
        &registry(),
        StartSource::Api,
        None,
    )
    .await
    .map(|started| started.exec_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_manual_trigger_ignores_max_active_runs() {
    let (url, _c) = setup().await;
    let schedule_id = insert_schedule(&url, "overlap_wf", "allow_all", false).await;
    start_with_schedule(&url, "overlap_wf", "already-running", Some(schedule_id)).await;

    manual_trigger(&url, "overlap_wf")
        .await
        .expect("AllowAll must accept a manual trigger at the cap");
    assert_eq!(executions(&url, "overlap_wf").await.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_manual_trigger_is_rejected_at_max_active_runs() {
    let (url, _c) = setup().await;
    let schedule_id = insert_schedule(&url, "overlap_wf", "skip", false).await;
    start_with_schedule(&url, "overlap_wf", "already-running", Some(schedule_id)).await;

    assert!(manual_trigger(&url, "overlap_wf").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_fires_every_catchup_slot_in_one_tick() {
    let (url, _c) = setup().await;
    insert_schedule(&url, "overlap_wf", "allow_all", true).await;

    tick(&url).await;

    let runs = executions(&url, "overlap_wf").await;
    assert!(
        runs.len() >= 3,
        "AllowAll must dispatch each due catch-up slot, got {}",
        runs.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_defers_catch_up_slots_past_the_tick_limit() {
    let (url, _c) = setup().await;
    let limit = usize::try_from(autumn_harvest::scheduler::ALLOW_ALL_MAX_STARTS_PER_TICK)
        .expect("small limit");
    let lag = i64::try_from(limit).expect("small limit") + 50;
    insert_schedule_with(&url, "overlap_wf", "allow_all", true, "interval:1", lag).await;

    tick(&url).await;
    assert_eq!(
        executions(&url, "overlap_wf").await.len(),
        limit,
        "one tick must start at most the AllowAll limit"
    );

    tick(&url).await;
    assert!(
        executions(&url, "overlap_wf").await.len() > limit,
        "the next tick must resume the deferred catch-up slots"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_overlapping_runs_complete_and_replay_clean() {
    let (url, _c) = setup().await;
    let schedule_id = insert_schedule(&url, "overlap_wf", "allow_all", false).await;

    with_worker(&url, || async {
        tick(&url).await;
        make_due(&url, schedule_id).await;
        tick(&url).await;

        let runs = executions(&url, "overlap_wf").await;
        assert_eq!(runs.len(), 2, "two scheduled runs must overlap");
        assert!(runs.iter().all(|r| r.state == "RUNNING"));

        for run in &runs {
            let exec_id = ExecutionId::from_uuid(run.id);
            send_signal(&url, exec_id, "finish", json!({ "run": run.workflow_id })).await;
            wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
        }
        for run in &runs {
            let exec_id = ExecutionId::from_uuid(run.id);
            assert_replays_clean(&url, exec_id, "overlap_wf", overlap_workflow).await;
        }
    })
    .await;
}

// ── Payload-matching signal wait ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_matching_completes_only_on_a_matching_payload() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        let exec_id = start(&url, "order_wf", "order-1").await;
        send_signal(&url, exec_id, "order", json!({ "id": 41 })).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let execution = load_execution_from_url(&url, exec_id).await;
        assert_eq!(execution.state, "RUNNING", "order 41 must not complete it");

        send_signal(&url, exec_id, "order", json!({ "id": 42 })).await;
        let execution =
            wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
        assert_eq!(execution.output, Some(json!({ "id": 42 })));
        assert_replays_clean(&url, exec_id, "order_wf", order_workflow).await;
    })
    .await;
}

// ── Durable promise ──────────────────────────────────────────────────────

/// Waits until `publish_token` received the token for `exec_id`.
async fn published_token(exec_id: ExecutionId) -> PromiseId {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let found = PUBLISHED
            .lock()
            .expect("lock")
            .iter()
            .filter_map(|t| t.parse::<PromiseId>().ok())
            .find(|id| id.execution_id() == exec_id);
        if let Some(id) = found {
            return id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no token for {exec_id}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_settles_once_and_replays_clean() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        let exec_id = start(&url, "promise_wf", "promise-1").await;
        let id = published_token(exec_id).await;

        let mut conn = connect(&url).await;
        let first = durable_promise::resolve(&mut conn, &id, json!({ "approved": true }))
            .await
            .expect("first resolve");
        let second = durable_promise::resolve(&mut conn, &id, json!({ "approved": false }))
            .await
            .expect("second resolve");
        let late_reject = durable_promise::reject(&mut conn, &id, "too late")
            .await
            .expect("late reject");
        assert!(first, "the first settlement wins");
        assert!(!second, "a second resolve is a no-op");
        assert!(!late_reject, "a reject after a resolve is a no-op");

        let execution =
            wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
        assert_eq!(
            execution.output,
            Some(json!({ "resolved": { "approved": true } }))
        );
        assert_replays_clean(&url, exec_id, "promise_wf", promise_workflow).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_rejection_reaches_the_workflow() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        let exec_id = start(&url, "promise_wf", "promise-2").await;
        let id = published_token(exec_id).await;

        let mut conn = connect(&url).await;
        assert!(
            durable_promise::reject(&mut conn, &id, "budget exceeded")
                .await
                .expect("reject")
        );

        let execution =
            wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
        assert_eq!(
            execution.output,
            Some(json!({ "rejected": "budget exceeded" }))
        );
        assert_replays_clean(&url, exec_id, "promise_wf", promise_workflow).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_settled_before_the_wait_is_buffered() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        let exec_id = start(&url, "late_wait_promise_wf", "promise-3").await;
        let id = PromiseId::new(exec_id, "approval").expect("valid key");

        let mut conn = connect(&url).await;
        assert!(
            durable_promise::resolve(&mut conn, &id, json!("early"))
                .await
                .expect("resolve")
        );
        send_signal(&url, exec_id, "go", Value::Null).await;

        let execution =
            wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
        assert_eq!(execution.output, Some(json!({ "resolved": "early" })));
        assert_replays_clean(
            &url,
            exec_id,
            "late_wait_promise_wf",
            late_wait_promise_workflow,
        )
        .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_resolve_on_a_finished_run_is_an_error() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        let exec_id = start(&url, "promise_wf", "promise-4").await;
        let id = published_token(exec_id).await;
        let mut conn = connect(&url).await;
        assert!(
            durable_promise::resolve(&mut conn, &id, json!(1))
                .await
                .expect("resolve")
        );
        wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;

        let stale = PromiseId::new(exec_id, "never-created").expect("valid key");
        let err = durable_promise::resolve(&mut conn, &stale, json!(2)).await;
        assert!(err.is_err(), "a finished run cannot take a new settlement");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_settles_from_another_workflow() {
    let (url, _c) = setup().await;

    with_worker(&url, || async {
        for (n, reject, expected) in [
            (5, false, json!({ "resolved": "resolved by a workflow" })),
            (6, true, json!({ "rejected": "rejected by a workflow" })),
        ] {
            let exec_id = start(&url, "promise_wf", &format!("promise-{n}")).await;
            let id = published_token(exec_id).await;
            let input = json!({ "token": id.to_string(), "reject": reject });
            let resolver =
                start_full(&url, "resolver_wf", &format!("resolver-{n}"), None, input).await;

            wait_for_execution_state_with_timeout(&url, resolver, "COMPLETED", WAIT).await;
            let execution =
                wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", WAIT).await;
            assert_eq!(execution.output, Some(expected));
            assert_replays_clean(&url, exec_id, "promise_wf", promise_workflow).await;
            assert_replays_clean(&url, resolver, "resolver_wf", resolver_workflow).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_rules_hold_on_the_plain_signal_path() {
    use autumn_harvest::durable_promise::PromiseSettlement;
    use autumn_harvest::signal::send_signal_idempotent;

    let (url, _c) = setup().await;
    let exec_id = start(&url, "promise_wf", "promise-7").await;
    let id = PromiseId::new(exec_id, "approval").expect("valid key");
    let name = id.signal_name();
    let settlement = PromiseSettlement::resolved(json!(1)).to_value();
    let mut conn = connect(&url).await;

    let first = send_signal_idempotent(&mut conn, exec_id, &name, settlement.clone(), None)
        .await
        .expect("first settlement");
    let second = send_signal_idempotent(&mut conn, exec_id, &name, settlement.clone(), None)
        .await
        .expect("second settlement");
    assert!(first, "a keyless settlement gets the promise key");
    assert!(!second, "so a second keyless settlement is a no-op");

    let malformed = PromiseId::new(exec_id, "other").expect("valid key");
    assert!(
        send_signal_idempotent(
            &mut conn,
            exec_id,
            &malformed.signal_name(),
            json!("ops"),
            None
        )
        .await
        .is_err(),
        "a payload that is not a settlement is refused"
    );
    assert!(
        send_signal_idempotent(&mut conn, exec_id, &name, settlement, Some("my-key"))
            .await
            .is_err(),
        "a settlement with a different key is refused"
    );
    assert!(
        send_signal_idempotent(&mut conn, exec_id, "order", json!(1), Some(&name))
            .await
            .is_err(),
        "another signal cannot take the promise key"
    );
}

/// Signal-with-start parameters for `promise_wf` that carry `payload` on the
/// signal `signal_name`.
fn sws_params<'a>(
    workflow_id: &'a str,
    signal_name: &'a str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> autumn_harvest::execution::SignalWithStartParams<'a> {
    autumn_harvest::execution::SignalWithStartParams {
        workflow_name: "promise_wf",
        workflow_id,
        exec_id: ExecutionId::new(),
        input: Value::Null,
        parent_id: None,
        queue_name: "default",
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
        signal_name,
        signal_payload: payload,
        idempotency_key: idempotency_key.map(str::to_owned),
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
        workflow_info: None,
        start_source_override: None,
        start_source_ref_override: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_promise_rules_hold_on_signal_with_start() {
    use autumn_harvest::durable_promise::PromiseSettlement;
    use autumn_harvest::execution::signal_with_start_workflow_execution;

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    let name = "harvest.promise:approval";
    let settlement = PromiseSettlement::resolved(json!(1)).to_value();

    let first = signal_with_start_workflow_execution(
        &mut conn,
        sws_params("sws-1", name, settlement.clone(), None),
    )
    .await
    .expect("first settlement");
    let second = signal_with_start_workflow_execution(
        &mut conn,
        sws_params("sws-1", name, settlement.clone(), None),
    )
    .await
    .expect("second settlement");
    assert!(
        first.signal_delivered,
        "a keyless settlement gets the promise key"
    );
    assert!(!second.signal_delivered, "so a second one is a no-op");

    for (workflow_id, payload, key) in [
        ("sws-2", json!("ops"), None),
        ("sws-3", settlement.clone(), Some("my-key")),
    ] {
        let refused = signal_with_start_workflow_execution(
            &mut conn,
            sws_params(workflow_id, name, payload, key),
        )
        .await;
        assert!(refused.is_err(), "{workflow_id} must be refused");
        assert!(
            executions(&url, "promise_wf")
                .await
                .iter()
                .all(|r| r.workflow_id != workflow_id),
            "a refused signal-with-start must not start {workflow_id}"
        );
    }
}
