//! AC3 for issue #783: the `execution_id` an activity reads from `ctx.info()`
//! **is** the `{exec_id}` segment of the management API's workflow routes.
//!
//! The core-crate suite (`autumn-harvest/tests/integration/activity_info_tests.rs`)
//! can only prove the reported id round-trips through `FromStr` and selects the
//! right `harvest_workflow_executions` row — the HTTP route lives in this crate,
//! so the literal "`GET /api/harvest/workflows/{id}` → 200" clause has to be
//! asserted here.
//!
//! The proof is deliberately end-to-end and self-reporting: the activity itself
//! returns `ctx.info().execution_id.to_string()` as its output, a real worker
//! drives the run to `COMPLETED`, and the *activity-observed* string — never a
//! value the test knew up front — is spliced straight into the request URI. A
//! 404 control on a fresh id proves the 200 is attributable to that id rather
//! than to the route answering 200 for anything.
//!
//! Dual-mode DB: prefers `HARVEST_TEST_DATABASE_URL` (creates a fresh uniquely
//! named database on that server, migrates it) so the suite is runnable without
//! Docker; falls back to testcontainers Postgres 16 (the mode CI runs).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt as _;

/// Keeps the backing database alive for the test's duration.
#[allow(dead_code)]
enum DbGuard {
    // Boxed to keep the enum small (clippy::large_enum_variant).
    Container(Box<ContainerAsync<Postgres>>),
    LocalPg,
}

/// Replace the database name (final path segment) in a Postgres URL.
fn with_db(base: &str, db: &str) -> String {
    let (before, _after) = base.rsplit_once('/').expect("url has a database segment");
    format!("{before}/{db}")
}

async fn setup_database() -> (String, DbGuard) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_actinfo_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let url = with_db(&admin_url, &db_name);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect test db");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("run migrations");
        (url, DbGuard::LocalPg)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, DbGuard::Container(Box::new(container)))
    }
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Runs the correlating activity and propagates its output verbatim, so the
/// execution's recorded `output` is exactly what the *activity* saw.
fn wf_correlate(ctx: &autumn_harvest::context::WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("correlate_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// The AC3 subject: an activity reporting the owning run's id the way an
/// operator-facing log line would (`ctx.info().execution_id`), rendered with
/// `Display` — the exact form that has to be pasteable into a route.
fn correlate_activity(ctx: &autumn_harvest::ActivityContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(json!(ctx.info().execution_id.to_string())) })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "activity_info_api_integration",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

fn act_info(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_info_api_integration",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
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

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![wf_info("correlate_wf", wf_correlate)],
        vec![act_info("correlate_activity", correlate_activity)],
    ))
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("activity-info-api-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn start_params(exec_id: ExecutionId) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        workflow_name: "correlate_wf",
        workflow_id: "activity-info-correlation",
        exec_id,
        input: json!({"ok": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
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
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .expect("read body");
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// Polls the execution row until it is `COMPLETED` and returns its `output`.
async fn wait_for_output(pool: &DbPool, exec_id: ExecutionId) -> Value {
    for _ in 0..600 {
        let mut conn = pool.get().await.unwrap();
        let row: Option<(String, Option<Value>)> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
            .select((
                harvest_workflow_executions::state,
                harvest_workflow_executions::output,
            ))
            .first(&mut conn)
            .await
            .optional()
            .expect("load execution");
        if let Some((state, output)) = row {
            assert_ne!(state, "FAILED", "the correlating run must not fail");
            if state == "COMPLETED" {
                return output.expect("a COMPLETED run records its output");
            }
        }
        drop(conn);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workflow did not reach COMPLETED in time");
}

async fn run_worker_to_completion(pool: &DbPool, exec_id: ExecutionId) -> Value {
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "activity-info-api-worker".to_string();
    runtime_config.queues = vec!["default".to_string()];
    runtime_config.poll_interval = Duration::from_millis(20);
    runtime_config.shard_assignments = vec![ShardId::new(0)];
    let worker = Arc::new(Worker::new(runtime_config, test_registry()).unwrap());
    let handle = {
        let worker = worker.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    let output = wait_for_output(pool, exec_id).await;

    worker.shutdown();
    let _ = handle.await;
    output
}

/// AC3: the id an activity reads from `ctx.info()` opens the owning run through
/// the real management API with zero directory lookups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_reported_execution_id_opens_the_run_over_http() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    {
        let mut conn = pool.get().await.unwrap();
        start_or_load_workflow_execution(&mut conn, start_params(exec_id), None)
            .await
            .unwrap();
    }

    let output = run_worker_to_completion(&pool, exec_id).await;

    // The string the ACTIVITY observed — not a value this test knew up front.
    let reported = output
        .as_str()
        .expect("the activity returns its execution id as a JSON string")
        .to_string();

    // Rendered bare, so it is pasteable straight into the route.
    assert_eq!(
        reported,
        exec_id.as_uuid().to_string(),
        "info().execution_id must render as the bare uuid the route accepts"
    );

    // The literal AC3 clause: GET /workflows/{id} -> 200, using the reported id.
    let (status, body) = get_json(&app, &format!("/workflows/{reported}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the activity-reported id must open the run: body={body}"
    );
    assert_eq!(
        body["execution"]["id"].as_str(),
        Some(reported.as_str()),
        "the route must resolve to the very run the activity was part of"
    );

    // Control: the 200 is attributable to that id, not to the route answering
    // 200 for anything shaped like a uuid.
    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _) = get_json(&app, &format!("/workflows/{}", unknown.as_uuid())).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an id no activity ever reported must not resolve"
    );
}
