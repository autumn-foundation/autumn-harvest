use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dag::DagBuilder;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, DagInfo, WorkflowInfo};
use autumn_harvest::models::{HarvestSchedule, TaskQueueItem, WorkflowExecution};
use autumn_harvest::policy::Schedule;
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, compile_dag_catalog, register_schedules,
    register_workflow_schedules, tick_once, tick_once_sharded,
};
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_schedules, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, RetentionConfig, StartWorkflowParams, TimeoutType, WorkflowContext,
    WorkflowSchedule, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::{
    HarvestMode, HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::Barrier;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}
type HarvestApiApp = axum::Router;

#[derive(diesel::QueryableByName)]
struct CountByName {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct ExistsByName {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

async fn setup_sharded_test_database_urls() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 1 database");

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");

    for shard_url in [&shard0_url, &shard1_url] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(shard_url)
            .await
            .expect("failed to connect to shard database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("failed to apply harvest migrations to shard database");
    }

    ((shard0_url, shard1_url), container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        // Sized at 8 for comfortable headroom under the concurrent-request money
        // test. Since the Codex F4 fix, a single-shard backfill reuses the per-slot
        // exec conn for budget accounting and holds exactly one connection at a time
        // (proven pool-size-1 safe by
        // `backfill_single_shard_pool_size_one_does_not_deadlock`), so 8 is generous
        // rather than a floor.
        .max_size(8)
        .build()
        .expect("failed to build test pool")
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn test_app_state(pool: DbPool) -> AppState {
    AppState::for_test().with_pool(pool).with_profile("test")
}

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

fn build_test_worker(registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    build_test_worker_with_poll_interval(registry, Duration::from_millis(25))
}

fn build_test_worker_with_poll_interval(
    registry: Arc<HandlerRegistry>,
    poll_interval: Duration,
) -> Arc<Worker> {
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "test-worker".to_string();
    runtime_config.poll_interval = poll_interval;

    Arc::new(Worker::new(runtime_config, registry).expect("worker config should be valid"))
}

fn spawn_test_worker(worker: Arc<Worker>, pool: DbPool) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        worker.run(&pool).await;
    })
}

async fn shutdown_test_worker(worker: &Arc<Worker>, worker_task: tokio::task::JoinHandle<()>) {
    worker.shutdown();
    worker_task
        .await
        .expect("worker task should shut down cleanly");
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn get_response(app: &HarvestApiApp, uri: impl Into<String>) -> axum::response::Response {
    let uri = uri.into();
    app.clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed")
}

async fn post_json(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn patch_json(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("PATCH request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

fn approval_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name: "approval_workflow",
            module: "tests",
            handler: approval_workflow,
            execution_timeout: None,
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
        }],
        vec![],
    ))
}

fn approval_and_timer_signal_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                mcp: false,
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
                execution_timeout: None,
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
            },
            WorkflowInfo {
                mcp: false,
                name: "timer_then_signal_workflow",
                module: "tests",
                handler: timer_then_signal_workflow,
                execution_timeout: None,
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
            },
        ],
        vec![],
    ))
}

fn recording_activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler: record_activity,
    }
}

fn blocking_activity_info(name: &'static str, start_to_close: Duration) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: Some(start_to_close),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler: wait_on_barrier_activity,
    }
}

fn recording_registry(
    log: Arc<Mutex<Vec<String>>>,
    activity_names: &[&'static str],
) -> Arc<HandlerRegistry> {
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(log) as Box<dyn std::any::Any + Send + Sync>,
    );

    Arc::new(HandlerRegistry::with_state(
        vec![],
        activity_names
            .iter()
            .copied()
            .map(recording_activity_info)
            .collect(),
        Arc::new(state),
    ))
}

async fn register_test_schedules(database_url: &str, dag_catalog: &DagCatalog, reason: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect(reason);
    register_schedules(&mut conn, dag_catalog)
        .await
        .expect("failed to register dag schedules");
}

async fn insert_workflow_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: json!({ "workflow_id": workflow_id, "shard": shard.as_i32() }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
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
        },
        None,
    )
    .await
    .expect("workflow insert should succeed");
    exec_id
}

async fn mark_workflow_completed_on_url(
    database_url: &str,
    exec_id: ExecutionId,
    completed_at: chrono::DateTime<chrono::Utc>,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow completion update");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(Some(completed_at)),
        ))
        .execute(&mut conn)
        .await
        .expect("failed to mark workflow complete");
}

struct ChildWorkflowFixture<'a> {
    database_url: &'a str,
    shard: ShardId,
    parent_id: ExecutionId,
    workflow_name: &'a str,
    workflow_id: &'a str,
    state: &'a str,
    error: Option<&'a str>,
    started_offset_secs: i64,
}

async fn insert_child_workflow_on_url(fixture: ChildWorkflowFixture<'_>) -> ExecutionId {
    let ChildWorkflowFixture {
        database_url,
        shard,
        parent_id,
        workflow_name,
        workflow_id,
        state,
        error,
        started_offset_secs,
    } = fixture;
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for child workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: json!({ "workflow_id": workflow_id, "shard": shard.as_i32() }),
            parent_id: Some(parent_id.as_uuid()),
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
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
        },
        None,
    )
    .await
    .expect("child workflow insert should succeed");

    let started_at = chrono::Utc::now() - chrono::Duration::seconds(started_offset_secs);
    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(started_at + chrono::Duration::seconds(5))
    };
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::error.eq(error.map(ToOwned::to_owned)),
            harvest_workflow_executions::started_at.eq(started_at),
            harvest_workflow_executions::completed_at.eq(completed_at),
        ))
        .execute(&mut conn)
        .await
        .expect("failed to update child workflow fixture state");

    exec_id
}

async fn insert_dead_letter_on_url(
    database_url: &str,
    queue_name: &str,
    activity_name: &str,
) -> uuid::Uuid {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: queue_name.to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: None,
            activity_name: Some(activity_name.to_string()),
            input: json!({ "queue": queue_name }),
            error: format!("{activity_name} failed"),
            attempts: 3,

            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed")
}

fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

fn manual_pipeline_info_named(name: &'static str) -> DagInfo {
    DagInfo {
        name,
        module: "tests",
        schedule: Some(Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_manual_pipeline_dag,
        workflow_handler: None,
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

fn find_dag_name_for_shard(router: &ShardRouter, prefix: &str, target: ShardId) -> &'static str {
    for attempt in 0..4096 {
        let candidate = format!("{prefix}_{attempt}");
        if router.pick_for_dag(&candidate) == target {
            return leak_string(candidate);
        }
    }
    panic!("failed to find dag name mapping to shard {target}");
}

fn sharded_manual_dag_names(router: &ShardRouter) -> (&'static str, &'static str) {
    (
        find_dag_name_for_shard(router, "manual_zero", ShardId::new(0)),
        find_dag_name_for_shard(router, "manual_one", ShardId::new(1)),
    )
}

fn sharded_manual_dag_catalog(
    dag_on_zero: &'static str,
    dag_on_one: &'static str,
) -> Arc<DagCatalog> {
    Arc::new(
        compile_dag_catalog(vec![
            manual_pipeline_info_named(dag_on_zero),
            manual_pipeline_info_named(dag_on_one),
        ])
        .expect("sharded manual dags should compile"),
    )
}

async fn register_sharded_manual_dag_schedules(
    shard0_url: &str,
    shard1_url: &str,
    dag_on_zero: &'static str,
    dag_on_one: &'static str,
) {
    let shard0_catalog = compile_dag_catalog(vec![manual_pipeline_info_named(dag_on_zero)])
        .expect("shard 0 dag should compile");
    let shard1_catalog = compile_dag_catalog(vec![manual_pipeline_info_named(dag_on_one)])
        .expect("shard 1 dag should compile");
    register_test_schedules(
        shard0_url,
        &shard0_catalog,
        "failed to register shard 0 schedules",
    )
    .await;
    register_test_schedules(
        shard1_url,
        &shard1_catalog,
        "failed to register shard 1 schedules",
    )
    .await;
}

async fn seed_dag_run_on_url(database_url: &str, dag_name: &str) -> uuid::Uuid {
    use autumn_harvest::models::NewWorkflowExecution;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to shard for dag-run seed");
    let seeded_run_id = uuid::Uuid::new_v4();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&NewWorkflowExecution {
            continued_from_exec_id: None,
            first_exec_id: None,
            id: seeded_run_id,
            workflow_name: dag_name,
            workflow_id: &seeded_run_id.to_string(),
            run_id: uuid::Uuid::new_v4(),
            shard_id: 0,
            input: json!({ "seeded": true }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,

            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            completion_callbacks: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(&mut conn)
        .await
        .expect("failed to seed dag workflow execution");
    seeded_run_id
}

fn build_sharded_dag_api_app(
    shard0_url: &str,
    shard1_url: &str,
    dag_catalog: Arc<DagCatalog>,
    registry: Arc<HandlerRegistry>,
    router: ShardRouter,
) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(shard0_url, shard1_url));
    let registered_dag_names = dag_catalog.keys().cloned().collect::<Vec<_>>();
    api_state.install(
        HarvestApiRuntime::new(
            registry,
            dag_catalog,
            Arc::new(Vec::new()),
            Some("scheduler-sharded".to_string()),
            vec!["default".to_string()],
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
            router,
        )
        .with_registered_dag_names(registered_dag_names),
    );
    harvest_api_router(api_state).with_state(test_app_state_without_database())
}

async fn assert_sharded_dag_list_and_runs(
    app: &HarvestApiApp,
    dag_on_zero: &str,
    dag_on_one: &str,
    seeded_run_id: uuid::Uuid,
) {
    let (dags_status, dags_json) = get_json(app, "/dags").await;
    assert_eq!(dags_status, StatusCode::OK);
    let dags = dags_json
        .as_array()
        .expect("dags response must be an array");
    assert!(
        dags.iter().any(|dag| dag["name"] == dag_on_zero),
        "dag from shard 0 should be listed"
    );
    assert!(
        dags.iter().any(|dag| dag["name"] == dag_on_one),
        "dag from shard 1 should be listed"
    );

    let (runs_status, runs_json) = get_json(app, format!("/dags/{dag_on_one}/runs")).await;
    assert_eq!(runs_status, StatusCode::OK);
    assert!(
        runs_json
            .as_array()
            .expect("dag runs response must be an array")
            .iter()
            .any(|row| row["id"] == seeded_run_id.to_string()),
        "dag runs must come from the dag's owning shard"
    );
}

async fn assert_sharded_dag_patch_and_trigger(
    app: &HarvestApiApp,
    shard0_url: &str,
    shard1_url: &str,
    dag_on_zero: &str,
    dag_on_one: &str,
) {
    let (patch_status, patched_dag) = patch_json(
        app,
        format!("/dags/{dag_on_one}"),
        json!({ "paused": true }),
    )
    .await;
    assert_eq!(patch_status, StatusCode::OK);
    assert_eq!(patched_dag["dag_name"], dag_on_one);
    assert_eq!(patched_dag["is_paused"], true);
    assert!(
        load_schedule_from_url(shard1_url, dag_on_one)
            .await
            .is_paused,
        "pause updates must hit the dag's owning shard"
    );
    assert!(
        !load_schedule_from_url(shard0_url, dag_on_zero)
            .await
            .is_paused,
        "patching a shard 1 dag must not mutate shard 0 schedules"
    );

    let before_trigger_count = count_dag_runs_from_url(shard1_url, dag_on_one).await;
    let (paused_trigger_status, paused_trigger_json) = post_json(
        app,
        format!("/dags/{dag_on_one}/trigger"),
        json!({ "conf": { "manual": true } }),
    )
    .await;
    assert_eq!(paused_trigger_status, StatusCode::CONFLICT);
    assert!(
        paused_trigger_json.to_string().contains("paused"),
        "paused DAG triggers should be deferred: {paused_trigger_json}"
    );
    assert_eq!(
        count_dag_runs_from_url(shard1_url, dag_on_one).await,
        before_trigger_count,
        "paused trigger must not create a run on the dag's owning shard"
    );

    let mut shard1_conn = <AsyncPgConnection as AsyncConnection>::establish(shard1_url)
        .await
        .expect("failed to connect fresh Postgres client for dag run state update");
    diesel::update(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(dag_on_one)),
    )
    .set(harvest_workflow_executions::state.eq("COMPLETED"))
    .execute(&mut shard1_conn)
    .await
    .expect("failed to mark seeded dag runs completed");

    let (resume_status, resume_json) = patch_json(
        app,
        format!("/dags/{dag_on_one}"),
        json!({ "paused": false }),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume_json["is_paused"], false);

    let before_resumed_trigger_count = count_dag_runs_from_url(shard1_url, dag_on_one).await;
    let (trigger_status, _trigger_json) = post_json(
        app,
        format!("/dags/{dag_on_one}/trigger"),
        json!({ "conf": { "manual": true } }),
    )
    .await;
    assert_eq!(trigger_status, StatusCode::CREATED);
    assert_eq!(
        count_dag_runs_from_url(shard1_url, dag_on_one).await,
        before_resumed_trigger_count + 1,
        "triggered runs must be inserted on the dag's owning shard"
    );
    assert!(
        load_latest_dag_run_from_url(shard0_url, dag_on_one)
            .await
            .is_none(),
        "trigger must not create runs on the default shard for a shard 1 dag"
    );
}

async fn load_execution_from_url(database_url: &str, exec_id: &str) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for execution query");
    harvest_workflow_executions::table
        .find(
            exec_id
                .parse::<autumn_harvest::ExecutionId>()
                .expect("invalid execution id")
                .as_uuid(),
        )
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

async fn load_workflow_rows_from_url(
    database_url: &str,
    workflow_name: &str,
    workflow_id: &str,
) -> Vec<WorkflowExecution> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow lookup");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .order(harvest_workflow_executions::created_at.asc())
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("failed to load workflow rows by workflow key")
}

async fn count_workflow_executions_by_name_from_url(
    database_url: &str,
    workflow_name: &str,
) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow count");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count workflow rows by workflow name")
}

async fn load_latest_workflow_execution_by_name_from_url(
    database_url: &str,
    workflow_name: &str,
) -> Option<WorkflowExecution> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow lookup");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .order(harvest_workflow_executions::created_at.desc())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to load workflow rows by workflow name")
}

async fn count_workflow_tasks_from_url(database_url: &str, exec_id: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task lookup");
    let exec_id = exec_id
        .parse::<autumn_harvest::ExecutionId>()
        .expect("invalid execution id");
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count workflow tasks")
}

async fn load_task_from_url(database_url: &str, task_id: uuid::Uuid) -> TaskQueueItem {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for task query");
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("failed to load replayed task")
}

async fn load_activity_task_by_name_from_url_optional(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
) -> Option<TaskQueueItem> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for activity task query");
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::activity_name.eq(Some(activity_name)))
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to load optional activity task")
}

async fn wait_for_activity_task_state_from_url(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
    state: &str,
) -> TaskQueueItem {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last_state = None;
    loop {
        if let Some(task) =
            load_activity_task_by_name_from_url_optional(database_url, exec_id, activity_name).await
        {
            if task.state == state {
                return task;
            }
            last_state = Some(task.state);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "activity task {activity_name} did not reach {state}; last state: {last_state:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn seed_scheduled_activity_task_from_url(
    database_url: &str,
    workflow_name: &str,
    workflow_id: &str,
    activity_name: &str,
    activity_input: Value,
) -> (ExecutionId, ActivityExecId, uuid::Uuid) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let activity_id = ActivityExecId::new();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for scheduled activity seed");
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&autumn_harvest::models::NewWorkflowExecution {
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name,
            workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: 0,
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,

            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            completion_callbacks: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(&mut conn)
        .await
        .expect("failed to seed workflow execution");
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: activity_name.to_string(),
                input: activity_input.clone(),
                queue: "default".to_string(),
            },
        ],
        0,
    )
    .await
    .expect("failed to seed activity history");

    let mut params = autumn_harvest::queue::EnqueueParams::new(
        "default",
        autumn_harvest::queue::TaskType::Activity,
        activity_input,
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some(activity_name.to_string());
    params.activity_id = Some(activity_id.as_uuid());
    let task_id = autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("failed to seed activity task");
    (exec_id, activity_id, task_id)
}

async fn count_activity_scheduled_events_from_url(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
) -> usize {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for history query");
    store::load_history(&mut conn, exec_id)
        .await
        .expect("failed to load workflow history")
        .events
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                WorkflowEvent::ActivityScheduled { name, .. } if name == activity_name
            )
        })
        .count()
}

#[derive(Debug, Default)]
struct ActivityHistoryCounts {
    scheduled: usize,
    started: usize,
    completed: usize,
    failed: usize,
    timed_out: usize,
}

impl ActivityHistoryCounts {
    const fn terminal(&self) -> usize {
        self.completed + self.failed + self.timed_out
    }
}

async fn activity_history_counts_from_url(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
) -> ActivityHistoryCounts {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for history query");
    let events = store::load_history(&mut conn, exec_id)
        .await
        .expect("failed to load workflow history")
        .events;
    let activity_ids = events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == activity_name => Some(*activity_id),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();

    let mut counts = ActivityHistoryCounts {
        scheduled: activity_ids.len(),
        ..ActivityHistoryCounts::default()
    };
    for event in events {
        match event {
            WorkflowEvent::ActivityStarted { activity_id, .. }
                if activity_ids.contains(&activity_id) =>
            {
                counts.started += 1;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
                if activity_ids.contains(&activity_id) =>
            {
                counts.completed += 1;
            }
            WorkflowEvent::ActivityFailed { activity_id, .. }
                if activity_ids.contains(&activity_id) =>
            {
                counts.failed += 1;
            }
            WorkflowEvent::ActivityTimedOut { activity_id, .. }
                if activity_ids.contains(&activity_id) =>
            {
                counts.timed_out += 1;
            }
            _ => {}
        }
    }
    counts
}

async fn wait_for_activity_started_event_from_url(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let counts = activity_history_counts_from_url(database_url, exec_id, activity_name).await;
        if counts.started >= 1 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "activity {activity_name} did not record ActivityStarted"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn count_dead_letters_from_url(database_url: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for dead-letter count");
    harvest_dead_letters::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead letters")
}

async fn count_dag_runs_from_url(database_url: &str, dag_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for dag-run count");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dag workflow executions")
}

async fn load_schedule_from_url(database_url: &str, dag_name: &str) -> HarvestSchedule {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for schedule query");
    harvest_schedules::table
        .filter(harvest_schedules::dag_name.eq(dag_name))
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload harvest schedule")
}

async fn load_schedule_from_url_optional(
    database_url: &str,
    dag_name: &str,
) -> Option<HarvestSchedule> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for optional schedule query");
    harvest_schedules::table
        .filter(harvest_schedules::dag_name.eq(dag_name))
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to reload optional harvest schedule")
}

async fn load_workflow_only_schedule_from_url_optional(
    database_url: &str,
    workflow_name: &str,
) -> Option<HarvestSchedule> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for optional workflow schedule query");
    harvest_schedules::table
        .filter(harvest_schedules::workflow_name.eq(workflow_name))
        .filter(harvest_schedules::dag_name.is_null())
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to reload optional workflow-only harvest schedule")
}

async fn count_schedule_rows_for_name_from_url(database_url: &str, name: &str) -> usize {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for schedule row count");
    let rows: Vec<HarvestSchedule> = harvest_schedules::table
        .filter(
            harvest_schedules::dag_name
                .eq(name)
                .or(harvest_schedules::workflow_name.eq(name)),
        )
        .select(HarvestSchedule::as_select())
        .load(&mut conn)
        .await
        .expect("failed to count schedule rows by name");
    rows.len()
}

async fn load_latest_dag_run_from_url(
    database_url: &str,
    dag_name: &str,
) -> Option<WorkflowExecution> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for dag run query");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .order(harvest_workflow_executions::created_at.desc())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to reload latest dag workflow execution")
}

async fn wait_for_workflow_state(
    database_url: &str,
    exec_id: &str,
    expected_state: &str,
) -> WorkflowExecution {
    for _ in 0..900 {
        let execution = load_execution_from_url(database_url, exec_id).await;
        if execution.state == expected_state {
            return execution;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("workflow {exec_id} did not reach state {expected_state}");
}

async fn wait_for_dag_run_state(
    database_url: &str,
    dag_name: &str,
    expected_state: &str,
) -> WorkflowExecution {
    for _ in 0..900 {
        if let Some(run) = load_latest_dag_run_from_url(database_url, dag_name).await
            && run.state == expected_state
        {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("dag {dag_name} did not reach state {expected_state}");
}

async fn wait_for_workflow_terminal_state(database_url: &str, exec_id: &str) -> WorkflowExecution {
    for _ in 0..900 {
        let execution = load_execution_from_url(database_url, exec_id).await;
        if matches!(
            execution.state.as_str(),
            "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "TERMINATED"
        ) {
            return execution;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("workflow {exec_id} did not reach a terminal state");
}

async fn insert_retention_fixture_execution(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    workflow_id: &str,
    state: &str,
    completed_at_expr: &str,
) {
    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions (
            id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, completed_at, created_at
        ) VALUES (
            $1, 'retention_fixture', '{workflow_id}', gen_random_uuid(), 0, '{state}', '{{}}'::jsonb, 'default',
            NOW() - INTERVAL '11 days', {completed_at_expr}, NOW() - INTERVAL '11 days'
        )"
    ))
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture workflow execution");

    if state == "RUNNING" {
        return;
    }

    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp)
         VALUES ($1, 0, 'WorkflowCompleted', '{}'::jsonb, NOW() - INTERVAL '10 days')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture event");

    diesel::sql_query(
        "INSERT INTO harvest_task_queue (
            id, queue_name, task_type, workflow_exec_id, input, state, priority, max_attempts, scheduled_at
         ) VALUES (
            gen_random_uuid(), 'default', 'workflow', $1, '{}'::jsonb, 'COMPLETED', 0, 1, NOW() - INTERVAL '10 days'
         )",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture task");

    diesel::sql_query(
        "INSERT INTO harvest_timers (workflow_exec_id, timer_id, fires_at, fired)
         VALUES ($1, 'fixture-timer', NOW() - INTERVAL '10 days', TRUE)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture timer");

    diesel::sql_query(
        "INSERT INTO harvest_signals (workflow_exec_id, signal_name, payload, consumed)
         VALUES ($1, 'fixture-signal', '{}'::jsonb, TRUE)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture signal");

    diesel::sql_query(
        "INSERT INTO harvest_dead_letters (
            id, original_task_id, queue_name, task_type, workflow_exec_id, input, error, attempts, failed_at
         ) VALUES (
            gen_random_uuid(), gen_random_uuid(), 'default', 'workflow', $1, '{}'::jsonb, 'fixture', 1, NOW() - INTERVAL '10 days'
         )",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture dead letter");
}

async fn seed_retention_fixtures(
    conn: &mut AsyncPgConnection,
    old_exec_a: uuid::Uuid,
    old_exec_b: uuid::Uuid,
    recent_exec: uuid::Uuid,
    inflight_exec: uuid::Uuid,
) {
    for (exec_id, workflow_id, state, completed_at_expr) in [
        (
            old_exec_a,
            "retention-old-a",
            "COMPLETED",
            "NOW() - INTERVAL '10 days'",
        ),
        (
            old_exec_b,
            "retention-old-b",
            "FAILED",
            "NOW() - INTERVAL '9 days'",
        ),
        (
            recent_exec,
            "retention-recent",
            "COMPLETED",
            "NOW() - INTERVAL '2 days'",
        ),
        (inflight_exec, "retention-inflight", "RUNNING", "NULL"),
    ] {
        insert_retention_fixture_execution(conn, exec_id, workflow_id, state, completed_at_expr)
            .await;
    }

    // harvest_dag_runs was dropped in Step 5 of issue #256; DAG runs are now
    // workflow executions. The retention fixture for old_exec_b is already
    // covered by the insert_retention_fixture_execution call above.
}

async fn trigger_retention_and_wait(app: &HarvestApiApp) {
    let (run_now_status, run_now_json) =
        post_json(app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    for _ in 0..40 {
        let (_status, status_json) = get_json(app, "/admin/retention").await;
        let deleted_total: u64 = status_json["per_shard"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|tick| tick["deleted_count"].as_u64())
            .sum();
        if deleted_total >= 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn count_execution_rows(conn: &mut AsyncPgConnection, exec_id: uuid::Uuid) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .get_result::<CountByName>(conn)
        .await
        .expect("count query should succeed")
        .count
}

async fn count_child_rows(conn: &mut AsyncPgConnection, table: &str, exec_id: uuid::Uuid) -> i64 {
    diesel::sql_query(format!(
        "SELECT COUNT(*) AS count FROM {table} WHERE workflow_exec_id = $1"
    ))
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result::<CountByName>(conn)
    .await
    .expect("child count query should succeed")
    .count
}

async fn assert_retention_cleanup_state(
    conn: &mut AsyncPgConnection,
    old_exec_a: uuid::Uuid,
    old_exec_b: uuid::Uuid,
    recent_exec: uuid::Uuid,
    inflight_exec: uuid::Uuid,
) {
    assert_eq!(count_execution_rows(conn, old_exec_a).await, 0);
    assert_eq!(count_execution_rows(conn, old_exec_b).await, 0);
    assert_eq!(count_execution_rows(conn, recent_exec).await, 1);
    assert_eq!(count_execution_rows(conn, inflight_exec).await, 1);

    for table in [
        "harvest_events",
        "harvest_task_queue",
        "harvest_timers",
        "harvest_signals",
        "harvest_dead_letters",
    ] {
        for exec_id in [old_exec_a, old_exec_b] {
            let count = count_child_rows(conn, table, exec_id).await;
            assert_eq!(count, 0, "cascade should clear {table} for {exec_id}");
        }
    }
}

fn approval_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let request_id = input.get("request_id").cloned().unwrap_or(Value::Null);
        ctx.register_query("status", {
            let request_id = request_id.clone();
            move || {
                json!({
                    "phase": "waiting",
                    "request_id": request_id,
                })
            }
        });

        let approval = ctx
            .wait_for_signal("approved")
            .await
            .map_err(|error| error.to_string())?;

        Ok(json!({
            "phase": "approved",
            "approval": approval,
        }))
    })
}

fn timer_then_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("cooldown", 3)
            .await
            .map_err(|error| error.to_string())?;
        let approval = ctx
            .wait_for_signal("approved")
            .await
            .map_err(|error| error.to_string())?;

        Ok(json!({
            "timer": "fired",
            "approval": approval,
        }))
    })
}

fn manual_pipeline_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        for step in ["extract", "transform", "notify"] {
            ctx.execute_activity_raw(step, dag_activity_input(&input, step), "default")
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(Value::Null)
    })
}

fn interval_pipeline_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw(
            "interval_step",
            dag_activity_input(&input, "interval_step"),
            "default",
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn parallel_activities_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first =
            ctx.execute_activity_raw("parallel_a", json!({ "dag_task": "parallel_a" }), "default");
        let second =
            ctx.execute_activity_raw("parallel_b", json!({ "dag_task": "parallel_b" }), "default");
        let (first, second) = tokio::join!(first, second);
        first.map_err(|error| error.to_string())?;
        second.map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn staggered_parallel_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let fast = ctx.execute_activity_raw(
            "parallel_fast",
            json!({ "dag_task": "parallel_fast", "delay_ms": 0 }),
            "default",
        );
        let slow = ctx.execute_activity_raw(
            "parallel_slow",
            json!({ "dag_task": "parallel_slow", "delay_ms": 450 }),
            "default",
        );
        let (fast, slow) = tokio::join!(fast, slow);
        fast.map_err(|error| error.to_string())?;
        slow.map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn parallel_same_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let left = ctx.execute_activity_raw(
            "shared_parallel",
            json!({ "dag_task": "left", "delay_ms": 75 }),
            "default",
        );
        let right = ctx.execute_activity_raw(
            "shared_parallel",
            json!({ "dag_task": "right", "delay_ms": 75 }),
            "default",
        );
        let (left, right) = tokio::join!(left, right);
        left.map_err(|error| error.to_string())?;
        right.map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn barrier_parallel_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first = ctx.execute_activity_raw(
            "barrier_first",
            json!({ "dag_task": "barrier_first", "barrier": true }),
            "default",
        );
        let second = ctx.execute_activity_raw(
            "barrier_second",
            json!({ "dag_task": "barrier_second", "barrier": true }),
            "default",
        );
        let (first, second) = tokio::join!(first, second);
        first.map_err(|error| error.to_string())?;
        second.map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn timeout_completion_race_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw(
            "timeout_completion_race",
            json!({ "dag_task": "timeout_completion_race" }),
            "default",
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })
}

fn wait_on_barrier_activity<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    let barrier = Arc::clone(
        ctx.state::<Arc<Barrier>>()
            .expect("barrier state must be registered"),
    );
    Box::pin(async move {
        barrier.wait().await;
        Ok(input)
    })
}

fn dag_activity_input(conf: &Value, task: &str) -> Value {
    match conf {
        Value::Object(object) => {
            let mut object = object.clone();
            object.insert("dag_task".to_string(), Value::String(task.to_string()));
            Value::Object(object)
        }
        other => json!({
            "conf": other,
            "dag_task": task,
        }),
    }
}

fn record_activity<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if input
            .get("barrier")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && let Some(barrier) = ctx.state::<Arc<Barrier>>()
        {
            barrier.wait().await;
        }

        let entries = ctx
            .state::<Arc<Mutex<Vec<String>>>>()
            .expect("shared log state must be registered");
        let step = input
            .get("dag_task")
            .or_else(|| input.get("step"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        entries
            .lock()
            .expect("log mutex poisoned")
            .push(step.clone());

        if let Some(delay_ms) = input.get("delay_ms").and_then(Value::as_u64) {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        Ok(json!({ "step": step }))
    })
}

fn build_manual_pipeline_dag(dag: &mut DagBuilder) {
    const fn extract() {}
    const fn transform() {}
    const fn notify() {}

    let extract = dag.activity(extract);
    let transform = dag.activity(transform).upstream(&extract);
    let _notify = dag.activity(notify).upstream(&transform);
}

fn build_interval_pipeline_dag(dag: &mut DagBuilder) {
    const fn interval_step() {}

    let _step = dag.activity(interval_step);
}

fn manual_pipeline_info() -> DagInfo {
    DagInfo {
        name: "manual_pipeline",
        module: "tests",
        schedule: Some(Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_manual_pipeline_dag,
        workflow_handler: Some(manual_pipeline_workflow),
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

fn interval_pipeline_info() -> DagInfo {
    DagInfo {
        name: "interval_pipeline",
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(1))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(interval_pipeline_workflow),
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

fn classic_interval_pipeline_info() -> DagInfo {
    DagInfo {
        name: "interval_pipeline",
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(1))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_interval_pipeline_dag,
        workflow_handler: None,
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

fn workflow_info_named(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler: approval_workflow,
        execution_timeout: None,
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

fn unified_manual_dag_info_named(name: &'static str, default_queue: &'static str) -> DagInfo {
    DagInfo {
        name,
        module: "tests",
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: Some(default_queue),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

fn manual_interval_pipeline_info() -> DagInfo {
    DagInfo {
        name: "interval_pipeline",
        module: "tests",
        schedule: Some(Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_interval_pipeline_dag,
        workflow_handler: None,
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }
}

#[tokio::test]
async fn harvest_api_uses_installed_storage_pool_when_app_state_has_no_database() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let app = harvest_api_router(api_state.clone()).with_state(test_app_state_without_database());

    let (start_status, start_json) = post_json(
        &app,
        "/workflows/approval_workflow/start",
        json!({
            "workflow_id": "approval-42",
            "input": { "request_id": "42" },
        }),
    )
    .await;
    assert_eq!(start_status, StatusCode::CREATED);
    let exec_id = start_json["execution_id"]
        .as_str()
        .expect("execution_id must be a string")
        .to_string();

    let (list_status, listed) = get_json(&app, "/workflows").await;
    assert_eq!(list_status, StatusCode::OK);
    assert!(
        listed
            .as_array()
            .expect("workflow list must be an array")
            .iter()
            .any(|row| row["id"] == exec_id),
        "started workflow should be listed"
    );

    let (query_status, query_json) =
        get_json(&app, format!("/workflows/{exec_id}/query/status")).await;
    assert_eq!(query_status, StatusCode::OK);
    assert_eq!(query_json["phase"], "waiting");
    assert_eq!(query_json["request_id"], "42");

    let (signal_status, _signal_json) = post_json(
        &app,
        format!("/workflows/{exec_id}/signal/approved"),
        json!({ "approved": true }),
    )
    .await;
    assert_eq!(signal_status, StatusCode::ACCEPTED);

    let execution = wait_for_workflow_state(&database_url, &exec_id, "COMPLETED").await;
    assert_eq!(execution.workflow_name, "approval_workflow");

    let (details_status, details_json) = get_json(&app, format!("/workflows/{exec_id}")).await;
    assert_eq!(details_status, StatusCode::OK);
    let history = details_json["history"]
        .as_array()
        .expect("workflow history must be an array");
    assert!(
        history
            .iter()
            .any(|event| event["type"] == "SignalReceived"),
        "history should include the delivered signal"
    );
    assert!(
        history
            .iter()
            .any(|event| event["type"] == "WorkflowCompleted"),
        "history should include workflow completion"
    );

    shutdown_test_worker(&worker, worker_task).await;
}

#[tokio::test]
async fn harvest_api_workflow_details_include_parent_id_at_top_level() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "parent-details",
    )
    .await;
    let child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "child-details",
        state: "RUNNING",
        error: None,
        started_offset_secs: 0,
    })
    .await;

    let (status, details_json) = get_json(&app, format!("/workflows/{child}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(details_json["parent_id"], parent.to_string());
}

#[tokio::test]
async fn harvest_api_lists_direct_workflow_children_with_filters() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "parent-children",
    )
    .await;
    let failed_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "failed-child",
        state: "FAILED",
        error: Some("charge card failed\nstack trace omitted"),
        started_offset_secs: 10,
    })
    .await;
    insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "running-child",
        state: "RUNNING",
        error: None,
        started_offset_secs: 20,
    })
    .await;
    insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "email_child",
        workflow_id: "failed-other-name",
        state: "FAILED",
        error: Some("smtp failed"),
        started_offset_secs: 30,
    })
    .await;

    let response = get_response(
        &app,
        format!("/workflows/{parent}/children?status=Failed&workflow_name=billing_child"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = read_json_response(response).await;
    let items = body["items"]
        .as_array()
        .expect("children response must have an items array");

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["exec_id"], failed_child.to_string());
    assert_eq!(items[0]["workflow_name"], "billing_child");
    assert_eq!(items[0]["status"], "Failed");
    assert_eq!(items[0]["error_summary"], "charge card failed");
    assert_eq!(items[0]["shard_id"], 0);
    assert_eq!(items[0]["depth"], 0);
    assert!(items[0]["started_at"].is_string());
    assert!(items[0]["completed_at"].is_string());
    assert!(body["next_cursor"].is_null());
}

#[tokio::test]
async fn harvest_api_filters_workflow_children_by_continued_as_new_status() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "parent-continued-as-new",
    )
    .await;
    let continued_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "continued-child",
        state: "CONTINUED_AS_NEW",
        error: None,
        started_offset_secs: 10,
    })
    .await;
    insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "failed-child",
        state: "FAILED",
        error: Some("not the requested state"),
        started_offset_secs: 5,
    })
    .await;

    let (status, body) = get_json(
        &app,
        format!("/workflows/{parent}/children?status=ContinuedAsNew"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"]
        .as_array()
        .expect("children response must have an items array");

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["exec_id"], continued_child.to_string());
    assert_eq!(items[0]["status"], "ContinuedAsNew");
}

#[tokio::test]
async fn load_workflow_children_applies_limit_and_cursor_before_returning_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "store-page-parent",
    )
    .await;
    let newest_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "store-page-newest",
        state: "FAILED",
        error: None,
        started_offset_secs: 5,
    })
    .await;
    let middle_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "store-page-middle",
        state: "FAILED",
        error: None,
        started_offset_secs: 10,
    })
    .await;
    let oldest_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "store-page-oldest",
        state: "FAILED",
        error: None,
        started_offset_secs: 15,
    })
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for store page query");
    let first_page = store::load_workflow_children(
        &mut conn,
        parent,
        &store::WorkflowChildFilters {
            statuses: Vec::new(),
            workflow_name: None,
            cursor: None,
            limit: Some(2),
        },
        0,
    )
    .await
    .expect("first child page should load");
    assert_eq!(
        first_page.iter().map(|row| row.exec_id).collect::<Vec<_>>(),
        vec![newest_child, middle_child]
    );

    let cursor_row = first_page
        .last()
        .expect("first page should include a cursor row");
    let second_page = store::load_workflow_children(
        &mut conn,
        parent,
        &store::WorkflowChildFilters {
            statuses: Vec::new(),
            workflow_name: None,
            cursor: Some(store::WorkflowChildCursor {
                started_at: cursor_row.started_at,
                exec_id: cursor_row.exec_id.as_uuid(),
            }),
            limit: Some(2),
        },
        0,
    )
    .await
    .expect("second child page should load");

    assert_eq!(
        second_page
            .iter()
            .map(|row| row.exec_id)
            .collect::<Vec<_>>(),
        vec![oldest_child]
    );
}

#[tokio::test]
async fn harvest_api_lists_workflow_children_across_shards_and_paginates() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let mut shard0_conn = <AsyncPgConnection as AsyncConnection>::establish(&shard0_url)
        .await
        .expect("failed to connect to shard 0");
    let index_exists: ExistsByName = diesel::sql_query(
        "SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = 'idx_harvest_we_parent_id'
         ) AS exists",
    )
    .get_result(&mut shard0_conn)
    .await
    .expect("failed to inspect parent_id index");
    assert!(
        index_exists.exists,
        "parent_id lookup must have a per-shard index"
    );

    let parent = insert_workflow_on_url(
        &shard0_url,
        ShardId::new(0),
        "fanout_parent",
        "parent-cross-shard",
    )
    .await;
    let newest_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &shard1_url,
        shard: ShardId::new(1),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "newest-cross-shard-child",
        state: "FAILED",
        error: Some("newest failed"),
        started_offset_secs: 5,
    })
    .await;
    let older_child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &shard0_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "billing_child",
        workflow_id: "older-cross-shard-child",
        state: "FAILED",
        error: Some("older failed"),
        started_offset_secs: 20,
    })
    .await;

    let (first_status, first_page) =
        get_json(&app, format!("/workflows/{parent}/children?limit=1")).await;
    assert_eq!(first_status, StatusCode::OK);
    let first_items = first_page["items"]
        .as_array()
        .expect("children response must have an items array");
    assert_eq!(first_items.len(), 1);
    assert_eq!(first_items[0]["exec_id"], newest_child.to_string());
    assert_eq!(first_items[0]["shard_id"], 1);
    let cursor = first_page["next_cursor"]
        .as_str()
        .expect("limited page should return a cursor");
    let encoded_cursor = cursor.replace('|', "%7C");

    let (second_status, second_page) = get_json(
        &app,
        format!("/workflows/{parent}/children?limit=1&cursor={encoded_cursor}"),
    )
    .await;
    assert_eq!(second_status, StatusCode::OK);
    let second_items = second_page["items"]
        .as_array()
        .expect("children response must have an items array");
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["exec_id"], older_child.to_string());
    assert_eq!(second_items[0]["shard_id"], 0);
    assert!(second_page["next_cursor"].is_null());
}

#[tokio::test]
async fn harvest_api_recursive_children_traverse_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &shard0_url,
        ShardId::new(0),
        "fanout_parent",
        "cross-shard-recursive-parent",
    )
    .await;
    let child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &shard1_url,
        shard: ShardId::new(1),
        parent_id: parent,
        workflow_name: "middle_child",
        workflow_id: "cross-shard-recursive-child",
        state: "RUNNING",
        error: None,
        started_offset_secs: 10,
    })
    .await;
    let grandchild = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &shard0_url,
        shard: ShardId::new(0),
        parent_id: child,
        workflow_name: "leaf_child",
        workflow_id: "cross-shard-recursive-grandchild",
        state: "FAILED",
        error: Some("leaf failed across shards"),
        started_offset_secs: 5,
    })
    .await;

    let (status, body) = get_json(&app, format!("/workflows/{parent}/children?depth=1")).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"]
        .as_array()
        .expect("children response must have an items array");

    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|row| row["exec_id"] == child.to_string()
        && row["depth"] == 0
        && row["shard_id"] == 1));
    assert!(
        items
            .iter()
            .any(|row| row["exec_id"] == grandchild.to_string()
                && row["depth"] == 1
                && row["shard_id"] == 0)
    );
}

#[tokio::test]
async fn harvest_api_children_distinguishes_empty_parent_from_missing_parent() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "parent-without-children",
    )
    .await;

    let (empty_status, empty_body) = get_json(&app, format!("/workflows/{parent}/children")).await;
    assert_eq!(empty_status, StatusCode::OK);
    assert_eq!(
        empty_body["items"]
            .as_array()
            .expect("children response must have an items array")
            .len(),
        0
    );
    assert!(empty_body["next_cursor"].is_null());

    let missing_parent = ExecutionId::new_for_shard(ShardId::new(0));
    let missing_response =
        get_response(&app, format!("/workflows/{missing_parent}/children")).await;
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn harvest_api_children_supports_recursive_depth_with_cap() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let parent = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "fanout_parent",
        "recursive-parent",
    )
    .await;
    let child = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: parent,
        workflow_name: "middle_child",
        workflow_id: "recursive-child",
        state: "RUNNING",
        error: None,
        started_offset_secs: 10,
    })
    .await;
    let grandchild = insert_child_workflow_on_url(ChildWorkflowFixture {
        database_url: &database_url,
        shard: ShardId::new(0),
        parent_id: child,
        workflow_name: "leaf_child",
        workflow_id: "recursive-grandchild",
        state: "FAILED",
        error: Some("leaf failed"),
        started_offset_secs: 5,
    })
    .await;

    let (status, body) = get_json(&app, format!("/workflows/{parent}/children?depth=1")).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"]
        .as_array()
        .expect("children response must have an items array");
    assert_eq!(items.len(), 2);
    assert!(items.iter().any(|row| row["exec_id"] == child.to_string()
        && row["depth"] == 0
        && row["workflow_name"] == "middle_child"));
    assert!(
        items
            .iter()
            .any(|row| row["exec_id"] == grandchild.to_string()
                && row["depth"] == 1
                && row["workflow_name"] == "leaf_child")
    );

    let (filtered_status, filtered_body) = get_json(
        &app,
        format!("/workflows/{parent}/children?depth=1&status=Failed"),
    )
    .await;
    assert_eq!(filtered_status, StatusCode::OK);
    let filtered_items = filtered_body["items"]
        .as_array()
        .expect("filtered children response must have an items array");
    assert_eq!(filtered_items.len(), 1);
    assert_eq!(filtered_items[0]["exec_id"], grandchild.to_string());
    assert_eq!(filtered_items[0]["depth"], 1);

    let too_deep = get_response(&app, format!("/workflows/{parent}/children?depth=6")).await;
    assert_eq!(too_deep.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn harvest_api_duplicate_start_reuses_existing_execution() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let payload = json!({
        "workflow_id": "approval-duplicate",
        "input": { "request_id": "dup-1" },
    });

    let (first_status, first_json) =
        post_json(&app, "/workflows/approval_workflow/start", payload.clone()).await;
    assert_eq!(first_status, StatusCode::CREATED);
    let first_exec_id = first_json["execution_id"]
        .as_str()
        .expect("first execution_id must be a string")
        .to_owned();

    let (second_status, second_json) =
        post_json(&app, "/workflows/approval_workflow/start", payload).await;
    assert_eq!(
        second_status,
        StatusCode::OK,
        "duplicate start should reuse the existing execution"
    );
    assert_eq!(
        second_json["execution_id"].as_str(),
        Some(first_exec_id.as_str()),
        "duplicate start should return the original execution id"
    );

    let executions =
        load_workflow_rows_from_url(&database_url, "approval_workflow", "approval-duplicate").await;
    assert_eq!(
        executions.len(),
        1,
        "duplicate start should not create a second workflow row"
    );
    let tasks = count_workflow_tasks_from_url(&database_url, &first_exec_id).await;
    assert_eq!(
        tasks, 1,
        "duplicate start should not enqueue duplicate workflow tasks"
    );
}

#[tokio::test]
async fn harvest_api_stack_endpoint_returns_shape() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let (start_status, start_json) = post_json(
        &app,
        "/workflows/approval_workflow/start",
        json!({"workflow_id":"stack-shape","input":{"request_id":"stack"}}),
    )
    .await;
    assert_eq!(start_status, StatusCode::CREATED);
    let exec_id = start_json["execution_id"].as_str().unwrap();

    let (status, payload) = get_json(&app, format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["exec_id"], exec_id);
    assert_eq!(payload["workflow_name"], "approval_workflow");
    assert!(payload["pending_activities"].is_array());
    assert!(payload["pending_local_activities"].is_array());
    assert!(payload["pending_timers"].is_array());
    assert!(payload["pending_signals"].is_array());
    assert!(payload["buffered_signals"].is_array());
    assert!(payload["pending_child_workflows"].is_array());
    assert!(payload["last_event_id"].is_number());
}

#[tokio::test]
async fn harvest_api_stack_endpoint_surfaces_rate_limit_throttling() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "approval_workflow",
        "stack-throttling",
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for test setup");

    // Insert a saturated rate limit bucket
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at) \
         VALUES ('test_rate_limit_bucket', 0.0, 10.0, 0.0, NOW(), NOW(), NOW())"
    )
    .execute(&mut conn)
    .await
    .expect("failed to insert rate limit bucket");

    // Insert a pending activity task with the rate limit key
    diesel::sql_query(
        "INSERT INTO harvest_task_queue ( \
            id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, attempt, max_attempts, scheduled_at, rate_limit_key \
         ) VALUES ( \
            gen_random_uuid(), 'default', 'activity', $1, 'some_activity', '{}'::jsonb, 'PENDING', 0, 0, 5, NOW(), 'test_rate_limit_bucket' \
         )"
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("failed to insert rate limited task");

    let (status, payload) = get_json(&app, format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["exec_id"], exec_id.to_string());

    let pending = payload["pending_activities"]
        .as_array()
        .expect("pending_activities must be an array");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0]["task_status"],
        "waiting on rate_limit_key=test_rate_limit_bucket"
    );
    // An activity that has never heartbeated reports a null checkpoint (#503).
    assert!(
        pending[0]["heartbeat_details"].is_null(),
        "heartbeat_details must be null when no checkpoint flushed"
    );
    assert_eq!(pending[0]["heartbeat_details_truncated"], false);
    assert!(pending[0]["heartbeat_details_bytes"].is_null());
}

#[tokio::test]
async fn harvest_api_stack_endpoint_surfaces_heartbeat_checkpoint() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "approval_workflow",
        "stack-hb",
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for test setup");

    let checkpoint = serde_json::json!({"processed": 4500, "total": 10000});
    diesel::sql_query(
        "INSERT INTO harvest_task_queue ( \
            id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, attempt, max_attempts, scheduled_at, started_at, last_heartbeat_at, heartbeat_details \
         ) VALUES ( \
            gen_random_uuid(), 'default', 'activity', $1, 'pipeline', '{}'::jsonb, 'RUNNING', 0, 0, 5, NOW(), NOW(), NOW(), $2 \
         )",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(checkpoint.clone())
    .execute(&mut conn)
    .await
    .expect("failed to insert heartbeating task");

    let (status, payload) = get_json(&app, format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK);

    let pending = payload["pending_activities"]
        .as_array()
        .expect("pending_activities must be an array");
    assert_eq!(pending.len(), 1);
    // The latest checkpoint payload is surfaced verbatim (#503).
    assert_eq!(pending[0]["heartbeat_details"], checkpoint);
    assert_eq!(pending[0]["heartbeat_details_truncated"], false);
    assert!(
        pending[0]["heartbeat_details_bytes"].as_u64().unwrap() > 0,
        "byte size must be reported alongside the payload"
    );
    assert!(
        pending[0]["last_heartbeat_at"].is_string(),
        "last_heartbeat_at remains present"
    );
    // A small, in-budget checkpoint is not budget-omitted (#503 review).
    assert_eq!(pending[0]["heartbeat_details_omitted_for_budget"], false);
    assert_eq!(payload["checkpoints_truncated_for_budget"], false);
}

#[tokio::test]
async fn harvest_api_stack_endpoint_truncates_oversized_heartbeat_checkpoint() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "approval_workflow",
        "stack-hb-big",
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for test setup");

    // Default activity-result cap is 2 MiB; a 2.1 MiB blob must trip the guard.
    let big_blob = "x".repeat(2 * 1024 * 1024 + 100 * 1024);
    let checkpoint = serde_json::json!({ "blob": big_blob });
    diesel::sql_query(
        "INSERT INTO harvest_task_queue ( \
            id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, attempt, max_attempts, scheduled_at, started_at, last_heartbeat_at, heartbeat_details \
         ) VALUES ( \
            gen_random_uuid(), 'default', 'activity', $1, 'pipeline', '{}'::jsonb, 'RUNNING', 0, 0, 5, NOW(), NOW(), NOW(), $2 \
         )",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Jsonb, _>(checkpoint)
    .execute(&mut conn)
    .await
    .expect("failed to insert oversized heartbeating task");

    let (status, payload) = get_json(&app, format!("/workflows/{exec_id}/stack")).await;
    assert_eq!(status, StatusCode::OK);

    let pending = payload["pending_activities"]
        .as_array()
        .expect("pending_activities must be an array");
    assert_eq!(pending.len(), 1);
    // Over-cap payload is withheld; only the truncation marker + size are returned (#503).
    assert!(
        pending[0]["heartbeat_details"].is_null(),
        "over-cap payload must be withheld"
    );
    assert_eq!(pending[0]["heartbeat_details_truncated"], true);
    assert!(
        pending[0]["heartbeat_details_bytes"].as_u64().unwrap() > 2 * 1024 * 1024,
        "reported size must exceed the 2 MiB cap"
    );
}

#[tokio::test]
async fn harvest_api_cancels_workflows_and_rejects_late_signals() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let (start_status, start_json) = post_json(
        &app,
        "/workflows/approval_workflow/start",
        json!({
            "workflow_id": "approval-cancelled",
            "input": { "request_id": "cancelled" },
        }),
    )
    .await;
    assert_eq!(start_status, StatusCode::CREATED);
    let exec_id = start_json["execution_id"]
        .as_str()
        .expect("start response should include execution_id")
        .to_string();

    let cancel_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{exec_id}/cancel"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "reason": "operator changed their mind" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("cancel request failed");
    let cancel_status = cancel_response.status();
    assert_eq!(cancel_status, StatusCode::ACCEPTED);
    let cancel_json = read_json_response(cancel_response).await;
    assert_eq!(cancel_json["ok"], true);
    assert_eq!(cancel_json["execution_id"], exec_id);
    assert_eq!(cancel_json["state"], "CANCELLED");
    assert_eq!(cancel_json["reason"], "operator changed their mind");
    assert_eq!(cancel_json["newly_cancelled"], true);
    assert_eq!(cancel_json["failed_task_count"], 1);

    let (details_status, details_json) = get_json(&app, format!("/workflows/{exec_id}")).await;
    assert_eq!(details_status, StatusCode::OK);
    assert_eq!(details_json["execution"]["state"], "CANCELLED");
    assert_eq!(
        details_json["execution"]["error"],
        "operator changed their mind"
    );
    let history = details_json["history"]
        .as_array()
        .expect("workflow history must be an array");
    assert!(
        history.iter().any(|event| {
            event["type"] == "WorkflowCancelled"
                && event["data"]["reason"] == "operator changed their mind"
        }),
        "history should include the cancellation event"
    );

    let (signal_status, _signal_json) = post_json(
        &app,
        format!("/workflows/{exec_id}/signal/approved"),
        json!({ "approved": true }),
    )
    .await;
    assert_eq!(
        signal_status,
        StatusCode::BAD_REQUEST,
        "late signals to cancelled workflows should be rejected"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn external_runner_processes_workflows_started_via_management_api() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let web_runtime = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                mcp: false,
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
                execution_timeout: None,
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
            }])
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("external web runtime should start without local ownership");

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                mcp: false,
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
                execution_timeout: None,
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
            }])
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: true,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("external runner should start");

    api_state.install_storage_pool(web_runtime.storage_pool());
    api_state.install(web_runtime.api_runtime());
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let (start_status, start_json) = post_json(
        &app,
        "/workflows/approval_workflow/start",
        json!({
            "workflow_id": "approval-external-runner",
            "input": { "request_id": "external-runner" },
        }),
    )
    .await;
    assert_eq!(start_status, StatusCode::CREATED);
    let exec_id = start_json["execution_id"]
        .as_str()
        .expect("execution_id must be present")
        .to_string();

    let (signal_status, _signal_json) = post_json(
        &app,
        format!("/workflows/{exec_id}/signal/approved"),
        json!({ "approved": true }),
    )
    .await;
    assert_eq!(signal_status, StatusCode::ACCEPTED);

    let execution = wait_for_workflow_state(&database_url, &exec_id, "COMPLETED").await;
    assert_eq!(execution.workflow_name, "approval_workflow");

    runner.stop().await;
    web_runtime.stop().await;
}

#[tokio::test]
async fn worker_enqueues_multiple_activity_commands_from_one_workflow_task() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "parallel_activities_workflow",
            module: "tests",
            handler: parallel_activities_workflow,
            execution_timeout: None,
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
        }],
        vec![
            recording_activity_info("parallel_a"),
            recording_activity_info("parallel_b"),
        ],
        Arc::new(state),
    ));
    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "parallel_activities_workflow",
        "parallel-activity-level",
    )
    .await;

    let execution = wait_for_workflow_terminal_state(&database_url, &exec_id.to_string()).await;
    shutdown_test_worker(&worker, worker_task).await;

    assert_eq!(execution.state, "COMPLETED");
    assert_eq!(execution.workflow_name, "parallel_activities_workflow");
    let mut observed = log.lock().expect("log mutex poisoned").clone();
    observed.sort();
    assert_eq!(observed, vec!["parallel_a", "parallel_b"]);
}

#[tokio::test]
async fn worker_does_not_reschedule_inflight_parallel_activity_after_sibling_completes() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "staggered_parallel_workflow",
            module: "tests",
            handler: staggered_parallel_workflow,
            execution_timeout: None,
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
        }],
        vec![
            recording_activity_info("parallel_fast"),
            recording_activity_info("parallel_slow"),
        ],
        Arc::new(state),
    ));
    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "staggered_parallel_workflow",
        "staggered-activity-level",
    )
    .await;

    let execution = wait_for_workflow_terminal_state(&database_url, &exec_id.to_string()).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    shutdown_test_worker(&worker, worker_task).await;

    assert_eq!(execution.state, "COMPLETED");
    let mut observed = log.lock().expect("log mutex poisoned").clone();
    observed.sort();
    assert_eq!(
        observed,
        vec!["parallel_fast", "parallel_slow"],
        "the slow sibling must not be enqueued a second time while its first task is still running"
    );
    assert_eq!(
        count_activity_scheduled_events_from_url(&database_url, exec_id, "parallel_slow").await,
        1,
        "replay while the slow sibling is in-flight must not append a duplicate ActivityScheduled event"
    );
}

#[tokio::test]
async fn worker_resolves_parallel_sibling_tasks_that_share_activity_name() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "parallel_same_activity_workflow",
            module: "tests",
            handler: parallel_same_activity_workflow,
            execution_timeout: None,
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
        }],
        vec![recording_activity_info("shared_parallel")],
        Arc::new(state),
    ));
    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "parallel_same_activity_workflow",
        "parallel-same-activity",
    )
    .await;

    let execution = wait_for_workflow_terminal_state(&database_url, &exec_id.to_string()).await;
    shutdown_test_worker(&worker, worker_task).await;

    assert_eq!(execution.state, "COMPLETED");
    let mut observed = log.lock().expect("log mutex poisoned").clone();
    observed.sort();
    assert_eq!(observed, vec!["left", "right"]);
}

#[tokio::test]
async fn worker_serializes_terminal_events_for_parallel_activity_completions() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "barrier_parallel_workflow",
            module: "tests",
            handler: barrier_parallel_workflow,
            execution_timeout: None,
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
        }],
        vec![
            recording_activity_info("barrier_first"),
            recording_activity_info("barrier_second"),
        ],
        Arc::new(state),
    ));
    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "barrier_parallel_workflow",
        "parallel-terminal-events",
    )
    .await;

    let execution = wait_for_workflow_terminal_state(&database_url, &exec_id.to_string()).await;
    shutdown_test_worker(&worker, worker_task).await;

    assert_eq!(execution.state, "COMPLETED");
    let mut observed = log.lock().expect("log mutex poisoned").clone();
    observed.sort();
    assert_eq!(observed, vec!["barrier_first", "barrier_second"]);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn worker_does_not_append_completion_after_activity_timeout() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let barrier = Arc::new(Barrier::new(2));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    state.insert(
        std::any::TypeId::of::<Arc<Barrier>>(),
        Box::new(Arc::clone(&barrier)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "timeout_completion_race_workflow",
            module: "tests",
            handler: timeout_completion_race_workflow,
            execution_timeout: None,
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
        }],
        vec![blocking_activity_info(
            "timeout_completion_race",
            Duration::from_secs(60),
        )],
        Arc::new(state),
    ));
    let exec_id = insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "timeout_completion_race_workflow",
        "timeout-completion-race",
    )
    .await;

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    let running_task = wait_for_activity_task_state_from_url(
        &database_url,
        exec_id,
        "timeout_completion_race",
        "RUNNING",
    )
    .await;
    wait_for_activity_started_event_from_url(&database_url, exec_id, "timeout_completion_race")
        .await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for forced timeout sweep");
        assert!(
            running_task.start_to_close.is_some(),
            "activity task should persist a start-to-close timeout"
        );
        let history = store::load_history(&mut conn, exec_id)
            .await
            .expect("failed to load activity history before timeout");
        store::append_events(
            &mut conn,
            exec_id,
            &[WorkflowEvent::ActivityTimedOut {
                activity_id: ActivityExecId::from_uuid(
                    running_task
                        .activity_id
                        .expect("activity task should carry activity_id"),
                ),
                timeout_type: TimeoutType::StartToClose,
            }],
            history.next_event_id,
        )
        .await
        .expect("failed to append competing timeout");
        autumn_harvest::queue::fail_task(&mut conn, running_task.id, "activity timed out")
            .await
            .expect("failed to fail timed-out activity task");
        autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id)
            .await
            .expect("failed to wake workflow after timeout");
    }

    barrier.wait().await;
    let execution = wait_for_workflow_terminal_state(&database_url, &exec_id.to_string()).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    shutdown_test_worker(&worker, worker_task).await;

    assert_eq!(execution.state, "FAILED");
    let counts =
        activity_history_counts_from_url(&database_url, exec_id, "timeout_completion_race").await;
    assert_eq!(counts.scheduled, 1);
    assert_eq!(
        counts.timed_out, 1,
        "timeout sweeper should record the activity timeout"
    );
    assert_eq!(
        counts.completed, 0,
        "late worker completion must not append a second terminal event after timeout"
    );
    assert_eq!(
        counts.terminal(),
        1,
        "activity history must contain exactly one terminal event"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn timeout_sweeper_does_not_append_timeout_after_activity_completion() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let (exec_id, activity_id, task_id) = seed_scheduled_activity_task_from_url(
        &database_url,
        "timeout_sweeper_completion_race",
        "timeout-sweeper-completion-race",
        "sweeper_completion_race",
        json!({ "step": "sweeper_completion_race" }),
    )
    .await;

    {
        let mut setup_conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for expired activity setup");
        store::append_events(
            &mut setup_conn,
            exec_id,
            &[WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: autumn_harvest::types::WorkerId::new("race-worker"),
            }],
            2,
        )
        .await
        .expect("failed to append activity start event");
        diesel::update(harvest_task_queue::table.find(task_id))
            .set((
                harvest_task_queue::state.eq("RUNNING"),
                harvest_task_queue::started_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(60))),
                harvest_task_queue::start_to_close.eq(Some(chrono::Duration::seconds(1))),
            ))
            .execute(&mut setup_conn)
            .await
            .expect("failed to mark seeded task as expired and running");
    }

    let mut lock_conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow lock");
    lock_conn
        .batch_execute("BEGIN")
        .await
        .expect("failed to begin workflow lock transaction");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(&mut lock_conn)
        .await
        .expect("failed to lock workflow execution");

    let mut timeout_conn = pool
        .get()
        .await
        .expect("failed to connect for timeout enforcement");
    let timeout_handle = tokio::spawn(async move {
        autumn_harvest::timeout::enforce_timeouts_once(
            &mut timeout_conn,
            &autumn_harvest::telemetry::NoOpMetrics,
            std::time::Duration::from_secs(5),
            &None,
            &[],
            None,
            None,
            60,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    store::append_events(
        &mut lock_conn,
        exec_id,
        &[WorkflowEvent::ActivityCompleted {
            activity_id,
            output: json!({ "completed": true }),
        }],
        3,
    )
    .await
    .expect("failed to append competing completion event");
    autumn_harvest::queue::complete_task(&mut lock_conn, task_id, json!({ "completed": true }))
        .await
        .expect("failed to complete competing activity task");
    lock_conn
        .batch_execute("COMMIT")
        .await
        .expect("failed to commit competing completion");

    timeout_handle
        .await
        .expect("timeout enforcement task should not panic")
        .expect("timeout enforcement should succeed");

    let counts =
        activity_history_counts_from_url(&database_url, exec_id, "sweeper_completion_race").await;
    assert_eq!(counts.scheduled, 1);
    assert_eq!(counts.completed, 1);
    assert_eq!(
        counts.timed_out, 0,
        "timeout sweeper must not append ActivityTimedOut after completion wins"
    );
    assert_eq!(
        counts.terminal(),
        1,
        "activity history must contain exactly one terminal event"
    );
}

#[tokio::test]
async fn worker_does_not_append_started_after_activity_timeout() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let registry = recording_registry(Arc::clone(&log), &["timeout_before_start"]);
    let (exec_id, activity_id, task_id) = seed_scheduled_activity_task_from_url(
        &database_url,
        "activity_start_timeout_race",
        "activity-start-timeout-race",
        "timeout_before_start",
        json!({ "step": "timeout_before_start" }),
    )
    .await;

    let mut lock_conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow lock");
    lock_conn
        .batch_execute("BEGIN")
        .await
        .expect("failed to begin workflow lock transaction");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(&mut lock_conn)
        .await
        .expect("failed to lock workflow execution");

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());
    wait_for_activity_task_state_from_url(
        &database_url,
        exec_id,
        "timeout_before_start",
        "RUNNING",
    )
    .await;

    store::append_events(
        &mut lock_conn,
        exec_id,
        &[WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: TimeoutType::StartToClose,
        }],
        2,
    )
    .await
    .expect("failed to append competing timeout");
    autumn_harvest::queue::fail_task(&mut lock_conn, task_id, "activity timed out")
        .await
        .expect("failed to fail timed-out activity task");
    lock_conn
        .batch_execute("COMMIT")
        .await
        .expect("failed to commit competing timeout");

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown_test_worker(&worker, worker_task).await;

    let counts =
        activity_history_counts_from_url(&database_url, exec_id, "timeout_before_start").await;
    assert_eq!(counts.scheduled, 1);
    assert_eq!(counts.timed_out, 1);
    assert_eq!(
        counts.started, 0,
        "worker must not append ActivityStarted after a timeout wins the workflow lock"
    );
}

#[tokio::test]
async fn retention_janitor_deletes_only_rows_older_than_max_age_and_cascades_children() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
                audit_retention_days: 90,
                schedule_decision_retention_days: 7,
                archival_timeout_secs: 30,
                ..Default::default()
            })
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("retention janitor runner should start");

    let old_exec_a = uuid::Uuid::new_v4();
    let old_exec_b = uuid::Uuid::new_v4();
    let recent_exec = uuid::Uuid::new_v4();
    let inflight_exec = uuid::Uuid::new_v4();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");
    seed_retention_fixtures(
        &mut conn,
        old_exec_a,
        old_exec_b,
        recent_exec,
        inflight_exec,
    )
    .await;

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    trigger_retention_and_wait(&app).await;

    let mut verify_conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to reconnect for verification");
    assert_retention_cleanup_state(
        &mut verify_conn,
        old_exec_a,
        old_exec_b,
        recent_exec,
        inflight_exec,
    )
    .await;

    runner.stop().await;
}

#[tokio::test]
async fn harvest_api_signal_does_not_wake_timer_waits_early() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_and_timer_signal_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (start_status, start_json) = post_json(
        &app,
        "/workflows/timer_then_signal_workflow/start",
        json!({
            "workflow_id": "timer-signal-1",
            "input": { "request_id": "timer-signal-1" },
        }),
    )
    .await;
    assert_eq!(start_status, StatusCode::CREATED);
    let exec_id = start_json["execution_id"]
        .as_str()
        .expect("start response should include execution_id")
        .to_string();

    let mut timer_wait_established = false;
    for _ in 0..200 {
        let (details_status, details_json) = get_json(&app, format!("/workflows/{exec_id}")).await;
        assert_eq!(details_status, StatusCode::OK);
        let history = details_json["history"]
            .as_array()
            .expect("workflow history must be an array");
        let timer_started = history.iter().any(|event| event["type"] == "TimerStarted");
        let timer_fired = history.iter().any(|event| event["type"] == "TimerFired");
        if timer_started && !timer_fired {
            timer_wait_established = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        timer_wait_established,
        "workflow should be parked on a pending timer before the signal is sent"
    );

    let (signal_status, _signal_json) = post_json(
        &app,
        format!("/workflows/{exec_id}/signal/approved"),
        json!({ "approved": true }),
    )
    .await;
    assert_eq!(signal_status, StatusCode::ACCEPTED);

    let execution = wait_for_workflow_state(&database_url, &exec_id, "COMPLETED").await;
    assert_eq!(
        execution.output,
        Some(json!({
            "timer": "fired",
            "approval": { "approved": true },
        }))
    );

    let (details_status, details_json) = get_json(&app, format!("/workflows/{exec_id}")).await;
    assert_eq!(details_status, StatusCode::OK);
    let history = details_json["history"]
        .as_array()
        .expect("workflow history must be an array");
    assert_eq!(
        history
            .iter()
            .filter(|event| event["type"] == "TimerStarted")
            .count(),
        1,
        "signal enqueue should not duplicate timer scheduling"
    );
    assert_eq!(
        history
            .iter()
            .filter(|event| event["type"] == "TimerFired")
            .count(),
        1,
        "timer should fire exactly once"
    );
    assert!(
        history
            .iter()
            .any(|event| event["type"] == "SignalReceived"),
        "history should include the delivered signal"
    );

    shutdown_test_worker(&worker, worker_task).await;
}

#[tokio::test]
async fn harvest_api_lists_and_replays_dead_letters() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    let original_task_id = uuid::Uuid::new_v4();
    let dlq_id = {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for dead-letter setup");
        autumn_harvest::dlq::dead_letter(
            &mut conn,
            &autumn_harvest::dlq::NewDeadLetterEntry {
                original_task_id,
                queue_name: "critical".to_string(),
                task_type: "ACTIVITY".to_string(),
                workflow_exec_id: None,
                activity_name: Some("send_receipt".to_string()),
                input: json!({ "order_id": 42 }),
                error: "smtp is in the coffin".to_string(),
                attempts: 3,

                owner: None,
                severity: None,
            },
        )
        .await
        .expect("dead-letter setup should insert row")
    };

    let (list_status, list_json) = get_json(&app, "/dead-letters").await;
    assert_eq!(list_status, StatusCode::OK);
    let listed = list_json
        .as_array()
        .expect("dead-letter list response must be an array");
    assert!(
        listed.iter().any(|row| {
            row["id"] == dlq_id.to_string()
                && row["original_task_id"] == original_task_id.to_string()
                && row["queue_name"] == "critical"
                && row["task_type"] == "ACTIVITY"
                && row["activity_name"] == "send_receipt"
                && row["attempts"] == 3
        }),
        "inserted dead-letter row should be listed"
    );

    let (replay_status, replay_json) =
        post_json(&app, format!("/dead-letters/{dlq_id}/replay"), json!({})).await;
    assert_eq!(replay_status, StatusCode::ACCEPTED);
    assert_eq!(replay_json["ok"], true);
    assert_eq!(replay_json["dead_letter_id"], dlq_id.to_string());
    let replayed_task_id = replay_json["task_id"]
        .as_str()
        .expect("replay response should include task_id")
        .parse::<uuid::Uuid>()
        .expect("task_id should be a uuid");

    let replayed = load_task_from_url(&database_url, replayed_task_id).await;
    assert_eq!(replayed.queue_name, "critical");
    assert_eq!(replayed.task_type, "activity");
    assert_eq!(replayed.activity_name.as_deref(), Some("send_receipt"));
    assert_eq!(replayed.input, json!({ "order_id": 42 }));
    assert_eq!(replayed.state, "PENDING");
    assert_eq!(replayed.attempt, 0);
    assert_eq!(replayed.max_attempts, 3);
    assert_eq!(count_dead_letters_from_url(&database_url).await, 0);
}

#[tokio::test]
async fn harvest_api_lists_workflows_and_dead_letters_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let exec_on_zero = insert_workflow_on_url(
        &shard0_url,
        ShardId::new(0),
        "workflow_on_zero",
        "workflow-zero",
    )
    .await;
    let exec_on_one = insert_workflow_on_url(
        &shard1_url,
        ShardId::new(1),
        "workflow_on_one",
        "workflow-one",
    )
    .await;
    let dead_letter_on_zero =
        insert_dead_letter_on_url(&shard0_url, "zero-queue", "zero_task").await;
    let dead_letter_on_one = insert_dead_letter_on_url(&shard1_url, "one-queue", "one_task").await;

    let (workflow_status, workflow_json) = get_json(&app, "/workflows?limit=10").await;
    assert_eq!(workflow_status, StatusCode::OK);
    let workflows = workflow_json
        .as_array()
        .expect("workflow list response must be an array");
    assert!(
        workflows
            .iter()
            .any(|row| row["id"] == exec_on_zero.to_string()
                && row["workflow_name"] == "workflow_on_zero"),
        "workflow from shard 0 should be listed"
    );
    assert!(
        workflows
            .iter()
            .any(|row| row["id"] == exec_on_one.to_string()
                && row["workflow_name"] == "workflow_on_one"),
        "workflow from shard 1 should be listed"
    );

    let (dead_letter_status, dead_letter_json) = get_json(&app, "/dead-letters?limit=10").await;
    assert_eq!(dead_letter_status, StatusCode::OK);
    let dead_letters = dead_letter_json
        .as_array()
        .expect("dead-letter list response must be an array");
    assert!(
        dead_letters
            .iter()
            .any(|row| row["id"] == dead_letter_on_zero.to_string()
                && row["queue_name"] == "zero-queue"),
        "dead letter from shard 0 should be listed"
    );
    assert!(
        dead_letters
            .iter()
            .any(|row| row["id"] == dead_letter_on_one.to_string()
                && row["queue_name"] == "one-queue"),
        "dead letter from shard 1 should be listed"
    );

    let (replay_status, replay_json) = post_json(
        &app,
        format!("/dead-letters/{dead_letter_on_one}/replay"),
        json!({}),
    )
    .await;
    assert_eq!(replay_status, StatusCode::ACCEPTED);
    let replayed_task_id = replay_json["task_id"]
        .as_str()
        .expect("replay response should include task_id")
        .parse::<uuid::Uuid>()
        .expect("task_id should be a uuid");
    let replayed = load_task_from_url(&shard1_url, replayed_task_id).await;
    assert_eq!(replayed.queue_name, "one-queue");
    assert_eq!(replayed.activity_name.as_deref(), Some("one_task"));
    assert_eq!(count_dead_letters_from_url(&shard0_url).await, 1);
    assert_eq!(count_dead_letters_from_url(&shard1_url).await, 0);
}

#[tokio::test]
async fn harvest_api_lists_and_triggers_manual_dags() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name: "manual_pipeline",
            module: "tests",
            handler: manual_pipeline_workflow,
            execution_timeout: None,
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
        }],
        vec![],
    ));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![manual_pipeline_info()])
            .expect("manual pipeline dag should compile"),
    );
    register_test_schedules(
        &database_url,
        dag_catalog.as_ref(),
        "failed to connect for schedule registration",
    )
    .await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (dags_status, dags_json) = get_json(&app, "/dags").await;
    assert_eq!(dags_status, StatusCode::OK);
    assert!(
        dags_json
            .as_array()
            .expect("dags response must be an array")
            .iter()
            .any(|dag| dag["name"] == "manual_pipeline"),
        "registered dag should be listed"
    );

    let (trigger_status, trigger_json) = post_json(
        &app,
        "/dags/manual_pipeline/trigger",
        json!({ "conf": { "step": "extract" } }),
    )
    .await;
    assert_eq!(trigger_status, StatusCode::CREATED);
    let run_id = trigger_json["execution_id"]
        .as_str()
        .expect("trigger response should include execution_id");
    let run = load_execution_from_url(&database_url, run_id).await;
    assert_eq!(run.workflow_name, "manual_pipeline");

    let (runs_status, runs_json) = get_json(&app, "/dags/manual_pipeline/runs").await;
    assert_eq!(runs_status, StatusCode::OK);
    assert!(
        runs_json
            .as_array()
            .expect("dag runs response must be an array")
            .iter()
            .any(|row| row["id"] == run.id.to_string()),
        "triggered dag run should be listed"
    );
}

#[tokio::test]
async fn harvest_api_rejects_dag_trigger_for_workflow_without_dag_registration() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        approval_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (trigger_status, _trigger_json) = post_json(
        &app,
        "/dags/approval_workflow/trigger",
        json!({ "conf": { "request_id": "not-a-dag" } }),
    )
    .await;

    assert_eq!(trigger_status, StatusCode::NOT_FOUND);
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, "approval_workflow").await,
        0,
        "workflow-only registrations must not be started via the DAG trigger path"
    );
}

#[tokio::test]
async fn harvest_api_rejects_dag_run_listing_for_workflow_without_dag_registration() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    insert_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "approval_workflow",
        "workflow-only-run",
    )
    .await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        approval_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (runs_status, _runs_json) = get_json(&app, "/dags/approval_workflow/runs").await;

    assert_eq!(runs_status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn harvest_api_triggers_manual_only_unified_dag_on_declared_default_queue() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_info = unified_manual_dag_info_named("manual_only_unified", "dag-workers");
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![dag_info]).expect("manual-only unified dag should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named("manual_only_unified")],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (trigger_status, _trigger_json) = post_json(
        &app,
        "/dags/manual_only_unified/trigger",
        json!({ "conf": { "manual": true } }),
    )
    .await;

    assert_eq!(trigger_status, StatusCode::CREATED);
    let execution =
        load_latest_workflow_execution_by_name_from_url(&database_url, "manual_only_unified")
            .await
            .expect("manual-only unified DAG trigger should create an execution");
    assert_eq!(
        execution.queue_name, "dag-workers",
        "manual-only unified DAGs must use their default_queue without requiring a schedule row"
    );
}

#[tokio::test]
async fn harvest_api_enforces_max_active_runs_for_manual_dag_triggers() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "manual_max_active_unified";
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![unified_manual_dag_info_named(dag_name, "dag-workers")])
            .expect("manual-only unified DAG should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (first_status, _first_json) = post_json(
        &app,
        format!("/dags/{dag_name}/trigger"),
        json!({ "conf": { "ordinal": 1 } }),
    )
    .await;
    assert_eq!(first_status, StatusCode::CREATED);

    let (second_status, second_json) = post_json(
        &app,
        format!("/dags/{dag_name}/trigger"),
        json!({ "conf": { "ordinal": 2 } }),
    )
    .await;

    assert_eq!(second_status, StatusCode::CONFLICT);
    assert!(
        second_json.to_string().contains("max_active_runs"),
        "response should explain that the DAG concurrency gate deferred the trigger: {second_json}"
    );
    assert_eq!(
        count_dag_runs_from_url(&database_url, dag_name).await,
        1,
        "manual DAG triggers must not create a second RUNNING workflow past max_active_runs"
    );
}

#[tokio::test]
async fn harvest_api_defers_manual_dag_trigger_when_schedule_is_paused() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "paused_manual_trigger_unified";
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("manual unified DAG should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Manual,
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: true,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for paused schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
        .await
        .expect("paused DAG schedule should register");

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (trigger_status, trigger_json) = post_json(
        &app,
        format!("/dags/{dag_name}/trigger"),
        json!({ "conf": { "manual": true } }),
    )
    .await;

    assert_eq!(trigger_status, StatusCode::CONFLICT);
    assert!(
        trigger_json.to_string().contains("paused"),
        "response should explain that the paused schedule deferred the trigger: {trigger_json}"
    );
    assert_eq!(
        count_dag_runs_from_url(&database_url, dag_name).await,
        0,
        "paused manual DAG triggers must not create RUNNING workflow executions"
    );
}

#[tokio::test]
async fn harvest_api_patch_creates_pause_row_for_manual_only_unified_dag() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "manual_only_pause_row";
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![unified_manual_dag_info_named(dag_name, "dag-workers")])
            .expect("manual-only unified DAG should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    assert!(
        load_schedule_from_url_optional(&database_url, dag_name)
            .await
            .is_none(),
        "test starts without a durable schedule row for the manual-only DAG"
    );

    let (patch_status, patch_json) =
        patch_json(&app, format!("/dags/{dag_name}"), json!({ "paused": true })).await;

    assert_eq!(patch_status, StatusCode::OK);
    assert_eq!(patch_json["dag_name"], dag_name);
    assert_eq!(patch_json["is_paused"], true);
    let schedule = load_schedule_from_url(&database_url, dag_name).await;
    assert_eq!(schedule.dag_name.as_deref(), Some(dag_name));
    assert!(schedule.workflow_name.is_none());
    assert!(schedule.is_paused);
    assert!(
        schedule.schedule_expr.is_none(),
        "DAGs without a schedule attribute should keep a manual-only pause row"
    );
}

#[tokio::test]
async fn harvest_api_rejects_workflow_schedule_creation_for_registered_dag_name() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![unified_manual_dag_info_named(
            "manual_only_schedule_target",
            "dag-workers",
        )])
        .expect("manual-only unified dag should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named("manual_only_schedule_target")],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        dag_catalog,
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (status, body) = post_json(
        &app,
        "/admin/schedules/workflow",
        json!({
            "workflow_name": "manual_only_schedule_target",
            "schedule_expr": "interval:60",
            "queue_name": "dag-workers"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("registered DAG"),
        "error should explain that DAG names cannot be managed as workflow schedules: {body}"
    );
    assert!(
        load_workflow_only_schedule_from_url_optional(&database_url, "manual_only_schedule_target")
            .await
            .is_none(),
        "rejected workflow schedule creation must not create a workflow-only DAG row"
    );
}

#[tokio::test]
async fn harvest_api_lists_unscheduled_unified_dags_from_catalog() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![unified_manual_dag_info_named(
            "catalog_only_unified",
            "dag-workers",
        )])
        .expect("manual-only unified dag should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named("catalog_only_unified")],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (dags_status, dags_json) = get_json(&app, "/dags").await;
    assert_eq!(dags_status, StatusCode::OK);
    let dags = dags_json
        .as_array()
        .expect("dags response must be an array");
    let row = dags
        .iter()
        .find(|dag| dag["name"] == "catalog_only_unified")
        .expect("manual-only unified DAG should be listed even without a schedule row");
    assert_eq!(row["task_count"], 1);
    assert!(row["schedule_expr"].is_null());
}

#[tokio::test]
async fn harvest_api_routes_dag_reads_and_mutations_to_owned_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let (dag_on_zero, dag_on_one) = sharded_manual_dag_names(&router);
    let dag_catalog = sharded_manual_dag_catalog(dag_on_zero, dag_on_one);
    let registry = recording_registry(
        Arc::new(Mutex::new(Vec::<String>::new())),
        &["extract", "transform", "notify"],
    );

    register_sharded_manual_dag_schedules(&shard0_url, &shard1_url, dag_on_zero, dag_on_one).await;
    let seeded_run_id = seed_dag_run_on_url(&shard1_url, dag_on_one).await;
    let app = build_sharded_dag_api_app(
        &shard0_url,
        &shard1_url,
        Arc::clone(&dag_catalog),
        registry,
        router,
    );

    assert_sharded_dag_list_and_runs(&app, dag_on_zero, dag_on_one, seeded_run_id).await;
    assert_sharded_dag_patch_and_trigger(&app, &shard0_url, &shard1_url, dag_on_zero, dag_on_one)
        .await;
}

#[tokio::test]
async fn harvest_api_rejects_workflow_start_for_registered_dag_name() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let dag_name = find_dag_name_for_shard(&router, "workflow_start_dag", ShardId::new(1));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![unified_manual_dag_info_named(dag_name, "dag-workers")])
            .expect("manual-only unified dag should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let app = build_sharded_dag_api_app(&shard0_url, &shard1_url, dag_catalog, registry, router);

    let (status, body) = post_json(
        &app,
        format!("/workflows/{dag_name}/start"),
        json!({ "input": { "source": "generic-workflow-route" } }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("registered DAG"),
        "error should direct DAG starts away from the generic workflow route: {body}"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard0_url, dag_name).await,
        0,
        "generic workflow route must not create default-shard DAG executions"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard1_url, dag_name).await,
        0,
        "rejected generic DAG workflow start must not create an owning-shard run either"
    );
}

#[tokio::test]
async fn harvest_api_rejects_non_dry_run_backfill_for_paused_dag_schedule() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_info = DagInfo {
        name: "paused_backfill_dag",
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(3600))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    };
    let workflow_schedule = dag_info
        .as_workflow_schedule()
        .expect("scheduled unified DAG should lower to a workflow schedule");
    let dag_catalog =
        Arc::new(compile_dag_catalog(vec![dag_info]).expect("scheduled DAG should compile"));
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named("paused_backfill_dag")],
        vec![],
    ));

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for DAG schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to register DAG workflow schedule");
    }
    let schedule = load_schedule_from_url(&database_url, "paused_backfill_dag").await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for pausing DAG schedule");
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(harvest_schedules::is_paused.eq(true))
            .execute(&mut conn)
            .await
            .expect("failed to pause DAG schedule");
    }

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        dag_catalog,
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));
    let backfill_at = chrono::Utc::now() - chrono::Duration::hours(1);

    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({
            "from": backfill_at,
            "to": backfill_at,
            "include_paused": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("paused DAG schedule"),
        "error should explain that paused DAG backfills do not run immediately: {body}"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, "paused_backfill_dag").await,
        0,
        "rejected paused DAG backfill must not enqueue a workflow execution"
    );
}

#[tokio::test]
async fn harvest_api_backfills_legacy_dag_schedule_null_queue_on_dag_default_queue() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "backfill_null_queue_dag";
    let dag_info = DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(3600))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    };
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![dag_info]).expect("scheduled unified DAG should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for legacy DAG schedule registration");
        let dag = dag_catalog
            .get(dag_name)
            .expect("compiled DAG should be registered");
        autumn_harvest::scheduler::ensure_dag_schedule(&mut conn, dag)
            .await
            .expect("legacy DAG schedule row should be created");
    }
    let schedule = load_schedule_from_url(&database_url, dag_name).await;
    assert!(
        schedule.queue_name.is_none(),
        "legacy/classic DAG rows can have no durable queue_name"
    );
    assert!(
        schedule.workflow_name.is_none(),
        "ensure_dag_schedule models the upgraded classic row before unified registration"
    );

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));
    let backfill_at = chrono::Utc::now() - chrono::Duration::hours(2);

    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({
            "from": backfill_at,
            "to": backfill_at
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["dispatched"], serde_json::json!(1));
    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("backfill should start the DAG workflow execution");
    assert_eq!(
        execution.queue_name, "dag-workers",
        "legacy DAG backfills should fall back to the registered DAG default_queue before default"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn harvest_api_backfill_matches_fractional_legacy_dag_workflow_id() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "fractional_legacy_backfill_dag";
    let dag_info = DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(3600))),
        catchup: false,
        max_active_runs: 5,
        default_queue: Some("dag-workers"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    };
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![dag_info]).expect("scheduled unified DAG should compile"),
    );
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(3600)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 5,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for fractional DAG schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to seed fractional DAG-backed workflow schedule");
    }
    let schedule = load_schedule_from_url(&database_url, dag_name).await;

    let backfill_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00.123456Z")
        .expect("fixed timestamp should parse")
        .with_timezone(&chrono::Utc);
    let migrated_workflow_id = format!(
        "sched:{dag_name}:{}.{:06}",
        backfill_at.timestamp(),
        backfill_at.timestamp_subsec_micros()
    );
    let seeded_exec = insert_workflow_on_url(
        &database_url,
        ShardId::UNENCODED,
        dag_name,
        &migrated_workflow_id,
    )
    .await;
    mark_workflow_completed_on_url(&database_url, seeded_exec, backfill_at).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({
            "from": backfill_at,
            "to": backfill_at
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["dispatched"], serde_json::json!(0));
    assert_eq!(body["skipped"], serde_json::json!(1));
    assert_eq!(
        body["skipped_reasons"]["already_exists"],
        serde_json::json!(1),
        "fractional migrated legacy workflow IDs must de-dupe matching DAG backfills"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, dag_name).await,
        1,
        "backfill should not create a duplicate run for a fractional legacy logical_date"
    );
}

#[tokio::test]
async fn harvest_api_rejects_backfill_for_unregistered_dag_schedule_row() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "removed_backfill_dag";
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(3600)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for stale DAG schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to seed stale DAG-backed workflow schedule");
    }
    let schedule = load_schedule_from_url(&database_url, dag_name).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));
    let backfill_at = chrono::Utc::now() - chrono::Duration::hours(2);

    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({
            "from": backfill_at,
            "to": backfill_at
        }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.to_string().contains("not registered"),
        "error should explain that stale DAG schedule rows cannot be backfilled: {body}"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, dag_name).await,
        0,
        "unregistered DAG schedule backfill must not start a workflow execution"
    );
}

// ── Backfill max_runs atomic reservation (issue #688) ─────────────────────────

/// Register a pure workflow-only (`dag_name` = NULL) hourly-cron schedule with a
/// given `max_runs` budget and stand up an app whose runtime knows the workflow,
/// so backfill dispatches actually create executions. Returns the app plus the
/// freshly-loaded schedule row (`runs_started` = 0 on insert).
async fn setup_workflow_backfill_app(
    database_url: &str,
    pool: DbPool,
    name: &'static str,
    max_runs: Option<u32>,
) -> (HarvestApiApp, HarvestSchedule) {
    let workflow_schedule = WorkflowSchedule {
        workflow_name: name.to_string(),
        dag_name: None,
        schedule: Schedule::Cron("0 * * * *".to_string()),
        input: Value::Null,
        catchup: false,
        // Keep max_active_runs high so it never gates the small backfill windows
        // below — the test isolates the max_runs budget path.
        max_active_runs: 1000,
        paused: false,
        queue_name: "default".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs,
        catchup_policy: None,
        retry_policy: None,
    };
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
            .await
            .expect("failed to connect for workflow schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("workflow schedule should register");
    }
    let schedule = load_workflow_only_schedule_from_url_optional(database_url, name)
        .await
        .expect("workflow-only schedule row should exist");

    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(name)],
        vec![],
    ));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(compile_dag_catalog(vec![]).expect("empty DAG catalog should compile")),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));
    (app, schedule)
}

/// Like `setup_workflow_backfill_app`, but the registered `WorkflowInfo` carries
/// a `throttle` policy (so `workflow_resolving_throttle` returns `Some` and the
/// backfill loop enters the throttle block, where the oversized-input check —
/// and its `release_backfill_budget_slot` release path — lives) and the schedule
/// input can be sized to overflow the workflow-input cap (issue #688 release
/// test). The throttle policy has no key (`key_expr = None`), so every slot
/// resolves the same empty key.
async fn setup_throttled_workflow_backfill_app(
    database_url: &str,
    pool: DbPool,
    name: &'static str,
    max_runs: Option<u32>,
    input: Value,
) -> (HarvestApiApp, HarvestSchedule) {
    let workflow_schedule = WorkflowSchedule {
        workflow_name: name.to_string(),
        dag_name: None,
        schedule: Schedule::Cron("0 * * * *".to_string()),
        input,
        catchup: false,
        max_active_runs: 1000,
        paused: false,
        queue_name: "default".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs,
        catchup_policy: None,
        retry_policy: None,
    };
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
            .await
            .expect("failed to connect for workflow schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("workflow schedule should register");
    }
    let schedule = load_workflow_only_schedule_from_url_optional(database_url, name)
        .await
        .expect("workflow-only schedule row should exist");

    let mut info = workflow_info_named(name);
    // A tiny per-workflow cap documents intent, but the effective cap is
    // max(per, registry-global), so it never *lowers* the cap below the
    // registry's 2 MiB default. The test's input is instead sized past that
    // default, forcing the oversized-input path without mutating the
    // process-global GLOBAL_MAX_WORKFLOW_INPUT_BYTES (which would race parallel
    // tests in this binary).
    info.max_input_bytes = Some(1);
    info.throttle = Some(
        autumn_harvest::throttle::ThrottlePolicy::from_rate_str("100/m", None, None, None)
            .expect("valid throttle policy"),
    );

    let registry = Arc::new(HandlerRegistry::new(vec![info], vec![]));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(compile_dag_catalog(vec![]).expect("empty DAG catalog should compile")),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));
    (app, schedule)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn backfill_atomic_reservation_two_concurrent_requests_dispatch_exactly_one() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_race_one_slot_wf";
    // Exactly one max_runs slot remains (max_runs = 1, runs_started = 0).
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, Some(1)).await;

    // A 3-hour hourly window → 4 candidate slots, more than the single remaining
    // slot, so two concurrent requests both have work to dispatch.
    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T13:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let uri = format!("/admin/schedules/{}/backfill", schedule.id);
    let body = json!({ "from": from, "to": to });

    // Fire both requests genuinely concurrently on a multi-thread runtime (two
    // spawned tasks, not cooperatively-scheduled `tokio::join!` futures on one
    // thread) so the atomic reservation is exercised under real thread-level
    // parallelism — a stale-snapshot gate would let BOTH dispatch here.
    let (app_a, app_b) = (app.clone(), app.clone());
    let (uri_a, uri_b) = (uri.clone(), uri.clone());
    let (body_a, body_b) = (body.clone(), body.clone());
    let handle_a = tokio::spawn(async move { post_json(&app_a, uri_a, body_a).await });
    let handle_b = tokio::spawn(async move { post_json(&app_b, uri_b, body_b).await });
    let (status_a, json_a) = handle_a.await.expect("backfill request A must not panic");
    let (status_b, json_b) = handle_b.await.expect("backfill request B must not panic");
    assert_eq!(status_a, StatusCode::OK, "req A body: {json_a}");
    assert_eq!(status_b, StatusCode::OK, "req B body: {json_b}");

    let dispatched_a = json_a["dispatched"].as_u64().unwrap();
    let dispatched_b = json_b["dispatched"].as_u64().unwrap();
    assert_eq!(
        dispatched_a + dispatched_b,
        1,
        "exactly one slot must be dispatched across two concurrent requests \
         (A={dispatched_a}, B={dispatched_b}); atomic reservation must not over-run max_runs"
    );

    let reloaded = load_workflow_only_schedule_from_url_optional(&database_url, name)
        .await
        .expect("schedule row should still exist");
    assert_eq!(
        reloaded.runs_started, 1,
        "runs_started must equal max_runs after the single dispatch"
    );
    assert!(
        reloaded.exhausted_at.is_some(),
        "schedule must transition to exhausted once the last slot is consumed"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, name).await,
        1,
        "exactly one execution row must exist for the schedule"
    );
}

/// F4 regression (issue #688 review, Codex): with a web DB pool of size 1
/// (single-shard), a non-dry-run backfill must NOT self-deadlock. Before the fix
/// the route held a dedicated schedule-shard connection across the dispatch loop
/// while each slot also checked out a per-slot exec conn — two concurrent
/// connections against a size-1 pool wedge forever (the route waits on a
/// connection it already holds). The fix reuses the per-slot exec conn for budget
/// accounting when the schedule and exec shards share a pool, so the route holds
/// exactly one connection at a time again (restoring pre-#688 pool-size-1 safety).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_single_shard_pool_size_one_does_not_deadlock() {
    let (database_url, _container) = setup_test_database_url().await;
    // Deliberately size the pool at 1: exercising the exact F4 self-deadlock
    // condition (pool.rs only forbids size 0, so 1 is a valid production config).
    let pool = {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url.as_str());
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("failed to build size-1 test pool")
    };
    let name = "backfill_pool_size_one_wf";
    // Unlimited budget so every planned slot actually dispatches, exercising the
    // reserve + transition budget path on the single shared connection.
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, None).await;

    let from = chrono::DateTime::parse_from_rfc3339("2026-05-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-05-01T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let uri = format!("/admin/schedules/{}/backfill", schedule.id);
    let body = json!({ "from": from, "to": to });

    // If the route self-deadlocks on the size-1 pool this times out (a clear
    // failure) rather than hanging the whole test binary indefinitely.
    let (status, json) = tokio::time::timeout(Duration::from_secs(20), post_json(&app, uri, body))
        .await
        .expect("backfill on a size-1 pool must return, not self-deadlock");

    assert_eq!(status, StatusCode::OK, "backfill body: {json}");
    assert!(
        json["dispatched"].as_u64().unwrap() >= 1,
        "at least one slot should dispatch, body: {json}"
    );
}

/// Issue #740 (AC3): a schedule backfill records the `backfill` source
/// referencing the schedule id. Backfill is only reachable through this
/// plugin HTTP handler (`POST /admin/schedules/{id}/backfill`), so this is the
/// single direct assertion of the backfill provenance path. Driven with a
/// non-throttled workflow schedule (the common backfill branch) whose runs are
/// dispatched immediately, then inspected on the created execution rows.
#[tokio::test]
async fn backfill_records_backfill_source_referencing_schedule_id() {
    let (database_url, _container) = overdue_read_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_provenance_wf";

    // Isolate this workflow/schedule name on a possibly-shared DB.
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("connect for isolation scrub");
        diesel::delete(
            harvest_workflow_executions::table
                .filter(harvest_workflow_executions::workflow_name.eq(name)),
        )
        .execute(&mut conn)
        .await
        .expect("clear prior executions");
        diesel::delete(harvest_schedules::table.filter(harvest_schedules::workflow_name.eq(name)))
            .execute(&mut conn)
            .await
            .expect("clear prior schedule");
    }

    // Unlimited budget → every planned slot dispatches immediately.
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, None).await;

    // A 1-hour hourly window → 2 candidate slots (10:00, 11:00), both dispatch.
    let from = chrono::DateTime::parse_from_rfc3339("2026-06-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-06-01T11:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "backfill body: {body}");
    assert!(
        body["dispatched"].as_u64().unwrap() >= 1,
        "at least one backfilled slot should dispatch, body: {body}"
    );

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect for row read");
    let rows: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(name))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("load backfilled runs");
    assert!(
        !rows.is_empty(),
        "a non-dry-run backfill should create execution rows"
    );
    for row in &rows {
        assert_eq!(
            row.start_source.as_deref(),
            Some("backfill"),
            "a backfilled run records the `backfill` source, not `api`"
        );
        assert_eq!(
            row.start_source_ref.as_deref(),
            Some(schedule.id.to_string().as_str()),
            "a backfilled run references the schedule id"
        );
    }
}

/// Deterministic reserve-then-RELEASE proof (issue #688): every slot reserves a
/// `max_runs` budget slot, enters the throttle block, hits the oversized-input
/// guard, and releases the reservation. No slot ever dispatches, and the
/// schedule's `runs_started` must return to exactly its pre-backfill value —
/// proving `release_backfill_budget_slot` returns every reserved slot.
#[tokio::test]
async fn backfill_release_on_oversized_input_leaves_budget_intact() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_release_oversized_wf";
    // A schedule input that serializes past the registry's default 2 MiB
    // workflow-input cap, so the throttle block's oversized-input guard trips
    // for every slot. (The effective cap is max(per-workflow, registry-global);
    // rather than lower the process-global cap — which would race parallel tests
    // — the input is sized past the default.)
    let big = "x".repeat(2 * 1024 * 1024 + 1024);
    let input = json!({ "blob": big });
    // Budget of 5 with a 2-slot window: budget is never exhausted, so any
    // net change in runs_started can only come from a leaked reservation.
    let (app, schedule) =
        setup_throttled_workflow_backfill_app(&database_url, pool, name, Some(5), input).await;

    // A 1-hour hourly window → 2 candidate slots (10:00, 11:00).
    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T11:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    let window_size = body["total"].as_u64().unwrap();
    assert_eq!(window_size, 2, "1-hour hourly window has 2 slots");
    assert_eq!(
        body["dispatched"],
        serde_json::json!(0),
        "no slot may dispatch — every one overflows the input cap"
    );
    assert_eq!(
        body["skipped_reasons"]["oversized_input"],
        serde_json::json!(window_size),
        "every slot must be skipped for oversized_input"
    );

    let reloaded = load_workflow_only_schedule_from_url_optional(&database_url, name)
        .await
        .expect("schedule row should still exist");
    assert_eq!(
        reloaded.runs_started, 0,
        "runs_started must return to its pre-backfill value — every reserved \
         slot was released, none leaked"
    );
    assert!(
        reloaded.exhausted_at.is_none(),
        "a schedule whose budget was never actually consumed must not be exhausted"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, name).await,
        0,
        "no execution rows may be created when every slot is skipped"
    );
}

#[tokio::test]
async fn backfill_over_window_dispatches_only_remaining_budget() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_over_window_wf";
    // Two slots of budget, a four-slot window.
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, Some(2)).await;

    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T13:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], serde_json::json!(4), "window has 4 slots");
    assert_eq!(body["dispatched"], serde_json::json!(2));
    assert_eq!(
        body["skipped_reasons"]["max_runs_exhausted"],
        serde_json::json!(2),
        "the two out-of-budget slots must be reported as max_runs_exhausted"
    );

    let reloaded = load_workflow_only_schedule_from_url_optional(&database_url, name)
        .await
        .expect("schedule row should still exist");
    assert_eq!(reloaded.runs_started, 2);
    assert!(reloaded.exhausted_at.is_some());
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, name).await,
        2,
    );
}

#[tokio::test]
async fn backfill_unlimited_schedule_increments_runs_started() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_unlimited_wf";
    // Unlimited budget (max_runs = None) — every slot dispatches, runs_started
    // still increments for observability, exhausted_at stays NULL.
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, None).await;

    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], serde_json::json!(3), "window has 3 slots");
    assert_eq!(body["dispatched"], serde_json::json!(3));

    let reloaded = load_workflow_only_schedule_from_url_optional(&database_url, name)
        .await
        .expect("schedule row should still exist");
    assert_eq!(
        reloaded.runs_started, 3,
        "runs_started must increase by the number dispatched even when unlimited"
    );
    assert!(
        reloaded.exhausted_at.is_none(),
        "an unlimited schedule must never transition to exhausted"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, name).await,
        3,
    );
}

/// A row with `max_runs = 0` in the column must be treated as UNLIMITED by the
/// atomic reservation guard (issue #688 review, Codex F1) — matching
/// `backfill_budget_reached`, the up-front exhaustion check, and the dry-run
/// projection, all of which read non-positive `max_runs` as "no cap". The
/// builder normalizes `0 → None`, so this writes `0` to the column DIRECTLY via
/// diesel to simulate a pre-existing / hand-written row. Without the
/// `max_runs <= 0` clause in the reservation predicate, `0 < 0` is false and the
/// backfill would wrongly skip every slot as `max_runs_exhausted`.
///
/// NOTE: the release-clears-exhaustion path (F2b) is a concurrent-only scenario
/// not deterministically reproducible in a single-request test: within one
/// request, once a slot transitions the row to exhausted, subsequent
/// reservations fail the `exhausted_at IS NULL` guard and set `budget_hit`, so no
/// release-after-transition occurs intra-request. F2b is covered by the
/// `clear_stale_max_runs_exhaustion_generates_the_guarded_clear_update` shape
/// test plus code reasoning, matching the no-Docker precedent.
#[tokio::test]
async fn backfill_max_runs_zero_is_treated_as_unlimited() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "backfill_max_runs_zero_wf";
    // Register unlimited (builder normalizes 0 → None), then force the column to
    // literal 0 to exercise the reservation guard's non-positive handling.
    let (app, schedule) = setup_workflow_backfill_app(&database_url, pool, name, None).await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect to force max_runs = 0");
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(harvest_schedules::max_runs.eq(Some(0_i32)))
            .execute(&mut conn)
            .await
            .expect("forcing max_runs = 0 must succeed");
    }

    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], serde_json::json!(3), "window has 3 slots");
    assert_eq!(
        body["dispatched"],
        serde_json::json!(3),
        "max_runs = 0 must be treated as unlimited — every slot dispatches"
    );

    let reloaded = load_workflow_only_schedule_from_url_optional(&database_url, name)
        .await
        .expect("schedule row should still exist");
    assert_eq!(
        reloaded.runs_started, 3,
        "runs_started must increase by the number dispatched (max_runs = 0 = unlimited)"
    );
    assert!(
        reloaded.exhausted_at.is_none(),
        "a max_runs = 0 (unlimited) schedule must never transition to exhausted"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, name).await,
        3,
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn backfill_dag_over_window_dispatches_only_remaining_budget() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "backfill_dag_over_window";
    // A unified DAG schedule row (dag_name set) with a two-slot budget over a
    // four-slot window — exercises the DAG loop's atomic reservation path.
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Cron("0 * * * *".to_string())),
            catchup: false,
            max_active_runs: 1000,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("unified DAG should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Cron("0 * * * *".to_string()),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1000,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: Some(2),
        catchup_policy: None,
        retry_policy: None,
    };
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for DAG schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("DAG schedule should register");
    }
    let schedule = load_schedule_from_url(&database_url, dag_name).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::clone(&dag_catalog),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["dag-workers".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool.clone()));

    let from = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let to = chrono::DateTime::parse_from_rfc3339("2026-04-01T13:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let (status, body) = post_json(
        &app,
        format!("/admin/schedules/{}/backfill", schedule.id),
        json!({ "from": from, "to": to }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], serde_json::json!(4), "window has 4 slots");
    assert_eq!(body["dispatched"], serde_json::json!(2));
    assert_eq!(
        body["skipped_reasons"]["max_runs_exhausted"],
        serde_json::json!(2),
    );

    let reloaded = load_schedule_from_url(&database_url, dag_name).await;
    assert_eq!(reloaded.runs_started, 2);
    assert!(reloaded.exhausted_at.is_some());
    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, dag_name).await,
        2,
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scheduler_tick_creates_and_executes_due_interval_runs() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "interval_pipeline",
            module: "tests",
            handler: interval_pipeline_workflow,
            execution_timeout: None,
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
        }],
        vec![ActivityInfo {
            name: "interval_step",
            module: "tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            requires: None,
            handler: record_activity,
        }],
        Arc::new(state),
    ));
    let dag_info = interval_pipeline_info();
    let workflow_schedule = dag_info
        .as_workflow_schedule()
        .expect("interval DAG should lower to a workflow schedule");
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![dag_info]).expect("interval pipeline dag should compile"),
    );

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to register interval dag workflow schedule");
    }

    let schedule = load_schedule_from_url(&database_url, "interval_pipeline").await;
    assert!(
        schedule.next_run_at.is_some(),
        "interval schedule should have next_run_at"
    );
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for forcing due interval schedule");
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force interval schedule due");
    }

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());

    tick_once(
        pool.clone(),
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::new(vec![workflow_schedule]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should succeed");

    let run = wait_for_dag_run_state(&database_url, "interval_pipeline", "COMPLETED").await;
    assert_eq!(run.workflow_name, "interval_pipeline");
    assert_eq!(
        log.lock().expect("log mutex poisoned").clone(),
        vec!["interval_step"]
    );
    shutdown_test_worker(&worker, worker_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn concurrent_scheduler_ticks_activate_due_dag_run_once() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut state = HashMap::new();
    state.insert(
        std::any::TypeId::of::<Arc<Mutex<Vec<String>>>>(),
        Box::new(Arc::clone(&log)) as Box<dyn std::any::Any + Send + Sync>,
    );
    let registry = Arc::new(HandlerRegistry::with_state(
        vec![WorkflowInfo {
            mcp: false,
            name: "interval_pipeline",
            module: "tests",
            handler: interval_pipeline_workflow,
            execution_timeout: None,
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
        }],
        vec![recording_activity_info("interval_step")],
        Arc::new(state),
    ));
    let dag_info = interval_pipeline_info();
    let workflow_schedule = dag_info
        .as_workflow_schedule()
        .expect("interval DAG should lower to a workflow schedule");
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![dag_info]).expect("interval pipeline dag should compile"),
    );
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to register interval dag workflow schedule");
    }

    let schedule = load_schedule_from_url(&database_url, "interval_pipeline").await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for forcing due interval schedule");
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force interval schedule due");
    }

    let gate = Arc::new(tokio::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let gate = Arc::clone(&gate);
        let pool = pool.clone();
        let registry = Arc::clone(&registry);
        let dag_catalog = Arc::clone(&dag_catalog);
        let workflow_schedules = Arc::new(vec![workflow_schedule.clone()]);
        handles.push(tokio::spawn(async move {
            gate.wait().await;
            tick_once(
                pool,
                registry,
                dag_catalog,
                workflow_schedules,
                SchedulerMonitor::offline(),
            )
            .await
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("concurrent scheduler tick task should not panic")
            .expect("concurrent scheduler tick should succeed");
    }

    let worker = build_test_worker(Arc::clone(&registry));
    let worker_task = spawn_test_worker(Arc::clone(&worker), pool.clone());
    let run = wait_for_dag_run_state(&database_url, "interval_pipeline", "COMPLETED").await;
    assert_eq!(run.workflow_name, "interval_pipeline");
    assert_eq!(
        count_dag_runs_from_url(&database_url, "interval_pipeline").await,
        1,
        "one due logical date should create one durable DAG run"
    );
    assert_eq!(
        log.lock().expect("log mutex poisoned").clone(),
        vec!["interval_step"],
        "concurrent schedulers must not double-activate the same queued run"
    );
    shutdown_test_worker(&worker, worker_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scheduler_ticks_dispatch_due_workflow_schedule_once() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
    let workflow_name = "scheduled_exact_once_workflow";
    let workflow_schedule =
        WorkflowSchedule::new(workflow_name, Schedule::Interval(Duration::from_secs(60)))
            .with_input(json!({ "source": "concurrent-scheduler" }));

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for workflow schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to register workflow schedule");
        diesel::update(
            harvest_schedules::table.filter(harvest_schedules::workflow_name.eq(workflow_name)),
        )
        .set(
            harvest_schedules::next_run_at
                .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
        )
        .execute(&mut conn)
        .await
        .expect("failed to force workflow schedule due");
    }

    let gate = Arc::new(tokio::sync::Barrier::new(8));
    let workflow_schedules = Arc::new(vec![workflow_schedule]);
    let empty_dags = Arc::new(DagCatalog::default());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let gate = Arc::clone(&gate);
        let pool = pool.clone();
        let registry = Arc::clone(&registry);
        let empty_dags = Arc::clone(&empty_dags);
        let workflow_schedules = Arc::clone(&workflow_schedules);
        handles.push(tokio::spawn(async move {
            gate.wait().await;
            tick_once(
                pool,
                registry,
                empty_dags,
                workflow_schedules,
                SchedulerMonitor::offline(),
            )
            .await
        }));
    }

    for handle in handles {
        handle
            .await
            .expect("concurrent scheduler tick task should not panic")
            .expect("concurrent scheduler tick should succeed");
    }

    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, workflow_name).await,
        1,
        "one due workflow schedule slot should create one execution"
    );
    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, workflow_name)
        .await
        .expect("scheduled workflow execution should exist");
    assert!(
        execution.workflow_id.starts_with("sched:")
            && execution
                .workflow_id
                .contains(&format!(":{workflow_name}:")),
        "scheduled workflow id must be deterministic for duplicate suppression"
    );
    assert_eq!(
        count_workflow_tasks_from_url(&database_url, &execution.id.to_string()).await,
        1,
        "duplicate scheduler ticks must not enqueue duplicate workflow tasks"
    );
}

#[tokio::test]
async fn register_workflow_schedules_accepts_unified_dag_schedule_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let workflow_schedule = WorkflowSchedule {
        workflow_name: "scheduled_unified_dag".to_string(),
        dag_name: Some("scheduled_unified_dag".to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
        .await
        .expect("unified DAG workflow schedule rows may carry both dag_name and workflow_name");

    let schedule = load_schedule_from_url(&database_url, "scheduled_unified_dag").await;
    assert_eq!(
        schedule.workflow_name.as_deref(),
        Some("scheduled_unified_dag")
    );
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
}

#[tokio::test]
async fn register_workflow_schedules_preserves_existing_dag_marker_for_workflow_schedule_target() {
    let (database_url, _container) = setup_test_database_url().await;
    let unified_dag_row = WorkflowSchedule {
        workflow_name: "preserve_dag_marker".to_string(),
        dag_name: Some("preserve_dag_marker".to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: json!({ "source": "dag" }),
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let workflow_only_update = WorkflowSchedule::new(
        "preserve_dag_marker",
        Schedule::Interval(Duration::from_secs(120)),
    )
    .with_input(json!({ "source": "workflow-api" }));

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for workflow schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&unified_dag_row))
        .await
        .expect("unified DAG row should register");
    let original = load_schedule_from_url(&database_url, "preserve_dag_marker").await;
    register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_only_update))
        .await
        .expect("workflow schedule update should not erase an existing DAG marker");

    let schedule = load_schedule_from_url(&database_url, "preserve_dag_marker").await;
    assert_eq!(schedule.id, original.id);
    assert_eq!(
        schedule.dag_name.as_deref(),
        Some("preserve_dag_marker"),
        "workflow schedule updates targeting a DAG-backed row must preserve dag_name"
    );
    assert_eq!(
        schedule.workflow_name.as_deref(),
        Some("preserve_dag_marker")
    );
    assert_eq!(
        schedule.workflow_input,
        Some(json!({ "source": "workflow-api" }))
    );
}

#[tokio::test]
async fn register_workflow_schedules_migrates_legacy_workflow_only_dag_row() {
    let (database_url, _container) = setup_test_database_url().await;
    let legacy_workflow_row = WorkflowSchedule {
        workflow_name: "legacy_workflow_only_dag".to_string(),
        dag_name: None,
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: json!({ "source": "legacy" }),
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "legacy-queue".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let unified_dag_row = WorkflowSchedule {
        workflow_name: "legacy_workflow_only_dag".to_string(),
        dag_name: Some("legacy_workflow_only_dag".to_string()),
        schedule: Schedule::Interval(Duration::from_secs(120)),
        input: json!({ "source": "unified" }),
        catchup: true,
        max_active_runs: 3,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for legacy workflow-only schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&legacy_workflow_row))
        .await
        .expect("legacy workflow-only row should register");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&unified_dag_row))
        .await
        .expect("unified DAG registration should update the legacy workflow_name row in place");

    let schedule = load_schedule_from_url(&database_url, "legacy_workflow_only_dag").await;
    assert_eq!(
        schedule.workflow_name.as_deref(),
        Some("legacy_workflow_only_dag")
    );
    assert_eq!(
        schedule.workflow_input,
        Some(json!({ "source": "unified" }))
    );
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
    assert!(schedule.catchup);
    assert_eq!(schedule.max_active_runs, 3);
}

#[tokio::test]
async fn ensure_dag_schedule_reuses_paused_legacy_workflow_only_dag_row() {
    let (database_url, _container) = setup_test_database_url().await;
    let dag_name = "paused_workflow_only_upgrade";
    let legacy_workflow_row = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: None,
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: json!({ "source": "legacy" }),
        catchup: false,
        max_active_runs: 1,
        paused: true,
        queue_name: "legacy-queue".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let paused_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00.123456Z")
        .expect("fixed pause timestamp should parse")
        .with_timezone(&chrono::Utc);
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("unified DAG should compile"),
    );

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for legacy workflow-only schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&legacy_workflow_row))
            .await
            .expect("legacy workflow-only row should register");
        let legacy = load_workflow_only_schedule_from_url_optional(&database_url, dag_name)
            .await
            .expect("legacy workflow-only row should exist");
        diesel::update(harvest_schedules::table.find(legacy.id))
            .set((
                harvest_schedules::paused_at.eq(Some(paused_at)),
                harvest_schedules::paused_by.eq(Some("ops-team")),
                harvest_schedules::pause_reason.eq(Some("incident-response")),
            ))
            .execute(&mut conn)
            .await
            .expect("failed to attach pause metadata to legacy workflow-only row");

        let dag = dag_catalog
            .get(dag_name)
            .expect("compiled DAG should be registered");
        autumn_harvest::scheduler::ensure_dag_schedule(&mut conn, dag)
            .await
            .expect("ensure_dag_schedule should reuse legacy workflow-only DAG row");
    }

    assert_eq!(
        count_schedule_rows_for_name_from_url(&database_url, dag_name).await,
        1,
        "ensure_dag_schedule should mark the legacy workflow-only row instead of inserting a fresh unpaused DAG row"
    );
    let schedule = load_schedule_from_url(&database_url, dag_name).await;
    assert_eq!(schedule.dag_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.workflow_name.as_deref(), Some(dag_name));
    assert!(schedule.is_paused);
    assert_eq!(schedule.paused_at, Some(paused_at));
    assert_eq!(schedule.paused_by.as_deref(), Some("ops-team"));
    assert_eq!(schedule.pause_reason.as_deref(), Some("incident-response"));
}

#[tokio::test]
async fn register_workflow_schedules_reuses_existing_dag_schedule_row_on_upgrade() {
    let (database_url, _container) = setup_test_database_url().await;
    let classic_dag = compile_dag_catalog(vec![DagInfo {
        name: "upgraded_scheduled_dag",
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(60))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default"),
        builder: build_interval_pipeline_dag,
        workflow_handler: None,

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }])
    .expect("classic scheduled DAG should compile");
    register_test_schedules(
        &database_url,
        &classic_dag,
        "failed to connect for classic schedule registration",
    )
    .await;
    let old_schedule = load_schedule_from_url(&database_url, "upgraded_scheduled_dag").await;
    assert!(
        old_schedule.workflow_name.is_none(),
        "pre-upgrade classic DAG schedule row should not have workflow_name"
    );

    let workflow_schedule = WorkflowSchedule {
        workflow_name: "upgraded_scheduled_dag".to_string(),
        dag_name: Some("upgraded_scheduled_dag".to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: json!({ "source": "upgrade" }),
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for unified schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
        .await
        .expect("unified DAG registration should convert the existing dag_name row");

    let upgraded_schedule = load_schedule_from_url(&database_url, "upgraded_scheduled_dag").await;
    assert_eq!(
        upgraded_schedule.id, old_schedule.id,
        "upgrade should reuse the existing dag_name row instead of inserting a conflicting row"
    );
    assert_eq!(
        upgraded_schedule.workflow_name.as_deref(),
        Some("upgraded_scheduled_dag")
    );
    assert_eq!(upgraded_schedule.queue_name.as_deref(), Some("dag-workers"));
    assert_eq!(
        upgraded_schedule.workflow_input,
        Some(json!({ "source": "upgrade" }))
    );
}

#[tokio::test]
async fn register_workflow_schedules_merges_split_legacy_dag_rows_before_upgrade() {
    let (database_url, _container) = setup_test_database_url().await;
    let dag_name = "split_legacy_dag";
    let classic_dag = compile_dag_catalog(vec![DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(60))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("classic-queue"),
        builder: build_interval_pipeline_dag,
        workflow_handler: None,

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }])
    .expect("classic scheduled DAG should compile");
    register_test_schedules(
        &database_url,
        &classic_dag,
        "failed to connect for classic schedule registration",
    )
    .await;

    let legacy_workflow_only_row = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: None,
        schedule: Schedule::Interval(Duration::from_secs(300)),
        input: json!({ "source": "legacy-workflow-only" }),
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "legacy-workflow-queue".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let unified_dag_row = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(120)),
        input: json!({ "source": "unified" }),
        catchup: true,
        max_active_runs: 2,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for split legacy schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&legacy_workflow_only_row))
        .await
        .expect("legacy workflow-only row should register beside the classic DAG row");
    assert_eq!(
        count_schedule_rows_for_name_from_url(&database_url, dag_name).await,
        2,
        "test setup should contain both the classic dag_name row and the workflow-only row"
    );

    register_workflow_schedules(&mut conn, std::slice::from_ref(&unified_dag_row))
        .await
        .expect("unified DAG registration should merge split legacy rows before updating");

    assert_eq!(
        count_schedule_rows_for_name_from_url(&database_url, dag_name).await,
        1,
        "upgrade should leave one canonical DAG-backed workflow schedule row"
    );
    let schedule = load_schedule_from_url(&database_url, dag_name).await;
    assert_eq!(schedule.dag_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.workflow_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.max_active_runs, 2);
    assert!(schedule.catchup);
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
    assert_eq!(
        schedule.workflow_input,
        Some(json!({ "source": "unified" }))
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn register_workflow_schedules_preserves_pause_metadata_when_merging_split_legacy_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let dag_name = "split_paused_legacy_dag";
    let paused_at = chrono::DateTime::parse_from_rfc3339("2026-05-14T02:00:00.654321Z")
        .expect("fixed pause timestamp should parse")
        .with_timezone(&chrono::Utc);
    let classic_dag = compile_dag_catalog(vec![DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(60))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("classic-queue"),
        builder: build_interval_pipeline_dag,
        workflow_handler: None,

        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,

        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
    }])
    .expect("classic scheduled DAG should compile");
    register_test_schedules(
        &database_url,
        &classic_dag,
        "failed to connect for classic schedule registration",
    )
    .await;

    let legacy_workflow_only_row = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: None,
        schedule: Schedule::Interval(Duration::from_secs(300)),
        input: json!({ "source": "legacy-workflow-only" }),
        catchup: false,
        max_active_runs: 1,
        paused: true,
        queue_name: "legacy-workflow-queue".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let unified_dag_row = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(120)),
        input: json!({ "source": "unified" }),
        catchup: true,
        max_active_runs: 2,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for split paused legacy schedule registration");
    register_workflow_schedules(&mut conn, std::slice::from_ref(&legacy_workflow_only_row))
        .await
        .expect("legacy paused workflow-only row should register beside the classic DAG row");
    let workflow_only = load_workflow_only_schedule_from_url_optional(&database_url, dag_name)
        .await
        .expect("workflow-only legacy row should exist");
    diesel::update(harvest_schedules::table.find(workflow_only.id))
        .set((
            harvest_schedules::paused_at.eq(Some(paused_at)),
            harvest_schedules::paused_by.eq(Some("ops-team")),
            harvest_schedules::pause_reason.eq(Some("incident-response")),
        ))
        .execute(&mut conn)
        .await
        .expect("failed to attach pause metadata to workflow-only row");

    register_workflow_schedules(&mut conn, std::slice::from_ref(&unified_dag_row))
        .await
        .expect("unified DAG registration should merge pause metadata before deleting split rows");

    assert_eq!(
        count_schedule_rows_for_name_from_url(&database_url, dag_name).await,
        1,
        "upgrade should leave one canonical DAG-backed workflow schedule row"
    );
    let schedule = load_schedule_from_url(&database_url, dag_name).await;
    assert!(schedule.is_paused);
    assert_eq!(schedule.paused_at, Some(paused_at));
    assert_eq!(schedule.paused_by.as_deref(), Some("ops-team"));
    assert_eq!(schedule.pause_reason.as_deref(), Some("incident-response"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scheduler_tick_dispatches_scheduled_unified_dag_on_dag_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let dag_name = find_dag_name_for_shard(&router, "scheduled_unified", ShardId::new(1));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("scheduled unified dag should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let harvest_pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let workflow_schedules = Arc::new(vec![workflow_schedule]);

    tick_once_sharded(
        harvest_pool.sharded_pool().clone(),
        router.clone(),
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::clone(&workflow_schedules),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("initial sharded tick should register the schedule on the DAG shard");

    assert!(
        load_latest_dag_run_from_url(&shard0_url, dag_name)
            .await
            .is_none(),
        "registration-only tick must not create a default-shard DAG execution"
    );
    let schedule = load_schedule_from_url(&shard1_url, dag_name).await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&shard1_url)
            .await
            .expect("failed to connect to shard 1 for forcing due schedule");
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force unified DAG workflow schedule due");
    }

    tick_once_sharded(
        harvest_pool.sharded_pool().clone(),
        router.clone(),
        registry,
        dag_catalog,
        workflow_schedules,
        SchedulerMonitor::offline(),
    )
    .await
    .expect("sharded tick should dispatch the due unified DAG schedule");

    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard1_url, dag_name).await,
        1,
        "scheduled unified DAG runs must be inserted on the DAG-owning shard"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard0_url, dag_name).await,
        0,
        "scheduled unified DAG runs must not be inserted on the default shard"
    );
    let execution = load_latest_workflow_execution_by_name_from_url(&shard1_url, dag_name)
        .await
        .expect("scheduled unified DAG execution should exist on shard 1");
    assert_eq!(
        ExecutionId::from_uuid(execution.id).shard(),
        ShardId::new(1)
    );
    assert_eq!(execution.queue_name, "dag-workers");
}

#[tokio::test]
async fn scheduler_tick_removes_stale_unified_dag_schedule_from_old_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let dag_name = find_dag_name_for_shard(&router, "moved_scheduled_unified", ShardId::new(1));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("scheduled unified dag should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let harvest_pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let workflow_schedules = Arc::new(vec![workflow_schedule.clone()]);

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&shard0_url)
            .await
            .expect("failed to connect to shard 0 for stale schedule registration");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to seed stale default-shard unified DAG schedule");
        let stale = load_schedule_from_url(&shard0_url, dag_name).await;
        diesel::update(harvest_schedules::table.find(stale.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force stale unified DAG schedule due");
    }

    tick_once_sharded(
        harvest_pool.sharded_pool().clone(),
        router.clone(),
        registry,
        dag_catalog,
        workflow_schedules,
        SchedulerMonitor::offline(),
    )
    .await
    .expect("sharded tick should clean stale rows before ticking schedules");

    assert!(
        load_schedule_from_url_optional(&shard0_url, dag_name)
            .await
            .is_none(),
        "stale default-shard row should be removed instead of dispatched"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard0_url, dag_name).await,
        0,
        "stale default-shard schedule must not dispatch a duplicate DAG execution"
    );
    let schedule = load_schedule_from_url(&shard1_url, dag_name).await;
    assert_eq!(schedule.workflow_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scheduler_tick_removes_legacy_workflow_only_dag_schedule_from_old_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let dag_name = find_dag_name_for_shard(
        &router,
        "moved_legacy_workflow_only_unified",
        ShardId::new(1),
    );
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("scheduled unified dag should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let harvest_pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let workflow_schedules = Arc::new(vec![workflow_schedule.clone()]);

    {
        let legacy_schedule = WorkflowSchedule {
            workflow_name: dag_name.to_string(),
            dag_name: None,
            schedule: Schedule::Interval(Duration::from_secs(60)),
            input: Value::Null,
            catchup: false,
            max_active_runs: 1,
            paused: false,
            queue_name: "dag-workers".to_string(),
            jitter: Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,
            execution_timeout: None,
            calendar: None,
            skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
            consecutive_failure_limit: None,
            end_at: None,
            max_runs: None,
            catchup_policy: None,
            retry_policy: None,
        };
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&shard0_url)
            .await
            .expect("failed to connect to shard 0 for legacy workflow-only schedule");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&legacy_schedule))
            .await
            .expect("failed to seed legacy default-shard workflow-only DAG schedule");
        let stale = load_workflow_only_schedule_from_url_optional(&shard0_url, dag_name)
            .await
            .expect("legacy workflow-only schedule should exist on shard 0");
        diesel::update(harvest_schedules::table.find(stale.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force legacy workflow-only DAG schedule due");
    }

    tick_once_sharded(
        harvest_pool.sharded_pool().clone(),
        router.clone(),
        registry,
        dag_catalog,
        workflow_schedules,
        SchedulerMonitor::offline(),
    )
    .await
    .expect("sharded tick should clean legacy workflow-only rows before ticking schedules");

    assert!(
        load_workflow_only_schedule_from_url_optional(&shard0_url, dag_name)
            .await
            .is_none(),
        "legacy default-shard workflow-only row should be removed instead of dispatched"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard0_url, dag_name).await,
        0,
        "legacy default-shard workflow-only schedule must not dispatch a duplicate DAG execution"
    );
    let schedule = load_schedule_from_url(&shard1_url, dag_name).await;
    assert_eq!(schedule.workflow_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scheduler_tick_removes_stale_classic_dag_schedule_from_old_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let router = two_shard_router();
    let dag_name = find_dag_name_for_shard(&router, "moved_classic_dag", ShardId::new(1));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("dag-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: Some(approval_workflow),

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("scheduled unified dag should compile"),
    );
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    let harvest_pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));
    let workflow_schedules = Arc::new(vec![workflow_schedule]);

    {
        let classic_catalog = compile_dag_catalog(vec![DagInfo {
            name: dag_name,
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("classic-workers"),
            builder: build_interval_pipeline_dag,
            workflow_handler: None,

            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100u32,

            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }])
        .expect("classic DAG schedule should compile");
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&shard0_url)
            .await
            .expect("failed to connect to shard 0 for stale classic DAG schedule");
        register_schedules(&mut conn, &classic_catalog)
            .await
            .expect("failed to seed stale classic DAG schedule on old shard");
        let stale = load_schedule_from_url(&shard0_url, dag_name).await;
        assert!(
            stale.workflow_name.is_none(),
            "seeded row must model the old classic DAG-only schedule shape"
        );
        diesel::update(harvest_schedules::table.find(stale.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force stale classic DAG schedule due");
    }

    tick_once_sharded(
        harvest_pool.sharded_pool().clone(),
        router.clone(),
        registry,
        dag_catalog,
        workflow_schedules,
        SchedulerMonitor::offline(),
    )
    .await
    .expect("sharded tick should clean stale classic DAG rows before ticking schedules");

    assert!(
        load_schedule_from_url_optional(&shard0_url, dag_name)
            .await
            .is_none(),
        "classic DAG-only row on a non-owner shard should be removed"
    );
    assert_eq!(
        count_workflow_executions_by_name_from_url(&shard0_url, dag_name).await,
        0,
        "stale classic DAG schedule must not dispatch from the old shard"
    );
    let schedule = load_schedule_from_url(&shard1_url, dag_name).await;
    assert_eq!(schedule.workflow_name.as_deref(), Some(dag_name));
    assert_eq!(schedule.queue_name.as_deref(), Some("dag-workers"));
}

#[tokio::test]
async fn scheduler_tick_does_not_dispatch_removed_dag_schedule_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "removed_unified_dag";
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for removed DAG schedule seed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to seed DAG-backed workflow schedule");
        let schedule = load_schedule_from_url(&database_url, dag_name).await;
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force removed DAG schedule due");
    }

    tick_once(
        pool.clone(),
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should skip removed DAG-managed schedule rows");

    assert_eq!(
        count_workflow_executions_by_name_from_url(&database_url, dag_name).await,
        0,
        "DAG-marked schedules whose DAG is no longer registered must not dispatch workflows"
    );
    let maybe_schedule = load_schedule_from_url_optional(&database_url, dag_name).await;
    if let Some(schedule) = maybe_schedule {
        assert!(
            schedule.next_run_at.is_none(),
            "kept removed-DAG rows must be disabled so future ticks do not repeatedly dispatch"
        );
    }
}

#[tokio::test]
async fn register_schedules_recomputes_next_run_when_schedule_changes() {
    let (database_url, _container) = setup_test_database_url().await;

    let interval_catalog = compile_dag_catalog(vec![classic_interval_pipeline_info()])
        .expect("interval pipeline dag should compile");
    register_test_schedules(
        &database_url,
        &interval_catalog,
        "failed to connect for interval schedule registration",
    )
    .await;

    let schedule = load_schedule_from_url(&database_url, "interval_pipeline").await;
    assert!(
        schedule.next_run_at.is_some(),
        "interval schedule should begin with a queued next_run_at"
    );

    let manual_catalog = compile_dag_catalog(vec![manual_interval_pipeline_info()])
        .expect("manual interval pipeline dag should compile");
    register_test_schedules(
        &database_url,
        &manual_catalog,
        "failed to connect for manual schedule registration",
    )
    .await;

    let updated = load_schedule_from_url(&database_url, "interval_pipeline").await;
    assert_eq!(updated.schedule_expr.as_deref(), Some("manual"));
    assert!(
        updated.next_run_at.is_none(),
        "changing an automatic schedule to manual should clear stale next_run_at"
    );
}

// ── helpers for pause/resume metadata tests ──────────────────────────────────

async fn post_json_with_actor(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
    actor: &str,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "application/json")
                .header("x-harvest-actor", actor)
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn seed_workflow_schedule_and_get_id(database_url: &str, workflow_name: &str) -> uuid::Uuid {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for schedule seeding");
    let ws = WorkflowSchedule::new(workflow_name, Schedule::Interval(Duration::from_secs(3600)));
    register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
        .await
        .expect("failed to register workflow schedule");
    harvest_schedules::table
        .filter(harvest_schedules::workflow_name.eq(workflow_name))
        .select(harvest_schedules::id)
        .first::<uuid::Uuid>(&mut conn)
        .await
        .expect("seeded schedule should be queryable")
}

fn find_schedule_in_list(list: &Value, id: uuid::Uuid) -> Value {
    list.as_array()
        .expect("schedule list must be a JSON array")
        .iter()
        .find(|s| s["id"].as_str() == Some(&id.to_string()))
        .cloned()
        .expect("schedule must appear in list")
}

// ── issue #229: pause/resume metadata (reason, paused_at, paused_by) ─────────

#[tokio::test]
async fn schedule_pause_with_reason_records_pause_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "pause_metadata_wf").await;

    let (status, ack) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/pause"),
        json!({ "reason": "incident-response" }),
        "ops-team",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "pause must return 200");
    assert_eq!(ack["ok"], true, "pause ack must be {{ok: true}}");

    let (list_status, list) = get_json(&app, "/admin/schedules").await;
    assert_eq!(list_status, StatusCode::OK);
    let entry = find_schedule_in_list(&list, id);

    assert_eq!(entry["is_paused"], true, "schedule must be paused");
    assert!(
        entry["paused_at"].is_string(),
        "paused_at must be a non-null timestamp string after pause; got: {}",
        entry["paused_at"]
    );
    assert_eq!(
        entry["paused_by"], "ops-team",
        "paused_by must record the actor from X-Harvest-Actor"
    );
    assert_eq!(
        entry["pause_reason"], "incident-response",
        "pause_reason must store the reason from the request body"
    );
}

#[tokio::test]
async fn schedule_pause_idempotent_does_not_overwrite_paused_at() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "pause_idempotent_wf").await;

    // First pause — alice owns it
    let (s1, _) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/pause"),
        json!({ "reason": "first pause" }),
        "alice",
    )
    .await;
    assert_eq!(s1, StatusCode::OK);

    let (_, list1) = get_json(&app, "/admin/schedules").await;
    let entry1 = find_schedule_in_list(&list1, id);
    let original_paused_at = entry1["paused_at"].clone();
    let original_paused_by = entry1["paused_by"].clone();
    assert!(
        original_paused_at.is_string(),
        "paused_at must be set after first pause"
    );
    assert_eq!(original_paused_by, "alice");

    // Wait so clock would advance if the timestamp were overwritten
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second pause — bob tries to take over
    let (s2, _) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/pause"),
        json!({ "reason": "second pause" }),
        "bob",
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "second pause must return 200 (idempotent)"
    );

    let (_, list2) = get_json(&app, "/admin/schedules").await;
    let entry2 = find_schedule_in_list(&list2, id);

    assert_eq!(
        entry2["paused_at"], original_paused_at,
        "paused_at must not change on a second pause (idempotency)"
    );
    assert_eq!(
        entry2["paused_by"], original_paused_by,
        "paused_by must not change on a second pause (idempotency)"
    );
}

#[tokio::test]
async fn schedule_resume_clears_pause_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "resume_clears_wf").await;

    // Pause first
    let (pause_status, _) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/pause"),
        json!({ "reason": "clearing test" }),
        "ops",
    )
    .await;
    assert_eq!(pause_status, StatusCode::OK);

    // Resume
    let (resume_status, resume_ack) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/resume"),
        json!({}),
        "ops",
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK, "resume must return 200");
    assert_eq!(resume_ack["ok"], true);

    let (_, list) = get_json(&app, "/admin/schedules").await;
    let entry = find_schedule_in_list(&list, id);

    assert_eq!(
        entry["is_paused"], false,
        "schedule must be active after resume"
    );
    assert!(
        entry["paused_at"].is_null(),
        "paused_at must be cleared to null after resume; got: {}",
        entry["paused_at"]
    );
    assert!(
        entry["paused_by"].is_null(),
        "paused_by must be cleared to null after resume; got: {}",
        entry["paused_by"]
    );
    assert!(
        entry["pause_reason"].is_null(),
        "pause_reason must be cleared to null after resume; got: {}",
        entry["pause_reason"]
    );
}

#[tokio::test]
async fn schedule_resume_idempotent_when_schedule_is_not_paused() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "resume_idempotent_wf").await;

    // First resume on an already-active schedule
    let (s1, ack1) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/resume"),
        json!({}),
        "ops",
    )
    .await;
    assert_eq!(
        s1,
        StatusCode::OK,
        "resume on non-paused schedule must return 200"
    );
    assert_eq!(ack1["ok"], true);

    // Second resume — also idempotent
    let (s2, ack2) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/resume"),
        json!({}),
        "ops",
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "second resume must also return 200");
    assert_eq!(ack2["ok"], true);
}

#[tokio::test]
async fn get_schedule_by_id_returns_entry_with_pause_fields() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "get_by_id_wf").await;

    // GET before pause: pause fields are null
    let (status, entry) = get_json(&app, format!("/admin/schedules/{id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET /admin/schedules/:id must return 200"
    );
    assert_eq!(entry["id"].as_str(), Some(id.to_string().as_str()));
    assert_eq!(entry["is_paused"], false);
    assert!(
        entry["paused_at"].is_null(),
        "paused_at must be null before any pause"
    );
    assert!(
        entry["paused_by"].is_null(),
        "paused_by must be null before any pause"
    );
    assert!(
        entry["pause_reason"].is_null(),
        "pause_reason must be null before any pause"
    );

    // Pause and verify via GET /admin/schedules/{id}
    post_json_with_actor(
        &app,
        format!("/admin/schedules/{id}/pause"),
        json!({ "reason": "testing get-by-id" }),
        "ops-bot",
    )
    .await;

    let (status2, paused_entry) = get_json(&app, format!("/admin/schedules/{id}")).await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(paused_entry["is_paused"], true);
    assert!(
        paused_entry["paused_at"].is_string(),
        "paused_at must be set"
    );
    assert_eq!(paused_entry["paused_by"], "ops-bot");
    assert_eq!(paused_entry["pause_reason"], "testing get-by-id");

    // 404 for unknown id
    let unknown = uuid::Uuid::new_v4();
    let (not_found_status, _) = get_json(&app, format!("/admin/schedules/{unknown}")).await;
    assert_eq!(
        not_found_status,
        StatusCode::NOT_FOUND,
        "unknown id must return 404"
    );
}

#[tokio::test]
async fn get_schedule_decisions_api_endpoints() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let id = seed_workflow_schedule_and_get_id(&database_url, "decision_test_wf").await;

    // Connect to database and insert some decisions
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let occurred_at = chrono::Utc::now();
    let next_fire_at = occurred_at + chrono::Duration::hours(1);

    autumn_harvest::schedule_decision::record_decision_graceful(
        &mut conn,
        None,
        Some(id),
        "decision_test_wf",
        "workflow",
        "fired",
        "fired_ok",
        Some(serde_json::json!({ "run_id": "run-abc-123" })),
        occurred_at,
        next_fire_at,
        0,
    )
    .await;

    // 1. Test fleet-wide decisions endpoint
    let (status, fleet_decisions) = get_json(&app, "/admin/schedules/decisions").await;
    assert_eq!(status, StatusCode::OK);
    let list = fleet_decisions.as_array().expect("array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["schedule_name"], "decision_test_wf");
    assert_eq!(list[0]["decision"], "fired");
    assert_eq!(list[0]["reason_code"], "fired_ok");

    // 2. Test single schedule decisions endpoint
    let (status2, single_decisions) =
        get_json(&app, format!("/admin/schedules/{id}/decisions")).await;
    assert_eq!(status2, StatusCode::OK);
    let list2 = single_decisions.as_array().expect("array");
    assert_eq!(list2.len(), 1);
    assert_eq!(list2[0]["schedule_id"], id.to_string());
    assert_eq!(list2[0]["decision"], "fired");
    assert_eq!(list2[0]["reason_code"], "fired_ok");
}

#[tokio::test]
async fn scheduler_tick_preserves_dag_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "metadata_dag";
    let workflow_schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "dag-workers".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };

    let dag_info = DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Interval(Duration::from_secs(60))),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: Some("ops-team"),
        runbook_url: Some("http://ops-runbook"),
        severity: Some("sev2"),
        mcp: false,
    };
    let dag_catalog = Arc::new(compile_dag_catalog(vec![dag_info]).expect("dag compiles"));

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for DAG schedule seed");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("failed to seed DAG-backed workflow schedule");
        let schedule = load_schedule_from_url(&database_url, dag_name).await;
        diesel::update(harvest_schedules::table.find(schedule.id))
            .set(
                harvest_schedules::next_run_at
                    .eq(Some(chrono::Utc::now() - chrono::Duration::seconds(1))),
            )
            .execute(&mut conn)
            .await
            .expect("failed to force DAG schedule due");
    }

    tick_once(
        pool.clone(),
        Arc::new(HandlerRegistry::new(
            vec![workflow_info_named(dag_name)],
            vec![],
        )),
        dag_catalog,
        Arc::new(vec![workflow_schedule]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should dispatch DAG schedule");

    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("scheduled execution should exist");
    assert_eq!(execution.owner.as_deref(), Some("ops-team"));
    assert_eq!(execution.runbook_url.as_deref(), Some("http://ops-runbook"));
    assert_eq!(execution.severity.as_deref(), Some("sev2"));
}

#[tokio::test]
async fn api_trigger_preserves_dag_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "api_metadata_dag";

    let dag_info = DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: build_interval_pipeline_dag,
        workflow_handler: Some(approval_workflow),
        jitter: ::std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: Some("dev-team"),
        runbook_url: Some("http://dev-runbook"),
        severity: Some("sev1"),
        mcp: false,
    };
    let dag_catalog = Arc::new(compile_dag_catalog(vec![dag_info]).expect("dag compiles"));
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(dag_name)],
        vec![],
    ));

    let schedule_id = seed_workflow_schedule_and_get_id(&database_url, dag_name).await;
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("connect");
        diesel::update(harvest_schedules::table.find(schedule_id))
            .set((
                harvest_schedules::dag_name.eq(Some(dag_name.to_string())),
                harvest_schedules::workflow_name.eq(None::<String>),
            ))
            .execute(&mut conn)
            .await
            .expect("update schedule");
    }

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let (status, ack) = post_json_with_actor(
        &app,
        format!("/admin/schedules/{schedule_id}/trigger"),
        json!({}),
        "ops-team",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ack["outcome"], "fired");

    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("triggered execution should exist");
    assert_eq!(execution.owner.as_deref(), Some("dev-team"));
    assert_eq!(execution.runbook_url.as_deref(), Some("http://dev-runbook"));
    assert_eq!(execution.severity.as_deref(), Some("sev1"));
}

// ── Overdue-schedule read fields (issue #696) ────────────────────────────────

/// Prefer a shared migrated Postgres via `HARVEST_TEST_DATABASE_URL` (Docker-free
/// local runs); otherwise start a fresh testcontainers Postgres 16.
async fn overdue_read_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let (url, container) = setup_test_database_url().await;
    (url, Some(container))
}

/// Insert a workflow schedule directly with an explicit `next_run_at`.
async fn insert_overdue_test_schedule(
    database_url: &str,
    wf_name: &str,
    next_run_at: chrono::DateTime<chrono::Utc>,
    is_paused: bool,
) -> uuid::Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect");
    // Isolate this schedule name on a possibly-shared DB.
    diesel::delete(harvest_schedules::table.filter(dsl::workflow_name.eq(wf_name)))
        .execute(&mut conn)
        .await
        .expect("clear prior schedule");
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(is_paused),
            dsl::next_run_at.eq(next_run_at),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(&mut conn)
        .await
        .expect("insert schedule");
    id
}

/// AC1: `GET /admin/schedules` and `/{id}` report `overdue` + `overdue_by_secs`
/// per schedule, computed from the schedule's own `next_run_at` and cadence.
#[tokio::test]
async fn schedule_read_reports_overdue_fields() {
    let (database_url, _container) = overdue_read_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let now = chrono::Utc::now();
    // interval:60 => grace = 61s. 300s past its slot => overdue.
    let wedged_id = insert_overdue_test_schedule(
        &database_url,
        "overdue_read_wedged",
        now - chrono::Duration::seconds(300),
        false,
    )
    .await;
    // Just fired => healthy.
    insert_overdue_test_schedule(
        &database_url,
        "overdue_read_healthy",
        now - chrono::Duration::seconds(10),
        false,
    )
    .await;

    // List endpoint — a complete fan-out returns a bare array.
    let (status, body) = get_json(&app, "/admin/schedules").await;
    assert_eq!(status, StatusCode::OK);
    let entries = body.as_array().expect("schedules list is an array");
    let wedged = entries
        .iter()
        .find(|e| e["name"] == "overdue_read_wedged")
        .expect("wedged schedule present in list");
    assert_eq!(
        wedged["overdue"], true,
        "wedged schedule must report overdue=true in the list"
    );
    let by = wedged["overdue_by_secs"]
        .as_i64()
        .expect("overdue_by_secs is an integer for an overdue schedule");
    assert!(by > 0, "overdue_by_secs must be positive, got {by}");

    let healthy = entries
        .iter()
        .find(|e| e["name"] == "overdue_read_healthy")
        .expect("healthy schedule present in list");
    assert_eq!(healthy["overdue"], false);
    assert!(
        healthy["overdue_by_secs"].is_null(),
        "a non-overdue schedule reports null overdue_by_secs"
    );

    // Single-schedule endpoint.
    let (status, single) = get_json(&app, format!("/admin/schedules/{wedged_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(single["overdue"], true);
    assert!(single["overdue_by_secs"].as_i64().unwrap_or(0) > 0);
}

/// Insert a `RUNNING` workflow execution for `wf_name` with a unique
/// `workflow_id`, so the schedule's shard-local capacity basis
/// (`scheduler::schedule_running_basis` = `RUNNING`/`PAUSED` count) counts it.
async fn insert_running_execution_for(database_url: &str, wf_name: &str) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = uuid::Uuid::new_v4().to_string();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for running execution seed");
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&autumn_harvest::models::NewWorkflowExecution {
            continued_from_exec_id: None,
            first_exec_id: None,
            id: exec_id.as_uuid(),
            workflow_name: wf_name,
            workflow_id: &workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: 0,
            input: Value::Null,
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            completion_callbacks: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(&mut conn)
        .await
        .expect("insert running execution");
}

/// AC (issue #696, Codex round 2): the create/upsert HTTP **response** must
/// report the same tick-exact, shard-local `at_capacity` as the list/get reads
/// and the `harvest.schedule.overdue` gauge. A same-cadence re-register preserves
/// the existing `next_run_at` (`apply_workflow_schedule_update` recomputes it only
/// on a cadence change), so a wedged-in-the-past `next_run_at` survives the
/// re-register. When the schedule is `Skip` + catchup AND at capacity, the tick
/// legitimately holds `next_run_at` in the past (a deferred fire, not a wedge), so
/// it must report `overdue == false`. Before the fix the response hardcoded
/// `at_capacity = false`, so it reported `overdue == true` — disagreeing with the
/// read == gauge == tick invariant.
#[tokio::test]
async fn schedule_create_response_at_capacity_is_not_overdue() {
    let (database_url, _container) = overdue_read_database_url().await;
    let pool = build_test_pool(&database_url);
    let name = "overdue_upsert_at_capacity_wf";

    let workflow_schedule = WorkflowSchedule {
        workflow_name: name.to_string(),
        dag_name: None,
        schedule: Schedule::Interval(Duration::from_secs(60)),
        input: Value::Null,
        catchup: true,
        max_active_runs: 1,
        paused: false,
        queue_name: "default".to_string(),
        jitter: Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
    };
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("connect for initial register");
        // Isolate the name on a possibly-shared DB.
        diesel::delete(harvest_schedules::table.filter(harvest_schedules::workflow_name.eq(name)))
            .execute(&mut conn)
            .await
            .expect("clear prior schedule");
        diesel::delete(
            harvest_workflow_executions::table
                .filter(harvest_workflow_executions::workflow_name.eq(name)),
        )
        .execute(&mut conn)
        .await
        .expect("clear prior executions");
        register_workflow_schedules(&mut conn, std::slice::from_ref(&workflow_schedule))
            .await
            .expect("register schedule");
        // Simulate a wedge: force next_run_at 300s into the past (> 61s grace for
        // interval:60). A same-cadence re-register preserves this value.
        diesel::update(harvest_schedules::table.filter(harvest_schedules::workflow_name.eq(name)))
            .set(
                harvest_schedules::next_run_at
                    .eq(chrono::Utc::now() - chrono::Duration::seconds(300)),
            )
            .execute(&mut conn)
            .await
            .expect("wedge next_run_at");
    }
    // At capacity: one RUNNING execution (>= max_active_runs = 1).
    insert_running_execution_for(&database_url, name).await;

    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info_named(name)],
        vec![],
    ));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(compile_dag_catalog(vec![]).expect("empty DAG catalog should compile")),
        Arc::new(vec![workflow_schedule]),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_api_router(api_state).with_state(test_app_state(pool));

    // Re-register via the create/upsert route with the SAME cadence, Skip+catchup.
    // Same cadence => next_run_at preserved (still 300s in the past).
    let (status, body) = post_json(
        &app,
        "/admin/schedules/workflow",
        json!({
            "workflow_name": name,
            "schedule_expr": "interval:60",
            "queue_name": "default",
            "catchup": true,
            "overlap_policy": "skip",
            "max_active_runs": 1
        }),
    )
    .await;

    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "create/upsert should succeed, got {status}: {body}"
    );
    assert_eq!(
        body["overdue"], false,
        "an at-capacity Skip+catchup schedule with a preserved past next_run_at must report \
         overdue=false in the create/upsert response (matching read == gauge == tick): {body}"
    );
    assert!(
        body["overdue_by_secs"].is_null(),
        "a non-overdue schedule reports null overdue_by_secs in the response: {body}"
    );
}

/// Seed a daily-cron schedule whose `next_run_at` slot (3 days ago) sits inside an
/// explicit calendar exclusion block spanning `[today-3 .. today+2]`, so a
/// `run_next_business_day` policy rebases the effective fire to `today+3` (future).
/// Returns `(deferred_id, control_id)`; the control has no calendar (same past
/// slot). Dates are computed relative to the real `Utc::now()` (the read path uses
/// the real clock), so the deferral is future regardless of which weekday CI runs.
async fn seed_calendar_deferred_read_schedules(database_url: &str) -> (uuid::Uuid, uuid::Uuid) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect");
    let cal = "cal_read_deferred";
    let deferred = "overdue_read_cal_deferred";
    let control = "overdue_read_cal_control";

    // Isolate on a possibly-shared DB.
    for wf in [deferred, control] {
        diesel::delete(harvest_schedules::table.filter(dsl::workflow_name.eq(wf)))
            .execute(&mut conn)
            .await
            .expect("clear prior schedule");
    }
    diesel::sql_query(format!(
        "DELETE FROM harvest_calendar_exclusions WHERE calendar_name = '{cal}'"
    ))
    .execute(&mut conn)
    .await
    .expect("clear prior exclusions");
    diesel::sql_query(format!(
        "DELETE FROM harvest_calendars WHERE name = '{cal}'"
    ))
    .execute(&mut conn)
    .await
    .expect("clear prior calendar");

    // Parent calendar row (FK target for both the exclusions and the schedule).
    diesel::sql_query(format!(
        "INSERT INTO harvest_calendars (id, name, built_in) VALUES (gen_random_uuid(), '{cal}', false)"
    ))
    .execute(&mut conn)
    .await
    .expect("insert calendar");

    let today = chrono::Utc::now().date_naive();
    let slot_date = today - chrono::Duration::days(3);
    // Exclude [today-3 .. today+2] (6 days) so the forward scan from the slot lands
    // on today+3 — comfortably in the future relative to the mid-run `now`.
    for offset in -3..=2 {
        let d = today + chrono::Duration::days(offset);
        diesel::sql_query(format!(
            "INSERT INTO harvest_calendar_exclusions (id, calendar_name, excluded_date) \
             VALUES (gen_random_uuid(), '{cal}', DATE '{d}')"
        ))
        .execute(&mut conn)
        .await
        .expect("insert exclusion");
    }

    // next_run_at = slot_date at midnight (a daily-cron slot 3 days in the past).
    let next_run_at = slot_date.and_hms_opt(0, 0, 0).expect("midnight").and_utc();

    let mut deferred_id = uuid::Uuid::nil();
    let mut control_id = uuid::Uuid::nil();
    for (wf, cal_name, skip_policy, out) in [
        (
            deferred,
            Some(cal),
            "run_next_business_day",
            &mut deferred_id,
        ),
        (control, None, "skip", &mut control_id),
    ] {
        let id = uuid::Uuid::new_v4();
        diesel::insert_into(harvest_schedules::table)
            .values((
                dsl::id.eq(id),
                dsl::workflow_name.eq(wf),
                dsl::schedule_expr.eq("cron:0 0 * * *"),
                dsl::timezone.eq("UTC"),
                dsl::catchup.eq(false),
                dsl::max_active_runs.eq(10),
                dsl::is_paused.eq(false),
                dsl::next_run_at.eq(next_run_at),
                dsl::jitter_secs.eq(0_i64),
                dsl::overlap_policy.eq("skip"),
                dsl::buffered_runs.eq(serde_json::json!([])),
                dsl::buffer_all_max.eq(100),
                dsl::skip_policy.eq(skip_policy),
                dsl::calendar_name.eq(cal_name),
            ))
            .execute(&mut conn)
            .await
            .expect("insert schedule");
        *out = id;
    }
    (deferred_id, control_id)
}

/// Codex round 3: `GET /admin/schedules/{id}` must honor the tick's calendar
/// deferral. A `run_next_business_day` schedule whose slot fell inside an
/// exclusion block, rebased to a FUTURE business day, reports `overdue: false`;
/// the same past slot WITHOUT a calendar reports `overdue: true` (control),
/// proving the calendar resolution is what suppresses the false positive.
#[tokio::test]
async fn schedule_read_honors_calendar_deferred_fire() {
    let (database_url, _container) = overdue_read_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let (deferred_id, control_id) = seed_calendar_deferred_read_schedules(&database_url).await;

    let (status, deferred) = get_json(&app, format!("/admin/schedules/{deferred_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deferred["overdue"], false,
        "a calendar-deferred (future business-day) schedule must report overdue=false: {deferred}"
    );
    assert!(
        deferred["overdue_by_secs"].is_null(),
        "a non-overdue schedule reports null overdue_by_secs: {deferred}"
    );

    let (status, control) = get_json(&app, format!("/admin/schedules/{control_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        control["overdue"], true,
        "the same past slot WITHOUT a calendar must still report overdue=true (control): {control}"
    );
}
