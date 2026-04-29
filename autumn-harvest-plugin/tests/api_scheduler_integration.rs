use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dag::DagBuilder;
use autumn_harvest::info::{ActivityInfo, DagInfo, WorkflowInfo};
use autumn_harvest::models::{
    DagRun, HarvestSchedule, NewDagRun, TaskQueueItem, WorkflowExecution,
};
use autumn_harvest::policy::Schedule;
use autumn_harvest::scheduler::{
    DagCatalog, SchedulerMonitor, compile_dag_catalog, register_schedules, tick_once,
};
use autumn_harvest::schema::{
    harvest_dag_runs, harvest_dead_letters, harvest_schedules, harvest_task_queue,
    harvest_workflow_executions,
};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ActivityContext, RetentionConfig, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution,
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
);
type HarvestApiApp = axum::Router;

#[derive(diesel::QueryableByName)]
struct CountByName {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
        conn.batch_execute(INIT_SQL)
            .await
            .expect("failed to apply harvest migrations to shard database");
    }

    ((shard0_url, shard1_url), container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
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
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "test-worker".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);

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
            name: "approval_workflow",
            module: "tests",
            handler: approval_workflow,
        }],
        vec![],
    ))
}

fn approval_and_timer_signal_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            WorkflowInfo {
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
            },
            WorkflowInfo {
                name: "timer_then_signal_workflow",
                module: "tests",
                handler: timer_then_signal_workflow,
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
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        handler: record_activity,
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
        },
    )
    .await
    .expect("workflow insert should succeed");
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
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to shard for dag-run seed");
    let now = chrono::Utc::now();
    let seeded_run_id = uuid::Uuid::new_v4();
    diesel::insert_into(harvest_dag_runs::table)
        .values(&NewDagRun {
            id: seeded_run_id,
            dag_name,
            workflow_exec_id: None,
            logical_date: now,
            data_interval_start: now,
            data_interval_end: now,
            conf: Some(json!({ "seeded": true })),
        })
        .execute(&mut conn)
        .await
        .expect("failed to seed dag run");
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
    api_state.install(HarvestApiRuntime::new(
        registry,
        dag_catalog,
        Some("scheduler-sharded".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
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
    let (patch_status, patch_json) = patch_json(
        app,
        format!("/dags/{dag_on_one}"),
        json!({ "paused": true }),
    )
    .await;
    assert_eq!(patch_status, StatusCode::OK);
    assert_eq!(patch_json["dag_name"], dag_on_one);
    assert_eq!(patch_json["is_paused"], true);
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
    let (trigger_status, _trigger_json) = post_json(
        app,
        format!("/dags/{dag_on_one}/trigger"),
        json!({ "conf": { "manual": true } }),
    )
    .await;
    assert_eq!(trigger_status, StatusCode::CREATED);
    assert_eq!(
        count_dag_runs_from_url(shard1_url, dag_on_one).await,
        before_trigger_count + 1,
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
    harvest_dag_runs::table
        .filter(harvest_dag_runs::dag_name.eq(dag_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dag runs")
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

async fn load_latest_dag_run_from_url(database_url: &str, dag_name: &str) -> Option<DagRun> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for dag run query");
    harvest_dag_runs::table
        .filter(harvest_dag_runs::dag_name.eq(dag_name))
        .order(harvest_dag_runs::created_at.desc())
        .select(DagRun::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to reload latest dag run")
}

async fn wait_for_workflow_state(
    database_url: &str,
    exec_id: &str,
    expected_state: &str,
) -> WorkflowExecution {
    for _ in 0..200 {
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
) -> DagRun {
    for _ in 0..100 {
        if let Some(run) = load_latest_dag_run_from_url(database_url, dag_name).await
            && run.state == expected_state
        {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("dag {dag_name} did not reach state {expected_state}");
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

    diesel::sql_query(
        "INSERT INTO harvest_dag_runs (
            id, dag_name, workflow_exec_id, state, logical_date, data_interval_start, data_interval_end, created_at, started_at, completed_at
         ) VALUES (
            gen_random_uuid(), 'retention_fixture_dag', $1, 'SUCCESS',
            NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days', NOW() - INTERVAL '9 days',
            NOW() - INTERVAL '10 days', NOW() - INTERVAL '10 days', NOW() - INTERVAL '9 days'
         )",
    )
    .bind::<diesel::sql_types::Uuid, _>(old_exec_b)
    .execute(conn)
    .await
    .expect("failed to insert fixture dag run");
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
    assert_eq!(count_execution_rows(conn, old_exec_b).await, 1);
    assert_eq!(count_execution_rows(conn, recent_exec).await, 1);
    assert_eq!(count_execution_rows(conn, inflight_exec).await, 1);

    for table in [
        "harvest_events",
        "harvest_task_queue",
        "harvest_timers",
        "harvest_signals",
        "harvest_dead_letters",
    ] {
        let count = count_child_rows(conn, table, old_exec_a).await;
        assert_eq!(count, 0, "cascade should clear {table} for {old_exec_a}");
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

fn record_activity<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
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
async fn harvest_api_duplicate_start_reuses_existing_execution() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
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
async fn harvest_api_cancels_workflows_and_rejects_late_signals() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = approval_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
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
async fn external_runner_processes_workflows_started_via_management_api() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let web_runtime = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
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
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .expect("external web runtime should start without local ownership");

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                name: "approval_workflow",
                module: "tests",
                handler: approval_workflow,
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
        },
        HarvestRunnerResources::new(pool.clone()),
    )
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
async fn retention_janitor_deletes_only_rows_older_than_max_age_and_cascades_children() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
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
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .expect("runner with retention should start");

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
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let registry = recording_registry(Arc::clone(&log), &["extract", "transform", "notify"]);
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

    let (trigger_status, _trigger_json) = post_json(
        &app,
        "/dags/manual_pipeline/trigger",
        json!({ "conf": { "step": "extract" } }),
    )
    .await;
    assert_eq!(trigger_status, StatusCode::CREATED);

    let run = wait_for_dag_run_state(&database_url, "manual_pipeline", "SUCCESS").await;
    assert_eq!(run.dag_name, "manual_pipeline");

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

    let recorded = log.lock().expect("log mutex poisoned").clone();
    assert_eq!(recorded, vec!["extract", "transform", "notify"]);
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
        vec![],
        vec![ActivityInfo {
            name: "interval_step",
            module: "tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            handler: record_activity,
        }],
        Arc::new(state),
    ));
    let dag_catalog = Arc::new(
        compile_dag_catalog(vec![interval_pipeline_info()])
            .expect("interval pipeline dag should compile"),
    );

    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect for schedule registration");
        register_schedules(&mut conn, dag_catalog.as_ref())
            .await
            .expect("failed to register interval dag schedules");
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

    tick_once(
        pool.clone(),
        Arc::clone(&registry),
        Arc::clone(&dag_catalog),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should succeed");

    let run = wait_for_dag_run_state(&database_url, "interval_pipeline", "SUCCESS").await;
    assert_eq!(run.dag_name, "interval_pipeline");
    assert_eq!(
        log.lock().expect("log mutex poisoned").clone(),
        vec!["interval_step"]
    );
}

#[tokio::test]
async fn register_schedules_recomputes_next_run_when_schedule_changes() {
    let (database_url, _container) = setup_test_database_url().await;

    let interval_catalog = compile_dag_catalog(vec![interval_pipeline_info()])
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
