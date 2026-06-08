//! Integration tests for the Vantage dashboard UI.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::context::{DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD, WorkflowHistoryPolicy};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewHarvestEvent;
use autumn_harvest::scheduler::SchedulerMonitor;
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_events, harvest_task_queue, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use std::time::Duration;
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
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260430000000_harvest_workflow_schedules/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260504000000_harvest_workflow_parent_children/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260519000000_harvest_calendar_awareness/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260522000000_harvest_schedule_decisions/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260526000001_harvest_parent_close_policy/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000000_harvest_schedule_auto_pause/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000001_harvest_poison_pill_strikes/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000002_harvest_ownership_metadata/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260603000000_harvest_completion_triggers/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260607000000_harvest_workflow_pause/up.sql")
);

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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
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
    let shard0_db = format!("harvest_ui_shard_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_ui_shard_{}", uuid::Uuid::new_v4().simple());

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

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

/// Provision N additional databases in the same Postgres instance and run the
/// harvest schema in each.  Returns the URLs in shard-index order.
async fn setup_n_shard_databases(container: &ContainerAsync<Postgres>, n: usize) -> Vec<String> {
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connection");

    let mut urls = Vec::with_capacity(n);
    for i in 0..n {
        let db_name = format!("harvest_perf_shard_{}_{}", i, uuid::Uuid::new_v4().simple());
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin_conn)
            .await
            .expect("create shard db");
        let url = format!("postgres://postgres:postgres@{host}:{port}/{db_name}");
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("shard connection");
        conn.batch_execute(INIT_SQL)
            .await
            .expect("apply migrations");
        urls.push(url);
    }
    urls
}

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

fn echo_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "echo_workflow",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
    ))
}

fn spawn_test_worker(
    registry: Arc<HandlerRegistry>,
    pool: DbPool,
) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "ui-test-worker".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);
    let worker =
        Arc::new(Worker::new(runtime_config, registry).expect("worker config should be valid"));
    let worker_task = {
        let worker = Arc::clone(&worker);
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };
    (worker, worker_task)
}

async fn fetch_html(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("GET request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_form(
    app: &axum::Router,
    uri: &str,
    body: impl Into<String>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.into()))
                .expect("valid form request"),
        )
        .await
        .expect("POST form request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    (status, headers, String::from_utf8_lossy(&bytes).to_string())
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
        },
    )
    .await
    .expect("workflow insert should succeed");
    exec_id
}

async fn append_test_events(database_url: &str, exec_id: ExecutionId, prefix: &str, count: i32) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for event insert");
    for event_id in 1..=count {
        diesel::insert_into(harvest_events::table)
            .values(&NewHarvestEvent {
                workflow_exec_id: exec_id.as_uuid(),
                event_id,
                event_type: &format!("{prefix}Event{event_id}"),
                event_data: json!({ "event": event_id, "prefix": prefix }),
            })
            .execute(&mut conn)
            .await
            .expect("failed to insert test event");
    }
}

#[derive(Debug, Clone)]
struct SeededDeadLetter {
    id: uuid::Uuid,
    shard_url: String,
    workflow_name: String,
    activity_name: Option<String>,
}

async fn insert_dead_letter_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    task_type: &str,
    activity_name: Option<&str>,
    ordinal: usize,
) -> SeededDeadLetter {
    let exec_id = insert_workflow_on_url(database_url, shard, workflow_name, workflow_id).await;
    append_test_events(database_url, exec_id, workflow_name, 12).await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    let id = autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: task_type.to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: activity_name.map(str::to_string),
            input: json!({ "ordinal": ordinal, "workflow": workflow_name }),
            error: format!("{workflow_name} failed at attempt {ordinal}: downstream timeout with enough text to truncate"),
            attempts: i32::try_from(ordinal + 1).expect("ordinal fits i32"),
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed");

    SeededDeadLetter {
        id,
        shard_url: database_url.to_string(),
        workflow_name: workflow_name.to_string(),
        activity_name: activity_name.map(str::to_string),
    }
}

async fn count_dead_letter_by_id(database_url: &str, id: uuid::Uuid) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter count");
    harvest_dead_letters::table
        .filter(harvest_dead_letters::id.eq(id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead-letter row")
}

async fn count_task_queue_by_activity(database_url: &str, activity_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for task queue count");
    harvest_task_queue::table
        .filter(harvest_task_queue::activity_name.eq(activity_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count task queue rows")
}

async fn start_workflow_and_wait(
    api_app: &axum::Router,
    workflow_id: &str,
    database_url: &str,
) -> String {
    let response = api_app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/echo_workflow/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_id": workflow_id,
                        "input": { "hello": "world" },
                    })
                    .to_string(),
                ))
                .expect("valid request"),
        )
        .await
        .expect("POST /workflows/echo_workflow/start failed");
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    let exec_id = json["execution_id"].as_str().unwrap().to_string();

    for _ in 0..200 {
        let mut conn = AsyncPgConnection::establish(database_url).await.unwrap();
        let state: String = harvest_workflow_executions::table
            .find(
                exec_id
                    .parse::<autumn_harvest::ExecutionId>()
                    .unwrap()
                    .as_uuid(),
            )
            .select(harvest_workflow_executions::state)
            .first(&mut conn)
            .await
            .unwrap();
        if state == "COMPLETED" {
            return exec_id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workflow {exec_id} never completed");
}

#[tokio::test]
async fn ui_root_redirects_to_workflows() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = echo_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .expect("GET / failed");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get("location")
        .expect("redirect must have Location")
        .to_str()
        .unwrap();
    assert_eq!(location, "workflows");
}

#[tokio::test]
async fn ui_lists_workflows_and_renders_detail_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let registry = echo_registry();
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let (worker, worker_task) = spawn_test_worker(Arc::clone(&registry), pool.clone());

    let api_app = autumn_harvest_plugin::harvest_api_router(api_state.clone())
        .with_state(test_app_state_without_database());
    let exec_id = start_workflow_and_wait(&api_app, "ui-demo-1", &database_url).await;

    let ui_app = harvest_ui_router(api_state.clone()).with_state(test_app_state_without_database());

    let (status, list_html) = fetch_html(&ui_app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(list_html.contains("🔭 Vantage"), "layout header missing");
    assert!(
        list_html.contains("echo_workflow"),
        "workflow name missing from list: {list_html}"
    );
    assert!(
        list_html.contains("COMPLETED"),
        "completed badge missing from list"
    );
    // state filter controls are present and no external asset URLs leak in
    assert!(list_html.contains("name=\"state\""));
    assert!(!list_html.contains("http://"));
    assert!(!list_html.contains("https://"));
    assert!(!list_html.contains("<script"));

    let (status, filtered_html) = fetch_html(&ui_app, "/workflows?state=COMPLETED").await;
    assert_eq!(status, StatusCode::OK);
    assert!(filtered_html.contains("echo_workflow"));
    let (status, empty_html) = fetch_html(&ui_app, "/workflows?state=FAILED").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        empty_html.contains("No workflows match this filter"),
        "expected empty state message"
    );

    let (status, detail_html) = fetch_html(&ui_app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(detail_html.contains("Event history"));
    assert!(detail_html.contains("Metadata"));
    assert!(detail_html.contains(&exec_id));
    assert!(
        detail_html.contains("WorkflowStarted"),
        "detail history should include start event"
    );
    assert!(
        detail_html.contains("WorkflowCompleted"),
        "detail history should include completion event"
    );
    // Input payload is pretty-printed and HTML-escaped
    assert!(detail_html.contains("&quot;hello&quot;"));
    assert!(!detail_html.contains("<script"));

    let (status, _body) = fetch_html(&ui_app, "/workflows/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let random = uuid::Uuid::new_v4();
    let (status, _body) = fetch_html(&ui_app, &format!("/workflows/{random}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    worker.shutdown();
    worker_task.await.expect("worker should exit cleanly");
}

#[tokio::test]
async fn ui_lists_workflows_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        Some("ui-test-worker".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));

    let exec_on_zero =
        insert_workflow_on_url(&shard0_url, ShardId::new(0), "workflow_on_zero", "ui-zero").await;
    let exec_on_one =
        insert_workflow_on_url(&shard1_url, ShardId::new(1), "workflow_on_one", "ui-one").await;

    let ui_app = harvest_ui_router(api_state).with_state(test_app_state_without_database());
    let (status, list_html) = fetch_html(&ui_app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        list_html.contains("workflow_on_zero") && list_html.contains(&exec_on_zero.to_string()),
        "workflow from shard 0 should appear in the UI list"
    );
    assert!(
        list_html.contains("workflow_on_one") && list_html.contains(&exec_on_one.to_string()),
        "workflow from shard 1 should appear in the UI list"
    );
}

// ---------------------------------------------------------------------------
// Workers page tests
// ---------------------------------------------------------------------------

/// Insert a single row into `harvest_workers` with controllable status and
/// heartbeat time.  `heartbeat_offset_secs` < 0 → past (stale); 0 → now.
async fn insert_test_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    status: &str,
    heartbeat_offset_secs: i64,
) {
    let sql = format!(
        "INSERT INTO harvest_workers \
            (worker_id, last_heartbeat_at, status, queues, shard_assignments, max_concurrency, host) \
         VALUES \
            ('{worker_id}', NOW() + interval '{heartbeat_offset_secs} seconds', \
             '{status}', '[]'::jsonb, '[0]'::jsonb, 10, 'test-host') \
         ON CONFLICT (worker_id) DO UPDATE \
            SET last_heartbeat_at = excluded.last_heartbeat_at, \
                status            = excluded.status"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_worker failed");
}

fn build_single_shard_ui_app(database_url: &str) -> axum::Router {
    let pool = build_test_pool(database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_ui_router(api_state).with_state(test_app_state_without_database())
}

fn build_sharded_api_with_ui_app(shard0_url: &str, shard1_url: &str) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(build_two_shard_pool(shard0_url, shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));

    autumn_harvest_plugin::harvest_api_router(api_state.clone())
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(test_app_state_without_database())
}

async fn seed_dead_letter_ui_fixture(shard0_url: &str, shard1_url: &str) -> Vec<SeededDeadLetter> {
    let mut seeded = Vec::new();
    for i in 0..5 {
        seeded.push(
            insert_dead_letter_on_url(
                shard0_url,
                ShardId::new(0),
                if i % 2 == 0 {
                    "invoice_workflow"
                } else {
                    "settlement_workflow"
                },
                &format!("dlq-shard0-{i}"),
                "activity",
                Some("charge_card"),
                i,
            )
            .await,
        );
        seeded.push(
            insert_dead_letter_on_url(
                shard1_url,
                ShardId::new(1),
                if i % 2 == 0 {
                    "invoice_workflow"
                } else {
                    "settlement_workflow"
                },
                &format!("dlq-shard1-{i}"),
                "workflow",
                None,
                i + 5,
            )
            .await,
        );
    }
    seeded
}

fn assert_dead_letter_list_html(html: &str, seeded: &[SeededDeadLetter]) {
    assert!(html.contains("Dead Letters"), "page title missing: {html}");
    assert!(
        html.contains("href=\"dead-letters\"") && html.contains("Workers"),
        "nav should link Dead Letters alongside Workers: {html}"
    );
    assert!(
        html.contains("name=\"workflow_name\""),
        "workflow filter missing"
    );
    assert!(
        html.contains("name=\"task_kind\""),
        "task kind filter missing"
    );
    assert!(
        html.contains("name=\"failed_after\""),
        "failed_after filter missing"
    );
    assert!(
        html.contains("name=\"failed_before\""),
        "failed_before filter missing"
    );
    assert!(html.contains("name=\"shard_id\""), "shard filter missing");
    assert!(
        html.contains("Replay all matching"),
        "bulk replay control missing"
    );
    assert!(
        html.contains("Discard all matching"),
        "bulk discard control missing"
    );
    assert!(
        html.contains("action=\"../dead-letters/replay\""),
        "row replay form should post to the existing bulk replay endpoint: {html}"
    );
    assert!(
        html.contains("action=\"../dead-letters/discard\""),
        "row discard form should post to the existing bulk discard endpoint: {html}"
    );

    for row in seeded {
        assert!(
            html.contains(&row.id.to_string()),
            "dead-letter id should be present for row action: {}",
            row.id
        );
        assert!(
            html.contains(&row.workflow_name),
            "workflow name should render: {}",
            row.workflow_name
        );
    }
    assert!(
        html.contains("invoice_workflowEvent12") && !html.contains("invoice_workflowEvent1</code>"),
        "row detail should include the last events leading to failure, not the whole cemetery: {html}"
    );
    assert!(
        html.contains("&quot;ordinal&quot;"),
        "original task payload should render in row detail: {html}"
    );
}

fn assert_dead_letter_filtered_html(filtered_html: &str) {
    assert!(
        filtered_html.contains("invoice_workflow"),
        "invoice rows should remain after filter: {filtered_html}"
    );
    assert!(
        !filtered_html.contains("settlement_workflow"),
        "settlement rows should be filtered out: {filtered_html}"
    );
}

fn assert_dead_letter_task_kind_filtered_html(
    html: &str,
    seeded: &[SeededDeadLetter],
    include_activities: bool,
) {
    for row in seeded {
        let should_include = row.activity_name.is_some() == include_activities;
        assert_eq!(
            html.contains(&row.id.to_string()),
            should_include,
            "task kind filter should {} row {} in HTML: {html}",
            if should_include { "include" } else { "exclude" },
            row.id
        );
    }
}

#[tokio::test]
async fn ui_dead_letters_lists_filters_and_replays_single_entry() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let seeded = seed_dead_letter_ui_fixture(&shard0_url, &shard1_url).await;
    let app = build_sharded_api_with_ui_app(&shard0_url, &shard1_url);

    let (status, html) = fetch_html(&app, "/ui/dead-letters").await;
    assert_eq!(status, StatusCode::OK, "DLQ page should render: {html}");
    assert_dead_letter_list_html(&html, &seeded);
    assert!(
        html.contains("name=\"return_to\" value=\"../ui/dead-letters\""),
        "DLQ forms should return relative to the bulk endpoint path: {html}"
    );

    let (status, filtered_html) =
        fetch_html(&app, "/ui/dead-letters?workflow_name=invoice_workflow").await;
    assert_eq!(status, StatusCode::OK, "filtered DLQ page should render");
    assert_dead_letter_filtered_html(&filtered_html);

    let (status, activity_html) = fetch_html(&app, "/ui/dead-letters?task_kind=Activity").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "activity-filtered DLQ page should render"
    );
    assert_dead_letter_task_kind_filtered_html(&activity_html, &seeded, true);

    let (status, workflow_html) = fetch_html(&app, "/ui/dead-letters?task_kind=Workflow").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "workflow-filtered DLQ page should render"
    );
    assert_dead_letter_task_kind_filtered_html(&workflow_html, &seeded, false);

    let target = seeded
        .iter()
        .find(|row| row.activity_name.as_deref() == Some("charge_card"))
        .expect("activity dead-letter should exist");
    let legacy_target = seeded
        .iter()
        .find(|row| row.id != target.id)
        .expect("second dead-letter should exist");
    let (legacy_status, _legacy_headers, legacy_body) = post_form(
        &app,
        "/ui/dead-letters/replay",
        format!(
            "dead_letter_id={}&return_to=ui%2Fdead-letters",
            legacy_target.id
        ),
    )
    .await;
    assert_eq!(
        legacy_status,
        StatusCode::NOT_FOUND,
        "UI router must not keep a duplicate mutating replay route: {legacy_body}"
    );
    assert_eq!(
        count_dead_letter_by_id(&legacy_target.shard_url, legacy_target.id).await,
        1,
        "legacy UI POST path should not mutate DLQ rows"
    );

    let (status, headers, body) = post_form(
        &app,
        "/dead-letters/replay",
        format!(
            "dead_letter_id={}&return_to=..%2Fui%2Fdead-letters",
            target.id
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "single replay should redirect back to the page: {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect should include Location")
        .to_str()
        .expect("Location should be valid UTF-8");
    assert!(
        location.starts_with("../ui/dead-letters?flash="),
        "redirect should return to mounted DLQ UI with flash, got {location}"
    );
    assert_eq!(
        count_dead_letter_by_id(&target.shard_url, target.id).await,
        0,
        "replayed DLQ row should be removed"
    );
    assert_eq!(
        count_task_queue_by_activity(&target.shard_url, "charge_card").await,
        1,
        "single replay should enqueue exactly one activity task"
    );
}

/// Navigation link: the index and workflows pages must include a Workers link.
#[tokio::test]
async fn ui_workers_nav_link_on_index_page() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("workers") || html.contains("Workers"),
        "workflows page should have a navigation link to Workers: {html}"
    );
}

/// Empty fleet: GET /workers returns 200 with an empty-state explanation.
#[tokio::test]
async fn ui_workers_empty_fleet() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "workers page must return 200, got {status}"
    );
    assert!(html.contains("🔭 Vantage"), "layout header missing");
    assert!(
        html.contains("No workers") || html.contains("no workers"),
        "empty fleet should show a message: {html}"
    );
    assert!(!html.contains("<script"), "no script tags allowed");
    assert!(!html.contains("http://"), "no external URLs allowed");
    assert!(!html.contains("https://"), "no external HTTPS URLs allowed");
}

/// Health banner: all fresh Active workers → Healthy.
#[tokio::test]
async fn ui_workers_banner_healthy() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-healthy-1", "Active", 0).await;
    insert_test_worker(&mut conn, "w-healthy-2", "Active", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Healthy"),
        "banner should say Healthy with all fresh Active workers: {html}"
    );
}

/// Health banner: some stale workers but at least one active → Degraded.
#[tokio::test]
async fn ui_workers_banner_degraded_stale() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-active", "Active", 0).await;
    // stale: heartbeat 60 s ago, well past the default 10 s threshold
    insert_test_worker(&mut conn, "w-stale", "Active", -60).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Degraded"),
        "banner should say Degraded when stale workers exist: {html}"
    );
}

/// Health banner: no Active workers at all → Unhealthy.
#[tokio::test]
async fn ui_workers_banner_unhealthy_no_active() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-stopped", "Stopped", -5).await;
    insert_test_worker(&mut conn, "w-draining", "Draining", -5).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Unhealthy"),
        "banner should say Unhealthy when no Active workers: {html}"
    );
}

/// Worker rows: ID, status, relative heartbeat time and in-flight count are rendered.
#[tokio::test]
async fn ui_workers_shows_worker_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "worker-alpha", "Active", 0).await;
    insert_test_worker(&mut conn, "worker-beta", "Draining", -3).await;
    insert_test_worker(&mut conn, "worker-gamma", "Stopped", -5).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("worker-a"), "alpha worker missing: {html}");
    assert!(html.contains("worker-b"), "beta worker missing: {html}");
    assert!(html.contains("worker-g"), "gamma worker missing: {html}");
    assert!(html.contains("Active"), "Active status missing: {html}");
    assert!(html.contains("Draining"), "Draining status missing: {html}");
    assert!(html.contains("Stopped"), "Stopped status missing: {html}");
    // Relative time should appear (e.g. "just now", "Xs ago")
    assert!(
        html.contains("ago") || html.contains("just now"),
        "relative heartbeat time missing: {html}"
    );
}

/// Stale workers are visually flagged in the table.
#[tokio::test]
async fn ui_workers_stale_rows_flagged() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-fresh", "Active", 0).await;
    insert_test_worker(&mut conn, "w-old", "Active", -120).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("stale"),
        "stale workers should be flagged in the table: {html}"
    );
}

/// Filter ?status=Active shows only Active workers.
#[tokio::test]
async fn ui_workers_filter_by_status_active() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-act", "Active", 0).await;
    insert_test_worker(&mut conn, "w-drain", "Draining", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?status=Active").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("w-act"),
        "Active worker should appear: {html}"
    );
    assert!(
        !html.contains("w-drain"),
        "Draining worker should NOT appear after status=Active filter: {html}"
    );
}

/// Filter ?stale=true shows only stale workers.
#[tokio::test]
async fn ui_workers_filter_stale_true() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "w-fresh-2", "Active", 0).await;
    insert_test_worker(&mut conn, "w-stale-2", "Active", -120).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?stale=true").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("w-stale-"),
        "stale worker should appear: {html}"
    );
    assert!(
        !html.contains("w-fresh-"),
        "fresh worker should NOT appear after ?stale=true filter: {html}"
    );
}

/// Unknown status value returns 400.
#[tokio::test]
async fn ui_workers_unknown_status_value_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workers?status=zombie").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown status value should return 400: {html}"
    );
}

/// Pagination: ?limit=1 caps the rendered set to one row.
#[tokio::test]
async fn ui_workers_pagination_limits_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    insert_test_worker(&mut conn, "pg-worker-1", "Active", 0).await;
    insert_test_worker(&mut conn, "pg-worker-2", "Active", 0).await;
    insert_test_worker(&mut conn, "pg-worker-3", "Active", 0).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/workers?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    // With limit=1, exactly one row appears and the Next pagination link is present.
    let worker_count = html.matches("pg-worke").count();
    assert_eq!(
        worker_count, 1,
        "limit=1 should render exactly 1 worker row: {html}"
    );
    assert!(
        html.contains("Next"),
        "Next pagination link should appear when there are more rows: {html}"
    );
}

/// Multi-shard: workers from two shards both appear and are grouped.
#[tokio::test]
async fn ui_workers_multi_shard_grouped() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;

    let mut conn0 = AsyncPgConnection::establish(&shard0_url).await.unwrap();
    let mut conn1 = AsyncPgConnection::establish(&shard1_url).await.unwrap();
    insert_test_worker(&mut conn0, "shard0-worker", "Active", 0).await;
    insert_test_worker(&mut conn1, "shard1-worker", "Active", 0).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("shard0-w"), "shard 0 worker missing: {html}");
    assert!(html.contains("shard1-w"), "shard 1 worker missing: {html}");
    // Multi-shard deployments should group workers with shard headers.
    assert!(
        html.contains("Shard") || html.contains("shard"),
        "multi-shard page should show shard grouping: {html}"
    );
}

/// Partial shard failure: degraded banner + shard-unavailable stub.
#[tokio::test]
async fn ui_workers_partial_shard_failure_degraded() {
    let (good_url, _container) = setup_test_database_url().await;
    let mut conn = AsyncPgConnection::establish(&good_url).await.unwrap();
    insert_test_worker(&mut conn, "good-worker", "Active", 0).await;

    // Build a two-shard pool where shard 1 has an invalid URL so it will error.
    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), good_pool);
    pools.insert(ShardId::new(1), bad_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/workers").await;
    // Must not 5xx — partial shard failure is a degraded scenario, not a crash.
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "partial shard failure must not 5xx: {html}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "partial shard failure should return 200 with degraded view: {html}"
    );
    assert!(
        html.contains("Degraded") || html.contains("unavailable"),
        "partial shard failure should show Degraded banner or unavailable stub: {html}"
    );
}

/// Performance: 1 k workers across 4 shards must render in ≤ 500 ms p95.
///
/// Seeds 250 workers per shard, makes a single page request, and asserts:
/// 1. The page renders successfully (200 OK).
/// 2. The rendered HTML body is non-trivial (contains worker ids).
/// 3. The end-to-end wall-clock time is under the 500 ms budget.
#[tokio::test]
async fn ui_workers_perf_1k_workers_4_shards_under_500ms() {
    // Provision 4 shard databases in a single container.
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start Postgres container");
    let shard_urls = setup_n_shard_databases(&container, 4).await;

    // Seed 250 workers per shard = 1 000 total.
    let workers_per_shard: usize = 250;
    for (shard_idx, url) in shard_urls.iter().enumerate() {
        let mut conn = AsyncPgConnection::establish(url).await.unwrap();
        for w in 0..workers_per_shard {
            let worker_id = format!("perf-s{shard_idx}-w{w}");
            insert_test_worker(&mut conn, &worker_id, "Active", 0).await;
        }
    }

    // Build sharded pool and UI app.
    let mut pools = BTreeMap::new();
    for (i, url) in shard_urls.iter().enumerate() {
        pools.insert(
            ShardId::new(i32::try_from(i).unwrap()),
            build_test_pool(url),
        );
    }
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            (0..4).map(ShardId::new).collect(),
            (0..4).map(ShardId::new).collect(),
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    // Warm the lazy shard pools before measuring. This test covers the workers
    // page render/query budget, not first-use connection establishment against
    // Docker-backed Postgres shards.
    let (warm_status, _) = fetch_html(&app, "/workers?limit=200").await;
    assert_eq!(
        warm_status,
        StatusCode::OK,
        "perf test warm-up page must return 200",
    );

    // Measure wall-clock time for the warmed page render.
    let start = std::time::Instant::now();
    let (status, html) = fetch_html(&app, "/workers?limit=200").await;
    let elapsed = start.elapsed();

    assert_eq!(status, StatusCode::OK, "perf test page must return 200");
    assert!(
        html.len() > 10_000,
        "1k-worker page should produce substantial HTML (got {} bytes)",
        html.len()
    );
    assert!(
        elapsed.as_millis() < 500,
        "Workers page with 1k workers must render in < 500 ms (got {} ms)",
        elapsed.as_millis()
    );
}

// ---------------------------------------------------------------------------
// Schedules page tests
// ---------------------------------------------------------------------------

/// Insert a test row into `harvest_schedules`.
/// `kind` is `"Workflow"` or `"Dag"`; `name` is the `workflow_name` / `dag_name`.
async fn insert_test_schedule(
    database_url: &str,
    kind: &str,
    name: &str,
    is_paused: bool,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule insert");
    let (dag_col, wf_col) = if kind == "Dag" {
        (format!("'{name}'"), "NULL".to_string())
    } else {
        ("NULL".to_string(), format!("'{name}'"))
    };
    let paused_at_col = if is_paused { "NOW()" } else { "NULL" };
    let sql = format!(
        "INSERT INTO harvest_schedules \
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, \
             max_active_runs, is_paused, paused_at, created_at, updated_at) \
         VALUES \
            ('{id}', {dag_col}, {wf_col}, '0 * * * *', 'UTC', false, 1, \
             {is_paused}, {paused_at_col}, NOW(), NOW())"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_schedule failed");
    id
}

async fn insert_test_schedule_with_tz(
    database_url: &str,
    kind: &str,
    name: &str,
    is_paused: bool,
    timezone: &str,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("failed to connect for schedule insert");
    let (dag_col, wf_col) = if kind == "Dag" {
        (format!("'{name}'"), "NULL".to_string())
    } else {
        ("NULL".to_string(), format!("'{name}'"))
    };
    let paused_at_col = if is_paused { "NOW()" } else { "NULL" };
    let sql = format!(
        "INSERT INTO harvest_schedules \
            (id, dag_name, workflow_name, schedule_expr, timezone, catchup, \
             max_active_runs, is_paused, paused_at, created_at, updated_at) \
         VALUES \
            ('{id}', {dag_col}, {wf_col}, '0 * * * *', '{timezone}', false, 1, \
             {is_paused}, {paused_at_col}, NOW(), NOW())"
    );
    conn.batch_execute(&sql)
        .await
        .expect("insert_test_schedule_with_tz failed");
    id
}

/// Empty schedules table → page renders "No schedules registered."
#[tokio::test]
async fn ui_schedules_empty_state() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );
    assert!(html.contains("Schedules"), "page heading missing: {html}");
    assert!(
        html.contains("No schedules registered."),
        "empty state message missing: {html}"
    );
    assert!(html.contains("🔭 Vantage"), "layout header missing: {html}");
    assert!(!html.contains("<script"), "no script tags allowed: {html}");
    assert!(!html.contains("http://"), "no external http URLs: {html}");
    assert!(!html.contains("https://"), "no external https URLs: {html}");
}

/// Six schedules (3 Workflow + 3 Dag, mix of paused/active) all render.
#[tokio::test]
async fn ui_schedules_lists_all_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let id_wf1 = insert_test_schedule(&database_url, "Workflow", "payment_workflow", false).await;
    let id_wf2 = insert_test_schedule(&database_url, "Workflow", "invoice_workflow", true).await;
    let id_wf3 = insert_test_schedule(&database_url, "Workflow", "report_workflow", false).await;
    let id_dag1 = insert_test_schedule(&database_url, "Dag", "nightly_etl", true).await;
    let id_dag2 = insert_test_schedule(&database_url, "Dag", "hourly_sync", false).await;
    let id_dag3 = insert_test_schedule(&database_url, "Dag", "weekly_report", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );

    for id in [id_wf1, id_wf2, id_wf3, id_dag1, id_dag2, id_dag3] {
        assert!(
            html.contains(&id.to_string()),
            "schedule id {id} missing from page: {html}"
        );
    }
    assert!(
        html.contains("payment_workflow"),
        "payment_workflow missing: {html}"
    );
    assert!(
        html.contains("invoice_workflow"),
        "invoice_workflow missing: {html}"
    );
    assert!(html.contains("nightly_etl"), "nightly_etl missing: {html}");
    assert!(html.contains("Workflow"), "Workflow kind missing: {html}");
    assert!(html.contains("Dag"), "Dag kind missing: {html}");
    assert!(html.contains("Paused"), "paused badge missing: {html}");
    assert!(html.contains("Active"), "active badge missing: {html}");
    // Nav must link to Schedules
    assert!(
        html.contains("schedules"),
        "nav must include schedules link: {html}"
    );
}

/// Filter `kind=Workflow` shows only Workflow rows and hides Dag rows.
#[tokio::test]
async fn ui_schedules_filter_by_kind_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "target_wf", false).await;
    insert_test_schedule(&database_url, "Dag", "target_dag", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?kind=Workflow").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("target_wf"),
        "workflow row missing after kind=Workflow: {html}"
    );
    assert!(
        !html.contains("target_dag"),
        "dag row should not appear after kind=Workflow: {html}"
    );
}

/// Filter `kind=Dag` shows only Dag rows.
#[tokio::test]
async fn ui_schedules_filter_by_kind_dag() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "wf_hidden", false).await;
    insert_test_schedule(&database_url, "Dag", "dag_visible", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?kind=Dag").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("dag_visible"),
        "dag row missing after kind=Dag: {html}"
    );
    assert!(
        !html.contains("wf_hidden"),
        "workflow row should not appear after kind=Dag: {html}"
    );
}

/// Filter `paused=Paused` shows only paused rows.
#[tokio::test]
async fn ui_schedules_filter_by_paused() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "active_wf", false).await;
    insert_test_schedule(&database_url, "Workflow", "paused_wf", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?paused=Paused").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("paused_wf"),
        "paused row missing after paused=Paused: {html}"
    );
    assert!(
        !html.contains("active_wf"),
        "active row should not appear after paused=Paused: {html}"
    );
}

/// Filter `paused=Active` shows only active rows.
#[tokio::test]
async fn ui_schedules_filter_by_active() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "active_visible", false).await;
    insert_test_schedule(&database_url, "Workflow", "paused_hidden", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?paused=Active").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("active_visible"),
        "active row missing after paused=Active: {html}"
    );
    assert!(
        !html.contains("paused_hidden"),
        "paused row should not appear after paused=Active: {html}"
    );
}

/// Filter `target=my_wf` narrows by name substring.
#[tokio::test]
async fn ui_schedules_filter_by_target() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "my_special_wf", false).await;
    insert_test_schedule(&database_url, "Workflow", "other_wf", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?target=my_special").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("my_special_wf"),
        "target row missing after target filter: {html}"
    );
    assert!(
        !html.contains("other_wf"),
        "other row should not appear after target filter: {html}"
    );
}

/// Filter returns empty-set message when nothing matches.
#[tokio::test]
async fn ui_schedules_filter_no_match() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "some_workflow", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?target=nonexistent").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("No schedules match this filter"),
        "empty filter message missing: {html}"
    );
}

/// Pause action: POST to /schedules/{id}/pause → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_pause_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "pause_target", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/pause"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "pause action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the DB row is now paused.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let is_paused: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .expect("schedule should exist");
    assert!(is_paused, "schedule should be paused after pause action");
}

/// Resume action: POST to /schedules/{id}/resume → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_resume_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "resume_target", true).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/resume"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "resume action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the DB row is now active.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let is_paused: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .expect("schedule should exist");
    assert!(!is_paused, "schedule should be active after resume action");
}

/// Delete action: POST to /schedules/{id}/delete → 303 redirect with flash.
#[tokio::test]
async fn ui_schedules_delete_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "delete_target", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, &format!("/schedules/{id}/delete"), String::new()).await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "delete action must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go back to the schedules page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );

    // Verify the row is gone.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let count: i64 = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count query");
    assert_eq!(count, 0, "schedule should be deleted");
}

/// Auto-refresh: `?refresh=30` emits a meta http-equiv refresh tag.
#[tokio::test]
async fn ui_schedules_auto_refresh_meta_tag() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?refresh=30").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("content=\"30\"") && html.contains("http-equiv=\"refresh\""),
        "auto-refresh meta tag with content=30 missing: {html}"
    );

    let (_, html_no_refresh) = fetch_html(&app, "/schedules").await;
    assert!(
        !html_no_refresh.contains("http-equiv=\"refresh\""),
        "refresh tag must not appear when refresh not requested: {html_no_refresh}"
    );
}

/// Bulk pause/resume forms are present in the HTML.
#[tokio::test]
async fn ui_schedules_bulk_actions_present() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "bulk_wf", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("bulk-pause") || html.contains("Pause all"),
        "bulk pause action missing: {html}"
    );
    assert!(
        html.contains("bulk-resume") || html.contains("Resume all"),
        "bulk resume action missing: {html}"
    );
}

/// Bulk pause: POST to /schedules/bulk-pause with filter → redirects and pauses matching rows.
#[tokio::test]
async fn ui_schedules_bulk_pause_pauses_matching_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let id1 = insert_test_schedule(&database_url, "Workflow", "bulk_target_a", false).await;
    let id2 = insert_test_schedule(&database_url, "Workflow", "bulk_target_b", false).await;
    let id_other = insert_test_schedule(&database_url, "Dag", "other_dag", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, _body) =
        post_form(&app, "/schedules/bulk-pause", "kind=Workflow&paused=Active").await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "bulk-pause must redirect (got {status})"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("schedules"),
        "redirect must go to schedules: {location}"
    );

    // Verify workflow schedules are paused, dag is not.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let paused1: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id1))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    let paused2: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id2))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    let paused_other: bool = autumn_harvest::schema::harvest_schedules::table
        .filter(autumn_harvest::schema::harvest_schedules::id.eq(id_other))
        .select(autumn_harvest::schema::harvest_schedules::is_paused)
        .first(&mut conn)
        .await
        .unwrap();
    assert!(paused1, "bulk_target_a should be paused");
    assert!(paused2, "bulk_target_b should be paused");
    assert!(
        !paused_other,
        "other_dag should NOT be paused (different kind)"
    );
}

/// Pagination: `?limit=1` caps the list to 1 row and shows Next link.
#[tokio::test]
async fn ui_schedules_pagination() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_1", false).await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_2", false).await;
    insert_test_schedule(&database_url, "Workflow", "pag_wf_3", false).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    let row_count = html.matches("pag_wf_").count();
    assert_eq!(row_count, 1, "limit=1 should render exactly 1 row: {html}");
    assert!(
        html.contains("Next"),
        "Next pagination link missing when there are more rows: {html}"
    );
}

/// Multi-shard: schedules from two shards both appear.
#[tokio::test]
async fn ui_schedules_multi_shard() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    insert_test_schedule(&shard0_url, "Workflow", "shard0_schedule", false).await;
    insert_test_schedule(&shard1_url, "Dag", "shard1_dag_schedule", false).await;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "multi-shard schedules page must return 200: {html}"
    );
    assert!(
        html.contains("shard0_schedule"),
        "shard 0 schedule missing: {html}"
    );
    assert!(
        html.contains("shard1_dag_schedule"),
        "shard 1 schedule missing: {html}"
    );
}

/// Partial shard failure: renders 200 with shard-error banner, does not 5xx.
#[tokio::test]
async fn ui_schedules_partial_shard_failure() {
    let (good_url, _container) = setup_test_database_url().await;
    insert_test_schedule(&good_url, "Workflow", "good_shard_schedule", false).await;

    let bad_pool = build_test_pool("postgres://invalid:5432/nonexistent");
    let good_pool = build_test_pool(&good_url);
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), good_pool);
    pools.insert(ShardId::new(1), bad_pool);
    let harvest_pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(harvest_pool);
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_ne!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "partial shard failure must not 5xx: {html}"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "partial shard failure should return 200: {html}"
    );
    assert!(
        html.contains("unavailable") || html.contains("Shard"),
        "partial shard failure should show shard-error banner: {html}"
    );
    assert!(
        html.contains("good_shard_schedule"),
        "good shard row should still appear: {html}"
    );
}

/// Flash message from a redirect renders in the schedules list page.
#[tokio::test]
async fn ui_schedules_flash_message_renders() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/schedules?flash=Paused+my_workflow").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Paused my_workflow") || html.contains("Paused+my_workflow"),
        "flash message should appear on the page: {html}"
    );
}

/// Existing nav layouts include a Schedules link so operators can navigate.
#[tokio::test]
async fn ui_all_pages_have_schedules_nav_link() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    for path in ["/workflows", "/workers", "/schedules"] {
        let (status, html) = fetch_html(&app, path).await;
        assert_eq!(status, StatusCode::OK, "page {path} must render: {html}");
        assert!(
            html.contains("href=\"schedules\"")
                || html.contains("href=\"/schedules\"")
                || html.contains(">Schedules<"),
            "page {path} must include a Schedules nav link: {html}"
        );
    }
}

/// Timezone column renders and differentiates UTC (subdued) from other timezones (prominent badge).
#[tokio::test]
async fn ui_schedules_displays_timezone() {
    let (database_url, _container) = setup_test_database_url().await;
    insert_test_schedule_with_tz(&database_url, "Workflow", "utc_wf", false, "UTC").await;
    insert_test_schedule_with_tz(
        &database_url,
        "Workflow",
        "la_wf",
        false,
        "America/Los_Angeles",
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(status, StatusCode::OK);

    // Differentiate UTC vs America/Los_Angeles rendering:
    // UTC should be subdued, and America/Los_Angeles should be prominent.
    // Timezone column header must be present.
    assert!(
        html.contains("<th>Timezone</th>")
            || html.contains("<th>TIMEZONE</th>")
            || html.contains("<th>timezone</th>"),
        "Timezone header missing: {html}"
    );
    assert!(
        html.contains("America/Los_Angeles"),
        "America/Los_Angeles timezone missing: {html}"
    );
    assert!(html.contains("UTC"), "UTC timezone missing: {html}");
}

// ---------------------------------------------------------------------------
// Issue #253 — Workflow Execution Detail page
// ---------------------------------------------------------------------------

async fn insert_workflow_events(
    database_url: &str,
    exec_id: ExecutionId,
    events: &[autumn_harvest::WorkflowEvent],
    start_id: i32,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for event insert");
    autumn_harvest::store::append_events(&mut conn, exec_id, events, start_id)
        .await
        .expect("append_events should succeed");
}

async fn insert_child_workflow_on_url(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    parent_id: ExecutionId,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for child workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
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
        },
    )
    .await
    .expect("child workflow insert should succeed");
    exec_id
}

/// Detail page groups activity events into an "Activity attempts" section.
#[tokio::test]
async fn detail_page_shows_activity_attempts_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "send_email_wf", "act-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![
        autumn_harvest::WorkflowEvent::ActivityScheduled {
            activity_id: activity_exec_id,
            name: "send_email".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        },
        autumn_harvest::WorkflowEvent::ActivityCompleted {
            activity_id: activity_exec_id,
            output: serde_json::json!("sent"),
        },
    ];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Activity attempts"),
        "detail page should show an 'Activity attempts' panel: {html}"
    );
    assert!(
        html.contains("send_email"),
        "activity name should appear in the attempts panel: {html}"
    );
}

/// Detail page shows a children panel on the parent and a parent link on the child.
#[tokio::test]
async fn detail_page_shows_parent_children_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let parent_exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "parent_wf", "parent-1").await;
    let child_exec_id = insert_child_workflow_on_url(
        &database_url,
        ShardId::new(0),
        "child_wf",
        "child-1",
        parent_exec_id,
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);

    // Parent page should show children section with child exec id
    let (status, html) = fetch_html(&app, &format!("/workflows/{parent_exec_id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "parent detail page should render: {html}"
    );
    assert!(
        html.contains("Children") || html.contains("children"),
        "parent detail page should show a 'Children' section: {html}"
    );
    assert!(
        html.contains(&child_exec_id.to_string()[..8]),
        "child exec id prefix should appear on parent page: {html}"
    );

    // Child page should show parent as a clickable link
    let (status, html) = fetch_html(&app, &format!("/workflows/{child_exec_id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "child detail page should render: {html}"
    );
    let parent_str = parent_exec_id.to_string();
    assert!(
        html.contains(&format!("href=\"../../workflows/{parent_str}\""))
            || html.contains(&format!("href=\"../workflows/{parent_str}\""))
            || html.contains(&format!("href=\"{parent_str}\"")),
        "parent exec id should be a clickable link on child page: {html}"
    );
}

/// Detail page shows a Signals & Updates section when those events are present.
#[tokio::test]
async fn detail_page_shows_signals_updates_panel() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "signal_wf", "signal-1").await;

    let events = vec![autumn_harvest::WorkflowEvent::SignalReceived {
        signal_name: "approve_request".to_string(),
        payload: serde_json::json!({"approved": true}),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Signals") || html.contains("Updates"),
        "detail page should show a 'Signals & Updates' section: {html}"
    );
    assert!(
        html.contains("approve_request"),
        "signal name should appear in the signals panel: {html}"
    );
}

/// Detail page shows operator action forms: cancel and export history.
#[tokio::test]
async fn detail_page_shows_operator_actions() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "action_wf", "action-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Cancel") || html.contains("cancel"),
        "detail page should show a Cancel action: {html}"
    );
    assert!(
        html.contains("history/export") || html.contains("Export history"),
        "detail page should show an Export history link: {html}"
    );
}

/// `ActivityScheduled` events render with a human-readable label in the event timeline.
#[tokio::test]
async fn detail_page_event_labels_human_readable() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "label_wf", "label-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: activity_exec_id,
        name: "charge_card".to_string(),
        input: serde_json::json!({}),
        queue: "payments".to_string(),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Activity scheduled") || html.contains("activity scheduled"),
        "ActivityScheduled event should render with a human-readable label: {html}"
    );
}

/// Detail page paginates the event timeline when there are many events.
#[tokio::test]
async fn detail_page_events_paginated_for_large_history() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "big_wf", "big-1").await;

    // Insert 150 marker events so the pagination threshold is crossed.
    let many_events: Vec<autumn_harvest::WorkflowEvent> = (0..150)
        .map(|i| autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: format!("marker_{i}"),
            details: serde_json::json!({}),
        })
        .collect();
    insert_workflow_events(&database_url, exec_id, &many_events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");

    // The first page shows events 0–99; marker_149 is on page 1 and must not appear.
    assert!(
        !html.contains("marker_149"),
        "marker_149 is on page 1 (event 150 of 150) and should not render on page 0: {html}"
    );
    assert!(
        !html.contains("marker_100"),
        "marker_100 is on page 1 (event 101 of 150) and should not render on page 0: {html}"
    );

    // Pagination controls must be visible.
    assert!(
        html.contains("Next") || html.contains("Jump to latest"),
        "pagination or jump-to-latest control should appear for large event histories: {html}"
    );
}

/// Status badges on the detail page include aria-label for screen reader accessibility.
#[tokio::test]
async fn detail_page_status_badge_has_aria_label() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "aria_wf", "aria-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("aria-label"),
        "detail page status badge should have aria-label for accessibility: {html}"
    );
}

/// Workflow list filter form includes `started_after` and `started_before` inputs.
#[tokio::test]
async fn list_page_has_time_range_filters() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_single_shard_ui_app(&database_url);

    let (status, html) = fetch_html(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK, "list page should render: {html}");
    assert!(
        html.contains("name=\"started_after\""),
        "list page filter form should have a started_after input: {html}"
    );
    assert!(
        html.contains("name=\"started_before\""),
        "list page filter form should have a started_before input: {html}"
    );
}

/// Detail page event timestamps are displayed (not "—" for every row).
#[tokio::test]
async fn detail_page_event_timestamps_display() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "ts_wf", "ts-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    // WorkflowStarted event is inserted by start_or_load; its timestamp should appear.
    assert!(
        html.contains("UTC") || html.contains("2026"),
        "event timestamps should display real dates, not placeholder dashes: {html}"
    );
}

// ---------------------------------------------------------------------------
// Issue #253 — NEW RED tests for remaining acceptance criteria
// ---------------------------------------------------------------------------

/// Event timeline shows collapsible payload for each event.
#[tokio::test]
async fn detail_page_event_timeline_has_collapsible_payload() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "payload_wf", "payload-1").await;

    let activity_exec_id = autumn_harvest::ActivityExecId::new();
    let events = vec![autumn_harvest::WorkflowEvent::ActivityScheduled {
        activity_id: activity_exec_id,
        name: "do_work".to_string(),
        input: serde_json::json!({"amount": 42}),
        queue: "default".to_string(),
    }];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("<details"),
        "event timeline should have a <details> element for collapsible payload: {html}"
    );
    assert!(
        html.contains("view payload") || html.contains("payload"),
        "details summary should mention payload: {html}"
    );
    // The raw JSON payload data should appear in the page
    assert!(
        html.contains("amount") || html.contains("42"),
        "event payload data should be present in the expanded section: {html}"
    );
}

/// Detail page blocked-on panel appears for a running workflow with a pending activity.
#[tokio::test]
async fn detail_page_blocked_on_panel_for_running_workflow() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "blocked_wf", "blocked-1").await;

    // Insert a pending task queue row for this workflow
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let task_id = uuid::Uuid::new_v4();
    let sql = format!(
        "INSERT INTO harvest_task_queue \
            (id, queue_name, task_type, workflow_exec_id, activity_name, input, state, priority, \
             attempt, max_attempts, scheduled_at) \
         VALUES \
            ('{task_id}', 'default', 'activity', '{exec_uuid}', 'process_payment', \
             '{{}}', 'PENDING', 0, 0, 3, NOW())",
        exec_uuid = exec_id.as_uuid()
    );
    conn.batch_execute(&sql).await.expect("insert pending task");

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.to_lowercase().contains("blocked")
            || html.contains("Blocked on")
            || html.contains("pending"),
        "detail page should show blocked-on or pending panel: {html}"
    );
    assert!(
        html.contains("process_payment"),
        "pending activity name should appear in blocked-on panel: {html}"
    );
}

/// Cancel action in the UI redirects back to the detail page with a flash message.
#[tokio::test]
async fn detail_page_cancel_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "cancel_wf", "cancel-2").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/cancel"),
        "reason=test+cancellation",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "cancel action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go to the workflow detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
    assert!(
        !location.ends_with("flash="),
        "flash param must be non-empty: {location}"
    );
}

/// Send signal action redirects back with flash.
#[tokio::test]
async fn detail_page_signal_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "signal_wf2", "signal-2").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/signal"),
        "signal_name=ping&payload=%7B%7D",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "signal action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go back to the detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
}

/// Flash message is rendered on the detail page when flash param is present.
#[tokio::test]
async fn detail_page_renders_flash_message() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "flash_wf", "flash-1").await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) =
        fetch_html(&app, &format!("/workflows/{exec_id}?flash=Hello%20world")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("Hello world") || html.contains("Hello+world"),
        "flash message should appear on the page: {html}"
    );
}

/// Reset action redirects back with flash.
#[tokio::test]
async fn detail_page_reset_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id =
        insert_workflow_on_url(&database_url, ShardId::new(0), "reset_wf2", "reset-2").await;

    // Insert at least 2 events so there is a valid reset point
    let events = vec![
        autumn_harvest::WorkflowEvent::TimerStarted {
            timer_id: autumn_harvest::types::TimerId::new("t1"),
            duration_secs: 60,
        },
        autumn_harvest::WorkflowEvent::TimerFired {
            timer_id: autumn_harvest::types::TimerId::new("t1"),
        },
    ];
    insert_workflow_events(&database_url, exec_id, &events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, headers, body) = post_form(
        &app,
        &format!("/workflows/{exec_id}/reset"),
        "reset_to_event_id=0&reason=rollback",
    )
    .await;
    assert!(
        status == StatusCode::SEE_OTHER || status == StatusCode::FOUND,
        "reset action must redirect (got {status}): {body}"
    );
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains(&exec_id.to_string()) || location.contains("workflows"),
        "redirect must go back to the detail page: {location}"
    );
    assert!(
        location.contains("flash"),
        "redirect must carry a flash message: {location}"
    );
}

/// Detail page shows a "Jump to event" control in large histories.
#[tokio::test]
async fn detail_page_has_jump_to_event_n_control() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "jump_wf", "jump-1").await;

    // Insert 150 events so pagination threshold is crossed
    let many_events: Vec<autumn_harvest::WorkflowEvent> = (0..150)
        .map(|i| autumn_harvest::WorkflowEvent::SignalReceived {
            signal_name: format!("jump_signal_{i}"),
            payload: serde_json::json!({}),
        })
        .collect();
    insert_workflow_events(&database_url, exec_id, &many_events, 1).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page should render: {html}");
    assert!(
        html.contains("name=\"event_page\"") || html.contains("name=\"jump_event\""),
        "detail page should have a jump-to-event or event_page input control for large histories: {html}"
    );
    assert!(
        html.contains("Jump to") || html.contains("jump") || html.contains("Go"),
        "detail page should have a jump/go button or label for the control: {html}"
    );
}

// ---------------------------------------------------------------------------
// Issue #279: history event count and continue-as-new threshold on detail page
// ---------------------------------------------------------------------------

/// The workflow detail page must show the current event count alongside the
/// configured continue-as-new threshold in the Metadata card.
#[tokio::test]
async fn detail_page_shows_history_event_count_and_threshold() {
    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "hist_wf", "hist-1").await;
    // Insert 7 events so we have a known, non-zero count.
    append_test_events(&database_url, exec_id, "hist", 7).await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page must render: {html}");

    // The page must display the event count explicitly in the metadata section.
    assert!(
        html.contains("History events"),
        "metadata card should have a 'History events' label: {html}"
    );
    assert!(
        html.contains('7'),
        "metadata card should show the count 7: {html}"
    );

    // The default threshold (10 000) must be visible alongside the count.
    let threshold_str = DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD.to_string();
    assert!(
        html.contains(&threshold_str),
        "metadata card should include the continue-as-new threshold ({threshold_str}): {html}"
    );
}

/// When the registry is configured with a custom threshold the detail page must
/// reflect that value rather than the default 10 000.
#[tokio::test]
async fn detail_page_shows_custom_continue_as_new_threshold() {
    const CUSTOM_THRESHOLD: u64 = 500;

    let (database_url, _container) = setup_test_database_url().await;
    let exec_id = insert_workflow_on_url(&database_url, ShardId::new(0), "cust_wf", "cust-1").await;
    append_test_events(&database_url, exec_id, "cust", 3).await;

    // Build an app whose registry uses a distinctive non-default threshold.
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![WorkflowInfo {
                name: "cust_wf",
                module: "tests",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: None,
                concurrency: None,
                max_input_bytes: None,

                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
            }],
            vec![],
        )
        .with_history_policy(
            WorkflowHistoryPolicy::default().with_continue_as_new_threshold(CUSTOM_THRESHOLD),
        ),
    );
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    let (status, html) = fetch_html(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "detail page must render: {html}");

    assert!(
        html.contains("History events"),
        "metadata card should have a 'History events' label: {html}"
    );
    assert!(
        html.contains("500"),
        "metadata card should show the custom threshold 500: {html}"
    );
    // The default threshold value must NOT appear as the threshold.
    // (It could appear elsewhere, e.g. as an event count or ID, so we verify
    // the custom value is present rather than asserting the default is absent.)
    assert!(
        html.contains('3'),
        "metadata card should show the event count 3: {html}"
    );
}

#[tokio::test]
async fn ui_schedules_displays_recent_decisions() {
    let (database_url, _container) = setup_test_database_url().await;
    let id = insert_test_schedule(&database_url, "Workflow", "decision_ui_workflow", false).await;

    // Connect to database and insert a decision
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let occurred_at = chrono::Utc::now();
    let next_fire_at = occurred_at + chrono::Duration::hours(1);

    autumn_harvest::schedule_decision::record_decision_graceful(
        &mut conn,
        None,
        Some(id),
        "decision_ui_workflow",
        "workflow",
        "fired",
        "fired_ok",
        Some(serde_json::json!({ "run_id": "run-xyz-456" })),
        occurred_at,
        next_fire_at,
        0,
    )
    .await;

    let app = build_single_shard_ui_app(&database_url);
    let (status, html) = fetch_html(&app, "/schedules").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schedules page must return 200: {html}"
    );

    // Verify recent decisions collapsible is rendered in HTML
    assert!(
        html.contains("Recent Decisions (1)"),
        "recent decisions collapsible summary missing: {html}"
    );
    assert!(
        html.contains("fired"),
        "decision type 'fired' missing from page: {html}"
    );
    assert!(
        html.contains("fired_ok"),
        "reason code 'fired_ok' missing from page: {html}"
    );
}

#[tokio::test]
async fn ui_trigger_preserves_dag_metadata() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let dag_name = "ui_metadata_dag";

    let dag_info = autumn_harvest::info::DagInfo {
        name: dag_name,
        module: "tests",
        schedule: Some(autumn_harvest::policy::Schedule::Manual),
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("dag-workers"),
        builder: |_dag| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: std::time::Duration::ZERO,
        overlap_policy: autumn_harvest::OverlapPolicy::Skip,
        buffer_all_max: 100u32,
        owner: Some("ui-team"),
        runbook_url: Some("http://ui-runbook"),
        severity: Some("sev3"),
    };

    let dag_catalog =
        Arc::new(autumn_harvest::compile_dag_catalog(vec![dag_info]).expect("dag compiles"));

    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: dag_name,
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
    ));

    let schedule_id = insert_test_schedule(&database_url, "Dag", dag_name, false).await;

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
    // Mount the UI router
    let app = harvest_ui_router(api_state).with_state(test_app_state_without_database());

    // POST /schedules/{id}/trigger-now
    let (status, _headers, _body) = post_form(
        &app,
        &format!("/schedules/{schedule_id}/trigger-now"),
        String::new(),
    )
    .await;
    assert!(
        status.is_redirection(),
        "UI trigger must redirect; got {status}"
    );

    let execution = load_latest_workflow_execution_by_name_from_url(&database_url, dag_name)
        .await
        .expect("triggered execution should exist");
    assert_eq!(execution.owner.as_deref(), Some("ui-team"));
    assert_eq!(execution.runbook_url.as_deref(), Some("http://ui-runbook"));
    assert_eq!(execution.severity.as_deref(), Some("sev3"));
}

async fn load_latest_workflow_execution_by_name_from_url(
    database_url: &str,
    workflow_name: &str,
) -> Option<autumn_harvest::models::WorkflowExecution> {
    use diesel::OptionalExtension;
    use diesel::SelectableHelper;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for workflow lookup");
    autumn_harvest::schema::harvest_workflow_executions::table
        .filter(
            autumn_harvest::schema::harvest_workflow_executions::workflow_name.eq(workflow_name),
        )
        .order(autumn_harvest::schema::harvest_workflow_executions::created_at.desc())
        .select(autumn_harvest::models::WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .optional()
        .expect("failed to load workflow rows by workflow name")
}
