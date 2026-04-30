//! Integration tests for the Vantage dashboard UI.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::SchedulerMonitor;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
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

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

fn echo_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "echo_workflow",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
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
