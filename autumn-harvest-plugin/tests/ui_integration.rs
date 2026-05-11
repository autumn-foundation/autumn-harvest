//! Integration tests for the Vantage dashboard UI.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewHarvestEvent;
use autumn_harvest::scheduler::SchedulerMonitor;
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_events, harvest_task_queue, harvest_workflow_executions,
};
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
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
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

    // Measure wall-clock time for the page render.
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
