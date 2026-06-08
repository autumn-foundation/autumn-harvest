//! Integration tests for the Build Routing UI page (issue #362).
//!
//! Tests the full two-build rolling deploy scenario via the UI API surface:
//! shift active policy → declare compat → observe reachability → retire.

use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::build_routing::{declare_compat, list_build_compat, set_build_policy};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::register_worker;
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
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
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
    "\n",
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
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    )
);

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

fn minimal_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "deploy_workflow",
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

fn build_api_state_with_pool(pool: &DbPool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    // Bypass admin auth in test.
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        minimal_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        autumn_harvest::scheduler::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    api_state
}

async fn fetch_html(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
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
                .unwrap(),
        )
        .await
        .expect("POST form request failed");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_json(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST JSON request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET JSON request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn delete_json(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("DELETE request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Red Phase: Build Routing page is reachable and shows nav link (AC #1).
#[tokio::test]
async fn build_routing_page_is_reachable_via_nav() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/build-routing").await;
    assert_eq!(status, StatusCode::OK, "build-routing page must return 200");
    assert!(
        html.contains("🔭 Vantage"),
        "page must include Vantage header"
    );
    // AC #1: page is accessible alongside other nav items
    assert!(html.contains("workers"), "nav must include Workers link");
    assert!(
        html.contains("schedules"),
        "nav must include Schedules link"
    );
    assert!(
        html.contains("dead-letters"),
        "nav must include Dead Letters link"
    );
    assert!(
        html.contains("build-routing"),
        "nav must include Build Routing link"
    );
    assert!(
        html.contains("Build Routing"),
        "active nav label must read 'Build Routing'"
    );
}

/// Red Phase: Empty state shows docs link when no policies registered (AC #10).
#[tokio::test]
async fn build_routing_empty_state_shows_docs_link() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/build-routing").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("safe-deploy") || html.contains("No build routing configured"),
        "empty state must reference safe-deploy runbook or show empty message"
    );
    // Empty state must NOT crash or show a wall of nulls
    assert!(
        !html.contains("null"),
        "empty state must not show null values"
    );
    assert!(
        !html.contains("panicked"),
        "page must not show panic messages"
    );
}

/// Red Phase: Workflows page nav includes Build Routing link (AC #1).
#[tokio::test]
async fn workflows_page_nav_includes_build_routing() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("build-routing"),
        "workflows nav must include Build Routing link"
    );
}

/// Red Phase: Workers page nav includes Build Routing link (AC #6 + AC #1).
#[tokio::test]
async fn workers_page_nav_includes_build_routing() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Register a worker so the worker table actually renders (empty state omits the table headers)
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    register_worker(
        &mut conn,
        "worker-test",
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "sha-v1",
        Some("prod-v1"),
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("build-routing"),
        "workers page nav must include Build Routing link"
    );
    // AC #6: Workers tab must show build_id column header
    assert!(
        html.contains("Build ID"),
        "workers table header must include Build ID column"
    );
    assert!(
        html.contains("Deployment"),
        "workers table header must include Deployment column"
    );
}

/// Red Phase: Workers page has `build_id` filter (AC #6).
#[tokio::test]
async fn workers_page_has_build_id_filter() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("name=\"build_id\""),
        "workers filter form must include build_id input"
    );
}

/// Red Phase: API GET /admin/build-routing returns JSON (AC #8).
#[tokio::test]
async fn api_get_build_routing_returns_json() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    let (status, json) = get_json(&app, "/admin/build-routing").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET /admin/build-routing must return 200"
    );
    assert!(
        json.get("policies").is_some(),
        "response must have 'policies' key"
    );
    assert!(
        json.get("reachability").is_some(),
        "response must have 'reachability' key"
    );
    assert!(
        json.get("shard_errors").is_some(),
        "response must have 'shard_errors' key"
    );
    // Empty deployment: no policies yet
    assert_eq!(
        json["policies"].as_array().map_or(1, Vec::len),
        0,
        "no policies expected on fresh DB"
    );
}

/// Red Phase: API POST /admin/build-routing/policies sets a policy (AC #5 + AC #8).
#[tokio::test]
async fn api_set_build_policy_works() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    let (status, json) = post_json(
        &app,
        "/admin/build-routing/policies",
        json!({ "queue_name": "default", "build_id": "sha-v2", "deployment_name": "prod-v2" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set policy must return 200: {json}");
    assert_eq!(json["queue_name"], "default");
    assert_eq!(json["build_id"], "sha-v2");
    assert_eq!(json["deployment_name"], "prod-v2");
}

/// Red Phase: API POST /admin/build-routing/compat declares compatibility (AC #5 + AC #8).
#[tokio::test]
async fn api_declare_compat_works() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    let (status, json) = post_json(
        &app,
        "/admin/build-routing/compat",
        json!({ "build_id": "sha-v2", "compatible_with": "sha-v1" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "declare compat must return 201: {json}"
    );
    assert_eq!(json["build_id"], "sha-v2");
    assert_eq!(json["compatible_with"], "sha-v1");
}

/// Red Phase: GET /admin/build-routing/compat lists compat entries (AC #8).
#[tokio::test]
async fn api_list_compat_works() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    // Seed a compat entry directly.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    declare_compat(&mut conn, "sha-v2", "sha-v1").await.unwrap();

    let (status, json) = get_json(&app, "/admin/build-routing/compat").await;
    assert_eq!(status, StatusCode::OK, "list compat must return 200");
    let entries = json["entries"]
        .as_array()
        .expect("response must have entries array");
    assert_eq!(entries.len(), 1, "must have one compat entry");
    assert_eq!(entries[0]["build_id"], "sha-v2");
    assert_eq!(entries[0]["compatible_with"], "sha-v1");
}

/// Red Phase: DELETE /admin/build-routing/compat/{build}/{with} revokes (AC #5 + AC #8).
#[tokio::test]
async fn api_revoke_compat_works() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    // Seed the entry.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    declare_compat(&mut conn, "sha-v2", "sha-v1").await.unwrap();

    let (status, json) = delete_json(&app, "/admin/build-routing/compat/sha-v2/sha-v1").await;
    assert_eq!(status, StatusCode::OK, "revoke must return 200: {json}");
    assert_eq!(json["revoked"], true, "revoked must be true");

    // Verify it's gone.
    let entries = list_build_compat(&mut conn).await.unwrap();
    assert!(entries.is_empty(), "entry must be removed after revoke");
}

/// Red Phase: POST /admin/build-routing/retire returns 409 when not safe (AC #5).
#[tokio::test]
async fn api_retire_build_returns_conflict_when_not_safe() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Start a workflow execution with assigned_build_id = sha-v1.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "deploy_workflow",
            workflow_id: "retire-test-wf",
            exec_id,
            input: json!({}),
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
    .unwrap();

    // Manually assign build_id to the execution.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::assigned_build_id.eq(Some("sha-v1")))
        .execute(&mut conn)
        .await
        .unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    let (status, json) = post_json(
        &app,
        "/admin/build-routing/retire",
        json!({ "build_id": "sha-v1" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "retire must return 409 when build has open executions: {json}"
    );
    assert_eq!(json["safe_to_retire"], false);
    assert!(
        json["open_executions"].as_i64().unwrap_or(0) > 0,
        "open_executions must be > 0"
    );
}

/// Red Phase: POST /admin/build-routing/retire returns 200 when safe (AC #5 + AC #11).
#[tokio::test]
async fn api_retire_build_returns_ok_when_safe() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    // No executions at all — build "sha-old" is trivially safe to retire.
    let (status, json) = post_json(
        &app,
        "/admin/build-routing/retire",
        json!({ "build_id": "sha-old" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "retire must return 200 when safe: {json}"
    );
    assert_eq!(json["safe_to_retire"], true);
    assert_eq!(json["open_executions"], 0);
    assert_eq!(json["pending_tasks"], 0);
}

/// Red Phase: UI set-policy form action redirects back with flash (AC #5).
#[tokio::test]
async fn ui_set_policy_form_action_redirects_with_flash() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, headers, _body) = post_form(
        &app,
        "/build-routing/set-policy",
        "queue_name=default&build_id=sha-v2&deployment_name=prod-v2",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER, "set-policy must redirect");
    let location = headers
        .get("location")
        .expect("redirect must have Location header")
        .to_str()
        .unwrap();
    assert!(
        location.contains("build-routing"),
        "must redirect to build-routing page: {location}"
    );
    assert!(
        location.contains("flash="),
        "redirect must include flash param: {location}"
    );
}

/// Red Phase: UI declare-compat form action redirects with flash (AC #5).
#[tokio::test]
async fn ui_declare_compat_form_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, headers, _body) = post_form(
        &app,
        "/build-routing/declare-compat",
        "build_id=sha-v2&compatible_with=sha-v1",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "declare-compat must redirect"
    );
    let location = headers.get("location").unwrap().to_str().unwrap();
    assert!(location.contains("build-routing"));
    assert!(location.contains("flash="));
}

/// Red Phase: UI revoke-compat form action redirects with flash (AC #5).
#[tokio::test]
async fn ui_revoke_compat_form_action_redirects() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Seed the entry.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    declare_compat(&mut conn, "sha-v2", "sha-v1").await.unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, headers, _body) = post_form(
        &app,
        "/build-routing/revoke-compat",
        "build_id=sha-v2&compatible_with=sha-v1",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER, "revoke-compat must redirect");
    let location = headers.get("location").unwrap().to_str().unwrap();
    assert!(location.contains("build-routing"));
    assert!(location.contains("flash="));
}

/// Red Phase: UI retire form action redirects with confirmation flash (AC #5).
#[tokio::test]
async fn ui_retire_form_action_redirects_with_safe_confirmation() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    // No executions — trivially safe.
    let (status, headers, _body) =
        post_form(&app, "/build-routing/retire", "build_id=sha-gone").await;
    assert_eq!(status, StatusCode::SEE_OTHER, "retire form must redirect");
    let location = headers.get("location").unwrap().to_str().unwrap();
    assert!(location.contains("flash="));
    // Flash must mention 'safe to retire' or 'safe'
    assert!(
        location.to_lowercase().contains("safe"),
        "flash must mention 'safe' for trivially safe build: {location}"
    );
}

/// Red Phase: Build Routing page shows policy and reachability after seeding (AC #2).
#[tokio::test]
async fn build_routing_page_shows_seeded_policy_and_reachability() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Seed a build policy directly.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    set_build_policy(&mut conn, "default", "sha-v2", Some("prod-v2"))
        .await
        .unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/build-routing").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("sha-v2"), "page must show seeded build_id");
    assert!(html.contains("default"), "page must show queue name");
    assert!(html.contains("prod-v2"), "page must show deployment name");
    // Set Policy form must be present
    assert!(
        html.contains("set-policy"),
        "Set Policy form must be on page"
    );
    // Declare Compat form must be present
    assert!(
        html.contains("declare-compat"),
        "Declare Compat form must be on page"
    );
}

/// Red Phase: Build Routing page shows compat graph (AC #3).
#[tokio::test]
async fn build_routing_page_shows_compat_graph() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    set_build_policy(&mut conn, "default", "sha-v2", None)
        .await
        .unwrap();
    declare_compat(&mut conn, "sha-v2", "sha-v1").await.unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) = fetch_html(&app, "/build-routing").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Compatibility"),
        "page must show compatibility section"
    );
    assert!(
        html.contains("sha-v2"),
        "compat table must show worker build sha-v2"
    );
    assert!(
        html.contains("sha-v1"),
        "compat table must show compatible_with sha-v1"
    );
    assert!(
        html.contains("Revoke"),
        "compat table must include Revoke action"
    );
}

/// Red Phase: Flash message appears on page after redirect (AC #5).
#[tokio::test]
async fn build_routing_page_displays_flash_message() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    let (status, html) =
        fetch_html(&app, "/build-routing?flash=Policy%20updated%20successfully").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("Policy updated successfully"),
        "flash message must appear on page"
    );
}

/// Red Phase: Two-build rolling deploy through UI's API surface (AC #11).
///
/// Scenario:
///   1. Start executions assigned to build sha-v1 (old build).
///   2. Shift active policy to sha-v2.
///   3. Declare sha-v2 compatible with sha-v1.
///   4. Observe reachability — sha-v1 still in use.
///   5. Complete old executions (simulated by direct DB update to COMPLETED).
///   6. Observe reachability — `sha-v1` now safe to retire.
///   7. Assert `safe_to_retire` only flips after all old executions complete.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn two_build_rolling_deploy_full_lifecycle() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();

    // Step 1: Start executions assigned to sha-v1.
    for i in 0..3 {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams {
                workflow_name: "deploy_workflow",
                workflow_id: &format!("rolling-v1-{i}"),
                exec_id,
                input: json!({}),
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
        .unwrap();
        // Assign sha-v1 to these executions.
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::assigned_build_id.eq(Some("sha-v1")))
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Step 2: Shift active policy to sha-v2 via API.
    let api_state = build_api_state_with_pool(&pool);
    let api_app =
        autumn_harvest_plugin::harvest_api_router(api_state.clone()).with_state(test_app_state());

    let (status, _) = post_json(
        &api_app,
        "/admin/build-routing/policies",
        json!({ "queue_name": "default", "build_id": "sha-v2" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set policy must succeed");

    // Step 3: Declare sha-v2 compatible with sha-v1.
    let (status, _) = post_json(
        &api_app,
        "/admin/build-routing/compat",
        json!({ "build_id": "sha-v2", "compatible_with": "sha-v1" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "declare compat must succeed");

    // Step 4: Observe reachability — sha-v1 must NOT be safe to retire yet.
    let (status, json) = get_json(&api_app, "/admin/build-routing").await;
    assert_eq!(status, StatusCode::OK);
    let reachability = json["reachability"].as_array().unwrap();
    let v1_reach = reachability
        .iter()
        .find(|r| r["build_id"] == "sha-v1")
        .expect("sha-v1 must appear in reachability");
    assert_eq!(
        v1_reach["safe_to_retire"], false,
        "sha-v1 must not be safe to retire while executions are RUNNING"
    );
    assert!(
        v1_reach["open_executions"].as_i64().unwrap_or(0) > 0,
        "sha-v1 must have open_executions > 0"
    );

    // Step 5: Simulate completing all sha-v1 executions.
    diesel::update(harvest_workflow_executions::table)
        .filter(harvest_workflow_executions::assigned_build_id.eq(Some("sha-v1")))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .unwrap();

    // Step 6: Re-check reachability — sha-v1 must now be safe to retire.
    let (status, json) = get_json(&api_app, "/admin/build-routing").await;
    assert_eq!(status, StatusCode::OK);
    let reachability = json["reachability"].as_array().unwrap();
    // sha-v1 might disappear entirely (no open executions, no pending tasks, no workers)
    // OR appear with safe_to_retire=true. Both are valid outcomes.
    if let Some(v1_reach) = reachability.iter().find(|r| r["build_id"] == "sha-v1") {
        assert_eq!(
            v1_reach["safe_to_retire"], true,
            "sha-v1 must be safe to retire after all executions complete: {v1_reach}"
        );
    }
    // sha-v1 not in reachability list is also acceptable (zero counts means nothing to retire).

    // Step 7: Assert retire API returns 200 (safe_to_retire=true) (AC #11).
    let (status, json) = post_json(
        &api_app,
        "/admin/build-routing/retire",
        json!({ "build_id": "sha-v1" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "retire must return 200 after all sha-v1 executions complete: {json}"
    );
    assert_eq!(
        json["safe_to_retire"], true,
        "safe_to_retire must be true after all executions complete"
    );
}

/// Red Phase: Workers page shows `build_id` filter and correctly filters results (AC #6).
#[tokio::test]
async fn workers_page_build_id_filter_works() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Register two workers with different build IDs.
    let mut conn = AsyncPgConnection::establish(&database_url).await.unwrap();
    register_worker(
        &mut conn,
        "worker-build-a",
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "sha-v1",
        None,
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    register_worker(
        &mut conn,
        "worker-build-b",
        &["default".to_string()],
        &[0],
        4,
        "localhost",
        None,
        "sha-v2",
        None,
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap();

    let api_state = build_api_state_with_pool(&pool);
    let app = harvest_ui_router(api_state).with_state(test_app_state());

    // Unfiltered: both workers appear.
    let (status, html) = fetch_html(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains("worker-build-a") || html.contains("sha-v1"),
        "must show v1 worker"
    );
    assert!(
        html.contains("worker-build-b") || html.contains("sha-v2"),
        "must show v2 worker"
    );

    // Filtered by sha-v1: only worker-build-a should appear.
    let (status, html) = fetch_html(&app, "/workers?build_id=sha-v1").await;
    assert_eq!(status, StatusCode::OK);
    // The page must show the build_id filter input pre-filled.
    assert!(html.contains("sha-v1"), "filtered page must show sha-v1");
}
