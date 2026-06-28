//! Integration tests for `GET /admin/workflow-types/reachability` (issue #520).
//!
//! Verifies the three verdicts (`safe_to_remove` / `in_use` / `orphaned`), the
//! `?workflow_type=` filter, cross-shard aggregation with an unreachable shard
//! reported as `partial`, and the admin auth boundary.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

/// A trivial workflow handler — only its registration name matters here.
fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

fn workflow_info_named(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        name,
        module: "tests",
        handler: noop_workflow,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
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

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

fn single_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    )
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn build_api_app(
    pool: HarvestDbPool,
    router: ShardRouter,
    registered: Vec<&'static str>,
) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    let workflows = registered.into_iter().map(workflow_info_named).collect();
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(workflows, vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(autumn_web::AppState::for_test())
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

async fn insert_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({}),
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
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution");

    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(Utc::now())
    };
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq(state),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(completed_at),
    ))
    .execute(&mut conn)
    .await
    .expect("failed to update workflow state");

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append start event");
    exec_id
}

fn item<'a>(report: &'a Value, workflow_type: &str) -> &'a Value {
    report
        .get("items")
        .and_then(Value::as_array)
        .expect("items array")
        .iter()
        .find(|item| item.get("workflow_type").and_then(Value::as_str) == Some(workflow_type))
        .unwrap_or_else(|| panic!("expected item for {workflow_type}"))
}

#[tokio::test]
async fn reachability_assigns_safe_in_use_and_orphaned_verdicts() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    // onboarding: registered, all terminal -> safe_to_remove.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "ob-1",
        "COMPLETED",
    )
    .await;
    // subscription: registered, one RUNNING -> in_use.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "subscription",
        "sub-1",
        "RUNNING",
    )
    .await;
    insert_execution(
        &database_url,
        ShardId::new(0),
        "subscription",
        "sub-2",
        "COMPLETED",
    )
    .await;
    // legacy_export: NOT registered, one RUNNING -> orphaned.
    insert_execution(
        &database_url,
        ShardId::new(0),
        "legacy_export",
        "leg-1",
        "RUNNING",
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec!["onboarding", "subscription"],
    );

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("complete")
    );

    let onboarding = item(&report, "onboarding");
    assert_eq!(onboarding.get("registered"), Some(&json!(true)));
    assert_eq!(onboarding.get("non_terminal_count"), Some(&json!(0)));
    assert_eq!(
        onboarding.get("verdict").and_then(Value::as_str),
        Some("safe_to_remove")
    );

    let subscription = item(&report, "subscription");
    assert_eq!(subscription.get("registered"), Some(&json!(true)));
    assert_eq!(subscription.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        subscription.get("verdict").and_then(Value::as_str),
        Some("in_use")
    );
    assert!(
        subscription
            .get("oldest_non_terminal_age_secs")
            .and_then(Value::as_i64)
            .is_some()
    );

    let legacy = item(&report, "legacy_export");
    assert_eq!(legacy.get("registered"), Some(&json!(false)));
    assert_eq!(legacy.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        legacy.get("verdict").and_then(Value::as_str),
        Some("orphaned")
    );
}

#[tokio::test]
async fn reachability_filter_for_absent_type_returns_safe_zero_object() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        single_shard_router(),
        vec!["onboarding"],
    );

    let (status, report) = get_json(
        &app,
        "/admin/workflow-types/reachability?workflow_type=never_started",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = report
        .get("items")
        .and_then(Value::as_array)
        .expect("items");
    assert_eq!(items.len(), 1);
    let only = &items[0];
    assert_eq!(
        only.get("workflow_type").and_then(Value::as_str),
        Some("never_started")
    );
    assert_eq!(only.get("non_terminal_count"), Some(&json!(0)));
    assert_eq!(
        only.get("verdict").and_then(Value::as_str),
        Some("safe_to_remove")
    );
}

#[tokio::test]
async fn reachability_aggregates_across_shards_and_reports_unavailable_shard() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    insert_execution(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "ob-shard0",
        "RUNNING",
    )
    .await;

    // Shard 0 points at the live DB; shard 1 points at an unreachable URL.
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), build_test_pool(&database_url));
    shard_pools.insert(
        ShardId::new(1),
        build_test_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent"),
    );
    let pool = HarvestDbPool::sharded(ShardedDbPool::from_map(shard_pools, ShardId::new(0)));
    let app = build_api_app(pool, two_shard_router(), vec!["onboarding"]);

    let (status, report) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::OK);
    // A partial answer must never be mistaken for "safe to remove".
    assert_eq!(
        report.get("status").and_then(Value::as_str),
        Some("partial")
    );

    let inspections = report
        .get("shards")
        .and_then(Value::as_array)
        .expect("shards");
    let unreachable = inspections
        .iter()
        .find(|entry| entry.get("shard_id").and_then(Value::as_i64) == Some(1))
        .expect("shard 1 reported");
    assert_eq!(
        unreachable.get("status").and_then(Value::as_str),
        Some("unavailable")
    );
    assert!(unreachable.get("error").and_then(Value::as_str).is_some());

    let onboarding = item(&report, "onboarding");
    assert_eq!(onboarding.get("non_terminal_count"), Some(&json!(1)));
    assert_eq!(
        onboarding.get("verdict").and_then(Value::as_str),
        Some("in_use")
    );
}

#[tokio::test]
async fn reachability_requires_admin_auth() {
    // No admin boundary set -> the shared `/admin/*` guard must reject.
    let app =
        harvest_api_router(HarvestApiState::new()).with_state(autumn_web::AppState::for_test());
    let (status, _) = get_json(&app, "/admin/workflow-types/reachability").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
