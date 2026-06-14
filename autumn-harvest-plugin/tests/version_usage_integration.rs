use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::WorkflowEvent;
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
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
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

async fn setup_two_shards_with_migrations() -> ((String, String), ContainerAsync<Postgres>) {
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
    let shard0_db = format!("harvest_version_usage_0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_version_usage_1_{}", uuid::Uuid::new_v4().simple());

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
    autumn_web::migrate::run_pending(&shard0_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 0");
    autumn_web::migrate::run_pending(&shard1_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 1");
    ((shard0_url, shard1_url), container)
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

fn build_api_app(pool: HarvestDbPool, router: ShardRouter) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
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

async fn insert_version_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    change_id: &str,
    recorded_version: u32,
) -> ExecutionId {
    insert_version_execution_with_markers(
        database_url,
        shard,
        workflow_name,
        workflow_id,
        state,
        &[(change_id, recorded_version)],
    )
    .await
}

async fn insert_version_execution_with_markers(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    markers: &[(&str, u32)],
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

    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
    }];
    events.extend(markers.iter().map(|(change_id, recorded_version)| {
        WorkflowEvent::MarkerRecorded {
            name: format!("version:{change_id}"),
            details: json!(recorded_version),
        }
    }));
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append version marker");
    exec_id
}

#[tokio::test]
async fn version_usage_report_groups_active_and_terminal_version_markers() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    insert_version_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "active-v1",
        "RUNNING",
        "billing_checkout_v2_tax",
        1,
    )
    .await;
    insert_version_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "terminal-v1",
        "COMPLETED",
        "billing_checkout_v2_tax",
        1,
    )
    .await;
    insert_version_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "active-v2",
        "RUNNING",
        "billing_checkout_v2_tax",
        2,
    )
    .await;
    insert_version_execution_with_markers(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "two-gates",
        "RUNNING",
        &[("billing_checkout_v2_tax", 2), ("invoice_rollup_v3", 1)],
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        ShardRouter::single(),
    );
    let (status, body) = get_json(&app, "/admin/version-gates/usage").await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["status"], "complete");
    let items = body["items"].as_array().expect("items must be an array");
    let row = items
        .iter()
        .find(|item| {
            item["workflow_name"] == "billing_checkout"
                && item["change_id"] == "billing_checkout_v2_tax"
                && item["recorded_version"] == 1
        })
        .expect("expected billing_checkout version-1 usage row");
    assert_eq!(row["active_executions"], 1);
    assert_eq!(row["terminal_executions"], 1);
    assert_eq!(row["shard_coverage"]["matched_shards"], json!([0]));

    let tax_v2 = items
        .iter()
        .find(|item| {
            item["workflow_name"] == "billing_checkout"
                && item["change_id"] == "billing_checkout_v2_tax"
                && item["recorded_version"] == 2
        })
        .expect("version 2 must be counted separately from version 1");
    assert_eq!(tax_v2["active_executions"], 2);

    let invoice_gate = items
        .iter()
        .find(|item| item["change_id"] == "invoice_rollup_v3")
        .expect("second independent gate must be reported separately");
    assert_eq!(invoice_gate["recorded_version"], 1);

    let (active_status, active_body) = get_json(
        &app,
        "/admin/version-gates/usage?change_id=billing_checkout_v2_tax&recorded_version=1&state_group=active",
    )
    .await;
    assert_eq!(active_status, StatusCode::OK, "body: {active_body}");
    assert_eq!(active_body["items"][0]["active_executions"], 1);
    assert_eq!(active_body["items"][0]["terminal_executions"], 0);

    let (terminal_status, terminal_body) = get_json(
        &app,
        "/admin/version-gates/usage?change_id=billing_checkout_v2_tax&recorded_version=1&state_group=terminal",
    )
    .await;
    assert_eq!(terminal_status, StatusCode::OK, "body: {terminal_body}");
    assert_eq!(terminal_body["items"][0]["active_executions"], 0);
    assert_eq!(terminal_body["items"][0]["terminal_executions"], 1);
}

#[tokio::test]
async fn version_usage_report_merges_matching_groups_across_two_shards() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;
    insert_version_execution(
        &shard0_url,
        ShardId::new(0),
        "billing_checkout",
        "shard0-v1",
        "RUNNING",
        "billing_checkout_v2_tax",
        1,
    )
    .await;
    insert_version_execution(
        &shard1_url,
        ShardId::new(1),
        "billing_checkout",
        "shard1-v1",
        "RUNNING",
        "billing_checkout_v2_tax",
        1,
    )
    .await;

    let app = build_api_app(
        build_two_shard_pool(&shard0_url, &shard1_url),
        two_shard_router(),
    );
    let (status, body) = get_json(
        &app,
        "/admin/version-gates/usage?change_id=billing_checkout_v2_tax&recorded_version=1",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["status"], "complete");
    assert_eq!(body["items"][0]["active_executions"], 2);
    assert_eq!(
        body["items"][0]["shard_coverage"]["matched_shards"],
        json!([0, 1])
    );
    assert_eq!(body["shards"][0]["status"], "inspected");
    assert_eq!(body["shards"][1]["status"], "inspected");
}
