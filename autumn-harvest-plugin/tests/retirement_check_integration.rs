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
use testcontainers::ImageExt;
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

async fn setup_two_shards_with_migrations() -> ((String, String), ContainerAsync<Postgres>) {
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
    let shard0_db = format!("harvest_retirement_0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_retirement_1_{}", uuid::Uuid::new_v4().simple());

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
    api_state.set_admin_auth_boundary(true);
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

/// Insert a workflow execution with a single version marker event.
async fn insert_versioned_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    change_id: &str,
    recorded_version: u32,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
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
        chain_execution_timeout: None,
        chain_deadline_at: None,
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

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: format!("version:{change_id}"),
            details: json!(recorded_version),
        },
    ];
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append version marker");
    exec_id
}

/// Insert a workflow execution with NO version marker (pre-gate execution).
async fn insert_execution_without_marker(
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
        continued_from_exec_id: None,
        first_exec_id: None,
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
        chain_execution_timeout: None,
        chain_deadline_at: None,
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

    // Only append WorkflowStarted — no MarkerRecorded event for any change id.
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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn retirement_check_returns_404_equivalent_empty_for_unknown_change_id() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        ShardRouter::single(),
    );

    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=nonexistent&min_safe_version=2",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    // Empty report — not a 404 or panic
    assert_eq!(body["status"], "safe");
    assert_eq!(body["safe_to_retire"], true);
    assert!(
        body["blockers"].as_array().unwrap().is_empty(),
        "no blockers for unknown change id"
    );
}

#[tokio::test]
async fn retirement_check_blocked_while_v1_running_safe_after_terminal() {
    let (database_url, _container) = setup_database_url_with_migrations().await;

    // Insert v1 running execution — blocks retirement
    let exec_id = insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "run-v1",
        "RUNNING",
        "billing_v2_tax",
        1,
    )
    .await;

    // Insert v2 running execution — not a blocker (version >= min_safe_version)
    insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "run-v2",
        "RUNNING",
        "billing_v2_tax",
        2,
    )
    .await;

    // Insert v1 terminal execution — not a blocker (terminal)
    insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "done-v1",
        "COMPLETED",
        "billing_v2_tax",
        1,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        ShardRouter::single(),
    );

    // Retirement check for v1 (min_safe_version=2 means v<2 is old)
    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v2_tax&min_safe_version=2",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["status"], "blocked",
        "v1 running should block retirement"
    );
    assert_eq!(body["safe_to_retire"], false);

    let blockers = body["blockers"].as_array().expect("blockers array");
    assert_eq!(blockers.len(), 1, "one version-1 blocker row");
    let blocker = &blockers[0];
    assert_eq!(blocker["recorded_version"], 1);
    assert_eq!(blocker["active_executions"], 1);
    assert_eq!(blocker["terminal_executions"], 1);

    // Sample IDs must include the running execution
    let sample_ids: Vec<String> = blocker["sample_active_execution_ids"]
        .as_array()
        .expect("sample ids array")
        .iter()
        .filter_map(|v| v.as_str().map(ToString::to_string))
        .collect();
    assert!(
        sample_ids.contains(&exec_id.as_uuid().to_string()),
        "sample IDs should include the running v1 execution; got {sample_ids:?}"
    );

    // Now mark the v1 execution as COMPLETED
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .unwrap();
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq("COMPLETED"),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
    ))
    .execute(&mut conn)
    .await
    .unwrap();

    // Re-check — should now be safe
    let (status2, body2) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v2_tax&min_safe_version=2",
    )
    .await;

    assert_eq!(status2, StatusCode::OK, "body: {body2}");
    assert_eq!(
        body2["status"], "safe",
        "all v1 executions are now terminal"
    );
    assert_eq!(body2["safe_to_retire"], true);
    // Blocker row still appears but with zero active
    let blockers2 = body2["blockers"].as_array().expect("blockers2");
    if !blockers2.is_empty() {
        assert_eq!(blockers2[0]["active_executions"], 0);
    }
}

#[tokio::test]
async fn retirement_check_state_group_active_hides_terminal_rows() {
    let (database_url, _container) = setup_database_url_with_migrations().await;

    // Only terminal v1 — should not appear when state_group=active
    insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "done-only",
        "COMPLETED",
        "billing_v2_tax",
        1,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        ShardRouter::single(),
    );

    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v2_tax&min_safe_version=2&state_group=active",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["status"], "safe");
    assert!(body["blockers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn retirement_check_aggregates_counts_across_two_shards() {
    let ((shard0_url, shard1_url), _container) = setup_two_shards_with_migrations().await;

    // One v1 running on each shard
    insert_versioned_execution(
        &shard0_url,
        ShardId::new(0),
        "billing_checkout",
        "shard0-v1",
        "RUNNING",
        "billing_v2_tax",
        1,
    )
    .await;
    insert_versioned_execution(
        &shard1_url,
        ShardId::new(1),
        "billing_checkout",
        "shard1-v1",
        "RUNNING",
        "billing_v2_tax",
        1,
    )
    .await;

    let app = build_api_app(
        build_two_shard_pool(&shard0_url, &shard1_url),
        two_shard_router(),
    );

    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v2_tax&min_safe_version=2",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["status"], "blocked");
    assert_eq!(body["blockers"][0]["active_executions"], 2);
    assert_eq!(
        body["blockers"][0]["shard_coverage"]["matched_shards"],
        json!([0, 1])
    );
    assert_eq!(body["shards"][0]["status"], "inspected");
    assert_eq!(body["shards"][1]["status"], "inspected");
}

#[tokio::test]
async fn retirement_check_degraded_when_one_shard_unavailable() {
    let (database_url, _container) = setup_database_url_with_migrations().await;

    insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "shard0-v1",
        "RUNNING",
        "billing_v2_tax",
        1,
    )
    .await;

    // Shard 1 has a broken pool (bad URL)
    let bad_pool = {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://postgres:postgres@127.0.0.1:1/nonexistent",
        );
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("pool builds even with bad url")
    };
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(&database_url));
    pools.insert(ShardId::new(1), bad_pool);
    let pool = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let router = two_shard_router();
    let app = build_api_app(pool, router);

    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v2_tax&min_safe_version=2",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    // Must be partial (not safe), naming the unavailable shard
    assert_eq!(body["status"], "partial", "one shard unavailable → partial");
    assert_eq!(body["safe_to_retire"], false);

    let unavailable_shard = body["shards"]
        .as_array()
        .expect("shards array")
        .iter()
        .find(|s| s["status"] == "unavailable")
        .expect("exactly one unavailable shard");
    assert_eq!(unavailable_shard["shard_id"], 1);
}

/// Executions that started before a version gate was inserted carry no
/// `MarkerRecorded` event for that change id.  When the gate is later added to
/// the code and `workflow_name` is supplied to the retirement check, these
/// pre-gate executions must appear as blockers (`recorded_version` = 0) so that
/// operators are not misled into retiring the old branch prematurely.
#[tokio::test]
async fn retirement_check_includes_pre_gate_executions_as_blockers() {
    let (database_url, _container) = setup_database_url_with_migrations().await;

    // Insert an execution that has no version marker for "billing_v3_tax" at all.
    let pre_gate_id = insert_execution_without_marker(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "pre-gate-run",
        "RUNNING",
    )
    .await;

    // Also insert a properly-gated v1 execution for the same workflow.
    insert_versioned_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "gated-v1",
        "RUNNING",
        "billing_v3_tax",
        1,
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::single(build_test_pool(&database_url)),
        ShardRouter::single(),
    );

    // Without workflow_name: pre-gate execution is invisible (no marker to match).
    let (status_no_wf, body_no_wf) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v3_tax&min_safe_version=2",
    )
    .await;
    assert_eq!(status_no_wf, StatusCode::OK, "body: {body_no_wf}");
    let total_active_no_wf: i64 = body_no_wf["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|b| b["active_executions"].as_i64())
        .sum();
    assert_eq!(
        total_active_no_wf, 1,
        "without workflow_name filter only the gated v1 execution is visible"
    );

    // With workflow_name: pre-gate execution must appear as a blocker.
    let (status, body) = get_json(
        &app,
        "/admin/version-gates/retirement-check?change_id=billing_v3_tax&min_safe_version=2&workflow_name=billing_checkout",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["status"], "blocked");
    assert_eq!(body["safe_to_retire"], false);

    // There must be a blocker row with recorded_version=0 for pre-gate executions.
    let blockers = body["blockers"].as_array().expect("blockers array");
    let pre_gate_row = blockers
        .iter()
        .find(|b| b["recorded_version"] == 0)
        .expect("pre-gate blocker row with recorded_version=0 must be present");
    assert!(
        pre_gate_row["active_executions"].as_i64().unwrap() >= 1,
        "pre-gate row must count the markerless running execution"
    );

    // The pre-gate execution id should appear in sample_active_execution_ids.
    let sample_ids: Vec<String> = pre_gate_row["sample_active_execution_ids"]
        .as_array()
        .expect("sample ids")
        .iter()
        .filter_map(|v| v.as_str().map(ToString::to_string))
        .collect();
    assert!(
        sample_ids.contains(&pre_gate_id.as_uuid().to_string()),
        "pre-gate execution must be listed in sample ids; got {sample_ids:?}"
    );
}
