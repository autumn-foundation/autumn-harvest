//! Integration tests for the DLQ root-cause aggregation endpoint (issue #385).
//!
//! Seeds dead-letter rows spanning multiple workflow names and failure
//! signatures, then asserts each `group_by` combination returns the expected
//! counts and that the per-group counts reconcile to `filtered_total`.

use std::collections::BTreeMap;

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260616000001_harvest_workflow_schedule_id/up.sql"
    ),
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
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
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
    include_str!("../../autumn-harvest/migrations/20260613000000_harvest_workflow_sla/up.sql"),
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
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260609000001_harvest_workflow_current_details/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260613000001_harvest_schedule_catchup_window/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql")
);

type HarvestApiApp = axum::Router;

// Two failure signatures used across the seeded fixture.
const ERROR_SIG1: &str = "stripe: rate limited";
const ERROR_SIG2: &str = "db timeout after 30000ms connecting";
const SIG1: &str = "stripe: rate limited";
const SIG2: &str = "db timeout after <NUM>ms connecting";

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
        diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, INIT_SQL)
            .await
            .expect("failed to apply harvest migrations to shard database");
    }

    ((shard0_url, shard1_url), container)
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

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn build_dlq_app(pool: DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_sharded_dlq_app(shard0_url: &str, shard1_url: &str) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(build_two_shard_pool(shard0_url, shard1_url));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
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
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

/// Insert a workflow execution row on `database_url` and return its exec id.
async fn insert_execution(database_url: &str, shard: i32, workflow_name: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for execution insert");
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("{workflow_name}-{}", uuid::Uuid::new_v4().simple()),
        run_id: uuid::Uuid::new_v4(),
        shard_id: shard,
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
    exec_id
}

/// Insert `count` dead-letter rows linked to `exec_id` carrying `error`.
async fn insert_dlq_rows(
    database_url: &str,
    exec_id: ExecutionId,
    activity_name: &str,
    error: &str,
    count: usize,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    for _ in 0..count {
        autumn_harvest::dlq::dead_letter(
            &mut conn,
            &autumn_harvest::dlq::NewDeadLetterEntry {
                original_task_id: uuid::Uuid::new_v4(),
                queue_name: "default".to_string(),
                task_type: "ACTIVITY".to_string(),
                workflow_exec_id: Some(exec_id.as_uuid()),
                activity_name: Some(activity_name.to_string()),
                input: json!({ "test": true }),
                error: error.to_string(),
                attempts: 3,
                owner: None,
                severity: None,
            },
        )
        .await
        .expect("dead-letter insert should succeed");
    }
}

/// Seed the standard 50-row fixture on `database_url` (single shard).
///
/// Distribution (`workflow_name` × `failure_signature`):
///   onboarding × sig1 = 20, onboarding × sig2 = 5  (onboarding total 25)
///   billing    × sig1 = 10, billing    × sig2 = 8  (billing total 18)
///   reporting  × sig1 = 4,  reporting  × sig2 = 3  (reporting total 7)
/// Totals: 50 rows, sig1 = 34, sig2 = 16.
async fn seed_fixture(database_url: &str, shard: i32) {
    let onboarding = insert_execution(database_url, shard, "onboarding").await;
    let billing = insert_execution(database_url, shard, "billing").await;
    let reporting = insert_execution(database_url, shard, "reporting").await;

    insert_dlq_rows(database_url, onboarding, "charge_card", ERROR_SIG1, 20).await;
    insert_dlq_rows(database_url, onboarding, "load_profile", ERROR_SIG2, 5).await;
    insert_dlq_rows(database_url, billing, "charge_card", ERROR_SIG1, 10).await;
    insert_dlq_rows(database_url, billing, "settle", ERROR_SIG2, 8).await;
    insert_dlq_rows(database_url, reporting, "charge_card", ERROR_SIG1, 4).await;
    insert_dlq_rows(database_url, reporting, "render", ERROR_SIG2, 3).await;
}

/// Map a `groups` array into name -> count for a single-dimension grouping.
fn counts_by(groups: &[Value], dim: &str) -> BTreeMap<String, i64> {
    let mut map = BTreeMap::new();
    for group in groups {
        let key = group["key"][dim].as_str().unwrap_or("<null>").to_string();
        map.insert(key, group["count"].as_i64().unwrap_or(0));
    }
    map
}

fn sum_group_counts(groups: &[Value]) -> i64 {
    groups
        .iter()
        .map(|g| g["count"].as_i64().unwrap_or(0))
        .sum()
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_group_by_workflow_name() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=workflow_name").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], 50);
    assert_eq!(body["filtered_total"], 50);

    let groups = body["groups"].as_array().expect("groups array");
    let counts = counts_by(groups, "workflow_name");
    assert_eq!(counts.get("onboarding"), Some(&25));
    assert_eq!(counts.get("billing"), Some(&18));
    assert_eq!(counts.get("reporting"), Some(&7));
    assert_eq!(sum_group_counts(groups), 50, "counts reconcile to total");
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn aggregate_group_by_failure_signature() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=failure_signature").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let groups = body["groups"].as_array().expect("groups array");
    let counts = counts_by(groups, "failure_signature");
    assert_eq!(counts.get(SIG1), Some(&34), "counts: {counts:?}");
    assert_eq!(counts.get(SIG2), Some(&16), "counts: {counts:?}");
    assert_eq!(sum_group_counts(groups), 50);
}

#[tokio::test]
async fn aggregate_group_by_activity_name() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=activity_name").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let groups = body["groups"].as_array().expect("groups array");
    let counts = counts_by(groups, "activity_name");
    // charge_card is shared across onboarding(20)+billing(10)+reporting(4) = 34.
    assert_eq!(counts.get("charge_card"), Some(&34), "counts: {counts:?}");
    assert_eq!(sum_group_counts(groups), 50);
}

#[tokio::test]
async fn aggregate_hierarchical_workflow_then_signature() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(
        &app,
        "/dead-letters/aggregate?group_by=workflow_name&group_by=failure_signature",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let groups = body["groups"].as_array().expect("groups array");
    // 3 workflow names × 2 signatures = 6 groups.
    assert_eq!(
        groups.len(),
        6,
        "expected 6 hierarchical groups: {groups:?}"
    );

    // Find onboarding × sig1 == 20 and verify hierarchical key shape.
    let onboarding_sig1 = groups
        .iter()
        .find(|g| {
            g["key"]["workflow_name"] == "onboarding" && g["key"]["failure_signature"] == SIG1
        })
        .expect("onboarding × sig1 group must exist");
    assert_eq!(onboarding_sig1["count"], 20);

    assert_eq!(
        sum_group_counts(groups),
        50,
        "hierarchical counts reconcile"
    );
}

#[tokio::test]
async fn aggregate_filter_applies_before_grouping() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(
        &app,
        "/dead-letters/aggregate?workflow_name=onboarding&group_by=failure_signature",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], 50, "total is the unfiltered DLQ size");
    assert_eq!(body["filtered_total"], 25, "filter narrows to onboarding");

    let groups = body["groups"].as_array().expect("groups array");
    let counts = counts_by(groups, "failure_signature");
    assert_eq!(counts.get(SIG1), Some(&20));
    assert_eq!(counts.get(SIG2), Some(&5));
    assert_eq!(sum_group_counts(groups), 25);
}

#[tokio::test]
async fn aggregate_samples_per_group_is_capped() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(
        &app,
        "/dead-letters/aggregate?group_by=workflow_name&samples_per_group=2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    for group in body["groups"].as_array().expect("groups array") {
        let samples = group["sample_dead_letter_ids"]
            .as_array()
            .expect("sample ids array");
        assert!(samples.len() <= 2, "samples capped at 2: {samples:?}");
        assert!(
            !samples.is_empty(),
            "each group should carry at least one sample"
        );
    }
}

#[tokio::test]
async fn aggregate_limit_groups_rolls_into_other() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));
    seed_fixture(&database_url, 0).await;

    let (status, body) = get_json(
        &app,
        "/dead-letters/aggregate?group_by=workflow_name&group_by=failure_signature&limit_groups=2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["truncated"], true);

    let groups = body["groups"].as_array().expect("groups array");
    // 2 top groups + 1 _other rollup row.
    assert_eq!(groups.len(), 3);
    let other = groups.last().expect("last group");
    assert_eq!(other["key"]["_other"], true);
    // Totals still reconcile to filtered_total even with rollup.
    assert_eq!(sum_group_counts(groups), 50);
}

#[tokio::test]
async fn aggregate_invalid_group_by_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=tenant_id").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["detail"]
            .as_str()
            .unwrap_or("")
            .contains("unknown group_by dimension"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn aggregate_missing_group_by_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, body) = get_json(&app, "/dead-letters/aggregate?limit_groups=10").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["detail"]
            .as_str()
            .unwrap_or("")
            .contains("at least one group_by"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn aggregate_limit_groups_out_of_range_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, body) = get_json(
        &app,
        "/dead-letters/aggregate?group_by=queue_name&limit_groups=9999",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["detail"]
            .as_str()
            .unwrap_or("")
            .contains("limit_groups"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn aggregate_merges_counts_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let app = build_sharded_dlq_app(&shard0_url, &shard1_url);

    // Same fixture on both shards: counts must double after the cross-shard merge.
    seed_fixture(&shard0_url, 0).await;
    seed_fixture(&shard1_url, 1).await;

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=workflow_name").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["total"], 100, "total sums across shards");
    assert_eq!(body["filtered_total"], 100);

    let groups = body["groups"].as_array().expect("groups array");
    let counts = counts_by(groups, "workflow_name");
    assert_eq!(counts.get("onboarding"), Some(&50));
    assert_eq!(counts.get("billing"), Some(&36));
    assert_eq!(counts.get("reporting"), Some(&14));
    assert_eq!(sum_group_counts(groups), 100);
}
