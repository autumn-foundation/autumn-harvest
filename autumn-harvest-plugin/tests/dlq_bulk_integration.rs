use std::collections::BTreeMap;

use autumn_harvest::schema::{harvest_dead_letters, harvest_task_queue};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
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
);

type HarvestApiApp = axum::Router;

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

async fn insert_dlq_row(database_url: &str, activity_name: &str, task_type: &str) -> uuid::Uuid {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: task_type.to_string(),
            workflow_exec_id: None,
            activity_name: Some(activity_name.to_string()),
            input: json!({ "test": true }),
            error: format!("{activity_name} failed"),
            attempts: 3,
        },
    )
    .await
    .expect("dead-letter insert should succeed")
}

async fn count_dlq_rows(database_url: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter count");
    harvest_dead_letters::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead letters")
}

async fn count_task_queue_rows_by_activity(database_url: &str, activity_name: &str) -> i64 {
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

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bulk_replay_with_empty_filter_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, body) = post_json(&app, "/dead-letters/replay", json!({})).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("bulk filter must specify at least one criterion"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn bulk_discard_with_empty_filter_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, body) = post_json(&app, "/dead-letters/discard", json!({})).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("bulk filter must specify at least one criterion"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn bulk_replay_dry_run_previews_without_writing() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    for _ in 0..3 {
        insert_dlq_row(&database_url, "preview_task", "ACTIVITY").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "preview_task", "dry_run": true }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 3, "matched should be pre-limit total");
    assert_eq!(body["acted_on"], 0, "dry-run must not write");
    assert_eq!(body["dry_run"], true);
    let ids = body["ids"].as_array().expect("ids must be an array");
    assert_eq!(ids.len(), 3, "ids should list all matched rows");

    assert_eq!(
        count_dlq_rows(&database_url).await,
        3,
        "dry-run must not remove DLQ rows"
    );
    assert_eq!(
        count_task_queue_rows_by_activity(&database_url, "preview_task").await,
        0,
        "dry-run must not enqueue tasks"
    );
}

#[tokio::test]
async fn bulk_replay_single_shard_drains_dlq_and_enqueues_tasks() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    for _ in 0..3 {
        insert_dlq_row(&database_url, "replay_task", "ACTIVITY").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "replay_task" }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 3);
    assert_eq!(body["acted_on"], 3);
    assert_eq!(body["skipped"], 0);
    assert_eq!(body["dry_run"], false);
    let failures = body["failures"].as_array().expect("failures must be array");
    assert!(failures.is_empty(), "unexpected failures: {failures:?}");

    assert_eq!(
        count_dlq_rows(&database_url).await,
        0,
        "DLQ must be empty after replay"
    );
    assert_eq!(
        count_task_queue_rows_by_activity(&database_url, "replay_task").await,
        3,
        "each replayed DLQ entry must produce one task queue row"
    );
}

#[tokio::test]
async fn bulk_replay_across_shards_drains_both_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let app = build_sharded_dlq_app(&shard0_url, &shard1_url);

    for _ in 0..2 {
        insert_dlq_row(&shard0_url, "cross_shard_task", "ACTIVITY").await;
        insert_dlq_row(&shard1_url, "cross_shard_task", "ACTIVITY").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "cross_shard_task" }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 4, "matched must sum across both shards");
    assert_eq!(body["acted_on"], 4);
    let failures = body["failures"].as_array().expect("failures must be array");
    assert!(failures.is_empty(), "unexpected failures: {failures:?}");

    assert_eq!(
        count_dlq_rows(&shard0_url).await,
        0,
        "shard 0 DLQ must be empty"
    );
    assert_eq!(
        count_dlq_rows(&shard1_url).await,
        0,
        "shard 1 DLQ must be empty"
    );
}

#[tokio::test]
async fn bulk_discard_removes_entries_without_enqueueing() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    for _ in 0..3 {
        insert_dlq_row(&database_url, "discard_task", "ACTIVITY").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "activity_name": "discard_task" }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 3);
    assert_eq!(body["acted_on"], 3);
    let failures = body["failures"].as_array().expect("failures must be array");
    assert!(failures.is_empty(), "unexpected failures: {failures:?}");

    assert_eq!(
        count_dlq_rows(&database_url).await,
        0,
        "DLQ must be empty after discard"
    );
    assert_eq!(
        count_task_queue_rows_by_activity(&database_url, "discard_task").await,
        0,
        "discard must not enqueue tasks"
    );
}

#[tokio::test]
async fn bulk_replay_per_row_failure_does_not_halt_other_rows() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    insert_dlq_row(&database_url, "mixed_task", "ACTIVITY").await;
    let bad_id = insert_dlq_row(&database_url, "mixed_task", "INVALID").await;
    insert_dlq_row(&database_url, "mixed_task", "ACTIVITY").await;

    let (status, body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "mixed_task" }),
    )
    .await;

    // acted_on > 0 so handler must return 200, not 500
    assert_eq!(status, StatusCode::OK, "response body: {body}");
    assert_eq!(body["matched"], 3, "all 3 rows matched");
    assert_eq!(body["acted_on"], 2, "only the 2 valid rows were replayed");
    assert_eq!(body["skipped"], 0);
    let failures = body["failures"].as_array().expect("failures must be array");
    assert_eq!(failures.len(), 1, "one per-row failure expected");
    assert_eq!(
        failures[0]["id"].as_str().unwrap_or(""),
        bad_id.to_string(),
        "failure must identify the bad row"
    );

    assert_eq!(
        count_dlq_rows(&database_url).await,
        1,
        "invalid row must remain in DLQ after per-row failure"
    );
    assert_eq!(
        count_task_queue_rows_by_activity(&database_url, "mixed_task").await,
        2,
        "valid rows must have been enqueued despite the per-row failure"
    );
}

#[tokio::test]
async fn bulk_replay_after_drain_returns_zero_matched() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    insert_dlq_row(&database_url, "idempotent_task", "ACTIVITY").await;
    insert_dlq_row(&database_url, "idempotent_task", "ACTIVITY").await;

    // First call drains the DLQ
    let (first_status, first_body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "idempotent_task" }),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(first_body["acted_on"], 2);

    // Second call with same filter — rows are gone, nothing to match
    let (second_status, second_body) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "activity_name": "idempotent_task" }),
    )
    .await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body["matched"], 0, "no rows remain to match");
    assert_eq!(second_body["acted_on"], 0);
    let ids = second_body["ids"].as_array().expect("ids must be an array");
    assert!(ids.is_empty(), "no ids expected when nothing matched");
}
