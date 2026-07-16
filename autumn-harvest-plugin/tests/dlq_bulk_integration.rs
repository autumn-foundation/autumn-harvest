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
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
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
        conn.batch_execute(autumn_harvest::full_migrations_sql())
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

async fn get_json_bulk(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
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

            owner: None,
            severity: None,
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

// ---------------------------------------------------------------------------
// Cause-dimension bulk filters (issue #613)
// ---------------------------------------------------------------------------

fn poison_pill_error() -> String {
    autumn_harvest::dlq::DeadLetterReason::PoisonPill {
        crash_strikes: 3,
        last_worker_id: Some("worker-7".to_string()),
    }
    .to_string()
}

/// Insert a dead-letter row carrying an arbitrary `error` string.
async fn insert_dlq_row_with_error(
    database_url: &str,
    activity_name: &str,
    error: &str,
) -> uuid::Uuid {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: None,
            activity_name: Some(activity_name.to_string()),
            input: json!({ "test": true }),
            error: error.to_string(),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed")
}

async fn count_dlq_rows_by_activity(database_url: &str, activity_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter count");
    harvest_dead_letters::table
        .filter(harvest_dead_letters::activity_name.eq(activity_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead letters by activity")
}

async fn post_form(app: &HarvestApiApp, uri: &str, body: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST form request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

/// The AC5 round-trip money test: the bulk dry-run `matched` count for a cause
/// cohort equals the same cohort's count from the aggregate facet, proving both
/// surfaces share the classifier (lossless by construction).
#[tokio::test]
async fn bulk_cause_dry_run_count_equals_aggregate_facet() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    for _ in 0..4 {
        insert_dlq_row_with_error(&database_url, "pp", &poison_pill_error()).await;
    }
    for _ in 0..3 {
        insert_dlq_row_with_error(&database_url, "plain", "connection refused").await;
    }

    // Aggregate facet count for poison_pill.
    let (agg_status, agg_body) =
        get_json_bulk(&app, "/dead-letters/aggregate?group_by=dlq_reason").await;
    assert_eq!(agg_status, StatusCode::OK, "agg body: {agg_body}");
    let facet = agg_body["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .find(|g| g["key"]["dlq_reason"] == "poison_pill")
        .expect("poison_pill facet")["count"]
        .as_i64()
        .expect("count");
    assert_eq!(facet, 4);

    // Bulk discard dry-run for the same cohort must report the same matched.
    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "dry_run": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["matched"], facet, "discard matched must equal facet");
    assert_eq!(body["dry_run"], true);

    // Same for replay dry-run.
    let (rstatus, rbody) = post_json(
        &app,
        "/dead-letters/replay",
        json!({ "dlq_reason": "poison_pill", "dry_run": true }),
    )
    .await;
    assert_eq!(rstatus, StatusCode::OK, "body: {rbody}");
    assert_eq!(rbody["matched"], facet, "replay matched must equal facet");
}

#[tokio::test]
async fn bulk_discard_error_class_deletes_only_matching() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    for _ in 0..4 {
        insert_dlq_row_with_error(&database_url, "pp", &poison_pill_error()).await;
    }
    for _ in 0..3 {
        insert_dlq_row_with_error(&database_url, "plain", "connection refused").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "error_class": "PoisonPill" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["matched"], 4);
    assert_eq!(body["acted_on"], 4);

    assert_eq!(
        count_dlq_rows_by_activity(&database_url, "pp").await,
        0,
        "all PoisonPill rows discarded"
    );
    assert_eq!(
        count_dlq_rows_by_activity(&database_url, "plain").await,
        3,
        "non-matching rows must survive"
    );
}

#[tokio::test]
async fn bulk_cause_post_filter_precedes_limit() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    // Interleave poison_pill and plain rows so a limit applied BEFORE the cause
    // post-filter would clip the wrong rows.
    for _ in 0..5 {
        insert_dlq_row_with_error(&database_url, "pp", &poison_pill_error()).await;
        insert_dlq_row_with_error(&database_url, "plain", "connection refused").await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "limit": 3 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["matched"], 5,
        "matched counts all matching (pre-limit)"
    );
    assert_eq!(body["acted_on"], 3, "limit caps acted-on to 3");

    // Exactly 3 poison_pill rows discarded; every plain row untouched — proving
    // the cause post-filter ran before the limit clip.
    assert_eq!(
        count_dlq_rows_by_activity(&database_url, "pp").await,
        2,
        "3 of 5 poison_pill rows discarded"
    );
    assert_eq!(
        count_dlq_rows_by_activity(&database_url, "plain").await,
        5,
        "no plain rows may be discarded"
    );
}

#[tokio::test]
async fn bulk_empty_cause_filter_json_is_rejected_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, _body) =
        post_json(&app, "/dead-letters/discard", json!({ "dlq_reason": "" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty cause must 400");

    let (wstatus, _wbody) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "   " }),
    )
    .await;
    assert_eq!(
        wstatus,
        StatusCode::BAD_REQUEST,
        "whitespace-only cause must 400"
    );
}

#[tokio::test]
async fn bulk_empty_cause_filter_form_is_rejected_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    // An empty cause value in a form must be REJECTED, never silently dropped
    // (which would leave workflow_name=x as the only filter and widen scope).
    let (status, _body) =
        post_form(&app, "/dead-letters/discard", "dlq_reason=&workflow_name=x").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty form cause must 400");
}

#[tokio::test]
async fn bulk_cause_only_filter_is_not_rejected_as_empty() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    // A cause-only filter is substantive: it must not trip the empty-filter 400.
    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "dry_run": true }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "cause-only filter must be accepted: {body}"
    );
}

#[tokio::test]
async fn bulk_cause_across_shards_honors_global_limit() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_test_database_urls().await;
    let app = build_sharded_dlq_app(&shard0_url, &shard1_url);

    for _ in 0..4 {
        insert_dlq_row_with_error(&shard0_url, "pp", &poison_pill_error()).await;
        insert_dlq_row_with_error(&shard1_url, "pp", &poison_pill_error()).await;
    }

    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "limit": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["matched"], 8, "matched sums the cohort across shards");
    assert_eq!(
        body["acted_on"], 5,
        "global limit caps acted-on across shards"
    );

    let remaining = count_dlq_rows(&shard0_url).await + count_dlq_rows(&shard1_url).await;
    assert_eq!(remaining, 3, "8 matched - 5 acted = 3 remain");
}

/// Insert a dead-letter row on a specific `queue_name` with a specific
/// `attempts` count (issue #613, P2-1: exercise the SQL-expressible bulk filter
/// dimensions).
async fn insert_dlq_row_full(
    database_url: &str,
    activity_name: &str,
    queue_name: &str,
    attempts: i32,
    error: &str,
) -> uuid::Uuid {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: uuid::Uuid::new_v4(),
            queue_name: queue_name.to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: None,
            activity_name: Some(activity_name.to_string()),
            input: json!({ "test": true }),
            error: error.to_string(),
            attempts,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert should succeed")
}

async fn count_dlq_rows_by_queue(database_url: &str, queue_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for dead-letter count");
    harvest_dead_letters::table
        .filter(harvest_dead_letters::queue_name.eq(queue_name))
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count dead letters by queue")
}

/// The AC5 over-action superset money test (issue #613, P2-1): a cause facet
/// read scoped to one queue must re-select EXACTLY that queue's rows in a bulk
/// operation — never every queue's. Before the bulk filter learned
/// `queue_name`, an operator feeding a `?queue_name=qa` facet into a discard
/// would silently act across ALL queues.
#[tokio::test]
async fn bulk_cause_plus_queue_round_trips_exactly() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    // Same cause (poison_pill) split across two queues.
    for _ in 0..3 {
        insert_dlq_row_full(&database_url, "pp", "qa", 3, &poison_pill_error()).await;
    }
    for _ in 0..2 {
        insert_dlq_row_full(&database_url, "pp", "qb", 3, &poison_pill_error()).await;
    }

    // Aggregate facet scoped to qa reports 3 poison_pill rows.
    let (agg_status, agg_body) = get_json_bulk(
        &app,
        "/dead-letters/aggregate?queue_name=qa&group_by=dlq_reason",
    )
    .await;
    assert_eq!(agg_status, StatusCode::OK, "agg body: {agg_body}");
    let facet = agg_body["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .find(|g| g["key"]["dlq_reason"] == "poison_pill")
        .expect("poison_pill facet")["count"]
        .as_i64()
        .expect("count");
    assert_eq!(facet, 3, "aggregate facet scoped to qa sees only qa's rows");

    // Discard dry-run with the SAME scope must match exactly 3 (NOT 5).
    let (dstatus, dbody) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "queue_name": "qa", "dry_run": true }),
    )
    .await;
    assert_eq!(dstatus, StatusCode::OK, "body: {dbody}");
    assert_eq!(
        dbody["matched"], 3,
        "scoped discard must match only qa's cohort, not all queues"
    );

    // Execute it: only qa's 3 removed, qb's 2 survive.
    let (estatus, ebody) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "dlq_reason": "poison_pill", "queue_name": "qa" }),
    )
    .await;
    assert_eq!(estatus, StatusCode::OK, "body: {ebody}");
    assert_eq!(ebody["matched"], 3);
    assert_eq!(ebody["acted_on"], 3);

    assert_eq!(
        count_dlq_rows_by_queue(&database_url, "qa").await,
        0,
        "qa's cohort must be fully discarded"
    );
    assert_eq!(
        count_dlq_rows_by_queue(&database_url, "qb").await,
        2,
        "qb's rows must survive — the over-action gap is closed"
    );
}

/// `min_attempts` narrows a bulk operation to entries at or above the bound
/// (issue #613, P2-1).
#[tokio::test]
async fn bulk_min_attempts_filter_narrows() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    insert_dlq_row_full(&database_url, "flaky", "default", 1, "connection refused").await;
    insert_dlq_row_full(&database_url, "flaky", "default", 3, "connection refused").await;
    insert_dlq_row_full(&database_url, "flaky", "default", 5, "connection refused").await;

    // min_attempts=3 acts only on the attempts>=3 rows (the 3 and the 5).
    let (status, body) = post_json(
        &app,
        "/dead-letters/discard",
        json!({ "activity_name": "flaky", "min_attempts": 3 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["matched"], 2, "only attempts>=3 rows match");
    assert_eq!(body["acted_on"], 2);

    assert_eq!(
        count_dlq_rows(&database_url).await,
        1,
        "the attempts=1 row must survive"
    );
}

/// A non-positive `min_attempts` matches EVERY DLQ row (`attempts >= 0` is
/// universally true, attempt counts being 1-based), so on the DESTRUCTIVE bulk
/// path it must be rejected with `400` at the request boundary rather than
/// slipping past the empty-filter safety guard (AC8, issue #613). Covers both
/// the reported negative case and `0`; asserts nothing is acted on.
#[tokio::test]
async fn bulk_negative_min_attempts_is_rejected_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    insert_dlq_row_full(&database_url, "flaky", "default", 1, "connection refused").await;
    insert_dlq_row_full(&database_url, "flaky", "default", 3, "connection refused").await;

    // Negative: the reported all-rows-match bypass.
    let (neg_status, neg_body) =
        post_json(&app, "/dead-letters/discard", json!({ "min_attempts": -1 })).await;
    assert_eq!(
        neg_status,
        StatusCode::BAD_REQUEST,
        "negative min_attempts must 400, body: {neg_body}"
    );

    // Zero: `attempts >= 0` also matches every row.
    let (zero_status, zero_body) =
        post_json(&app, "/dead-letters/discard", json!({ "min_attempts": 0 })).await;
    assert_eq!(
        zero_status,
        StatusCode::BAD_REQUEST,
        "zero min_attempts must 400, body: {zero_body}"
    );

    // Neither malformed request touched the DLQ.
    assert_eq!(
        count_dlq_rows(&database_url).await,
        2,
        "a rejected bulk request must act on no rows"
    );
}
