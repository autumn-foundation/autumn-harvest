//! Integration tests for partial-availability of cross-shard fan-out READ
//! endpoints (issue #756).
//!
//! When one shard's pool is unreachable, the cross-shard list/aggregate read
//! endpoints (`GET /workflows`, `/workers`, `/workers/health`, `/admin/schedules`,
//! `/dead-letters`, `/dead-letters/aggregate`) must return `200 OK` with the
//! union of the *reachable* shards' results plus a machine-readable
//! `unavailable_shards` indicator — never a whole-request `500`. WRITES keep
//! strict failure semantics, and the single-execution business-id resolver
//! (#805) keeps its 503-to-avoid-false-404 behavior.
//!
//! Dual-mode: uses a running Postgres from `HARVEST_TEST_DATABASE_URL` (as an
//! admin URL onto which two fresh shard databases are created) when set — no
//! Docker required — else boots a fresh testcontainers Postgres. Docker is not
//! available in every sandbox, so these tests are compile-checked there and run
//! Docker-backed in CI (matching the #543/#544/#601 precedent).

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
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

/// Derive a per-shard database URL from the admin/base URL, replacing the
/// database-name path segment with `dbname` while preserving any query string
/// (e.g. `?sslmode=require`). The query is split off *before* the path segment
/// is swapped so it is never lost. A URL with no query yields
/// `{prefix}/{dbname}`, byte-identical to the previous `rsplit_once('/')` build.
fn shard_url(base_url: &str, dbname: &str) -> String {
    let (base, query) = match base_url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (base_url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    query.map_or_else(
        || format!("{prefix}/{dbname}"),
        |q| format!("{prefix}/{dbname}?{q}"),
    )
}

#[test]
fn shard_url_preserves_query_params() {
    // No query — byte-identical to the old `rsplit_once('/')` construction.
    assert_eq!(
        shard_url("postgres://u:p@host:5432/postgres", "shard0"),
        "postgres://u:p@host:5432/shard0"
    );
    // Query params (sslmode, application_name, ...) must survive the swap.
    assert_eq!(
        shard_url(
            "postgres://u:p@host:5432/postgres?sslmode=require",
            "shard0"
        ),
        "postgres://u:p@host:5432/shard0?sslmode=require"
    );
    assert_eq!(
        shard_url(
            "postgres://u:p@host:5432/postgres?sslmode=require&application_name=harvesttest",
            "shard1"
        ),
        "postgres://u:p@host:5432/shard1?sslmode=require&application_name=harvesttest"
    );
}

/// Two independent shard databases plus an optional container guard.
///
/// With `HARVEST_TEST_DATABASE_URL` set, two fresh databases are created and
/// migrated on that server (guard `None`); otherwise a fresh testcontainers
/// Postgres backs both shard databases (guard `Some`).
async fn setup_two_shards() -> ((String, String), Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let s0 = format!("harvest_shard_{}", Uuid::new_v4().simple());
    let s1 = format!("harvest_shard_{}", Uuid::new_v4().simple());

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in [&s0, &s1] {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }

    // Address the freshly-created shard databases by swapping the database-name
    // path segment, preserving any query string (e.g. `?sslmode=require`) that
    // the admin URL may carry.
    let url0 = shard_url(&admin_url, &s0);
    let url1 = shard_url(&admin_url, &s1);
    for url in [&url0, &url1] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("shard connect");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate shard");
    }
    ((url0, url1), guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Sharded, admin-gated app. `shard1_down = true` points shard 1 at a
/// non-existent database so its pool fails to acquire a connection.
fn build_app(url0: &str, url1: &str, shard1_down: bool) -> HarvestApiApp {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(url0));
    let shard1_url = if shard1_down {
        // Reuse the shard-1 URL but retarget it at a database that does not
        // exist, so `pool.get()` (acquire) fails at connect time.
        url1.replace("harvest_shard_", "missing_db_")
    } else {
        url1.to_string()
    };
    pools.insert(ShardId::new(1), build_pool(&shard1_url));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn post_status(app: &HarvestApiApp, uri: &str) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .expect("request");
    response.status()
}

async fn seed_execution(url: &str, shard: i32, workflow_name: &str, state: &str) -> (Uuid, String) {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("{workflow_name}-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &wf_id,
        run_id: Uuid::new_v4(),
        shard_id: shard,
        input: serde_json::json!({}),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert");
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set(dsl::state.eq(state))
        .execute(&mut conn)
        .await
        .expect("force state");
    (exec_id.as_uuid(), wf_id)
}

async fn seed_dead_letter(url: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::sql_query(
        "INSERT INTO harvest_dead_letters \
             (id, original_task_id, queue_name, task_type, input, error, attempts, failed_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), 'default', 'activity', \
                 '{}'::jsonb, 'boom', 3, NOW())",
    )
    .execute(&mut conn)
    .await
    .expect("seed dead letter");
}

async fn seed_worker(url: &str, worker_id: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let sql = format!(
        "INSERT INTO harvest_workers \
            (worker_id, last_heartbeat_at, status, queues, shard_assignments, max_concurrency, host) \
         VALUES \
            ('{worker_id}', NOW(), 'Active', '[]'::jsonb, '[0]'::jsonb, 10, 'test-host') \
         ON CONFLICT (worker_id) DO NOTHING"
    );
    conn.batch_execute(&sql).await.expect("seed worker");
}

/// The degraded envelope names the down shard with a reason.
fn assert_names_down_shard(body: &Value) {
    let unavailable = body["unavailable_shards"]
        .as_array()
        .expect("unavailable_shards array present on a degraded response");
    let named = unavailable
        .iter()
        .find(|u| u["shard_id"] == 1)
        .expect("the down shard (1) must be named in unavailable_shards");
    assert!(
        !named["reason"].as_str().unwrap_or("").is_empty(),
        "the down shard must carry a non-empty reason"
    );
    assert_eq!(body["status"], "partial", "status must be partial");
}

/// Seed a child execution under `parent` on `shard`.
///
/// `GET /workflows/{id}/children` traverses `parent_id` across every shard, so a
/// cross-shard child (issue #956) is discoverable only through that fan-out —
/// which is exactly why the endpoint has to degrade rather than `500` when one
/// shard is down.
async fn seed_child_execution(
    url: &str,
    shard: i32,
    parent: Uuid,
    workflow_name: &str,
    state: &str,
) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("{workflow_name}-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &wf_id,
        run_id: Uuid::new_v4(),
        shard_id: shard,
        input: serde_json::json!({}),
        parent_id: Some(parent),
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
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert child");
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set(dsl::state.eq(state))
        .execute(&mut conn)
        .await
        .expect("force child state");
    exec_id.as_uuid()
}

// ── #956 AC7: GET /workflows/{id}/children degrades to a partial 200 ─────────

/// A parent's children view must report an unreachable shard rather than `500`.
///
/// This traversal always spanned every shard — a child could live anywhere
/// because it follows `parent_id` across the fleet — but it propagated a pool
/// error with `?`, so one unreachable shard turned the whole call into a `500`.
/// Cross-shard child *placement* (issue #956) makes that failure routine rather
/// than exotic: a fan-out deliberately puts children on other shards, so a
/// single shard being down is now the expected reason a parent's children view
/// is incomplete.
#[tokio::test]
async fn get_workflow_children_partial_names_down_shard_and_returns_reachable_children() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let (parent, _wf) = seed_execution(&url0, 0, "orchestrator", "RUNNING").await;
    let child = seed_child_execution(&url0, 0, parent, "process_one", "COMPLETED").await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, &format!("/workflows/{parent}/children")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a down shard must not 500 a parent's children view; got {body}"
    );
    assert_names_down_shard(&body);

    let items = body["items"]
        .as_array()
        .expect("children response carries an items array");
    assert_eq!(
        items.len(),
        1,
        "the reachable shard's child must be present"
    );
    assert_eq!(items[0]["exec_id"], child.to_string());
}

/// With every shard reachable the response is `complete` and names nothing —
/// the additive-field contract (#756 AC3) applied to this endpoint.
#[tokio::test]
async fn get_workflow_children_happy_path_reports_complete() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let (parent, _wf) = seed_execution(&url0, 0, "orchestrator", "RUNNING").await;
    seed_child_execution(&url0, 0, parent, "process_one", "COMPLETED").await;
    let app = build_app(&url0, &url1, false);

    let (status, body) = get_json(&app, &format!("/workflows/{parent}/children")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete", "got {body}");
    assert_eq!(
        body["unavailable_shards"]
            .as_array()
            .expect("unavailable_shards is always present")
            .len(),
        0
    );
    assert_eq!(body["items"].as_array().expect("items").len(), 1);
}

/// The depth-traversal variant degrades too: a shard that fails at *any* depth
/// level is degraded for the whole walk, because a missed level's children are
/// missed descendants.
#[tokio::test]
async fn get_workflow_children_tree_traversal_also_degrades() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let (parent, _wf) = seed_execution(&url0, 0, "orchestrator", "RUNNING").await;
    let child = seed_child_execution(&url0, 0, parent, "process_one", "COMPLETED").await;
    seed_child_execution(&url0, 0, child, "grandchild", "COMPLETED").await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, &format!("/workflows/{parent}/children?depth=2")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a down shard must not 500 the depth traversal; got {body}"
    );
    assert_names_down_shard(&body);
    assert_eq!(
        body["items"].as_array().expect("items").len(),
        2,
        "both reachable descendants must still be returned"
    );
}

// ── AC7: GET /workflows degrades to a partial 200 ────────────────────────────

#[tokio::test]
async fn get_workflows_partial_names_down_shard_and_returns_reachable_runs() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let (_exec, _wf) = seed_execution(&url0, 0, "onboarding", "RUNNING").await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/workflows").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a down shard must not 500; got {body}"
    );
    assert_names_down_shard(&body);
    // The reachable shard's run still flows through the degraded envelope.
    let workflows = body["workflows"]
        .as_array()
        .expect("degraded envelope has a workflows array");
    assert_eq!(workflows.len(), 1, "shard 0's run must be present");
    assert_eq!(workflows[0]["workflow_name"], "onboarding");
}

// ── AC3: happy path is an unchanged bare array ───────────────────────────────

#[tokio::test]
async fn get_workflows_happy_path_returns_bare_array() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_execution(&url0, 0, "onboarding", "RUNNING").await;
    let app = build_app(&url0, &url1, false);

    let (status, body) = get_json(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.is_array(),
        "the happy path is byte-for-byte a bare JSON array, not an envelope: {body}"
    );
    assert_eq!(body.as_array().unwrap().len(), 1);
}

// ── AC1/AC2: the other list/aggregate reads degrade too ──────────────────────

#[tokio::test]
async fn get_workers_partial_names_down_shard() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_worker(&url0, "worker-a").await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    assert_names_down_shard(&body);
    let workers = body["workers"].as_array().expect("workers array");
    assert_eq!(workers.len(), 1, "the reachable worker still appears");
}

#[tokio::test]
async fn get_schedules_partial_names_down_shard() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/admin/schedules").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    assert_names_down_shard(&body);
    assert!(body["schedules"].is_array());
}

#[tokio::test]
async fn get_dead_letters_partial_names_down_shard() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_dead_letter(&url0).await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/dead-letters").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    assert_names_down_shard(&body);
    let dls = body["dead_letters"].as_array().expect("dead_letters array");
    assert_eq!(dls.len(), 1, "the reachable dead-letter still appears");
}

#[tokio::test]
async fn dead_letters_aggregate_partial_carries_status() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_dead_letter(&url0).await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/dead-letters/aggregate?group_by=workflow_name").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    assert_names_down_shard(&body);
    // Additive object fields — the aggregate object keeps its own shape.
    assert!(body["groups"].is_array());
    // The reachable shard's seeded dead-letter actually merged into the count,
    // proving the partial aggregate is real data and not an empty degradation.
    assert!(
        body["total"].as_i64().unwrap_or(0) >= 1,
        "the reachable shard's dead-letter must be counted in total; got {body}"
    );
}

#[tokio::test]
async fn workers_health_partial_carries_status() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    seed_worker(&url0, "worker-a").await;
    let app = build_app(&url0, &url1, true);

    let (status, body) = get_json(&app, "/workers/health").await;
    assert_eq!(status, StatusCode::OK, "got {body}");
    assert_names_down_shard(&body);
    // Additive: the FleetHealth fields stay at the top level.
    assert!(body["healthy"].is_number());
    assert!(body["by_queue"].is_object());
}

// ── AC4: writes still fail hard on a down shard ──────────────────────────────

#[tokio::test]
async fn write_still_fails_when_a_shard_is_down() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let app = build_app(&url0, &url1, true);

    // A single-shard-scan write (`replay_dead_letter_from_shards`) hard-`?`s on
    // the first unreachable shard — it must NOT be relaxed to a partial success.
    let random = Uuid::new_v4();
    let status = post_status(&app, &format!("/dead-letters/{random}/replay")).await;
    assert!(
        status.is_server_error(),
        "a cross-shard write must still fail (>=500) when a shard is down; got {status}"
    );
}

// ── AC5: the #805 business-id read still 503s (no false 404) ──────────────────

#[tokio::test]
async fn by_id_read_still_503s_when_a_shard_is_down() {
    let ((url0, url1), _guard) = setup_two_shards().await;
    let app = build_app(&url0, &url1, true);

    // The business-id resolver cannot confirm absence while a shard is down, so
    // it returns 503 rather than a false 404 — this must not regress into a
    // partial-availability 200.
    let status = get_json(&app, "/workflows/by-id/onboarding/does-not-exist")
        .await
        .0;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the #805 by-id resolver must keep 503-to-avoid-false-404 semantics"
    );
}
