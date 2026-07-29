//! HTTP integration tests for task-queue pause/resume (issue #619).
//!
//! Drives the three management routes end to end against a real Postgres:
//!
//!   * `POST   /admin/queues/{queue_name}/pause`
//!   * `POST   /admin/queues/{queue_name}/resume`
//!   * `GET    /admin/queues/paused`
//!
//! Coverage:
//!   (a) AC1/AC6 — pause returns 200, is durable, and writes an audit row
//!       carrying the operator identity and the human-readable reason.
//!   (b) AC7 — the read endpoint lists paused queues with `paused_at`,
//!       `reason` and a held-task count.
//!   (c) AC1 — pause is idempotent: a repeat returns `newly_paused: false` and
//!       preserves the ORIGINAL provenance (actor/reason/paused_at).
//!   (d) AC1/AC6 — resume returns 200, clears the row, and audits.
//!   (e) Resume of an unpaused queue is an idempotent no-op (never a 404/500).
//!   (f) A blank or oversized queue name is rejected 400 (never a 500).
//!   (g) Admin gate: without the admin boundary the mutating routes 401.
//!   (h) AC1 durability — a pause survives a full teardown of the API state
//!       (the "plugin restart" proxy): a fresh router over the same database
//!       still reports the queue as held, because the gate is a row, not a
//!       process-local cache.

// Test-code style lints, consistent with `queue_pause_tests.rs` in the core
// crate: the module docs name response/database fields verbatim.
#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

/// Actor identity sent as `x-harvest-actor` — the header the default
/// `HarvestApiState::extract_actor` reads into audit rows.
const TEST_ACTOR: &str = "alice@ops";

/// Swap the database-name path segment of a Postgres URL, preserving any query
/// string (so a fresh per-test database can be addressed off an admin URL).
fn replace_db_name(url: &str, db: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// Provision a pristine, migrated Postgres database for one test.
///
/// When `HARVEST_TEST_DATABASE_URL` is set it is treated as an admin base URL
/// and a fresh, uniquely-named database is created on that server, so the
/// exact-count assertions are never contaminated by a sibling test. Otherwise a
/// throwaway testcontainers Postgres is booted (requires Docker).
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };

    let fresh_db = format!("harvest_qpause_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {fresh_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("create fresh test database");

    let db_url = replace_db_name(&admin_url, &fresh_db);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&db_url)
        .await
        .expect("connect to fresh test database");
    diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        autumn_harvest::full_migrations_sql(),
    )
    .await
    .expect("apply migrations to fresh test database");

    (db_url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with_admin_boundary(pool, true)
}

/// Without the outer-auth-boundary declaration the built-in
/// `require_harvest_admin` guard applies — mirrors the `build_app_no_admin`
/// precedent in `fail_now_integration.rs`.
fn build_app_no_admin(pool: &DbPool) -> HarvestApiApp {
    build_app_with_admin_boundary(pool, false)
}

fn build_app_with_admin_boundary(pool: &DbPool, boundary: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if boundary {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("queue-pause-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn read_json(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    read_json(response).await
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .header("x-harvest-actor", TEST_ACTOR)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    read_json(response).await
}

/// Enqueue `n` PENDING activity tasks on `queue` so the held-task count is
/// non-trivial.
async fn seed_pending_tasks(pool: &DbPool, queue: &str, n: usize) {
    let mut conn = pool.get().await.expect("pooled conn");
    for _ in 0..n {
        let exec_id = autumn_harvest::types::ExecutionId::new();
        diesel::sql_query(
            "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, queue_name) \
             VALUES ($1, 'qp_wf', $2, 0, '{}'::jsonb, $3)",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("insert execution");

        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
             (id, workflow_exec_id, task_type, queue_name, state, scheduled_at, input) \
             VALUES (gen_random_uuid(), $1, 'activity', $2, 'PENDING', NOW(), '{}'::jsonb)",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Text, _>(queue)
        .execute(&mut conn)
        .await
        .expect("insert task");
    }
}

#[derive(diesel::QueryableByName, Debug)]
struct AuditRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    operation: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_type: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    route_or_command: String,
}

async fn audit_rows(pool: &DbPool, operation: &str) -> Vec<AuditRow> {
    let mut conn = pool.get().await.expect("pooled conn");
    diesel::sql_query(
        "SELECT operation, actor, target_type, target_id, status, route_or_command \
         FROM harvest_audit_log WHERE operation = $1 ORDER BY occurred_at",
    )
    .bind::<diesel::sql_types::Text, _>(operation)
    .load::<AuditRow>(&mut conn)
    .await
    .expect("load audit rows")
}

// ── (a) + (b) + (c) + (d): the full operator round trip ─────────────────────

/// AC1/AC6/AC7: pause → list → resume, with audit rows on both mutations and
/// the held-task count surfaced on the read.
#[tokio::test]
async fn pause_list_resume_round_trip_with_audit() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_pending_tasks(&pool, "email-workers", 3).await;

    // ── pause ────────────────────────────────────────────────────────────
    let (status, body) = post_json(
        &app,
        "/admin/queues/email-workers/pause",
        json!({"reason": "SMTP provider outage"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "pause body: {body}");
    assert_eq!(body["queue_name"], "email-workers");
    assert_eq!(body["newly_paused"], true);
    assert_eq!(body["reason"], "SMTP provider outage");
    assert_eq!(body["paused_by"], TEST_ACTOR);
    assert_eq!(
        body["held_task_count"], 3,
        "the pause response reports how much work is held"
    );
    assert!(
        body["paused_at"].is_string(),
        "paused_at must be surfaced: {body}"
    );

    // ── list (AC7) ───────────────────────────────────────────────────────
    let (status, body) = get_json(&app, "/admin/queues/paused").await;
    assert_eq!(status, StatusCode::OK, "list body: {body}");
    let queues = body["paused_queues"].as_array().expect("queues array");
    assert_eq!(queues.len(), 1, "exactly one paused queue: {body}");
    let q = &queues[0];
    assert_eq!(q["queue_name"], "email-workers");
    assert_eq!(q["reason"], "SMTP provider outage");
    assert_eq!(q["paused_by"], TEST_ACTOR);
    assert_eq!(q["held_task_count"], 3);
    assert!(q["paused_at"].is_string());

    // ── idempotent repeat (AC1) ──────────────────────────────────────────
    let (status, body) = post_json(
        &app,
        "/admin/queues/email-workers/pause",
        json!({"reason": "a second, different reason"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "repeat pause body: {body}");
    assert_eq!(
        body["newly_paused"], false,
        "a repeat pause is a no-op, not an error"
    );
    assert_eq!(
        body["reason"], "SMTP provider outage",
        "the ORIGINAL provenance is preserved — a repeat never rewrites who paused it or why"
    );
    assert_eq!(body["paused_by"], TEST_ACTOR);

    // ── resume ───────────────────────────────────────────────────────────
    let (status, body) = post_json(&app, "/admin/queues/email-workers/resume", json!({})).await;
    assert_eq!(status, StatusCode::OK, "resume body: {body}");
    assert_eq!(body["queue_name"], "email-workers");
    assert_eq!(body["newly_resumed"], true);
    assert_eq!(
        body["released_task_count"], 3,
        "resume reports how much work it released"
    );
    assert!(
        body["paused_duration_secs"].is_i64(),
        "resume reports how long the hold lasted: {body}"
    );
    assert_eq!(
        body["released_reason"], "SMTP provider outage",
        "resume echoes the reason it is releasing -- the pause row is deleted, \
         so this is the last moment the *why* is recoverable: {body}"
    );
    assert_eq!(body["released_paused_by"], TEST_ACTOR);

    let (status, body) = get_json(&app, "/admin/queues/paused").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["paused_queues"]
            .as_array()
            .expect("queues array")
            .is_empty(),
        "nothing is paused after resume: {body}"
    );

    // ── audit (AC6) ──────────────────────────────────────────────────────
    let pauses = audit_rows(&pool, "queue.pause").await;
    assert_eq!(
        pauses.len(),
        2,
        "both the fresh pause AND the idempotent repeat are audited"
    );
    assert_eq!(pauses[0].actor, TEST_ACTOR, "operator identity is recorded");
    assert_eq!(pauses[0].operation, "queue.pause");
    assert_eq!(pauses[0].target_type, "queue");
    assert_eq!(pauses[0].target_id, "email-workers");
    assert_eq!(pauses[0].status, "succeeded");
    assert_eq!(
        pauses[0].route_or_command,
        "POST /admin/queues/{queue_name}/pause"
    );

    let resumes = audit_rows(&pool, "queue.resume").await;
    assert_eq!(resumes.len(), 1);
    assert_eq!(resumes[0].actor, TEST_ACTOR);
    assert_eq!(resumes[0].target_type, "queue");
    assert_eq!(resumes[0].target_id, "email-workers");
    assert_eq!(resumes[0].status, "succeeded");
    assert_eq!(
        resumes[0].route_or_command,
        "POST /admin/queues/{queue_name}/resume"
    );
}

// ── (e): resume of an unpaused queue is a no-op ─────────────────────────────

/// Resuming a queue that is not paused is an idempotent success, never a 404 or
/// a 500 — an operator retrying a resume after a lost response must not have to
/// distinguish "already resumed" from "failed".
#[tokio::test]
async fn resume_of_an_unpaused_queue_is_an_idempotent_no_op() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(&app, "/admin/queues/never-paused/resume", json!({})).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["newly_resumed"], false);
    assert_eq!(body["released_task_count"], 0);
}

// ── (f): input validation ───────────────────────────────────────────────────

/// A blank or oversized queue name is rejected with a structured 400, never a
/// 500 and never a silently-created unusable row.
#[tokio::test]
async fn invalid_queue_names_are_rejected_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Whitespace-only.
    let (status, body) =
        post_json(&app, "/admin/queues/%20%20/pause", json!({"reason": "x"})).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "blank queue name must be 400: {body}"
    );

    // Over the 255-char cap.
    let long = "q".repeat(256);
    let (status, body) = post_json(
        &app,
        &format!("/admin/queues/{long}/pause"),
        json!({"reason": "x"}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "oversized queue name must be 400: {body}"
    );

    // Nothing was created.
    let (status, body) = get_json(&app, "/admin/queues/paused").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["paused_queues"]
            .as_array()
            .expect("queues array")
            .is_empty(),
        "a rejected request must not create a pause row: {body}"
    );
}

// ── (g): admin gate ─────────────────────────────────────────────────────────

/// Without the outer auth boundary the built-in admin guard rejects both
/// mutating routes — pausing dispatch fleet-wide is an admin-only action.
#[tokio::test]
async fn pause_and_resume_require_admin() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_no_admin(&pool);

    for (uri, body) in [
        ("/admin/queues/email-workers/pause", json!({"reason": "x"})),
        ("/admin/queues/email-workers/resume", json!({})),
    ] {
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
            .expect("POST request");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{uri} must be admin-gated"
        );
    }
}

// ── (h): AC1 durability across a "plugin restart" ───────────────────────────

/// AC1: the paused state is durable and is re-applied before the worker pool
/// begins claiming. Because the gate is a ROW consulted by the claim query —
/// not a process-local cache seeded at boot — a completely fresh router over
/// the same database observes the hold with no re-application step and no boot
/// window in which a held queue is briefly claimable.
#[tokio::test]
async fn pause_survives_a_full_api_state_teardown() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);

    {
        let app = build_app(&pool);
        let (status, _) = post_json(
            &app,
            "/admin/queues/email-workers/pause",
            json!({"reason": "downstream outage"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // `app` (and its whole HarvestApiState) is dropped here.
    }

    // A brand-new router + API state over the same database.
    let restarted = build_app(&pool);
    let (status, body) = get_json(&restarted, "/admin/queues/paused").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let queues = body["paused_queues"].as_array().expect("queues array");
    assert_eq!(
        queues.len(),
        1,
        "the pause survived the restart with no re-application step: {body}"
    );
    assert_eq!(queues[0]["queue_name"], "email-workers");
    assert_eq!(queues[0]["reason"], "downstream outage");

    // And the claim gate itself still holds — the authoritative check, not just
    // the read model.
    let mut conn = pool.get().await.expect("pooled conn");
    assert!(
        autumn_harvest::queue_pause::is_queue_paused(&mut conn, "email-workers")
            .await
            .expect("is_queue_paused"),
        "the claim-time gate must still see the queue as held after a restart"
    );
}

// ── Post-review hardening (AC7 multi-shard write path, partial reporting) ─────

/// Build a two-shard app: shard 0 is the live database, shard 1 points at an
/// unreachable URL. Mirrors the `workflow_reachability_integration.rs`
/// partial-shard harness.
fn build_two_shard_app(pool: &DbPool) -> HarvestApiApp {
    use autumn_harvest::shard::ShardedDbPool;
    use autumn_harvest::types::ShardId;

    // Shard 0 is the live database; shard 1 points at an unreachable URL, so
    // every operation against it fails at connection-acquire time.
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), pool.clone());
    shard_pools.insert(
        ShardId::new(1),
        build_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent"),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(ShardedDbPool::from_map(
        shard_pools,
        ShardId::new(0),
    )));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("queue-pause-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            ShardId::new(0),
        ),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// AC7, the write half: `shard_id` scopes the hold to one shard, and the read
/// endpoint reports that scope back.
///
/// Every other test in this file exercises the fleet-wide default; this one
/// pins the *scoped* branch of `queue_pause_target_pools`, which is the half an
/// operator reaches for when only one shard's downstream is degraded.
#[tokio::test]
async fn a_shard_scoped_pause_targets_only_that_shard() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_two_shard_app(&pool);

    // Shard 0 is the live one, so a hold scoped to it must fully apply (200)
    // even though shard 1 is unreachable — a scoped request must not fan out.
    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": "shard-0 provider degraded", "shard_id": 0 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a hold scoped to a reachable shard must fully apply: {body}"
    );
    assert_eq!(body["status"], "complete");
    assert_eq!(body["ok"], true);
    assert_eq!(body["scope_shard_id"], 0);
    assert!(
        body.get("partial_failures").is_none(),
        "a fully-applied scoped hold must not report partial failures: {body}"
    );

    let (status, listed) = get_json(&app, "/admin/queues/paused").await;
    assert_eq!(status, StatusCode::OK);
    let entry = listed["paused_queues"]
        .as_array()
        .expect("paused_queues array")
        .iter()
        .find(|q| q["queue_name"] == "payments")
        .expect("the scoped hold must be listed");
    assert_eq!(
        entry["scope_shard_id"], 0,
        "the read endpoint must report the shard the hold was scoped to (AC7): {listed}"
    );
}

/// A *fleet-wide* hold that reaches only some shards is NOT in effect
/// fleet-wide, and must not be reported as an unqualified success.
///
/// The shards it missed keep dispatching into exactly the outage the operator
/// is riding out, so a `200 {"ok": true}` here is the one way this feature can
/// silently fail an incident. Asserts the honest `207` / `ok: false` /
/// `status: "partial"` shape instead.
#[tokio::test]
async fn a_partially_applied_fleet_wide_pause_is_reported_as_partial() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_two_shard_app(&pool);

    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": "provider outage" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::MULTI_STATUS,
        "a hold that reached only some shards must NOT report 200: {body}"
    );
    assert_eq!(body["ok"], false, "partial application is not ok: {body}");
    assert_eq!(body["status"], "partial");
    assert!(
        body["partial_failures"]
            .as_str()
            .is_some_and(|s| s.contains("shard 1")),
        "the unreached shard must be named so the operator can act on it: {body}"
    );
}

/// A `shard_id` that no shard router knows is a `400`; the storage-pool guard
/// still answers `503`. Both are audited as failed attempts.
#[tokio::test]
async fn an_unknown_shard_id_is_rejected_and_audited() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_two_shard_app(&pool);

    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": "outage", "shard_id": 99 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an unknown shard id is a client error, not a 500: {body}"
    );

    // The rejected attempt on a fleet-wide dispatch kill switch must leave a
    // trail — an incident review needs the attempts, not only the successes.
    let rows = audit_rows(&pool, "queue.pause").await;
    assert!(
        rows.iter().any(|r| r.status == "failed"),
        "a rejected pause must be audited as failed: {rows:?}"
    );
}

/// A malformed body and a blank reason are likewise audited, not silently
/// dropped.
#[tokio::test]
async fn rejected_pause_attempts_are_audited() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, _) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "shard_id": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing reason is a 400");

    let (status, _) = post_json(&app, "/admin/queues/%20%20/pause", json!({ "reason": "x" })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "blank queue name is a 400");

    let rows = audit_rows(&pool, "queue.pause").await;
    assert_eq!(
        rows.iter().filter(|r| r.status == "failed").count(),
        2,
        "both rejected attempts must be audited: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r.actor == TEST_ACTOR),
        "a rejected attempt must record WHO attempted it: {rows:?}"
    );
}

/// The operator `reason` is bounded at the API boundary, so it can never be
/// persisted (and re-rendered into the Vantage Workers page on every load) at
/// an unbounded size.
#[tokio::test]
async fn an_oversized_reason_is_truncated_not_stored_whole() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let huge = "x".repeat(5_000);
    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": huge }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored = body["reason"].as_str().expect("reason echoed");
    assert!(
        stored.len() <= 500,
        "the durable operator reason must be capped at the API boundary, got {} bytes",
        stored.len()
    );
}

/// An idempotent re-pause must echo the hold that is actually in effect —
/// including its **scope** (issue #619 review, Codex).
///
/// `pause_queue` deliberately preserves the ORIGINAL provenance on a re-pause,
/// and the handler already echoes the preserved `reason`/`paused_by`/`paused_at`.
/// The scope must travel with them: re-pausing a shard-scoped hold with a
/// fleet-wide request previously reported `scope_shard_id: null` alongside
/// `newly_paused: false` and the original reason — describing a hold that
/// covers one shard as though it covered the fleet, which is exactly the
/// mis-read that lets an operator believe an outage is contained when it is not.
#[tokio::test]
async fn an_idempotent_repause_echoes_the_effective_scope_not_the_request() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Original hold: scoped to shard 0.
    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": "shard-0 provider degraded", "shard_id": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "first pause must apply: {body}");
    assert_eq!(body["newly_paused"], true);
    assert_eq!(body["scope_shard_id"], 0);

    // Re-pause with NO shard_id (a fleet-wide request). The core preserves the
    // original row, so the response must describe THAT row, not this request.
    let (status, body) = post_json(
        &app,
        "/admin/queues/payments/pause",
        json!({ "reason": "different reason, ignored on a no-op" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "re-pause is an idempotent no-op: {body}"
    );
    assert_eq!(
        body["newly_paused"], false,
        "the queue was already held, so nothing transitioned: {body}"
    );
    assert_eq!(
        body["reason"], "shard-0 provider degraded",
        "the ORIGINAL reason is preserved (pre-existing contract): {body}"
    );
    assert_eq!(
        body["scope_shard_id"], 0,
        "the effective scope must be preserved alongside the reason -- reporting \
         null here would describe a shard-scoped hold as fleet-wide: {body}"
    );
}
