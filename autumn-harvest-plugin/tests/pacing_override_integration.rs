#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
//! HTTP integration tests for TTL'd runtime pacing overrides (issue #945).
//!
//! Proves the success metric end to end, against the real dispatch/admission
//! primitives (not just the pure `resolve_effective_rate_limit` helper unit
//! tests already covering the math in isolation):
//!
//! - `POST /admin/rate-limits/{activity_name}/override` takes effect on the
//!   **existing** `claim_task` dispatch path with **no worker restart** — the
//!   very next claim attempt against the same connection pool observes it.
//! - The override **self-expires** at its TTL with **no operator action** —
//!   nothing clears the row; Postgres's own `> NOW()` comparison inside
//!   `claim_task_query` simply stops matching once the deadline passes.
//! - `DELETE .../override` clears an override **before** its TTL, and that
//!   also takes effect immediately on the same dispatch path.
//! - The workflow-start throttle side (`POST /admin/start-throttle/{name}/
//!   override`) is gated by the very same `harvest_rate_limit_buckets` /
//!   `try_consume_rate_limit_token` mechanism, exercised here through the
//!   real `POST /workflows/{name}/start` admission path (not a bare call to
//!   the underlying primitive) — the most literal reading of "one CLI call
//!   changes downstream behaviour with no restart".
//!
//! Tests run against a real Postgres (set `HARVEST_TEST_DATABASE_URL` to a
//! migrated database to run directly with `--test-threads=1`, otherwise a
//! fresh testcontainers Postgres is booted with the full migration set).

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::queue::{self, EnqueueParams, TaskType, claim_task};
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

/// Leak a UNIQUE `&'static str` name so each test's activity/workflow (and
/// therefore its `harvest_rate_limit_buckets`/`harvest_start_throttle` rows)
/// is isolated even when `HARVEST_TEST_DATABASE_URL` points every test in
/// this binary -- and every past/future `cargo test` invocation -- at the
/// SAME shared local Postgres database (mirrors `chain_timeout_tests.rs`).
fn leaked_name(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

// ── DB + app setup ──────────────────────────────────────────────────────────

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

/// A statically (non per-key) rate-limited activity that runs inline — no
/// real I/O needed since this test never dispatches the handler body.
fn rate_limited_activity_info(name: &'static str, rps: f64, burst: f64) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("email-queue"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: Some(rps),
        rate_limit_burst: Some(burst),
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    }
}

/// A statically (non per-key) throttled workflow — a single global bucket
/// per workflow type, resolved via `throttle::bucket_key(name, "")`.
fn static_throttled_info(name: &'static str, rate: &str, burst: f64) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: Some(
            ThrottlePolicy::from_rate_str(rate, Some(burst), None, None).expect("valid rate"),
        ),
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

fn build_app(
    pool: &DbPool,
    activities: Vec<ActivityInfo>,
    workflows: Vec<WorkflowInfo>,
) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(workflows, activities);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::new()),
        Arc::new(Vec::new()),
        Some("pacing-override-test".to_string()),
        vec!["default".to_string(), "email-queue".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

// ── HTTP helpers ─────────────────────────────────────────────────────────────

async fn json_request(
    app: &HarvestApiApp,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-harvest-admin", "true");
    let request = if let Some(b) = body {
        builder = builder.header("content-type", "application/json");
        builder.body(Body::from(b.to_string())).unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    };
    let response = app.clone().oneshot(request).await.expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    json_request(app, "GET", uri, None).await
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    json_request(app, "POST", uri, Some(body)).await
}

async fn delete_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    json_request(app, "DELETE", uri, None).await
}

/// Asserts two RFC3339 timestamp strings describe the same instant to
/// within a generous tolerance, rather than requiring byte-identical
/// strings.
///
/// A pacing-override `expires_at` value is legitimately re-serialized
/// through two different clocks in this test suite: the SET/CLEAR HTTP
/// response reports the Rust-side `chrono::Utc::now() + ttl` read
/// (nanosecond precision), while a later GET reflects what Postgres
/// actually persisted in the `TIMESTAMPTZ` column (microsecond precision
/// -- Postgres truncates, it does not round, so the two values can differ
/// by up to ~1 microsecond even though they describe the same override).
/// Comparing raw strings is over-strict and flags that expected precision
/// truncation as a failure; comparing parsed instants with a tolerance
/// proves the two reads agree on the override's expiry without being
/// brittle to it.
fn assert_close_instant(a: Option<&str>, b: Option<&str>, msg: &str) {
    let parse = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap_or_else(|e| panic!("{msg}: failed to parse {s:?}: {e}"))
            .with_timezone(&chrono::Utc)
    };
    match (a, b) {
        (Some(a), Some(b)) => {
            let (ta, tb) = (parse(a), parse(b));
            let delta_ms = (ta - tb).num_milliseconds().abs();
            assert!(
                delta_ms < 1000,
                "{msg}: {a} and {b} differ by {delta_ms}ms (expected sub-second precision drift only)"
            );
        }
        (None, None) => {}
        _ => panic!("{msg}: one side is null and the other is not (a={a:?}, b={b:?})"),
    }
}

// ── Direct DB helpers (dispatch-path proof) ─────────────────────────────────

async fn set_bucket_tokens(conn: &mut AsyncPgConnection, key: &str, tokens: f64) {
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens=$2, last_refilled_at=NOW() WHERE key=$1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(tokens)
    .execute(conn)
    .await
    .expect("set tokens");
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'pacing-override-test', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

async fn enqueue_gated_activity(
    conn: &mut AsyncPgConnection,
    queue: &str,
    activity: &str,
) -> uuid::Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some(activity.to_string());
    params.activity_id = Some(uuid::Uuid::new_v4());
    params.rate_limit_key = Some(activity.to_string());
    queue::enqueue(conn, &params).await.expect("enqueue")
}

const WORKER: &str = "worker-a";
const BUILD: &str = "";

async fn claim(conn: &mut AsyncPgConnection, queue: &str) -> Option<uuid::Uuid> {
    claim_task(conn, &[queue.to_string()], WORKER, BUILD, None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

/// Delete a workflow's pending throttle backlog row(s), simulating that the
/// scanner (`throttle::fire_due_throttled_starts`) has already drained it.
/// The FIFO fast-path guard in `reserve_or_defer` defers any NEW start for a
/// `bucket_key` with a still-pending backlog entry -- regardless of an active
/// override -- so a test proving "the override activates the very next
/// admission" must first prove the backlog it created to demonstrate the
/// gated baseline is gone, exactly as a live scanner tick would clear it.
async fn clear_throttle_backlog(conn: &mut AsyncPgConnection, bucket_key: &str) {
    diesel::sql_query("DELETE FROM harvest_start_throttle WHERE bucket_key=$1")
        .bind::<diesel::sql_types::Text, _>(bucket_key)
        .execute(conn)
        .await
        .expect("clear throttle backlog");
}

// ── Rate-limit override: activation + TTL auto-revert, no restart ──────────

#[tokio::test]
async fn rate_limit_override_activates_immediately_and_reverts_at_ttl_with_no_restart() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let queue = leaked_name("email-queue");
    // Baseline: a near-zero refill rate so a drained bucket stays gated for
    // the whole span of this test unless an override is active.
    let mut info = rate_limited_activity_info(name, 0.001, 1.0);
    info.default_queue = Some(queue);
    let app = build_app(&pool, vec![info], vec![]);

    // The declared bucket is registered exactly as worker startup would
    // (issue #699's static-limiter registration), then drained dry.
    {
        let mut conn = pool.get().await.expect("conn");
        queue::ensure_rate_limit_bucket(&mut conn, name, 0.001, 1.0)
            .await
            .expect("ensure bucket");
        set_bucket_tokens(&mut conn, name, 0.0).await;
    }

    // Sanity: at the declared baseline the task is NOT claimable.
    let task1 = {
        let mut conn = pool.get().await.expect("conn");
        let task = enqueue_gated_activity(&mut conn, queue, name).await;
        assert!(
            claim(&mut conn, queue).await.is_none(),
            "baseline rate limit should still gate the task"
        );
        task
    };

    // Set a TTL'd override with a much higher rate.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 1000.0, "burst": 1000.0, "ttl_secs": 2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "override response: {body}");
    assert_eq!(body["override_active"], json!(true));
    assert_eq!(body["effective_refill_rate"], json!(1000.0));
    assert_eq!(body["effective_burst"], json!(1000.0));

    // Success metric: the VERY NEXT claim on the SAME pool sees the override.
    // No worker was restarted, no process was touched between the HTTP call
    // and this claim -- proving the override lands on the live dispatch path.
    {
        let mut conn = pool.get().await.expect("conn");
        let claimed = claim(&mut conn, queue).await;
        assert_eq!(
            claimed,
            Some(task1),
            "override must activate on the existing token-consumption path with no restart"
        );
    }

    // Drain the bucket again and let the TTL lapse with NO operator action
    // (nothing calls DELETE .../override here).
    let task2 = {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, name, 0.0).await;
        enqueue_gated_activity(&mut conn, queue, name).await
    };
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    {
        let mut conn = pool.get().await.expect("conn");
        let claimed = claim(&mut conn, queue).await;
        assert!(
            claimed.is_none(),
            "override must self-expire at its TTL with no operator action, task2={task2}"
        );
    }

    // The read surface agrees with the reverted dispatch state.
    let (status, list) = get_json(&app, "/admin/rate-limits").await;
    assert_eq!(status, StatusCode::OK);
    let entry = list
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["key"] == json!(name))
        .unwrap_or_else(|| panic!("{name} bucket listed"));
    assert_eq!(entry["override_active"], json!(false));
    assert_eq!(entry["effective_refill_rate"], json!(0.001));
    assert_eq!(entry["effective_burst"], json!(1.0));
}

// ── Rate-limit override: DELETE clears it early ─────────────────────────────

#[tokio::test]
async fn rate_limit_override_delete_reverts_immediately_before_ttl() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let queue = leaked_name("email-queue");
    let mut info = rate_limited_activity_info(name, 0.001, 1.0);
    info.default_queue = Some(queue);
    let app = build_app(&pool, vec![info], vec![]);

    {
        let mut conn = pool.get().await.expect("conn");
        queue::ensure_rate_limit_bucket(&mut conn, name, 0.001, 1.0)
            .await
            .expect("ensure bucket");
    }

    // A long-lived override (5 minutes) so a real TTL lapse can never be the
    // reason dispatch reverts below -- only the explicit DELETE can explain it.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 1000.0, "burst": 1000.0, "ttl_secs": 300 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "override response: {body}");
    assert_eq!(body["override_active"], json!(true));

    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, name, 0.0).await;
        let task = enqueue_gated_activity(&mut conn, queue, name).await;
        assert_eq!(
            claim(&mut conn, queue).await,
            Some(task),
            "override should be active immediately"
        );
    }

    // Clear it early. The TTL has 4+ minutes left, so this can only be the
    // explicit DELETE taking effect.
    let (status, body) = delete_json(&app, &format!("/admin/rate-limits/{name}/override")).await;
    assert_eq!(status, StatusCode::OK, "clear response: {body}");
    assert_eq!(body["override_active"], json!(false));
    assert_eq!(body["effective_refill_rate"], json!(0.001));

    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, name, 0.0).await;
        let task = enqueue_gated_activity(&mut conn, queue, name).await;
        let claimed = claim(&mut conn, queue).await;
        assert!(
            claimed.is_none(),
            "DELETE .../override must revert to baseline immediately, before any TTL elapses, task={task}"
        );
    }
}

// ── Workflow-start throttle: activation + TTL auto-revert via real /start ──

#[tokio::test]
async fn throttle_override_activates_immediately_and_reverts_at_ttl_via_real_start_route() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    // Very slow declared baseline: 1 token per hour, burst 1.
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "1/h", 1.0)]);

    let start_uri = format!("/workflows/{name}/start");
    let start = |workflow_id: &str| json!({ "workflow_id": workflow_id, "input": {} });

    // First start burns the sole declared token -> admitted immediately.
    let (status, body) = post_json(&app, &start_uri, start("job-1")).await;
    assert_eq!(status, StatusCode::CREATED, "first start: {body}");

    // Second start (fresh workflow_id, so no idempotent-retry bypass) finds
    // the bucket empty at the declared 1/h baseline -> deferred. This creates
    // a pending `harvest_start_throttle` backlog row for the bucket key.
    let (status, body) = post_json(&app, &start_uri, start("job-2")).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "baseline should defer: {body}"
    );
    assert_eq!(body["throttled"], json!(true));

    // Override with a much higher rate.
    let (status, body) = post_json(
        &app,
        &format!("/admin/start-throttle/{name}/override"),
        json!({ "refill_per_sec": 1000.0, "burst": 1000.0, "ttl_secs": 2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "override response: {body}");
    assert_eq!(body["override_active"], json!(true));
    assert_eq!(body["effective_refill_rate"], json!(1000.0));

    // Simulate a scanner tick clearing job-2's backlog: `reserve_or_defer`'s
    // FIFO fast-path guard defers any NEW start for a key with a still-
    // pending backlog entry (so a burst can't jump the queue), which would
    // otherwise mask the override's effect on a *fresh* admission below.
    let bucket_key = autumn_harvest::throttle::bucket_key(name, "");
    {
        let mut conn = pool.get().await.expect("conn");
        clear_throttle_backlog(&mut conn, &bucket_key).await;
    }

    // Success metric on the throttle side: the very next real start call
    // (same running app, no restart) is admitted immediately.
    let (status, body) = post_json(&app, &start_uri, start("job-3")).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "override must activate the very next /start call with no restart: {body}"
    );

    // Drain the bucket back to zero and let the TTL lapse with no operator
    // action (nothing calls DELETE here).
    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, &bucket_key, 0.0).await;
    }
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let (status, body) = post_json(&app, &start_uri, start("job-4")).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "override must self-expire at its TTL with no operator action: {body}"
    );
    assert_eq!(body["throttled"], json!(true));

    // GET /admin/start-throttle agrees with the reverted state.
    let (status, list) = get_json(&app, "/admin/start-throttle").await;
    assert_eq!(status, StatusCode::OK);
    let entry = list
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["workflow_name"] == json!(name))
        .unwrap_or_else(|| panic!("{name} backlog entry listed"));
    assert_eq!(entry["override_active"], json!(false));
}

// ── Workflow-start throttle: DELETE clears it early ─────────────────────────

#[tokio::test]
async fn throttle_override_delete_reverts_immediately_before_ttl() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "1/h", 1.0)]);

    let start_uri = format!("/workflows/{name}/start");
    let start = |workflow_id: &str| json!({ "workflow_id": workflow_id, "input": {} });

    let (status, body) = post_json(
        &app,
        &format!("/admin/start-throttle/{name}/override"),
        json!({ "refill_per_sec": 1000.0, "burst": 1000.0, "ttl_secs": 300 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "override response: {body}");
    assert_eq!(body["override_active"], json!(true));

    // Drain the bucket to zero BEFORE asserting job-1 is admitted. Without
    // this, a brand-new bucket's own declared baseline (burst 1.0) already
    // has exactly enough headroom for one first start regardless of whether
    // the override is active -- the assertion below would pass even if the
    // override did nothing at all. Draining first makes admission provably
    // depend on the override's much higher rate/burst.
    let key = autumn_harvest::throttle::bucket_key(name, "");
    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, &key, 0.0).await;
    }

    let (status, body) = post_json(&app, &start_uri, start("job-1")).await;
    assert_eq!(status, StatusCode::CREATED, "override active: {body}");

    let (status, body) = delete_json(&app, &format!("/admin/start-throttle/{name}/override")).await;
    assert_eq!(status, StatusCode::OK, "clear response: {body}");
    assert_eq!(body["override_active"], json!(false));

    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, &key, 0.0).await;
    }

    let (status, body) = post_json(&app, &start_uri, start("job-2")).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "DELETE .../override must revert to baseline immediately, before any TTL elapses: {body}"
    );
    assert_eq!(body["throttled"], json!(true));
}

// ── Validation edge cases (AC coverage, not new mechanism) ──────────────────

#[tokio::test]
async fn override_undeclared_rate_limit_activity_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let declared = leaked_name("declared_but_ungated");
    let undeclared = leaked_name("never_registered");
    // `declared` is registered but has NO rate_limit_rps -- no declared
    // limit to override.
    let mut plain = rate_limited_activity_info(declared, 1.0, 1.0);
    plain.rate_limit_rps = None;
    plain.rate_limit_burst = None;
    let app = build_app(&pool, vec![plain], vec![]);

    let (status, _body) = post_json(
        &app,
        &format!("/admin/rate-limits/{declared}/override"),
        json!({ "refill_rate": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "declared but no rate limit");

    let (status, _body) = post_json(
        &app,
        &format!("/admin/rate-limits/{undeclared}/override"),
        json!({ "refill_rate": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "never registered");
}

#[tokio::test]
async fn override_dynamic_per_key_rate_limit_returns_409() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let mut dynamic = rate_limited_activity_info(name, 5.0, 5.0);
    dynamic.rate_limit_key_expr = Some("input.tenant_id");
    let app = build_app(&pool, vec![dynamic], vec![]);

    let (status, _body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn override_invalid_ttl_and_rate_return_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let app = build_app(
        &pool,
        vec![rate_limited_activity_info(name, 1.0, 1.0)],
        vec![],
    );
    let path = format!("/admin/rate-limits/{name}/override");

    let (status, _body) =
        post_json(&app, &path, json!({ "refill_rate": 5.0, "ttl_secs": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "zero TTL");

    let (status, _body) = post_json(
        &app,
        &path,
        json!({ "refill_rate": 5.0, "ttl_secs": 999_999_999 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "TTL above server cap");

    let (status, _body) =
        post_json(&app, &path, json!({ "refill_rate": -1.0, "ttl_secs": 60 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-positive refill_rate");

    let (status, _body) = post_json(&app, &path, json!({ "ttl_secs": 60 })).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "neither refill_rate nor burst set"
    );
}

// ── Throttle-side validation edge cases (mirrors the rate-limit side) ──────

#[tokio::test]
async fn override_undeclared_throttle_workflow_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let declared = leaked_name("declared_but_unthrottled");
    let undeclared = leaked_name("never_registered_wf");
    // `declared` is registered but has NO throttle policy -- no declared
    // throttle to override.
    let mut plain = static_throttled_info(declared, "5/m", 5.0);
    plain.throttle = None;
    let app = build_app(&pool, vec![], vec![plain]);

    let (status, _body) = post_json(
        &app,
        &format!("/admin/start-throttle/{declared}/override"),
        json!({ "refill_per_sec": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "declared but no throttle");

    let (status, _body) = post_json(
        &app,
        &format!("/admin/start-throttle/{undeclared}/override"),
        json!({ "refill_per_sec": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "never registered");
}

#[tokio::test]
async fn override_dynamic_per_key_throttle_returns_409() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let dynamic = WorkflowInfo {
        throttle: Some(
            ThrottlePolicy::from_rate_str("5/m", Some(5.0), Some("input.tenant_id"), None)
                .expect("valid rate"),
        ),
        ..static_throttled_info(name, "5/m", 5.0)
    };
    let app = build_app(&pool, vec![], vec![dynamic]);

    let (status, _body) = post_json(
        &app,
        &format!("/admin/start-throttle/{name}/override"),
        json!({ "refill_per_sec": 5.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn override_throttle_invalid_ttl_and_rate_return_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "5/m", 5.0)]);
    let path = format!("/admin/start-throttle/{name}/override");

    let (status, _body) =
        post_json(&app, &path, json!({ "refill_per_sec": 5.0, "ttl_secs": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "zero TTL");

    let (status, _body) = post_json(
        &app,
        &path,
        json!({ "refill_per_sec": 5.0, "ttl_secs": 999_999_999 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "TTL above server cap");

    let (status, _body) = post_json(
        &app,
        &path,
        json!({ "refill_per_sec": -1.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-positive refill_per_sec"
    );

    let (status, _body) = post_json(&app, &path, json!({ "ttl_secs": 60 })).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "neither refill_per_sec nor burst set"
    );
}

// ── burst boundary: must be >= 1.0 (both routes) ────────────────────────────

#[tokio::test]
async fn override_burst_boundary_rejects_below_one_accepts_at_one() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let rl_name = leaked_name("send_email");
    let wf_name = leaked_name("onboard_user");
    let app = build_app(
        &pool,
        vec![rate_limited_activity_info(rl_name, 1.0, 1.0)],
        vec![static_throttled_info(wf_name, "5/m", 5.0)],
    );

    // Just under the boundary -- rejected on both routes.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{rl_name}/override"),
        json!({ "burst": 0.999, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "rate-limit burst<1: {body}"
    );

    let (status, body) = post_json(
        &app,
        &format!("/admin/start-throttle/{wf_name}/override"),
        json!({ "burst": 0.999, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "throttle burst<1: {body}");

    // Exactly at the boundary -- accepted (inclusive) on both routes.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{rl_name}/override"),
        json!({ "burst": 1.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "rate-limit burst==1: {body}");
    assert_eq!(body["override_burst"], json!(1.0));

    let (status, body) = post_json(
        &app,
        &format!("/admin/start-throttle/{wf_name}/override"),
        json!({ "burst": 1.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "throttle burst==1: {body}");
    assert_eq!(body["override_burst"], json!(1.0));
}

// ── expires_at: reflects the TTL, clears to null on DELETE ─────────────────

#[tokio::test]
async fn override_expires_at_reflects_ttl_and_reverts_to_null_on_delete() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let app = build_app(
        &pool,
        vec![rate_limited_activity_info(name, 1.0, 1.0)],
        vec![],
    );

    let before = chrono::Utc::now();
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 5.0, "ttl_secs": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set: {body}");
    let expires_at: chrono::DateTime<chrono::Utc> = body["expires_at"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .expect("expires_at must be a present, parseable RFC3339 timestamp");
    let delta = (expires_at - before).num_seconds();
    assert!(
        (95..=105).contains(&delta),
        "expires_at should land ~100s after the SET call (was {delta}s), body={body}"
    );

    // The read surface (GET /admin/rate-limits) reports the same
    // expires_at the SET response promised -- compared as parsed instants,
    // not raw strings: the SET response reports the Rust-side
    // `chrono::Utc::now()` read (nanosecond precision) while the list read
    // reflects what Postgres actually persisted (TIMESTAMPTZ is
    // microsecond precision), so the two strings legitimately differ in
    // their trailing sub-microsecond digits even though they describe the
    // same instant to within Postgres's storage precision.
    let (status, list) = get_json(&app, "/admin/rate-limits").await;
    assert_eq!(status, StatusCode::OK);
    let entry = list
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["key"] == json!(name))
        .unwrap_or_else(|| panic!("{name} bucket listed"));
    assert_close_instant(
        entry["override_expires_at"].as_str(),
        body["expires_at"].as_str(),
        "the list read must agree with the SET response's expires_at",
    );

    // DELETE clears it back to null -- never a stale/lingering timestamp.
    let (status, body) = delete_json(&app, &format!("/admin/rate-limits/{name}/override")).await;
    assert_eq!(status, StatusCode::OK, "clear: {body}");
    assert_eq!(
        body["expires_at"],
        json!(null),
        "a cleared override must report expires_at: null, not a stale timestamp"
    );
}

// ── GET /admin/start-throttle/{workflow_name}/override — read-only lookup ──
//
// The read-only companion route (added to close a visibility gap: the
// backlog-driven `GET /admin/start-throttle` list only ever surfaces a
// workflow while it has pending deferred starts, so an override active on a
// currently-quiet workflow -- or one set before any admission pressure has
// ever occurred -- would otherwise be invisible). Proves it resolves and
// reports live override state directly against the bucket the
// token-consumption path (`throttle::reserve_or_defer`) actually reads,
// independent of the backlog.

#[tokio::test]
async fn get_start_throttle_override_reflects_live_state_across_set_and_delete() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "5/m", 5.0)]);
    let uri = format!("/admin/start-throttle/{name}/override");

    // Before any admission pressure and before any override: the bucket row
    // does not exist yet. The route must still answer with the declared
    // baseline rather than 404 -- "no override configured" is a valid answer.
    let (status, body) = get_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK, "no bucket row yet: {body}");
    assert_eq!(body["override_active"], json!(false));
    assert_eq!(body["effective_refill_rate"], json!(5.0 / 60.0));
    assert_eq!(body["effective_burst"], json!(5.0));
    assert_eq!(body["expires_at"], json!(null));

    // Set an override -- GET reflects it immediately, with zero pending
    // deferred-start backlog anywhere in this test.
    let (status, set_body) = post_json(
        &app,
        &uri,
        json!({ "refill_per_sec": 1000.0, "burst": 1000.0, "ttl_secs": 300 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set: {set_body}");

    let (status, body) = get_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK, "after set: {body}");
    assert_eq!(body["override_active"], json!(true));
    assert_eq!(body["effective_refill_rate"], json!(1000.0));
    assert_eq!(body["effective_burst"], json!(1000.0));
    // Same Rust-vs-Postgres sub-microsecond precision note as above -- the
    // GET read reflects the DB-persisted (microsecond-truncated) value,
    // the SET response reports the pre-write (nanosecond-precision) read.
    assert_close_instant(
        body["expires_at"].as_str(),
        set_body["expires_at"].as_str(),
        "GET must agree with the SET response's expires_at",
    );

    // Clear it -- GET reverts to the declared baseline immediately.
    let (status, _body) = delete_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = get_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK, "after clear: {body}");
    assert_eq!(body["override_active"], json!(false));
    assert_eq!(body["effective_refill_rate"], json!(5.0 / 60.0));
    assert_eq!(body["effective_burst"], json!(5.0));
    assert_eq!(body["expires_at"], json!(null));
}

#[tokio::test]
async fn get_start_throttle_override_undeclared_and_dynamic_key_errors() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let declared = leaked_name("declared_but_unthrottled");
    let undeclared = leaked_name("never_registered_wf");
    let dynamic_name = leaked_name("onboard_user");

    let mut plain = static_throttled_info(declared, "5/m", 5.0);
    plain.throttle = None;
    let dynamic = WorkflowInfo {
        throttle: Some(
            ThrottlePolicy::from_rate_str("5/m", Some(5.0), Some("input.tenant_id"), None)
                .expect("valid rate"),
        ),
        ..static_throttled_info(dynamic_name, "5/m", 5.0)
    };
    let app = build_app(&pool, vec![], vec![plain, dynamic]);

    let (status, _body) =
        get_json(&app, &format!("/admin/start-throttle/{declared}/override")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "declared but no throttle");

    let (status, _body) = get_json(
        &app,
        &format!("/admin/start-throttle/{undeclared}/override"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "never registered");

    let (status, _body) = get_json(
        &app,
        &format!("/admin/start-throttle/{dynamic_name}/override"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "dynamic per-key throttle");
}

// ── Audit trail: every SET/CLEAR writes a readable audit record ────────────
//
// AC4 requires "every set/clear writes an audit record". The prior tests
// only proved the mechanism statically (the handler always calls
// `audit::insert_audit` before returning); these read the record BACK
// through the real `GET /admin/audit` route after a genuine HTTP SET/CLEAR
// call, closing the gap between "the code path exists" and "the row is
// actually queryable by an operator".

#[tokio::test]
async fn rate_limit_override_set_and_clear_record_audit_rows() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let app = build_app(
        &pool,
        vec![rate_limited_activity_info(name, 1.0, 1.0)],
        vec![],
    );

    post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 5.0, "ttl_secs": 60 }),
    )
    .await;
    delete_json(&app, &format!("/admin/rate-limits/{name}/override")).await;

    let (status, body) = get_json(
        &app,
        "/admin/audit?operation=rate_limit.pacing_override.set",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let records = body.as_array().expect("audit response is a raw array");
    let set_record = records
        .iter()
        .find(|r| r["target_id"] == json!(name))
        .unwrap_or_else(|| panic!("expected a SET audit record for {name}, got {records:?}"));
    assert_eq!(set_record["status"], json!("succeeded"));
    assert_eq!(
        set_record["route_or_command"],
        json!("POST /admin/rate-limits/{activity_name}/override")
    );

    let (status, body) = get_json(
        &app,
        "/admin/audit?operation=rate_limit.pacing_override.clear",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let records = body.as_array().expect("audit response is a raw array");
    let clear_record = records
        .iter()
        .find(|r| r["target_id"] == json!(name))
        .unwrap_or_else(|| panic!("expected a CLEAR audit record for {name}, got {records:?}"));
    assert_eq!(clear_record["status"], json!("succeeded"));
    assert_eq!(
        clear_record["route_or_command"],
        json!("DELETE /admin/rate-limits/{activity_name}/override")
    );
}

#[tokio::test]
async fn throttle_override_set_and_clear_record_audit_rows() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "5/m", 5.0)]);
    let key = autumn_harvest::throttle::bucket_key(name, "");

    post_json(
        &app,
        &format!("/admin/start-throttle/{name}/override"),
        json!({ "refill_per_sec": 5.0, "ttl_secs": 60 }),
    )
    .await;
    delete_json(&app, &format!("/admin/start-throttle/{name}/override")).await;

    let (status, body) = get_json(
        &app,
        "/admin/audit?operation=start_throttle.pacing_override.set",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let records = body.as_array().expect("audit response is a raw array");
    let set_record = records
        .iter()
        .find(|r| r["target_id"] == json!(key))
        .unwrap_or_else(|| panic!("expected a SET audit record for {key}, got {records:?}"));
    assert_eq!(set_record["status"], json!("succeeded"));
    assert_eq!(
        set_record["route_or_command"],
        json!("POST /admin/start-throttle/{workflow_name}/override")
    );

    let (status, body) = get_json(
        &app,
        "/admin/audit?operation=start_throttle.pacing_override.clear",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let records = body.as_array().expect("audit response is a raw array");
    let clear_record = records
        .iter()
        .find(|r| r["target_id"] == json!(key))
        .unwrap_or_else(|| panic!("expected a CLEAR audit record for {key}, got {records:?}"));
    assert_eq!(clear_record["status"], json!("succeeded"));
    assert_eq!(
        clear_record["route_or_command"],
        json!("DELETE /admin/start-throttle/{workflow_name}/override")
    );
}
