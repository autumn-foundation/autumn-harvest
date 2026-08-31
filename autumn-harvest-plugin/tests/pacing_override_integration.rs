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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::queue::{self, EnqueueParams, TaskType, claim_task};
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::register_worker;
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
use diesel_async::SimpleAsyncConnection;
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
    autumn_harvest::test_init_sql().as_bytes().to_vec()
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

/// A pool pointed at an unreachable host -- every `get()`/query against it
/// fails immediately with a connection error. Mirrors the dead-port pattern
/// already used in `completion_triggers_integration.rs` for simulating one
/// down shard alongside one live shard (issue #945 review, P2).
fn dead_pool() -> DbPool {
    build_pool("postgres://postgres:postgres@localhost:12345/non_existent")
}

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

/// A single fresh, migrated shard database (guard `None` under
/// `HARVEST_TEST_DATABASE_URL`, `Some` when backed by a fresh testcontainers
/// Postgres) -- used as the ONE *live* shard for the shard-outage tests
/// below, paired with a [`dead_pool`] for the unreachable shard.
async fn setup_one_shard() -> (String, Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let db = format!("harvest_pacing_{}", Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(&admin_url)
        .await
        .expect("admin connect");
    diesel::sql_query(format!("CREATE DATABASE {db}"))
        .execute(&mut admin)
        .await
        .expect("create db");

    let url = shard_url(&admin_url, &db);
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("shard connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrate shard");

    (url, guard)
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
        quota: None,
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

/// Sharded variant of [`build_app`] -- lets a caller install an arbitrary
/// [`HarvestDbPool`] (e.g. one built with an unreachable pool for a given
/// shard, via [`ShardedDbPool::from_map`]) instead of always wrapping a
/// single live pool. Used to exercise `get_start_throttle_pacing_override`'s
/// partial/total shard-outage handling (issue #945 review, P2): a status
/// read that silently degrades a real live override into
/// `{"override_active": false}` on a shard outage is worse than an explicit
/// failure.
fn build_sharded_app(
    harvest_pool: HarvestDbPool,
    router: ShardRouter,
    activities: Vec<ActivityInfo>,
    workflows: Vec<WorkflowInfo>,
) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(harvest_pool);

    let registry = HandlerRegistry::new(workflows, activities);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::new()),
        Arc::new(Vec::new()),
        Some("pacing-override-sharded-test".to_string()),
        vec!["default".to_string(), "email-queue".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        router,
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

/// Directly writes every accrual-relevant column on a `harvest_rate_limit_buckets`
/// row -- baseline rate/burst, current `tokens`, `last_refilled_at` (anchored
/// `last_refilled_seconds_ago` seconds in the past), and the TTL'd override
/// fields (anchored so the override EXPIRED `override_expired_seconds_ago`
/// seconds ago, when `Some`) -- bypassing both `ensure_rate_limit_bucket`'s
/// insert-only semantics and the HTTP override route's `NOW() + ttl` /
/// positive-rate validation, so a P1-regression scenario (an override that
/// expired PARTWAY through the elapsed interval, with no intervening write
/// to settle the bucket) can be constructed deterministically (issue #945
/// review, P1).
#[allow(clippy::too_many_arguments)]
async fn set_bucket_accrual_state(
    conn: &mut AsyncPgConnection,
    key: &str,
    refill_rate: f64,
    burst: f64,
    tokens: f64,
    last_refilled_seconds_ago: f64,
    override_refill_rate: Option<f64>,
    override_burst: Option<f64>,
    override_expired_seconds_ago: Option<f64>,
) {
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, \
              override_refill_rate, override_burst, override_expires_at) \
         VALUES ( \
             $1, $2, $3, $4, NOW() - make_interval(secs => $5), $6, $7, \
             CASE WHEN $8::DOUBLE PRECISION IS NULL THEN NULL \
                  ELSE NOW() - make_interval(secs => $8) END \
         ) \
         ON CONFLICT (key) DO UPDATE SET \
             refill_rate = EXCLUDED.refill_rate, \
             burst = EXCLUDED.burst, \
             tokens = EXCLUDED.tokens, \
             last_refilled_at = EXCLUDED.last_refilled_at, \
             override_refill_rate = EXCLUDED.override_refill_rate, \
             override_burst = EXCLUDED.override_burst, \
             override_expires_at = EXCLUDED.override_expires_at",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(refill_rate)
    .bind::<diesel::sql_types::Double, _>(burst)
    .bind::<diesel::sql_types::Double, _>(tokens)
    .bind::<diesel::sql_types::Double, _>(last_refilled_seconds_ago)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(override_refill_rate)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(override_burst)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Double>, _>(override_expired_seconds_ago)
    .execute(conn)
    .await
    .expect("set bucket accrual state");
}

/// Reads the LIVE numeric result of `queue::effective_available_tokens_expr`
/// executed against a real Postgres row -- the strongest possible proof that
/// the rendered SQL, not just its string shape, computes the mathematically
/// correct piecewise accrual (issue #945 review, P1).
async fn read_available_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Double)]
        available: f64,
    }
    let expr = queue::effective_available_tokens_expr("harvest_rate_limit_buckets");
    diesel::sql_query(format!(
        "SELECT {expr} AS available FROM harvest_rate_limit_buckets WHERE key = $1"
    ))
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result::<Row>(conn)
    .await
    .expect("read available tokens")
    .available
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

    // Let the TTL fully lapse with NO operator action (nothing calls
    // DELETE .../override here) BEFORE draining the bucket again. The
    // drain-to-zero must land strictly AFTER the override has expired:
    // resetting the bucket WHILE the override is still counting down
    // would let tokens legitimately re-accrue at the (still-active)
    // override rate for whatever life it has left -- correct
    // piecewise-accrual math (issue #945 review, P1), but not what this
    // step is proving. This step proves the narrower, AC2-literal claim:
    // a FRESH drain taken strictly after `expires_at` is governed purely
    // by the reverted baseline rate, with zero override residue.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let task2 = {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, name, 0.0).await;
        enqueue_gated_activity(&mut conn, queue, name).await
    };

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

// ── P1 review: piecewise accrual across an override that expired          ──
// ── mid-interval with no intervening write (issue #945 review)            ──
//
// A naive `elapsed * effective_refill_rate` applies "whichever rate is
// effective RIGHT NOW" to the WHOLE elapsed interval. Once an override has
// expired with no intervening debit/refund to settle the bucket, that
// retroactively refills at the (now-reverted) baseline rate for the portion
// of the interval the override was still supposed to be throttling -- a
// phantom instant refill the moment the TTL lapses. Both tests below fix the
// bucket's `last_refilled_at`/`override_expires_at` far enough in the past,
// with the override's own accrual rate pinned to EXACTLY `0.0`, that segment
// 1 (the override-active portion) provably contributes ZERO tokens
// regardless of how much real wall-clock overhead the test itself adds --
// only segment 2 (the short post-expiry tail, at the fast baseline rate)
// can accrue anything, so the assertions are robust to CI timing jitter by
// construction, not by a narrow numeric tolerance.

#[tokio::test]
async fn rate_limit_override_expiring_mid_interval_accrues_piecewise_not_the_reverted_baseline_across_the_whole_interval()
 {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let mut conn = pool.get().await.expect("conn");

    // Baseline: a comparatively fast 1 token/sec. Override: fully paused
    // (0.0 tokens/sec) for the whole time it was active. The bucket was
    // last settled 10 DAYS ago and the override expired only 1 second ago
    // (i.e. it was active for the ENTIRE ~10-day span minus that last
    // second, then reverted to baseline just before "now" plus whatever
    // this test's own overhead adds). This is the shape that actually
    // exercises the P1 bug: almost the WHOLE elapsed interval was
    // legitimately override-governed (and paused), with only a sliver of
    // genuine post-expiry baseline accrual.
    set_bucket_accrual_state(
        &mut conn,
        name,
        /* refill_rate */ 1.0,
        /* burst */ 1000.0,
        /* tokens */ 0.0,
        /* last_refilled_seconds_ago */ 10.0 * 86_400.0,
        /* override_refill_rate */ Some(0.0),
        /* override_burst */ Some(1000.0),
        /* override_expired_seconds_ago */ Some(1.0),
    )
    .await;

    let available = read_available_tokens(&mut conn, name).await;

    // BUGGY (pre-fix) shape: `elapsed_total(~10 days) * baseline_rate(1.0)`
    // is enormous and clamps to the burst ceiling -- the token bucket would
    // read as FULL (1000.0) the instant the TTL lapsed, even though the
    // override was paused for all but the last second of that 10-day span.
    assert!(
        available < 1000.0,
        "must not clamp to the full burst via a phantom instant refill at \
         the reverted baseline rate applied across the WHOLE elapsed \
         interval; available={available}"
    );
    // CORRECT (piecewise) shape: segment 1 (the ~10-day override-active
    // span) contributes EXACTLY zero at override_refill_rate=0.0 regardless
    // of its exact duration, so only segment 2 -- the ~1-second post-expiry
    // tail, plus this test's own real overhead -- can accrue anything, at
    // the 1 token/sec baseline. Generous even against tens of seconds of
    // CI overhead, while still an order of magnitude below the buggy 1000.0.
    assert!(
        (0.0..100.0).contains(&available),
        "expected only the short post-expiry tail (~1s of accrual at \
         1 token/sec, plus test overhead) to have accrued, not a refill \
         proportional to the whole 10-day interval; available={available}"
    );
}

#[tokio::test]
async fn rate_limit_override_expiring_mid_interval_does_not_phantom_refill_the_dispatch_gate() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let queue = leaked_name("email-queue");
    let mut conn = pool.get().await.expect("conn");

    // Identical accrual shape to the pure-formula test above (an override
    // that was paused for essentially the WHOLE ~10-day span and expired
    // only 1 second ago), but proven through the REAL dispatch/claim path
    // (the same gate every activity task is admitted through), not a raw
    // SELECT of the formula. The baseline rate here is deliberately much
    // slower (0.01/s, vs. 1.0/s above) so that even a generous multi-
    // second margin for this test's OWN wall-clock overhead between the
    // write and the claim keeps the correct segment-2 accrual (~1 second
    // of post-expiry tail at 0.01/s) safely under the 1.0-token dispatch
    // threshold, while the buggy flat-rate shape (applying 0.01/s across
    // the whole ~10-day interval) is off by four orders of magnitude and
    // clamps straight to the burst ceiling either way.
    set_bucket_accrual_state(
        &mut conn,
        name,
        /* refill_rate */ 0.01,
        /* burst */ 1000.0,
        /* tokens */ 0.0,
        /* last_refilled_seconds_ago */ 10.0 * 86_400.0,
        /* override_refill_rate */ Some(0.0),
        /* override_burst */ Some(1000.0),
        /* override_expired_seconds_ago */ Some(1.0),
    )
    .await;

    let task = enqueue_gated_activity(&mut conn, queue, name).await;
    assert!(
        claim(&mut conn, queue).await.is_none(),
        "a task must NOT become claimable via a phantom instant refill the \
         moment a long-paused override's TTL lapses -- the bucket has only \
         accrued ~1 second's worth of tokens at the baseline rate, well \
         under the 1.0 needed to dispatch, task={task}"
    );
}

// ── Round-4 review: the eligibility explainer must reuse the SAME piecewise ──
// ── accrual formula the claim gate enforces, not re-derive it in Rust        ──
//
// `evaluate_eligibility_for_shard` (the `GET /admin/queues/{queue}/
// eligibility` handler behind issue #611's operator triage endpoint)
// independently re-computed "tokens available right now" in Rust via a flat
// `elapsed_total * effective_refill_rate`, applying whichever rate is
// effective AT THE MOMENT OF THE READ to the WHOLE elapsed interval -- the
// exact class of bug `queue::token_accrual_expr` was written to fix at the
// SQL layer for `claim_task` itself (issue #945 review, round 1, P1; see the
// two tests immediately above this one). Because the diagnostic and the real
// gate used two different formulas, an operator polling this endpoint during
// an incident could be told a task is dispatchable ("no impediment") when
// `claim_task` is, in fact, still refusing to claim it -- the worst possible
// answer from a triage tool.
//
// Reuses the identical accrual shape as the two P1 tests above (an override
// that was paused for essentially the WHOLE ~10-day span and expired only 1
// second ago) so the assertion is robust to CI timing jitter by
// construction: the buggy flat-rate shape clamps straight to the
// burst (1000.0) ceiling regardless of exact overhead, while the correct
// piecewise shape can only ever accrue the short post-expiry tail.
#[tokio::test]
async fn eligibility_endpoint_reports_the_saturation_claim_task_actually_enforces() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    let queue = leaked_name("email-queue");
    let worker_id = leaked_name("worker");
    let mut info = rate_limited_activity_info(name, 0.01, 1000.0);
    info.default_queue = Some(queue);
    let app = build_app(&pool, vec![info], vec![]);

    let mut conn = pool.get().await.expect("conn");

    // Identical accrual shape to the second claim-path P1 test above
    // (`..._does_not_phantom_refill_the_dispatch_gate`), including its
    // deliberately SLOW 0.01/s baseline: the bucket was last settled 10
    // DAYS ago, an override paused it at EXACTLY 0.0 tokens/sec for
    // essentially that whole span, and the override expired only 1 second
    // ago. The slow baseline keeps the correct post-expiry-tail accrual
    // (~1s at 0.01/s) safely under the 1.0-token dispatch threshold even
    // with this test's own wall-clock overhead (worker registration + an
    // HTTP round trip) between the write and the eligibility read -- a
    // FAST baseline (e.g. 1.0/s, as the pure-formula test above uses) would
    // let that same overhead alone accrue >= 1.0 tokens and mask the bug.
    // `claim_task` (proven by the test immediately above, with the same
    // 0.01/s shape) refuses to claim this task -- the eligibility endpoint
    // must agree.
    set_bucket_accrual_state(
        &mut conn,
        name,
        /* refill_rate */ 0.01,
        /* burst */ 1000.0,
        /* tokens */ 0.0,
        /* last_refilled_seconds_ago */ 10.0 * 86_400.0,
        /* override_refill_rate */ Some(0.0),
        /* override_burst */ Some(1000.0),
        /* override_expired_seconds_ago */ Some(1.0),
    )
    .await;

    enqueue_gated_activity(&mut conn, queue, name).await;

    // An otherwise perfectly eligible worker: right queue, right shard,
    // fresh heartbeat, no build/sticky/capability constraint -- the ONLY
    // thing that can make it ineligible is the rate-limit bucket above.
    register_worker(
        &mut conn,
        worker_id,
        &[queue.to_string()],
        &[0],
        10,
        "localhost",
        None,
        "",
        None,
        &std::collections::HashMap::new(),
        0,
    )
    .await
    .expect("worker registration should succeed");

    let (status, body) = get_json(&app, &format!("/admin/queues/{queue}/eligibility")).await;
    assert_eq!(status, StatusCode::OK, "eligibility response: {body}");

    // BUGGY (pre-fix) shape: `elapsed_total(~10 days) * effective_refill_rate
    // (0.01, the reverted baseline)` is off by four orders of magnitude and
    // clamps straight to the burst ceiling -- the diagnostic would read the
    // bucket as FULL and report the worker ELIGIBLE, even though
    // `claim_task` refuses the very same task.
    //
    // CORRECT (piecewise) shape: segment 1 (the ~10-day override-active
    // span) contributes EXACTLY zero at override_refill_rate=0.0, so only
    // the short post-expiry tail can have accrued -- well under the 1.0
    // needed to dispatch at the slow 0.01/s baseline -- and the worker must
    // be reported INELIGIBLE with `rate_limit_exhausted`, agreeing with the
    // real claim gate.
    let ineligible = body["ineligible_workers"]
        .as_array()
        .unwrap_or_else(|| panic!("ineligible_workers must be an array: {body}"));
    let entry = ineligible
        .iter()
        .find(|w| w["worker_id"] == json!(worker_id))
        .unwrap_or_else(|| {
            panic!(
                "worker must be reported ineligible -- agreeing with the real \
                 claim gate, which refuses this task -- not eligible via a \
                 phantom instant refill applied across the whole ~10-day \
                 interval; full response: {body}"
            )
        });
    let reasons: Vec<&str> = entry["reason_codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        reasons.contains(&"rate_limit_exhausted"),
        "expected rate_limit_exhausted, got {reasons:?}"
    );
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

    // Let the TTL fully lapse with no operator action (nothing calls
    // DELETE here) BEFORE draining the bucket back to zero. The drain
    // must land strictly AFTER the override has expired -- see the
    // matching comment on the rate-limit-side TTL test above for why
    // resetting mid-override would legitimately re-accrue tokens at the
    // still-active override rate under correct piecewise-accrual math.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    {
        let mut conn = pool.get().await.expect("conn");
        set_bucket_tokens(&mut conn, &bucket_key, 0.0).await;
    }

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

// ── P1 review round 3: the SCANNER's candidate gate must honor an active   ──
// ── override, not just the claim/dispatch gate (issue #945 review)         ──
//
// `throttle_override_activates_immediately_and_reverts_at_ttl_via_real_start_
// route` above proves the override is honored on the *admission* path
// (`reserve_or_defer`) and manually simulates a scanner tick by deleting the
// backlog row directly -- it never actually drives `fire_due_throttled_
// starts`. This test closes that gap: it drives the REAL scanner against a
// backlog row that the BASELINE-only candidate gate would leave stranded
// for the lifetime of any realistic test window (an effectively-frozen
// 1/hour rate), and proves the scanner's own pre-filter honors the override
// rate instead.

#[tokio::test]
async fn throttle_scanner_honors_active_override_in_its_candidate_gate() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    // Baseline: 1 token per HOUR, burst 1 (~0.000278/s) -- so slow relative
    // to this test's short sleep window that the scanner's candidate
    // pre-filter would never select a drained bucket's backlog row within
    // the lifetime of this test unless the override rate governs it: at
    // 0.000278/s even a full second of elapsed time accrues under 0.0003
    // tokens, four orders of magnitude below the 1.0 threshold.
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "1/h", 1.0)]);

    let start_uri = format!("/workflows/{name}/start");
    let start = |workflow_id: &str| json!({ "workflow_id": workflow_id, "input": {} });

    // First start burns the sole declared token -> admitted immediately.
    let (status, body) = post_json(&app, &start_uri, start("scan-job-1")).await;
    assert_eq!(status, StatusCode::CREATED, "first start: {body}");

    // Second start (fresh workflow_id) finds the bucket empty at the
    // effectively-frozen 1/hour baseline -> deferred, creating a pending
    // `harvest_start_throttle` backlog row for the bucket key.
    let (status, body) = post_json(&app, &start_uri, start("scan-job-2")).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "baseline should defer: {body}"
    );
    assert_eq!(body["throttled"], json!(true));

    let bucket_key = autumn_harvest::throttle::bucket_key(name, "");

    // Override with a much higher rate.
    let (status, body) = post_json(
        &app,
        &format!("/admin/start-throttle/{name}/override"),
        json!({ "refill_per_sec": 1000.0, "burst": 1000.0, "ttl_secs": 60 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "override response: {body}");
    assert_eq!(body["override_active"], json!(true));

    // Give the override rate a tiny, real window to accrue (1000 tokens/sec
    // needs well under 1ms to reach the 1.0-token threshold; a short sleep
    // here is just headroom against CI scheduling jitter, not load-bearing
    // math).
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drive the REAL scanner directly -- not a manual row deletion. Money
    // assertion: at the frozen 1/hour baseline this row can NEVER become a
    // scanner candidate within any realistic test window, so a scanner that
    // fires it here can only be honoring the ACTIVE OVERRIDE in its own
    // candidate pre-filter (the exact site the P1 finding names).
    let fired_count = {
        let mut conn = pool.get().await.expect("conn");
        autumn_harvest::throttle::fire_due_throttled_starts(
            &mut conn,
            &None,
            &[],
            &autumn_harvest::NoOpMetrics,
        )
        .await
        .expect("scanner tick")
    };
    assert_eq!(
        fired_count, 1,
        "the scanner must fire the backlog row once the override makes tokens \
         available, even though the frozen baseline rate never would -- if \
         this is 0, the scanner's candidate gate is still baseline-only"
    );

    // The backlog row is gone -- the scanner actually claimed and fired it,
    // not merely counted it.
    {
        let mut conn = pool.get().await.expect("conn");
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        let row: Count = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_start_throttle WHERE bucket_key = $1",
        )
        .bind::<diesel::sql_types::Text, _>(&bucket_key)
        .get_result(&mut conn)
        .await
        .expect("count backlog rows");
        assert_eq!(
            row.n, 0,
            "the scanner-fired row must be deleted from the backlog, proving \
             it was actually claimed and processed, not just observed"
        );
    }
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

// ── issue #945 review, round 2 ──────────────────────────────────────────────
//
// Two independent P2 findings on the round-1 fix commit:
//
// 1. `declared_*`/`effective_*` in every SET/CLEAR/GET response must reflect
//    the bucket's PERSISTED baseline, not the live registry declaration —
//    the separate, pre-existing, PERMANENT `POST /admin/rate-limits/{key}`
//    route (issue #332) can change a bucket's `refill_rate`/`burst` columns
//    independently of the registry, and the dispatch/claim path
//    (`effective_refill_rate_expr`/`effective_burst_expr`) reads those
//    columns, not the registry.
// 2. `GET /admin/start-throttle/{workflow_name}/override` must not silently
//    misreport a shard outage as "no override configured" — a total outage
//    must fail closed (`503`), and a partial outage must surface `207` with
//    the unreachable shard(s) named, never a bare `200`.

#[tokio::test]
async fn rate_limit_override_reports_persisted_baseline_not_registry_after_legacy_permanent_change()
{
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("send_email");
    // Registered/declared baseline: 5.0/5.0. `rate_limit_key: None` on
    // `rate_limited_activity_info` means the bucket key IS `name`.
    let app = build_app(
        &pool,
        vec![rate_limited_activity_info(name, 5.0, 5.0)],
        vec![],
    );

    // Permanently change the persisted baseline via the pre-existing,
    // SEPARATE permanent route — never touches the registry's declared
    // 5.0/5.0.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}"),
        json!({ "refill_rate": 999.0, "burst": 999.0 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "legacy permanent set: {body}");

    // SET a TTL'd override touching ONLY refill_rate. `declared_*` in the
    // response must be the PERSISTED 999.0/999.0, never the registry's
    // 5.0/5.0 — and the omitted burst's effective value must fall back to
    // the persisted 999.0, not the registry's 5.0.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{name}/override"),
        json!({ "refill_rate": 42.0, "ttl_secs": 300 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set: {body}");
    assert_eq!(
        body["declared_refill_rate"],
        json!(999.0),
        "SET declared_refill_rate must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["declared_burst"],
        json!(999.0),
        "SET declared_burst must be the persisted baseline: {body}"
    );
    assert_eq!(body["override_refill_rate"], json!(42.0));
    assert_eq!(body["override_burst"], json!(null));
    assert_eq!(body["effective_refill_rate"], json!(42.0));
    assert_eq!(
        body["effective_burst"],
        json!(999.0),
        "omitted burst must fall back to the PERSISTED baseline, not the \
         registry's declared 5.0: {body}"
    );

    // CLEAR — the reverted `declared_*`/`effective_*` must still be the
    // persisted 999.0/999.0, never the registry's 5.0/5.0.
    let (status, body) = delete_json(&app, &format!("/admin/rate-limits/{name}/override")).await;
    assert_eq!(status, StatusCode::OK, "clear: {body}");
    assert_eq!(
        body["declared_refill_rate"],
        json!(999.0),
        "CLEAR declared_refill_rate must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["declared_burst"],
        json!(999.0),
        "CLEAR declared_burst must be the persisted baseline: {body}"
    );
    assert_eq!(body["effective_refill_rate"], json!(999.0));
    assert_eq!(body["effective_burst"], json!(999.0));
}

#[tokio::test]
async fn throttle_override_reports_persisted_baseline_not_registry_after_legacy_permanent_change() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let name = leaked_name("onboard_user");
    let app = build_app(&pool, vec![], vec![static_throttled_info(name, "5/m", 5.0)]);
    let key = autumn_harvest::throttle::bucket_key(name, "");

    // Permanently change the persisted baseline via the SAME shared-table
    // legacy route, addressed by the throttle's own bucket key directly —
    // never touches the registry's declared `5/m` (5.0/60.0 refill_rate) /
    // 5.0 burst.
    let (status, body) = post_json(
        &app,
        &format!("/admin/rate-limits/{key}"),
        json!({ "refill_rate": 500.0, "burst": 500.0 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "legacy permanent set: {body}");

    let uri = format!("/admin/start-throttle/{name}/override");
    let (status, body) = post_json(
        &app,
        &uri,
        json!({ "refill_per_sec": 1.0, "ttl_secs": 300 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set: {body}");
    assert_eq!(
        body["declared_refill_rate"],
        json!(500.0),
        "SET declared_refill_rate must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["declared_burst"],
        json!(500.0),
        "SET declared_burst must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["effective_burst"],
        json!(500.0),
        "omitted burst must fall back to the PERSISTED baseline, not the \
         registry's declared 5.0: {body}"
    );

    let (status, body) = get_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK, "get: {body}");
    assert_eq!(
        body["declared_refill_rate"],
        json!(500.0),
        "GET declared_refill_rate must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["declared_burst"],
        json!(500.0),
        "GET declared_burst must be the persisted baseline: {body}"
    );

    let (status, body) = delete_json(&app, &uri).await;
    assert_eq!(status, StatusCode::OK, "clear: {body}");
    assert_eq!(
        body["declared_refill_rate"],
        json!(500.0),
        "CLEAR declared_refill_rate must be the persisted baseline: {body}"
    );
    assert_eq!(
        body["declared_burst"],
        json!(500.0),
        "CLEAR declared_burst must be the persisted baseline: {body}"
    );
    assert_eq!(body["effective_refill_rate"], json!(500.0));
    assert_eq!(body["effective_burst"], json!(500.0));
}

#[tokio::test]
async fn get_start_throttle_pacing_override_returns_503_on_total_shard_outage() {
    let name = leaked_name("onboard_user");
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), dead_pool());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        ShardId::new(0),
    );

    let app = build_sharded_app(
        HarvestDbPool::sharded(sharded_pool),
        router,
        vec![],
        vec![static_throttled_info(name, "5/m", 5.0)],
    );

    let (status, body) = get_json(&app, &format!("/admin/start-throttle/{name}/override")).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a total shard outage must fail closed, not silently report \
         override_active:false: {body}"
    );
    assert!(
        body.get("errors").is_some(),
        "503 body should name the unreachable shard(s): {body}"
    );
}

#[tokio::test]
async fn get_start_throttle_pacing_override_returns_207_on_partial_shard_outage_and_reflects_reachable_shard()
 {
    let (live_url, _container) = setup_one_shard().await;
    let live_pool = build_pool(&live_url);
    let name = leaked_name("onboard_user");
    let key = autumn_harvest::throttle::bucket_key(name, "");

    // Seed an ACTIVE override directly on the live shard -- bypasses the SET
    // route so this test isolates the GET route's partial-outage handling
    // from SET's own (separately covered) partial-write behaviour.
    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(300);
    {
        let mut conn = live_pool.get().await.expect("live conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $2, $2, NOW(), NOW(), NOW(), $3, $3, $4)",
        )
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Double, _>(5.0 / 60.0)
        .bind::<diesel::sql_types::Double, _>(1000.0)
        .bind::<diesel::sql_types::Timestamptz, _>(expires_at)
        .execute(&mut conn)
        .await
        .expect("seed row");
    }

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), live_pool);
    pools.insert(ShardId::new(1), dead_pool());
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let app = build_sharded_app(
        HarvestDbPool::sharded(sharded_pool),
        router,
        vec![],
        vec![static_throttled_info(name, "5/m", 5.0)],
    );

    let (status, body) = get_json(&app, &format!("/admin/start-throttle/{name}/override")).await;
    assert_eq!(
        status,
        StatusCode::MULTI_STATUS,
        "one shard down must not silently report all-healthy: {body}"
    );
    assert!(
        body["shard_errors"]
            .as_array()
            .is_some_and(|e| !e.is_empty()),
        "207 body should name the unreachable shard(s): {body}"
    );
    let overridden = &body["override"];
    assert_eq!(
        overridden["override_active"],
        json!(true),
        "must still reflect the reachable shard's live override: {overridden}"
    );
    assert_eq!(overridden["override_refill_rate"], json!(1000.0));
    assert_eq!(overridden["override_burst"], json!(1000.0));
}

/// Two GENUINELY LIVE, fully reachable shards that DISAGREE on override
/// state (issue #945 review, round 4, P2): shard 0 carries a live ACTIVE
/// override; shard 1 has no override at all (a cleared/never-set row).
///
/// This is deliberately NOT an outage scenario -- both shards answer their
/// query successfully, so `shard_errors` must be empty. The pre-fix merge
/// picked a single "representative" row across shards by raw `tokens` count
/// alone (`existing.tokens <= b.tokens => existing`), which has nothing to
/// do with whether an override is active. Seeding the ACTIVE shard with
/// MORE tokens than the INACTIVE shard reproduces the exact failure Codex's
/// review named: a fully-reachable GET silently returning `200
/// {"override_active": false}` while a live override still governs dispatch
/// on shard 0.
#[tokio::test]
async fn get_start_throttle_pacing_override_reports_disagreement_across_live_shards() {
    let (url_0, _container_0) = setup_one_shard().await;
    let (url_1, _container_1) = setup_one_shard().await;
    let pool_0 = build_pool(&url_0);
    let pool_1 = build_pool(&url_1);

    let name = leaked_name("onboard_user");
    let key = autumn_harvest::throttle::bucket_key(name, "");

    // Shard 0: a live ACTIVE override, seeded with MORE tokens than shard 1
    // -- this is what makes the pre-fix "fewest tokens wins" merge pick the
    // WRONG (inactive) shard as the representative.
    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(300);
    {
        let mut conn = pool_0.get().await.expect("shard 0 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $2, $3, NOW(), NOW(), NOW(), $4, $4, $5)",
        )
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Double, _>(5.0 / 60.0)
        .bind::<diesel::sql_types::Double, _>(900.0)
        .bind::<diesel::sql_types::Double, _>(1000.0)
        .bind::<diesel::sql_types::Timestamptz, _>(expires_at)
        .execute(&mut conn)
        .await
        .expect("seed shard 0 (active)");
    }

    // Shard 1: no override at all (NULL override columns), seeded with
    // FEWER tokens than shard 0.
    {
        let mut conn = pool_1.get().await.expect("shard 1 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $2, $3, NOW(), NOW(), NOW(), NULL, NULL, NULL)",
        )
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Double, _>(5.0 / 60.0)
        .bind::<diesel::sql_types::Double, _>(5.0)
        .execute(&mut conn)
        .await
        .expect("seed shard 1 (inactive)");
    }

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool_0);
    pools.insert(ShardId::new(1), pool_1);
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let app = build_sharded_app(
        HarvestDbPool::sharded(sharded_pool),
        router,
        vec![],
        vec![static_throttled_info(name, "5/m", 5.0)],
    );

    let (status, body) = get_json(&app, &format!("/admin/start-throttle/{name}/override")).await;

    // Both shards answered successfully -- this is a disagreement, not an
    // outage, so `shard_errors` must be empty even though the status is
    // 207.
    assert!(
        body["shard_errors"]
            .as_array()
            .is_some_and(std::vec::Vec::is_empty),
        "both shards are fully reachable -- shard_errors must be empty: {body}"
    );
    assert_eq!(
        status,
        StatusCode::MULTI_STATUS,
        "a genuine cross-shard override disagreement must not be silently \
         flattened into a plain 200: {body}"
    );
    assert_eq!(
        body["shard_disagreement"],
        json!(true),
        "the two shards disagree on override state and must say so: {body}"
    );

    let overridden = &body["override"];
    assert_eq!(
        overridden["override_active"],
        json!(true),
        "the SAFE answer -- a live override on ANY shard must never be \
         silently reported as inactive just because another shard has \
         fewer raw tokens: {overridden}"
    );
    assert_eq!(overridden["override_refill_rate"], json!(1000.0));
    assert_eq!(overridden["override_burst"], json!(1000.0));

    let shards = &body["shards"];
    assert_eq!(
        shards["0"]["override_active"],
        json!(true),
        "per-shard breakdown must show shard 0 as active: {shards}"
    );
    assert_eq!(
        shards["1"]["override_active"],
        json!(false),
        "per-shard breakdown must show shard 1 as inactive: {shards}"
    );
}

// ── Round 5: cross-shard disagreement holes in the round-4 fix ─────────────

/// Issue #945 review, round 5, finding 1: `GET /admin/rate-limits` (the
/// LIST endpoint) still merged every key down to whichever shard had the
/// fewest raw `tokens`, converting ONLY that single row -- so a fully
/// reachable, genuinely disagreeing fleet (one shard actively overridden,
/// one not) could report `override_active: false` for a key that is, in
/// fact, still overridden on another live shard. This is the same class of
/// bug round 4 fixed for the single-key `get_start_throttle_pacing_override`
/// read; this test proves the fix now also covers the plain rate-limit
/// list.
#[tokio::test]
async fn list_rate_limits_reports_disagreement_across_live_shards() {
    let (url_0, _container_0) = setup_one_shard().await;
    let (url_1, _container_1) = setup_one_shard().await;
    let pool_0 = build_pool(&url_0);
    let pool_1 = build_pool(&url_1);

    let name = leaked_name("send_email");

    // Shard 0: a live ACTIVE override, seeded with MORE tokens than shard 1
    // -- this is what makes the pre-fix "fewest tokens wins" merge pick the
    // WRONG (inactive) shard as the sole representative.
    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(300);
    {
        let mut conn = pool_0.get().await.expect("shard 0 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $2, $3, NOW(), NOW(), NOW(), $4, $4, $5)",
        )
        .bind::<diesel::sql_types::Text, _>(name)
        .bind::<diesel::sql_types::Double, _>(0.001)
        .bind::<diesel::sql_types::Double, _>(1000.0)
        .bind::<diesel::sql_types::Double, _>(1000.0)
        .bind::<diesel::sql_types::Timestamptz, _>(expires_at)
        .execute(&mut conn)
        .await
        .expect("seed shard 0 (active)");
    }

    // Shard 1: no override at all (NULL override columns), seeded with
    // FEWER tokens than shard 0.
    {
        let mut conn = pool_1.get().await.expect("shard 1 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $2, $3, NOW(), NOW(), NOW(), NULL, NULL, NULL)",
        )
        .bind::<diesel::sql_types::Text, _>(name)
        .bind::<diesel::sql_types::Double, _>(0.001)
        .bind::<diesel::sql_types::Double, _>(5.0)
        .execute(&mut conn)
        .await
        .expect("seed shard 1 (inactive)");
    }

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool_0);
    pools.insert(ShardId::new(1), pool_1);
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let app = build_sharded_app(
        HarvestDbPool::sharded(sharded_pool),
        router,
        vec![rate_limited_activity_info(name, 0.001, 5.0)],
        vec![],
    );

    let (status, list) = get_json(&app, "/admin/rate-limits").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the list read itself never fails on a genuine disagreement, only surfaces it: {list}"
    );
    let entry = list
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["key"] == json!(name))
        .unwrap_or_else(|| panic!("{name} bucket listed: {list:?}"));

    assert_eq!(
        entry["override_active"],
        json!(true),
        "the SAFE answer -- a live override on ANY shard must never be \
         silently reported as inactive just because another shard has \
         fewer raw tokens (this is the exact pre-fix bug): {entry}"
    );
    assert_eq!(
        entry["shard_disagreement"],
        json!(true),
        "the two shards genuinely disagree on override state and the list \
         entry must say so: {entry}"
    );
}

/// Issue #945 review, round 5, finding 2: the round-4 disagreement check
/// compared only the RAW `override_refill_rate`/`override_burst` columns.
/// A partial legacy `POST /admin/rate-limits/{key}` fan-out (issue #332)
/// can leave shards with divergent persisted BASELINES; a subsequent
/// one-field override (only `refill_rate` set, `burst` omitted) then
/// writes byte-IDENTICAL override columns to every shard -- so the raw
/// comparison reports "no disagreement" even though the *resolved
/// effective* burst genuinely differs per shard (each omitted-override
/// field falls back to that shard's own, diverged, baseline burst).
#[tokio::test]
async fn get_start_throttle_pacing_override_detects_diverged_baseline_disagreement() {
    let (url_0, _container_0) = setup_one_shard().await;
    let (url_1, _container_1) = setup_one_shard().await;
    let pool_0 = build_pool(&url_0);
    let pool_1 = build_pool(&url_1);

    let name = leaked_name("onboard_user");
    let key = autumn_harvest::throttle::bucket_key(name, "");

    // Shard 0: baseline burst = 20.0 (as if a fully successful legacy SET),
    // override_refill_rate = 5.0, override_burst = NULL (falls back to
    // this shard's baseline: 20.0).
    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(300);
    {
        let mut conn = pool_0.get().await.expect("shard 0 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $3, $3, NOW(), NOW(), NOW(), $4, NULL, $5)",
        )
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Double, _>(50.0)
        .bind::<diesel::sql_types::Double, _>(20.0)
        .bind::<diesel::sql_types::Double, _>(5.0)
        .bind::<diesel::sql_types::Timestamptz, _>(expires_at)
        .execute(&mut conn)
        .await
        .expect("seed shard 0");
    }

    // Shard 1: DIVERGED baseline burst = 30.0 (as if a partial legacy
    // `POST /admin/rate-limits/{key}` only reached shard 0, or a later
    // fan-out to shard 1 alone changed it), the SAME
    // override_refill_rate = 5.0, override_burst = NULL (falls back to
    // THIS shard's baseline: 30.0). Raw override columns are byte-identical
    // to shard 0's.
    {
        let mut conn = pool_1.get().await.expect("shard 1 conn");
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at, \
              override_refill_rate, override_burst, override_expires_at) \
             VALUES ($1, $2, $3, $3, NOW(), NOW(), NOW(), $4, NULL, $5)",
        )
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Double, _>(100.0)
        .bind::<diesel::sql_types::Double, _>(30.0)
        .bind::<diesel::sql_types::Double, _>(5.0)
        .bind::<diesel::sql_types::Timestamptz, _>(expires_at)
        .execute(&mut conn)
        .await
        .expect("seed shard 1");
    }

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), pool_0);
    pools.insert(ShardId::new(1), pool_1);
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let app = build_sharded_app(
        HarvestDbPool::sharded(sharded_pool),
        router,
        vec![],
        vec![static_throttled_info(name, "5/m", 5.0)],
    );

    let (status, body) = get_json(&app, &format!("/admin/start-throttle/{name}/override")).await;

    assert!(
        body["shard_errors"]
            .as_array()
            .is_some_and(std::vec::Vec::is_empty),
        "both shards are fully reachable: {body}"
    );
    assert_eq!(
        status,
        StatusCode::MULTI_STATUS,
        "byte-identical override columns must not mask a genuine \
         effective-value disagreement caused by diverged persisted \
         baselines: {body}"
    );
    assert_eq!(
        body["shard_disagreement"],
        json!(true),
        "resolved effective burst differs (20.0 vs 30.0) even though the \
         raw override_refill_rate/override_burst columns are identical on \
         both shards: {body}"
    );

    let shards = &body["shards"];
    assert_eq!(
        shards["0"]["effective_burst"],
        json!(20.0),
        "shard 0's omitted override_burst falls back to ITS OWN baseline: {shards}"
    );
    assert_eq!(
        shards["1"]["effective_burst"],
        json!(30.0),
        "shard 1's omitted override_burst falls back to ITS OWN (diverged) baseline: {shards}"
    );
}
