#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
// `use` inside a test fn keeps the Diesel DSL import scoped to the one test
// that needs it, matching the sibling multi-shard suites.
#![allow(clippy::items_after_statements)]
//! HTTP integration tests for explicit shard pinning / data-residency workflow
//! placement (issue #697).
//!
//! Exercises `POST /workflows/{name}/start`'s optional placement fields against
//! two **genuinely separate** Postgres shard databases:
//! - **`AC1a`** — an explicit `shard_id` pins the execution to that shard.
//! - **`AC1b`** — a `residency_key` resolves through the router's declared
//!   residency map to the same shard, deterministically.
//! - **AC1 (no placement)** — omitting both is byte-for-byte today's rendezvous
//!   routing, and the response omits `shard_id` entirely.
//! - **AC2/AC6** — an out-of-range shard, a drained (readable-but-not-writable)
//!   shard, an unmapped residency key, and supplying *both* fields each return
//!   a `400` JSON error — never a `500`, never a silent fall back to the
//!   default shard, never a silent re-hash.
//! - **AC5** — the chosen shard is observable after start: the response's
//!   `shard_id`, the encoded `ExecutionId`, and the row's `shard_id` column all
//!   agree, and the row exists on the intended shard's database **only**.
//! - **Success metric** — N workflows across K residency keys land 100% on the
//!   intended shard.
//!
//! Dual-mode: uses a running Postgres from `HARVEST_TEST_DATABASE_URL` (as an
//! admin URL onto which two fresh shard databases are created) when set — no
//! Docker required — else boots a fresh testcontainers Postgres. Docker is not
//! available in every sandbox, so these tests are compile-checked there and run
//! Docker-backed in CI (matching the #543/#544/#601 precedent).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::debounce::DebouncePolicy;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
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

const EU_SHARD: i32 = 0;
const US_SHARD: i32 = 1;
/// Readable but deliberately kept OUT of `writable_shards`, standing in for a
/// shard an operator is draining.
const DRAINED_SHARD: i32 = 2;
/// Readable AND writable in the router, but with **no pool entry** — a shard
/// mid a shard-add rollout. Pinning to it must fail closed, never fall back to
/// the default shard's database.
const UNPOOLED_SHARD: i32 = 3;

/// Derive a per-shard database URL from the admin/base URL, replacing the
/// database-name path segment with `dbname` while preserving any query string.
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

/// Three independent shard databases (EU / US / drained) plus an optional
/// container guard.
async fn setup_three_shards() -> ((String, String, String), Option<ContainerAsync<Postgres>>) {
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

    let names: Vec<String> = (0..3)
        .map(|_| format!("harvest_shard_{}", Uuid::new_v4().simple()))
        .collect();

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in &names {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }

    let urls: Vec<String> = names.iter().map(|n| shard_url(&admin_url, n)).collect();
    for url in &urls {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("shard connect");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate shard");
    }

    ((urls[0].clone(), urls[1].clone(), urls[2].clone()), guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler: dummy_workflow,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
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

/// A workflow carrying a debounce policy, so a start against it is DEFERRED
/// and its shard is derived from the debounce key rather than any pin.
fn debounced_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.debounce = Some(DebouncePolicy {
        key_expr: "input.tenant_id",
        window: Duration::from_secs(30),
        max_wait: None,
    });
    info
}

/// A workflow carrying a start throttle (issue #607) with a **single** token,
/// so the first start reserves it and proceeds while the second is DEFERRED
/// (`202`). Unlike debounce/batch, a throttled start threads the resolved
/// shard through to `reserve_or_defer`, so a pin IS honoured there.
fn throttled_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.throttle = Some(ThrottlePolicy {
        // One token per hour, capacity one: start #1 debits it, start #2 defers.
        refill_per_sec: 1.0 / 3600.0,
        burst: 1.0,
        key_expr: None,
        schedule_to_start: None,
    });
    info
}

/// A three-shard app whose router declares a residency map:
/// `eu -> 0`, `us -> 1`. Shard 2 is readable but **not** writable (drained).
fn build_app(url_eu: &str, url_us: &str, url_drained: &str) -> HarvestApiApp {
    build_app_with_writable(url_eu, url_us, url_drained, &[EU_SHARD, US_SHARD])
}

/// As [`build_app`] but with a caller-chosen writable set, so a test can model
/// a shard being drained after work was already placed on it.
fn build_app_with_writable(
    url_eu: &str,
    url_us: &str,
    url_drained: &str,
    writable: &[i32],
) -> HarvestApiApp {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(EU_SHARD), build_pool(url_eu));
    pools.insert(ShardId::new(US_SHARD), build_pool(url_us));
    pools.insert(ShardId::new(DRAINED_SHARD), build_pool(url_drained));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(EU_SHARD)));

    let router = ShardRouter::new(
        vec![
            ShardId::new(EU_SHARD),
            ShardId::new(US_SHARD),
            ShardId::new(DRAINED_SHARD),
        ],
        writable.iter().copied().map(ShardId::new).collect(),
        ShardId::new(EU_SHARD),
    )
    .with_residency_map([
        ("eu".to_string(), ShardId::new(EU_SHARD)),
        ("us".to_string(), ShardId::new(US_SHARD)),
    ]);

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    let registry = HandlerRegistry::new(
        vec![
            plain_info("order_flow"),
            debounced_info("debounced_flow"),
            throttled_info("throttled_flow"),
        ],
        vec![],
    );
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("placement-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_start(app: &HarvestApiApp, body: Value) -> (StatusCode, Value) {
    post_start_inner(app, "order_flow", body, None).await
}

/// Start a *named* workflow (for the debounce/batch deferred-start cases).
async fn post_start_to(app: &HarvestApiApp, workflow: &str, body: Value) -> (StatusCode, Value) {
    post_start_inner(app, workflow, body, None).await
}

/// Start with an `Idempotency-Key` header (issue #808).
async fn post_start_keyed(app: &HarvestApiApp, body: Value, key: &str) -> (StatusCode, Value) {
    post_start_inner(app, "order_flow", body, Some(key)).await
}

async fn post_start_inner(
    app: &HarvestApiApp,
    workflow: &str,
    body: Value,
    idem_key: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/workflows/{workflow}/start"))
        .header("content-type", "application/json");
    if let Some(key) = idem_key {
        builder = builder.header("Idempotency-Key", key);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    // AC6 says the rejection is a **400 JSON** body. Parsing strictly (rather
    // than falling back to `Value::Null`) is what makes the rejection tests
    // falsifiable: a `text/plain` 400 would otherwise satisfy every
    // `body.get("execution_id").is_none()` assertion vacuously.
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "response body must be JSON (status {status}, parse error {error}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, parsed)
}

/// Count executions with this business `workflow_id` on the given shard DB.
async fn workflow_id_count(url: &str, workflow_id: &str) -> i64 {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    dsl::harvest_workflow_executions
        .filter(dsl::workflow_id.eq(workflow_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count")
}

/// Count `harvest_workflow_executions` rows with this id on the given shard DB.
async fn row_count(url: &str, exec_id: ExecutionId) -> i64 {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    dsl::harvest_workflow_executions
        .filter(dsl::id.eq(exec_id.as_uuid()))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count")
}

/// The `shard_id` column value for this execution on the given shard DB.
async fn row_shard_id(url: &str, exec_id: ExecutionId) -> i32 {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    dsl::harvest_workflow_executions
        .filter(dsl::id.eq(exec_id.as_uuid()))
        .select(dsl::shard_id)
        .get_result(&mut conn)
        .await
        .expect("shard_id")
}

fn exec_id_of(body: &Value) -> ExecutionId {
    body["execution_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no execution_id in {body}"))
        .parse()
        .expect("execution id parses")
}

// ── AC1a + AC5: explicit shard pin ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_shard_id_pins_the_execution_and_is_observable() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-eu-1", "input": {"n": 1}, "shard_id": US_SHARD }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    // AC5: the response reflects the pinned shard.
    assert_eq!(
        body["shard_id"].as_i64(),
        Some(i64::from(US_SHARD)),
        "response must report the pinned shard: {body}"
    );

    // AC5: the encoded ExecutionId agrees.
    let exec_id = exec_id_of(&body);
    assert_eq!(exec_id.shard(), ShardId::new(US_SHARD));

    // AC5: the row lives on the intended shard's database ONLY.
    assert_eq!(row_count(&us, exec_id).await, 1, "row must be on US");
    assert_eq!(row_count(&eu, exec_id).await, 0, "row must NOT be on EU");
    assert_eq!(row_shard_id(&us, exec_id).await, US_SHARD);
}

// ── AC1b + AC5: residency key ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residency_key_pins_the_execution_to_the_mapped_shard() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-us-1", "input": {}, "residency_key": "us" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(body["shard_id"].as_i64(), Some(i64::from(US_SHARD)));

    let exec_id = exec_id_of(&body);
    assert_eq!(exec_id.shard(), ShardId::new(US_SHARD));
    assert_eq!(row_count(&us, exec_id).await, 1);
    assert_eq!(row_count(&eu, exec_id).await, 0);
}

// ── Success metric: N workflows across K residency keys ──────────────────────

/// Issue #697 success metric: **100%** of workflows started with a residency
/// key land on the intended shard. Runs N starts across K keys and asserts
/// every single one — no sampling, no tolerance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residency_conformance_n_workflows_across_k_keys() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    const N: usize = 12;
    let cases = [("eu", EU_SHARD, &eu), ("us", US_SHARD, &us)];

    for (key, expected_shard, expected_url) in cases {
        for i in 0..N {
            let (status, body) = post_start(
                &app,
                json!({
                    "workflow_id": format!("conformance-{key}-{i}"),
                    "input": {},
                    "residency_key": key,
                }),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "start {key}/{i}: {body}");

            let exec_id = exec_id_of(&body);
            assert_eq!(
                exec_id.shard(),
                ShardId::new(expected_shard),
                "residency key '{key}' run {i} must land on shard {expected_shard}"
            );
            assert_eq!(
                body["shard_id"].as_i64(),
                Some(i64::from(expected_shard)),
                "residency key '{key}' run {i} response shard"
            );
            assert_eq!(
                row_count(expected_url, exec_id).await,
                1,
                "residency key '{key}' run {i} row must be on shard {expected_shard}"
            );
        }
    }
}

// ── AC1: no placement is byte-for-byte today's routing ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_placement_uses_rendezvous_routing_and_omits_shard_id() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-unpinned-1", "input": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");

    // The response must NOT carry `shard_id` when no placement was requested —
    // byte-for-byte the pre-#697 response shape.
    assert!(
        body.get("shard_id").is_none(),
        "an unpinned start must omit shard_id: {body}"
    );

    // It still landed *somewhere* writable, chosen by the unchanged hash.
    let exec_id = exec_id_of(&body);
    let router = ShardRouter::new(
        vec![
            ShardId::new(EU_SHARD),
            ShardId::new(US_SHARD),
            ShardId::new(DRAINED_SHARD),
        ],
        vec![ShardId::new(EU_SHARD), ShardId::new(US_SHARD)],
        ShardId::new(EU_SHARD),
    );
    assert_eq!(
        exec_id.shard(),
        router.pick_for_new_workflow("order_flow", "order-unpinned-1"),
        "an unpinned start must use the unchanged rendezvous hash"
    );
}

// ── AC2 + AC6: rejections are 400 JSON, never 500, never a silent fallback ───

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_shard_is_rejected_400_and_starts_nothing() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-bad-shard", "input": {}, "shard_id": 99 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an out-of-range shard must be a 400, not a 500 or a silent default: {body}"
    );
    assert!(
        body.get("execution_id").is_none(),
        "a rejected placement must start nothing: {body}"
    );

    // And nothing was silently written to the default shard.
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&eu)
        .await
        .expect("connect");
    let count: i64 = dsl::harvest_workflow_executions
        .filter(dsl::workflow_id.eq("order-bad-shard"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(count, 0, "no silent fallback to the default shard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_shard_pin_is_rejected_400() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-drained", "input": {}, "shard_id": DRAINED_SHARD }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "pinning to a readable-but-draining shard must be rejected: {body}"
    );
    assert!(
        body.get("execution_id").is_none(),
        "a rejected placement must start nothing: {body}"
    );
    // The rejection must start nothing ANYWHERE -- neither on the drained shard
    // nor via a silent fall back to the default shard.
    for (label, url) in [("drained", &drained), ("default", &eu)] {
        assert_eq!(
            workflow_id_count(url, "order-drained").await,
            0,
            "no execution should exist on the {label} shard"
        );
    }

    // The client-facing body must NOT enumerate the writable set (which would
    // leak live drain state); the operator-facing detail is logged instead.
    let rendered = body.to_string();
    assert!(
        !rendered.contains("writable shards are"),
        "the 400 body must not enumerate the deployment's writable shard set: {body}"
    );
}

/// A drained pin is a *fresh-start-only* rejection: it must sit after the #808
/// committed-replay probe, so an at-least-once retry of a keyed start that
/// already committed still returns its `200` no-op rather than flipping to a
/// `400` the moment the operator begins draining the pinned shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_keyed_replay_survives_the_pinned_shard_being_drained() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;

    // Deployment A: shard 1 (US) is writable. Commit a keyed, US-pinned start.
    let app = build_app_with_writable(&eu, &us, &drained, &[EU_SHARD, US_SHARD]);
    let (status, body) = post_start_keyed(
        &app,
        json!({ "input": {}, "residency_key": "us" }),
        "delivery-drain-1",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "first delivery: {body}");
    let original = exec_id_of(&body);
    assert_eq!(original.shard(), ShardId::new(US_SHARD));

    // Deployment B: same databases, but shard 1 has since been drained out of
    // the writable set. The provider retries the same delivery.
    let drained_app = build_app_with_writable(&eu, &us, &drained, &[EU_SHARD]);
    let (retry_status, retry_body) = post_start_keyed(
        &drained_app,
        json!({ "input": {}, "residency_key": "us" }),
        "delivery-drain-1",
    )
    .await;
    assert_eq!(
        retry_status,
        StatusCode::OK,
        "a committed keyed replay must still dedup to 200 even though the pinned \
         shard is now draining -- the retry creates no new work: {retry_body}"
    );
    assert_eq!(
        exec_id_of(&retry_body),
        original,
        "the replay must return the original execution"
    );

    // But a GENUINELY FRESH pinned start at the drained shard is still refused.
    let (fresh_status, fresh_body) = post_start_keyed(
        &drained_app,
        json!({ "input": {}, "residency_key": "us" }),
        "delivery-drain-UNSEEN",
    )
    .await;
    assert_eq!(
        fresh_status,
        StatusCode::BAD_REQUEST,
        "a fresh start at a drained shard must still be rejected: {fresh_body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmapped_residency_key_is_rejected_400_never_hashed() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-apac", "input": {}, "residency_key": "apac" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an unmapped residency key must be rejected, never silently hashed: {body}"
    );
    assert!(body.get("execution_id").is_none(), "body: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supplying_both_placement_fields_is_rejected_400() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({
            "workflow_id": "order-both",
            "input": {},
            "shard_id": US_SHARD,
            "residency_key": "eu",
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "shard_id and residency_key are mutually exclusive: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blank_residency_key_is_rejected_400() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": "order-blank", "input": {}, "residency_key": "   " }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
}

/// A debounced (or batched) start is DEFERRED and derives its shard from the
/// debounce/batch key so all admissions for that key collapse on one shard.
/// A per-request pin cannot be honoured there, so it must be REJECTED rather
/// than silently ignored — silently accepting it would return `202` with no
/// `shard_id` for the caller to check while placing residency-bound data on a
/// hash-chosen shard, the exact silent-fallback this feature exists to remove.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_on_a_debounced_workflow_is_rejected_not_silently_ignored() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    for placement in [
        json!({ "residency_key": "us" }),
        json!({ "shard_id": US_SHARD }),
    ] {
        let mut body = json!({ "workflow_id": "deb-1", "input": { "tenant_id": "acme" } });
        for (k, v) in placement.as_object().expect("object") {
            body[k] = v.clone();
        }

        let (status, resp) = post_start_to(&app, "debounced_flow", body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a pinned debounced start must be rejected, never accepted-and-re-hashed: {resp}"
        );
        // Specifically NOT the 202-Accepted deferred-start response.
        assert!(
            resp.get("debounced").is_none(),
            "the request must not have been admitted as a deferred start: {resp}"
        );
    }

    // The same workflow WITHOUT a pin still debounces normally (the guard is
    // scoped to pinned requests only).
    let (status, resp) = post_start_to(
        &app,
        "debounced_flow",
        json!({ "workflow_id": "deb-2", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "an UNPINNED debounced start must be unaffected: {resp}"
    );
}

/// A shard the **router** knows (readable + writable) but for which the pool
/// map has **no entry** — a real state mid a shard-add rollout, since the
/// router's shard set and the pool map are configured independently.
///
/// This is the exact silent-residency-breach path: `pool_for` falls back to the
/// default shard's pool, so without an explicit-pool lookup the row would be
/// written to the **default** database while the response, the encoded
/// `ExecutionId`, and the row's `shard_id` column all claimed the pinned shard.
/// The pinned branch must therefore fail closed with a `503` and write nothing.
fn build_app_with_unpooled_shard(url_eu: &str, url_us: &str, url_drained: &str) -> HarvestApiApp {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(EU_SHARD), build_pool(url_eu));
    pools.insert(ShardId::new(US_SHARD), build_pool(url_us));
    pools.insert(ShardId::new(DRAINED_SHARD), build_pool(url_drained));
    // NOTE: deliberately NO pool for UNPOOLED_SHARD.
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(EU_SHARD)));

    let router = ShardRouter::new(
        vec![
            ShardId::new(EU_SHARD),
            ShardId::new(US_SHARD),
            ShardId::new(DRAINED_SHARD),
            ShardId::new(UNPOOLED_SHARD),
        ],
        vec![
            ShardId::new(EU_SHARD),
            ShardId::new(US_SHARD),
            ShardId::new(UNPOOLED_SHARD),
        ],
        ShardId::new(EU_SHARD),
    )
    .with_residency_map([
        ("eu".to_string(), ShardId::new(EU_SHARD)),
        ("us".to_string(), ShardId::new(US_SHARD)),
    ]);

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    let registry = HandlerRegistry::new(
        vec![
            plain_info("order_flow"),
            debounced_info("debounced_flow"),
            throttled_info("throttled_flow"),
        ],
        vec![],
    );
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("placement-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// AC2/AC5 (fail closed): a pin to a shard the router accepts but that has no
/// pool of its own must **not** silently write to the default shard's database
/// while reporting the pinned shard back to the caller. It is a `503` (a
/// deployment-configuration gap, not a caller error) and nothing is started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routable_but_unpooled_shard_fails_closed_503_and_writes_nothing() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app_with_unpooled_shard(&eu, &us, &drained);

    let workflow_id = "order-unpooled";
    let (status, body) = post_start(
        &app,
        json!({ "workflow_id": workflow_id, "input": {}, "shard_id": UNPOOLED_SHARD }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a routable-but-unpooled pinned shard must fail closed with 503, never \
         201 (a silent write to the default shard) and never 500: {body}"
    );
    assert!(
        body.get("execution_id").is_none(),
        "a failed-closed pin must start nothing: {body}"
    );

    // The load-bearing half: the row must NOT have landed on the default shard.
    assert_eq!(
        workflow_id_count(&eu, workflow_id).await,
        0,
        "the default shard's database must be untouched -- a silent fallback \
         here would write residency-bound data to the wrong country"
    );
    assert_eq!(workflow_id_count(&us, workflow_id).await, 0);
    assert_eq!(workflow_id_count(&drained, workflow_id).await, 0);
}

/// Codex P1 (issue #697 review): the #808 committed-replay probe must resolve
/// a **pinned** request's connection with `exact_pool_for`, never `pool_for`.
///
/// `pool_for` falls back to the DEFAULT shard's pool for a shard with no pool
/// entry, so a pinned-but-unpooled request would probe the **wrong database**
/// for its idempotency claim. When a same-key claim already exists there (from
/// an earlier unpinned start) that is a false HIT: the caller gets `200` plus
/// an `execution_id` on the *default* shard while having explicitly asked for a
/// different jurisdiction -- a silent cross-shard residency breach, and exactly
/// the failure this feature exists to remove. Failing closed (`503`) is right.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_committed_replay_probe_never_falls_back_to_the_default_shard() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let key = "delivery-probe-fallback-1";

    // Commit a keyed start on the DEFAULT shard (EU) by making it the only
    // writable shard, so the claim row deterministically lands there.
    let eu_only = build_app_with_writable(&eu, &us, &drained, &[EU_SHARD]);
    let (status, body) = post_start_keyed(
        &eu_only,
        json!({ "workflow_id": "order-probe", "input": {} }),
        key,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "seed start: {body}");
    let seeded = exec_id_of(&body);
    assert_eq!(
        seeded.shard(),
        ShardId::new(EU_SHARD),
        "the seeded claim must live on the default shard for this test to bite"
    );

    // Now retry the SAME key, but pinned to a routable-yet-unpooled shard.
    let unpooled_app = build_app_with_unpooled_shard(&eu, &us, &drained);
    let (retry_status, retry_body) = post_start_keyed(
        &unpooled_app,
        json!({ "workflow_id": "order-probe", "input": {}, "shard_id": UNPOOLED_SHARD }),
        key,
    )
    .await;

    assert_eq!(
        retry_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a pinned start at an unpooled shard must fail closed, never resolve a \
         committed replay out of the DEFAULT shard's database: {retry_body}"
    );
    // The load-bearing half: the default shard's execution must not be handed
    // back as though it satisfied the pinned request.
    assert!(
        retry_body.get("execution_id").is_none(),
        "the pinned request must return no execution: {retry_body}"
    );
    assert!(
        !retry_body.to_string().contains(&seeded.to_string()),
        "the default shard's execution ({seeded}) must never be returned for a \
         request pinned elsewhere: {retry_body}"
    );
    // And nothing new was written anywhere.
    assert_eq!(workflow_id_count(&eu, "order-probe").await, 1);
    assert_eq!(workflow_id_count(&us, "order-probe").await, 0);
    assert_eq!(workflow_id_count(&drained, "order-probe").await, 0);
}

/// Codex P2 (issue #697 review): a THROTTLED start honours an explicit pin --
/// the resolved shard is threaded into `reserve_or_defer` -- so its deferred
/// `202` must echo `shard_id` too. There is no `execution_id` to decode yet, so
/// without the echo a caller has no way at all to audit where a deferred pinned
/// start will run. An unpinned throttled `202` must stay byte-identical to a
/// pre-#697 build (no `shard_id` key at all).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttled_deferred_start_echoes_the_pinned_shard() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    // Start #1 debits the single token and runs immediately, pinned to US.
    let (first_status, first_body) = post_start_to(
        &app,
        "throttled_flow",
        json!({ "workflow_id": "thr-1", "input": {}, "shard_id": US_SHARD }),
    )
    .await;
    assert_eq!(
        first_status,
        StatusCode::CREATED,
        "first start: {first_body}"
    );
    assert_eq!(exec_id_of(&first_body).shard(), ShardId::new(US_SHARD));

    // Start #2 finds an empty bucket and is DEFERRED -- the 202 must still
    // report the shard the scanner will eventually fire it on.
    let (second_status, second_body) = post_start_to(
        &app,
        "throttled_flow",
        json!({ "workflow_id": "thr-2", "input": {}, "shard_id": US_SHARD }),
    )
    .await;
    assert_eq!(
        second_status,
        StatusCode::ACCEPTED,
        "second start must be throttled/deferred: {second_body}"
    );
    assert_eq!(
        second_body.get("throttled").and_then(Value::as_bool),
        Some(true),
        "expected the deferred-start body: {second_body}"
    );
    assert_eq!(
        second_body.get("shard_id").and_then(Value::as_i64),
        Some(i64::from(US_SHARD)),
        "a pinned throttled 202 must echo the resolved shard: {second_body}"
    );
}

/// The AC1 half of the throttle echo: an UNPINNED throttled `202` must omit
/// `shard_id` entirely, so a pre-#697 client sees a byte-identical body.
///
/// Uses an EU-only writable set so both starts deterministically land on the
/// same shard -- throttle buckets are **shard-local** (`start-throttle:{wf}:{key}`
/// lives in that shard's `harvest_rate_limit_buckets`), so an unpinned start
/// rendezvous-hashed onto a different shard would find a full bucket and run
/// immediately instead of deferring.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpinned_throttled_deferred_start_omits_shard_id() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app_with_writable(&eu, &us, &drained, &[EU_SHARD]);

    let (first_status, first_body) = post_start_to(
        &app,
        "throttled_flow",
        json!({ "workflow_id": "thr-open-1", "input": {} }),
    )
    .await;
    assert_eq!(
        first_status,
        StatusCode::CREATED,
        "first start: {first_body}"
    );

    let (second_status, second_body) = post_start_to(
        &app,
        "throttled_flow",
        json!({ "workflow_id": "thr-open-2", "input": {} }),
    )
    .await;
    assert_eq!(
        second_status,
        StatusCode::ACCEPTED,
        "second unpinned start must be throttled/deferred: {second_body}"
    );
    assert_eq!(
        second_body.get("throttled").and_then(Value::as_bool),
        Some(true),
        "expected the deferred-start body: {second_body}"
    );
    assert!(
        second_body.get("shard_id").is_none(),
        "an unpinned throttled 202 must omit shard_id entirely: {second_body}"
    );
}

/// AC6 over HTTP: a negative `shard_id` and the reserved `UNENCODED` sentinel
/// are both `400` at the API boundary. The pure-parse tests assert only that
/// the error names `shard_id`; these assert the end-to-end wire behaviour
/// (never a `500`, never a silent re-hash onto the default shard).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negative_and_sentinel_shard_ids_are_rejected_400_over_http() {
    let ((eu, us, drained), _guard) = setup_three_shards().await;
    let app = build_app(&eu, &us, &drained);

    for (label, shard_id, workflow_id) in [
        ("negative", -1_i64, "order-negative-shard"),
        ("unencoded sentinel", 65_535_i64, "order-sentinel-shard"),
    ] {
        let (status, body) = post_start(
            &app,
            json!({ "workflow_id": workflow_id, "input": {}, "shard_id": shard_id }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a {label} shard_id must be a 400: {body}"
        );
        assert!(
            body.get("execution_id").is_none(),
            "a {label} shard_id must start nothing: {body}"
        );
        assert_eq!(
            workflow_id_count(&eu, workflow_id).await,
            0,
            "a {label} shard_id must never silently re-hash onto the default shard"
        );
    }
}
