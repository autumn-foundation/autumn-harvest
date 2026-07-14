#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
//! HTTP integration tests for request-scoped start idempotency (issue #808).
//!
//! Exercises the plugin `POST /workflows/{name}/start` idempotency path against
//! a real Postgres container:
//! - **Header dedup** — two starts with the same `Idempotency-Key` header (and
//!   auto-generated workflow_ids) converge on one execution; first `201`
//!   (`started_fresh: true`), second `200` (`deduplicated: true`) with the same
//!   `execution_id`.
//! - **Body-field dedup** — same, keyed via the body `idempotency_key`.
//! - **No-key byte-identical** — a start without a key omits the two #808 flags.
//! - **Empty key → 400.**
//! - **Idempotency + throttle → 400** (mutually exclusive).
//! - **Concurrent same-key** — N simultaneous same-key starts → exactly one
//!   execution, zero errors.

use std::pin::Pin;
use std::sync::Arc;

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
use diesel::sql_types::Text;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
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

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(16)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn plain_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "tests",
        handler: dummy_workflow,
        execution_timeout: None,
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

fn throttled_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.throttle = Some(
        ThrottlePolicy::from_rate_str("100/m", Some(10.0), Some("input.tenant_id"), None)
            .expect("valid rate"),
    );
    info
}

/// A published input schema requiring an object with an `email` field.
fn require_email_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "email": { "type": "string" } },
        "required": ["email"]
    })
}

/// A workflow that publishes the `require_email_schema` input schema (issue #373),
/// so `POST /start` validates `input` against it on a *fresh* start.
fn schema_info(name: &'static str) -> WorkflowInfo {
    let mut info = plain_info(name);
    info.input_schema = Some(require_email_schema);
    info
}

fn build_app(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(infos, vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// POST a start, optionally supplying an `Idempotency-Key` header.
async fn post_start(
    app: &HarvestApiApp,
    wf: &str,
    body: Value,
    idem_header: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        // `harvest_api_router` mounts routes at the root (`/workflows/...`), not
        // under `/api/harvest` (the plugin nests that prefix outside this router);
        // sibling integration tests (e.g. `workflow_result_integration`,
        // `erase_payloads_integration`) hit the root path for the same reason.
        .uri(format!("/workflows/{wf}/start"))
        .header("content-type", "application/json")
        .header("x-harvest-admin", "true");
    if let Some(k) = idem_header {
        builder = builder.header("Idempotency-Key", k);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("POST request");
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

/// Like [`post_start`] but sends an arbitrary raw request body (which may be
/// malformed/undeserializable JSON), so tests can exercise the JSON-extractor
/// rejection path (issue #808, Codex P2).
async fn post_start_raw_body(
    app: &HarvestApiApp,
    wf: &str,
    raw_body: &str,
    idem_header: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/workflows/{wf}/start"))
        .header("content-type", "application/json")
        .header("x-harvest-admin", "true");
    if let Some(k) = idem_header {
        builder = builder.header("Idempotency-Key", k);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(raw_body.to_string())).unwrap())
        .await
        .expect("POST request");
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

async fn raw_connect(url: &str) -> diesel_async::AsyncPgConnection {
    diesel_async::AsyncPgConnection::establish(url)
        .await
        .expect("raw connection should establish")
}

async fn execution_count(conn: &mut diesel_async::AsyncPgConnection, wf: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<Text, _>(wf)
    .get_result::<Count>(conn)
    .await
    .expect("count executions")
    .count
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// AC: two starts with the same Idempotency-Key header (auto-generated
/// workflow_ids) converge on one execution — first 201 started_fresh, second
/// 200 deduplicated with the SAME execution_id.
#[tokio::test]
async fn header_key_dedups_to_one_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {"n": 1}}), Some("dk-1")).await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    assert_eq!(b1["started_fresh"], json!(true));
    assert_eq!(b1["deduplicated"], json!(false));
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(&app, "order_flow", json!({"input": {"n": 1}}), Some("dk-1")).await;
    assert_eq!(s2, StatusCode::OK, "second (dedup): {b2}");
    assert_eq!(b2["started_fresh"], json!(false));
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(
        b2["execution_id"].as_str().unwrap(),
        exec1,
        "dedup returns the original execution_id"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// AC: the key may also come from the body `idempotency_key` field.
#[tokio::test]
async fn body_key_dedups_to_one_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let body = json!({"input": {"n": 1}, "idempotency_key": "body-dk"});
    let (s1, b1) = post_start(&app, "order_flow", body.clone(), None).await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(&app, "order_flow", body, None).await;
    assert_eq!(s2, StatusCode::OK, "second: {b2}");
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// AC5: the header wins over the body field.
#[tokio::test]
async fn header_key_wins_over_body_field() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    // First start reserves under the HEADER key "H".
    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {}}), Some("H")).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // Second start supplies body key "H2" but header "H" — header wins, so it
    // deduplicates onto exec1 rather than starting fresh under "H2".
    let body = json!({"input": {}, "idempotency_key": "H2"});
    let (s2, b2) = post_start(&app, "order_flow", body, Some("H")).await;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// AC1/AC5: a no-key start's response is byte-for-byte identical to a pre-#808
/// build — the two flags are OMITTED, not present-as-null.
#[tokio::test]
async fn no_key_start_omits_the_new_response_flags() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let (s, b) = post_start(&app, "order_flow", json!({"input": {}}), None).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let obj = b.as_object().unwrap();
    assert!(
        !obj.contains_key("started_fresh"),
        "started_fresh must be omitted on the no-key path"
    );
    assert!(
        !obj.contains_key("deduplicated"),
        "deduplicated must be omitted on the no-key path"
    );
    // The classic fields are present and unchanged.
    assert!(obj.contains_key("execution_id"));
    assert_eq!(obj["workflow_name"], json!("order_flow"));
}

/// AC: an empty (whitespace-only) key is a client error, not a silent
/// non-idempotent start.
#[tokio::test]
async fn empty_key_is_rejected_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let (s, b) = post_start(&app, "order_flow", json!({"input": {}}), Some("   ")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");

    let mut conn = raw_connect(&url).await;
    assert_eq!(
        execution_count(&mut conn, "order_flow").await,
        0,
        "no execution created on a rejected empty-key start"
    );
}

/// AC: idempotency_key is mutually exclusive with a throttle policy.
#[tokio::test]
async fn key_with_throttled_workflow_is_rejected_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![throttled_info("sync_tenant")]);

    let body = json!({"input": {"tenant_id": "acme"}});
    let (s, b) = post_start(&app, "sync_tenant", body, Some("dk")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"]
            .as_str()
            .unwrap_or_default()
            .contains("idempotency_key"),
        "error names the conflict: {b}"
    );
}

/// AC: N simultaneous same-key starts create exactly one execution, zero errors.
#[tokio::test]
async fn concurrent_same_key_starts_create_one_execution() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let n = 8;
    let mut handles = Vec::new();
    for _ in 0..n {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            post_start(&app, "order_flow", json!({"input": {"n": 1}}), Some("race")).await
        }));
    }

    let mut created = 0;
    let mut deduped = 0;
    let mut exec_ids = std::collections::HashSet::new();
    for h in handles {
        let (status, body) = h.await.expect("task join");
        assert!(
            status == StatusCode::CREATED || status == StatusCode::OK,
            "no errors under concurrency: {status} {body}"
        );
        exec_ids.insert(body["execution_id"].as_str().unwrap().to_string());
        if body["deduplicated"] == json!(true) {
            deduped += 1;
        } else if body["started_fresh"] == json!(true) {
            created += 1;
        }
    }

    assert_eq!(created, 1, "exactly one request created the run");
    assert_eq!(deduped, n - 1, "every other request deduped");
    assert_eq!(
        exec_ids.len(),
        1,
        "all requests report the same execution_id"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// FIX 3 (issue #808 review): an oversized key is a clean 400 at the boundary —
/// never a 500 from overflowing the composite-PK btree tuple limit at INSERT.
#[tokio::test]
async fn over_length_key_is_rejected_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let long_key = "x".repeat(600); // > MAX_START_IDEMPOTENCY_KEY_LEN (512)
    let (s, b) = post_start(&app, "order_flow", json!({"input": {}}), Some(&long_key)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{b}");
    assert!(
        b["error"].as_str().unwrap_or_default().contains("too long"),
        "error names the length cap: {b}"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(
        execution_count(&mut conn, "order_flow").await,
        0,
        "no execution created on a rejected over-length key"
    );
}

/// FIX 4 (issue #808 review): an out-of-range `execution_timeout_secs` on the
/// keyed path is a 400, not a panic/500 (the debounce/batch range guard is
/// skipped for keyed starts).
#[tokio::test]
async fn keyed_start_with_out_of_range_timeout_is_rejected_with_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let body = json!({
        "input": {},
        "execution_timeout_secs": i64::MAX,
    });
    let (s, b) = post_start(&app, "order_flow", body, Some("dk-timeout")).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "expected 400, not a panic: {b}");
    assert!(
        b["error"]
            .as_str()
            .unwrap_or_default()
            .contains("execution_timeout_secs"),
        "error names the field: {b}"
    );

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 0);
}

/// FIX 7 (issue #808 review): a same-key hit returns the cached outcome
/// regardless of `reuse_policy` — a second same-key start with `reject_duplicate`
/// is a 200 dedup (same execution_id), NOT a 409.
#[tokio::test]
async fn same_key_hit_short_circuits_reject_duplicate() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {}}), Some("K")).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // Same key, reject_duplicate reuse policy: the idempotency dedup precedes
    // the reuse-policy matrix, so this is a 200 dedup, not a 409.
    let body = json!({"input": {}, "reuse_policy": "reject_duplicate"});
    let (s2, b2) = post_start(&app, "order_flow", body, Some("K")).await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "same-key hit must not 409 under reject_duplicate: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// FIX 1 (issue #808 review): in a MULTI-shard deployment, two same-key starts
/// with auto-generated (omitted) workflow_ids converge on exactly ONE execution
/// — because the shard is derived from the KEY, not the per-request workflow_id.
/// Pre-fix (workflow_id routing) they could land on different shards → two
/// distinct claim rows → two executions.
#[tokio::test]
async fn multi_shard_same_key_converges_on_one_execution() {
    let (url0, _c0) = setup_database().await;
    let (url1, _c1) = setup_database().await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);

    // Two logical shards backed by two SEPARATE physical databases.
    let mut map = std::collections::BTreeMap::new();
    map.insert(ShardId::new(0), pool0);
    map.insert(ShardId::new(1), pool1);
    let sharded = ShardedDbPool::from_map(map, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded));
    let registry = HandlerRegistry::new(vec![plain_info("order_flow")], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-multishard".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    // Two same-key starts, each with an OMITTED workflow_id (auto-generated).
    let (s1, b1) = post_start(
        &app,
        "order_flow",
        json!({"input": {"n": 1}}),
        Some("delivery-9"),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(
        &app,
        "order_flow",
        json!({"input": {"n": 1}}),
        Some("delivery-9"),
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "second must dedup: {b2}");
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(
        b2["execution_id"].as_str().unwrap(),
        exec1,
        "both requests converge on the same execution across shards"
    );

    // Exactly one execution total across BOTH physical databases.
    let mut c0 = raw_connect(&url0).await;
    let mut c1 = raw_connect(&url1).await;
    let total =
        execution_count(&mut c0, "order_flow").await + execution_count(&mut c1, "order_flow").await;
    assert_eq!(total, 1, "exactly one execution across both shards");
}

/// FINDING 1 (Codex P1, issue #808): when the caller supplies an EXPLICIT
/// `workflow_id`, a keyed start must route by `workflow_id` (not the key) so the
/// reuse-policy matrix and the shard-local `(name, workflow_id)` uniqueness
/// invariant are preserved. Here a prior run with an explicit `workflow_id`
/// exists on the `workflow_id`-derived shard; a same-`workflow_id` keyed start
/// with `reject_duplicate` must see it and 409 — NOT create a second run on the
/// key-derived shard. The (wid, key) pair is chosen so the two rules genuinely
/// route to different shards, so a regression to key-routing would 201 instead.
#[tokio::test]
async fn explicit_workflow_id_keyed_start_preserves_reject_duplicate() {
    let (url0, _c0) = setup_database().await;
    let (url1, _c1) = setup_database().await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);

    let mut map = std::collections::BTreeMap::new();
    map.insert(ShardId::new(0), pool0);
    map.insert(ShardId::new(1), pool1);
    let sharded = ShardedDbPool::from_map(map, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    // Choose a (workflow_id, key) pair that routes to DIFFERENT shards, so this
    // test would fail (201, duplicate run) under the pre-fix key-routing.
    let wf = "order_flow";
    let key = "delivery-42";
    let key_shard = router.pick_for_idempotency_key(wf, key);
    let mut chosen_wid = None;
    for i in 0..10_000 {
        let wid = format!("wid-{i}");
        if router.pick_for_new_workflow(wf, &wid) != key_shard {
            chosen_wid = Some(wid);
            break;
        }
    }
    let wid = chosen_wid.expect("some workflow_id routes to a shard other than the key's");

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded));
    let registry = HandlerRegistry::new(vec![plain_info(wf)], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-explicit-wid".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    // Prior run: explicit workflow_id, NO key → routes by workflow_id.
    let (s0, b0) = post_start(
        &app,
        wf,
        json!({"input": {"n": 1}, "workflow_id": wid}),
        None,
    )
    .await;
    assert_eq!(s0, StatusCode::CREATED, "prior run: {b0}");

    // Keyed start, SAME explicit workflow_id, reject_duplicate → must 409
    // (routes by workflow_id, sees the prior run), not 201 on the key shard.
    let (s1, b1) = post_start(
        &app,
        wf,
        json!({"input": {"n": 2}, "workflow_id": wid, "reuse_policy": "reject_duplicate"}),
        Some(key),
    )
    .await;
    assert_eq!(
        s1,
        StatusCode::CONFLICT,
        "reuse policy must be preserved for an explicit workflow_id: {b1}"
    );

    // Exactly one execution total across both shards (no duplicate created).
    let mut c0 = raw_connect(&url0).await;
    let mut c1 = raw_connect(&url1).await;
    let total = execution_count(&mut c0, wf).await + execution_count(&mut c1, wf).await;
    assert_eq!(total, 1, "no duplicate run created on the key shard");
}

/// FINDING 1 (Codex P1, issue #808): an explicit-`workflow_id` keyed start still
/// dedups a same-`(workflow_id, key)` retry — both route by `workflow_id` to the
/// same shard, hit the same claim row, and the second returns the same
/// execution_id as a 200 no-op.
#[tokio::test]
async fn explicit_workflow_id_keyed_retry_dedups() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    let body = json!({"input": {"n": 1}, "workflow_id": "cart-7"});
    let (s1, b1) = post_start(&app, "order_flow", body.clone(), Some("evt-1")).await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    let (s2, b2) = post_start(&app, "order_flow", body, Some("evt-1")).await;
    assert_eq!(s2, StatusCode::OK, "retry dedups: {b2}");
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// FINDING (Codex P2, issue #808): when a keyed start OMITS `workflow_id`, the
/// server auto-generates it AND routes by the key. The auto-generated
/// `workflow_id` must be MINTED onto the key-shard so that its later explicit
/// reuse routes (via `pick_for_new_workflow`) to the same shard the execution
/// lives on. Here we assert that the RETURNED `workflow_id` hashes to the SAME
/// shard as the RETURNED `execution_id`'s encoded shard — i.e. the minting
/// worked.
#[tokio::test]
async fn auto_generated_workflow_id_belongs_to_the_key_shard() {
    let (url0, _c0) = setup_database().await;
    let (url1, _c1) = setup_database().await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);

    let mut map = std::collections::BTreeMap::new();
    map.insert(ShardId::new(0), pool0);
    map.insert(ShardId::new(1), pool1);
    let sharded = ShardedDbPool::from_map(map, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let router_check = router.clone();

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded));
    let registry = HandlerRegistry::new(vec![plain_info("order_flow")], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-mint-wid".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    // Keyed start OMITTING workflow_id (auto-generated + minted onto the key shard).
    let (s1, b1) = post_start(
        &app,
        "order_flow",
        json!({"input": {"n": 1}}),
        Some("mint-delivery"),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");

    let returned_wid = b1["workflow_id"].as_str().unwrap();
    let returned_exec = b1["execution_id"].as_str().unwrap();
    let exec_shard = returned_exec.parse::<ExecutionId>().unwrap().shard();

    // The minted workflow_id must route to the SAME shard the execution lives on.
    assert_eq!(
        router_check.pick_for_new_workflow("order_flow", returned_wid),
        exec_shard,
        "auto-generated workflow_id must be minted onto the key-routed shard so \
         its later explicit reuse routes to where the execution lives"
    );
}

/// FINDING (Codex P2, issue #808): because the auto-generated `workflow_id` is
/// minted onto the key-shard, a later request that reuses that RETURNED
/// `workflow_id` explicitly (with `reject_duplicate`, no key) routes to the same
/// shard, SEES the existing execution, and 409s — preserving the shard-local
/// `(workflow_name, workflow_id)` uniqueness invariant. Pre-fix, the random
/// auto-generated `workflow_id` could hash to a different shard, so the explicit
/// reuse would miss the run and create a second execution with the same id.
#[tokio::test]
async fn explicit_reuse_of_a_keyed_auto_workflow_id_is_seen_uniqueness_preserved() {
    let (url0, _c0) = setup_database().await;
    let (url1, _c1) = setup_database().await;
    let pool0 = build_pool(&url0);
    let pool1 = build_pool(&url1);

    let mut map = std::collections::BTreeMap::new();
    map.insert(ShardId::new(0), pool0);
    map.insert(ShardId::new(1), pool1);
    let sharded = ShardedDbPool::from_map(map, ShardId::new(0));
    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::sharded(sharded));
    let registry = HandlerRegistry::new(vec![plain_info("order_flow")], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-mint-reuse".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    let app = harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"));

    // Keyed start OMITTING workflow_id → minted onto the key shard.
    let (s1, b1) = post_start(
        &app,
        "order_flow",
        json!({"input": {"n": 1}}),
        Some("reuse-delivery"),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    let returned_wid = b1["workflow_id"].as_str().unwrap().to_string();

    // Explicit reuse of the RETURNED workflow_id, reject_duplicate, NO key.
    // Routes by workflow_id → the key shard → sees the run → 409.
    let (s2, b2) = post_start(
        &app,
        "order_flow",
        json!({"input": {"n": 2}, "workflow_id": returned_wid, "reuse_policy": "reject_duplicate"}),
        None,
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::CONFLICT,
        "explicit reuse of a minted keyed workflow_id must see the run and 409: {b2}"
    );

    // Exactly one execution total across both shards for that workflow_id.
    let mut c0 = raw_connect(&url0).await;
    let mut c1 = raw_connect(&url1).await;
    let total =
        execution_count(&mut c0, "order_flow").await + execution_count(&mut c1, "order_flow").await;
    assert_eq!(
        total, 1,
        "no duplicate run created for the minted workflow_id"
    );
}

/// FINDING 2 (Codex P2, issue #808): a keyed replay of an already-successful
/// start must bypass a raised admission gate (the reserve path resolves it to a
/// 200 no-op — not fresh admission). A genuinely fresh keyed start is still
/// gated.
#[tokio::test]
async fn keyed_replay_bypasses_a_raised_admission_gate() {
    use autumn_harvest::admission_gate::set_global_admission_gate_cache;
    use autumn_harvest::{AdmissionGate, AdmissionGateId, GateScope};

    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);

    // Build the app inline so we can manipulate the gate cache.
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let registry = HandlerRegistry::new(vec![plain_info("order_flow")], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("idem-gate".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    // PR #1051 (issue #618) made the CORE start primitive authoritative for
    // admission gating: `execution::evaluate_start_gate` reads the *process-global*
    // cache (`admission_gate::global_admission_gate_cache()`), not this api_state
    // -local one. So mutating the local cache below is invisible to enforcement
    // unless we also publish the (interior-mutable) cache handle to the global
    // static — exactly as `admission_gate_authoritative_localpg.rs` does. The
    // global is torn down to `None` at the end so sibling serial tests (this
    // suite runs `--test-threads=1`) are unaffected.
    let raise_gate = || {
        api_state.initialize_gate_cache(vec![AdmissionGate {
            id: AdmissionGateId(uuid::Uuid::new_v4()),
            scope: GateScope::WorkflowName("order_flow".to_string()),
            reason: "incident".to_string(),
            message: None,
            created_by: "op".to_string(),
            created_at: chrono::Utc::now(),
            expires_at: None,
        }]);
        set_global_admission_gate_cache(Some(api_state.gate_cache()));
    };
    let lift_gate = || {
        api_state.initialize_gate_cache(vec![]);
        set_global_admission_gate_cache(Some(api_state.gate_cache()));
    };

    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));

    // 1. Gate raised → a FRESH keyed start is rejected (503), as today.
    raise_gate();
    let (s0, _b0) = post_start(&app, "order_flow", json!({"input": {}}), Some("G")).await;
    assert_eq!(
        s0,
        StatusCode::SERVICE_UNAVAILABLE,
        "a fresh keyed start must still be gated"
    );

    // 2. Lift the gate and create the run + claim for key "G".
    lift_gate();
    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {}}), Some("G")).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // 3. Re-raise the gate; a same-key RETRY must bypass it → 200 dedup.
    raise_gate();
    let (s2, b2) = post_start(&app, "order_flow", json!({"input": {}}), Some("G")).await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "a keyed replay must bypass the raised gate: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);

    // Tear down the process-global cache so sibling serial tests in this binary
    // are not gated by a leaked handle.
    set_global_admission_gate_cache(None);
}

/// FINDING (Codex P2, issue #808): a committed keyed replay short-circuits to the
/// 200 no-op BEFORE fresh-start-only input-schema validation (#373). The first
/// start passes the schema; a same-key retry with a body that WOULD now fail the
/// schema is not re-validated — it returns the original execution as a 200 dedup,
/// never a 400.
#[tokio::test]
async fn keyed_replay_bypasses_input_schema_validation() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![schema_info("order_flow")]);

    // Sanity: a fresh start (different key) with an invalid body IS rejected —
    // proving the schema path is live in this harness.
    let (s_bad, b_bad) = post_start(
        &app,
        "order_flow",
        json!({"input": {}}),
        Some("fresh-invalid"),
    )
    .await;
    assert_eq!(
        s_bad,
        StatusCode::BAD_REQUEST,
        "a fresh keyed start with a schema-invalid body must be rejected: {b_bad}"
    );

    // 1. First start with key "K" and a schema-VALID body → 201.
    let (s1, b1) = post_start(
        &app,
        "order_flow",
        json!({"input": {"email": "a@b.com"}}),
        Some("K"),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // 2. Same-key retry with a body that WOULD fail the schema (no email) → must
    // short-circuit to the 200 no-op, NOT 400.
    let (s2, b2) = post_start(&app, "order_flow", json!({"input": {}}), Some("K")).await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "a keyed replay must bypass fresh-start-only schema validation: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// FINDING (Codex P2, issue #808): a committed keyed replay short-circuits BEFORE
/// completion-callback SSRF validation (#605). The first start has no callback; a
/// same-key retry carrying a callback target that WOULD be rejected by the SSRF
/// policy is not re-validated — it returns the original execution as a 200 dedup,
/// never a 422.
#[tokio::test]
async fn keyed_replay_bypasses_callback_validation() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    // A well-formed but non-allowlisted https callback target: the default SSRF
    // policy (HTTPS-only + allowlist-required) rejects it.
    let bad_callback =
        json!([{ "url": "https://evil.example.com/hook", "filter": {"type": "CompletedOnly"} }]);

    // Sanity: a fresh start (different key) carrying the bad callback IS rejected
    // (the SSRF-rejection path returns 422 Unprocessable Entity) — proving the
    // callback-validation path is live in this harness.
    let (s_bad, b_bad) = post_start(
        &app,
        "order_flow",
        json!({"input": {}, "completion_callbacks": bad_callback}),
        Some("fresh-cb"),
    )
    .await;
    assert_eq!(
        s_bad,
        StatusCode::UNPROCESSABLE_ENTITY,
        "a fresh keyed start with a rejected callback must be rejected: {b_bad}"
    );

    // 1. First start with key "C" and NO callback → 201.
    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {}}), Some("C")).await;
    assert_eq!(s1, StatusCode::CREATED, "{b1}");
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // 2. Same-key retry carrying the bad callback → must short-circuit to the
    // 200 no-op, NOT 400.
    let (s2, b2) = post_start(
        &app,
        "order_flow",
        json!({"input": {}, "completion_callbacks": bad_callback}),
        Some("C"),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "a keyed replay must bypass fresh-start-only callback validation: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// FINDING 1 (Codex P2): the committed-replay dedup probe runs BEFORE the
/// throttle/debounce/batch mutual-exclusion `400`. So a keyed start that
/// succeeded while the workflow had NO throttle policy must, on an
/// at-least-once retry made AFTER the workflow gained one, still return the
/// existing execution as a `200` no-op — never the mutual-exclusion `400`
/// (the retry creates no deferred start). A FRESH keyed start against the
/// throttled workflow (a probe miss) still `400`s.
#[tokio::test]
async fn keyed_replay_against_a_throttled_workflow_dedups() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);

    // 1. Original delivery: the workflow has NO throttle policy yet, so a keyed
    //    start goes through the normal reserve+start path and writes the
    //    `harvest_start_idempotency` claim + execution row (the real path,
    //    simulating a delivery made before the policy existed).
    let plain_app = build_app(&pool, vec![plain_info("sync_tenant")]);
    let (s1, b1) = post_start(
        &plain_app,
        "sync_tenant",
        json!({"input": {"tenant_id": "acme"}}),
        Some("delivery-42"),
    )
    .await;
    assert_eq!(
        s1,
        StatusCode::CREATED,
        "original keyed start creates: {b1}"
    );
    assert_eq!(b1["started_fresh"], json!(true));
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // 2. The workflow is LATER configured with a throttle policy. A same-key
    //    retry against the now-throttled app (same pool/DB) must dedup to the
    //    original run with `200`, NOT be rejected `400` by the mutual-exclusion.
    let throttled_app = build_app(&pool, vec![throttled_info("sync_tenant")]);
    let (s2, b2) = post_start(
        &throttled_app,
        "sync_tenant",
        json!({"input": {"tenant_id": "acme"}}),
        Some("delivery-42"),
    )
    .await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "a committed keyed replay must dedup, not hit the throttle mutual-exclusion 400: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["execution_id"].as_str().unwrap(), exec1);

    // 3. A FRESH keyed start (a probe miss — new key) against the throttled
    //    workflow still returns `400`: the mutual-exclusion applies to genuine
    //    fresh keyed starts.
    let (s3, b3) = post_start(
        &throttled_app,
        "sync_tenant",
        json!({"input": {"tenant_id": "acme"}}),
        Some("delivery-99"),
    )
    .await;
    assert_eq!(
        s3,
        StatusCode::BAD_REQUEST,
        "a fresh keyed start against a throttled workflow is still rejected: {b3}"
    );
    assert!(
        b3["error"]
            .as_str()
            .unwrap_or_default()
            .contains("idempotency_key"),
        "error names the conflict: {b3}"
    );

    // Exactly one execution overall (the retry deduped; the fresh 400 created nothing).
    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "sync_tenant").await, 1);
}

/// A malformed/undeserializable JSON body that is undeserializable at the axum
/// extractor level. Kept in one place so the three malformed-body tests below
/// exercise the identical wire input (`execution_timeout_secs` wrong-typed →
/// `JsonDataError`).
const MALFORMED_START_BODY: &str =
    r#"{"input": {"n": 1}, "execution_timeout_secs": "not-a-number"}"#;

/// issue #808 (Codex P2): a retry that carries its exactly-once key in the
/// `Idempotency-Key` HEADER, but whose JSON body is now malformed, must still
/// return the advertised `200` no-op for a committed claim — the body is
/// irrelevant on a key hit, so the JSON-extractor rejection must not win.
#[tokio::test]
async fn header_key_replay_survives_a_malformed_body() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    // Original delivery: valid body, auto-generated workflow_id (routes by key).
    let (s1, b1) = post_start(&app, "order_flow", json!({"input": {"n": 1}}), Some("mk-1")).await;
    assert_eq!(s1, StatusCode::CREATED, "first: {b1}");
    assert_eq!(b1["started_fresh"], json!(true));
    let exec1 = b1["execution_id"].as_str().unwrap().to_string();

    // Retry: SAME header key, MALFORMED body → 200 no-op, same execution_id,
    // NOT the extractor's 400/422.
    let (s2, b2) =
        post_start_raw_body(&app, "order_flow", MALFORMED_START_BODY, Some("mk-1")).await;
    assert_eq!(
        s2,
        StatusCode::OK,
        "committed header-key replay must dedup despite a malformed body: {b2}"
    );
    assert_eq!(b2["deduplicated"], json!(true));
    assert_eq!(b2["started_fresh"], json!(false));
    assert_eq!(
        b2["execution_id"].as_str().unwrap(),
        exec1,
        "dedup returns the original execution_id"
    );

    // Exactly one execution — the malformed-body retry created nothing new.
    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 1);
}

/// AC1: a malformed body with NO idempotency key returns the exact JSON-extractor
/// rejection (a client error, never a `200`) — byte-for-byte the no-key behavior,
/// unchanged by the #808 header-probe fix. A header-key malformed body with no
/// committed claim (a probe miss) returns the IDENTICAL rejection, proving the
/// fallback never fabricates a start.
#[tokio::test]
async fn malformed_body_returns_the_extractor_rejection_when_not_a_committed_replay() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, vec![plain_info("order_flow")]);

    // No key: the exact axum rejection (client error, never 200).
    let (s_nokey, _b_nokey) =
        post_start_raw_body(&app, "order_flow", MALFORMED_START_BODY, None).await;
    assert!(
        s_nokey.is_client_error(),
        "no-key malformed body returns a client error, got {s_nokey}"
    );
    assert_ne!(s_nokey, StatusCode::OK, "never a 200 no-op without a claim");

    // Header key present but no committed claim (probe miss) → identical
    // rejection (the fallback falls through to `rejection.into_response()`).
    let (s_miss, _b_miss) = post_start_raw_body(
        &app,
        "order_flow",
        MALFORMED_START_BODY,
        Some("never-started-key"),
    )
    .await;
    assert!(
        s_miss.is_client_error(),
        "header-key malformed body with no claim returns a client error, got {s_miss}"
    );
    assert_ne!(s_miss, StatusCode::OK, "a probe miss never returns 200");
    assert_eq!(
        s_miss, s_nokey,
        "AC1: the probe-miss rejection is byte-identical to the no-key rejection"
    );

    // Nothing was started by either malformed request.
    let mut conn = raw_connect(&url).await;
    assert_eq!(execution_count(&mut conn, "order_flow").await, 0);
}
