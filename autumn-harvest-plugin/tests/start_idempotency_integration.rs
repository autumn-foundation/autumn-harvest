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
use autumn_harvest::types::ShardId;
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
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
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
    "\n",
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
    include_str!(
        "../../autumn-harvest/migrations/20260708000001_harvest_completion_trigger_condition/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    "\n",
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
    include_str!("../../autumn-harvest/migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260628000001_harvest_execution_origin/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #808: the start-idempotency table under test.
    include_str!("../../autumn-harvest/migrations/20260709000000_harvest_start_idempotency/up.sql"),
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
        .uri(format!("/api/harvest/workflows/{wf}/start"))
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

/// FINDING 2 (Codex P2, issue #808): a keyed replay of an already-successful
/// start must bypass a raised admission gate (the reserve path resolves it to a
/// 200 no-op — not fresh admission). A genuinely fresh keyed start is still
/// gated.
#[tokio::test]
async fn keyed_replay_bypasses_a_raised_admission_gate() {
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
    };
    let lift_gate = || api_state.initialize_gate_cache(vec![]);

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
}
