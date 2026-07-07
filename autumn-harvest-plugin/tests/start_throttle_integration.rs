#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
//! HTTP integration tests for the workflow-start throttle (issue #607).
//!
//! Exercises the plugin `POST /workflows/{name}/start` throttle admission path
//! against a real Postgres container:
//! - an under-limit start returns `201 Created` (a token was available);
//! - once the bucket is empty, the excess start returns `202 Accepted`
//!   (`throttled: true`) and a durable pending row is written;
//! - `GET /admin/start-throttle` surfaces the per-key backlog.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::throttle::ThrottlePolicy;
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
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
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
    // issue #607: the start-throttle table under test.
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_start_throttle/up.sql"),
    "\n",
    // issue #606: harvest_task_queue.session_id (worker sessions), merged in from trunk-dev.
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #607 code review: companion index for the per-key-fair scanner query.
    include_str!(
        "../../autumn-harvest/migrations/20260707000000_harvest_start_throttle_bucket_deferred_idx/up.sql"
    ),
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
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn dummy_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "status": "ok" })) })
}

fn throttled_info(rate: &str, burst: f64) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name: "sync_tenant",
        module: "tests",
        handler: dummy_workflow,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: Some(
            ThrottlePolicy::from_rate_str(rate, Some(burst), Some("input.tenant_id"), None)
                .expect("valid rate"),
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

/// A workflow with BOTH a throttle policy and a debounce policy configured
/// (code-review fix, issue #607): the batch-start endpoint must reject items
/// for this workflow with a clear mutual-exclusion error instead of silently
/// wasting a reserved throttle token on a debounce rejection.
fn throttled_and_debounced_info(rate: &str, burst: f64) -> WorkflowInfo {
    let mut info = throttled_info(rate, burst);
    info.name = "conflicting_policies";
    info.debounce = Some(autumn_harvest::debounce::DebouncePolicy {
        key_expr: "input.tenant_id",
        window: std::time::Duration::from_secs(30),
        max_wait: None,
    });
    info
}

fn build_app(pool: &DbPool, info: WorkflowInfo) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(vec![info], vec![]);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("throttle-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));

    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
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
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
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

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
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

#[tokio::test]
async fn under_limit_starts_then_defers_excess_and_backlog_is_visible() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 2 → the first two starts for a tenant admit immediately (201),
    // then the bucket empties and the rest defer (202).
    let app = build_app(&pool, throttled_info("100/m", 2.0));

    let start = |i: usize| {
        json!({
            "workflow_id": format!("job-{i}"),
            "input": { "tenant_id": "acme", "n": i },
        })
    };

    // First two: 201 Created (a token was available).
    for i in 0..2 {
        let (status, body) = post_json(&app, "/workflows/sync_tenant/start", start(i)).await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "under-limit start #{i}: {body:?}"
        );
    }

    // Next three: 202 Accepted, throttled + deferred.
    for i in 2..5 {
        let (status, body) = post_json(&app, "/workflows/sync_tenant/start", start(i)).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "throttled start #{i}: {body:?}"
        );
        assert_eq!(body["throttled"], json!(true));
        assert_eq!(body["throttle_key"], json!("acme"));
        assert_eq!(body["workflow_name"], json!("sync_tenant"));
    }

    // Operator visibility: the per-key backlog is 3.
    let (status, body) = get_json(&app, "/admin/start-throttle").await;
    assert_eq!(status, StatusCode::OK, "admin read: {body:?}");
    let arr = body.as_array().expect("array");
    let acme = arr
        .iter()
        .find(|e| e["throttle_key"] == json!("acme"))
        .expect("acme backlog present");
    assert_eq!(acme["deferred_count"], json!(3));
    assert_eq!(acme["workflow_name"], json!("sync_tenant"));
}

#[tokio::test]
async fn distinct_tenants_throttle_independently_over_http() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 1 → the second start for the SAME tenant defers, but a different
    // tenant's first start still admits (separate bucket).
    let app = build_app(&pool, throttled_info("100/m", 1.0));

    let (s1, _) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "a1", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);

    let (s2, b2) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "a2", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(s2, StatusCode::ACCEPTED, "same tenant defers: {b2:?}");

    let (s3, b3) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "b1", "input": { "tenant_id": "globex" } }),
    )
    .await;
    assert_eq!(
        s3,
        StatusCode::CREATED,
        "a different tenant is unaffected by acme's bucket: {b3:?}"
    );
}

/// A workflow with both a resolving throttle and a resolving debounce policy
/// must be rejected per-item over the batch-start route with a clear
/// mutual-exclusion error — not silently mishandled (reserving, then wasting, a
/// throttle token on a debounce rejection). Code-review fix, issue #607.
#[tokio::test]
async fn batch_start_rejects_item_with_conflicting_throttle_and_debounce_policies() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_and_debounced_info("100/m", 5.0));

    let (status, body) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                {
                    "workflow_name": "conflicting_policies",
                    "workflow_id": "job-1",
                    "input": { "tenant_id": "acme" },
                }
            ],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "best-effort batch: {body:?}");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["status"], json!("rejected"));
    assert_eq!(
        results[0]["error"],
        json!("start throttle is mutually exclusive with debounce")
    );
    // No throttle token was wasted: the bucket for this key is untouched (the
    // guard fires before reserve_or_defer is ever called), so no pending row
    // should exist either.
    let (backlog_status, backlog) = get_json(&app, "/admin/start-throttle").await;
    assert_eq!(backlog_status, StatusCode::OK);
    assert_eq!(
        backlog.as_array().map(Vec::len),
        Some(0),
        "no pending throttle row should be created for a rejected item: {backlog:?}"
    );
}

/// Code-review fix (issue #607, items 1 & 2): `context_headers`/`priority`
/// submitted on `POST /workflows/{name}/start` must round-trip onto the
/// resulting execution/task, on BOTH the immediate path (this test) and the
/// throttle-deferred path (below) -- previously both were silently hardcoded
/// to `None`/default regardless of what the caller sent.
#[tokio::test]
async fn context_headers_and_priority_round_trip_on_immediate_start() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // Generous burst so this start is always immediate (201).
    let app = build_app(&pool, throttled_info("100/m", 100.0));

    let (status, body) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({
            "workflow_id": "ctx-job",
            "input": { "tenant_id": "acme" },
            "context_headers": { "trace-id": "abc123" },
            "priority": "high",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "immediate start: {body:?}");
    let exec_id = body["execution_id"].as_str().expect("execution_id");

    // context_headers persisted onto the execution row. NOT verified via the
    // GET /workflows/{id} describe response: WorkflowExecution.context_headers
    // is deliberately `#[serde(skip)]`d from every management-API response
    // (raw header values may carry auth tokens/tenant secrets) -- verify the
    // stored column directly instead.
    let mut conn = pool.get().await.expect("conn");
    #[derive(diesel::QueryableByName)]
    struct CtxRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        context_headers: Option<serde_json::Value>,
    }
    let ctx_row: CtxRow =
        diesel::sql_query("SELECT context_headers FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::parse_str(exec_id).unwrap())
            .get_result(&mut conn)
            .await
            .expect("execution row");
    assert_eq!(
        ctx_row
            .context_headers
            .as_ref()
            .and_then(|v| v.get("trace-id"))
            .cloned(),
        Some(json!("abc123")),
        "context_headers must round-trip onto the execution row"
    );

    // priority persisted onto the initial workflow task's queue row.
    #[derive(diesel::QueryableByName)]
    struct P {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        priority: i32,
    }
    let row: P = diesel::sql_query(
        "SELECT priority FROM harvest_task_queue WHERE workflow_exec_id = $1 \
         AND task_type = 'workflow' LIMIT 1",
    )
    .bind::<diesel::sql_types::Uuid, _>(uuid::Uuid::parse_str(exec_id).unwrap())
    .get_result(&mut conn)
    .await
    .expect("task queue row");
    assert_eq!(
        row.priority,
        autumn_harvest::types::Priority::High.as_i32(),
        "priority must round-trip onto the initial workflow task"
    );
}

/// Same round-trip guarantee on the throttle-deferred path: the deferred
/// start's `context_headers`/`priority` must survive the
/// defer -> `DebounceStartOptions` -> scanner-fire round trip, not just the
/// immediate path.
#[tokio::test]
async fn context_headers_and_priority_round_trip_on_deferred_start() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 1 -> the second start for the same tenant defers.
    let app = build_app(&pool, throttled_info("100/m", 1.0));

    let (s1, _) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "seed", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(s1, StatusCode::CREATED);

    let (status, body) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({
            "workflow_id": "ctx-deferred-job",
            "input": { "tenant_id": "acme" },
            "context_headers": { "trace-id": "def456" },
            "priority": "critical",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "deferred start: {body:?}");

    // Drive the scanner: refill the bucket and fire.
    let mut conn = pool.get().await.expect("conn");
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 1.0, last_refilled_at = NOW() \
         WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>("start-throttle:sync_tenant:acme")
    .execute(&mut conn)
    .await
    .expect("refill bucket");
    let fired = autumn_harvest::throttle::fire_due_throttled_starts(
        &mut conn,
        &None,
        &[] as &[autumn_harvest::types::ShardId],
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("fire due");
    assert_eq!(fired, 1, "the deferred start must fire this tick");

    // Find the execution that was created for ctx-deferred-job.
    #[derive(diesel::QueryableByName)]
    struct ExecRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        context_headers: Option<serde_json::Value>,
    }
    let exec: ExecRow = diesel::sql_query(
        "SELECT id, context_headers FROM harvest_workflow_executions \
         WHERE workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>("ctx-deferred-job")
    .get_result(&mut conn)
    .await
    .expect("execution row");
    assert_eq!(
        exec.context_headers
            .as_ref()
            .and_then(|v| v.get("trace-id"))
            .cloned(),
        Some(json!("def456")),
        "context_headers must survive the defer -> fire round trip"
    );

    #[derive(diesel::QueryableByName)]
    struct P {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        priority: i32,
    }
    let row: P = diesel::sql_query(
        "SELECT priority FROM harvest_task_queue WHERE workflow_exec_id = $1 \
         AND task_type = 'workflow' LIMIT 1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.id)
    .get_result(&mut conn)
    .await
    .expect("task queue row");
    assert_eq!(
        row.priority,
        autumn_harvest::types::Priority::Critical.as_i32(),
        "priority must survive the defer -> fire round trip"
    );
}

/// Code-review fix (issue #607, item 9): every batch-start result -- `started`,
/// `rejected`, and `deferred` alike -- must include the `workflow_id` the item
/// resolved to (caller-supplied, or server-generated when omitted), so a
/// caller can poll/cancel/correlate the eventual run even for a deferred item
/// that has no `execution_id` yet.
#[tokio::test]
async fn batch_start_results_include_workflow_id_for_every_outcome() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 1 -> the second item for the same tenant defers.
    let app = build_app(&pool, throttled_info("100/m", 1.0));

    let (status, body) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "batch-started-job",
                    "input": { "tenant_id": "acme" },
                },
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "batch-deferred-job",
                    "input": { "tenant_id": "acme" },
                },
                {
                    "workflow_name": "unregistered_workflow",
                    "input": { "tenant_id": "acme" },
                },
            ],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "best-effort batch: {body:?}");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 3);

    let started = results
        .iter()
        .find(|r| r["status"] == json!("started"))
        .expect("one started result");
    assert_eq!(
        started["workflow_id"],
        json!("batch-started-job"),
        "started result: {started:?}"
    );

    let deferred = results
        .iter()
        .find(|r| r["status"] == json!("deferred"))
        .expect("one deferred result");
    assert_eq!(
        deferred["workflow_id"],
        json!("batch-deferred-job"),
        "deferred result must include workflow_id so the caller can correlate \
         the eventual run: {deferred:?}"
    );

    // The pre-validation rejection (unregistered workflow name) never resolves
    // a workflow_id at all (no explicit id was given, and none is generated
    // for an item rejected before Phase 1) -- workflow_id is correctly absent.
    let rejected = results
        .iter()
        .find(|r| r["status"] == json!("rejected"))
        .expect("one rejected result");
    assert!(
        rejected.get("workflow_id").is_none(),
        "a pre-validation rejection for an omitted id has no resolved workflow_id: {rejected:?}"
    );
}
