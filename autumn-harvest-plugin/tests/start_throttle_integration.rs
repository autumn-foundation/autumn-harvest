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
use diesel::sql_types::{Jsonb, Text, Uuid as SqlUuid};
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
    // issue #607 round 4 backfill tests: harvest_schedules_kind_check, needed
    // before inserting a workflow-kind schedule row.
    include_str!("../../autumn-harvest/migrations/20260514010000_unified_dag_schedule_kind/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    // issue #607 round 4 backfill tests: harvest_schedules.jitter_secs / overlap_policy
    // / buffered_runs / buffer_all_max, needed to insert a workflow-kind schedule row.
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
    // issue #607 round 4 backfill tests: harvest_schedules.consecutive_failure_limit
    // / consecutive_failure_count / auto_paused_at, needed to insert a workflow-kind
    // schedule row.
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
    include_str!("../../autumn-harvest/migrations/20260706000001_harvest_start_throttle/up.sql"),
    "\n",
    // issue #606: harvest_task_queue.session_id (worker sessions), merged in from trunk-dev.
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #607 code review: companion index for the per-key-fair scanner query.
    include_str!(
        "../../autumn-harvest/migrations/20260707000000_harvest_start_throttle_bucket_deferred_idx/up.sql"
    ),
    "\n",
    // issue #607 code review: index for the (workflow_name, workflow_id)
    // idempotent-retry lookup.
    include_str!(
        "../../autumn-harvest/migrations/20260708000000_harvest_start_throttle_workflow_id_idx/up.sql"
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

/// Like `build_app`, but also returns the `HarvestApiState` handle so a test
/// can arm an admission gate (issue #377) after the app is built -- needed
/// to reproduce the gate-vs-pending-throttle-retry interaction (code review,
/// issue #607). `HarvestApiState` is `Clone` over shared `Arc` internals
/// (including the gate cache), so mutating this handle affects the router
/// built from its clone.
fn build_app_with_state(pool: &DbPool, info: WorkflowInfo) -> (HarvestApiApp, HarvestApiState) {
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

    let app =
        harvest_api_router(api_state.clone()).with_state(AppState::for_test().with_profile("test"));
    (app, api_state)
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

/// A raw (non-pooled) connection for direct schedule-row insertion and
/// assertion queries that the HTTP surface has no endpoint for.
async fn raw_connect(url: &str) -> diesel_async::AsyncPgConnection {
    diesel_async::AsyncPgConnection::establish(url)
        .await
        .expect("raw connection should establish")
}

/// Insert a due, workflow-kind (non-DAG), `interval`-scheduled row so a
/// backfill request has real slots to plan. `workflow_input` is fixed for
/// the whole schedule, so every planned slot resolves to the *same*
/// throttle key -- deliberately, so a single schedule can exercise pacing
/// across a burst of backfilled slots.
async fn insert_workflow_schedule(
    conn: &mut diesel_async::AsyncPgConnection,
    wf_name: &str,
    input: Value,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_schedules \
         (id, workflow_name, schedule_expr, timezone, catchup, max_active_runs, \
          is_paused, jitter_secs, overlap_policy, buffered_runs, buffer_all_max, \
          skip_policy, workflow_input) \
         VALUES ($1, $2, 'interval:3600', 'UTC', false, 10, false, 0, 'skip', \
          '[]'::jsonb, 0, 'skip', $3)",
    )
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(wf_name)
    .bind::<Jsonb, _>(input)
    .execute(conn)
    .await
    .expect("insert workflow schedule");
    id
}

async fn execution_count(conn: &mut diesel_async::AsyncPgConnection, wf_name: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<Text, _>(wf_name)
    .get_result::<Count>(conn)
    .await
    .expect("count executions")
    .count
}

async fn throttle_row_count(conn: &mut diesel_async::AsyncPgConnection, wf_name: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_start_throttle WHERE workflow_name = $1",
    )
    .bind::<Text, _>(wf_name)
    .get_result::<Count>(conn)
    .await
    .expect("count throttle rows")
    .count
}

async fn runs_started_for_schedule(
    conn: &mut diesel_async::AsyncPgConnection,
    schedule_id: uuid::Uuid,
) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct RunsStarted {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        runs_started: i32,
    }
    diesel::sql_query("SELECT runs_started FROM harvest_schedules WHERE id = $1")
        .bind::<SqlUuid, _>(schedule_id)
        .get_result::<RunsStarted>(conn)
        .await
        .expect("read runs_started")
        .runs_started
}

/// Like `insert_workflow_schedule`, but with a caller-specified
/// `max_active_runs` (issue #607 round 5: `schedule_backfill`'s own
/// `max_active_runs` overlap gate must count pending throttled slots left
/// over from an earlier, separate backfill call, not just RUNNING/PAUSED
/// executions).
async fn insert_workflow_schedule_with_max_active(
    conn: &mut diesel_async::AsyncPgConnection,
    wf_name: &str,
    input: Value,
    max_active_runs: i32,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_schedules \
         (id, workflow_name, schedule_expr, timezone, catchup, max_active_runs, \
          is_paused, jitter_secs, overlap_policy, buffered_runs, buffer_all_max, \
          skip_policy, workflow_input) \
         VALUES ($1, $2, 'interval:3600', 'UTC', false, $4, false, 0, 'skip', \
          '[]'::jsonb, 0, 'skip', $3)",
    )
    .bind::<SqlUuid, _>(id)
    .bind::<Text, _>(wf_name)
    .bind::<Jsonb, _>(input)
    .bind::<diesel::sql_types::Integer, _>(max_active_runs)
    .execute(conn)
    .await
    .expect("insert workflow schedule");
    id
}

/// Directly seed a `harvest_rate_limit_buckets` row with zero tokens and zero
/// refill, so the very first admission attempt against `(wf_name,
/// resolved_key)` finds the bucket already empty and defers immediately --
/// without first needing to consume (and thereby start a real execution for)
/// a token, which would confound a test that wants to isolate "a pending
/// throttle row with no corresponding execution" from "a genuinely-started
/// execution".
async fn seed_empty_bucket(
    conn: &mut diesel_async::AsyncPgConnection,
    wf_name: &str,
    resolved_key: &str,
) {
    let key = autumn_harvest::throttle::bucket_key(wf_name, resolved_key);
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets \
         (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.0, 1.0, 0.0, NOW())",
    )
    .bind::<Text, _>(key)
    .execute(conn)
    .await
    .expect("seed empty bucket");
}

/// Issue #607 round 4: `schedule_backfill`'s workflow branch previously
/// admitted every backfilled slot immediately, bypassing throttle pacing
/// entirely -- an operator backfilling hundreds of slots for a throttled
/// workflow got them all admitted at once. Verifies the fix: burst = 2
/// against 5 planned hourly slots admits exactly 2 executions immediately
/// and durably defers the remaining 3 as pending throttle rows (each
/// counted in `skipped_reasons["throttled"]`, per the scheduler-tick
/// precedent where a deferred fire still counts as "dispatched" -- the
/// slot was consumed, just not admitted yet).
#[tokio::test]
async fn schedule_backfill_paces_admissions_and_defers_the_excess() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 2.0));
    let mut conn = raw_connect(&url).await;

    let schedule_id =
        insert_workflow_schedule(&mut conn, "sync_tenant", json!({ "tenant_id": "acme" })).await;

    let now = chrono::Utc::now();
    let from = now - chrono::Duration::hours(4);
    let (status, body) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        json!({ "from": from, "to": now }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "backfill request: {body:?}");
    assert_eq!(
        body["total"],
        json!(5),
        "5 hourly slots from `from` to `to` inclusive: {body:?}"
    );
    assert_eq!(
        body["dispatched"],
        json!(5),
        "every slot is consumed (started immediately or durably deferred): {body:?}"
    );
    assert_eq!(body["failed"], json!(0), "{body:?}");
    assert_eq!(body["skipped"], json!(0), "{body:?}");

    let real_executions = execution_count(&mut conn, "sync_tenant").await;
    assert_eq!(
        real_executions, 2,
        "only burst=2 slots should have started immediately, the rest deferred"
    );
    let pending_rows = throttle_row_count(&mut conn, "sync_tenant").await;
    assert_eq!(
        pending_rows, 3,
        "the remaining 3 slots must be durable pending throttle rows, not admitted"
    );
}

/// Issue #607 round 4: an oversized backfilled slot must be skipped (not
/// durably deferred into a pending row that could never successfully fire)
/// -- mirroring the fix already applied to the single-start, batch, and
/// scheduler-tick call sites.
#[tokio::test]
async fn schedule_backfill_skips_oversized_input_before_deferring() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 1 drains on the first (only) slot, so the oversized input
    // hits an empty bucket and -- absent the fix -- would be durably
    // deferred rather than skipped.
    let app = build_app(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    let oversized_input = json!({
        "tenant_id": "acme",
        "payload": "x".repeat(3 * 1024 * 1024),
    });
    let schedule_id = insert_workflow_schedule(&mut conn, "sync_tenant", oversized_input).await;

    let now = chrono::Utc::now();
    let (status, body) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        json!({ "from": now, "to": now }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "backfill request: {body:?}");
    assert_eq!(body["total"], json!(1), "{body:?}");
    assert_eq!(
        body["dispatched"],
        json!(0),
        "the oversized slot must not be counted as dispatched: {body:?}"
    );
    assert_eq!(
        body["skipped"],
        json!(1),
        "the oversized slot must be skipped, not deferred: {body:?}"
    );
    let reasons = body["skipped_reasons"]
        .as_object()
        .expect("skipped_reasons object");
    assert_eq!(
        reasons.get("oversized_input"),
        Some(&json!(1)),
        "skip reason must name the byte-cap violation: {body:?}"
    );

    let real_executions = execution_count(&mut conn, "sync_tenant").await;
    assert_eq!(real_executions, 0, "no execution should have started");
    let pending_rows = throttle_row_count(&mut conn, "sync_tenant").await;
    assert_eq!(
        pending_rows, 0,
        "no pending throttle row should exist for the oversized slot"
    );
}

/// Code-review fix (issue #607 round 5): a throttled scheduled fire durably
/// defers before any `harvest_workflow_executions` row exists, so a *second,
/// separate* `schedule_backfill` request recomputing `running_at_start` from
/// `query_running_count` alone would not see an earlier request's still-
/// pending deferred slot and could dispatch (and again defer) past
/// `max_active_runs` -- the throttle scanner would later fire both pending
/// rows without ever re-checking the schedule's overlap gate. Verifies the
/// fix: with `max_active_runs = 1` and an already-empty bucket, a first
/// backfill call defers its one slot into a pending throttle row (no
/// execution), and a *second* backfill call for a different slot is skipped
/// with `skipped_reasons["max_active_runs"]` instead of writing a second
/// pending row.
#[tokio::test]
async fn schedule_backfill_counts_pending_throttled_slots_against_max_active_runs() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    // Pre-empty the bucket so the very first planned slot defers immediately,
    // rather than consuming the sole token and becoming a real (RUNNING)
    // execution -- isolating "a pending row with no execution" from
    // `query_running_count`'s own, already-correct RUNNING/PAUSED count.
    seed_empty_bucket(&mut conn, "sync_tenant", "acme").await;

    let schedule_id = insert_workflow_schedule_with_max_active(
        &mut conn,
        "sync_tenant",
        json!({ "tenant_id": "acme" }),
        1,
    )
    .await;

    let now = chrono::Utc::now();
    let (status1, body1) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        json!({ "from": now, "to": now }),
    )
    .await;
    assert_eq!(status1, StatusCode::OK, "first backfill call: {body1:?}");
    assert_eq!(body1["total"], json!(1), "{body1:?}");
    assert_eq!(
        body1["dispatched"],
        json!(1),
        "the slot is consumed (durably deferred): {body1:?}"
    );
    assert_eq!(body1["skipped"], json!(0), "{body1:?}");

    assert_eq!(
        execution_count(&mut conn, "sync_tenant").await,
        0,
        "the sole slot must have been deferred, not started"
    );
    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        1,
        "exactly one pending throttle row after the first call"
    );

    // A second, separate backfill call for a *different* slot (a different
    // hour, so it plans to a distinct workflow_id and isn't rejected as an
    // already-existing run) must see the first call's still-pending deferred
    // slot counted against max_active_runs=1, and skip rather than defer a
    // second row.
    let next_hour = now + chrono::Duration::hours(1);
    let (status2, body2) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        json!({ "from": next_hour, "to": next_hour }),
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "second backfill call: {body2:?}");
    assert_eq!(body2["total"], json!(1), "{body2:?}");
    assert_eq!(
        body2["dispatched"],
        json!(0),
        "the second slot must NOT be dispatched: {body2:?}"
    );
    assert_eq!(
        body2["skipped"],
        json!(1),
        "the second slot must be skipped by the max_active_runs gate: {body2:?}"
    );
    let reasons2 = body2["skipped_reasons"]
        .as_object()
        .expect("skipped_reasons object");
    assert_eq!(
        reasons2.get("max_active_runs"),
        Some(&json!(1)),
        "skip reason must name max_active_runs, not silently defer again: {body2:?}"
    );

    assert_eq!(
        execution_count(&mut conn, "sync_tenant").await,
        0,
        "still no execution should have started"
    );
    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        1,
        "no second pending throttle row should have been created for the second call"
    );
}

/// Code-review fix (issue #607 round 7): `schedule_backfill`'s throttle
/// branch treated every `Deferred` outcome as a fresh dispatch, even when
/// `reserve_or_defer` actually resolved to an *already-existing* pending row
/// for the same `workflow_id` (its own idempotency shortcut) -- e.g. an
/// operator repeating the exact same backfill window while the first call's
/// throttled slot is still durably pending. That double-counted the slot in
/// `dispatched`/`dispatched_this_call`, which then double-spent the
/// schedule's `max_runs` budget for a call that created nothing new.
/// Verifies a repeated identical backfill window reports the already-
/// pending slot as skipped (`already_exists`), not dispatched, and does not
/// advance `runs_started` a second time.
#[tokio::test]
async fn repeated_backfill_call_does_not_double_count_an_already_pending_throttle_row() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    // Pre-empty the bucket so the sole planned slot defers immediately on
    // the first call.
    seed_empty_bucket(&mut conn, "sync_tenant", "acme").await;
    let schedule_id =
        insert_workflow_schedule(&mut conn, "sync_tenant", json!({ "tenant_id": "acme" })).await;

    let now = chrono::Utc::now();
    let window = json!({ "from": now, "to": now });

    let (status1, body1) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        window.clone(),
    )
    .await;
    assert_eq!(status1, StatusCode::OK, "first backfill call: {body1:?}");
    assert_eq!(body1["dispatched"], json!(1), "{body1:?}");
    assert_eq!(body1["skipped"], json!(0), "{body1:?}");
    assert_eq!(throttle_row_count(&mut conn, "sync_tenant").await, 1);
    let runs_started_after_first = runs_started_for_schedule(&mut conn, schedule_id).await;
    assert_eq!(
        runs_started_after_first, 1,
        "the first, genuinely-fresh deferral must spend one run-budget slot"
    );

    // Repeat the exact same window: the single planned slot resolves to the
    // exact same deterministic workflow_id, which already has a pending
    // throttle row from the first call.
    let (status2, body2) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/backfill"),
        window,
    )
    .await;
    assert_eq!(
        status2,
        StatusCode::OK,
        "second (repeated) backfill call: {body2:?}"
    );
    assert_eq!(
        body2["dispatched"],
        json!(0),
        "the already-pending slot must not be reported as freshly dispatched: {body2:?}"
    );
    assert_eq!(
        body2["skipped"],
        json!(1),
        "the already-pending slot must be reported as skipped: {body2:?}"
    );
    let reasons2 = body2["skipped_reasons"]
        .as_object()
        .expect("skipped_reasons object");
    assert_eq!(
        reasons2.get("already_exists"),
        Some(&json!(1)),
        "skip reason must name already_exists, not silently double-count: {body2:?}"
    );

    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        1,
        "no second pending throttle row should have been created"
    );
    assert_eq!(
        runs_started_for_schedule(&mut conn, schedule_id).await,
        runs_started_after_first,
        "the repeated call must not spend a second run-budget slot for the \
         same already-pending row"
    );
}

/// Code-review fix (issue #607 round 6): `POST /admin/schedules/{id}/trigger`
/// (manual trigger-now) previously called the start primitive directly with
/// no throttle check at all -- unlike scheduled and backfilled fires, which
/// both already pace through `reserve_or_defer`. An operator repeatedly
/// hitting this route for a throttled workflow schedule could bypass its
/// declared rate/burst entirely. Verifies a manual trigger against an
/// already-empty bucket is durably deferred (`outcome: "deferred"`, no
/// `execution_id`) instead of admitted immediately.
#[tokio::test]
async fn manual_trigger_defers_when_throttle_bucket_is_empty() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    seed_empty_bucket(&mut conn, "sync_tenant", "acme").await;
    let schedule_id =
        insert_workflow_schedule(&mut conn, "sync_tenant", json!({ "tenant_id": "acme" })).await;

    let (status, body) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/trigger"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "trigger request: {body:?}");
    assert_eq!(body["outcome"], json!("deferred"), "{body:?}");
    assert_eq!(
        body["execution_id"],
        Value::Null,
        "a deferred trigger must not carry an execution id yet: {body:?}"
    );

    assert_eq!(
        execution_count(&mut conn, "sync_tenant").await,
        0,
        "the triggered run must have been deferred, not started"
    );
    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        1,
        "exactly one pending throttle row should exist for the deferred trigger"
    );
}

/// A manual trigger with an available token still starts immediately
/// (`outcome: "fired"`), unaffected by the throttle-deferral fix above.
#[tokio::test]
async fn manual_trigger_starts_immediately_when_token_available() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    let schedule_id =
        insert_workflow_schedule(&mut conn, "sync_tenant", json!({ "tenant_id": "acme" })).await;

    let (status, body) = post_json(
        &app,
        &format!("/admin/schedules/{schedule_id}/trigger"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "trigger request: {body:?}");
    assert_eq!(body["outcome"], json!("fired"), "{body:?}");
    assert!(
        body["execution_id"].is_string(),
        "a fired trigger must carry a real execution id: {body:?}"
    );

    assert_eq!(
        execution_count(&mut conn, "sync_tenant").await,
        1,
        "the sole token should have admitted the trigger immediately"
    );
    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        0,
        "no pending throttle row should exist when a token was available"
    );
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

/// Code-review fix (issue #607 round 7): the admission gate's (#377)
/// idempotent-retry bypass only checked for an existing non-terminal
/// *execution*, not an existing *pending throttle row* -- so a retry for an
/// explicit `workflow_id` that is already durably deferred in
/// `harvest_start_throttle` was wrongly blocked with a `503` once a gate
/// activated, even though `reserve_or_defer`'s own idempotency shortcut
/// resolves the retry to the exact same pending row without creating any
/// new admission. Verifies the gate bypass now also recognizes a pending
/// throttle row, while a genuinely fresh `workflow_id` is still blocked.
#[tokio::test]
async fn retry_with_a_pending_throttle_row_bypasses_the_admission_gate() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let (app, api_state) = build_app_with_state(&pool, throttled_info("100/m", 1.0));
    let mut conn = raw_connect(&url).await;

    // First request: bucket empty, defers durably.
    seed_empty_bucket(&mut conn, "sync_tenant", "acme").await;
    let (status1, body1) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "gate-retry-job", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(status1, StatusCode::ACCEPTED, "first request: {body1:?}");
    assert_eq!(body1["throttled"], json!(true), "{body1:?}");
    assert_eq!(throttle_row_count(&mut conn, "sync_tenant").await, 1);

    // Arm a fleet-wide admission gate blocking all new admissions.
    api_state.initialize_gate_cache(vec![autumn_harvest::AdmissionGate {
        id: autumn_harvest::AdmissionGateId(uuid::Uuid::new_v4()),
        scope: autumn_harvest::GateScope::Fleet,
        reason: "incident".to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: chrono::Utc::now(),
        expires_at: None,
    }]);

    // Retry with the exact same explicit workflow_id: must NOT be blocked by
    // the gate (it resolves to the same already-pending row, not a fresh
    // admission), and must NOT create a second pending row.
    let (status2, body2) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "gate-retry-job", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(
        status2,
        StatusCode::ACCEPTED,
        "retry must bypass the gate via the pending-throttle-row shortcut: {body2:?}"
    );
    assert_eq!(body2["throttled"], json!(true), "{body2:?}");
    assert_eq!(
        throttle_row_count(&mut conn, "sync_tenant").await,
        1,
        "no second pending row should be created"
    );

    // Sanity: a genuinely NEW workflow_id is still blocked by the same gate.
    let (status3, body3) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "gate-retry-job-fresh", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(
        status3,
        StatusCode::SERVICE_UNAVAILABLE,
        "a genuinely fresh start must still be blocked by the gate: {body3:?}"
    );
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

/// Code-review fix (issue #607 round 5): an atomic batch is an all-or-nothing
/// synchronous insert with no way to represent a `Deferred` outcome. Before
/// this fix, `atomic=true` items skipped the throttle admission check
/// entirely, letting a client bypass a workflow's rate policy simply by
/// submitting an atomic batch instead of a plain start. Verifies an atomic
/// item for a throttled workflow is rejected outright, in pre-validation --
/// before any token is ever reserved or pending row written.
#[tokio::test]
async fn atomic_batch_rejects_item_for_a_throttled_workflow() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 5.0));

    let (status, body) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": true,
            "items": [
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "atomic-job-1",
                    "input": { "tenant_id": "acme" },
                }
            ],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT, "atomic batch: {body:?}");
    let rejected = body["rejected"].as_array().expect("rejected array");
    assert_eq!(rejected.len(), 1, "{body:?}");
    let err = rejected[0]["error"].as_str().expect("error message");
    assert!(
        err.contains("start-throttle policy"),
        "rejection reason must name the throttle policy, not a generic error: {err}"
    );

    // Confirm the bypass is actually closed, not just re-labeled: no token
    // was ever reserved and no execution or pending row exists for this item.
    let mut conn = raw_connect(&url).await;
    assert_eq!(
        execution_count(&mut conn, "sync_tenant").await,
        0,
        "an atomic-rejected item must never start an execution"
    );
    let (backlog_status, backlog) = get_json(&app, "/admin/start-throttle").await;
    assert_eq!(backlog_status, StatusCode::OK);
    assert_eq!(
        backlog.as_array().map(Vec::len),
        Some(0),
        "no pending throttle row should be created for a pre-validation-rejected \
         atomic item: {backlog:?}"
    );
}

/// A non-atomic (best-effort) batch is unaffected by the atomic-rejection fix
/// above: a throttled item still goes through the normal admission path and
/// is durably deferred when the bucket is empty, exactly like a standalone
/// start.
#[tokio::test]
async fn non_atomic_batch_still_throttles_normally() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, throttled_info("100/m", 1.0));

    let (status1, body1) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "non-atomic-job-1",
                    "input": { "tenant_id": "acme" },
                }
            ],
        }),
    )
    .await;
    assert_eq!(status1, StatusCode::OK, "{body1:?}");
    assert_eq!(
        body1["results"][0]["status"],
        json!("started"),
        "first item consumes the sole token: {body1:?}"
    );

    let (status2, body2) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "non-atomic-job-2",
                    "input": { "tenant_id": "acme" },
                }
            ],
        }),
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "{body2:?}");
    assert_eq!(
        body2["results"][0]["status"],
        json!("deferred"),
        "second item for the same key finds the bucket empty: {body2:?}"
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

/// Code-review fix (issue #607): a best-effort batch item for a throttled
/// workflow whose input exceeds the effective byte cap must be rejected
/// immediately, not durably deferred. Without the fix, an oversized item
/// hitting an *empty* bucket would be written to `harvest_start_throttle`
/// as `Deferred` -- a row that can never successfully fire (the throttle
/// scanner's own cap enforcement rejects it every tick, forever) -- rather
/// than failing fast with a clear per-item error and zero pending rows.
#[tokio::test]
async fn batch_start_rejects_oversized_item_before_deferring() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // burst = 1 -> the bucket is drained by the first (normal-sized) item,
    // so the second (oversized) item would hit an empty bucket and -- absent
    // the fix -- be durably deferred instead of rejected.
    let app = build_app(&pool, throttled_info("100/m", 1.0));

    let (drain_status, _) = post_json(
        &app,
        "/workflows/sync_tenant/start",
        json!({ "workflow_id": "drain-job", "input": { "tenant_id": "acme" } }),
    )
    .await;
    assert_eq!(
        drain_status,
        StatusCode::CREATED,
        "drain call consumes the sole token"
    );

    let oversized_input = "x".repeat(3 * 1024 * 1024);
    let (status, body) = post_json(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                {
                    "workflow_name": "sync_tenant",
                    "workflow_id": "oversized-batch-job",
                    "input": { "tenant_id": "acme", "payload": oversized_input },
                }
            ],
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "best-effort batch: {body:?}");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["status"],
        json!("rejected"),
        "an oversized item must be rejected immediately, not deferred: {body:?}"
    );
    assert_eq!(results[0]["workflow_id"], json!("oversized-batch-job"));
    let err = results[0]["error"].as_str().expect("error message");
    assert!(
        err.contains("exceeds cap"),
        "rejection reason must name the byte-cap violation: {err}"
    );

    // No pending throttle row was ever written for the oversized item.
    let (backlog_status, backlog) = get_json(&app, "/admin/start-throttle").await;
    assert_eq!(backlog_status, StatusCode::OK);
    let arr = backlog.as_array().expect("array");
    let acme_backlog = arr
        .iter()
        .find(|e| e["throttle_key"] == json!("acme"))
        .map_or(0, |e| e["deferred_count"].as_i64().unwrap_or(0));
    assert_eq!(
        acme_backlog, 0,
        "no pending row should exist for the oversized item: {backlog:?}"
    );
}
