//! HTTP integration tests for `PATCH /admin/schedules/{id}` — issue #771.
//!
//! In-place partial schedule update: identity (`schedule_id`) preservation,
//! #488 carryover survival across an edit, validate-before-commit (400 leaves
//! the row byte-for-byte unchanged), workflow-type immutability, DAG-row
//! rejection, the #350 fire-claim 409, audit rows, and immediate list
//! visibility.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor, tick_once};
use autumn_harvest::schema::{
    harvest_audit_log, harvest_events, harvest_schedules, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetentionConfig, WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
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
    include_str!(
        "../../autumn-harvest/migrations/20260504000000_harvest_workflow_parent_children/up.sql"
    ),
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
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
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
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260710000002_harvest_workflow_continue_chain/up.sql"
    ),
    // 20260706000001_harvest_start_throttle is deliberately omitted: the tick's
    // dispatch path probes `to_regclass('harvest_start_throttle')` and treats a
    // missing table as "no pending throttled starts" (see
    // `throttle::pending_throttle_count_for_workflow`), so scheduler-driven
    // tests run correctly without it.
);

type HarvestApiApp = axum::Router;

// ── Harness ───────────────────────────────────────────────────────────────────

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

/// Incremental ETL workflow used by the carryover money test: reads the
/// previous run's cursor via `last_completion_result` (issue #488) and
/// increments it.
fn incremental_etl_handler<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async move {
        let prev: Option<Value> = ctx
            .last_completion_result::<Value>()
            .map_err(|e| e.to_string())?;
        let prev_value = prev
            .as_ref()
            .and_then(|v| v.get("value"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        Ok(json!({"value": prev_value + 1}))
    })
}

fn etl_registry(wf_name: &'static str) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name: wf_name,
            module: "schedule_update_integration",
            handler: incremental_etl_handler,
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
        }],
        vec![],
    ))
}

fn build_app(pool: &DbPool, registry: Arc<HandlerRegistry>) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("scheduler-only".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_api_router(api_state).with_state(
        AppState::for_test()
            .with_pool(pool.clone())
            .with_profile("test"),
    )
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn request_json(
    app: &HarvestApiApp,
    method: &str,
    uri: impl Into<String>,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let mut builder = Request::builder().method(method).uri(&uri);
    if payload.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = payload.map_or_else(Body::empty, |payload| Body::from(payload.to_string()));
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .expect("request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    request_json(app, "GET", uri, None).await
}

async fn post_json(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
) -> (StatusCode, Value) {
    request_json(app, "POST", uri, Some(payload)).await
}

async fn patch_json(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
) -> (StatusCode, Value) {
    request_json(app, "PATCH", uri, Some(payload)).await
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Create a workflow schedule over HTTP and return its id.
async fn create_schedule(app: &HarvestApiApp, wf_name: &str, body_extra: Value) -> Uuid {
    let mut body = json!({
        "workflow_name": wf_name,
        "schedule_expr": "interval:60",
    });
    if let (Some(base), Some(extra)) = (body.as_object_mut(), body_extra.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }
    let (status, created) = post_json(app, "/admin/schedules/workflow", body).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create must succeed: {created}"
    );
    created["id"]
        .as_str()
        .expect("created schedule must have an id")
        .parse()
        .expect("id must be a UUID")
}

async fn load_schedule_row(url: &str, id: Uuid) -> HarvestSchedule {
    let mut conn = connect(url).await;
    harvest_schedules::table
        .find(id)
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .expect("load schedule row")
}

/// Arm the schedule's `next_run_at` `secs_ago` seconds in the past so the next
/// tick fires that slot (strictly decreasing values across calls, see #488).
async fn arm_slot(url: &str, id: Uuid, secs_ago: i64) {
    use harvest_schedules::dsl;
    let mut conn = connect(url).await;
    diesel::update(harvest_schedules::table.find(id))
        .set(dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(secs_ago)))
        .execute(&mut conn)
        .await
        .expect("arm slot");
}

fn make_worker(registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "schedule-update-test-worker".to_string();
    runtime_config.poll_interval = Duration::from_millis(25);
    Arc::new(Worker::new(runtime_config, registry).expect("worker config should be valid"))
}

async fn wait_for_completed(url: &str, wf_name: &str, min_count: i64) -> Vec<Uuid> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let mut conn = connect(url).await;
            let rows: Vec<Uuid> = harvest_workflow_executions::table
                .filter(harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
                .filter(harvest_workflow_executions::dsl::state.eq("COMPLETED"))
                .select(harvest_workflow_executions::dsl::id)
                .load(&mut conn)
                .await
                .unwrap_or_default();
            if i64::try_from(rows.len()).unwrap_or(i64::MAX) >= min_count {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for completed executions")
}

async fn tick(pool: &DbPool, registry: &Arc<HandlerRegistry>) {
    tick_once(
        pool.clone(),
        registry.clone(),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// The AC11 money test: create → run once → PATCH cron+input → the id is
/// unchanged, #488 carryover from the pre-edit run resolves on the next fire,
/// and the next fire uses the new cadence + new input.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn patch_preserves_identity_and_carryover_across_the_edit() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let wf_name = "sched_upd_etl_wf";
    let registry = etl_registry(wf_name);
    let app = build_app(&pool, registry.clone());

    let id = create_schedule(&app, wf_name, json!({"input": {"env": "A"}})).await;

    // ── Run 1 under the pre-edit spec ────────────────────────────────────────
    arm_slot(&url, id, 300).await;
    tick(&pool, &registry).await;

    let worker = make_worker(registry.clone());
    let pool_clone = pool.clone();
    let worker_clone = worker.clone();
    let handle = tokio::spawn(async move { worker_clone.run(&pool_clone).await });

    let run1_ids = wait_for_completed(&url, wf_name, 1).await;
    assert_eq!(run1_ids.len(), 1);

    // AC9 baseline: a PATCH must write nothing to harvest_events.
    let mut conn = connect(&url).await;
    let events_before_patch: i64 = harvest_events::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count events before patch");

    // ── PATCH cron + input ───────────────────────────────────────────────────
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"schedule_expr": "interval:120", "input": {"env": "B"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "patch must succeed: {patched}");
    assert_eq!(
        patched["id"].as_str(),
        Some(id.to_string().as_str()),
        "schedule_id must never change (AC11a)"
    );
    assert_eq!(patched["schedule_expr"], "interval:120");

    // AC9: zero harvest_events writes across the PATCH.
    let events_after_patch: i64 = harvest_events::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count events after patch");
    assert_eq!(
        events_after_patch, events_before_patch,
        "a schedule PATCH must never write to harvest_events (AC9)"
    );

    // GET /admin/schedules reflects the update immediately (AC10): not just
    // the expression — the recomputed next_run_at and the new input too.
    let (list_status, list) = get_json(&app, "/admin/schedules").await;
    assert_eq!(list_status, StatusCode::OK);
    let entry = list
        .as_array()
        .expect("list must be an array")
        .iter()
        .find(|e| e["id"].as_str() == Some(id.to_string().as_str()))
        .cloned()
        .expect("edited schedule must be listed");
    assert_eq!(entry["schedule_expr"], "interval:120");
    assert!(
        !entry["next_run_at"].is_null(),
        "the list must reflect a recomputed next_run_at (AC10): {entry}"
    );
    assert_eq!(
        entry["next_run_at"], patched["next_run_at"],
        "the list must reflect the same recomputed next_run_at the PATCH returned (AC10)"
    );
    assert_eq!(
        entry["workflow_input"],
        json!({"env": "B"}),
        "the list must reflect the edited input (AC10)"
    );

    // ── Run 2 under the post-edit spec ───────────────────────────────────────
    let t_before_tick2 = Utc::now();
    arm_slot(&url, id, 200).await;
    tick(&pool, &registry).await;
    let completed_ids = wait_for_completed(&url, wf_name, 2).await;
    let run2_id = *completed_ids.iter().find(|r| **r != run1_ids[0]).unwrap();

    let (run2_input, run2_output, run2_workflow_id): (Value, Option<Value>, String) =
        harvest_workflow_executions::table
            .find(run2_id)
            .select((
                harvest_workflow_executions::dsl::input,
                harvest_workflow_executions::dsl::output,
                harvest_workflow_executions::dsl::workflow_id,
            ))
            .first(&mut conn)
            .await
            .expect("load run 2");
    assert_eq!(
        run2_input,
        json!({"env": "B"}),
        "the post-edit fire must use the new input (AC11c)"
    );
    assert_eq!(
        run2_output,
        Some(json!({"value": 2})),
        "#488 carryover from the pre-edit run must resolve on the next fire (AC11b)"
    );
    assert!(
        run2_workflow_id.starts_with(&format!("sched:{id}:")),
        "the post-edit run must stay in the sched:{{schedule_id}}: workflow-id \
         namespace (AC6), got {run2_workflow_id}"
    );

    // AC11c strengthening: the advanced next_run_at must reflect the NEW
    // 120 s interval. Non-catchup dispatch advances to
    // next_run_after(schedule, tick_now) = tick_now + 120 with
    // tick_now >= t_before_tick2; under the OLD 60 s interval it would be
    // tick_now + 60 < t_before_tick2 + 120 (the tick starts immediately).
    let row_after_run2 = load_schedule_row(&url, id).await;
    let advanced = row_after_run2
        .next_run_at
        .expect("next_run_at must be advanced after the fire");
    assert!(
        advanced >= t_before_tick2 + chrono::Duration::seconds(120),
        "the advanced next_run_at must be driven by the NEW 120s interval \
         (got {advanced}, tick started {t_before_tick2})"
    );
    assert!(
        advanced <= Utc::now() + chrono::Duration::seconds(121),
        "the advanced next_run_at must be ~tick_now + 120s, got {advanced}"
    );

    worker.shutdown();
    let _ = handle.await;
}

/// An invalid-cron PATCH returns 400 and leaves the row byte-for-byte
/// unchanged (`AC11d`, validate-before-commit).
#[tokio::test]
async fn invalid_cron_patch_returns_400_and_leaves_row_unchanged() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_badcron_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_badcron_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"schedule_expr": "cron:not a cron", "input": {"should": "not persist"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "invalid cron must 400: {body}"
    );
    assert!(
        body.to_string().contains("schedule_expr"),
        "error must name the invalid field: {body}"
    );

    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap(),
        "a rejected patch must write nothing"
    );
}

/// The workflow type is not editable: a body containing `workflow_name`
/// returns 400.
#[tokio::test]
async fn workflow_name_in_body_is_rejected_with_400() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_immutable_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_immutable_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"workflow_name": "some_other_wf"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("workflow_name"),
        "error must explain workflow_name is not editable: {body}"
    );

    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        after.workflow_name.as_deref(),
        Some("sched_upd_immutable_wf"),
        "workflow_name must be unchanged"
    );
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
}

/// An unknown calendar name is rejected with 400 (mirrors the create path)
/// and writes nothing.
#[tokio::test]
async fn unknown_calendar_is_rejected_with_400() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_calendar_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_calendar_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"calendar": "does-not-exist"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("does-not-exist"),
        "error must name the unknown calendar: {body}"
    );

    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
}

/// `PATCH`ing an unknown schedule id returns 404; a malformed id returns 400.
#[tokio::test]
async fn unknown_or_malformed_id_is_rejected() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_404_wf");
    let app = build_app(&pool, registry);

    let unknown = Uuid::new_v4();
    let (status, _) = patch_json(
        &app,
        format!("/admin/schedules/{unknown}"),
        json!({"jitter_secs": 5}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = patch_json(&app, "/admin/schedules/not-a-uuid", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A DAG schedule row is owned by `PATCH /dags/{dag_name}` — this route
/// rejects it with 400.
#[tokio::test]
async fn dag_schedule_row_is_rejected_with_400() {
    use harvest_schedules::dsl;

    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_dagrow_wf");
    let app = build_app(&pool, registry);

    let dag_row_id = Uuid::new_v4();
    let mut conn = connect(&url).await;
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(dag_row_id),
            dsl::dag_name.eq("some_dag"),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(1),
            dsl::is_paused.eq(false),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(&mut conn)
        .await
        .expect("insert dag schedule row");

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{dag_row_id}"),
        json!({"jitter_secs": 5}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("/dags/"),
        "error must point at the DAG patch route: {body}"
    );
}

/// A live fire claim (issue #350) returns 409 so the edit never races an
/// in-flight fire; once the claim lease expires the edit proceeds.
#[tokio::test]
async fn live_fire_claim_returns_409_until_expired() {
    use harvest_schedules::dsl;

    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_claim_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_claim_wf", json!({})).await;

    let mut conn = connect(&url).await;
    diesel::update(harvest_schedules::table.find(id))
        .set((
            dsl::fire_claim_token.eq(Some(Uuid::new_v4())),
            dsl::fire_claimed_until.eq(Some(Utc::now() + chrono::Duration::seconds(25))),
        ))
        .execute(&mut conn)
        .await
        .expect("seed live claim");

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"jitter_secs": 5}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body.to_string().contains("firing"),
        "409 body must explain the schedule is firing right now: {body}"
    );

    // Expire the claim — the edit proceeds.
    diesel::update(harvest_schedules::table.find(id))
        .set(dsl::fire_claimed_until.eq(Some(Utc::now() - chrono::Duration::seconds(5))))
        .execute(&mut conn)
        .await
        .expect("expire claim");

    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"jitter_secs": 5}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["jitter_secs"], 5);
}

/// A successful PATCH writes a `schedule.update` audit row; a rejected PATCH
/// writes a failed one.
#[tokio::test]
async fn patch_writes_schedule_update_audit_rows() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_audit_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_audit_wf", json!({})).await;

    let (status, _) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"max_active_runs": 3}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut conn = connect(&url).await;
    let rows: Vec<(String, Option<String>)> = harvest_audit_log::table
        .filter(harvest_audit_log::dsl::operation.eq("schedule.update"))
        .select((
            harvest_audit_log::dsl::status,
            harvest_audit_log::dsl::target_id,
        ))
        .load(&mut conn)
        .await
        .expect("load audit rows");
    assert!(
        rows.iter()
            .any(|(s, t)| s == "succeeded" && t.as_deref() == Some(id.to_string().as_str())),
        "a succeeded schedule.update audit row must be recorded: {rows:?}"
    );

    let (status, _) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"schedule_expr": "cron:bogus cron"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let rows: Vec<String> = harvest_audit_log::table
        .filter(harvest_audit_log::dsl::operation.eq("schedule.update"))
        .filter(harvest_audit_log::dsl::status.eq("failed"))
        .select(harvest_audit_log::dsl::status)
        .load(&mut conn)
        .await
        .expect("load failed audit rows");
    assert!(
        !rows.is_empty(),
        "a failed schedule.update audit row must be recorded for the rejected patch"
    );
}

/// Tri-state semantics over HTTP: explicit JSON `null` clears a nullable
/// field; absence leaves it unchanged. An empty body is a valid no-op.
#[tokio::test]
async fn explicit_null_clears_and_absence_preserves_nullable_fields() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_tristate_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(
        &app,
        "sched_upd_tristate_wf",
        json!({"end_at": "2030-01-01T00:00:00Z", "max_runs": 9}),
    )
    .await;

    // Absent fields are unchanged by an unrelated patch.
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"jitter_secs": 3}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["max_runs"], 9);
    assert_eq!(patched["end_at"], "2030-01-01T00:00:00Z");

    // Explicit null clears them.
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"end_at": null, "max_runs": null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert!(
        patched["max_runs"].is_null(),
        "max_runs must be cleared: {patched}"
    );
    assert!(
        patched["end_at"].is_null(),
        "end_at must be cleared: {patched}"
    );

    // An empty body is a valid no-op returning the current entry.
    let (status, patched) = patch_json(&app, format!("/admin/schedules/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["jitter_secs"], 3);
    let _ = load_schedule_row(&url, id).await;
}

/// Write-path coverage for every remaining AC2 field: each PATCH round-trips
/// through the row so a field silently dropped by the merge/changeset can't
/// hide behind response-echo only.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn patch_covers_every_editable_field_write_path() {
    use harvest_schedules::dsl;

    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_fields_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(
        &app,
        "sched_upd_fields_wf",
        json!({"schedule_expr": "0 3 * * *", "overlap_policy": "buffer_all"}),
    )
    .await;

    // buffer_all_max: seed three buffered slots, tighten the cap to 2 and
    // assert the stored buffer is trimmed with it (#241 trim interaction).
    let mut conn = connect(&url).await;
    diesel::update(harvest_schedules::table.find(id))
        .set(dsl::buffered_runs.eq(json!([
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
            "2026-01-01T02:00:00Z"
        ])))
        .execute(&mut conn)
        .await
        .expect("seed buffered runs");
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"buffer_all_max": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["buffer_all_max"], 2);
    assert_eq!(
        patched["buffered_count"], 2,
        "tightening buffer_all_max must trim the stored buffer: {patched}"
    );
    let row = load_schedule_row(&url, id).await;
    assert_eq!(row.buffer_all_max, 2);
    assert_eq!(
        row.buffered_runs.as_array().map(Vec::len),
        Some(2),
        "the stored buffered_runs must be trimmed to the new cap"
    );

    // queue_name.
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"queue_name": "edited-queue"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    let row = load_schedule_row(&url, id).await;
    assert_eq!(row.queue_name.as_deref(), Some("edited-queue"));

    // skip_policy.
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"skip_policy": "run_next_business_day"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["skip_policy"], "run_next_business_day");
    let row = load_schedule_row(&url, id).await;
    assert_eq!(row.skip_policy, "run_next_business_day");

    // Legacy catchup bool (no catchup_policy stored → the bool governs).
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"catchup": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["catchup"], true);
    let row = load_schedule_row(&url, id).await;
    assert!(row.catchup, "legacy catchup bool must be persisted");
    assert!(
        row.catchup_policy.is_none(),
        "a legacy-bool edit must not mint an explicit catchup policy"
    );

    // consecutive_failure_limit.
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"consecutive_failure_limit": 4}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["consecutive_failure_limit"], 4);
    let row = load_schedule_row(&url, id).await;
    assert_eq!(row.consecutive_failure_limit, Some(4));

    // retry_policy: set…
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"retry_policy": {
            "max_attempts": 3,
            "initial_interval": {"secs": 1, "nanos": 0},
            "backoff_coefficient": 2.0,
            "max_interval": {"secs": 60, "nanos": 0},
            "non_retryable_errors": []
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["retry_policy"]["max_attempts"], 3);
    let row = load_schedule_row(&url, id).await;
    assert_eq!(
        row.retry_policy
            .as_ref()
            .and_then(|v| v.get("max_attempts"))
            .and_then(Value::as_i64),
        Some(3),
        "retry_policy must be persisted"
    );

    // …and clear (explicit JSON null).
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"retry_policy": null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert!(patched["retry_policy"].is_null());
    let row = load_schedule_row(&url, id).await;
    assert!(row.retry_policy.is_none(), "retry_policy must be cleared");

    // timezone-only PATCH on a cron schedule: the stored expression becomes
    // cron_tz:… AND next_run_at is recomputed (the cadence changed).
    let before = load_schedule_row(&url, id).await;
    assert_eq!(before.schedule_expr.as_deref(), Some("cron:0 3 * * *"));
    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"timezone": "America/New_York"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(
        patched["schedule_expr"], "cron_tz:America/New_York:0 3 * * *",
        "a timezone-only patch must re-anchor the stored cron expression"
    );
    let row = load_schedule_row(&url, id).await;
    assert_eq!(
        row.schedule_expr.as_deref(),
        Some("cron_tz:America/New_York:0 3 * * *")
    );
    assert_eq!(row.timezone, "America/New_York");
    assert_ne!(
        row.next_run_at, before.next_run_at,
        "re-anchoring the cron timezone changes the cadence and must recompute next_run_at"
    );
    assert!(
        row.next_run_at.is_some(),
        "the re-anchored cron must have a next slot"
    );
}

/// A timezone-only PATCH on an interval schedule is a no-op (the timezone is
/// meaningless for interval cadence), mirroring the create path's semantics.
#[tokio::test]
async fn timezone_only_patch_is_noop_for_interval_schedules() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_tz_interval_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_tz_interval_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"timezone": "Europe/Berlin"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        after.schedule_expr.as_deref(),
        Some("interval:60"),
        "the interval cadence must be untouched"
    );
    assert_eq!(
        after.next_run_at, before.next_run_at,
        "a timezone no-op must preserve the pending next_run_at"
    );
    assert_eq!(
        after.timezone, "UTC",
        "interval schedules always store UTC (the timezone is ignored)"
    );
}

/// A `catchup_window_secs`-only PATCH re-windows a stored `window` catchup
/// policy (and leaves the mode as `window`).
#[tokio::test]
async fn catchup_window_only_patch_rewindows_stored_window_policy() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_rewindow_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(
        &app,
        "sched_upd_rewindow_wf",
        json!({"catchup_policy": "window", "catchup_window_secs": 3600}),
    )
    .await;
    let before = load_schedule_row(&url, id).await;
    assert_eq!(before.catchup_policy.as_deref(), Some("window"));
    assert_eq!(before.catchup_window_secs, Some(3600));

    let (status, patched) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"catchup_window_secs": 7200}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["catchup_policy_effective"], "window");
    assert_eq!(patched["catchup_window_secs"], 7200);
    let row = load_schedule_row(&url, id).await;
    assert_eq!(row.catchup_policy.as_deref(), Some("window"));
    assert_eq!(row.catchup_window_secs, Some(7200));
}

/// Invalid overlap/skip/catchup policy values are rejected with 400 and the
/// row is byte-for-byte unchanged.
#[tokio::test]
async fn invalid_policy_values_are_rejected_with_400_and_write_nothing() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_badpolicy_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_badpolicy_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    for (field, body) in [
        ("overlap_policy", json!({"overlap_policy": "bogus"})),
        ("skip_policy", json!({"skip_policy": "bogus"})),
        ("catchup_policy", json!({"catchup_policy": "bogus"})),
    ] {
        let (status, resp) = patch_json(&app, format!("/admin/schedules/{id}"), body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "invalid {field} must 400: {resp}"
        );
        assert!(
            resp.to_string().contains(field),
            "the 400 must name the invalid field {field}: {resp}"
        );
        let after = load_schedule_row(&url, id).await;
        assert_eq!(
            serde_json::to_value(&after).unwrap(),
            serde_json::to_value(&before).unwrap(),
            "a rejected {field} patch must write nothing"
        );
    }
}

/// `deny_unknown_fields`: a typo'd field name is rejected with 400 rather
/// than silently deserializing to an all-`None` no-op body that 200s with a
/// SUCCEEDED audit row.
#[tokio::test]
async fn unknown_body_field_is_rejected_with_400_not_silently_ignored() {
    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_typo_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_typo_wf", json!({})).await;
    let before = load_schedule_row(&url, id).await;

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"scheduleexpr": "interval:30"}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a typo'd field must be rejected, not ignored: {body}"
    );
    assert!(
        body.to_string().contains("unknown field"),
        "the 400 must explain the unknown field: {body}"
    );

    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap(),
        "a rejected body must write nothing"
    );
}

/// A timezone-only PATCH on a row whose stored schedule expression is
/// unparseable returns 400 (repair by sending `schedule_expr` explicitly)
/// instead of leniently coercing the row to `manual` and `NULL`ing
/// `next_run_at` as a side effect. A stored `"manual"` expression remains a
/// legitimate timezone no-op.
#[tokio::test]
async fn timezone_only_patch_on_unparseable_stored_expr_returns_400() {
    use harvest_schedules::dsl;

    let (url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&url);
    let registry = etl_registry("sched_upd_corrupt_expr_wf");
    let app = build_app(&pool, registry);

    let id = create_schedule(&app, "sched_upd_corrupt_expr_wf", json!({})).await;
    let mut conn = connect(&url).await;
    diesel::update(harvest_schedules::table.find(id))
        .set(dsl::schedule_expr.eq(Some("interval:not-a-number")))
        .execute(&mut conn)
        .await
        .expect("corrupt the stored expression");
    let before = load_schedule_row(&url, id).await;

    let (status, body) = patch_json(
        &app,
        format!("/admin/schedules/{id}"),
        json!({"timezone": "America/New_York"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("unparseable"),
        "the 400 must explain the stored expression is unparseable: {body}"
    );
    let after = load_schedule_row(&url, id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap(),
        "the corrupt row must be left untouched (no lenient manual coercion)"
    );

    // A stored "manual" expression is parseable-by-convention: the timezone
    // edit is a documented no-op for it.
    let manual_registry = etl_registry("sched_upd_manual_tz_wf");
    let manual_app = build_app(&pool, manual_registry);
    let manual_id = create_schedule(
        &manual_app,
        "sched_upd_manual_tz_wf",
        json!({"schedule_expr": "manual"}),
    )
    .await;
    let (status, patched) = patch_json(
        &manual_app,
        format!("/admin/schedules/{manual_id}"),
        json!({"timezone": "America/New_York"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["schedule_expr"], "manual");
}
