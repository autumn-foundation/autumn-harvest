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
use autumn_harvest::schema::{harvest_audit_log, harvest_schedules, harvest_workflow_executions};
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

    // GET /admin/schedules reflects the update immediately (AC10).
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

    // ── Run 2 under the post-edit spec ───────────────────────────────────────
    arm_slot(&url, id, 200).await;
    tick(&pool, &registry).await;
    let completed_ids = wait_for_completed(&url, wf_name, 2).await;
    let run2_id = *completed_ids.iter().find(|r| **r != run1_ids[0]).unwrap();

    let mut conn = connect(&url).await;
    let (run2_input, run2_output): (Value, Option<Value>) = harvest_workflow_executions::table
        .find(run2_id)
        .select((
            harvest_workflow_executions::dsl::input,
            harvest_workflow_executions::dsl::output,
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
    // Touch the URL so the container stays alive via `url` usage.
    let _ = load_missing(&url).await;

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

async fn load_missing(url: &str) -> bool {
    let mut conn = connect(url).await;
    let count: i64 = harvest_schedules::table
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    count >= 0
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
