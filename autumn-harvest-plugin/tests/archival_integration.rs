// The registered `#[workflow]` fixture below has underscore-prefixed params
// that the macro-generated dispatch shim uses, and no `.await` in its body —
// matching the allow convention in the sibling workflow-defining test files
// (mcp_tools_http_tests, webhook_receiver_http_tests).
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use autumn_harvest::prelude::{WorkflowContext, workflow};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{HistoryArchiver, RetentionConfig, WorkflowEvent};

// Registered workflow matching the `workflow_name` used by
// `insert_retention_fixture_execution`, so a per-type retention override on
// "retention_fixture" passes the builder-time registration check (issue #737,
// AC6). The handler body is never executed by these retention/archival tests.
#[workflow]
async fn retention_fixture(_ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    Ok(())
}
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::{HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

#[derive(diesel::QueryableByName)]
struct CountByName {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

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
    // issue #523: workflow-level retry policy columns.
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
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
        "../../autumn-harvest/migrations/20260710000000_harvest_workflow_continue_chain/up.sql"
    ),
);

struct TestArchiver {
    calls: Arc<Mutex<Vec<autumn_harvest::history_export::HistoryExportDocument>>>,
    should_fail: Arc<AtomicBool>,
}

impl HistoryArchiver for TestArchiver {
    fn archive(
        &self,
        doc: &autumn_harvest::history_export::HistoryExportDocument,
    ) -> autumn_harvest::ArchiverFuture<'_> {
        let calls = self.calls.clone();
        let should_fail = std::sync::atomic::AtomicBool::load(
            &self.should_fail,
            std::sync::atomic::Ordering::SeqCst,
        );
        let doc = doc.clone();
        Box::pin(async move {
            if should_fail {
                return Err("simulated archive failure".into());
            }
            calls.lock().unwrap().push(doc);
            Ok(())
        })
    }
}

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start postgres container");
    let host = container.get_host().await.expect("failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");
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

async fn insert_retention_fixture_execution(
    conn: &mut AsyncPgConnection,
    exec_id: uuid::Uuid,
    workflow_id: &str,
    state: &str,
    completed_at_expr: &str,
) {
    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions (
            id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, completed_at, created_at
        ) VALUES (
            $1, 'retention_fixture', '{workflow_id}', gen_random_uuid(), 0, '{state}', '{{}}'::jsonb, 'default',
            NOW() - INTERVAL '11 days', {completed_at_expr}, NOW() - INTERVAL '11 days'
        )"
    ))
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(conn)
    .await
    .expect("failed to insert fixture workflow execution");

    let exec_id_typed = autumn_harvest::types::ExecutionId::from_uuid(exec_id);
    let events = vec![WorkflowEvent::WorkflowCompleted { output: json!({}) }];
    autumn_harvest::store::append_events(conn, exec_id_typed, &events, 0)
        .await
        .expect("failed to insert workflow completed event");
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn post_json(
    app: &HarvestApiApp,
    uri: impl Into<String>,
    payload: Value,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn count_execution_rows(conn: &mut AsyncPgConnection, exec_id: uuid::Uuid) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .get_result::<CountByName>(conn)
        .await
        .expect("count query should succeed")
        .count
}

#[tokio::test]
#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn archival_hook_executes_successfully_and_preserves_on_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let calls = Arc::new(Mutex::new(Vec::new()));
    let should_fail = Arc::new(AtomicBool::new(false));
    let archiver = TestArchiver {
        calls: calls.clone(),
        should_fail: should_fail.clone(),
    };

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
                audit_retention_days: 90,
                schedule_decision_retention_days: 7,
                archival_timeout_secs: 30,
                ..Default::default()
            })
            .history_archiver(archiver)
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("runner with retention and archiver should start");

    let successful_exec = uuid::Uuid::new_v4();
    let failed_exec = uuid::Uuid::new_v4();

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");

    // Insert old completed workflow (eligible for retention/archival)
    insert_retention_fixture_execution(
        &mut conn,
        successful_exec,
        "successful-archival",
        "COMPLETED",
        "NOW() - INTERVAL '10 days'",
    )
    .await;

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    let app = harvest_api_router(api_state).with_state(autumn_web::AppState::for_test());

    // 1. Run retention with archiver returning success.
    // The execution should be successfully archived and deleted.
    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // Poll the retention status to print details
    for _ in 0..10 {
        let (_status, status_json) = get_json(&app, "/admin/retention").await;
        println!("--- Retention Monitor Status: {status_json:?}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Wait for the successful delete.
    let mut deleted = false;
    for _ in 0..40 {
        if count_execution_rows(&mut conn, successful_exec).await == 0 {
            deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        deleted,
        "Workflow execution should be deleted on successful archive hook"
    );

    // Verify the mock archiver was invoked.
    {
        let guard = calls.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert_eq!(
            guard[0].execution_id,
            autumn_harvest::types::ExecutionId::from_uuid(successful_exec)
        );
        assert_eq!(guard[0].workflow_name, "retention_fixture");
    }

    // Clear calls for next run.
    calls.lock().unwrap().clear();

    // Now insert a second workflow execution.
    insert_retention_fixture_execution(
        &mut conn,
        failed_exec,
        "failed-archival",
        "COMPLETED",
        "NOW() - INTERVAL '10 days'",
    )
    .await;

    // Set archiver to fail.
    should_fail.store(true, Ordering::SeqCst);

    // 2. Run retention again. The archiver should return an error, and the execution must NOT be deleted.
    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // Wait a bit to verify it is NOT deleted.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let count = count_execution_rows(&mut conn, failed_exec).await;
    assert_eq!(
        count, 1,
        "Workflow execution must NOT be deleted if archive hook fails"
    );

    runner.stop().await;
}

/// Issue #737 (AC7, second clause): the #345 archival hook must still fire for
/// every deleted row regardless of which retention policy selected it — in
/// particular a row deleted because of a *per-workflow-type override* (not the
/// global `max_age`). Global is 30 days; the `retention_fixture` type is
/// overridden to 7 days; the seeded row completed 10 days ago. Without the
/// override 10 days < 30-day global would keep it, so its deletion is
/// attributable solely to the override — and the archiver must have been
/// invoked with that row before it was deleted.
#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn archival_hook_fires_for_override_deleted_row() {
    let _ = tracing_subscriber::fmt::try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let calls = Arc::new(Mutex::new(Vec::new()));
    let should_fail = Arc::new(AtomicBool::new(false));
    let archiver = TestArchiver {
        calls: calls.clone(),
        should_fail: should_fail.clone(),
    };

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            // "retention_fixture" must be registered for the override to pass
            // build-time validation (issue #737, AC6).
            .workflows(vec![retention_fixture_info()])
            .retention(
                RetentionConfig {
                    // Global 30 days: on its own, a 10-day-old row survives.
                    max_age_secs: Some(30 * 24 * 60 * 60),
                    tick_interval_secs: 60 * 60,
                    batch_size: 1000,
                    dry_run: false,
                    audit_retention_days: 90,
                    schedule_decision_retention_days: 7,
                    archival_timeout_secs: 30,
                    ..Default::default()
                }
                // Per-type override 7 days: makes the 10-day-old row deletable.
                .with_workflow_override(
                    "retention_fixture",
                    std::time::Duration::from_secs(7 * 24 * 60 * 60),
                ),
            )
            .history_archiver(archiver)
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("runner with per-type retention override and archiver should start");

    let override_exec = uuid::Uuid::new_v4();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");

    // 10 days old: deletable under the 7-day override, but NOT under the
    // 30-day global — so its deletion is attributable to the override alone.
    insert_retention_fixture_execution(
        &mut conn,
        override_exec,
        "override-archival",
        "COMPLETED",
        "NOW() - INTERVAL '10 days'",
    )
    .await;

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    let app = harvest_api_router(api_state).with_state(autumn_web::AppState::for_test());

    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // The override-selected row is deleted.
    let mut deleted = false;
    for _ in 0..40 {
        if count_execution_rows(&mut conn, override_exec).await == 0 {
            deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        deleted,
        "a row deletable only under a per-type override must be deleted by retention"
    );

    // AC7: the archival hook fired for that row before it was deleted.
    {
        let guard = calls.lock().unwrap();
        assert_eq!(
            guard.len(),
            1,
            "archiver must be invoked exactly once for the override-deleted row"
        );
        assert_eq!(
            guard[0].execution_id,
            autumn_harvest::types::ExecutionId::from_uuid(override_exec)
        );
        assert_eq!(
            guard[0].workflow_name, "retention_fixture",
            "archival hook must fire for the override-selected type"
        );
    }

    runner.stop().await;
}

// A mock archiver that sleeps for 5 seconds to trigger the timeout
struct SlowArchiver;
impl HistoryArchiver for SlowArchiver {
    fn archive(
        &self,
        _doc: &autumn_harvest::history_export::HistoryExportDocument,
    ) -> autumn_harvest::ArchiverFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn archival_hook_times_out_and_preserves_execution() {
    let _ = tracing_subscriber::fmt::try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
                audit_retention_days: 90,
                schedule_decision_retention_days: 7,
                archival_timeout_secs: 1, // 1 second timeout
                ..Default::default()
            })
            .history_archiver(SlowArchiver)
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("runner with retention and slow archiver should start");

    let slow_exec = uuid::Uuid::new_v4();

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");

    // Insert old completed workflow (eligible for retention/archival)
    insert_retention_fixture_execution(
        &mut conn,
        slow_exec,
        "slow-archival",
        "COMPLETED",
        "NOW() - INTERVAL '10 days'",
    )
    .await;

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    let app = harvest_api_router(api_state).with_state(autumn_web::AppState::for_test());

    // Trigger retention
    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // Wait 2.5 seconds (longer than the 1s timeout but shorter than the 5s sleep)
    // and verify that the database row is NOT deleted.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    let count = count_execution_rows(&mut conn, slow_exec).await;
    assert_eq!(
        count, 1,
        "Workflow execution must NOT be deleted if the archival hook times out"
    );

    runner.stop().await;
}

/// Issue #921 review (Codex P2, follow-up): a `task_type = "CALLBACK"`
/// dead-letter row (issue #605) must survive retention alongside its
/// `FAILED` `harvest_completion_deliveries` row -- an earlier review round
/// already scoped the completion-deliveries delete to `state = 'DELIVERED'`
/// so a `FAILED` delivery survives for redrive, but the *separate*
/// `harvest_dead_letters` delete was still unconditional, silently dropping
/// the same delivery's DLQ entry from `GET /dead-letters` discovery even
/// though the delivery row (and its redrive path) still exists.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retention_preserves_a_failed_callback_delivery_and_its_dead_letter() {
    let _ = tracing_subscriber::fmt::try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
                audit_retention_days: 90,
                schedule_decision_retention_days: 7,
                archival_timeout_secs: 30,
                ..Default::default()
            })
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("runner with retention should start");

    let exec_id = uuid::Uuid::new_v4();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");

    insert_retention_fixture_execution(
        &mut conn,
        exec_id,
        "callback-dlq-retention",
        "COMPLETED",
        "NOW() - INTERVAL '10 days'",
    )
    .await;

    // A FAILED completion-delivery row (already-exhausted callback).
    let delivery_id = uuid::Uuid::new_v4();
    diesel::insert_into(autumn_harvest::schema::harvest_completion_deliveries::table)
        .values(autumn_harvest::models::NewCompletionDelivery {
            id: delivery_id,
            workflow_exec_id: exec_id,
            shard_id: 0,
            callback_index: 0,
            workflow_name: "callback-dlq-retention",
            workflow_id: "callback-dlq-retention",
            target_url: "https://receiver.example.com/hook",
            event_filter: json!({ "type": "AnyTerminal" }),
            terminal_state: "COMPLETED",
            payload: json!({ "delivery_id": delivery_id, "state": "COMPLETED" }),
            max_attempts: 5,
            retry_policy: json!({
                "max_attempts": 5,
                "initial_interval": { "secs": 30, "nanos": 0 },
                "backoff_coefficient": 2.0,
                "max_interval": { "secs": 600, "nanos": 0 },
                "non_retryable_errors": [],
                "jitter": "None"
            }),
            next_attempt_at: chrono::Utc::now(),
        })
        .execute(&mut conn)
        .await
        .expect("failed to seed FAILED completion-delivery fixture row");
    diesel::update(autumn_harvest::schema::harvest_completion_deliveries::table.find(delivery_id))
        .set(autumn_harvest::schema::harvest_completion_deliveries::state.eq("FAILED"))
        .execute(&mut conn)
        .await
        .expect("failed to mark fixture delivery FAILED");

    // The matching CALLBACK dead-letter written on exhaustion.
    let dead_letter_id = autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: delivery_id,
            queue_name: "completion-callback".to_string(),
            task_type: "CALLBACK".to_string(),
            workflow_exec_id: Some(exec_id),
            activity_name: None,
            input: json!({ "delivery_id": delivery_id, "state": "COMPLETED" }),
            error: "delivery exhausted 5 attempts".to_string(),
            attempts: 5,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("failed to seed CALLBACK dead-letter fixture row");

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    let app = harvest_api_router(api_state).with_state(autumn_web::AppState::for_test());

    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // Wait for the execution row to be collected.
    let mut deleted = false;
    for _ in 0..40 {
        if count_execution_rows(&mut conn, exec_id).await == 0 {
            deleted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(deleted, "execution row should be collected by retention");

    // The FAILED completion-delivery row survives (pre-existing fix).
    let delivery_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_completion_deliveries WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(delivery_id)
    .get_result::<CountByName>(&mut conn)
    .await
    .expect("count query should succeed")
    .count;
    assert_eq!(
        delivery_count, 1,
        "a FAILED completion-delivery row must survive its owning execution's retention"
    );

    // The matching CALLBACK dead-letter row also survives (this fix).
    let dead_letter_count: i64 =
        diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_dead_letters WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(dead_letter_id)
            .get_result::<CountByName>(&mut conn)
            .await
            .expect("count query should succeed")
            .count;
    assert_eq!(
        dead_letter_count, 1,
        "a CALLBACK dead-letter row must survive retention alongside its FAILED delivery row, \
         so it stays visible via GET /dead-letters"
    );

    runner.stop().await;
}

/// Issue #921 review (Codex P2, follow-up): a `harvest_completion_deliveries`
/// row that resolves to `DELIVERED` *after* its owning execution has already
/// been collected by an earlier retention pass would otherwise never be
/// revisited -- retention only ever iterates over still-live executions, so
/// an orphaned `DELIVERED` row (its `workflow_exec_id` naming no execution
/// at all) would carry its frozen result/error PII with no retention bound.
/// A dedicated per-tick reclaim step deletes exactly these orphaned
/// `DELIVERED` rows; a still-open orphaned `FAILED` row (awaiting redrive)
/// and a `DELIVERED` row whose owner is a live, non-candidate execution must
/// both survive untouched.
fn orphan_reclaim_delivery_fixture(
    id: uuid::Uuid,
    workflow_exec_id: uuid::Uuid,
) -> autumn_harvest::models::NewCompletionDelivery<'static> {
    autumn_harvest::models::NewCompletionDelivery {
        id,
        workflow_exec_id,
        shard_id: 0,
        callback_index: 0,
        workflow_name: "orphan-reclaim-test",
        workflow_id: "orphan-reclaim-test",
        target_url: "https://receiver.example.com/hook",
        event_filter: json!({ "type": "AnyTerminal" }),
        terminal_state: "COMPLETED",
        payload: json!({ "state": "COMPLETED" }),
        max_attempts: 5,
        retry_policy: json!({
            "max_attempts": 5,
            "initial_interval": { "secs": 30, "nanos": 0 },
            "backoff_coefficient": 2.0,
            "max_interval": { "secs": 600, "nanos": 0 },
            "non_retryable_errors": [],
            "jitter": "None"
        }),
        next_attempt_at: chrono::Utc::now(),
    }
}

async fn orphan_reclaim_delivery_row_exists(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> bool {
    diesel::sql_query("SELECT COUNT(*) AS count FROM harvest_completion_deliveries WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<CountByName>(conn)
        .await
        .expect("count query should succeed")
        .count
        > 0
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retention_reclaims_an_orphaned_delivered_completion_delivery() {
    let _ = tracing_subscriber::fmt::try_init();
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);
    let api_state = HarvestApiState::new();

    let runner = HarvestRunner::start(
        autumn_harvest::HarvestBuilder::new()
            .retention(RetentionConfig {
                max_age_secs: Some(7 * 24 * 60 * 60),
                tick_interval_secs: 60 * 60,
                batch_size: 1000,
                dry_run: false,
                audit_retention_days: 90,
                schedule_decision_retention_days: 7,
                archival_timeout_secs: 30,
                ..Default::default()
            })
            .build(),
        &HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: autumn_harvest_plugin::HarvestDatabaseConfig {
                url: Some(database_url.clone()),
            },
            outbox: autumn_harvest_plugin::HarvestOutboxConfig::default(),
            batch: autumn_harvest_plugin::HarvestBatchConfig::default(),
            readiness: autumn_harvest_plugin::HarvestReadinessConfig::default(),
        },
        HarvestRunnerResources::new(pool.clone()),
    )
    .await
    .expect("runner with retention should start");

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for retention fixture");

    // Orphan #1: DELIVERED, owner does not exist at all (simulates the
    // owner having been collected by an earlier retention pass). Must be
    // reclaimed.
    let orphan_delivered_id = uuid::Uuid::new_v4();
    let orphan_exec_id = uuid::Uuid::new_v4();
    diesel::insert_into(autumn_harvest::schema::harvest_completion_deliveries::table)
        .values(orphan_reclaim_delivery_fixture(
            orphan_delivered_id,
            orphan_exec_id,
        ))
        .execute(&mut conn)
        .await
        .expect("failed to seed orphaned DELIVERED fixture row");
    diesel::update(
        autumn_harvest::schema::harvest_completion_deliveries::table.find(orphan_delivered_id),
    )
    .set(autumn_harvest::schema::harvest_completion_deliveries::state.eq("DELIVERED"))
    .execute(&mut conn)
    .await
    .expect("failed to mark orphaned fixture DELIVERED");

    // Orphan #2: FAILED, owner also does not exist. Must survive -- only
    // DELIVERED orphans are reclaimed; a FAILED row may still be awaiting
    // an operator's redrive.
    let orphan_failed_id = uuid::Uuid::new_v4();
    diesel::insert_into(autumn_harvest::schema::harvest_completion_deliveries::table)
        .values(orphan_reclaim_delivery_fixture(
            orphan_failed_id,
            uuid::Uuid::new_v4(),
        ))
        .execute(&mut conn)
        .await
        .expect("failed to seed orphaned FAILED fixture row");
    diesel::update(
        autumn_harvest::schema::harvest_completion_deliveries::table.find(orphan_failed_id),
    )
    .set(autumn_harvest::schema::harvest_completion_deliveries::state.eq("FAILED"))
    .execute(&mut conn)
    .await
    .expect("failed to mark orphaned fixture FAILED");

    // Non-orphan: DELIVERED, but the owner is a live RUNNING execution (so
    // it will never become a retention candidate). Must survive -- this
    // reclaim step is scoped to *orphaned* rows only, not every DELIVERED
    // row in the table.
    let live_exec_id = uuid::Uuid::new_v4();
    insert_retention_fixture_execution(
        &mut conn,
        live_exec_id,
        "orphan-reclaim-live-owner",
        "RUNNING",
        "NULL",
    )
    .await;
    let live_owned_delivered_id = uuid::Uuid::new_v4();
    diesel::insert_into(autumn_harvest::schema::harvest_completion_deliveries::table)
        .values(orphan_reclaim_delivery_fixture(
            live_owned_delivered_id,
            live_exec_id,
        ))
        .execute(&mut conn)
        .await
        .expect("failed to seed live-owned DELIVERED fixture row");
    diesel::update(
        autumn_harvest::schema::harvest_completion_deliveries::table.find(live_owned_delivered_id),
    )
    .set(autumn_harvest::schema::harvest_completion_deliveries::state.eq("DELIVERED"))
    .execute(&mut conn)
    .await
    .expect("failed to mark live-owned fixture DELIVERED");

    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());
    api_state.set_admin_auth_boundary(true);
    let app = harvest_api_router(api_state).with_state(autumn_web::AppState::for_test());

    let (run_now_status, run_now_json) =
        post_json(&app, "/admin/retention/run-now", json!({})).await;
    assert_eq!(run_now_status, StatusCode::OK);
    assert_eq!(run_now_json["ok"], true);

    // Poll until the orphaned DELIVERED row is reclaimed (or time out).
    let mut reclaimed = false;
    for _ in 0..40 {
        if !orphan_reclaim_delivery_row_exists(&mut conn, orphan_delivered_id).await {
            reclaimed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        reclaimed,
        "an orphaned DELIVERED completion-delivery row must be reclaimed by retention"
    );

    assert!(
        orphan_reclaim_delivery_row_exists(&mut conn, orphan_failed_id).await,
        "an orphaned FAILED completion-delivery row must survive -- only DELIVERED orphans \
         are reclaimed"
    );
    assert!(
        orphan_reclaim_delivery_row_exists(&mut conn, live_owned_delivered_id).await,
        "a DELIVERED completion-delivery row owned by a live, non-candidate execution must \
         survive -- the reclaim is scoped to orphaned rows only"
    );

    runner.stop().await;
}

use autumn_harvest_plugin::HarvestMode;
