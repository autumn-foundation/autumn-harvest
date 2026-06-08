use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use autumn_harvest::worker::DbPool;
use autumn_harvest::{HistoryArchiver, RetentionConfig, WorkflowEvent};
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::{HarvestRunner, HarvestRunnerResources, HarvestRuntimeConfig};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
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
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    )
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

use autumn_harvest_plugin::HarvestMode;
