//! Integration tests for ADR-0001 trace-context propagation through the
//! HTTP workflow-start boundary (issue #136 AC).
//!
//! Verifies that a `TraceContextCarrier` captured by the `TraceContextPropagator`
//! at the `POST /workflows/{name}/start` call site is stored durably in the
//! `harvest_task_queue.trace_context` column so the worker can restore it before
//! invoking the workflow executor.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::TelemetryConfig;
use autumn_harvest::schema::harvest_task_queue;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::{TraceContextCarrier, TraceContextPropagator};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{RetentionConfig, WorkflowInfo};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// -------------------------------------------------------------------------
// Schema migrations (same set as ui_integration.rs)
// -------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260501020000_harvest_batch_processed_ids/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
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
    "
",
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
);

// -------------------------------------------------------------------------
// Mock trace-context propagator
// -------------------------------------------------------------------------

/// A `TraceContextPropagator` that always returns a fixed carrier from
/// `capture()` — simulating a real HTTP span context being captured by a
/// `tracing-opentelemetry` bridge in production.
struct FixedCarrierPropagator {
    carrier: TraceContextCarrier,
}

impl TraceContextPropagator for FixedCarrierPropagator {
    fn capture(&self) -> Option<TraceContextCarrier> {
        Some(self.carrier.clone())
    }

    fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
        // no-op for this test
        Box::new(())
    }
}

// -------------------------------------------------------------------------
// Test helpers
// -------------------------------------------------------------------------

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool build failed")
}

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

/// Verifies end-to-end trace-context propagation through the HTTP boundary:
///
/// `POST /workflows/{name}/start`
///   → `harvest.workflow.schedule` span emitted
///   → `TraceContextPropagator::capture()` called
///   → carrier stored in `harvest_task_queue.trace_context`
///
/// The worker can later call `TraceContextPropagator::install(carrier)` to
/// restore the originating span context before executing the workflow.
#[tokio::test]
async fn start_workflow_stores_captured_trace_context_in_task_queue() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // A fixed W3C traceparent that the mock propagator will "capture" from the
    // current active span context (simulating a tracing-opentelemetry bridge).
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let expected_carrier = TraceContextCarrier::from_traceparent(traceparent);

    // Build registry with mock propagator wired into TelemetryConfig.
    let telemetry = TelemetryConfig::builder()
        .propagator(Arc::new(FixedCarrierPropagator {
            carrier: expected_carrier.clone(),
        }))
        .build();

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![WorkflowInfo {
            name: "echo_workflow",
            module: "telemetry_propagation_tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,
        }],
        vec![],
        Arc::new(HashMap::new()),
        Arc::new(telemetry),
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        autumn_harvest::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let app: axum::Router =
        autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    // POST /workflows/echo_workflow/start — the handler emits
    // harvest.workflow.schedule and calls capture_trace_context().
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/echo_workflow/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "workflow_id": "telem-prop-test-001",
                        "input": null,
                    })
                    .to_string(),
                ))
                .expect("valid request"),
        )
        .await
        .expect("POST failed");

    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "workflow start should succeed"
    );

    // Read the task queue row and verify trace_context is stored.
    let mut conn = pool.get().await.expect("pool connection");
    let rows: Vec<autumn_harvest::models::TaskQueueItem> = harvest_task_queue::table
        .select(autumn_harvest::models::TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("query harvest_task_queue");

    assert_eq!(rows.len(), 1, "exactly one task should be queued");

    let stored_json = rows[0]
        .trace_context
        .as_ref()
        .expect("trace_context column must be non-NULL when propagator returns Some");

    let stored =
        TraceContextCarrier::from_json(stored_json).expect("stored trace_context must deserialize");

    assert_eq!(
        stored.traceparent.as_deref(),
        Some(traceparent),
        "stored traceparent must match the carrier returned by the mock propagator"
    );
}

/// When no propagator is configured (the default `NoOpPropagator`),
/// `capture_trace_context()` returns `None` and `trace_context` stays NULL.
#[tokio::test]
async fn start_workflow_leaves_trace_context_null_when_no_propagator() {
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    // Default registry uses NoOpPropagator → capture() returns None.
    let registry = Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: "echo_workflow",
            module: "telemetry_propagation_tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,
        }],
        vec![],
    ));

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::clone(&registry),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        autumn_harvest::SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));

    let app: axum::Router =
        autumn_harvest_plugin::harvest_api_router(api_state).with_state(test_app_state());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workflows/echo_workflow/start")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "workflow_id": "telem-noop-test-001",
                        "input": null,
                    })
                    .to_string(),
                ))
                .expect("valid request"),
        )
        .await
        .expect("POST failed");

    assert_eq!(response.status(), StatusCode::CREATED);

    let mut conn = pool.get().await.expect("pool connection");
    let rows: Vec<autumn_harvest::models::TaskQueueItem> = harvest_task_queue::table
        .select(autumn_harvest::models::TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("query harvest_task_queue");

    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].trace_context.is_none(),
        "trace_context must be NULL when no propagator is configured"
    );
}
