use autumn_web::reexports::axum;
use autumn_web::reexports::http;
use axum::body::Body;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use http::{Request, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260501020000_harvest_batch_processed_ids/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260504000000_harvest_workflow_parent_children/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
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
    include_str!(
        "../../autumn-harvest/migrations/20260616000001_harvest_workflow_schedule_id/up.sql"
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
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
);

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

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &autumn_harvest::worker::DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("canary-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

#[tokio::test]
async fn test_replay_canary_api_endpoint() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Authenticated request (admin) should hit the route and return a Pass verdict report
    let auth_req = Request::builder()
        .method("POST")
        .uri("/admin/workflows/replay-canary")
        .header("content-type", "application/json")
        .header("x-harvest-admin", "true")
        .body(Body::from(json!({ "sample_size": 10 }).to_string()))
        .unwrap();
    let response = app.clone().oneshot(auth_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let report: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(report["verdict"], "pass");
    assert_eq!(report["sampled"], 0);
    assert_eq!(report["replay_succeeded"], 0);
    assert_eq!(report["replay_failed"], 0);
    assert_eq!(report["truncated"], false);
}

fn activity_wf_info() -> autumn_harvest::info::WorkflowInfo {
    autumn_harvest::info::WorkflowInfo {
        mcp: false,
        name: "activity_wf",
        module: module_path!(),
        handler: |ctx, input| {
            Box::pin(async move {
                let _ = ctx
                    .execute_activity_raw("my_activity", input, "default")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(json!("done"))
            })
        },
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

fn build_app_with_wf(pool: &autumn_harvest::worker::DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let registry = HandlerRegistry::new(vec![activity_wf_info()], vec![]);

    api_state.install(HarvestApiRuntime::new(
        Arc::new(registry),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("canary-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

#[tokio::test]
async fn test_replay_canary_api_endpoint_fails_on_divergence() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
    use autumn_harvest::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with_wf(&pool);

    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = ExecutionId::new();
    let run_id = uuid::Uuid::new_v4();

    diesel::insert_into(harvest_workflow_executions::table)
        .values((
            harvest_workflow_executions::id.eq(exec_id.as_uuid()),
            harvest_workflow_executions::workflow_name.eq("activity_wf".to_string()),
            harvest_workflow_executions::workflow_id.eq("wf-3".to_string()),
            harvest_workflow_executions::run_id.eq(run_id),
            harvest_workflow_executions::shard_id.eq(0),
            harvest_workflow_executions::state.eq("RUNNING".to_string()),
            harvest_workflow_executions::input.eq(json!("hi")),
            harvest_workflow_executions::queue_name.eq("default".to_string()),
            harvest_workflow_executions::started_at.eq(Utc::now()),
            harvest_workflow_executions::created_at.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 1: WorkflowStarted
    let start_event = WorkflowEvent::WorkflowStarted {
        input: json!("hi"),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    };
    let start_json = serde_json::to_value(&start_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(1),
            harvest_events::event_type.eq("WorkflowStarted".to_string()),
            harvest_events::event_data.eq(start_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Event 2: ActivityScheduled with a mismatching activity name "different_activity"
    let act_event = WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "different_activity".to_string(),
        input: json!("hi"),
        queue: "default".to_string(),
    };
    let act_json = serde_json::to_value(&act_event).unwrap();

    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(2),
            harvest_events::event_type.eq("ActivityScheduled".to_string()),
            harvest_events::event_data.eq(act_json),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    // Hit the endpoint.
    let auth_req = Request::builder()
        .method("POST")
        .uri("/admin/workflows/replay-canary")
        .header("content-type", "application/json")
        .header("x-harvest-admin", "true")
        .body(Body::from(json!({ "sample_size": 10 }).to_string()))
        .unwrap();

    let response = app.oneshot(auth_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let report: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(report["verdict"], "fail");
    assert_eq!(report["sampled"], 1);
    assert_eq!(report["replay_succeeded"], 0);
    assert_eq!(report["replay_failed"], 1);
    assert_eq!(report["details"].as_array().unwrap().len(), 1);
    let error_detail = report["details"][0]["error"].as_str().unwrap();
    assert!(error_detail.contains("expected") && error_detail.contains("got"));
}
