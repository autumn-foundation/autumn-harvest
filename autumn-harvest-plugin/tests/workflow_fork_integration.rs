//! HTTP integration tests for `POST /workflows/{id}/fork` (issue #2000).
//!
//! The route wraps `fork::fork_workflow_execution`. These tests check the HTTP
//! contract: the status codes, the response fields, the admin gate and the
//! provenance of the fork row. The engine suite `fork_tests.rs` checks what
//! a fork runs.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{ExecutionId, ShardId};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool, admin_boundary: bool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(admin_boundary);
    // Issue #1802: with no admin boundary, the 401 must come from the admin
    // gate, not from the mutation gate.
    api_state.set_allow_unauthenticated_mutations(!admin_boundary);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("fork-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

async fn post_fork(app: &axum::Router, exec_id: &str, body: Value, admin: bool) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/workflows/{exec_id}/fork"))
        .header("content-type", "application/json");
    if admin {
        request = request
            .header("x-harvest-admin", "true")
            .header("x-harvest-actor", "fork-operator");
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// Insert a `COMPLETED` run with a one-event history.
async fn seed_completed(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = json!({ "order": 7 });
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "fork_http_wf",
        workflow_id: &format!("wf-{}", Uuid::new_v4()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
        parent_id: None,
        queue_name: "fork-http-unpolled",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert source");
    autumn_harvest::store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::WorkflowStarted {
                input,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCompleted {
                output: json!("done"),
            },
        ],
        0,
    )
    .await
    .expect("append history");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(json!("done"))),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .expect("complete source");
    exec_id
}

async fn load(conn: &mut AsyncPgConnection, exec_id: &str) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.parse::<Uuid>().expect("uuid"))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_route_creates_a_recorded_fork_and_leaves_the_source() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let source = seed_completed(&mut conn).await;
    let source_id = source.to_string();
    let before = load(&mut conn, &source_id).await;

    let (status, body) = post_fork(&app, &source_id, json!({ "reason": "what-if" }), true).await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    for field in [
        "new_exec_id",
        "workflow_id",
        "forked_from_exec_id",
        "fork_event_id",
        "events_carried_over",
        "effects",
    ] {
        assert!(body.get(field).is_some(), "missing {field}: {body}");
    }
    assert_eq!(body["effects"], json!("recorded"), "recorded is the default");
    assert_eq!(body["forked_from_exec_id"], json!(source_id));
    assert_eq!(body["fork_event_id"], json!(0));

    let fork = load(&mut conn, body["new_exec_id"].as_str().unwrap()).await;
    assert_eq!(fork.state, "RUNNING");
    assert_eq!(fork.start_source.as_deref(), Some("fork"));
    assert_eq!(fork.start_source_ref.as_deref(), Some(source_id.as_str()));
    assert_ne!(fork.workflow_id, before.workflow_id);

    let after = load(&mut conn, &source_id).await;
    assert_eq!(after.state, "COMPLETED");
    assert_eq!(after.output, before.output);
    assert_eq!(after.completed_at, before.completed_at);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_route_is_admin_only() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, false);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let source = seed_completed(&mut conn).await;

    let (status, _) = post_fork(&app, &source.to_string(), json!({}), false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_route_maps_refusals() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, true);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    // The terminal event is event 1, so fork point 1 carries it.
    let source = seed_completed(&mut conn).await.to_string();
    let at_terminal = json!({ "fork_point": { "type": "event_id", "event_id": 1 } });
    let (status, body) = post_fork(&app, &source, at_terminal, true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");

    let bad_override = json!({
        "activity_overrides": [{ "activity_name": "charge", "occurrence": 0, "output": 1 }]
    });
    let (status, body) = post_fork(&app, &source, bad_override, true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");

    let (status, _) = post_fork(&app, &Uuid::new_v4().to_string(), json!({}), true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    autumn_harvest::erase::erase_workflow_payloads(&mut conn, source.parse().unwrap(), "gdpr")
        .await
        .expect("erase the source");
    let live = json!({ "effects": "live" });
    let (status, body) = post_fork(&app, &source, live, true).await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {body}");
    assert!(body["message"].as_str().unwrap_or_default().contains("erased"));
}
