//! Operator-facing external activity handoff API coverage (issue #168).

use std::sync::Arc;

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalActivityToken, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowEvent, external_task};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    (url, container)
}

fn build_api_app(pool: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::single(),
    ));
    harvest_api_router(api_state).with_state(autumn_web::AppState::for_test())
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
    body: Value,
) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn seed_external_handoff(
    database_url: &str,
    workflow_name: &str,
    workflow_id: &str,
    activity_name: &str,
) -> (ExecutionId, ActivityExecId, ExternalActivityToken) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");

    let execution = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
        input: json!({ "raw": "not exposed in handoff list" }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
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
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&execution)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution");

    store::append_single_event(
        &mut conn,
        exec_id,
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            name: activity_name.to_string(),
            token,
            input: json!({ "raw": "redacted" }),
            queue: "approvals".to_string(),
            schedule_to_close_secs: 3600,
        },
    )
    .await
    .expect("failed to append awaiting event");
    external_task::record_external_task(
        &mut conn,
        exec_id,
        token,
        activity_id,
        activity_name,
        "approvals",
        3600,
    )
    .await
    .expect("failed to record external task");

    (exec_id, activity_id, token)
}

async fn terminal_event_count(database_url: &str, exec_id: ExecutionId) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    autumn_harvest::schema::harvest_events::table
        .filter(autumn_harvest::schema::harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(
            autumn_harvest::schema::harvest_events::event_type.eq("ActivityCompletedExternally"),
        )
        .count()
        .get_result(&mut conn)
        .await
        .expect("failed to count terminal events")
}

#[tokio::test]
async fn pending_handoff_can_be_listed_completed_and_retried_idempotently() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let app = build_api_app(HarvestDbPool::from(build_test_pool(&database_url)));
    let (exec_id, activity_id, token) = seed_external_handoff(
        &database_url,
        "billing_checkout",
        "invoice-42",
        "manager_approval",
    )
    .await;

    let (status, list) = get_json(&app, "/admin/external-handoffs?state=PENDING").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["status"], "ok");
    assert_eq!(list["items"].as_array().expect("items array").len(), 1);
    let item = &list["items"][0];
    assert_eq!(item["token"], token.to_string());
    assert_eq!(item["workflow"]["execution_id"], exec_id.to_string());
    assert_eq!(item["workflow"]["workflow_name"], "billing_checkout");
    assert_eq!(item["workflow"]["workflow_id"], "invoice-42");
    assert_eq!(item["workflow"]["shard_id"], 0);
    assert_eq!(item["activity"]["activity_id"], activity_id.to_string());
    assert_eq!(item["activity"]["activity_name"], "manager_approval");
    assert_eq!(item["state"], "PENDING");
    assert!(item["created_at"].is_string());
    assert!(item["updated_at"].is_string());
    assert!(item["deadline_at"].is_string());
    assert_eq!(item["payloads"]["redacted"], true);
    assert!(
        item.get("input").is_none(),
        "raw payload must not be exposed"
    );

    let (status, detail) = get_json(&app, format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["external_handoffs"][0]["token"], token.to_string());

    let (status, ack) = post_json(
        &app,
        format!("/activities/external/{token}/complete"),
        json!({ "output": { "approved": true } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ack}");
    assert_eq!(ack["newly_resolved"], true);
    assert_eq!(ack["status"], "completed");

    let (status, pending) = get_json(&app, "/admin/external-handoffs?state=PENDING").await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    assert!(pending["items"].as_array().expect("items array").is_empty());

    let (status, completed) = get_json(&app, "/admin/external-handoffs?state=COMPLETED").await;
    assert_eq!(status, StatusCode::OK, "{completed}");
    assert_eq!(completed["items"][0]["state"], "COMPLETED");

    let (status, retry_ack) = post_json(
        &app,
        format!("/activities/external/{token}/complete"),
        json!({ "output": { "approved": true } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry_ack}");
    assert_eq!(retry_ack["newly_resolved"], false);
    assert_eq!(retry_ack["status"], "already_terminal");
    assert_eq!(retry_ack["current_state"], "COMPLETED");
    assert_eq!(terminal_event_count(&database_url, exec_id).await, 1);
}

#[tokio::test]
async fn failing_handoff_returns_terminal_state_for_retries() {
    let (database_url, _container) = setup_database_url_with_migrations().await;
    let app = build_api_app(HarvestDbPool::from(build_test_pool(&database_url)));
    let (_exec_id, _activity_id, token) = seed_external_handoff(
        &database_url,
        "billing_checkout",
        "invoice-43",
        "webhook_wait",
    )
    .await;

    let (status, ack) = post_json(
        &app,
        format!("/activities/external/{token}/fail"),
        json!({ "error": "manager rejected", "retryable": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ack}");
    assert_eq!(ack["newly_resolved"], true);
    assert_eq!(ack["status"], "failed");

    let (status, retry_ack) = post_json(
        &app,
        format!("/activities/external/{token}/fail"),
        json!({ "error": "manager rejected again", "retryable": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry_ack}");
    assert_eq!(retry_ack["newly_resolved"], false);
    assert_eq!(retry_ack["status"], "already_terminal");
    assert_eq!(retry_ack["current_state"], "FAILED");
}
