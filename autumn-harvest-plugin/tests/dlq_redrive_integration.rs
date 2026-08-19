//! HTTP integration tests for `POST /dlq/redrive` — issue #510.
//!
//! Drives the redrive endpoint through the real management router against a
//! Postgres container, covering: dry-run (no mutation), a real redrive that
//! reactivates a FAILED execution and writes a `dlq.redrive` audit row, the
//! empty-filter 400, the admin guard, the `max` cap (matched vs redriven), and
//! a terminal (COMPLETED) owning execution surfacing as a per-row failure.

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::schema::harvest_audit_log;
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{Priority, StartWorkflowParams, store};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_test_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (database_url, container)
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_app(pool: DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_app_no_admin(pool: DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_json(app: &HarvestApiApp, uri: &str, payload: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request failed");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn connect(database_url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect")
}

/// Seed a workflow execution sealed in `state` (history ends in `terminal`) plus
/// a matching workflow-task DLQ row. Returns `(exec_id, dlq_id)`.
async fn seed(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    queue: &str,
    state: &str,
    terminal: WorkflowEvent,
    error: &str,
) -> (ExecutionId, Uuid) {
    let started = autumn_harvest::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "redrive_http_wf",
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: json!({"k": "v"}),
            parent_id: None,
            queue_name: queue,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");
    let exec_id = started.exec_id;

    let history = store::load_history(conn, exec_id).await.expect("history");
    store::append_events(conn, exec_id, &[terminal], history.next_event_id)
        .await
        .expect("append terminal");
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state=$1, completed_at=NOW() WHERE id=$2",
    )
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("seal");

    let dlq_id = autumn_harvest::dlq::dead_letter(
        conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: queue.to_string(),
            task_type: "workflow".to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: None,
            input: json!({"k": "v"}),
            error: error.to_string(),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dlq insert");

    (exec_id, dlq_id)
}

/// Seed a FAILED execution (history ends in `WorkflowFailed`) plus a DLQ row.
async fn seed_failed(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    queue: &str,
    error: &str,
) -> (ExecutionId, Uuid) {
    seed(
        conn,
        workflow_id,
        queue,
        "FAILED",
        WorkflowEvent::workflow_failed(error.to_string()),
        error,
    )
    .await
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct R {
        #[diesel(sql_type = diesel::sql_types::Text)]
        v: String,
    }
    diesel::sql_query("SELECT state AS v FROM harvest_workflow_executions WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<R>(conn)
        .await
        .expect("state")
        .v
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn redrive_empty_filter_returns_400() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_app(build_test_pool(&url));
    let (status, _body) = post_json(&app, "/dlq/redrive", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn redrive_requires_admin() {
    let (url, _c) = setup_test_database_url().await;
    let app = build_app_no_admin(build_test_pool(&url));
    let (status, _body) = post_json(&app, "/dlq/redrive", json!({"queue": "default"})).await;
    assert!(
        status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
        "redrive must be admin-guarded, got {status}"
    );
}

#[tokio::test]
async fn redrive_dry_run_previews_without_mutation() {
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    let (exec_id, _dlq) = seed_failed(&mut conn, "dry-1", "dryq", "boom").await;

    let app = build_app(build_test_pool(&url));
    let (status, body) = post_json(
        &app,
        "/dlq/redrive",
        json!({"queue": "dryq", "dry_run": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["dry_run"], true);
    assert_eq!(body["matched"], 1);
    assert_eq!(body["redriven"], 0);
    assert_eq!(body["ids"].as_array().unwrap().len(), 1);
    // Nothing mutated.
    assert_eq!(execution_state(&mut conn, exec_id).await, "FAILED");
}

#[tokio::test]
async fn redrive_real_reactivates_and_writes_audit() {
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    let (exec_id, _dlq) = seed_failed(&mut conn, "real-1", "realq", "connection refused").await;

    let app = build_app(build_test_pool(&url));
    let (status, body) = post_json(
        &app,
        "/dlq/redrive",
        json!({"error_contains": "connection", "reason": "fixed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["redriven"], 1);
    assert_eq!(body["failed"], 0);

    // Execution reactivated.
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");

    // Exactly one dlq.redrive audit row.
    let audit_count: i64 = harvest_audit_log::table
        .filter(harvest_audit_log::operation.eq("dlq.redrive"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count audit");
    assert_eq!(audit_count, 1);
}

#[tokio::test]
async fn redrive_max_caps_matched_vs_redriven() {
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    for i in 0..4 {
        seed_failed(&mut conn, &format!("cap-{i}"), "capq", "boom").await;
    }

    let app = build_app(build_test_pool(&url));
    let (status, body) = post_json(&app, "/dlq/redrive", json!({"queue": "capq", "max": 2})).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["matched"], 4);
    assert_eq!(body["redriven"], 2);
}

#[tokio::test]
async fn redrive_terminal_execution_surfaces_as_failure() {
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    // COMPLETED owning execution → not resurrectable.
    let (_exec, _dlq) = seed(
        &mut conn,
        "term-1",
        "termq",
        "COMPLETED",
        WorkflowEvent::WorkflowCompleted {
            output: json!({"ok": true}),
        },
        "boom",
    )
    .await;

    let app = build_app(build_test_pool(&url));
    let (status, body) = post_json(&app, "/dlq/redrive", json!({"queue": "termq"})).await;
    // redriven == 0 and a failure present → handler returns 500 (all-failed).
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body: {body}");
    assert_eq!(body["redriven"], 0);
    assert_eq!(body["failed"], 1);
    assert!(
        body["failures"][0]["reason"]
            .as_str()
            .unwrap_or("")
            .to_lowercase()
            .contains("not resurrectable"),
        "body: {body}"
    );
}
