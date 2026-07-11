//! Integration tests for the build-routing percentage ramp management API
//! (issue #604): `POST /admin/build-routing/ramp`, `GET /admin/build-routing`,
//! `DELETE /admin/build-routing/ramp/{queue_name}`.
//!
//! TDD structure:
//!   RED  – these tests drove the handler/route design
//!   GREEN – handlers in `autumn-harvest-plugin/src/api.rs`
//!
//! Docker/testcontainers is not available in this sandbox, so these tests are
//! compile-checked only here (`cargo test --no-run` / `cargo check --tests`);
//! they exercise a real Postgres instance in CI.

use autumn_harvest::build_routing::set_build_policy;
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::AsyncPgConnection;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

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
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
);

type HarvestApiApp = axum::Router;

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

fn build_ramp_app(pool: DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
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
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
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

async fn delete_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("DELETE request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

#[tokio::test]
async fn set_ramp_then_get_shows_base_target_and_percent() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let mut conn = pool.get().await.expect("get conn");
    set_build_policy(&mut conn, "default", "base-v1", None)
        .await
        .expect("seed base policy");
    drop(conn);

    let app = build_ramp_app(pool);

    let (status, body) = post_json(
        &app,
        "/admin/build-routing/ramp",
        json!({ "queue_name": "default", "target_build_id": "canary-v2", "ramp_percent": 25 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["target_build_id"], "canary-v2");
    assert_eq!(body["ramp_percent"], 25);

    let (status, body) = get_json(&app, "/admin/build-routing").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let policies = body["policies"].as_array().expect("policies array");
    let default_policy = policies
        .iter()
        .find(|p| p["queue_name"] == "default")
        .expect("default policy present");
    assert_eq!(default_policy["build_id"], "base-v1");
    assert_eq!(default_policy["target_build_id"], "canary-v2");
    assert_eq!(default_policy["ramp_percent"], 25);
}

#[tokio::test]
async fn set_ramp_without_base_policy_returns_conflict() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let app = build_ramp_app(pool);

    let (status, _body) = post_json(
        &app,
        "/admin/build-routing/ramp",
        json!({ "queue_name": "no-base-policy", "target_build_id": "canary-v2", "ramp_percent": 25 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn set_ramp_rejects_percent_out_of_range() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let mut conn = pool.get().await.expect("get conn");
    set_build_policy(&mut conn, "default", "base-v1", None)
        .await
        .expect("seed base policy");
    drop(conn);
    let app = build_ramp_app(pool);

    let (status, _body) = post_json(
        &app,
        "/admin/build-routing/ramp",
        json!({ "queue_name": "default", "target_build_id": "canary-v2", "ramp_percent": 101 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn clear_ramp_removes_target_and_percent() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let mut conn = pool.get().await.expect("get conn");
    set_build_policy(&mut conn, "default", "base-v1", None)
        .await
        .expect("seed base policy");
    drop(conn);
    let app = build_ramp_app(pool);

    post_json(
        &app,
        "/admin/build-routing/ramp",
        json!({ "queue_name": "default", "target_build_id": "canary-v2", "ramp_percent": 50 }),
    )
    .await;

    let (status, body) = delete_json(&app, "/admin/build-routing/ramp/default").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["ramp_percent"], Value::Null);

    let (_status, body) = get_json(&app, "/admin/build-routing").await;
    let policies = body["policies"].as_array().expect("policies array");
    let default_policy = policies
        .iter()
        .find(|p| p["queue_name"] == "default")
        .expect("default policy present");
    assert_eq!(default_policy["ramp_percent"], Value::Null);
    assert_eq!(default_policy["target_build_id"], Value::Null);
}

#[tokio::test]
async fn clear_ramp_on_unknown_queue_returns_not_found() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let app = build_ramp_app(pool);

    let (status, _body) = delete_json(&app, "/admin/build-routing/ramp/no-such-queue").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn set_ramp_records_audit_row() {
    let (db_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&db_url);
    let mut conn = pool.get().await.expect("get conn");
    set_build_policy(&mut conn, "default", "base-v1", None)
        .await
        .expect("seed base policy");
    drop(conn);
    let app = build_ramp_app(pool.clone());

    post_json(
        &app,
        "/admin/build-routing/ramp",
        json!({ "queue_name": "default", "target_build_id": "canary-v2", "ramp_percent": 25 }),
    )
    .await;

    let (status, body) = get_json(&app, "/admin/audit?operation=build_routing.ramp.set").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let records = body.as_array().expect("audit response is a raw array");
    assert!(
        !records.is_empty(),
        "expected an audit record for build_routing.ramp.set"
    );
}
