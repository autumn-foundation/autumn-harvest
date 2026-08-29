//! HTTP integration tests for `GET /admin/codec/rotation` — issue #948, AC7.
//!
//! Covers the admin gate, the un-rotated zero state, the per-shard
//! rows-remaining-per-key-id shape after a real rotation, and the contract
//! registration that keeps `docs/api-contract.json` honest.
//!
//! Runs against a real Postgres 16 container (or `HARVEST_TEST_DATABASE_URL`,
//! in which case each test gets its own throwaway database — the rotation
//! census is shard-wide by design and cannot share a database).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_harvest_plugin::{management_api_response_fields, management_api_routes};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

#[derive(Debug)]
struct XorCodec(u8);

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

// ── contract registration ────────────────────────────────────────────────────

#[test]
fn the_rotation_route_is_registered_in_the_management_api_contract() {
    let routes = management_api_routes();
    assert!(
        routes.contains(&("GET", "/admin/codec/rotation")),
        "GET /admin/codec/rotation must be listed in management_api_routes; found: {routes:?}"
    );
}

#[test]
fn the_rotation_route_declares_its_response_fields() {
    let declared = management_api_response_fields()
        .iter()
        .find(|(method, path, _)| *method == "GET" && *path == "/admin/codec/rotation")
        .map(|(_, _, fields)| *fields)
        .expect("GET /admin/codec/rotation must declare its response fields");
    let fields = declared.expect("the route returns an object, not a bare array");
    for expected in [
        "active_key_id",
        "registered_key_ids",
        "shards",
        "rows_remaining_total",
        "status",
        "unavailable_shards",
    ] {
        assert!(
            fields.contains(&expected),
            "response field {expected:?} must be declared; found: {fields:?}"
        );
    }
}

// ── harness ──────────────────────────────────────────────────────────────────

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_codec_admin_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let cut = admin_url
            .rfind('/')
            .expect("a postgres URL has a database path");
        let url = format!("{}/{db_name}", &admin_url[..cut]);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn build_app_inner(pool: &DbPool, codecs: PayloadCodecs, admin: bool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    if admin {
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.set_payload_codecs(codecs);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("codec-rotation-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request should not fail");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn seed_event_under_key(pool: &DbPool, codecs: &PayloadCodecs, key_id: &str) {
    use autumn_harvest::schema::harvest_workflow_executions;
    let mut conn = pool.get().await.expect("pool conn");
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "rotation_admin_wf",
        workflow_id: &Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
        parent_id: None,
        queue_name: "default",
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert execution");

    let restore = codecs.active_key_id();
    codecs.set_active_key(key_id).expect("activate for fixture");
    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: json!({"user": "alice"}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
        codecs,
    )
    .await
    .expect("append events");
    codecs.set_active_key(&restore).expect("restore active key");
}

// ── behaviour ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_codec_rotation_requires_admin_auth() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_inner(&pool, PayloadCodecs::default(), false);

    let (status, _body) = get_json(&app, "/admin/codec/rotation").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_unrotated_deployment_reports_a_clean_zero_state() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let app = build_app_inner(&pool, PayloadCodecs::default(), true);

    let (status, body) = get_json(&app, "/admin/codec/rotation").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["active_key_id"], "legacy");
    assert_eq!(body["registered_key_ids"], json!([]));
    assert_eq!(body["rows_remaining_total"], json!(0));
    assert_eq!(body["status"], "complete");
    assert_eq!(body["unavailable_shards"], json!([]));
}

#[tokio::test]
async fn rotation_progress_reports_rows_per_key_id_per_shard() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1", Arc::new(XorCodec(0x11)))
        .expect("register k1");
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k1").expect("activate k1");

    seed_event_under_key(&pool, &codecs, "k1").await;
    seed_event_under_key(&pool, &codecs, "k1").await;
    seed_event_under_key(&pool, &codecs, "k2").await;
    codecs.set_active_key("k2").expect("rotate to k2");

    let app = build_app_inner(&pool, codecs, true);
    let (status, body) = get_json(&app, "/admin/codec/rotation").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["active_key_id"], "k2");
    assert_eq!(body["registered_key_ids"], json!(["k1", "k2"]));
    assert_eq!(body["status"], "complete");
    let shards = body["shards"].as_array().expect("shards array");
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0]["shard_id"], json!(0));
    assert_eq!(shards[0]["rows_by_key_id"]["k1"], json!(2));
    assert_eq!(shards[0]["rows_by_key_id"]["k2"], json!(1));
    assert_eq!(
        shards[0]["rows_remaining"],
        json!(2),
        "rows_remaining sums the NON-active key ids"
    );
    assert_eq!(shards[0]["cursor"], Value::Null, "no sweep has run yet");
    assert_eq!(body["rows_remaining_total"], json!(2));
}
