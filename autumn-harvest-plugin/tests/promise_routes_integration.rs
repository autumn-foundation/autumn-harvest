//! Durable promise settlement over the HTTP signal routes (issue #1985).
//!
//! Each route has a keyed dedupe fast path that reads `harvest_signals` before
//! the insert. These tests prove that the promise settlement rules run before
//! that read. When `HARVEST_TEST_DATABASE_URL` is set, it is an admin URL and
//! each test gets a fresh database. Otherwise a testcontainer starts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

static DB_SEQ: AtomicU64 = AtomicU64::new(0);

const PROMISE: &str = "harvest.promise:approval";

/// Replaces the database name in `url` with `db`.
fn with_db_name(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// Returns the URL of a fresh, migrated database.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("promise_routes_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        (with_db_name(&admin_url, &db), None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "approvals",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
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

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("promise-routes-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

async fn post(
    app: &axum::Router,
    uri: &str,
    key: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(key) = key {
        req = req.header("idempotency-key", key);
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, json)
}

fn sws_body(workflow_id: &str, signal_name: &str, payload: &Value, key: Option<&str>) -> Value {
    let mut body = json!({
        "workflow_id": workflow_id,
        "start_input": {},
        "signal_name": signal_name,
        "signal_payload": payload,
    });
    if let Some(key) = key {
        body["idempotency_key"] = json!(key);
    }
    body
}

/// A mismatched key that an unrelated signal already used must not read as a
/// dedupe hit. The route refuses it, and the promise still settles once.
#[tokio::test]
async fn the_signal_route_refuses_a_promise_key_that_another_signal_used() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post(
        &app,
        "/workflows/approvals/signal-with-start",
        None,
        &sws_body("run-a", "bootstrap", &json!({}), None),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let exec = body["execution_id"].as_str().unwrap().to_string();
    let signal_uri = |name: &str| format!("/workflows/{exec}/signal/{name}");

    let (status, body) = post(&app, &signal_uri("other"), Some("shared-key"), &json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");

    let settlement = json!({"outcome": "resolved", "value": "ops"});
    let (status, body) = post(&app, &signal_uri(PROMISE), Some("shared-key"), &settlement).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a mismatched key must not report a dedupe success: {body}"
    );

    let (status, body) = post(&app, &signal_uri(PROMISE), None, &settlement).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["signal_delivered"], json!(true), "{body}");

    let (status, body) = post(&app, &signal_uri(PROMISE), None, &settlement).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(
        body["signal_delivered"],
        json!(false),
        "the second settlement is a no-op: {body}"
    );
}

/// Signal-with-start refuses a settlement that breaks the rules before its
/// committed-replay probe.
#[tokio::test]
async fn signal_with_start_refuses_a_promise_settlement_that_breaks_the_rules() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let uri = "/workflows/approvals/signal-with-start";

    let (status, body) = post(
        &app,
        uri,
        None,
        &sws_body("run-b", "other", &json!({}), Some("evt-1")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let settlement = json!({"outcome": "resolved", "value": 1});
    let (status, body) = post(
        &app,
        uri,
        None,
        &sws_body("run-b", PROMISE, &settlement, Some("evt-1")),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a mismatched key must not replay an unrelated signal: {body}"
    );

    let (status, body) = post(
        &app,
        uri,
        None,
        &sws_body("run-b", PROMISE, &json!({"value": 1}), None),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a malformed settlement is refused: {body}"
    );
}

/// A named promise belongs to one run. Over HTTP, a settlement for a new run
/// must not replay the settlement of an earlier run with the same workflow id.
/// The caller sends the key that the docs give, so the replay probe runs.
#[tokio::test]
async fn signal_with_start_settles_a_named_promise_in_a_new_run_over_http() {
    let (url, _container) = setup().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let uri = "/workflows/approvals/signal-with-start";
    let settlement = json!({"outcome": "resolved", "value": "ops"});

    let (status, first) = post(
        &app,
        uri,
        None,
        &sws_body("run-c", PROMISE, &settlement, Some(PROMISE)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    assert_eq!(first["signal_delivered"], json!(true), "{first}");
    let first_exec: ExecutionId = first["execution_id"].as_str().unwrap().parse().unwrap();

    // A finished run keeps its signal rows. Terminate deletes unconsumed ones,
    // so this test ends the run directly. No worker runs here.
    let mut conn = pool.get().await.expect("conn");
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = 'COMPLETED', completed_at = now() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(first_exec.as_uuid())
    .execute(&mut conn)
    .await
    .expect("complete run 1");
    drop(conn);

    let (status, second) = post(
        &app,
        uri,
        None,
        &sws_body("run-c", PROMISE, &settlement, Some(PROMISE)),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a new run must start: {second}"
    );
    assert_eq!(second["started_fresh"], json!(true), "{second}");
    assert_eq!(second["signal_delivered"], json!(true), "{second}");
    assert_ne!(second["execution_id"], first["execution_id"]);
}
