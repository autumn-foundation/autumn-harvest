//! The API rate limiter against real tokens (issue #1827).
//!
//! The tests mint scoped API tokens in a real Postgres. Set
//! `HARVEST_TEST_DATABASE_URL` to a migrated database and run with
//! `--test-threads=1`. Otherwise each test boots a testcontainers Postgres.
//!
//! - AC1: at 10 req/s, a burst of 100 starts from one token gets 429s. Another
//!   token is unaffected.
//! - AC2: each rejection is a metric sample. Sustained rejections write one
//!   `api.rate_limit_sustained` audit row with the token as the actor.

#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_harvest_plugin::api_rate_limit::{ApiRateLimit, BucketRate};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const START: &str = "/workflows/billing/start";

/// Records each `record_api_rate_limited` call.
#[derive(Default)]
struct CapturingMetrics {
    rate_limited: Mutex<Vec<(String, String)>>,
}

impl CapturingMetrics {
    fn rate_limited(&self) -> Vec<(String, String)> {
        self.rate_limited.lock().unwrap().clone()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_api_rate_limited(&self, route_class: &str, client_kind: &str) {
        self.rate_limited
            .lock()
            .unwrap()
            .push((route_class.to_owned(), client_kind.to_owned()));
    }
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
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
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn api_state(pool: &DbPool, metrics: Arc<CapturingMetrics>) -> HarvestApiState {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("rate-limit-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// The minting app: an embedder boundary and tokens, with no limiter.
fn minting_app(pool: &DbPool) -> axum::Router {
    let state = api_state(pool, Arc::default());
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .mount(harvest_api_router(state.clone()), &state)
}

/// Standalone-token mode with the limiter at 10 req/s on both classes.
fn limited_app(pool: &DbPool, metrics: Arc<CapturingMetrics>, limit: ApiRateLimit) -> axum::Router {
    let state = api_state(pool, metrics);
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_rate_limit(limit)
        .mount(harvest_api_router(state.clone()), &state)
}

const fn ten_per_second() -> ApiRateLimit {
    ApiRateLimit::new(BucketRate::per_second(10), BucketRate::per_second(10))
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_api_tokens",
        "DELETE FROM harvest_audit_log",
    ] {
        diesel::sql_query(stmt).execute(conn).await.unwrap();
    }
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    bearer: Option<&str>,
) -> (StatusCode, Option<String>, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-harvest-admin", "true");
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(b) = bearer {
        builder = builder.header("authorization", format!("Bearer {b}"));
    }
    let request = builder
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.expect("request");
    let status = response.status();
    let retry_after = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, retry_after, json)
}

/// Mint a `mutate` token. Return its secret and id.
async fn mint(app: &axum::Router, name: &str) -> (String, String) {
    let body = json!({ "name": name, "scope": "mutate" });
    let (status, _, created) = send(app, "POST", "/admin/tokens", Some(body), None).await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201: {created:?}");
    let secret = created["secret"].as_str().unwrap().to_owned();
    let (_, _, list) = send(app, "GET", "/admin/tokens", None, None).await;
    let id = list
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == name)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    (secret, id)
}

/// Send `n` starts with `bearer`. Return the 429 count and each `Retry-After`.
async fn burst(app: &axum::Router, bearer: &str, n: usize) -> (usize, Vec<String>) {
    let mut limited = 0;
    let mut retry_after = Vec::new();
    for _ in 0..n {
        let (status, header, _) = send(app, "POST", START, Some(json!({})), Some(bearer)).await;
        if status == StatusCode::TOO_MANY_REQUESTS {
            limited += 1;
            retry_after.push(header.expect("a 429 carries Retry-After"));
        }
    }
    (limited, retry_after)
}

async fn sustained_rows(conn: &mut AsyncPgConnection) -> Vec<(String, String)> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        actor: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        error_summary: String,
    }
    diesel::sql_query(
        "SELECT actor, COALESCE(error_summary, '') AS error_summary FROM harvest_audit_log \
         WHERE operation = 'api.rate_limit_sustained' ORDER BY occurred_at",
    )
    .load::<Row>(conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.actor, r.error_summary))
    .collect()
}

/// Poll for the audit row. The limiter writes it off the request path.
async fn await_sustained_rows(conn: &mut AsyncPgConnection, want: usize) -> Vec<(String, String)> {
    for _ in 0..100 {
        let rows = sustained_rows(conn).await;
        if rows.len() >= want {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sustained_rows(conn).await
}

/// AC1: with the limiter at 10 req/s, a burst of 100 starts from one token
/// gets 429s, while another token is unaffected.
#[tokio::test]
async fn a_burst_from_one_token_is_limited_and_another_token_is_not() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let minting = minting_app(&pool);
    let (noisy, _) = mint(&minting, "noisy").await;
    let (quiet, _) = mint(&minting, "quiet").await;
    let metrics = Arc::new(CapturingMetrics::default());
    let app = limited_app(&pool, Arc::clone(&metrics), ten_per_second());

    let (limited, retry_after) = burst(&app, &noisy, 100).await;
    let (quiet_limited, _) = burst(&app, &quiet, 10).await;

    // The bucket refills during the burst, so a slow runner sees fewer 429s.
    assert!(
        (50..=90).contains(&limited),
        "about 90 of 100 starts are refused, got {limited}"
    );
    assert!(
        retry_after.iter().all(|v| v.parse::<u64>().unwrap() >= 1),
        "Retry-After is whole seconds, never zero: {retry_after:?}"
    );
    assert_eq!(quiet_limited, 0, "another token keeps its own bucket");

    // AC2: one metric sample per rejection, labelled by class and key kind.
    let samples = metrics.rate_limited();
    assert_eq!(samples.len(), limited);
    assert!(
        samples
            .iter()
            .all(|(class, kind)| class == "mutating" && kind == "token"),
        "{samples:?}"
    );
}

/// AC2: sustained rejections write one audit row per bucket per window. The
/// actor is the token, never its secret.
#[tokio::test]
async fn sustained_rejections_write_one_audit_row() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;
    let minting = minting_app(&pool);
    let (noisy, noisy_id) = mint(&minting, "noisy").await;
    let (quiet, _) = mint(&minting, "quiet").await;
    let limit = ten_per_second().with_sustained_audit(20, Duration::from_secs(60));
    let app = limited_app(&pool, Arc::default(), limit);

    let (limited, _) = burst(&app, &noisy, 100).await;
    // At most ten rejections stay under the threshold of twenty.
    burst(&app, &quiet, 20).await;
    let rows = await_sustained_rows(&mut conn, 1).await;
    // Give a stray second write time to land before the count is final.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rows_after = sustained_rows(&mut conn).await;

    assert!(limited >= 20, "the burst crosses the threshold: {limited}");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows_after.len(), 1, "one row per bucket per window");
    let (actor, summary) = &rows[0];
    assert_eq!(actor, &format!("token:{noisy_id}"));
    assert!(
        !summary.contains(&noisy),
        "the secret never reaches the audit"
    );
    assert!(summary.contains("mutating"), "{summary}");
}
