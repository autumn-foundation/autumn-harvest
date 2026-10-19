//! Durable routes end to end, with testcontainers.
//!
//! Each test drives one plugin-wired app against a real Postgres. One route,
//! `/durable`, reads its call settings from test headers, so each test can
//! pick the workflow, the scope, the wait and the status mapping.
//!
//! Requires Docker. Each test uses a multi-thread runtime, because
//! `TestApp::plugin` blocks on plugin startup.

#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use autumn_harvest::event_batch::BatchPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::{ExecutionId, HarvestError, WorkflowHandleClient, WorkflowInfo};
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::durable_route::{DurableOutcome, DurableRoute, REPLAYED_HEADER};
use autumn_web::prelude::*;
use autumn_web::reexports::axum;
use autumn_web::test::{TestApp, TestClient, TestResponse};
use axum::http::{HeaderMap, StatusCode};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const EXEC_HEADER: &str = "x-harvest-execution-id";

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// The tags that `dr_record` saw, one entry for each run of the activity.
static SIDE_EFFECTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn side_effects(tag: &str) -> usize {
    SIDE_EFFECTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|seen| *seen == tag)
        .count()
}

/// Records one side effect for `tag` and returns how many it has seen.
#[activity]
async fn dr_record(_ctx: &ActivityContext, tag: String) -> Result<u64, String> {
    let mut seen = SIDE_EFFECTS.lock().unwrap_or_else(PoisonError::into_inner);
    seen.push(tag.clone());
    Ok(seen.iter().filter(|t| **t == tag).count() as u64)
}

/// Runs one side effect and echoes the principal and the body.
#[workflow]
async fn dr_order(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let tag = input["body"]["tag"].as_str().unwrap_or("none").to_owned();
    let count = ctx
        .execute_activity_raw("dr_record", json!(tag), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({"user": input["user"], "body": input["body"], "count": count}))
}

/// Waits for the `go` signal, then echoes the body and the signal.
#[workflow]
async fn dr_parked(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let go = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(json!({"body": input["body"], "go": go}))
}

/// Fails with a typed failure whose class the body names. With
/// `body.no_details`, the failure has no details.
#[workflow]
async fn dr_typed_failure(_ctx: &WorkflowContext, input: Value) -> Result<Value, WorkflowFailure> {
    let error_type = input["body"]["error_type"]
        .as_str()
        .unwrap_or("OutOfStock")
        .to_owned();
    let failure = WorkflowFailure::new(error_type, "secret workflow message");
    if input["body"]["no_details"] == json!(true) {
        return Err(failure);
    }
    Err(failure.with_details(json!({"sku": "A-1"})))
}

/// Fails with a plain string.
#[workflow]
async fn dr_untyped_failure(_ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    Err("secret workflow message".to_owned())
}

/// Fails on every attempt. The plugin gives it two attempts.
#[workflow]
async fn dr_always_fails(_ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    Err("secret workflow message".to_owned())
}

/// Parks until its one-second execution timeout ends it. With `body.fast`,
/// it completes at once instead.
#[workflow(execution_timeout = "1s")]
async fn dr_slow(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    if input["body"]["fast"] == json!(true) {
        return Ok(json!({"fast": true}));
    }
    let _ = ctx.wait_for_signal("never").await;
    Ok(Value::Null)
}

/// Continues as new `body.hops` times, then completes.
#[workflow]
async fn dr_chain(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let target = input
        .get("target")
        .and_then(Value::as_u64)
        .or_else(|| input["body"]["hops"].as_u64())
        .unwrap_or(2);
    let hop = input.get("hop").and_then(Value::as_u64).unwrap_or(0);
    if hop < target {
        ctx.continue_as_new(json!({"hop": hop + 1, "target": target}))
            .await
            .map_err(|e| e.to_string())?;
        return Ok(json!({"carry": hop}));
    }
    Ok(json!({"final": hop}))
}

/// Fails once, then continues as `dr_chain_end` on the retry attempt.
///
/// The retry attempt holds the continue-as-new event. The successor has
/// another workflow type. The route must read the live attempt and hop by
/// the event.
#[workflow]
async fn dr_retry_then_switch(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    let tag = input["body"]["tag"].as_str().unwrap_or("none").to_owned();
    let count = ctx
        .execute_activity_raw("dr_record", json!(tag), "default")
        .await
        .map_err(|e| e.to_string())?;
    if count == json!(1) {
        return Err("first attempt fails".to_owned());
    }
    ctx.continue_as_new_as_type("dr_chain_end", json!({"from": "retry"}))
        .await
        .map_err(|e| e.to_string())?;
    Ok(Value::Null)
}

/// Continues as new into `dr_switch_mid`: the first change of type.
#[workflow]
async fn dr_switch_start(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.continue_as_new_as_type("dr_switch_mid", json!({}))
        .await
        .map_err(|e| e.to_string())?;
    Ok(Value::Null)
}

/// Continues as new into `dr_chain_end`: the second change of type.
#[workflow]
async fn dr_switch_mid(ctx: &WorkflowContext, _input: Value) -> Result<Value, String> {
    ctx.continue_as_new_as_type("dr_chain_end", json!({"from": "mid"}))
        .await
        .map_err(|e| e.to_string())?;
    Ok(Value::Null)
}

/// The end of a cross-type continue-as-new chain.
#[workflow]
async fn dr_chain_end(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(json!({"final": input["from"]}))
}

/// A debounce workflow. A transactional start refuses it.
#[workflow(debounce(key = "input.body.tag", window = "10s"))]
async fn dr_debounced(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

/// Accepts an input up to 4 MiB, over the 2 MiB deployment cap.
#[workflow(max_input_bytes = "4MiB")]
async fn dr_big(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(json!({"pad": input["body"]["pad"].as_str().map_or(0, str::len)}))
}

/// `dr_order` with a batch policy. A transactional start refuses it.
fn dr_batched_info() -> WorkflowInfo {
    let mut info = __autumn_workflow_info_dr_order();
    info.name = "dr_batched";
    info.batch = Some(BatchPolicy {
        key_expr: "input.body.tag".to_owned(),
        max_size: 10,
        max_wait: Duration::from_secs(10),
    });
    info
}

/// `dr_order` with a throttle policy. A transactional start refuses it.
fn dr_throttled_info() -> WorkflowInfo {
    let mut info = __autumn_workflow_info_dr_order();
    info.name = "dr_throttled";
    info.throttle = Some(
        ThrottlePolicy::from_rate_str("100/m", Some(10.0), Some("input.body.tag"), None)
            .expect("valid rate"),
    );
    info
}

/// Publishes an input schema: `body.sku` must be a string.
#[workflow]
async fn dr_schema(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input["body"].clone())
}

fn dr_schema_input() -> Value {
    json!({
        "type": "object",
        "required": ["body"],
        "properties": {
            "body": {
                "type": "object",
                "required": ["sku"],
                "properties": {"sku": {"type": "string"}}
            }
        }
    })
}

// ── Routes ────────────────────────────────────────────────────────────────────

/// The `Idempotency-Key` of each request that reached the route handler.
static HANDLER_ENTRIES: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn handler_entries(key: &str) -> usize {
    HANDLER_ENTRIES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|seen| *seen == key)
        .count()
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The durable test route. The test headers pick the call settings.
#[post("/durable")]
async fn durable(
    durable: DurableRoute,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> DurableOutcome {
    if let Some(key) = header(&headers, "idempotency-key") {
        HANDLER_ENTRIES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(key.to_owned());
    }
    let user = header(&headers, "x-user").map(str::to_owned);
    let workflow = header(&headers, "x-workflow")
        .unwrap_or("dr_order")
        .to_owned();
    // `x-raw-body` makes the body the whole input.
    let input = if headers.contains_key("x-raw-body") {
        body
    } else {
        json!({"user": user, "body": body})
    };
    let scoped = match &user {
        Some(user) => durable.scope(user),
        None => durable.unscoped(),
    };
    let mut call = scoped.call(&workflow, input);
    if let Some(ms) = header(&headers, "x-wait-ms").and_then(|v| v.parse().ok()) {
        call = call.wait(Duration::from_millis(ms));
    }
    if let Some(status) = header(&headers, "x-success").and_then(|v| v.parse().ok()) {
        call = call.success_status(StatusCode::from_u16(status).expect("status"));
    }
    if let Some((error_type, status)) = header(&headers, "x-map").and_then(|v| v.split_once('=')) {
        let status = StatusCode::from_u16(status.parse().expect("status")).expect("status");
        call = call.failure_status(error_type, status);
    }
    if headers.contains_key("x-expose") {
        call = call.expose_execution_id();
    }
    call.send().await
}

/// Scopes the request to the tenant in `x-tenant`, as Autumn's tenancy
/// middleware does.
async fn tenant_scope(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let tenant = request
        .headers()
        .get("x-tenant")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none")
        .to_owned();
    autumn_web::tenancy::with_tenant(tenant, next.run(request)).await
}

// ── Harness ───────────────────────────────────────────────────────────────────

/// A migrated Postgres 16 container for one test.
struct TestPg {
    _container: ContainerAsync<Postgres>,
    url: String,
    pool: Pool<AsyncPgConnection>,
}

async fn setup_db() -> TestPg {
    setup_db_with_pool(8).await
}

async fn setup_db_with_pool(max_size: usize) -> TestPg {
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    // A short acquire bound makes a starved pool fail fast, so a request that
    // held its connection through the wait would answer 503.
    let pool = Pool::builder(manager)
        .max_size(max_size)
        .wait_timeout(Some(Duration::from_secs(2)))
        .runtime(deadpool::Runtime::Tokio1)
        .build()
        .expect("failed to build pool");
    TestPg {
        _container: container,
        url,
        pool,
    }
}

fn base_plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_dr_order(),
            __autumn_workflow_info_dr_parked(),
            __autumn_workflow_info_dr_typed_failure(),
            __autumn_workflow_info_dr_untyped_failure(),
            __autumn_workflow_info_dr_always_fails().with_retry_policy(
                autumn_harvest::policy::RetryPolicy::exponential(2, Duration::from_millis(100)),
            ),
            __autumn_workflow_info_dr_slow(),
            __autumn_workflow_info_dr_chain(),
            __autumn_workflow_info_dr_retry_then_switch().with_retry_policy(
                autumn_harvest::policy::RetryPolicy::exponential(2, Duration::from_millis(100)),
            ),
            __autumn_workflow_info_dr_chain_end(),
            __autumn_workflow_info_dr_switch_start(),
            __autumn_workflow_info_dr_switch_mid(),
            __autumn_workflow_info_dr_debounced(),
            dr_batched_info(),
            dr_throttled_info(),
            __autumn_workflow_info_dr_big(),
            __autumn_workflow_info_dr_schema().with_input_schema_fn(dr_schema_input),
        ])
        .activities(activities![dr_record])
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .allow_unauthenticated_mutations()
}

fn app_config(db: &TestPg) -> autumn_web::config::AutumnConfig {
    autumn_web::config::AutumnConfig {
        profile: Some("test".into()),
        security: autumn_web::security::SecurityConfig {
            csrf: autumn_web::security::CsrfConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        database: autumn_web::config::DatabaseConfig {
            url: Some(db.url.clone()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn app_with(db: &TestPg, plugin: HarvestPlugin) -> TestApp {
    let mut route = __autumn_route_info_durable();
    let mut tenant_route = __autumn_route_info_durable();
    tenant_route.path = "/tenant/durable";
    tenant_route.handler = tenant_route
        .handler
        .layer(axum::middleware::from_fn(tenant_scope));
    route.name = "durable";
    tenant_route.name = "tenant_durable";
    TestApp::new()
        .config(app_config(db))
        .routes(vec![route, tenant_route])
        .plugin(plugin)
        .with_db(db.pool.clone())
}

async fn build_app(db: &TestPg) -> TestClient {
    app_with(db, base_plugin()).build()
}

/// One request to `/durable` with `key` and the test headers in `extra`.
async fn post(
    client: &TestClient,
    key: Option<&str>,
    extra: &[(&str, &str)],
    body: &Value,
) -> TestResponse {
    post_to(client, "/durable", key, extra, body).await
}

async fn post_to(
    client: &TestClient,
    path: &str,
    key: Option<&str>,
    extra: &[(&str, &str)],
    body: &Value,
) -> TestResponse {
    let mut request = client.post(path);
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    for (name, value) in extra {
        request = request.header(name, value);
    }
    request.json(body).send().await
}

fn exec_id_of(response: &TestResponse) -> ExecutionId {
    response
        .header(EXEC_HEADER)
        .expect("execution id header")
        .parse()
        .expect("execution id")
}

#[derive(QueryableByName)]
struct Memo {
    #[diesel(sql_type = Text)]
    memo: String,
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count_runs(db: &TestPg, workflow: &str) -> i64 {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT count(DISTINCT workflow_id)::bigint AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1",
    )
    .bind::<Text, _>(workflow)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count runs")
    .n
}

async fn signal(client: &TestClient, exec_id: ExecutionId, payload: Value) {
    let pool = client
        .state()
        .extension::<autumn_harvest_plugin::HarvestDbPool>()
        .expect("harvest pool");
    let mut conn = pool.pool_for_execution(exec_id).get().await.expect("conn");
    autumn_harvest::signal::send_signal(&mut conn, exec_id, "go", payload)
        .await
        .expect("signal");
}

fn problem_type(body: &Value) -> &str {
    body["type"].as_str().unwrap_or_default()
}

fn slug_type(slug: &str) -> String {
    format!("urn:autumn-harvest:durable-route:{slug}")
}

/// Polls `/durable` with the same request until the answer is not pending.
async fn until_done(
    client: &TestClient,
    key: &str,
    extra: &[(&str, &str)],
    body: &Value,
) -> TestResponse {
    for _ in 0..100 {
        let response = post(client, Some(key), extra, body).await;
        if response.status != StatusCode::ACCEPTED && response.status != StatusCode::CONFLICT {
            return response;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the run did not end after 100 tries");
}

// ── Completed runs and dedupe ─────────────────────────────────────────────────

// AC-16
#[tokio::test(flavor = "multi_thread")]
async fn completed_run_returns_200_with_output() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac16"});
    let response = post(&client, Some("key-ac16"), &[("x-user", "ada")], &body).await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    assert_eq!(response.header("content-type"), Some("application/json"));
    assert_eq!(response.header(REPLAYED_HEADER), None);
    assert_eq!(response.header(EXEC_HEADER), None);
    assert_eq!(
        response.json::<Value>(),
        json!({"user": "ada", "body": body, "count": 1})
    );
}

// AC-17
#[tokio::test(flavor = "multi_thread")]
async fn retry_with_same_key_runs_workflow_once() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac17"});
    let first = post(&client, Some("key-ac17"), &[("x-user", "ada")], &body).await;
    let second = post(&client, Some("key-ac17"), &[("x-user", "ada")], &body).await;
    assert_eq!(first.status.as_u16(), 200);
    assert_eq!(second.status.as_u16(), 200);
    assert_eq!(first.json::<Value>(), second.json::<Value>());
    assert_eq!(first.header(REPLAYED_HEADER), None);
    assert_eq!(second.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(count_runs(&db, "dr_order").await, 1);
    assert_eq!(side_effects("ac17"), 1);
}

// AC-18
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_duplicates_without_layer_share_one_run() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac18"});
    let requests = (0..20).map(|_| post(&client, Some("key-ac18"), &[("x-user", "ada")], &body));
    let responses = futures::future::join_all(requests).await;
    let first = responses[0].json::<Value>();
    for response in &responses {
        assert_eq!(response.status.as_u16(), 200, "{}", response.text());
        assert_eq!(response.json::<Value>(), first);
    }
    assert_eq!(count_runs(&db, "dr_order").await, 1);
    assert_eq!(side_effects("ac18"), 1);
}

// AC-19
#[tokio::test(flavor = "multi_thread")]
async fn principals_with_same_key_get_separate_runs() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac19"});
    let ada = post(&client, Some("key-ac19"), &[("x-user", "ada")], &body).await;
    let bob = post(&client, Some("key-ac19"), &[("x-user", "bob")], &body).await;
    assert_eq!(ada.status.as_u16(), 200);
    assert_eq!(bob.status.as_u16(), 200);
    assert_eq!(ada.json::<Value>()["user"], "ada");
    assert_eq!(bob.json::<Value>()["user"], "bob");
    assert_eq!(bob.header(REPLAYED_HEADER), None);
    assert_eq!(count_runs(&db, "dr_order").await, 2);
}

// AC-20
#[tokio::test(flavor = "multi_thread")]
async fn tenants_with_same_key_get_separate_runs() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac20"});
    let path = "/tenant/durable";
    let a = post_to(
        &client,
        path,
        Some("key-ac20"),
        &[("x-user", "ada"), ("x-tenant", "a")],
        &body,
    )
    .await;
    let b = post_to(
        &client,
        path,
        Some("key-ac20"),
        &[("x-user", "ada"), ("x-tenant", "b")],
        &body,
    )
    .await;
    let a_again = post_to(
        &client,
        path,
        Some("key-ac20"),
        &[("x-user", "ada"), ("x-tenant", "a")],
        &body,
    )
    .await;
    assert_eq!(a.status.as_u16(), 200, "{}", a.text());
    assert_eq!(b.status.as_u16(), 200, "{}", b.text());
    assert_eq!(b.header(REPLAYED_HEADER), None);
    assert_eq!(a_again.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(count_runs(&db, "dr_order").await, 2);
}

/// How many tables hold `needle` in any column of any row.
///
/// Each table counts its own matches on the row cast to text, so only an
/// integer goes through XML.
async fn tables_containing(db: &TestPg, needle: &str) -> i64 {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT count(*)::bigint AS n FROM information_schema.tables t \
         WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE' \
           AND (xpath('/row/c/text()', query_to_xml(format( \
                 'SELECT count(*) AS c FROM %I.%I r WHERE r::text LIKE %L', \
                 t.table_schema, t.table_name, '%' || $1 || '%'), false, true, '')))[1] \
               ::text::bigint > 0",
    )
    .bind::<Text, _>(needle)
    .get_result::<Count>(&mut conn)
    .await
    .expect("scan")
    .n
}

// AC-21
#[tokio::test(flavor = "multi_thread")]
async fn raw_key_never_reaches_the_database() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let key = "raw-key-7f3a9c";
    let response = post(
        &client,
        Some(key),
        &[("x-user", "ada")],
        &json!({"tag": "ac21"}),
    )
    .await;
    assert_eq!(response.status.as_u16(), 200);
    assert_eq!(
        tables_containing(&db, key).await,
        0,
        "the raw key is in a table"
    );
    // The same scan finds the digest claim, so it can see a stored value.
    assert!(tables_containing(&db, "durable-route:v1:").await >= 1);
}

// AC-22
#[tokio::test(flavor = "multi_thread")]
async fn changed_body_with_same_key_returns_422_without_new_run() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let first = post(
        &client,
        Some("key-ac22"),
        &[("x-user", "ada")],
        &json!({"tag": "ac22"}),
    )
    .await;
    assert_eq!(first.status.as_u16(), 200);
    let changed = post(
        &client,
        Some("key-ac22"),
        &[("x-user", "ada")],
        &json!({"tag": "ac22-x"}),
    )
    .await;
    assert_eq!(changed.status.as_u16(), 422, "{}", changed.text());
    assert_eq!(
        problem_type(&changed.json::<Value>()),
        slug_type("idempotency-key-reused")
    );
    assert_eq!(changed.header(REPLAYED_HEADER), None);
    assert_eq!(count_runs(&db, "dr_order").await, 1);
    assert_eq!(side_effects("ac22-x"), 0);
    let again = post(
        &client,
        Some("key-ac22"),
        &[("x-user", "ada")],
        &json!({"tag": "ac22"}),
    )
    .await;
    assert_eq!(again.json::<Value>(), first.json::<Value>());
}

// AC-23
#[tokio::test(flavor = "multi_thread")]
async fn same_body_after_claim_window_attaches_to_original_run() {
    let db = setup_db().await;
    let plugin = base_plugin().start_idempotency_window(Duration::from_secs(1));
    let client = app_with(&db, plugin).build();
    let extra = [("x-user", "ada"), ("x-expose", "1")];
    let same = json!({"tag": "ac23-same"});
    let first = post(&client, Some("key-ac23-a"), &extra, &same).await;
    assert_eq!(first.status.as_u16(), 200);
    let other = post(
        &client,
        Some("key-ac23-b"),
        &extra,
        &json!({"tag": "ac23-b"}),
    )
    .await;
    assert_eq!(other.status.as_u16(), 200);
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // The same body attaches to the original run, and renews the claim.
    let again = post(&client, Some("key-ac23-a"), &extra, &same).await;
    assert_eq!(again.status.as_u16(), 200, "{}", again.text());
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(exec_id_of(&again), exec_id_of(&first));
    assert_eq!(side_effects("ac23-same"), 1);
    let changed = post(
        &client,
        Some("key-ac23-a"),
        &extra,
        &json!({"tag": "ac23-x"}),
    )
    .await;
    assert_eq!(
        changed.status.as_u16(),
        422,
        "the renewed claim binds the body again"
    );

    // Another body under an expired key starts a new run.
    let fresh = post(
        &client,
        Some("key-ac23-b"),
        &extra,
        &json!({"tag": "ac23-c"}),
    )
    .await;
    assert_eq!(fresh.status.as_u16(), 200, "{}", fresh.text());
    assert_eq!(fresh.header(REPLAYED_HEADER), None);
    assert_ne!(exec_id_of(&fresh), exec_id_of(&other));
    assert_eq!(count_runs(&db, "dr_order").await, 3);
}

// ── Key, input and setup problems ─────────────────────────────────────────────

// AC-24
#[tokio::test(flavor = "multi_thread")]
async fn missing_or_invalid_key_returns_400_and_starts_nothing() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac24"});
    let missing = post(&client, None, &[("x-user", "ada")], &body).await;
    assert_eq!(missing.status.as_u16(), 400);
    assert_eq!(
        missing.header("content-type"),
        Some("application/problem+json")
    );
    assert_eq!(
        problem_type(&missing.json::<Value>()),
        slug_type("idempotency-key-missing")
    );
    let twice = client
        .post("/durable")
        .header("idempotency-key", "a")
        .header("idempotency-key", "b")
        .header("x-user", "ada")
        .json(&body)
        .send()
        .await;
    assert_eq!(twice.status.as_u16(), 400);
    assert_eq!(
        problem_type(&twice.json::<Value>()),
        slug_type("idempotency-key-invalid")
    );
    assert_eq!(count_runs(&db, "dr_order").await, 0);
    assert_eq!(side_effects("ac24"), 0);
}

// AC-25
#[tokio::test(flavor = "multi_thread")]
async fn schema_violation_returns_422_and_key_stays_usable() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [("x-user", "ada"), ("x-workflow", "dr_schema")];
    let bad = post(&client, Some("key-ac25"), &extra, &json!({"sku": 5})).await;
    assert_eq!(bad.status.as_u16(), 422, "{}", bad.text());
    let text = bad.text();
    assert!(!text.contains("sku"), "no field path in the body: {text}");
    // The body holds the fixed problem members and nothing from the schema.
    let problem = bad.json::<Value>();
    let mut members: Vec<&str> = problem
        .as_object()
        .expect("problem object")
        .keys()
        .map(String::as_str)
        .collect();
    members.sort_unstable();
    assert_eq!(members, ["code", "detail", "status", "title", "type"]);
    assert_eq!(problem_type(&problem), slug_type("input-invalid"));
    assert_eq!(problem["code"], "harvest.durable_route.input_invalid");
    assert_eq!(problem["status"], 422);
    for leak in ["string", "number", "violation", "expected"] {
        assert!(!text.contains(leak), "{leak} in {text}");
    }
    assert_eq!(count_runs(&db, "dr_schema").await, 0);
    let good = post(&client, Some("key-ac25"), &extra, &json!({"sku": "A-1"})).await;
    assert_eq!(good.status.as_u16(), 200, "{}", good.text());
    assert_eq!(good.json::<Value>(), json!({"sku": "A-1"}));
}

// AC-26
#[tokio::test(flavor = "multi_thread")]
async fn oversized_input_returns_413_and_starts_nothing() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    // Over the 2 MiB deployment-wide input cap.
    let big = json!({"tag": "ac26", "pad": "x".repeat(2_200_000)});
    let response = post(&client, Some("key-ac26"), &[("x-user", "ada")], &big).await;
    assert_eq!(response.status.as_u16(), 413, "{}", response.status);
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("input-too-large")
    );
    assert_eq!(count_runs(&db, "dr_order").await, 0);
}

// AC-27
#[tokio::test(flavor = "multi_thread")]
async fn unusable_workflow_returns_500_misconfigured() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    for (key, workflow) in [
        ("key-ac27-a", "dr_not_registered"),
        ("key-ac27-b", "dr_debounced"),
        ("key-ac27-c", "dr_batched"),
        ("key-ac27-d", "dr_throttled"),
    ] {
        let response = post(
            &client,
            Some(key),
            &[("x-user", "ada"), ("x-workflow", workflow)],
            &json!({"tag": "ac27"}),
        )
        .await;
        assert_eq!(
            response.status.as_u16(),
            500,
            "{workflow}: {}",
            response.text()
        );
        let body = response.json::<Value>();
        assert_eq!(problem_type(&body), slug_type("misconfigured"));
        assert!(
            !body.to_string().contains(workflow),
            "no internal text: {body}"
        );
    }
}

// ── Pending runs ──────────────────────────────────────────────────────────────

// AC-28
#[tokio::test(flavor = "multi_thread")]
async fn pending_then_retry_returns_result() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac28"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "300"),
    ];
    let pending = post(&client, Some("key-ac28"), &extra, &body).await;
    assert_eq!(pending.status.as_u16(), 202, "{}", pending.text());
    assert_eq!(pending.header("retry-after"), Some("1"));
    assert_eq!(pending.json::<Value>(), json!({"state": "pending"}));

    let exposed = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
    ];
    let again = post(&client, Some("key-ac28"), &exposed, &body).await;
    assert_eq!(again.status.as_u16(), 202);
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    signal(&client, exec_id_of(&again), json!("now")).await;

    let done = until_done(&client, "key-ac28", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(done.json::<Value>(), json!({"body": body, "go": "now"}));
    assert_eq!(done.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
}

// AC-29
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_layer_pending_is_409_and_not_cached() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin()).idempotent().build();
    let body = json!({"tag": "ac29"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "200"),
        ("x-expose", "1"),
    ];
    let pending = post(&client, Some("key-ac29"), &extra, &body).await;
    assert_eq!(pending.status.as_u16(), 409, "{}", pending.text());
    assert_eq!(pending.header("retry-after"), Some("1"));
    assert_eq!(
        pending.header("content-type"),
        Some("application/problem+json")
    );
    let problem = pending.json::<Value>();
    assert_eq!(problem_type(&problem), slug_type("pending"));
    assert_eq!(problem["state"], "pending");
    signal(&client, exec_id_of(&pending), json!("now")).await;

    let done = until_done(&client, "key-ac29", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(done.json::<Value>()["go"], "now");
    // Autumn now replays its cached copy of the 200. The handler does not run.
    let entries = handler_entries("key-ac29");
    let cached = post(&client, Some("key-ac29"), &extra, &body).await;
    assert_eq!(
        handler_entries("key-ac29"),
        entries,
        "Autumn answered from its cache"
    );
    assert_eq!(cached.status.as_u16(), 200);
    assert_eq!(cached.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(cached.json::<Value>(), done.json::<Value>());
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
}

// AC-30
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_layer_answers_cached_mismatch() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin()).idempotent().build();
    let extra = [("x-user", "ada")];
    let first = post(&client, Some("key-ac30"), &extra, &json!({"tag": "ac30"})).await;
    assert_eq!(first.status.as_u16(), 200);
    let entries = handler_entries("key-ac30");
    let changed = post(&client, Some("key-ac30"), &extra, &json!({"tag": "ac30-x"})).await;
    assert_eq!(changed.status.as_u16(), 422);
    // Autumn's own answer, not the route's problem body.
    assert_eq!(
        changed.text(),
        "idempotency key reused with different payload"
    );
    assert_ne!(
        changed.header("content-type"),
        Some("application/problem+json")
    );
    assert_eq!(
        handler_entries("key-ac30"),
        entries,
        "the route did not run"
    );
    assert_eq!(count_runs(&db, "dr_order").await, 1);
    assert_eq!(side_effects("ac30-x"), 0);
}

// AC-31
#[tokio::test(flavor = "multi_thread")]
async fn wait_is_clamped_below_request_timeout() {
    let db = setup_db().await;
    let mut config = app_config(&db);
    config.server.timeouts.request_timeout_ms = Some(6_000);
    let mut route = __autumn_route_info_durable();
    route.name = "durable";
    let client = TestApp::new()
        .config(config)
        .routes(vec![route])
        .plugin(base_plugin())
        .with_db(db.pool.clone())
        .idempotent()
        .build();
    let body = json!({"tag": "ac31"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "30000"),
        ("x-expose", "1"),
    ];
    let started = Instant::now();
    let pending = post(&client, Some("key-ac31"), &extra, &body).await;
    let elapsed = started.elapsed();
    assert_eq!(pending.status.as_u16(), 409, "{}", pending.text());
    // Under the layer the route counts half of the 6 s deadline, less 2 s of
    // headroom. It waits about 1 s, not 4 s.
    assert!(
        elapsed < Duration::from_millis(2_500),
        "answered in {elapsed:?}"
    );
    signal(&client, exec_id_of(&pending), json!("now")).await;
    // Autumn holds no in-flight lock, so the retry reaches the route.
    let done = until_done(&client, "key-ac31", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
}

// AC-32
#[tokio::test(flavor = "multi_thread")]
async fn waiter_cap_answers_pending_at_once() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin().durable_route_max_waiters(1)).build();
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "4000"),
    ];
    let body_a = json!({"tag": "a"});
    let body_b = json!({"tag": "b"});
    let holder = async {
        let started = Instant::now();
        let response = post(&client, Some("key-ac32-a"), &extra, &body_a).await;
        (response, started.elapsed())
    };
    let second = async {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let started = Instant::now();
        let response = post(&client, Some("key-ac32-b"), &extra, &body_b).await;
        (response, started.elapsed())
    };
    let ((holder, holder_elapsed), (second, elapsed)) = tokio::join!(holder, second);
    // The holder took the only permit and waited out its budget.
    assert_eq!(holder.status.as_u16(), 202, "{}", holder.text());
    assert!(
        holder_elapsed >= Duration::from_millis(3_500),
        "held {holder_elapsed:?}"
    );
    // The second request found the cap full and did not wait.
    assert_eq!(second.status.as_u16(), 202, "{}", second.text());
    assert!(
        elapsed < Duration::from_millis(1_000),
        "answered in {elapsed:?}"
    );
}

// AC-33
#[tokio::test(flavor = "multi_thread")]
async fn completed_replay_needs_no_waiter_permit() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin().durable_route_max_waiters(0)).build();
    let body = json!({"tag": "ac33"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-expose", "1"),
    ];
    // With no permit, the default 5 s wait does not happen.
    let started = Instant::now();
    let pending = post(&client, Some("key-ac33"), &extra, &body).await;
    assert_eq!(pending.status.as_u16(), 202, "{}", pending.text());
    assert!(started.elapsed() < Duration::from_secs(2), "it waited");
    signal(&client, exec_id_of(&pending), json!("now")).await;
    // A completed run answers from the first read, which needs no permit.
    let done = until_done(&client, "key-ac33", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(done.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(done.json::<Value>()["go"], "now");
}

// AC-34
#[tokio::test(flavor = "multi_thread")]
async fn dropped_request_still_commits_and_retry_attaches() {
    use diesel_async::SimpleAsyncConnection as _;
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac34"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "10000"),
    ];
    // The lock lets the claim probe read but blocks the claim upsert, so the
    // request is dropped inside the start transaction.
    let mut blocker = db.pool.get().await.expect("blocker");
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_start_idempotency IN SHARE ROW EXCLUSIVE MODE")
        .await
        .expect("lock");
    let dropped = tokio::time::timeout(
        Duration::from_millis(500),
        post(&client, Some("key-ac34"), &extra, &body),
    )
    .await;
    assert!(
        dropped.is_err(),
        "the request must still be in its start when dropped"
    );
    assert_eq!(count_runs(&db, "dr_parked").await, 0);
    blocker.batch_execute("COMMIT").await.expect("unlock");
    drop(blocker);
    // The start task outlives the request and commits the run.
    let mut runs = 0;
    for _ in 0..50 {
        runs = count_runs(&db, "dr_parked").await;
        if runs == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(runs, 1, "the detached start committed the run");
    let exposed = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
    ];
    let pending = post(&client, Some("key-ac34"), &exposed, &body).await;
    assert_eq!(pending.status.as_u16(), 202);
    assert_eq!(pending.header(REPLAYED_HEADER), Some("true"));
    signal(&client, exec_id_of(&pending), json!("now")).await;
    let done = until_done(&client, "key-ac34", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
}

// AC-53
#[tokio::test(flavor = "multi_thread")]
async fn reads_after_the_start_end_before_the_request_deadline() {
    use diesel_async::SimpleAsyncConnection as _;
    let db = setup_db().await;
    let mut config = app_config(&db);
    config.server.timeouts.request_timeout_ms = Some(4_000);
    let mut route = __autumn_route_info_durable();
    route.name = "durable";
    let client = TestApp::new()
        .config(config)
        .routes(vec![route])
        .plugin(base_plugin())
        .with_db(db.pool.clone())
        .build();
    let mut blocker = db.pool.get().await.expect("blocker");
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_start_idempotency IN SHARE ROW EXCLUSIVE MODE")
        .await
        .expect("lock");
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
    ];
    let body = json!({"tag": "ac53"});
    let started = Instant::now();
    let response = post(&client, Some("key-ac53"), &extra, &body).await;
    let elapsed = started.elapsed();
    blocker.batch_execute("COMMIT").await.expect("unlock");
    // The stalled start ends at the 1 s margin with a pending answer, not with
    // Autumn's deadline 503.
    assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    assert_eq!(response.header("retry-after"), Some("1"));
    assert!(
        elapsed < Duration::from_millis(3_900),
        "answered in {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(2_500),
        "answered in {elapsed:?}"
    );
    // No run is known yet, so the answer names none.
    assert_eq!(response.header(EXEC_HEADER), None);
    assert_eq!(response.header(REPLAYED_HEADER), None);
    // The start was not stopped. It commits, and a retry attaches to it.
    let mut runs = 0;
    for _ in 0..50 {
        runs = count_runs(&db, "dr_parked").await;
        if runs > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(runs, 1, "the detached start committed one run");
    let again = post(&client, Some("key-ac53"), &extra, &body).await;
    assert_eq!(again.status.as_u16(), 202, "{}", again.text());
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
}

// AC-89
#[tokio::test(flavor = "multi_thread")]
async fn a_read_cut_at_the_deadline_keeps_the_run_headers() {
    use diesel_async::SimpleAsyncConnection as _;
    let db = setup_db().await;
    let mut config = app_config(&db);
    config.server.timeouts.request_timeout_ms = Some(4_000);
    let mut route = __autumn_route_info_durable();
    route.name = "durable";
    let client = TestApp::new()
        .config(config)
        .routes(vec![route])
        .plugin(base_plugin())
        .with_db(db.pool.clone())
        .build();
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_typed_failure"),
        ("x-map", "OutOfStock=422"),
        ("x-expose", "1"),
    ];
    let body = json!({"error_type": "OutOfStock"});
    let first = post(&client, Some("key-ac89"), &extra, &body).await;
    assert_eq!(first.status.as_u16(), 422, "{}", first.text());
    let exec_id = exec_id_of(&first);
    // The retry attaches without the history. Its typed-failure read then
    // stalls on the lock until the deadline margin.
    let mut blocker = db.pool.get().await.expect("blocker");
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_events IN ACCESS EXCLUSIVE MODE")
        .await
        .expect("lock");
    let started = Instant::now();
    let again = post(&client, Some("key-ac89"), &extra, &body).await;
    let elapsed = started.elapsed();
    blocker.batch_execute("COMMIT").await.expect("unlock");
    assert_eq!(again.status.as_u16(), 202, "{}", again.text());
    assert_eq!(again.header("retry-after"), Some("1"));
    assert!(
        elapsed < Duration::from_millis(3_900),
        "answered in {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(2_500),
        "the deadline cut the read: {elapsed:?}"
    );
    assert_eq!(exec_id_of(&again), exec_id);
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(
        again.json::<Value>()["execution_id"],
        json!(exec_id.to_string())
    );
}

// AC-91
#[tokio::test(flavor = "multi_thread")]
async fn a_request_timeout_with_no_time_left_starts_nothing() {
    let db = setup_db().await;
    let mut config = app_config(&db);
    config.server.timeouts.request_timeout_ms = Some(1_000);
    let mut route = __autumn_route_info_durable();
    route.name = "durable";
    let client = TestApp::new()
        .config(config)
        .routes(vec![route])
        .plugin(base_plugin())
        .with_db(db.pool.clone())
        .build();
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
    ];
    let response = post(&client, Some("key-ac91"), &extra, &json!({"tag": "ac91"})).await;
    assert_eq!(response.status.as_u16(), 500, "{}", response.text());
    assert!(
        response.header("retry-after").is_none(),
        "the answer is final"
    );
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("misconfigured")
    );
    assert_eq!(count_runs(&db, "dr_parked").await, 0);
}

// AC-54
#[tokio::test(flavor = "multi_thread")]
async fn retry_attempt_that_switches_type_returns_final_output() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_retry_then_switch"),
        ("x-wait-ms", "5000"),
    ];
    let response = until_done(&client, "key-ac54", &extra, &json!({"tag": "ac54"})).await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
    assert_eq!(response.json::<Value>(), json!({"final": "retry"}));
    assert_eq!(side_effects("ac54"), 2, "one side effect for each attempt");
}

// ── Terminal mapping ──────────────────────────────────────────────────────────

// AC-35
#[tokio::test(flavor = "multi_thread")]
async fn success_status_201_is_applied_to_completed_runs() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac35"});
    let extra = [("x-user", "ada"), ("x-success", "201")];
    let first = post(&client, Some("key-ac35"), &extra, &body).await;
    let replay = post(&client, Some("key-ac35"), &extra, &body).await;
    assert_eq!(first.status.as_u16(), 201, "{}", first.text());
    assert_eq!(replay.status.as_u16(), 201);
    assert_eq!(replay.header(REPLAYED_HEADER), Some("true"));
}

// AC-36
#[tokio::test(flavor = "multi_thread")]
async fn mapped_failure_returns_mapped_status_with_details() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_typed_failure"),
        ("x-map", "OutOfStock=422"),
    ];
    let response = post(
        &client,
        Some("key-ac36"),
        &extra,
        &json!({"error_type": "OutOfStock"}),
    )
    .await;
    assert_eq!(response.status.as_u16(), 422, "{}", response.text());
    let body = response.json::<Value>();
    assert_eq!(problem_type(&body), slug_type("failed"));
    assert_eq!(body["error_type"], "OutOfStock");
    assert_eq!(body["details"], json!({"sku": "A-1"}));
    assert!(!body.to_string().contains("secret workflow message"));
    assert!(response.header("retry-after").is_none());
}

// AC-37
#[tokio::test(flavor = "multi_thread")]
async fn unmapped_failure_returns_generic_500() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let cases = [
        (
            "key-ac37-a",
            "dr_typed_failure",
            json!({"error_type": "Unmapped"}),
        ),
        ("key-ac37-b", "dr_untyped_failure", json!({})),
    ];
    for (key, workflow, body) in cases {
        let extra = [
            ("x-user", "ada"),
            ("x-workflow", workflow),
            ("x-map", "OutOfStock=422"),
        ];
        let response = post(&client, Some(key), &extra, &body).await;
        assert_eq!(
            response.status.as_u16(),
            500,
            "{workflow}: {}",
            response.text()
        );
        assert!(response.header("retry-after").is_none());
        let problem = response.json::<Value>();
        assert_eq!(problem_type(&problem), slug_type("failed"));
        assert!(problem.get("error_type").is_none());
        assert!(problem.get("details").is_none());
        assert!(!problem.to_string().contains("secret workflow message"));
    }
}

async fn start_parked(client: &TestClient, key: &str) -> ExecutionId {
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
    ];
    let pending = post(client, Some(key), &extra, &json!({"tag": key})).await;
    assert_eq!(pending.status.as_u16(), 202, "{}", pending.text());
    exec_id_of(&pending)
}

async fn retry_parked(client: &TestClient, key: &str) -> TestResponse {
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "5000"),
    ];
    until_done(client, key, &extra, &json!({"tag": key})).await
}

fn handle_client(client: &TestClient) -> std::sync::Arc<WorkflowHandleClient> {
    client
        .state()
        .extension::<WorkflowHandleClient>()
        .expect("handle client")
}

// AC-38
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_run_returns_500_cancelled() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let exec_id = start_parked(&client, "key-ac38").await;
    handle_client(&client)
        .handle(exec_id)
        .cancel("test")
        .await
        .expect("cancel");
    let response = retry_parked(&client, "key-ac38").await;
    assert_eq!(response.status.as_u16(), 500, "{}", response.text());
    assert!(response.header("retry-after").is_none());
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("cancelled")
    );
}

// AC-39
#[tokio::test(flavor = "multi_thread")]
async fn terminated_run_returns_500_terminated() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let exec_id = start_parked(&client, "key-ac39").await;
    handle_client(&client)
        .handle(exec_id)
        .terminate("test")
        .await
        .expect("terminate");
    let response = retry_parked(&client, "key-ac39").await;
    assert_eq!(response.status.as_u16(), 500, "{}", response.text());
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("terminated")
    );
}

// AC-40
#[tokio::test(flavor = "multi_thread")]
async fn timed_out_run_returns_500_timed_out() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_slow"),
        ("x-wait-ms", "5000"),
    ];
    let response = until_done(&client, "key-ac40", &extra, &json!({"tag": "ac40"})).await;
    assert_eq!(response.status.as_u16(), 500, "{}", response.text());
    assert!(response.header("retry-after").is_none());
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("timed-out")
    );
}

// AC-41
#[tokio::test(flavor = "multi_thread")]
async fn continued_as_new_chain_returns_final_output() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_chain"),
        ("x-wait-ms", "5000"),
    ];
    let short = until_done(&client, "key-ac41-a", &extra, &json!({"tag": "ac41"})).await;
    assert_eq!(short.status.as_u16(), 200, "{}", short.text());
    assert_eq!(short.json::<Value>(), json!({"final": 2}));
    // A chain longer than any per-request step cap still resolves, because
    // a same-type chain takes one lookup.
    let long = until_done(
        &client,
        "key-ac41-b",
        &extra,
        &json!({"tag": "ac41", "hops": 130}),
    )
    .await;
    assert_eq!(long.status.as_u16(), 200, "{}", long.text());
    assert_eq!(long.json::<Value>(), json!({"final": 130}));
}

// AC-42
#[tokio::test(flavor = "multi_thread")]
async fn erased_run_returns_410() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac42"});
    let extra = [("x-user", "ada"), ("x-expose", "1")];
    let first = post(&client, Some("key-ac42"), &extra, &body).await;
    assert_eq!(first.status.as_u16(), 200);
    let exec_id = exec_id_of(&first);
    {
        let mut conn = db.pool.get().await.expect("conn");
        autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "test")
            .await
            .expect("erase");
    }
    let replay = post(&client, Some("key-ac42"), &extra, &body).await;
    assert_eq!(replay.status.as_u16(), 410, "{}", replay.text());
    assert_eq!(problem_type(&replay.json::<Value>()), slug_type("erased"));
}

/// One erasure case: the key, the headers, the body, and the status and slug
/// before the erasure.
type ErasureCase<'a> = (&'a str, &'a [(&'a str, &'a str)], Value, u16, &'a str);

// AC-85
#[tokio::test(flavor = "multi_thread")]
async fn erased_runs_answer_410_whatever_their_end() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let handles = handle_client(&client);
    let cancelled = start_parked(&client, "key-ac85-a").await;
    handles
        .handle(cancelled)
        .cancel("test")
        .await
        .expect("cancel");
    let terminated = start_parked(&client, "key-ac85-b").await;
    handles
        .handle(terminated)
        .terminate("test")
        .await
        .expect("terminate");
    let parked = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "5000"),
        ("x-expose", "1"),
    ];
    let slow = [
        ("x-user", "ada"),
        ("x-workflow", "dr_slow"),
        ("x-wait-ms", "5000"),
        ("x-expose", "1"),
    ];
    let untyped = [
        ("x-user", "ada"),
        ("x-workflow", "dr_untyped_failure"),
        ("x-expose", "1"),
    ];
    let typed = [
        ("x-user", "ada"),
        ("x-workflow", "dr_typed_failure"),
        ("x-map", "OutOfStock=422"),
        ("x-expose", "1"),
    ];
    let cases: [ErasureCase<'_>; 7] = [
        (
            "key-ac85-a",
            &parked,
            json!({"tag": "key-ac85-a"}),
            500,
            "cancelled",
        ),
        (
            "key-ac85-b",
            &parked,
            json!({"tag": "key-ac85-b"}),
            500,
            "terminated",
        ),
        (
            "key-ac85-c",
            &slow,
            json!({"tag": "ac85-c"}),
            500,
            "timed-out",
        ),
        ("key-ac85-d", &untyped, json!({}), 500, "failed"),
        (
            "key-ac85-e",
            &typed,
            json!({"error_type": "Unmapped"}),
            500,
            "failed",
        ),
        (
            "key-ac85-f",
            &typed,
            json!({"error_type": "OutOfStock"}),
            422,
            "failed",
        ),
        // Erasure leaves no tombstone in a failure with no details.
        (
            "key-ac85-g",
            &typed,
            json!({"error_type": "OutOfStock", "no_details": true}),
            422,
            "failed",
        ),
    ];
    for (key, extra, body, status, slug) in cases {
        let ended = until_done(&client, key, extra, &body).await;
        assert_eq!(ended.status.as_u16(), status, "{key}: {}", ended.text());
        assert_eq!(problem_type(&ended.json::<Value>()), slug_type(slug));
        {
            let mut conn = db.pool.get().await.expect("conn");
            autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id_of(&ended), "test")
                .await
                .expect("erase");
        }
        let again = post(&client, Some(key), extra, &body).await;
        assert_eq!(again.status.as_u16(), 410, "{key}: {}", again.text());
        assert!(again.header("retry-after").is_none(), "{key}");
        assert_eq!(problem_type(&again.json::<Value>()), slug_type("erased"));
    }
}

// AC-86
#[tokio::test(flavor = "multi_thread")]
async fn a_tombstone_input_does_not_read_as_erased() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    // A route that passes the raw body lets a client send the tombstone.
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
        ("x-raw-body", "1"),
    ];
    let body = json!({"_harvest_erased": true});
    let pending = post(&client, Some("key-ac86"), &extra, &body).await;
    assert_eq!(pending.status.as_u16(), 202, "{}", pending.text());
    handle_client(&client)
        .handle(exec_id_of(&pending))
        .cancel("test")
        .await
        .expect("cancel");
    let ended = until_done(&client, "key-ac86", &extra, &body).await;
    assert_eq!(ended.status.as_u16(), 500, "{}", ended.text());
    assert_eq!(problem_type(&ended.json::<Value>()), slug_type("cancelled"));
}

/// The attempt that retries `exec_id`.
async fn retry_attempt_of(db: &TestPg, exec_id: ExecutionId) -> ExecutionId {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT id::text AS id FROM harvest_workflow_executions \
         WHERE retry_of_exec_id = $1::uuid",
    )
    .bind::<Text, _>(exec_id.to_string())
    .get_result::<Id>(&mut conn)
    .await
    .expect("one retry attempt")
    .id
    .parse()
    .expect("execution id")
}

// AC-87
#[tokio::test(flavor = "multi_thread")]
async fn erasure_is_read_from_the_live_retry_attempt() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_always_fails"),
        ("x-wait-ms", "5000"),
        ("x-expose", "1"),
    ];
    let body = json!({"tag": "ac87"});
    let ended = until_done(&client, "key-ac87", &extra, &body).await;
    assert_eq!(ended.status.as_u16(), 500, "{}", ended.text());
    let first = exec_id_of(&ended);
    let live = retry_attempt_of(&db, first).await;
    // The answer comes from the last attempt, so erasing the first changes
    // nothing.
    for (erased, status, slug) in [(first, 500, "failed"), (live, 410, "erased")] {
        {
            let mut conn = db.pool.get().await.expect("conn");
            autumn_harvest::erase::erase_workflow_payloads(&mut conn, erased, "test")
                .await
                .expect("erase");
        }
        let again = post(&client, Some("key-ac87"), &extra, &body).await;
        assert_eq!(again.status.as_u16(), status, "{}", again.text());
        assert_eq!(problem_type(&again.json::<Value>()), slug_type(slug));
    }
}

// AC-43
#[tokio::test(flavor = "multi_thread")]
async fn run_memo_names_the_route() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let response = post(
        &client,
        Some("key-ac43"),
        &[("x-user", "ada-secret")],
        &json!({"tag": "ac43"}),
    )
    .await;
    assert_eq!(response.status.as_u16(), 200);

    let mut conn = db.pool.get().await.expect("conn");
    let memo = diesel::sql_query(
        "SELECT memo::text AS memo FROM harvest_workflow_executions WHERE workflow_name = 'dr_order'",
    )
    .get_result::<Memo>(&mut conn)
    .await
    .expect("memo")
    .memo;
    let memo: Value = serde_json::from_str(&memo).expect("memo json");
    assert_eq!(
        memo,
        json!({"durable_route": {"method": "POST", "route": "/durable"}})
    );
    let text = memo.to_string();
    assert!(!text.contains("key-ac43") && !text.contains("ada-secret") && !text.contains("ac43"));
}

// ── Resource bounds and operator actions ─────────────────────────────────────

// AC-55
#[tokio::test(flavor = "multi_thread")]
async fn start_connection_is_released_before_the_wait() {
    let db = setup_db_with_pool(4).await;
    let client = build_app(&db).await;
    let pool = client
        .state()
        .extension::<autumn_harvest_plugin::HarvestDbPool>()
        .expect("harvest pool");
    assert_eq!(
        pool.default_pool().status().max_size,
        4,
        "the route shares the 4-slot pool"
    );
    // Eight requests wait 7 s each, longer than the 2 s pool acquire bound.
    // A request that kept a pooled connection through its wait would starve
    // the others. A starved read answers pending early, so each waiter must
    // use up its whole budget.
    let budget = Duration::from_secs(7);
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "7000"),
    ];
    let bodies: Vec<Value> = (0..8)
        .map(|i| json!({"tag": format!("ac55-{i}")}))
        .collect();
    let keys: Vec<String> = (0..8).map(|i| format!("key-ac55-{i}")).collect();
    let waiters = keys.iter().zip(&bodies).map(|(key, body)| async {
        let sent = Instant::now();
        let response = post(&client, Some(key.as_str()), &extra, body).await;
        (response, sent.elapsed())
    });
    // A ninth request arrives during the waits. Its start and its run need
    // the pool, so a starved pool answers 503.
    let probe = async {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let body = json!({"tag": "ac55-probe"});
        post(&client, Some("key-ac55-probe"), &[("x-user", "ada")], &body).await
    };
    let (waited, probe) = tokio::join!(futures::future::join_all(waiters), probe);
    for (response, elapsed) in waited {
        assert_eq!(response.status.as_u16(), 202, "{}", response.text());
        assert!(
            elapsed + Duration::from_millis(250) >= budget,
            "a waiter answered after {elapsed:?}, before its budget ended"
        );
    }
    assert_eq!(probe.status.as_u16(), 200, "{}", probe.text());
    assert_eq!(count_runs(&db, "dr_parked").await, 8);
}

/// Rerun a terminal run, as an operator does. Returns the state it sealed.
async fn rerun(db: &TestPg, exec_id: ExecutionId) -> String {
    rerun_with(db, exec_id, None).await
}

/// [`rerun`] with another input, so the rerun can end another way.
///
/// Each app installs a process-wide admission cache that loads after start.
/// Until it loads, a rerun fails closed, so the helper tries again.
async fn rerun_with(db: &TestPg, exec_id: ExecutionId, input: Option<Value>) -> String {
    for _ in 0..50 {
        let mut conn = db.pool.get().await.expect("conn");
        let outcome = autumn_harvest::execution::rerun_workflow_execution(
            &mut conn,
            exec_id,
            autumn_harvest::execution::RerunRequest {
                input_override: input.clone(),
                workflow_id_override: None,
                started_by: None,
                concurrency_key: None,
                concurrency_limit: None,
                concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                max_workflow_input_bytes: 0,
                max_execution_timeout_ceiling: None,
                max_workflow_chain_timeout_ceiling: None,
                max_workflow_attempts_ceiling: None,
                trace_context: None,
            },
            None,
        )
        .await;
        match outcome {
            Ok(outcome) => return outcome.source_prior_state,
            Err(HarvestError::AdmissionBlocked { gate_id, .. }) if gate_id.is_nil() => {
                drop(conn);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("rerun: {error:?}"),
        }
    }
    panic!("the admission cache never loaded");
}

// AC-56
#[tokio::test(flavor = "multi_thread")]
async fn rerun_sealed_run_reports_its_original_outcome() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac56"});
    let extra = [("x-user", "ada"), ("x-expose", "1")];
    let first = post(&client, Some("key-ac56"), &extra, &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(rerun(&db, exec_id_of(&first)).await, "COMPLETED");
    // The original request keeps the outcome that its own run recorded.
    let again = post(&client, Some("key-ac56"), &extra, &body).await;
    assert_eq!(again.status.as_u16(), 200, "{}", again.text());
    assert_eq!(again.json::<Value>(), first.json::<Value>());
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
}

/// Backends whose last statement opened a result-wait listener.
async fn result_listeners(db: &TestPg) -> i64 {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT count(*)::bigint AS n FROM pg_stat_activity \
         WHERE query = 'LISTEN \"harvest_events\"'",
    )
    .get_result::<Count>(&mut conn)
    .await
    .expect("listeners")
    .n
}

// AC-57
#[tokio::test(flavor = "multi_thread")]
async fn listen_connections_never_exceed_waiter_cap() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin().durable_route_max_waiters(2)).build();
    let baseline = result_listeners(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "4000"),
    ];
    let bodies: Vec<Value> = (0..6)
        .map(|i| json!({"tag": format!("ac57-{i}")}))
        .collect();
    let keys: Vec<String> = (0..6).map(|i| format!("key-ac57-{i}")).collect();
    let requests = futures::future::join_all(
        keys.iter()
            .zip(&bodies)
            .map(|(key, body)| post(&client, Some(key.as_str()), &extra, body)),
    );
    let sample = async {
        tokio::time::sleep(Duration::from_millis(2_000)).await;
        result_listeners(&db).await - baseline
    };
    let (responses, during) = tokio::join!(requests, sample);
    for response in responses {
        assert_eq!(response.status.as_u16(), 202, "{}", response.text());
    }
    assert!(during >= 1, "the sample sees the waiting listeners");
    assert!(during <= 2, "{during} result listeners exceed the cap of 2");
}

// ── Gaps that the acceptance audit found ──────────────────────────────────────

// AC-68
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_layer_answers_in_flight_duplicate() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin()).idempotent().build();
    let body = json!({"tag": "ac68"});
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "3000"),
        ("x-expose", "1"),
    ];
    let first = post(&client, Some("key-ac68"), &extra, &body);
    // The duplicate arrives while the first request still waits.
    let duplicate = async {
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        post(&client, Some("key-ac68"), &extra, &body).await
    };
    let (first, duplicate) = tokio::join!(first, duplicate);
    assert_eq!(duplicate.status.as_u16(), 409, "{}", duplicate.text());
    assert_eq!(duplicate.header("retry-after"), Some("1"));
    assert_ne!(
        duplicate.header("content-type"),
        Some("application/problem+json"),
        "Autumn answers the duplicate, not the route"
    );
    assert_eq!(
        handler_entries("key-ac68"),
        1,
        "the duplicate skipped the route"
    );
    assert_eq!(first.status.as_u16(), 409, "{}", first.text());
    assert_eq!(problem_type(&first.json::<Value>()), slug_type("pending"));
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
    signal(&client, exec_id_of(&first), json!("now")).await;
    let done = until_done(&client, "key-ac68", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(count_runs(&db, "dr_parked").await, 1);
}

/// A codec envelope that names a codec that this app does not have.
fn unknown_codec_envelope() -> String {
    json!({
        "_harvest_codec_envelope": 1,
        "codec_id": "kms-rotated-away",
        "data": "e30=",
    })
    .to_string()
}

// AC-69
#[tokio::test(flavor = "multi_thread")]
async fn undecodable_output_returns_result_unavailable() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac69"});
    let extra = [("x-user", "ada"), ("x-expose", "1")];
    let first = post(&client, Some("key-ac69"), &extra, &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    {
        // Another writer stored the output under a codec that this app lacks.
        let mut conn = db.pool.get().await.expect("conn");
        let updated = diesel::sql_query(
            "UPDATE harvest_workflow_executions SET output = $1::jsonb WHERE id = $2::uuid",
        )
        .bind::<Text, _>(unknown_codec_envelope())
        .bind::<Text, _>(exec_id_of(&first).to_string())
        .execute(&mut conn)
        .await
        .expect("plant envelope");
        assert_eq!(updated, 1);
    }
    let again = post(&client, Some("key-ac69"), &extra, &body).await;
    assert_eq!(again.status.as_u16(), 500, "{}", again.text());
    assert!(again.header("retry-after").is_none(), "the answer is final");
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(
        problem_type(&again.json::<Value>()),
        slug_type("result-unavailable")
    );
    assert!(!again.text().contains("kms-rotated-away"));
}

// AC-70
#[tokio::test(flavor = "multi_thread")]
async fn undecodable_typed_failure_returns_result_unavailable() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac70"});
    let mapped = [
        ("x-user", "ada"),
        ("x-workflow", "dr_typed_failure"),
        ("x-map", "OutOfStock=422"),
        ("x-expose", "1"),
    ];
    let first = until_done(&client, "key-ac70", &mapped, &body).await;
    assert_eq!(first.status.as_u16(), 422, "{}", first.text());
    {
        // Another writer stored the failure details under a codec that this
        // app lacks. A test fixture may rewrite history with the guard off.
        let mut conn = db.pool.get().await.expect("conn");
        let exec_id = exec_id_of(&first).to_string();
        let updated = autumn_harvest::append_only::with_guard_off(&mut conn, async move |conn| {
            diesel::sql_query(
                "UPDATE harvest_events \
                 SET event_data = jsonb_set(event_data, '{data,details}', $1::jsonb) \
                 WHERE workflow_exec_id = $2::uuid AND event_data->>'type' = 'WorkflowFailed'",
            )
            .bind::<Text, _>(unknown_codec_envelope())
            .bind::<Text, _>(exec_id)
            .execute(conn)
            .await
        })
        .await
        .expect("plant envelope");
        assert_eq!(updated, 1);
    }
    let again = post(&client, Some("key-ac70"), &mapped, &body).await;
    assert_eq!(again.status.as_u16(), 500, "{}", again.text());
    assert!(again.header("retry-after").is_none(), "the answer is final");
    assert_eq!(
        problem_type(&again.json::<Value>()),
        slug_type("result-unavailable")
    );
    // With no mapping, the route reads no history, so the answer stays failed.
    let unmapped = post(
        &client,
        Some("key-ac70"),
        &[("x-user", "ada"), ("x-workflow", "dr_typed_failure")],
        &body,
    )
    .await;
    assert_eq!(unmapped.status.as_u16(), 500, "{}", unmapped.text());
    assert_eq!(problem_type(&unmapped.json::<Value>()), slug_type("failed"));
}

// AC-71
#[tokio::test(flavor = "multi_thread")]
async fn same_type_chain_takes_one_step() {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin().durable_route_max_chain_steps(1)).build();
    // A walk that read one run at a time would need ten steps here.
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_chain"),
        ("x-wait-ms", "5000"),
    ];
    let body = json!({"tag": "ac71", "hops": 10});
    let done = until_done(&client, "key-ac71", &extra, &body).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(done.json::<Value>(), json!({"final": 10}));
}

/// Waits until a run of `workflow` reaches `state`.
async fn until_state(db: &TestPg, workflow: &str, state: &str) {
    for _ in 0..100 {
        let mut conn = db.pool.get().await.expect("conn");
        let found = diesel::sql_query(
            "SELECT count(*)::bigint AS n FROM harvest_workflow_executions \
             WHERE workflow_name = $1 AND state = $2",
        )
        .bind::<Text, _>(workflow)
        .bind::<Text, _>(state)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count")
        .n;
        if found > 0 {
            return;
        }
        drop(conn);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no {workflow} run reached {state}");
}

/// Runs the two-change chain to its end, then asks again under `steps`.
async fn two_changes_of_type(steps: usize) -> (TestResponse, Duration) {
    let db = setup_db().await;
    let client = app_with(&db, base_plugin().durable_route_max_chain_steps(steps)).build();
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_switch_start"),
        ("x-wait-ms", "0"),
    ];
    let body = json!({"tag": "ac72"});
    let first = post(&client, Some("key-ac72"), &extra, &body).await;
    assert_ne!(first.status.as_u16(), 500, "{}", first.text());
    until_state(&db, "dr_chain_end", "COMPLETED").await;
    let started = Instant::now();
    let answer = post(&client, Some("key-ac72"), &extra, &body).await;
    (answer, started.elapsed())
}

// AC-72
#[tokio::test(flavor = "multi_thread")]
async fn chain_step_cap_answers_result_unavailable() {
    // One step crosses only one change of type. A retry walks the same runs,
    // so the answer is final.
    let (capped, elapsed) = two_changes_of_type(1).await;
    assert_eq!(capped.status.as_u16(), 500, "{}", capped.text());
    assert!(
        capped.header("retry-after").is_none(),
        "the answer is final"
    );
    assert_eq!(
        problem_type(&capped.json::<Value>()),
        slug_type("result-unavailable")
    );
    assert!(elapsed < Duration::from_secs(2), "answered in {elapsed:?}");
}

// AC-72
#[tokio::test(flavor = "multi_thread")]
async fn chain_step_cap_admits_one_step_for_each_change_of_type() {
    let (done, _) = two_changes_of_type(2).await;
    assert_eq!(done.status.as_u16(), 200, "{}", done.text());
    assert_eq!(done.json::<Value>(), json!({"final": "mid"}));
}

// AC-73
#[tokio::test(flavor = "multi_thread")]
async fn oversized_input_is_refused_before_any_io() {
    use diesel_async::SimpleAsyncConnection as _;
    let db = setup_db().await;
    let mut config = app_config(&db);
    config.server.timeouts.request_timeout_ms = Some(4_000);
    let mut route = __autumn_route_info_durable();
    route.name = "durable";
    let client = TestApp::new()
        .config(config)
        .routes(vec![route])
        .plugin(base_plugin())
        .with_db(db.pool.clone())
        .build();
    // Every start now stalls, so only a check before any I/O answers fast.
    let mut blocker = db.pool.get().await.expect("blocker");
    blocker
        .batch_execute("BEGIN; LOCK TABLE harvest_start_idempotency IN SHARE ROW EXCLUSIVE MODE")
        .await
        .expect("lock");
    let big = json!({"tag": "ac73", "pad": "x".repeat(2_200_000)});
    let started = Instant::now();
    let response = post(&client, Some("key-ac73"), &[("x-user", "ada")], &big).await;
    let elapsed = started.elapsed();
    blocker.batch_execute("COMMIT").await.expect("unlock");
    assert_eq!(response.status.as_u16(), 413, "{}", response.status);
    assert_eq!(
        problem_type(&response.json::<Value>()),
        slug_type("input-too-large")
    );
    assert!(elapsed < Duration::from_secs(2), "answered in {elapsed:?}");
}

// AC-74
#[tokio::test(flavor = "multi_thread")]
async fn raised_input_cap_admits_a_larger_input() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    // Over the 2 MiB deployment cap, under the 4 MiB cap of `dr_big`.
    let big = json!({"tag": "ac74", "pad": "x".repeat(2_200_000)});
    let extra = [("x-user", "ada"), ("x-workflow", "dr_big")];
    let response = until_done(&client, "key-ac74", &extra, &big).await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.status);
    assert_eq!(response.json::<Value>(), json!({"pad": 2_200_000}));
}

// AC-75
#[tokio::test(flavor = "multi_thread")]
async fn unscoped_route_shares_one_key_space() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let body = json!({"tag": "ac75"});
    // With no `x-user`, the test route calls `.unscoped()`.
    let first = post(&client, Some("key-ac75"), &[], &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json::<Value>()["user"], Value::Null);
    let second = post(&client, Some("key-ac75"), &[], &body).await;
    assert_eq!(second.status.as_u16(), 200, "{}", second.text());
    assert_eq!(second.header(REPLAYED_HEADER), Some("true"));
    assert_eq!(second.json::<Value>(), first.json::<Value>());
    assert_eq!(side_effects("ac75"), 1);
    let changed = post(&client, Some("key-ac75"), &[], &json!({"tag": "ac75-x"})).await;
    assert_eq!(changed.status.as_u16(), 422, "{}", changed.text());
    assert_eq!(
        problem_type(&changed.json::<Value>()),
        slug_type("idempotency-key-reused")
    );
    // A scoped request with the same key is in another key space.
    let scoped = post(&client, Some("key-ac75"), &[("x-user", "ada")], &body).await;
    assert_eq!(scoped.status.as_u16(), 200, "{}", scoped.text());
    assert_eq!(scoped.header(REPLAYED_HEADER), None);
    assert_eq!(count_runs(&db, "dr_order").await, 2);
}

// AC-76
#[tokio::test(flavor = "multi_thread")]
async fn rerun_sealed_failures_report_their_original_outcome() {
    let db = setup_db().await;
    let client = build_app(&db).await;

    // A mapped failure keeps its mapped status.
    let mapped = [
        ("x-user", "ada"),
        ("x-workflow", "dr_typed_failure"),
        ("x-map", "OutOfStock=422"),
        ("x-expose", "1"),
    ];
    let body = json!({"tag": "ac76-a"});
    let failed = until_done(&client, "key-ac76-a", &mapped, &body).await;
    assert_eq!(failed.status.as_u16(), 422, "{}", failed.text());
    // The rerun fails with an unmapped class, so its outcome differs.
    let other = json!({"user": "ada", "body": {"error_type": "Other"}});
    assert_eq!(
        rerun_with(&db, exec_id_of(&failed), Some(other)).await,
        "FAILED"
    );
    // The rerun seals the original run, so the one failed run is the rerun.
    until_state(&db, "dr_typed_failure", "FAILED").await;
    let again = post(&client, Some("key-ac76-a"), &mapped, &body).await;
    assert_eq!(again.status.as_u16(), 422, "{}", again.text());
    assert_eq!(again.json::<Value>()["error_type"], "OutOfStock");

    // A cancelled run stays cancelled.
    let exec_id = start_parked(&client, "key-ac76-b").await;
    handle_client(&client)
        .handle(exec_id)
        .cancel("test")
        .await
        .expect("cancel");
    let cancelled = retry_parked(&client, "key-ac76-b").await;
    assert_eq!(cancelled.status.as_u16(), 500, "{}", cancelled.text());
    assert_eq!(rerun(&db, exec_id).await, "CANCELLED");
    let again = retry_parked(&client, "key-ac76-b").await;
    assert_eq!(problem_type(&again.json::<Value>()), slug_type("cancelled"));

    // A timed-out run stays timed out.
    let slow = [
        ("x-user", "ada"),
        ("x-workflow", "dr_slow"),
        ("x-wait-ms", "5000"),
        ("x-expose", "1"),
    ];
    let body = json!({"tag": "ac76-c"});
    let timed_out = until_done(&client, "key-ac76-c", &slow, &body).await;
    assert_eq!(timed_out.status.as_u16(), 500, "{}", timed_out.text());
    // The rerun completes, so its outcome differs.
    let fast = json!({"user": "ada", "body": {"fast": true}});
    assert_eq!(
        rerun_with(&db, exec_id_of(&timed_out), Some(fast)).await,
        "TIMED_OUT"
    );
    until_state(&db, "dr_slow", "COMPLETED").await;
    let again = post(&client, Some("key-ac76-c"), &slow, &body).await;
    assert_eq!(problem_type(&again.json::<Value>()), slug_type("timed-out"));
}

/// A log sink in memory.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

// AC-64
#[tokio::test(flavor = "multi_thread")]
async fn durable_route_logs_hold_no_key_scope_or_input() {
    use tracing::instrument::WithSubscriber as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    // With one live dispatcher, tracing-core lets another test thread cache
    // a callsite as never. A second dispatcher for the whole process turns
    // that path off.
    static KEEP: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    KEEP.get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new()));
    let db = setup_db().await;
    let client = build_app(&db).await;
    let captured = Captured::default();
    let writer = captured.clone();
    // Only the route's own events and span count.
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
        )
        .with(tracing_subscriber::filter::Targets::new().with_target(
            "autumn_harvest_plugin::durable_route",
            tracing::Level::TRACE,
        ));
    let dispatch = tracing::Dispatch::new(subscriber);
    let user = [("x-user", "scope-marker-64")];
    let completed = post(
        &client,
        Some("key-marker-64"),
        &user,
        &json!({"tag": "input-marker-64"}),
    )
    .with_subscriber(dispatch.clone())
    .await;
    assert_eq!(completed.status.as_u16(), 200, "{}", completed.text());
    let invalid = post(
        &client,
        Some("key-marker-64-b"),
        &[("x-user", "scope-marker-64"), ("x-workflow", "dr_schema")],
        &json!({"sku": ["input-marker-64"]}),
    )
    .with_subscriber(dispatch.clone())
    .await;
    assert_eq!(invalid.status.as_u16(), 422, "{}", invalid.text());
    let refused = post(
        &client,
        Some("key-marker-64-c"),
        &[("x-user", "scope-marker-64"), ("x-workflow", "dr_batched")],
        &json!({"tag": "input-marker-64"}),
    )
    .with_subscriber(dispatch)
    .await;
    assert_eq!(refused.status.as_u16(), 500, "{}", refused.text());

    let text = captured.text();
    assert!(text.contains("workflow=dr_order"), "{text}");
    assert!(text.contains("route=\"/durable\""), "{text}");
    assert!(text.contains("created=true"), "{text}");
    assert!(text.contains("exec_id="), "{text}");
    assert!(text.contains("outcome=\"completed\""), "{text}");
    assert!(text.contains("count=1"), "the schema log counts: {text}");
    assert!(
        text.contains("durable route cannot start its workflow")
            && text.contains("debounce, batch, or"),
        "the setup log names the cause: {text}"
    );
    for marker in ["key-marker-64", "scope-marker-64", "input-marker-64"] {
        assert!(!text.contains(marker), "{marker} in {text}");
    }
}

#[derive(QueryableByName)]
struct Id {
    #[diesel(sql_type = Text)]
    id: String,
}

/// The one completed run of `workflow`.
async fn completed_run(db: &TestPg, workflow: &str) -> ExecutionId {
    let mut conn = db.pool.get().await.expect("conn");
    diesel::sql_query(
        "SELECT id::text AS id FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND state = 'COMPLETED'",
    )
    .bind::<Text, _>(workflow)
    .get_result::<Id>(&mut conn)
    .await
    .expect("one completed run")
    .id
    .parse()
    .expect("execution id")
}

// AC-79
#[tokio::test(flavor = "multi_thread")]
async fn rerun_of_a_chain_end_keeps_the_original_outcome() {
    let db = setup_db().await;
    // One step must still reach the end of an eleven-run chain.
    let client = app_with(&db, base_plugin().durable_route_max_chain_steps(1)).build();
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_chain"),
        ("x-wait-ms", "5000"),
    ];
    let body = json!({"tag": "ac79", "hops": 10});
    let first = until_done(&client, "key-ac79", &extra, &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json::<Value>(), json!({"final": 10}));
    // An operator reruns the last run of the chain, and the rerun ends
    // another way. The rerun keeps the business id.
    let last = completed_run(&db, "dr_chain").await;
    let other = json!({"hop": 50, "target": 50});
    assert_eq!(rerun_with(&db, last, Some(other)).await, "COMPLETED");
    until_state(&db, "dr_chain", "COMPLETED").await;
    let again = post(&client, Some("key-ac79"), &extra, &body).await;
    assert_eq!(again.status.as_u16(), 200, "{}", again.text());
    assert_eq!(again.json::<Value>(), json!({"final": 10}));
    assert_eq!(again.header(REPLAYED_HEADER), Some("true"));
}

// AC-81
#[tokio::test(flavor = "multi_thread")]
async fn after_the_window_the_business_id_holder_answers() {
    let db = setup_db().await;
    let plugin = base_plugin().start_idempotency_window(Duration::from_secs(1));
    let client = app_with(&db, plugin).build();

    // A terminated run no longer holds the business id, so a retry after the
    // window starts a new run.
    let terminated = start_parked(&client, "key-ac81-a").await;
    handle_client(&client)
        .handle(terminated)
        .terminate("test")
        .await
        .expect("terminate");
    // The retry after an operator rerun attaches to the rerun.
    let order = [("x-user", "ada"), ("x-expose", "1")];
    let body = json!({"tag": "ac81-b"});
    let first = post(&client, Some("key-ac81-b"), &order, &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    let rerun_input = json!({"user": "ada", "body": {"tag": "ac81-rerun"}});
    assert_eq!(
        rerun_with(&db, exec_id_of(&first), Some(rerun_input)).await,
        "COMPLETED"
    );
    until_state(&db, "dr_order", "COMPLETED").await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let parked = [
        ("x-user", "ada"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
    ];
    let again = post(
        &client,
        Some("key-ac81-a"),
        &parked,
        &json!({"tag": "key-ac81-a"}),
    )
    .await;
    assert_eq!(again.status.as_u16(), 202, "{}", again.text());
    assert_ne!(exec_id_of(&again), terminated, "a new run started");
    assert_eq!(again.header(REPLAYED_HEADER), None);

    let later = post(&client, Some("key-ac81-b"), &order, &body).await;
    assert_eq!(later.status.as_u16(), 200, "{}", later.text());
    assert_eq!(later.json::<Value>()["body"]["tag"], "ac81-rerun");
    assert_ne!(exec_id_of(&later), exec_id_of(&first));
}

// AC-82
#[tokio::test(flavor = "multi_thread")]
async fn erasing_the_first_run_of_a_chain_keeps_the_final_output() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-workflow", "dr_chain"),
        ("x-wait-ms", "5000"),
        ("x-expose", "1"),
    ];
    let body = json!({"tag": "ac82", "hops": 1});
    let first = until_done(&client, "key-ac82", &extra, &body).await;
    assert_eq!(first.status.as_u16(), 200, "{}", first.text());
    assert_eq!(first.json::<Value>(), json!({"final": 1}));
    let head = exec_id_of(&first);
    {
        // Erasure works on one execution. The output lives on the last run.
        let mut conn = db.pool.get().await.expect("conn");
        autumn_harvest::erase::erase_workflow_payloads(&mut conn, head, "test")
            .await
            .expect("erase the first run");
    }
    let again = post(&client, Some("key-ac82"), &extra, &body).await;
    assert_eq!(again.status.as_u16(), 200, "{}", again.text());
    assert_eq!(again.json::<Value>(), json!({"final": 1}));
    assert_eq!(exec_id_of(&again), head, "the id names the first run");
}

#[derive(QueryableByName)]
struct Origin {
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    tenant: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    start_source: Option<String>,
}

// AC-83
#[tokio::test(flavor = "multi_thread")]
async fn durable_runs_have_no_tenant_and_record_the_transactional_source() {
    let db = setup_db().await;
    let client = build_app(&db).await;
    let extra = [
        ("x-user", "ada"),
        ("x-tenant", "acme"),
        ("x-workflow", "dr_parked"),
        ("x-wait-ms", "0"),
        ("x-expose", "1"),
        (
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ),
    ];
    let pending = post_to(
        &client,
        "/tenant/durable",
        Some("key-ac83"),
        &extra,
        &json!({"tag": "ac83"}),
    )
    .await;
    assert_eq!(pending.status.as_u16(), 202, "{}", pending.text());
    let exec_id = exec_id_of(&pending).to_string();
    let mut conn = db.pool.get().await.expect("conn");
    let origin = diesel::sql_query(
        "SELECT tenant, start_source FROM harvest_workflow_executions WHERE id = $1::uuid",
    )
    .bind::<Text, _>(&exec_id)
    .get_result::<Origin>(&mut conn)
    .await
    .expect("run row");
    assert_eq!(
        origin.tenant, None,
        "Autumn's tenant is not a Harvest tenant"
    );
    assert_eq!(origin.start_source.as_deref(), Some("transactional"));
    // The parked run keeps its workflow task, and the task holds no trace.
    let tasks = diesel::sql_query(
        "SELECT count(*)::bigint AS n FROM harvest_task_queue WHERE workflow_exec_id = $1::uuid",
    )
    .bind::<Text, _>(&exec_id)
    .get_result::<Count>(&mut conn)
    .await
    .expect("tasks")
    .n;
    assert!(tasks > 0, "the parked run has a task row");
    let traced = diesel::sql_query(
        "SELECT count(*)::bigint AS n FROM harvest_task_queue \
         WHERE workflow_exec_id = $1::uuid AND trace_context IS NOT NULL",
    )
    .bind::<Text, _>(&exec_id)
    .get_result::<Count>(&mut conn)
    .await
    .expect("traced tasks")
    .n;
    assert_eq!(traced, 0, "the run does not join the HTTP trace");
}

// AC-84
#[test]
fn ci_manifest_lists_every_durable_route_suite() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../.github/ci/integration-suites.txt"
    );
    let manifest = std::fs::read_to_string(path).expect("CI suite manifest");
    let rows: Vec<Vec<&str>> = manifest
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .map(|line| line.split_whitespace().collect())
        .collect();
    for (osclass, target) in [
        ("allos", "durable_route_docs"),
        ("linux", "durable_route_dispatch"),
        ("linux", "durable_route_integration"),
    ] {
        assert!(
            rows.iter().any(|row| row.len() >= 3
                && row[0] == osclass
                && row[1] == "autumn-harvest-plugin"
                && row[2] == target),
            "the CI manifest must run {target} on {osclass}"
        );
    }
}
