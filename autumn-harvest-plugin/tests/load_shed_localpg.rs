//! Local-Postgres tests for automatic load shedding (issue #1794).
//!
//! The tests build a real backlog in `harvest_task_queue`. They run one sampler
//! pass and drive the HTTP router. They assert the trip, the exemptions and the
//! clear.
//!
//! The suite needs `HARVEST_TEST_DATABASE_URL` and is a no-op without it. The
//! tests run one at a time because the admission gate cache is process global.
#![allow(clippy::await_holding_lock, clippy::too_many_lines)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::RetentionConfig;
use autumn_harvest::admission_gate::{
    AdmissionGateCache, GateScope, set_global_admission_gate_cache,
};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::load_shed::{LoadShedConfig, LoadShedPolicy, sample_once};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use tower::ServiceExt;

static TEST_SERIAL: Mutex<()> = Mutex::new(());

const WF: &str = "ls_wf";
const THROTTLED_WF: &str = "ls_throttled_wf";
const QUEUE: &str = "default";

/// Records the load-shed metrics and the manual-gate block count.
#[derive(Default)]
struct CapturingMetrics {
    active: Mutex<Vec<(String, bool)>>,
    rejected: Mutex<Vec<String>>,
    blocked: Mutex<usize>,
}

impl CapturingMetrics {
    fn rejected(&self) -> usize {
        self.rejected.lock().unwrap().len()
    }
    fn active(&self) -> Vec<bool> {
        self.active
            .lock()
            .unwrap()
            .iter()
            .map(|(_, a)| *a)
            .collect()
    }
}

impl MetricsRecorder for CapturingMetrics {
    fn record_load_shed_active(&self, queue: &str, active: bool) {
        self.active.lock().unwrap().push((queue.to_owned(), active));
    }
    fn record_load_shed_rejected(&self, queue: &str) {
        self.rejected.lock().unwrap().push(queue.to_owned());
    }
    fn record_admission_blocked(&self, _scope_kind: &str, _reason_hash: &str) {
        *self.blocked.lock().unwrap() += 1;
    }
}

/// Publishes a gate cache and clears it on drop, also on a failed assert.
struct GlobalCache;

impl GlobalCache {
    fn publish(cache: Arc<AdmissionGateCache>) -> Self {
        set_global_admission_gate_cache(Some(cache));
        Self
    }
}

impl Drop for GlobalCache {
    fn drop(&mut self) {
        set_global_admission_gate_cache(None);
    }
}

fn db_url() -> Option<String> {
    std::env::var("HARVEST_TEST_DATABASE_URL").ok()
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(6)
        .build()
        .expect("pool build failed")
}

fn wf_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
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
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

/// A throttled workflow with an empty bucket, so a start defers with `202`.
fn wf_info_throttled(name: &'static str) -> WorkflowInfo {
    let mut info = wf_info(name);
    info.throttle = Some(autumn_harvest::throttle::ThrottlePolicy {
        refill_per_sec: 0.0,
        burst: 0.0,
        key_expr: None,
        schedule_to_start: None,
    });
    info
}

fn build_api_state(pool: &DbPool, metrics: Arc<CapturingMetrics>) -> HarvestApiState {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info(WF), wf_info_throttled(THROTTLED_WF)],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("load-shed-test".to_owned()),
        vec![QUEUE.to_owned()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// Trip at 60 s, clear at 10 s, retry after 7 s.
///
/// The long sample interval keeps a sampled state fresh for the whole test, so
/// a slow runner cannot fail the gate open between two steps.
fn config() -> LoadShedConfig {
    let policy = LoadShedPolicy::new(
        Duration::from_secs(60),
        Duration::from_secs(10),
        Duration::from_secs(7),
    )
    .expect("valid policy");
    LoadShedConfig::new()
        .with_sample_interval(Duration::from_secs(60))
        .queue(QUEUE, policy)
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_admission_gates",
        "DELETE FROM harvest_start_throttle",
        "DELETE FROM harvest_rate_limit_buckets",
        "DELETE FROM harvest_start_idempotency",
        "DELETE FROM harvest_task_queue",
        "DELETE FROM harvest_signals",
        "DELETE FROM harvest_timers",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
        "DELETE FROM harvest_audit_log WHERE operation LIKE 'load_shed.%'",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

/// Make every pending task on the queue `age_secs` old.
async fn set_backlog_age(conn: &mut AsyncPgConnection, age_secs: i64) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET created_at = NOW() - make_interval(secs => $1), \
             scheduled_at = NOW() - make_interval(secs => $1) \
         WHERE queue_name = $2 AND state = 'PENDING'",
    )
    .bind::<BigInt, _>(age_secs)
    .bind::<Text, _>(QUEUE)
    .execute(conn)
    .await
    .expect("age backlog");
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .expect(sql)
        .n
}

async fn pending_count(conn: &mut AsyncPgConnection) -> i64 {
    count(
        conn,
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE queue_name = 'default' AND state = 'PENDING'",
    )
    .await
}

async fn executions_with_id(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id = $1",
    )
    .bind::<Text, _>(workflow_id)
    .get_result::<CountRow>(conn)
    .await
    .unwrap()
    .n
}

async fn audit_count(conn: &mut AsyncPgConnection, operation: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = $1 AND target_type = 'queue' AND target_id = $2 \
           AND actor = 'system' AND source = 'api' \
           AND route_or_command = 'background.load_shed_sampler'",
    )
    .bind::<Text, _>(operation)
    .bind::<Text, _>(QUEUE)
    .get_result::<CountRow>(conn)
    .await
    .unwrap()
    .n
}

struct Reply {
    status: StatusCode,
    retry_after: Option<String>,
    body: Value,
}

async fn post_with(app: &axum::Router, uri: &str, body: Value, headers: &[(&str, &str)]) -> Reply {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(header::RETRY_AFTER)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    Reply {
        status,
        retry_after,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    }
}

async fn post(app: &axum::Router, uri: &str, body: Value) -> Reply {
    post_with(app, uri, body, &[]).await
}

/// Assert the 429 shape of a shed start.
fn assert_shed(reply: &Reply, what: &str) {
    assert_shed_at(reply, what, 60);
}

/// Assert the 429 shape of a shed start whose sampled age is `min_age` or more.
fn assert_shed_at(reply: &Reply, what: &str, min_age: u64) {
    assert_eq!(
        reply.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{what}: {:?}",
        reply.body
    );
    assert_eq!(reply.retry_after.as_deref(), Some("7"), "{what}");
    assert_eq!(reply.body["error"], "load shed", "{what}");
    assert_eq!(reply.body["queue"], QUEUE, "{what}");
    assert_eq!(reply.body["retry_after_secs"], 7, "{what}");
    assert!(
        reply.body["oldest_pending_age_secs"].as_u64().unwrap() >= min_age,
        "{what}"
    );
}

async fn sample(api_state: &HarvestApiState, pool: &DbPool, metrics: &CapturingMetrics) {
    let ok = sample_once(
        api_state.gate_cache().load_shedder(),
        std::slice::from_ref(pool),
        pool,
        Some(metrics),
        &[],
    )
    .await;
    assert!(ok, "the sample must read every pool");
}

/// Shared setup: a configured shedder, a published cache and one existing run.
///
/// Returns the router and the execution id of the existing run.
async fn seed(
    pool: &DbPool,
    conn: &mut AsyncPgConnection,
    metrics: &Arc<CapturingMetrics>,
) -> (HarvestApiState, axum::Router, GlobalCache, String) {
    scrub(conn).await;
    let api_state = build_api_state(pool, Arc::clone(metrics));
    api_state.gate_cache().load_shedder().configure(config());
    let guard = GlobalCache::publish(api_state.gate_cache());
    let app = harvest_api_router(api_state.clone());

    // An existing run. No worker runs, so its task stays PENDING.
    let reply = post(
        &app,
        &format!("/workflows/{WF}/start"),
        json!({ "workflow_id": "existing" }),
    )
    .await;
    assert!(reply.status.is_success(), "seed start: {:?}", reply.body);
    let exec_id = reply.body["execution_id"].as_str().unwrap().to_owned();
    assert_eq!(pending_count(conn).await, 1);
    (api_state, app, guard, exec_id)
}

/// An old backlog sheds a new start with 429 and `Retry-After`. A signal and
/// an attaching start to an existing run pass. The gate holds inside the band
/// and clears after the backlog drains.
#[tokio::test]
async fn backlog_sheds_new_starts_and_clears_after_drain() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    let metrics = Arc::new(CapturingMetrics::default());
    let (api_state, app, _cache, exec_id) = seed(&pool, &mut conn, &metrics).await;
    let start_uri = format!("/workflows/{WF}/start");

    // The backlog is older than trip_age, so the sample trips the queue.
    set_backlog_age(&mut conn, 120).await;
    sample(&api_state, &pool, &metrics).await;
    assert_eq!(audit_count(&mut conn, "load_shed.trip").await, 1);

    // A new start is shed. It writes no execution and no task row.
    let reply = post(&app, &start_uri, json!({ "workflow_id": "fresh-1" })).await;
    assert_shed(&reply, "plain start");
    assert_eq!(executions_with_id(&mut conn, "fresh-1").await, 0);
    assert_eq!(pending_count(&mut conn).await, 1);
    assert_eq!(metrics.rejected(), 1);

    // A keyed start is shed too. Its key reservation rolls back.
    let key = [("idempotency-key", "ls-key-1")];
    let reply = post_with(&app, &start_uri, json!({ "workflow_id": "keyed" }), &key).await;
    assert_shed(&reply, "keyed start");

    // A signal to the existing run is not shed.
    let reply = post(
        &app,
        &format!("/workflows/{exec_id}/signal/poke"),
        json!({ "n": 1 }),
    )
    .await;
    assert_eq!(
        reply.status,
        StatusCode::ACCEPTED,
        "signal: {:?}",
        reply.body
    );

    // A start that attaches to the existing run creates nothing, so it passes.
    let reply = post(
        &app,
        &start_uri,
        json!({ "workflow_id": "existing", "reuse_policy": "allow_duplicate" }),
    )
    .await;
    assert!(reply.status.is_success(), "attach: {:?}", reply.body);
    assert_eq!(reply.body["execution_id"], exec_id);

    // Inside the band (clear < age < trip) the queue stays shed.
    set_backlog_age(&mut conn, 30).await;
    sample(&api_state, &pool, &metrics).await;
    let reply = post(&app, &start_uri, json!({ "workflow_id": "fresh-2" })).await;
    assert_shed_at(&reply, "inside the band", 30);
    assert_eq!(metrics.rejected(), 3);
    assert_eq!(audit_count(&mut conn, "load_shed.clear").await, 0);

    // The backlog drains, so the next sample clears the queue.
    diesel::sql_query("DELETE FROM harvest_task_queue")
        .execute(&mut conn)
        .await
        .unwrap();
    sample(&api_state, &pool, &metrics).await;
    assert_eq!(audit_count(&mut conn, "load_shed.clear").await, 1);
    let reply = post(&app, &start_uri, json!({ "workflow_id": "fresh-3" })).await;
    assert!(reply.status.is_success(), "after clear: {:?}", reply.body);
    assert_eq!(reply.retry_after, None);

    // The shed keyed start left no reservation, so the same key now starts.
    let reply = post_with(&app, &start_uri, json!({ "workflow_id": "keyed" }), &key).await;
    assert!(reply.status.is_success(), "keyed retry: {:?}", reply.body);
    assert_eq!(executions_with_id(&mut conn, "keyed").await, 1);

    // The active gauge went 1, 1, 0 across the three samples.
    assert_eq!(metrics.active(), [true, true, false]);
}

/// The manual gate runs first, so a start that both match gets 503.
#[tokio::test]
async fn manual_gate_wins_over_load_shed() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    let metrics = Arc::new(CapturingMetrics::default());
    let (api_state, app, _cache, _) = seed(&pool, &mut conn, &metrics).await;
    set_backlog_age(&mut conn, 120).await;
    sample(&api_state, &pool, &metrics).await;

    autumn_harvest::admission_gate::db::create_gate(
        &mut conn,
        &GateScope::Queue(QUEUE.to_owned()),
        "ls-incident",
        None,
        "test",
        None,
    )
    .await
    .unwrap();
    let gates = autumn_harvest::admission_gate::db::load_active_gates(&mut conn)
        .await
        .unwrap();
    api_state.gate_cache().refresh(gates);

    let reply = post(
        &app,
        &format!("/workflows/{WF}/start"),
        json!({ "workflow_id": "both" }),
    )
    .await;
    assert_eq!(
        reply.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{:?}",
        reply.body
    );
    assert_eq!(reply.retry_after, None);
    assert_eq!(metrics.rejected(), 0, "a gate block is not a shed");
    assert_eq!(*metrics.blocked.lock().unwrap(), 1);
}

/// Signal-with-start sheds only its create path. A throttled start is shed
/// before it can defer. Batch items are shed per item, and an atomic batch of
/// sheds answers 429.
#[tokio::test]
async fn other_start_routes_shed_fresh_creates() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    let metrics = Arc::new(CapturingMetrics::default());
    let (api_state, app, _cache, _) = seed(&pool, &mut conn, &metrics).await;
    set_backlog_age(&mut conn, 120).await;
    sample(&api_state, &pool, &metrics).await;

    // Signal-with-start: no run means a create, so it is shed.
    let sws_uri = format!("/workflows/{WF}/signal-with-start");
    let reply = post(
        &app,
        &sws_uri,
        json!({ "workflow_id": "sws-new", "signal_name": "poke" }),
    )
    .await;
    assert_shed(&reply, "signal-with-start create");
    assert_eq!(executions_with_id(&mut conn, "sws-new").await, 0);

    // Signal-with-start to the existing run delivers the signal.
    let reply = post(
        &app,
        &sws_uri,
        json!({ "workflow_id": "existing", "signal_name": "poke" }),
    )
    .await;
    assert!(reply.status.is_success(), "sws attach: {:?}", reply.body);

    // A throttled start would defer with 202. It is shed before the defer.
    let reply = post(
        &app,
        &format!("/workflows/{THROTTLED_WF}/start"),
        json!({ "workflow_id": "thr-1" }),
    )
    .await;
    assert_shed(&reply, "throttled start");
    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_start_throttle"
        )
        .await,
        0,
        "a shed start leaves no deferred row"
    );

    // Non-atomic batch: each fresh item is a per-item rejection.
    let reply = post(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": false,
            "items": [
                { "workflow_name": WF, "workflow_id": "b-1" },
                { "workflow_name": WF, "workflow_id": "b-2" },
            ],
        }),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    let results = reply.body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    for item in results {
        assert_eq!(item["status"], "rejected", "{item:?}");
        assert!(
            item["error"].as_str().unwrap().contains("load shed"),
            "{item:?}"
        );
    }

    // Atomic batch of sheds: 429 with Retry-After, nothing inserted.
    let reply = post(
        &app,
        "/workflows/batch_start",
        json!({
            "atomic": true,
            "items": [{ "workflow_name": WF, "workflow_id": "b-3" }],
        }),
    )
    .await;
    assert_eq!(
        reply.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{:?}",
        reply.body
    );
    assert_eq!(reply.retry_after.as_deref(), Some("7"));
    assert_eq!(executions_with_id(&mut conn, "b-3").await, 0);

    // sws create, throttled start, two batch items, one atomic item.
    assert_eq!(metrics.rejected(), 5);
}

/// A failed sample changes no state. The gauge still reports the live state.
#[tokio::test]
async fn failed_sample_keeps_state() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    let metrics = Arc::new(CapturingMetrics::default());
    let (api_state, _app, _cache, _) = seed(&pool, &mut conn, &metrics).await;
    set_backlog_age(&mut conn, 120).await;
    sample(&api_state, &pool, &metrics).await;

    // Port 1 refuses the connection, so the read fails.
    let dead = build_pool("postgres://postgres:postgres@127.0.0.1:1/none");
    let ok = sample_once(
        api_state.gate_cache().load_shedder(),
        &[pool.clone(), dead],
        &pool,
        Some(metrics.as_ref()),
        &[],
    )
    .await;
    assert!(!ok, "a failed read must report failure");
    let cache = api_state.gate_cache();
    assert!(
        cache
            .load_shedder()
            .check(QUEUE, std::time::Instant::now())
            .is_some(),
        "a failed read must not clear the queue"
    );
    assert_eq!(audit_count(&mut conn, "load_shed.clear").await, 0);
    assert_eq!(metrics.active(), [true, true]);
}

/// A hung audit write cannot undo or split a sample. The state change lands
/// before the audit write, and the write is bounded by one sample interval.
#[tokio::test]
async fn hung_audit_write_keeps_the_committed_state() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    let metrics = Arc::new(CapturingMetrics::default());
    let (api_state, _app, _cache, _) = seed(&pool, &mut conn, &metrics).await;
    let fast = LoadShedConfig::new()
        .with_sample_interval(Duration::from_secs(1))
        .queue(
            QUEUE,
            LoadShedPolicy::new(
                Duration::from_secs(60),
                Duration::from_secs(10),
                Duration::from_secs(7),
            )
            .unwrap(),
        );
    api_state.gate_cache().load_shedder().configure(fast);
    set_backlog_age(&mut conn, 120).await;

    // An audit pool of one connection, held here, so the audit write waits.
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
    let audit_pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .unwrap();
    let _held = audit_pool.get().await.unwrap();

    let started = std::time::Instant::now();
    let ok = sample_once(
        api_state.gate_cache().load_shedder(),
        std::slice::from_ref(&pool),
        &audit_pool,
        Some(metrics.as_ref()),
        &[],
    )
    .await;
    assert!(ok, "the read succeeded, so the sample succeeded");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the audit write must be bounded"
    );
    let cache = api_state.gate_cache();
    assert!(
        cache
            .load_shedder()
            .check(QUEUE, std::time::Instant::now())
            .is_some(),
        "the trip must be committed although its audit write hung"
    );
    assert_eq!(metrics.active(), [true]);
}

/// With no policy the shedder never sheds, whatever the backlog.
#[tokio::test]
async fn no_policy_never_sheds() {
    let Some(url) = db_url() else {
        eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
        return;
    };
    let _g = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = build_pool(&url);
    let mut conn = pool.get().await.unwrap();
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let api_state = build_api_state(&pool, Arc::clone(&metrics));
    let _cache = GlobalCache::publish(api_state.gate_cache());
    let app = harvest_api_router(api_state.clone());

    let start_uri = format!("/workflows/{WF}/start");
    let reply = post(&app, &start_uri, json!({ "workflow_id": "a" })).await;
    assert!(reply.status.is_success());
    set_backlog_age(&mut conn, 10_000).await;
    sample(&api_state, &pool, &metrics).await;
    let reply = post(&app, &start_uri, json!({ "workflow_id": "b" })).await;
    assert!(reply.status.is_success(), "{:?}", reply.body);
    assert_eq!(metrics.rejected(), 0);
}
