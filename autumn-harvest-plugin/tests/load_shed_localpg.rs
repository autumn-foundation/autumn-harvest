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
use autumn_harvest::admission_gate::set_global_admission_gate_cache;
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
const QUEUE: &str = "default";

/// Records the two load-shed metrics.
#[derive(Default)]
struct CapturingMetrics {
    active: Mutex<Vec<(String, bool)>>,
    rejected: Mutex<Vec<String>>,
}

impl MetricsRecorder for CapturingMetrics {
    fn record_load_shed_active(&self, queue: &str, active: bool) {
        self.active.lock().unwrap().push((queue.to_owned(), active));
    }
    fn record_load_shed_rejected(&self, queue: &str) {
        self.rejected.lock().unwrap().push(queue.to_owned());
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

fn build_api_state(pool: &DbPool, metrics: Arc<CapturingMetrics>) -> HarvestApiState {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info(WF)],
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
fn config() -> LoadShedConfig {
    let policy = LoadShedPolicy::new(
        Duration::from_secs(60),
        Duration::from_secs(10),
        Duration::from_secs(7),
    )
    .expect("valid policy");
    LoadShedConfig::new().queue(QUEUE, policy)
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_admission_gates",
        "DELETE FROM harvest_task_queue",
        "DELETE FROM harvest_signals",
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

async fn pending_count(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE queue_name = $1 AND state = 'PENDING'",
    )
    .bind::<Text, _>(QUEUE)
    .get_result::<CountRow>(conn)
    .await
    .unwrap()
    .n
}

async fn audit_count(conn: &mut AsyncPgConnection, operation: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = $1 AND target_type = 'queue' AND target_id = $2",
    )
    .bind::<Text, _>(operation)
    .bind::<Text, _>(QUEUE)
    .get_result::<CountRow>(conn)
    .await
    .unwrap()
    .n
}

async fn post(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Option<String>, Value) {
    let resp = app
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
        .unwrap();
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(header::RETRY_AFTER)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        retry_after,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
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

/// RED for issue #1794: an old backlog sheds a new start with 429 and
/// `Retry-After`. A signal and an attaching start to an existing run pass.
/// The gate holds inside the band and clears after the backlog drains.
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
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let api_state = build_api_state(&pool, Arc::clone(&metrics));
    api_state.gate_cache().load_shedder().configure(config());
    set_global_admission_gate_cache(Some(api_state.gate_cache()));
    let app = harvest_api_router(api_state.clone());

    // An existing run. No worker runs, so its task stays PENDING.
    let start_uri = format!("/workflows/{WF}/start");
    let (status, _, body) = post(&app, &start_uri, json!({ "workflow_id": "existing" })).await;
    assert!(status.is_success(), "seed start: {status} {body:?}");
    let exec_id = body["execution_id"].as_str().unwrap().to_owned();
    assert_eq!(pending_count(&mut conn).await, 1);

    // The backlog is older than trip_age, so the sample trips the queue.
    set_backlog_age(&mut conn, 120).await;
    sample(&api_state, &pool, &metrics).await;
    assert_eq!(audit_count(&mut conn, "load_shed.trip").await, 1);

    // A new start is shed with 429 and Retry-After.
    let (status, retry_after, body) =
        post(&app, &start_uri, json!({ "workflow_id": "fresh-1" })).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body:?}");
    assert_eq!(retry_after.as_deref(), Some("7"));
    assert_eq!(body["error"], "load shed");
    assert_eq!(body["queue"], QUEUE);
    assert_eq!(body["retry_after_secs"], 7);
    assert!(body["oldest_pending_age_secs"].as_u64().unwrap() >= 60);
    assert_eq!(
        metrics.rejected.lock().unwrap().as_slice(),
        [QUEUE.to_owned()]
    );

    // A signal to the existing run is not shed.
    let (status, _, body) = post(
        &app,
        &format!("/workflows/{exec_id}/signal/poke"),
        json!({ "n": 1 }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "signal must pass: {body:?}");

    // A start that attaches to the existing run creates nothing, so it passes.
    let (status, _, body) = post(
        &app,
        &start_uri,
        json!({ "workflow_id": "existing", "reuse_policy": "allow_duplicate" }),
    )
    .await;
    assert!(status.is_success(), "attach must pass: {status} {body:?}");
    assert_eq!(body["execution_id"], exec_id);

    // Inside the band (clear < age < trip) the queue stays shed.
    set_backlog_age(&mut conn, 30).await;
    sample(&api_state, &pool, &metrics).await;
    let (status, _, _) = post(&app, &start_uri, json!({ "workflow_id": "fresh-2" })).await;
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "hysteresis must hold"
    );
    assert_eq!(audit_count(&mut conn, "load_shed.clear").await, 0);

    // The backlog drains, so the next sample clears the queue.
    diesel::sql_query("DELETE FROM harvest_task_queue")
        .execute(&mut conn)
        .await
        .unwrap();
    sample(&api_state, &pool, &metrics).await;
    assert_eq!(audit_count(&mut conn, "load_shed.clear").await, 1);
    let (status, retry_after, body) =
        post(&app, &start_uri, json!({ "workflow_id": "fresh-3" })).await;
    set_global_admission_gate_cache(None);
    assert!(status.is_success(), "start after clear: {status} {body:?}");
    assert_eq!(retry_after, None);

    // The active gauge went 1, 1, 0 across the three samples.
    let active: Vec<bool> = metrics
        .active
        .lock()
        .unwrap()
        .iter()
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(active, [true, true, false]);
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
    set_global_admission_gate_cache(Some(api_state.gate_cache()));
    let app = harvest_api_router(api_state.clone());

    let start_uri = format!("/workflows/{WF}/start");
    let (status, _, _) = post(&app, &start_uri, json!({ "workflow_id": "a" })).await;
    assert!(status.is_success());
    set_backlog_age(&mut conn, 10_000).await;
    sample(&api_state, &pool, &metrics).await;
    let (status, _, body) = post(&app, &start_uri, json!({ "workflow_id": "b" })).await;
    set_global_admission_gate_cache(None);
    assert!(status.is_success(), "{status} {body:?}");
    assert!(metrics.active.lock().unwrap().is_empty());
}
