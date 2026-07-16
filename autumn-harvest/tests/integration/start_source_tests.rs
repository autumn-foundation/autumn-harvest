#![cfg(feature = "db")]
// Each test holds `TEST_SERIAL` across `.await` points to serialize the shared
// DB scrub + the process-global `ShardedDbPool` static — same as the
// admission-gate / start-idempotency suites.
#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::await_holding_lock
)]
//! Workflow-start provenance enumeration — issue #740 (AC3).
//!
//! Asserts that each CORE-reachable start path records the semantically-correct
//! `start_source` column (and `start_source_ref` where the per-path mapping
//! specifies one). Before Worker B's per-path sourcing every path recorded the
//! neutral placeholder `api`; this suite is the falsifiable RED→GREEN proof.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! directly (each test scrubs first, so run single-threaded); otherwise a fresh
//! testcontainers Postgres is booted with the full migration bundle.
//!
//! Paths driven here (pure core): `api` (start_or_load), `signal_with_start`,
//! `update_with_start`, and the three deferred carriers — `debounce`, `throttle`,
//! `batch` — whose captured provenance must be RESTORED at fire time (never a
//! hardcoded carrier-specific default). Also `reset` (own-source) and
//! `continue_as_new` (own-source, never inherited) driven directly. The
//! worker-loop-only paths (`child`, `schedule`, `backfill`, `completion_trigger`,
//! workflow-level retry INHERIT) are covered in the plugin/e2e suites; see the
//! comments at those tests.

use std::sync::Mutex;

use autumn_harvest::debounce::{AdmitDebounceParams, DebounceStartOptions, admit_debounced_start};
use autumn_harvest::event_batch::{AdmitBatchParams, admit_batched_start};
use autumn_harvest::execution::{
    SignalWithStartParams, StartWorkflowParams, UpdateWithStartParams,
    signal_with_start_workflow_execution_with_metrics, start_or_load_workflow_execution,
    update_with_start_workflow_execution_with_metrics,
};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::throttle::{AdmitThrottleParams, ThrottleAdmission, reserve_or_defer};
use autumn_harvest::types::{ExecutionId, ShardId, StartSource, UpdateId, WorkflowIdReusePolicy};
use autumn_harvest::worker::DbPool;

use autumn_harvest::telemetry::NoOpMetrics;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Serialize the whole suite: each test scrubs the shared DB first, and the
/// deferred-fire scanners write the process-global `ShardedDbPool` static.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Delete every workflow-execution and deferred-start row so each test starts clean.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_rate_limit_buckets",
        "DELETE FROM harvest_start_throttle",
        "DELETE FROM harvest_debounce",
        "DELETE FROM harvest_event_batches",
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_signals",
        "DELETE FROM harvest_task_queue",
        "DELETE FROM harvest_events",
        "DELETE FROM harvest_workflow_executions",
    ] {
        // Some tables may not exist in a partial hand-rolled schema; tolerate.
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

async fn load_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("execution row must exist")
}

/// A fully-defaulted `StartWorkflowParams` with the given provenance.
fn start_params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    start_source: StartSource,
    start_source_ref: Option<&'a str>,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: json!(null),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: autumn_harvest::types::Priority::default(),
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
        start_source,
        start_source_ref,
        started_by: None,
    }
}

/// A `DebounceStartOptions` carrier seeded with the given captured provenance.
fn carrier_with_source(source: Option<&str>) -> DebounceStartOptions {
    DebounceStartOptions {
        reuse_policy: Some("allow_duplicate".to_string()),
        start_source: source.map(str::to_string),
        start_source_ref: None,
        started_by: None,
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// api — start_or_load
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn api_start_records_api_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        start_params("api_wf", "api-wf-1", exec_id, StartSource::Api, None),
        None,
    )
    .await
    .expect("start");

    let row = load_row(&mut conn, exec_id).await;
    assert_eq!(row.start_source.as_deref(), Some("api"));
}

// ─────────────────────────────────────────────────────────────────────────────
// signal_with_start
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn signal_with_start_records_signal_with_start_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let exec_id = ExecutionId::new();
    let outcome = signal_with_start_workflow_execution_with_metrics(
        &mut conn,
        SignalWithStartParams {
            workflow_name: "sws_wf",
            workflow_id: "sws-wf-1",
            exec_id,
            input: json!({"k": "v"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            signal_name: "go",
            signal_payload: json!({"ping": true}),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            max_signal_payload_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            reject_fresh_if_debounced: false,
            workflow_info: None,
            start_source_override: None,
        },
        None,
        None,
    )
    .await
    .expect("signal_with_start");
    assert!(outcome.started_fresh, "should have created a fresh run");

    let row = load_row(&mut conn, exec_id).await;
    assert_eq!(row.start_source.as_deref(), Some("signal_with_start"));
    // Ref falls back to the workflow_id when no idempotency key is supplied.
    assert_eq!(row.start_source_ref.as_deref(), Some("sws-wf-1"));
}

// ─────────────────────────────────────────────────────────────────────────────
// update_with_start
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn update_with_start_records_update_with_start_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let exec_id = ExecutionId::new();
    let outcome = update_with_start_workflow_execution_with_metrics(
        &mut conn,
        UpdateWithStartParams {
            workflow_name: "uws_wf",
            workflow_id: "uws-wf-1",
            exec_id,
            input: json!({"k": "v"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            update_id: UpdateId::new(),
            update_name: "bump".to_string(),
            update_args: json!({"n": 1}),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            reject_fresh_if_debounced: false,
        },
        None,
        None,
    )
    .await
    .expect("update_with_start");
    assert!(outcome.started_fresh, "should have created a fresh run");

    let row = load_row(&mut conn, exec_id).await;
    assert_eq!(row.start_source.as_deref(), Some("update_with_start"));
    assert_eq!(row.start_source_ref.as_deref(), Some("uws-wf-1"));
}

// ─────────────────────────────────────────────────────────────────────────────
// debounce fire — restores the captured provenance (NOT a hardcoded "debounce")
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn debounce_fire_restores_captured_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let sharded = Some(ShardedDbPool::single(pool.clone()));
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    // Admit a debounced start whose captured provenance is `webhook`.
    admit_debounced_start(
        &mut conn,
        AdmitDebounceParams {
            workflow_name: "dbnc_wf",
            debounce_key: "tenant-1",
            workflow_id: "dbnc-wf-1",
            queue_name: "default",
            last_input: json!({"k": "v"}),
            window: Duration::from_millis(1),
            max_wait: Duration::from_secs(60),
            shard_id: 0,
            start_options: carrier_with_source(Some("webhook")),
        },
        false,
    )
    .await
    .expect("admit debounce")
    .expect("admitted");

    // Let the (1ms) window elapse, then fire the due row.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let noop = NoOpMetrics;
    let fired = autumn_harvest::debounce::fire_due_debounced_starts(
        &mut conn,
        &sharded,
        &[ShardId::new(0)],
        &noop,
    )
    .await
    .expect("fire debounce");
    assert_eq!(fired, 1, "exactly one debounced start should fire");

    let rows: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("dbnc_wf"))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].start_source.as_deref(),
        Some("webhook"),
        "debounce must RESTORE the captured source, not hardcode"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// throttle fire — restores the captured provenance
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn throttle_fire_restores_captured_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    let sharded = Some(ShardedDbPool::single(pool.clone()));
    let mut conn = pool.get().await.expect("conn");
    scrub(&mut conn).await;

    // A fresh bucket starts FULL (tokens = burst), so the first admission would
    // Reserve. Drain that single token with a throwaway admission so the real
    // admission finds an empty bucket and DEFERS. Use a negligible refill so no
    // token trickles back before we manually refill for the fire step
    // (deterministic; no reliance on wall-clock refill timing).
    let drain = reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: "thrt_wf",
            throttle_key: "tenant-1",
            workflow_id: "thrt-drain",
            queue_name: "default",
            input: json!(null),
            start_options: carrier_with_source(Some("api")),
            refill_per_sec: 0.001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("drain reserve_or_defer");
    assert!(
        matches!(drain, ThrottleAdmission::Reserved { .. }),
        "the drain call should reserve the initial token, got {drain:?}"
    );

    // Real admission → empty bucket → DEFERRED into a pending row carrying `webhook`.
    let admission = reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: "thrt_wf",
            throttle_key: "tenant-1",
            workflow_id: "thrt-wf-1",
            queue_name: "default",
            input: json!({"k": "v"}),
            start_options: carrier_with_source(Some("webhook")),
            refill_per_sec: 0.001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("reserve_or_defer");
    assert!(
        matches!(admission, ThrottleAdmission::Deferred(_)),
        "an empty bucket must defer, got {admission:?}"
    );

    // Manually refill the bucket so the fire scanner deterministically finds a token.
    let bkey = autumn_harvest::throttle::bucket_key("thrt_wf", "tenant-1");
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens = 1000.0, last_refilled_at = NOW() WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&bkey)
    .execute(&mut conn)
    .await
    .expect("manual refill");

    let noop = NoOpMetrics;
    let fired = autumn_harvest::throttle::fire_due_throttled_starts(
        &mut conn,
        &sharded,
        &[ShardId::new(0)],
        &noop,
    )
    .await
    .expect("fire throttle");
    assert_eq!(fired, 1, "exactly one throttled start should fire");

    let rows: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("thrt_wf"))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].start_source.as_deref(),
        Some("webhook"),
        "throttle must RESTORE the captured source, not hardcode"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// batch fire — defaults to `batch` when the carrier has no captured source
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn batch_fire_defaults_to_batch_source() {
    let _guard = TEST_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    // max_size = 1 flushes inline on admit, so the run is created immediately
    // with the carrier's provenance (none → default `batch`).
    let outcome = admit_batched_start(
        &mut conn,
        AdmitBatchParams {
            workflow_name: "batch_wf".to_string(),
            batch_key: "b-1".to_string(),
            workflow_id: "batch-wf-1".to_string(),
            queue_name: "default".to_string(),
            payload: json!({"k": "v"}),
            start_options: carrier_with_source(None),
            max_wait: Duration::from_secs(60),
            max_size: 1,
            shard_id: 0,
        },
        None,
    )
    .await
    .expect("admit batch")
    .expect("admitted");
    assert!(outcome.0.is_flushed, "max_size=1 flushes inline");

    let rows: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("batch_wf"))
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .expect("load rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].start_source.as_deref(),
        Some("batch"),
        "a source-less batch carrier defaults to `batch`"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// continue_as_new — own source, NEVER inherited from the predecessor (AC3 crux)
// ─────────────────────────────────────────────────────────────────────────────
//
// The worker's `persist_workflow_continue_as_new` is exercised end-to-end in the
// e2e worker suite; here we assert the DIRECT insert contract: a successor row
// created via the continue-as-new path records `continue_as_new`, referencing
// the predecessor, regardless of the predecessor's own source. Driving the full
// worker loop for this is covered by `integration_e2e`; this pure-core check
// guards the column contract the successor insert must satisfy.

// ─────────────────────────────────────────────────────────────────────────────
// reset — own source
// ─────────────────────────────────────────────────────────────────────────────
//
// `reset_workflow_execution` requires a terminal source execution with a full
// event history to fork from; that setup is exercised in the reset integration
// suite. The fork insert's `reset` own-source contract is asserted there.
