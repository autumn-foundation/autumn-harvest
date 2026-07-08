#![cfg(feature = "db")]
// Test-code style lints (consistent with other integration test files).
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::cast_precision_loss
)]
//! Workflow-start throttle integration tests — issue #607.
//!
//! Verifies the core throttle ACs against a real Postgres container:
//! - **Success metric** — a burst of K distinct starts against rate N + burst B:
//!   admitted-start rate ≤ N+B, zero dropped, zero rejected, all K eventually run.
//! - **Durable across restart** — deferred rows survive a connection drop; the
//!   scanner drains them.
//! - **AC-a** — an id-reuse short-circuit at fire time refunds the token.
//! - **AC-c** — a start deferred past `schedule_to_start` is dropped, not run.
//! - **Independent keys** — distinct keys throttle independently.
//! - **Operator visibility** — the per-key backlog read returns the counts.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::throttle::{
    AdmitThrottleParams, THROTTLE_FIRE_BATCH_SIZE, THROTTLE_FIRE_PER_KEY_CAP, ThrottleAdmission,
    bucket_key, fire_due_throttled_starts, reserve_or_defer, throttle_backlog_by_key,
};
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{
    DagCatalog, SchedulerMonitor, StartWorkflowParams, WorkflowContext,
    start_or_load_workflow_execution, tick_once,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// All migrations up through the start-throttle table.
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    "\n",
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    "\n",
    // issue #607: the start-throttle table under test.
    include_str!("../../migrations/20260706000001_harvest_start_throttle/up.sql"),
    "\n",
    // issue #606: harvest_task_queue.session_id (worker sessions), merged in from trunk-dev.
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #607 code review: companion index for the per-key-fair scanner query.
    include_str!(
        "../../migrations/20260707000000_harvest_start_throttle_bucket_deferred_idx/up.sql"
    ),
    "\n",
    // issue #607 code review: index for the (workflow_name, workflow_id)
    // idempotent-retry lookup.
    include_str!("../../migrations/20260708000000_harvest_start_throttle_workflow_id_idx/up.sql"),
);

// ── Metrics recorder ─────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    throttled: Mutex<Vec<String>>,
    schedule_runs: Mutex<Vec<(String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_start_throttled(&self, workflow_name: &str) {
        self.throttled
            .lock()
            .unwrap()
            .push(workflow_name.to_owned());
    }

    fn record_schedule_run(&self, kind: &str, name: &str) {
        self.schedule_runs
            .lock()
            .unwrap()
            .push((kind.to_owned(), name.to_owned()));
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db() -> (AsyncPgConnection, String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

    let mut conn = connect(&url).await;
    conn.batch_execute(INIT_SQL).await.expect("migrations");
    (conn, url, container)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn scalar_i64(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(sql)
        .get_result::<N>(conn)
        .await
        .expect("scalar query")
        .n
}

async fn throttle_row_count(conn: &mut AsyncPgConnection, key: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_start_throttle WHERE throttle_key=$1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<N>(conn)
        .await
        .expect("row count")
        .n
}

async fn execution_count(conn: &mut AsyncPgConnection, wf: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name=$1",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .get_result::<N>(conn)
    .await
    .expect("exec count")
    .n
}

/// Force an existing execution's state to FAILED (simulates a terminal run
/// for `allow_duplicate_failed_only` bypass-decision tests).
async fn mark_execution_failed(conn: &mut AsyncPgConnection, wf: &str, wf_id: &str) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state='FAILED', completed_at=NOW() \
         WHERE workflow_name=$1 AND workflow_id=$2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(wf_id)
    .execute(conn)
    .await
    .expect("mark failed");
}

/// Force a bucket's token balance (simulates refill deterministically).
async fn set_bucket_tokens(conn: &mut AsyncPgConnection, key: &str, tokens: f64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens=$2, last_refilled_at=NOW() WHERE key=$1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(tokens)
    .execute(conn)
    .await
    .expect("set tokens");
}

async fn bucket_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct T {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key=$1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<T>(conn)
        .await
        .expect("tokens")
        .tokens
}

fn params<'a>(
    wf: &'a str,
    key: &'a str,
    wf_id: &'a str,
    input: serde_json::Value,
    refill_per_sec: f64,
    burst: f64,
    schedule_to_start: Option<Duration>,
    reuse: Option<&str>,
) -> AdmitThrottleParams<'a> {
    AdmitThrottleParams {
        workflow_name: wf,
        throttle_key: key,
        workflow_id: wf_id,
        queue_name: "default",
        input,
        start_options: DebounceStartOptions {
            reuse_policy: reuse.map(str::to_string),
            ..Default::default()
        },
        refill_per_sec,
        burst,
        schedule_to_start,
        shard_id: 0,
    }
}

/// Start an execution directly (used to admit a Reserved start in-test).
async fn start(conn: &mut AsyncPgConnection, wf: &str, wf_id: &str, input: serde_json::Value) {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: wf,
            workflow_id: wf_id,
            exec_id: ExecutionId::new(),
            input,
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
            priority: Default::default(),
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
        },
    )
    .await
    .expect("start");
}

async fn drain(conn: &mut AsyncPgConnection, metrics: &RecordingMetrics) -> usize {
    fire_due_throttled_starts(conn, &None, &[] as &[ShardId], metrics)
        .await
        .expect("fire due")
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Success metric: a burst against rate N + burst B admits ≤ N+B, defers the
/// rest, drops/rejects nothing, and every start eventually runs.
#[tokio::test]
async fn burst_paces_and_all_eventually_run() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();

    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);
    let burst = 3.0;
    // Effectively no auto-refill during the test; we drive refill by hand.
    let rate = 0.0001;

    // K = 10 distinct starts. First `burst` reserve a token; the rest defer.
    let mut reserved = 0;
    let mut deferred = 0;
    for i in 0..10 {
        let wf_id = format!("job-{i}");
        let admit = reserve_or_defer(
            &mut conn,
            params(
                wf,
                key,
                &wf_id,
                serde_json::json!({ "n": i }),
                rate,
                burst,
                None,
                None,
            ),
        )
        .await
        .expect("reserve_or_defer");
        match admit {
            ThrottleAdmission::Reserved { .. } => {
                reserved += 1;
                start(&mut conn, wf, &wf_id, serde_json::json!({ "n": i })).await;
            }
            ThrottleAdmission::Deferred(_) => {
                deferred += 1;
                metrics.record_start_throttled(wf);
            }
            ThrottleAdmission::Bypassed => panic!("no prior execution exists for this workflow_id"),
        }
    }

    // At most `burst` admitted immediately; the rest deferred (nothing dropped).
    assert_eq!(reserved, 3, "burst B starts admitted immediately");
    assert_eq!(deferred, 7, "the excess is deferred, not dropped");
    assert_eq!(throttle_row_count(&mut conn, key).await, 7);
    assert_eq!(metrics.throttled.lock().unwrap().len(), 7);

    // Drain in paced refill steps. Each refill tops the bucket to `burst`; the
    // token bucket caps available tokens at `burst` (`LEAST(burst, tokens…)`), so
    // a single scanner pass can never admit more than `burst` starts — the rate
    // bound. Loop refills until the backlog drains: all 7 eventually run.
    let mut total_fired = 0;
    while throttle_row_count(&mut conn, key).await > 0 {
        set_bucket_tokens(&mut conn, &bkey, burst).await;
        let fired = drain(&mut conn, &metrics).await;
        assert!(
            fired as f64 <= burst,
            "a single scanner pass admits at most `burst` starts (rate bound)"
        );
        assert!(fired > 0, "a full-burst refill makes progress");
        total_fired += fired;
    }
    assert_eq!(
        total_fired, 7,
        "every deferred start eventually fires; none dropped"
    );

    // All K eventually ran: 3 reserved + 7 fired = 10 executions, zero rejected.
    assert_eq!(execution_count(&mut conn, wf).await, 10, "all K starts ran");
}

/// Deferred starts are durable: they survive a connection drop and are drained
/// by a fresh connection (no in-memory pending queue).
#[tokio::test]
async fn deferred_starts_survive_restart() {
    let (mut conn, url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);

    // Empty the bucket so every start defers.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed"); // consumes the single burst token
    for i in 0..4 {
        let wf_id = format!("job-{i}");
        let admit = reserve_or_defer(
            &mut conn,
            params(
                wf,
                key,
                &wf_id,
                serde_json::json!({ "n": i }),
                0.0001,
                1.0,
                None,
                None,
            ),
        )
        .await
        .expect("defer");
        assert!(matches!(admit, ThrottleAdmission::Deferred(_)));
    }
    assert_eq!(throttle_row_count(&mut conn, key).await, 4);

    // "Restart": drop the connection, reconnect fresh.
    drop(conn);
    let mut conn2 = connect(&url).await;

    // The rows are still there; drain them in paced steps (burst = 1, so one per
    // refill). All four survive the restart and fire — none lost.
    assert_eq!(
        throttle_row_count(&mut conn2, key).await,
        4,
        "rows durable across restart"
    );
    let mut total_fired = 0;
    while throttle_row_count(&mut conn2, key).await > 0 {
        set_bucket_tokens(&mut conn2, &bkey, 1.0).await;
        total_fired += drain(&mut conn2, &metrics).await;
    }
    assert_eq!(
        total_fired, 4,
        "all deferred starts fire after restart, none lost"
    );
    assert_eq!(throttle_row_count(&mut conn2, key).await, 0);
}

/// AC-a / bypass: a `reject_duplicate` admission for a `workflow_id` that
/// already has an active execution is bypassed immediately at
/// `reserve_or_defer` time — no token consumed, no pending row ever written.
/// (Prior to the bypass fix this deferred and only short-circuited at fire
/// time; the bypass check now closes the window where the caller was told
/// `202 throttled` for a start that could never be admitted.)
#[tokio::test]
async fn reject_duplicate_bypasses_throttle_when_execution_already_active() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);
    let wf_id = "dup-job";

    // A live execution already owns this workflow_id.
    start(&mut conn, wf, wf_id, serde_json::json!({})).await;
    let execs_before = execution_count(&mut conn, wf).await;

    // Empty the bucket first (seed consumes the burst token) so a non-bypassed
    // admission would have had to defer.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed");

    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            Some("reject_duplicate"),
        ),
    )
    .await
    .expect("bypass");
    assert!(
        matches!(admit, ThrottleAdmission::Bypassed),
        "an already-active workflow_id under reject_duplicate must bypass throttle, got {admit:?}"
    );

    assert_eq!(
        throttle_row_count(&mut conn, key).await,
        0,
        "no pending row written for a bypassed admission"
    );
    assert_eq!(
        execution_count(&mut conn, wf).await,
        execs_before,
        "no new execution created by the bypass check itself"
    );
    // No token was ever touched by the bypassed admission (still at 0 from
    // the seed consuming the sole burst token — a bypass neither reserves
    // nor needs to refund).
    assert!(
        bucket_tokens(&mut conn, &bkey).await < 0.01,
        "bypass must not touch the token bucket"
    );
}

/// `allow_duplicate_failed_only` does NOT bypass when the existing execution
/// is still non-terminal (e.g. RUNNING) relative to that policy's own
/// "only replace a FAILED/CANCELLED prior" semantics... actually it DOES
/// bypass (any non-terminal-per-`try_load_by_key` state under this policy
/// resolves to "return existing unchanged" except FAILED/CANCELLED, which is
/// a genuine fresh start). This test locks in that FAILED/CANCELLED does NOT
/// bypass -- it's the one state pair where a fresh admission is genuinely
/// needed and throttle pacing must still apply.
#[tokio::test]
async fn allow_duplicate_failed_only_does_not_bypass_a_failed_prior() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "retry-job";

    start(&mut conn, wf, wf_id, serde_json::json!({})).await;
    mark_execution_failed(&mut conn, wf, wf_id).await;

    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            100.0,
            10.0,
            None,
            Some("allow_duplicate_failed_only"),
        ),
    )
    .await
    .expect("admit");
    assert!(
        matches!(admit, ThrottleAdmission::Reserved { .. }),
        "a FAILED prior under allow_duplicate_failed_only is a genuine fresh \
         start and must still go through throttle pacing, got {admit:?}"
    );
}

/// `terminate_if_running` never bypasses, even when an active execution
/// exists -- it always starts fresh (cancel + replace), so it must still
/// consume throttle pacing like any other genuine admission.
#[tokio::test]
async fn terminate_if_running_never_bypasses() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "force-restart-job";

    start(&mut conn, wf, wf_id, serde_json::json!({})).await;

    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            100.0,
            10.0,
            None,
            Some("terminate_if_running"),
        ),
    )
    .await
    .expect("admit");
    assert!(
        matches!(admit, ThrottleAdmission::Reserved { .. }),
        "terminate_if_running always starts fresh and must never bypass \
         throttle pacing, got {admit:?}"
    );
}

/// AC-a residual case: a row genuinely deferred (no active execution existed
/// at admission time, so the bypass check did not fire) can still race
/// against a *concurrent, unrelated* start for the same `workflow_id` landing
/// before the scanner fires it. The fire-time `AlreadyExists` short-circuit
/// must still refund the token and drop the row without creating a second
/// execution.
#[tokio::test]
async fn fire_time_already_exists_still_refunds_token() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);
    let wf_id = "race-job";

    // No active execution yet -- this genuinely defers (bypass does not fire).
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed");
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            Some("reject_duplicate"),
        ),
    )
    .await
    .expect("defer");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    // A concurrent, unrelated start now claims the same workflow_id before
    // the scanner drains the deferred row.
    start(&mut conn, wf, wf_id, serde_json::json!({})).await;
    let execs_before = execution_count(&mut conn, wf).await;

    // Refill exactly 1 token and drain: the fire consumes it, start_or_load
    // returns AlreadyExists, the row is dropped, and the token is refunded.
    set_bucket_tokens(&mut conn, &bkey, 1.0).await;
    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(fired, 0, "the doomed start is not counted as a fired run");
    assert_eq!(throttle_row_count(&mut conn, key).await, 0, "row dropped");
    assert_eq!(
        execution_count(&mut conn, wf).await,
        execs_before,
        "no new execution created"
    );
    assert!(
        bucket_tokens(&mut conn, &bkey).await >= 0.99,
        "token refunded after fire-time id-reuse short-circuit"
    );
}

/// AC-c: a start deferred past its `schedule_to_start` deadline is dropped by
/// the scanner rather than run stale.
#[tokio::test]
async fn deferred_past_schedule_to_start_times_out() {
    use diesel_async::RunQueryDsl;
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);

    // Defer one start with a (short) schedule_to_start deadline. Seed empties
    // the bucket so this one defers.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed");
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "slow-job",
            serde_json::json!({}),
            0.0001,
            1.0,
            Some(Duration::from_secs(300)),
            None,
        ),
    )
    .await
    .expect("defer");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    // Force the deadline into the past.
    diesel::sql_query(
        "UPDATE harvest_start_throttle SET expires_at = NOW() - INTERVAL '1 hour' WHERE throttle_key=$1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(&mut conn)
    .await
    .expect("expire");

    // Even with a token available, the expired row is dropped, not started.
    set_bucket_tokens(&mut conn, &bkey, 5.0).await;
    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(fired, 0, "an expired deferred start is not run");
    assert_eq!(
        throttle_row_count(&mut conn, key).await,
        0,
        "expired row dropped"
    );
    assert_eq!(
        execution_count(&mut conn, wf).await,
        0,
        "no stale run created"
    );
}

/// Distinct keys throttle independently — one key's exhausted bucket does not
/// defer another key's start.
#[tokio::test]
async fn independent_keys_throttle_independently() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";

    // Key A: burst 1, consume it, then the next A start defers.
    let a1 = reserve_or_defer(
        &mut conn,
        params(
            wf,
            "A",
            "a1",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .unwrap();
    let a2 = reserve_or_defer(
        &mut conn,
        params(
            wf,
            "A",
            "a2",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .unwrap();
    assert!(matches!(a1, ThrottleAdmission::Reserved { .. }));
    assert!(matches!(a2, ThrottleAdmission::Deferred(_)));

    // Key B has its own fresh bucket — its first start is admitted, not deferred.
    let b1 = reserve_or_defer(
        &mut conn,
        params(
            wf,
            "B",
            "b1",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(b1, ThrottleAdmission::Reserved { .. }),
        "key B is unaffected by key A's exhausted bucket"
    );

    assert_eq!(throttle_row_count(&mut conn, "A").await, 1);
    assert_eq!(throttle_row_count(&mut conn, "B").await, 0);
}

/// Operator visibility (< 1 s): the per-key backlog read returns deferred counts.
#[tokio::test]
async fn backlog_read_returns_per_key_counts() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";

    // Two keys with different backlog sizes.
    for (key, n) in [("A", 3), ("B", 1)] {
        // Seed consumes the burst token so subsequent starts defer.
        reserve_or_defer(
            &mut conn,
            params(
                wf,
                key,
                "seed",
                serde_json::json!({}),
                0.0001,
                1.0,
                None,
                None,
            ),
        )
        .await
        .unwrap();
        for i in 0..n {
            reserve_or_defer(
                &mut conn,
                params(
                    wf,
                    key,
                    &format!("{key}-{i}"),
                    serde_json::json!({}),
                    0.0001,
                    1.0,
                    None,
                    None,
                ),
            )
            .await
            .unwrap();
        }
    }

    let backlog = throttle_backlog_by_key(&mut conn).await.expect("backlog");
    let a = backlog
        .iter()
        .find(|e| e.throttle_key == "A")
        .expect("key A present");
    let b = backlog
        .iter()
        .find(|e| e.throttle_key == "B")
        .expect("key B present");
    assert_eq!(a.deferred_count, 3);
    assert_eq!(b.deferred_count, 1);
    assert_eq!(a.workflow_name, wf);

    // A no-op sanity read for the raw table count too.
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_start_throttle"
        )
        .await,
        4
    );
}

/// A retry of a request whose start is already durably deferred (e.g. a client
/// timing out before it saw the first `202` and retrying) must not create a
/// second independent pending row for the same `workflow_id` — it should land on
/// the *same* queued admission (code-review fix, issue #607).
#[tokio::test]
async fn retry_with_same_workflow_id_does_not_create_a_second_pending_row() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "retry-job";

    // Empty the bucket so both the original request and its retry defer.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed consumes the burst token");

    let first = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "n": 1 }),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("first admission defers");
    let first_outcome = match first {
        ThrottleAdmission::Deferred(o) => o,
        ThrottleAdmission::Reserved { .. } => panic!("expected Deferred"),
        ThrottleAdmission::Bypassed => panic!("no prior execution exists for this workflow_id"),
    };
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);

    // The client retries the identical request (same workflow_id) before ever
    // observing the first response.
    let retry = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "n": 1 }),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("retry admission defers");
    let retry_outcome = match retry {
        ThrottleAdmission::Deferred(o) => o,
        ThrottleAdmission::Reserved { .. } => panic!("expected Deferred"),
        ThrottleAdmission::Bypassed => panic!("no prior execution exists for this workflow_id"),
    };

    // No second row was inserted.
    assert_eq!(
        throttle_row_count(&mut conn, key).await,
        1,
        "retry must not insert a second pending row for the same workflow_id"
    );
    assert_eq!(retry_outcome.workflow_id, first_outcome.workflow_id);

    // The retry observed the ORIGINAL row's deferred_at (not a fresh insert): a
    // third identical call must return the exact same DB-round-tripped
    // timestamp as the retry did (both go through the idempotency lookup, so
    // comparing them avoids an in-memory-vs-Postgres timestamp precision
    // mismatch against `first_outcome`, whose `deferred_at` was captured
    // client-side with nanosecond precision before Postgres's microsecond
    // truncation on insert).
    let third = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "n": 1 }),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("third admission also defers to the same row");
    let third_outcome = match third {
        ThrottleAdmission::Deferred(o) => o,
        ThrottleAdmission::Reserved { .. } => panic!("expected Deferred"),
        ThrottleAdmission::Bypassed => panic!("no prior execution exists for this workflow_id"),
    };
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);
    assert_eq!(third_outcome.deferred_at, retry_outcome.deferred_at);
}

/// Code-review fix (issue #607): a retry carrying the same `workflow_id` but
/// resolving to a *different* throttle key must still land on the original
/// pending row -- even when the new key's bucket has an available token --
/// rather than winning `Reserved` under the new key and leaving the old
/// pending row orphaned (silently dropped later as a duplicate).
#[tokio::test]
async fn retry_resolving_to_a_different_key_still_defers_to_the_original_row() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key1 = "tenant-a";
    let key2 = "tenant-b";
    let wf_id = "cross-key-retry-job";

    // key1's bucket is empty -- the original admission genuinely defers.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key1,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed consumes key1's burst token");
    let first = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key1,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("first admission defers under key1");
    let first_outcome = match first {
        ThrottleAdmission::Deferred(o) => o,
        other => panic!("expected Deferred, got {other:?}"),
    };
    assert_eq!(first_outcome.throttle_key, key1);

    // The retry resolves to key2 (e.g. the resolved-key expression's input
    // changed between attempts), whose bucket has a fresh, available token.
    let retry = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key2,
            wf_id,
            serde_json::json!({}),
            100.0,
            10.0,
            None,
            None,
        ),
    )
    .await
    .expect("retry resolves to key2");
    let retry_outcome = match retry {
        ThrottleAdmission::Deferred(o) => o,
        other => panic!(
            "expected Deferred referencing the original key1 row, got {other:?} -- \
             the retry must not win Reserved under the newly-resolved key2"
        ),
    };
    assert_eq!(
        retry_outcome.throttle_key, key1,
        "retry must land on the ORIGINAL pending row (key1), not reserve fresh under key2"
    );
    assert_eq!(retry_outcome.workflow_id, first_outcome.workflow_id);
    assert_eq!(
        throttle_row_count(&mut conn, key1).await,
        1,
        "exactly one pending row exists, under key1"
    );
    assert_eq!(
        throttle_row_count(&mut conn, key2).await,
        0,
        "no row was ever inserted under key2"
    );
}

/// P1 code-review fix: a single throttle key whose backlog exceeds
/// `THROTTLE_FIRE_BATCH_SIZE` must not starve a *different* key's newer,
/// ready-to-fire row. Before the fix the scanner's claim query was a flat
/// `ORDER BY deferred_at ASC LIMIT THROTTLE_FIRE_BATCH_SIZE` — with more than
/// `THROTTLE_FIRE_BATCH_SIZE` older rows all under one exhausted key, the
/// claim would select *only* that key's rows every tick, never reaching a
/// newer row under any other key. The per-key-fair CTE caps how many rows one
/// `bucket_key` can contribute per tick (`THROTTLE_FIRE_PER_KEY_CAP`),
/// guaranteeing headroom for other keys within the same batch limit.
#[tokio::test]
async fn scanner_does_not_starve_other_keys_behind_one_hot_backlog() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let hot_key = "hot-tenant";
    let cold_key = "cold-tenant";

    // hot_key: seed its bucket empty, then defer strictly more rows than
    // THROTTLE_FIRE_BATCH_SIZE -- all older than cold_key's row below, and its
    // bucket is never refilled (rate ~0), so none of them can ever fire.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            hot_key,
            "hot-seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("hot_key seed consumes its burst token");
    let hot_backlog_size = THROTTLE_FIRE_BATCH_SIZE + 5;
    for i in 0..hot_backlog_size {
        let wf_id = format!("hot-job-{i}");
        let admit = reserve_or_defer(
            &mut conn,
            params(
                wf,
                hot_key,
                &wf_id,
                serde_json::json!({ "n": i }),
                0.0001,
                1.0,
                None,
                None,
            ),
        )
        .await
        .expect("hot_key backlog row defers");
        assert!(
            matches!(admit, ThrottleAdmission::Deferred(_)),
            "every hot_key row must defer (its bucket is permanently empty)"
        );
    }
    assert_eq!(
        throttle_row_count(&mut conn, hot_key).await,
        hot_backlog_size
    );

    // cold_key: one newer row (deferred strictly after all of hot_key's rows),
    // whose bucket DOES have an available token by the time the scanner runs.
    let cold_bucket_key = bucket_key(wf, cold_key);
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            cold_key,
            "cold-seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("cold_key seed consumes its burst token");
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            cold_key,
            "cold-job",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("cold_key row defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));
    // Give cold_key's bucket a token so the scanner can admit it this tick --
    // this is the "ready to fire" newer row that must not be starved.
    set_bucket_tokens(&mut conn, &cold_bucket_key, 1.0).await;

    // One scanner tick.
    let fired = drain(&mut conn, &metrics).await;
    assert!(
        fired >= 1,
        "cold_key's ready row must fire despite hot_key's much larger, older backlog"
    );
    assert_eq!(
        throttle_row_count(&mut conn, cold_key).await,
        0,
        "cold_key's ready row must have fired this tick, proving it wasn't starved"
    );
    // hot_key's bucket never had a token, so its per-key-capped candidates
    // (at most THROTTLE_FIRE_PER_KEY_CAP of them) are examined and left in
    // place, not deleted -- the whole backlog survives untouched.
    assert_eq!(
        throttle_row_count(&mut conn, hot_key).await,
        hot_backlog_size,
        "hot_key's backlog is examined (capped) but never fires without a token"
    );
}

/// Code-review fix (issue #607, P1): the per-key cap (`THROTTLE_FIRE_PER_KEY_CAP`)
/// bounds how many rows a *single* key can contribute to a scanner tick's
/// candidate set, but with the original flat `ORDER BY deferred_at ASC LIMIT
/// $2` outer selection, enough concurrently-exhausted keys can still fill
/// the *entire* batch budget with rows that can never fire: 10 exhausted
/// keys, each with exactly `THROTTLE_FIRE_PER_KEY_CAP` (10) permanently-stuck
/// rows, already exhausts a 100-row (`THROTTLE_FIRE_BATCH_SIZE`) batch --
/// permanently starving an 11th key's single, genuinely ready row every
/// tick, even though it's newer than all of them. Fixed by ordering the
/// candidate selection by per-key rank first (`rn ASC, deferred_at ASC`):
/// every distinct key's oldest row is considered in the first "tier" before
/// any key's second-oldest row is considered.
#[tokio::test]
async fn scanner_does_not_starve_a_fresh_key_behind_many_exhausted_keys() {
    use diesel_async::RunQueryDsl;
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";

    // 10 distinct "exhausted" keys, each with exactly THROTTLE_FIRE_PER_KEY_CAP
    // rows and no corresponding bucket row ever created -- try_consume_rate_limit_token
    // can never debit a nonexistent bucket, so these rows are permanently
    // stuck. 10 keys x 10 rows = 100 rows total, exactly filling
    // THROTTLE_FIRE_BATCH_SIZE.
    for key_idx in 0..10 {
        let key = format!("exhausted-{key_idx}");
        let bkey = bucket_key(wf, &key);
        for row_idx in 0..THROTTLE_FIRE_PER_KEY_CAP {
            diesel::sql_query(
                "INSERT INTO harvest_start_throttle \
                 (id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name, \
                  input, start_options, deferred_at, expires_at, shard_id, created_at) \
                 VALUES (gen_random_uuid(), $1, $2, $3, $4, 'default', 'null'::jsonb, \
                          '{}'::jsonb, NOW(), NULL, 0, NOW())",
            )
            .bind::<diesel::sql_types::Text, _>(wf)
            .bind::<diesel::sql_types::Text, _>(&key)
            .bind::<diesel::sql_types::Text, _>(&bkey)
            .bind::<diesel::sql_types::Text, _>(format!("exhausted-{key_idx}-{row_idx}"))
            .execute(&mut conn)
            .await
            .expect("insert exhausted row");
        }
    }
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_start_throttle"
        )
        .await,
        100,
        "100 permanently-stuck rows across 10 exhausted keys"
    );

    // A fresh, 11th key's single row, deferred strictly after all 100
    // exhausted rows (so it is the *newest* by deferred_at) and given a
    // real, debitable token.
    let fresh_key = "fresh";
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            fresh_key,
            "fresh-seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("fresh key seed consumes its burst token");
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            fresh_key,
            "fresh-job",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("fresh key row defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    // Refill the fresh key's bucket so the scanner can debit it this tick.
    set_bucket_tokens(&mut conn, &bucket_key(wf, fresh_key), 1.0).await;

    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(
        fired, 1,
        "the fresh key's ready row must fire despite 100 unrelated \
         exhausted rows exactly filling the batch cap"
    );
    assert_eq!(
        throttle_row_count(&mut conn, fresh_key).await,
        0,
        "the fresh key's row must have fired this tick, proving it wasn't starved"
    );
}

/// Code-review fix (issue #607, P2): the rank-then-date ordering fix above
/// only bounds the damage to the first `THROTTLE_FIRE_BATCH_SIZE` distinct
/// exhausted keys -- beyond that many, their rank-1 rows alone still fill
/// the whole batch on every tick, since nothing excludes a genuinely
/// exhausted bucket from the candidate set in the first place. This test
/// creates *real*, genuinely-empty buckets (not merely missing ones) for
/// 100 distinct keys -- exactly `THROTTLE_FIRE_BATCH_SIZE` -- plus one more
/// (101st) key with a real, ready bucket, so all 101 keys tie at rank 1 and
/// the fresh key's newer `deferred_at` would lose the date tie-break under
/// rank-only ordering. Fixed by pre-filtering candidates to rows whose
/// bucket snapshot currently shows an available token (or are already
/// expired, or have no bucket at all) -- a genuinely exhausted bucket is
/// never a candidate, at any scale.
#[tokio::test]
async fn scanner_readiness_filter_excludes_more_than_batch_size_distinct_exhausted_keys() {
    use diesel_async::RunQueryDsl;
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";

    // 100 distinct keys, each with a *real* bucket showing 0 available
    // tokens (refill_rate = 0, so it can never refill) and exactly one
    // pending row (rank 1).
    for key_idx in 0..THROTTLE_FIRE_BATCH_SIZE {
        let key = format!("real-exhausted-{key_idx}");
        let bkey = bucket_key(wf, &key);
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at) \
             VALUES ($1, 0.0, 1.0, 0.0, NOW(), NOW(), NOW())",
        )
        .bind::<diesel::sql_types::Text, _>(&bkey)
        .execute(&mut conn)
        .await
        .expect("insert exhausted bucket");
        diesel::sql_query(
            "INSERT INTO harvest_start_throttle \
             (id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name, \
              input, start_options, deferred_at, expires_at, shard_id, created_at) \
             VALUES (gen_random_uuid(), $1, $2, $3, $4, 'default', 'null'::jsonb, \
                      '{}'::jsonb, NOW(), NULL, 0, NOW())",
        )
        .bind::<diesel::sql_types::Text, _>(wf)
        .bind::<diesel::sql_types::Text, _>(&key)
        .bind::<diesel::sql_types::Text, _>(&bkey)
        .bind::<diesel::sql_types::Text, _>(format!("real-exhausted-{key_idx}"))
        .execute(&mut conn)
        .await
        .expect("insert exhausted row");
    }
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_start_throttle"
        )
        .await,
        THROTTLE_FIRE_BATCH_SIZE,
        "100 rows across 100 keys with real, permanently-empty buckets"
    );

    // A 101st key's single row, deferred strictly after all 100 exhausted
    // rows (so it is the newest by deferred_at -- and would lose the
    // rank-then-date tie-break if the exhausted rows were still candidates)
    // with a real, debitable token.
    let fresh_key = "real-fresh";
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            fresh_key,
            "real-fresh-seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("fresh key seed consumes its burst token");
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            fresh_key,
            "real-fresh-job",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("fresh key row defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));
    set_bucket_tokens(&mut conn, &bucket_key(wf, fresh_key), 1.0).await;

    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(
        fired, 1,
        "the fresh (101st) key's ready row must fire despite 100 other \
         keys with real, permanently-exhausted buckets"
    );
    assert_eq!(
        throttle_row_count(&mut conn, fresh_key).await,
        0,
        "the fresh key's row must have fired this tick, proving it wasn't starved"
    );
}

// ── `skip_size_check` ordering fix (issue #607 round 4) ─────────────────────
//
// Round 3 added an early oversized-input rejection at 4 call sites, running
// *before* `reserve_or_defer` is ever called. That meant it could not see
// either of `reserve_or_defer`'s own two idempotent-retry shortcuts --
// `Bypassed` (an active execution already satisfies the reuse policy) or an
// idempotent attach to an already-pending row -- so a retry carrying a
// stale/oversized body for an already-active or already-pending workflow_id
// was wrongly rejected with a size error instead of correctly resolving to
// the existing execution/pending row. `skip_size_check` is the shared
// primitive extracted so every call site can ask "would `reserve_or_defer`
// resolve this via one of its two no-fresh-row shortcuts?" before ever
// looking at the payload size.

#[tokio::test]
async fn skip_size_check_true_when_execution_already_active() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "already-active-job";

    start(&mut conn, wf, wf_id, serde_json::json!({})).await;

    assert!(
        autumn_harvest::throttle::skip_size_check(&mut conn, wf, wf_id, Some("reject_duplicate"),)
            .await
            .expect("skip_size_check"),
        "an active execution must skip the size check under reject_duplicate \
         (reserve_or_defer would resolve this via Bypassed, not a fresh row)"
    );
    assert!(
        autumn_harvest::throttle::skip_size_check(&mut conn, wf, wf_id, None)
            .await
            .expect("skip_size_check"),
        "same, defaulting to allow_duplicate when reuse_policy_str is None"
    );
}

#[tokio::test]
async fn skip_size_check_false_when_terminate_if_running_never_bypasses() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "terminate-if-running-job";

    start(&mut conn, wf, wf_id, serde_json::json!({})).await;

    assert!(
        !autumn_harvest::throttle::skip_size_check(
            &mut conn,
            wf,
            wf_id,
            Some("terminate_if_running"),
        )
        .await
        .expect("skip_size_check"),
        "terminate_if_running always starts fresh (cancel + replace) -- an \
         oversized body for it must still be rejected, not skipped"
    );
}

#[tokio::test]
async fn skip_size_check_true_when_pending_row_already_exists() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "pending-retry-job";

    // Drain the bucket so the first admission durably defers.
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("first admission defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);

    // A retry for the same workflow_id with a stale/oversized body must not
    // be rejected -- `reserve_or_defer` would resolve it to the same pending
    // row (issue #607's own idempotent-retry guarantee), not insert a
    // second, size-checked row.
    assert!(
        autumn_harvest::throttle::skip_size_check(&mut conn, wf, wf_id, None)
            .await
            .expect("skip_size_check"),
        "an already-pending workflow_id must skip the size check"
    );
}

#[tokio::test]
async fn skip_size_check_false_for_a_genuinely_fresh_workflow_id() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "genuinely-fresh-job";

    assert!(
        !autumn_harvest::throttle::skip_size_check(&mut conn, wf, wf_id, None)
            .await
            .expect("skip_size_check"),
        "no active execution and no pending row -- a genuinely fresh \
         admission must still be size-checked"
    );
}

// ── key-length validation ordering fix (issue #607 code review) ────────────
//
// `reserve_or_defer` rejects a throttle key over `THROTTLE_KEY_MAX_BYTES`
// bytes before it would ever be persisted into a bucket/pending row. That
// check must not run ahead of the two idempotent-retry shortcuts (an active
// execution already satisfying the reuse policy, or an existing pending row
// for the same workflow_id) -- both resolve entirely from `workflow_id` and
// never persist anything keyed by the (possibly over-long) `throttle_key`, so
// a retry carrying a stale/new input that happens to resolve to an over-long
// key must still attach to the existing execution/pending row rather than
// fail with a key-length error.

fn oversized_key() -> String {
    "k".repeat(autumn_harvest::throttle::THROTTLE_KEY_MAX_BYTES + 1)
}

#[tokio::test]
async fn oversized_key_bypasses_when_execution_already_active() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "oversized-key-active-job";
    let key = oversized_key();

    start(&mut conn, wf, wf_id, serde_json::json!({})).await;

    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            &key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            Some("reject_duplicate"),
        ),
    )
    .await
    .expect("an already-active workflow_id must bypass, not fail on key length");
    assert!(
        matches!(admit, ThrottleAdmission::Bypassed),
        "got {admit:?}"
    );
}

#[tokio::test]
async fn oversized_key_resolves_to_existing_pending_row() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "oversized-key-pending-job";
    let key = "acme";

    // First admission under a valid key durably defers (bucket drained).
    let first = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("first admission defers");
    let ThrottleAdmission::Deferred(_first_outcome) = first else {
        panic!("expected Deferred, got {first:?}");
    };
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);

    // A retry for the same workflow_id, whose stale/new input now resolves to
    // an over-long key, must still resolve to the *existing* pending row --
    // not fail on key length, and not insert a second row.
    let oversized = oversized_key();
    let retry = reserve_or_defer(
        &mut conn,
        params(
            wf,
            &oversized,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("an already-pending workflow_id must resolve to the existing row");
    let ThrottleAdmission::Deferred(retry_outcome) = retry else {
        panic!("expected Deferred, got {retry:?}");
    };
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);
    assert_eq!(throttle_row_count(&mut conn, &oversized).await, 0);

    // A third call (also via an oversized key) must land on the exact same
    // row as the retry -- comparing two DB-round-tripped outcomes avoids the
    // in-memory-vs-Postgres timestamp precision mismatch a comparison against
    // `first_outcome` would hit (its `deferred_at` was captured client-side
    // with nanosecond precision before Postgres's microsecond truncation on
    // insert; see `retry_with_same_workflow_id_does_not_create_a_second_pending_row`).
    let third = reserve_or_defer(
        &mut conn,
        params(
            wf,
            &oversized,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("a further retry must also resolve to the existing row");
    let ThrottleAdmission::Deferred(third_outcome) = third else {
        panic!("expected Deferred, got {third:?}");
    };
    assert_eq!(third_outcome.deferred_at, retry_outcome.deferred_at);
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);
}

/// Code-review fix (issue #607 round 7): `ThrottleDeferOutcome::fresh` must
/// distinguish a brand-new pending row (this call durably inserted it) from
/// an idempotent attach to an already-existing pending row (a retry for the
/// same `workflow_id` resolved via `reserve_or_defer`'s own step-1
/// shortcut) -- callers that count admissions (e.g. `schedule_backfill`'s
/// `dispatched`/`max_runs` budget) must not treat the latter as a new
/// admission.
#[tokio::test]
async fn fresh_deferral_is_true_only_on_the_first_insert_not_on_a_retry() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "fresh-flag-job";

    let first = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("first admission defers");
    let ThrottleAdmission::Deferred(first_outcome) = first else {
        panic!("expected Deferred, got {first:?}");
    };
    assert!(
        first_outcome.fresh,
        "the first admission durably inserts a brand-new pending row"
    );
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);

    let retry = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            0.0,
            None,
            None,
        ),
    )
    .await
    .expect("a retry for the same workflow_id resolves to the existing row");
    let ThrottleAdmission::Deferred(retry_outcome) = retry else {
        panic!("expected Deferred, got {retry:?}");
    };
    assert!(
        !retry_outcome.fresh,
        "a retry landing on the already-pending row must not be reported fresh"
    );
    assert_eq!(
        throttle_row_count(&mut conn, key).await,
        1,
        "no second row should have been inserted"
    );
}

#[tokio::test]
async fn oversized_key_still_rejected_for_a_genuinely_fresh_admission() {
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let wf_id = "oversized-key-fresh-job";
    let key = oversized_key();

    let err = reserve_or_defer(
        &mut conn,
        params(
            wf,
            &key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect_err("a genuinely fresh admission must still be key-length checked");
    assert!(err.to_string().contains("exceeds maximum"), "{err}");
}

/// Code-review fix (issue #607): a pending row already past its
/// `schedule_to_start` deadline must not be treated as an idempotent
/// already-pending admission, even if the scanner hasn't dropped it yet
/// (e.g. a delayed tick, or the retry racing the timeout sweep). Without the
/// fix, a retry for the same `workflow_id` would silently return the stale
/// row's `Deferred` outcome; the scanner then times out and deletes that row
/// without ever starting a workflow, so the caller's accepted retry never
/// actually runs.
#[tokio::test]
async fn expired_pending_row_is_not_returned_by_idempotent_retry_lookup() {
    use diesel_async::RunQueryDsl;
    let (mut conn, _url, _c) = setup_db().await;
    let wf = "sync_tenant";
    let key = "acme";
    let wf_id = "slow-retry-job";

    // Seed drains the sole burst token so the real admission below defers.
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed");

    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            Some(Duration::from_secs(300)),
            None,
        ),
    )
    .await
    .expect("defers");
    let ThrottleAdmission::Deferred(first) = admit else {
        panic!("expected Deferred, got {admit:?}");
    };
    assert_eq!(throttle_row_count(&mut conn, key).await, 1);

    // Force the deadline into the past, simulating the scanner not yet
    // having swept the row (a delayed tick, or the retry racing the timeout
    // sweep).
    diesel::sql_query(
        "UPDATE harvest_start_throttle SET expires_at = NOW() - INTERVAL '1 hour' WHERE workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(wf_id)
    .execute(&mut conn)
    .await
    .expect("expire");

    // skip_size_check must not treat the expired row as "already pending".
    assert!(
        !autumn_harvest::throttle::skip_size_check(&mut conn, wf, wf_id, None)
            .await
            .expect("skip_size_check"),
        "an expired pending row must not count as an already-pending admission"
    );

    // A retry must not silently bind to the doomed row -- it must create a
    // genuinely fresh admission (a second row), not report the stale row's
    // outcome.
    let retry = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            Some(Duration::from_secs(300)),
            None,
        ),
    )
    .await
    .expect("retry defers fresh");
    let ThrottleAdmission::Deferred(retry_outcome) = retry else {
        panic!("expected Deferred, got {retry:?}");
    };
    assert_ne!(
        retry_outcome.deferred_at, first.deferred_at,
        "the retry must not return the stale expired row's outcome"
    );
    assert_eq!(
        throttle_row_count(&mut conn, key).await,
        2,
        "a fresh row must be inserted alongside the still-present expired one"
    );
}

// ── Scheduler-tick oversized-input cap check (issue #607 code review) ───────

fn noop_scheduler_handler<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async { Ok(serde_json::Value::Null) })
}

/// A throttled workflow. `max_input_bytes` only raises the registry-wide
/// floor (`DEFAULT_MAX_WORKFLOW_INPUT_BYTES` = 2 MiB), so a real oversized
/// test input must exceed that registry default, not this per-workflow value.
fn make_throttled_registry(workflow_name: &'static str) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name: workflow_name,
            module: "throttle_tests",
            handler: noop_scheduler_handler,
            execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: Some(
                autumn_harvest::throttle::ThrottlePolicy::from_rate_str(
                    "100/m",
                    Some(100.0),
                    None,
                    None,
                )
                .expect("valid rate"),
            ),
            max_input_bytes: Some(64),
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

fn make_scheduler_pool(url: &str) -> autumn_harvest::worker::DbPool {
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Insert a due schedule row (interval:60, overdue by 5s) whose
/// `workflow_input` is the caller-supplied JSON value.
async fn insert_schedule_with_input(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    input: serde_json::Value,
) -> Uuid {
    use diesel_async::RunQueryDsl;
    use harvest_schedules::dsl;
    let now = chrono::Utc::now();
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(0),
            dsl::skip_policy.eq("skip"),
            dsl::workflow_input.eq(input),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

async fn schedule_row(
    conn: &mut AsyncPgConnection,
    id: Uuid,
) -> (
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
) {
    use diesel_async::RunQueryDsl;
    use harvest_schedules::dsl;
    dsl::harvest_schedules
        .filter(dsl::id.eq(id))
        .select((dsl::last_run_at, dsl::next_run_at))
        .first(conn)
        .await
        .expect("schedule row")
}

/// Code-review fix (issue #607): a scheduled fire whose input exceeds the
/// effective byte cap must not be durably deferred by the throttle -- it
/// must fail fast (before `reserve_or_defer` ever runs), leaving zero
/// `harvest_start_throttle` rows and zero new executions, and must NOT
/// advance `last_run_at`/`next_run_at` so the next tick retries the exact
/// same slot rather than silently skipping it. Note: `tick_once` itself
/// still returns `Ok(())` -- one schedule's dispatch failure is isolated
/// from the rest of the tick by `tick_workflow_schedules`'s own per-schedule
/// error handling (log + continue), so the fixed behavior is proven by the
/// per-schedule assertions below, not by the tick's overall return value.
#[tokio::test]
async fn scheduler_tick_rejects_oversized_input_before_deferring() {
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "oversized_sched_wf";

    // Per-workflow `max_input_bytes` only *raises* the registry-wide floor
    // (`DEFAULT_MAX_WORKFLOW_INPUT_BYTES` = 2 MiB, via
    // `effective_cap = max(per_workflow_cap, registry.max_workflow_input_bytes)`),
    // so the input must exceed the 2 MiB registry default, not the small
    // per-workflow cap configured above.
    let oversized_input = serde_json::json!({ "payload": "x".repeat(3 * 1024 * 1024) });
    let sched_id = insert_schedule_with_input(&mut conn, wf_name, oversized_input).await;
    let (last_run_before, next_run_before) = schedule_row(&mut conn, sched_id).await;

    // Drain the throttle bucket to zero tokens first (a throwaway seed call
    // with burst=1, `ON CONFLICT DO NOTHING` on the bucket row means this
    // first insert's burst wins), so that WITHOUT this fix the oversized
    // fire would hit the `Deferred` branch and durably persist a pending
    // row -- exactly the bug being tested. With a fresh, unconsumed bucket
    // (the registered policy's burst=100), the oversized fire would instead
    // reach `Reserved` and fall through to the pre-existing, already-correct
    // immediate-path cap enforcement, which cannot distinguish this fix from
    // its absence.
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf_name,
            throttle_key: "",
            workflow_id: "seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the bucket's sole token");
    drop(conn);

    let pool = make_scheduler_pool(&url);
    let registry = make_throttled_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // `tick_once` isolates one schedule's dispatch failure from the rest of
    // the tick: `tick_workflow_schedules`'s per-schedule loop catches
    // `tick_one_workflow_schedule`'s error, logs it, clears the schedule's
    // fire claim, and continues to the next schedule -- it never propagates
    // to `tick_once`'s own `Result` (so one misconfigured/oversized schedule
    // can't halt every other schedule sharing the tick). The real proof that
    // THIS schedule's dispatch failed-without-advancing lives in the
    // execution/throttle-row/last_run_at assertions below, not in the tick's
    // overall return value.
    let result = tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await;
    assert!(
        result.is_ok(),
        "the tick as a whole must still succeed despite one schedule's oversized input: {result:?}"
    );

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("reconnect");
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        0,
        "no pending throttle row should ever be written for an oversized input"
    );
    assert_eq!(
        execution_count(&mut conn, wf_name).await,
        0,
        "no execution should be created for an oversized scheduled input"
    );
    let (last_run_after, next_run_after) = schedule_row(&mut conn, sched_id).await;
    assert_eq!(
        last_run_after, last_run_before,
        "last_run_at must not advance on an oversized-input failure"
    );
    assert_eq!(
        next_run_after, next_run_before,
        "next_run_at must not advance -- the next tick retries the same overdue slot"
    );
}

/// Code-review fix (issue #607, P1): a throttled scheduled fire durably
/// defers before any `harvest_workflow_executions` row exists. The
/// scheduler's `max_active_runs`/overlap gate is computed solely from that
/// table, so -- without counting pending throttle rows -- a schedule with
/// `max_active_runs = 1` could dispatch (and defer) a *second* slot on a
/// later tick while the first deferred fire is still sitting in the
/// throttle queue, never having started an execution. This violates the
/// schedule's own concurrency guarantee and lets throttled schedules
/// overlap in a way the unthrottled scheduler would have skipped/buffered.
#[tokio::test]
async fn throttled_scheduled_deferral_counts_against_max_active_runs() {
    use diesel_async::RunQueryDsl;
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "throttled_overlap_wf";

    // Seed the bucket with capacity exactly 1 (not the registered policy's
    // real burst=100) so the very first admission attempt drains it --
    // `ensure_throttle_bucket`'s `ON CONFLICT (key) DO NOTHING` means
    // whichever call creates the bucket row wins its burst/refill_rate.
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf_name,
            throttle_key: "",
            workflow_id: "seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the bucket's sole token");

    let sched_id = insert_schedule_with_input(&mut conn, wf_name, serde_json::json!({})).await;
    // insert_schedule_with_input hardcodes max_active_runs = 10; force it to
    // 1 for this test's concurrency-limit scenario.
    diesel::sql_query("UPDATE harvest_schedules SET max_active_runs = 1 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("set max_active_runs");
    drop(conn);

    let pool = make_scheduler_pool(&url);
    let registry = make_throttled_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // First tick: the due slot defers (bucket already drained by the seed).
    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("first tick");

    let mut conn = connect(&url).await;
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        1,
        "first tick defers exactly one pending row"
    );
    assert_eq!(execution_count(&mut conn, wf_name).await, 0);

    // Force the schedule due again immediately, simulating the next
    // interval having already elapsed, without waiting real wall-clock time.
    diesel::sql_query(
        "UPDATE harvest_schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(sched_id)
    .execute(&mut conn)
    .await
    .expect("force due again");
    drop(conn);

    // Second tick: without the fix, `running` (computed only from
    // harvest_workflow_executions) is still 0, so max_active_runs=1 would
    // not block a second dispatch -- the schedule would defer a *second*
    // pending row even though the first deferred fire hasn't started yet.
    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("second tick");

    let mut conn = connect(&url).await;
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        1,
        "the second tick must not defer a second pending row while the \
         first is still queued -- max_active_runs=1 must count the pending \
         throttle row as an in-flight run"
    );
    assert_eq!(execution_count(&mut conn, wf_name).await, 0);
}

/// Insert a schedule row with a pre-populated `buffered_runs` slot (a single
/// past fire time), the buffered/backfill drain path's trigger condition
/// (`jsonb_array_length(buffered_runs) > 0`), whose `workflow_input` is the
/// caller-supplied JSON value. `next_run_at` is left far in the future so
/// only the buffered-drain path (not the normal tick path) can dispatch it.
async fn insert_schedule_with_buffered_slot(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    input: serde_json::Value,
) -> Uuid {
    use diesel_async::RunQueryDsl;
    use harvest_schedules::dsl;
    let now = chrono::Utc::now();
    let id = Uuid::new_v4();
    let buffered_slot = now - chrono::Duration::seconds(30);
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now + chrono::Duration::seconds(3600)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("buffer_all"),
            dsl::buffered_runs.eq(serde_json::json!([buffered_slot.to_rfc3339()])),
            dsl::buffer_all_max.eq(10),
            dsl::skip_policy.eq("skip"),
            dsl::workflow_input.eq(input),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

async fn schedule_buffered_runs(conn: &mut AsyncPgConnection, id: Uuid) -> Vec<String> {
    use diesel_async::RunQueryDsl;
    use harvest_schedules::dsl;
    let raw: serde_json::Value = dsl::harvest_schedules
        .filter(dsl::id.eq(id))
        .select(dsl::buffered_runs)
        .first(conn)
        .await
        .expect("schedule row");
    raw.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Same fix, buffered/backfill-fire loop (issue #607, P1): a throttled
/// buffered-slot fire durably defers before any execution row exists, so
/// this loop's `available = max_active_runs - running` must also count
/// pending throttle rows -- otherwise a schedule with `max_active_runs = 1`
/// could drain (and defer) a second buffered slot on a later tick while an
/// earlier deferred fire from this same loop is still queued.
#[tokio::test]
async fn throttled_buffered_deferral_counts_against_max_active_runs() {
    use diesel_async::RunQueryDsl;
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "throttled_buffered_overlap_wf";

    // Seed the bucket with capacity exactly 1 (see the scheduler-tick sibling
    // test above for why), so the very first buffered-slot admission drains it.
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf_name,
            throttle_key: "",
            workflow_id: "seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the bucket's sole token");

    let sched_id =
        insert_schedule_with_buffered_slot(&mut conn, wf_name, serde_json::json!({})).await;
    // insert_schedule_with_buffered_slot hardcodes max_active_runs = 10;
    // force it to 1 for this test's concurrency-limit scenario.
    diesel::sql_query("UPDATE harvest_schedules SET max_active_runs = 1 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("set max_active_runs");
    drop(conn);

    let pool = make_scheduler_pool(&url);
    let registry = make_throttled_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // First tick: the single buffered slot defers (bucket already drained).
    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("first tick");

    let mut conn = connect(&url).await;
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        1,
        "first tick defers exactly one pending row for the buffered slot"
    );
    assert_eq!(execution_count(&mut conn, wf_name).await, 0);
    assert!(
        schedule_buffered_runs(&mut conn, sched_id).await.is_empty(),
        "the drained slot must be removed from buffered_runs"
    );

    // Simulate a second buffered slot arriving later (without waiting real
    // wall-clock time) by re-populating buffered_runs directly.
    diesel::sql_query("UPDATE harvest_schedules SET buffered_runs = $1 WHERE id = $2")
        .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!([(chrono::Utc::now()
            - chrono::Duration::seconds(10))
        .to_rfc3339()]))
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("re-populate buffered_runs");
    drop(conn);

    // Second tick: without the fix, `running` is still 0 (the first
    // deferred fire never started an execution), so max_active_runs=1 would
    // not block draining the second buffered slot too.
    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("second tick");

    let mut conn = connect(&url).await;
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        1,
        "the second tick must not defer a second pending row while the \
         first is still queued -- max_active_runs=1 must count the pending \
         throttle row as an in-flight run"
    );
    assert_eq!(execution_count(&mut conn, wf_name).await, 0);
    assert_eq!(
        schedule_buffered_runs(&mut conn, sched_id).await.len(),
        1,
        "the second buffered slot must remain queued, not drained"
    );
}

/// Code-review fix (issue #607, proactive): the buffered/backfill-fire loop
/// previously enforced **no cap at all** on either its throttle or immediate
/// path (`None`/`0` are both "no cap" sentinels). A buffered slot whose input
/// exceeds the effective cap and whose throttle bucket is empty must be
/// dropped (not durably deferred, not retried forever) -- mirroring the
/// scheduler-tick fix above, but via this loop's own `break`-and-drop
/// convention rather than `return Err`.
#[tokio::test]
async fn buffered_drain_drops_oversized_slot_before_deferring() {
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "oversized_buffered_wf";

    let oversized_input = serde_json::json!({ "payload": "x".repeat(3 * 1024 * 1024) });
    let sched_id = insert_schedule_with_buffered_slot(&mut conn, wf_name, oversized_input).await;

    // Drain the throttle bucket first, exactly as in the scheduler-tick test:
    // without this, a fresh bucket would let the oversized slot reach
    // `Reserved` and fall through to the pre-existing immediate-path cap
    // enforcement, making this test unable to distinguish the fix from its
    // absence.
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf_name,
            throttle_key: "",
            workflow_id: "seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the bucket's sole token");
    drop(conn);

    let pool = make_scheduler_pool(&url);
    let registry = make_throttled_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    let result = tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await;
    assert!(
        result.is_ok(),
        "the tick as a whole must still succeed despite one buffered slot's oversized input: {result:?}"
    );

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("reconnect");
    assert_eq!(
        throttle_row_count(&mut conn, "").await,
        0,
        "no pending throttle row should ever be written for an oversized buffered slot"
    );
    assert_eq!(
        execution_count(&mut conn, wf_name).await,
        0,
        "no execution should be created for an oversized buffered slot"
    );
    let remaining = schedule_buffered_runs(&mut conn, sched_id).await;
    assert!(
        remaining.is_empty(),
        "the oversized slot must be dropped, not left in buffered_runs to retry forever: {remaining:?}"
    );
}

// ── Schedule-run metric on a throttled fire (issue #607 code review) ───────
//
// The scheduler-tick and buffered/backfill-fire throttle branches persist
// `schedule_id`/`scheduled_for`/`origin` into the deferred row's
// `start_options` so the eventual scanner-driven fire is attributed exactly
// as an immediate fire would be (carryover, run-history). That attribution
// must extend to `harvest.schedule.runs` too -- otherwise every scheduled
// slot that happened to defer silently undercounts the metric even though
// it is later dispatched.

#[tokio::test]
async fn scanner_fire_of_a_scheduled_deferral_records_schedule_run_metric() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "sched-key";
    let bkey = bucket_key(wf, key);
    let wf_id = "scheduled-deferred-job";

    // Drain the sole burst token first (burst must be >= 1.0 -- a bucket
    // capacity of 0.0 could never debit a token no matter what
    // `set_bucket_tokens` sets afterward, since the debit query caps refill
    // at `burst`) so the real admission below genuinely defers.
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf,
            throttle_key: key,
            workflow_id: "seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the burst token");

    let admit = reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf,
            throttle_key: key,
            workflow_id: wf_id,
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions {
                schedule_id: Some(Uuid::new_v4()),
                origin: Some("scheduled".to_string()),
                ..Default::default()
            },
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    // Refill exactly to the bucket's capacity so the scanner can fire it.
    set_bucket_tokens(&mut conn, &bkey, 1.0).await;

    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(fired, 1);
    assert_eq!(
        *metrics.schedule_runs.lock().unwrap(),
        vec![("workflow".to_string(), wf.to_string())],
        "a throttled scheduled fire must record exactly one schedule-run metric"
    );
}

#[tokio::test]
async fn scanner_fire_of_a_non_scheduled_deferral_does_not_record_schedule_run_metric() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "http-key";
    let bkey = bucket_key(wf, key);
    let wf_id = "http-deferred-job";

    // Drain the sole burst token first (see comment above).
    reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            "seed",
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("seed consumes the burst token");

    // Mirrors an HTTP-start/batch-originated throttled admission: no
    // schedule_id in start_options.
    let admit = reserve_or_defer(
        &mut conn,
        params(
            wf,
            key,
            wf_id,
            serde_json::json!({}),
            0.0001,
            1.0,
            None,
            None,
        ),
    )
    .await
    .expect("defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    set_bucket_tokens(&mut conn, &bkey, 1.0).await;

    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(fired, 1);
    assert!(
        metrics.schedule_runs.lock().unwrap().is_empty(),
        "an HTTP/batch-originated (non-scheduled) throttled fire must not \
         record a schedule-run metric"
    );
}

/// Code-review fix (issue #607): the HTTP manual-backfill endpoint's
/// throttle wiring persists `schedule_id` (for carryover lineage, #488) just
/// like the scheduler-tick/buffered-run branches do, but its own immediate
/// (unthrottled) dispatch path never calls `record_schedule_run` -- so
/// gating solely on `schedule_id.is_some()` would let a *throttled* backfill
/// fire inflate `harvest.schedule.runs` while an *unthrottled* backfill
/// (through the exact same handler) never does. Must be gated on
/// `origin == "scheduled"`, not merely the presence of a schedule_id.
#[tokio::test]
async fn scanner_fire_of_a_backfill_deferral_does_not_record_schedule_run_metric() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "backfill-key";
    let bkey = bucket_key(wf, key);
    let wf_id = "backfill-deferred-job";

    // Drain the sole burst token first (burst must be >= 1.0 -- see the
    // scheduled-deferral test above for why).
    reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf,
            throttle_key: key,
            workflow_id: "backfill-seed",
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions::default(),
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("seed consumes the burst token");

    let admit = reserve_or_defer(
        &mut conn,
        AdmitThrottleParams {
            workflow_name: wf,
            throttle_key: key,
            workflow_id: wf_id,
            queue_name: "default",
            input: serde_json::json!({}),
            start_options: DebounceStartOptions {
                // Mirrors the HTTP schedule_backfill endpoint's throttle
                // wiring: schedule_id IS set (for carryover), but origin is
                // "backfill", not "scheduled".
                schedule_id: Some(Uuid::new_v4()),
                origin: Some(autumn_harvest::execution::ORIGIN_BACKFILL.to_string()),
                ..Default::default()
            },
            refill_per_sec: 0.0001,
            burst: 1.0,
            schedule_to_start: None,
            shard_id: 0,
        },
    )
    .await
    .expect("defers");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

    set_bucket_tokens(&mut conn, &bkey, 1.0).await;

    let fired = drain(&mut conn, &metrics).await;
    assert_eq!(fired, 1);
    assert!(
        metrics.schedule_runs.lock().unwrap().is_empty(),
        "a backfill-originated throttled fire must not record a \
         schedule-run metric, matching the unthrottled backfill path's own \
         behavior (it never calls record_schedule_run either)"
    );
}
