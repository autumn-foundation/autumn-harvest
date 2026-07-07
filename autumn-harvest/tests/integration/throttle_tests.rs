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

use std::sync::Mutex;
use std::time::Duration;

use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::throttle::{
    AdmitThrottleParams, THROTTLE_FIRE_BATCH_SIZE, ThrottleAdmission, bucket_key,
    fire_due_throttled_starts, reserve_or_defer, throttle_backlog_by_key,
};
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
    include_str!("../../migrations/20260706000000_harvest_start_throttle/up.sql"),
    "\n",
    // issue #606: harvest_task_queue.session_id (worker sessions), merged in from trunk-dev.
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #607 code review: companion index for the per-key-fair scanner query.
    include_str!(
        "../../migrations/20260707000000_harvest_start_throttle_bucket_deferred_idx/up.sql"
    ),
);

// ── Metrics recorder ─────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    throttled: Mutex<Vec<String>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_start_throttled(&self, workflow_name: &str) {
        self.throttled
            .lock()
            .unwrap()
            .push(workflow_name.to_owned());
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
