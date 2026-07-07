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
    AdmitThrottleParams, ThrottleAdmission, bucket_key, fire_due_throttled_starts,
    reserve_or_defer, throttle_backlog_by_key,
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

/// AC-a: an id-reuse short-circuit at fire time consumes no token — the reserved
/// token is refunded and no run is created.
#[tokio::test]
async fn id_reuse_short_circuit_refunds_token() {
    let (mut conn, _url, _c) = setup_db().await;
    let metrics = RecordingMetrics::default();
    let wf = "sync_tenant";
    let key = "acme";
    let bkey = bucket_key(wf, key);
    let wf_id = "dup-job";

    // A live execution already owns this workflow_id.
    start(&mut conn, wf, wf_id, serde_json::json!({})).await;
    let execs_before = execution_count(&mut conn, wf).await;

    // Defer a start for the SAME workflow_id under reject_duplicate.
    // Empty the bucket first (seed consumes the burst token).
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
    .expect("defer dup");
    assert!(matches!(admit, ThrottleAdmission::Deferred(_)));

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
    // Token refunded: balance is back at ~1 (a short-circuit consumed nothing).
    assert!(
        bucket_tokens(&mut conn, &bkey).await >= 0.99,
        "token refunded after id-reuse short-circuit"
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
