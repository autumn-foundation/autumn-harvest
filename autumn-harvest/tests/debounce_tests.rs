#![cfg(feature = "db")]
// Test-code style lints (consistent with other integration test files in this crate).
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::significant_drop_tightening
)]
//! Debounce integration tests — issue #499.
//!
//! Verifies the core debounce ACs against a real Postgres container:
//! AC5  — burst collapse: K upserts → 1 row → scanner starts exactly 1 execution
//! AC6  — trailing-edge: upsert, advance < window, upsert again → NOT fired before
//!         the extended deadline; fires after
//! AC7  — last-input-wins: the started run's input equals the most recent request's input
//! AC8  — independent keys: two distinct keys → two rows → two executions
//! AC9  — max-wait cap: continuous upserts never push effective_fire_at past max_fire_at
//! AC10 — backward-compatible: start without a debounce policy → unchanged single
//!         execution; no harvest_debounce row left behind
//! AC11 — operator visibility: list_pending_debounce returns the pending record with
//!         the expected fields

use std::sync::Mutex;
use std::time::Duration;

use autumn_harvest::debounce::{
    AdmitDebounceParams, DebounceStartOptions, admit_debounced_start, fire_due_debounced_starts,
    list_pending_debounce,
};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{ExecutionId, WorkflowIdReusePolicy};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use chrono::Utc;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// All migrations up through the debounce table.
const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    "\n",
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000000_harvest_build_policy_ramp/up.sql"),
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql")
);

// ── Metrics recorder ─────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    debounced: Mutex<Vec<String>>,
    fired: Mutex<Vec<(String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_debounced(&self, workflow_name: &str) {
        self.debounced
            .lock()
            .unwrap()
            .push(workflow_name.to_owned());
    }

    fn record_debounce_fired(&self, workflow_name: &str, queue: &str) {
        self.fired
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migrations");

    (conn, container)
}

/// Count rows in harvest_debounce for a given workflow_name + debounce_key.
async fn debounce_row_count(conn: &mut AsyncPgConnection, wf: &str, key: &str) -> i64 {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_debounce WHERE workflow_name=$1 AND debounce_key=$2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result::<Count>(conn)
    .await
    .expect("count query")
    .n
}

/// Count rows in harvest_workflow_executions for a given workflow_name + workflow_id.
async fn execution_count(conn: &mut AsyncPgConnection, wf: &str, wf_id: &str) -> i64 {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name=$1 AND workflow_id=$2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(wf_id)
    .get_result::<Count>(conn)
    .await
    .expect("execution count query")
    .n
}

fn no_op_metrics() -> RecordingMetrics {
    RecordingMetrics::default()
}

// Admit with no active gate (the common case these tests exercise). Mirrors the
// pre-gate call shape so `.await.expect(..)` still yields the outcome: passes
// `gate_active = false` and unwraps the `Option` (a non-gated admission is always
// `Some`). The gated/`None` path is covered by the plugin HTTP integration tests.
async fn admit_debounced_start_ungated(
    conn: &mut AsyncPgConnection,
    params: AdmitDebounceParams<'_>,
) -> autumn_harvest::error::HarvestResult<autumn_harvest::debounce::DebounceAdmitOutcome> {
    Ok(admit_debounced_start(conn, params, false)
        .await?
        .expect("non-gated admission must return Some"))
}

fn admit_params<'a>(
    wf: &'a str,
    key: &'a str,
    wf_id: &'a str,
    input: serde_json::Value,
    window: Duration,
    max_wait: Duration,
) -> AdmitDebounceParams<'a> {
    AdmitDebounceParams {
        workflow_name: wf,
        debounce_key: key,
        workflow_id: wf_id,
        queue_name: "default",
        last_input: input,
        start_options: DebounceStartOptions::default(),
        window,
        max_wait,
        shard_id: 0,
    }
}

// ── AC5: burst collapse ───────────────────────────────────────────────────────

#[tokio::test]
async fn burst_collapse_k_upserts_produce_one_row_and_one_execution() {
    let (mut conn, _c) = setup_db().await;

    let wf = "burst_wf";
    let key = "tenant:collapse";
    let wf_id = "burst-wf-001";
    let window = Duration::from_millis(1); // very short so the scanner can fire immediately
    let max_wait = Duration::from_secs(60);

    // K = 5 admissions for the same (workflow_name, debounce_key)
    for i in 0u32..5 {
        let input = serde_json::json!({ "seq": i });
        let outcome = admit_debounced_start_ungated(
            &mut conn,
            admit_params(wf, key, wf_id, input, window, max_wait),
        )
        .await
        .expect("admit");
        assert_eq!(outcome.pending_count, (i + 1) as i32);
    }

    // Exactly 1 row in harvest_debounce
    assert_eq!(debounce_row_count(&mut conn, wf, key).await, 1);

    // Wait for the window to definitely elapse, then fire
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let metrics = no_op_metrics();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1);

    // Row consumed
    assert_eq!(debounce_row_count(&mut conn, wf, key).await, 0);

    // Exactly 1 execution
    assert_eq!(execution_count(&mut conn, wf, wf_id).await, 1);
}

// ── AC6: trailing-edge semantics ─────────────────────────────────────────────

#[tokio::test]
async fn trailing_edge_extends_deadline_and_scanner_respects_it() {
    let (mut conn, _c) = setup_db().await;

    let wf = "trailing_wf";
    let key = "tenant:trailing";
    let wf_id = "trailing-wf-001";
    let window = Duration::from_millis(200); // 200ms window
    let max_wait = Duration::from_secs(60);
    let metrics = no_op_metrics();

    // First admission
    let first_outcome = admit_debounced_start_ungated(
        &mut conn,
        admit_params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "seq": 1 }),
            window,
            max_wait,
        ),
    )
    .await
    .expect("first admit");

    // Second admission while still within the window extends the deadline
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second_outcome = admit_debounced_start_ungated(
        &mut conn,
        admit_params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "seq": 2 }),
            window,
            max_wait,
        ),
    )
    .await
    .expect("second admit");

    // Deadline must have been pushed out (trailing edge)
    assert!(
        second_outcome.fire_at >= first_outcome.fire_at,
        "second deadline ({:?}) must be >= first ({:?})",
        second_outcome.fire_at,
        first_outcome.fire_at,
    );

    // Scanner called before the extended deadline should fire nothing
    let fired_early = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("early fire");
    assert_eq!(
        fired_early, 0,
        "scanner must not fire before the extended deadline"
    );

    // Wait for the full window to elapse after the second admission
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let fired_late = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("late fire");
    assert_eq!(
        fired_late, 1,
        "scanner must fire exactly once after the window"
    );

    // Execution was created
    assert_eq!(execution_count(&mut conn, wf, wf_id).await, 1);
}

// ── AC7: last-input-wins ──────────────────────────────────────────────────────

#[tokio::test]
async fn last_input_wins() {
    use diesel_async::RunQueryDsl;

    let (mut conn, _c) = setup_db().await;

    let wf = "last_input_wf";
    let key = "tenant:last_input";
    let wf_id = "last-input-wf-001";
    let window = Duration::from_millis(1);
    let max_wait = Duration::from_secs(60);

    for i in 0u32..3 {
        let input = serde_json::json!({ "value": i });
        admit_debounced_start_ungated(
            &mut conn,
            admit_params(wf, key, wf_id, input, window, max_wait),
        )
        .await
        .expect("admit");
    }

    // Verify the stored last_input is the last request's value (2)
    #[derive(diesel::QueryableByName)]
    struct InputRow {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        last_input: serde_json::Value,
    }

    let row: InputRow = diesel::sql_query(
        "SELECT last_input FROM harvest_debounce WHERE workflow_name=$1 AND debounce_key=$2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result(&mut conn)
    .await
    .expect("get stored input");

    assert_eq!(
        row.last_input,
        serde_json::json!({ "value": 2 }),
        "stored last_input must be the most recent request's input"
    );

    // Fire and verify the execution was created with that input
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let metrics = no_op_metrics();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1);

    // Verify the started workflow's input in harvest_events (WorkflowStarted)
    #[derive(diesel::QueryableByName)]
    struct EventRow {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        event_data: serde_json::Value,
    }

    let event: EventRow = diesel::sql_query(
        "SELECT e.event_data FROM harvest_events e
         JOIN harvest_workflow_executions w ON w.id = e.workflow_exec_id
         WHERE w.workflow_name = $1 AND w.workflow_id = $2
           AND e.event_data->>'type' = 'WorkflowStarted'
         LIMIT 1",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(wf_id)
    .get_result(&mut conn)
    .await
    .expect("get WorkflowStarted event");

    let started_input = event.event_data["data"]["input"].clone();
    assert_eq!(
        started_input,
        serde_json::json!({ "value": 2 }),
        "started execution must use the last-submitted input"
    );
}

// ── AC8: independent keys ─────────────────────────────────────────────────────

#[tokio::test]
async fn independent_keys_produce_independent_executions() {
    let (mut conn, _c) = setup_db().await;

    let wf = "independent_wf";
    let window = Duration::from_millis(1);
    let max_wait = Duration::from_secs(60);

    // Two distinct debounce keys
    for key in &["tenant:key-alpha", "tenant:key-beta"] {
        let wf_id = format!("independent-{key}");
        admit_debounced_start_ungated(
            &mut conn,
            admit_params(wf, key, &wf_id, serde_json::json!({}), window, max_wait),
        )
        .await
        .expect("admit");
    }

    // Two rows in harvest_debounce
    let total: i64 = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_debounce WHERE workflow_name=$1")
            .bind::<diesel::sql_types::Text, _>(wf)
            .get_result::<Count>(&mut conn)
            .await
            .expect("count")
            .n
    };
    assert_eq!(total, 2, "two distinct keys → two rows");

    // Fire both
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let metrics = no_op_metrics();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 2, "two keys → two executions fired");

    // Both debounce rows consumed
    let remaining: i64 = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_debounce WHERE workflow_name=$1")
            .bind::<diesel::sql_types::Text, _>(wf)
            .get_result::<Count>(&mut conn)
            .await
            .expect("count remaining")
            .n
    };
    assert_eq!(remaining, 0);
}

// ── AC9: max-wait cap ────────────────────────────────────────────────────────

#[tokio::test]
async fn max_wait_cap_prevents_endless_deferral() {
    let (mut conn, _c) = setup_db().await;

    let wf = "max_wait_wf";
    let key = "tenant:max_wait";
    let wf_id = "max-wait-wf-001";
    let window = Duration::from_secs(3600); // very long window
    let max_wait = Duration::from_millis(50); // but short cap

    // First admission establishes max_fire_at = now + 50ms
    let first = admit_debounced_start_ungated(
        &mut conn,
        admit_params(wf, key, wf_id, serde_json::json!({}), window, max_wait),
    )
    .await
    .expect("first admit");

    let first_max_fire_at = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Timestamptz)]
            max_fire_at: chrono::DateTime<Utc>,
        }
        diesel::sql_query(
            "SELECT max_fire_at FROM harvest_debounce WHERE workflow_name=$1 AND debounce_key=$2",
        )
        .bind::<diesel::sql_types::Text, _>(wf)
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<Row>(&mut conn)
        .await
        .expect("get max_fire_at")
        .max_fire_at
    };

    // Multiple subsequent admissions should never push effective_fire_at past max_fire_at
    for i in 1u32..5 {
        let outcome = admit_debounced_start_ungated(
            &mut conn,
            admit_params(
                wf,
                key,
                wf_id,
                serde_json::json!({ "i": i }),
                window,
                max_wait,
            ),
        )
        .await
        .expect("subsequent admit");
        assert!(
            outcome.fire_at <= first_max_fire_at,
            "effective_fire_at ({:?}) must never exceed max_fire_at ({:?})",
            outcome.fire_at,
            first_max_fire_at,
        );
    }

    // Also check from the first outcome: fire_at <= max_fire_at
    assert!(
        first.fire_at <= first_max_fire_at,
        "first fire_at must be <= max_fire_at"
    );

    // After the cap elapses the scanner fires
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let metrics = no_op_metrics();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1, "scanner must fire after max_wait elapses");
    assert_eq!(execution_count(&mut conn, wf, wf_id).await, 1);
}

// ── AC10: backward compatibility ──────────────────────────────────────────────

#[tokio::test]
async fn no_debounce_policy_uses_normal_start_path() {
    let (mut conn, _c) = setup_db().await;

    let wf = "no_debounce_wf";
    let wf_id = "no-debounce-wf-001";

    // Start a workflow the normal way (no debounce table involved)
    let result = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: wf,
            workflow_id: wf_id,
            exec_id: ExecutionId::new(),
            input: serde_json::json!({ "x": 1 }),
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
        },
    )
    .await
    .expect("normal start");

    // One execution created
    assert_eq!(execution_count(&mut conn, wf, wf_id).await, 1);

    // No harvest_debounce row created
    let debounce_rows: i64 = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_debounce")
            .get_result::<Count>(&mut conn)
            .await
            .expect("count")
            .n
    };
    assert_eq!(
        debounce_rows, 0,
        "no debounce policy → no harvest_debounce row"
    );

    // result is the normal StartedWorkflowExecution (exec_id is set)
    assert!(!result.exec_id.as_uuid().is_nil());
}

// ── AC11: operator visibility ─────────────────────────────────────────────────

#[tokio::test]
async fn list_pending_debounce_surfaces_pending_records() {
    let (mut conn, _c) = setup_db().await;

    let wf = "visibility_wf";
    let key = "tenant:visibility";
    let wf_id = "visibility-wf-001";
    let window = Duration::from_secs(60);
    let max_wait = Duration::from_secs(3600);

    admit_debounced_start_ungated(
        &mut conn,
        admit_params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "v": 1 }),
            window,
            max_wait,
        ),
    )
    .await
    .expect("admit");

    // Upsert again to get pending_count = 2
    admit_debounced_start_ungated(
        &mut conn,
        admit_params(
            wf,
            key,
            wf_id,
            serde_json::json!({ "v": 2 }),
            window,
            max_wait,
        ),
    )
    .await
    .expect("second admit");

    let pending = list_pending_debounce(&mut conn)
        .await
        .expect("list pending");

    assert_eq!(pending.len(), 1, "should have exactly one pending record");

    let record = &pending[0];
    assert_eq!(record.workflow_name, wf);
    assert_eq!(record.debounce_key, key);
    assert_eq!(record.workflow_id, wf_id);
    assert_eq!(record.pending_count, 2);
    assert_eq!(record.queue_name, "default");
    // effective_fire_at must be in the future
    assert!(
        record.effective_fire_at > Utc::now(),
        "effective_fire_at must be in the future"
    );
    assert!(
        record.max_fire_at >= record.effective_fire_at,
        "max_fire_at must be >= effective_fire_at"
    );
}

// ── AC5 success-metric variant: 1,000 upserts → exactly 1 execution ──────────

#[tokio::test]
async fn thousand_upserts_produce_exactly_one_execution() {
    let (mut conn, _c) = setup_db().await;

    let wf = "stress_wf";
    let key = "tenant:stress";
    let wf_id = "stress-wf-001";
    let window = Duration::from_millis(1);
    let max_wait = Duration::from_secs(60);

    for i in 0u32..1_000 {
        let input = serde_json::json!({ "seq": i });
        admit_debounced_start_ungated(
            &mut conn,
            admit_params(wf, key, wf_id, input, window, max_wait),
        )
        .await
        .expect("admit");
    }

    // Still only 1 row
    assert_eq!(debounce_row_count(&mut conn, wf, key).await, 1);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let metrics = no_op_metrics();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1);
    assert_eq!(execution_count(&mut conn, wf, wf_id).await, 1);
}

// ── Metric emission ───────────────────────────────────────────────────────────

#[tokio::test]
async fn fire_emits_debounce_fired_metric() {
    let (mut conn, _c) = setup_db().await;

    let wf = "metric_wf";
    let key = "tenant:metric";
    let wf_id = "metric-wf-001";
    let window = Duration::from_millis(1);
    let max_wait = Duration::from_secs(60);

    admit_debounced_start_ungated(
        &mut conn,
        admit_params(wf, key, wf_id, serde_json::json!({}), window, max_wait),
    )
    .await
    .expect("admit");

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let metrics = RecordingMetrics::default();
    let fired = fire_due_debounced_starts(&mut conn, &None, &[], &metrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1);

    let fired_events = metrics.fired.lock().unwrap();
    assert_eq!(fired_events.len(), 1);
    assert_eq!(fired_events[0].0, wf);
}
