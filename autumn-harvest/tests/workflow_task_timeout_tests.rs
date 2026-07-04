#![cfg(feature = "db")]
//! Workflow-task timeout quarantine integration tests — issue #494.
//!
//! The worker wraps each workflow-task dispatch in a bounded wall-clock budget
//! (`WorkerConfig::workflow_task_timeout`, default 10 s). A hung body that
//! exceeds the budget has its concurrency slot reclaimed and its execution's
//! consecutive-timeout counter incremented. At or above
//! `poison_pill_threshold` (default 3) the task is quarantined to the DLQ and
//! the owning execution is failed terminally.
//!
//! These tests exercise the DB-layer helpers directly against a real Postgres
//! container:
//!
//! - `reset_timed_out_workflow_task` reverts a RUNNING task to PENDING so it
//!   can be re-claimed without waiting for the orphan-reclaim staleness window.
//! - `quarantine_workflow_task_timeout` moves the task to the DLQ, fails the
//!   execution, and emits `harvest.workflow.task_timeout` + terminal metrics.
//! - `WorkerConfig::workflow_task_timeout` defaults to 10 s; zero disables.

use std::sync::Mutex;

use autumn_harvest::dlq::{DeadLetterReason, dead_letter_count};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::worker::{
    DbPool, quarantine_workflow_task_timeout, reset_timed_out_workflow_task,
};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// Keepalive handle for either a testcontainers container or a local
/// per-test database. Dropping this stops the container (testcontainers path).
/// For the local-PG path we keep `()` — each test uses a unique UUID-named
/// database so leftover databases are harmless.
enum Keepalive {
    #[allow(dead_code)]
    Container(Box<ContainerAsync<Postgres>>),
    /// No cleanup needed; the test database is abandoned (unique UUID name).
    LocalDb,
}

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
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
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
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
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql")
);

// ---------------------------------------------------------------------------
// Diesel helper types
// ---------------------------------------------------------------------------

#[derive(QueryableByName)]
struct ReasonRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    error: String,
}

// ---------------------------------------------------------------------------
// Recording metrics stub
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RecordingMetrics {
    task_timeouts: Mutex<Vec<(String, String)>>,
    terminal: Mutex<Vec<(String, String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_workflow_task_timeout(&self, workflow_name: &str, queue: &str) {
        self.task_timeouts
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }

    fn record_workflow_terminal(
        &self,
        workflow_name: &str,
        queue: &str,
        status: autumn_harvest::telemetry::WorkflowStatus,
    ) {
        self.terminal.lock().unwrap().push((
            workflow_name.to_owned(),
            queue.to_owned(),
            format!("{status:?}"),
        ));
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn setup() -> (AsyncPgConnection, DbPool, Keepalive) {
    // When `TEST_DATABASE_URL` is set (e.g. in environments where Docker image
    // pulls are blocked), create a fresh per-test database on the local
    // Postgres instance instead of spinning up a container.
    if let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") {
        let db_name = format!("harvest_test_{}", Uuid::new_v4().simple());
        // Create the fresh test database.
        let mut admin_conn = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect to admin DB");
        admin_conn
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\";"))
            .await
            .expect("create test database");

        // Build a URL pointing at the new database.
        // Strip trailing "/postgres" and replace with the new db name.
        let test_url = {
            let base = admin_url.trim_end_matches('/');
            let without_db = base.rfind('/').map_or(base, |i| &base[..i]);
            format!("{without_db}/{db_name}")
        };

        let mut conn = AsyncPgConnection::establish(&test_url)
            .await
            .expect("connect to test DB");
        conn.batch_execute(INIT_SQL).await.expect("migration");

        let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new(test_url);
        let pool = deadpool::managed::Pool::builder(mgr)
            .max_size(4)
            .build()
            .expect("pool build");

        return (conn, pool, Keepalive::LocalDb);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");

    let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    let pool = deadpool::managed::Pool::builder(mgr)
        .max_size(4)
        .build()
        .expect("pool build");

    (conn, pool, Keepalive::Container(Box::new(container)))
}

/// Insert a RUNNING workflow execution and return its UUID.
async fn insert_running_workflow(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, state, input, queue_name) \
         VALUES ($1, 'timeout_wf', 'wf-timeout-1', 0, 'RUNNING', '{}'::jsonb, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Insert a RUNNING task claimed by `worker_id` linked to `workflow_exec_id`.
async fn insert_running_workflow_task(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    worker_id: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, workflow_exec_id, input, state, worker_id, \
          attempt, max_attempts, started_at) \
         VALUES ($1, 'default', 'workflow', $2, '{}'::jsonb, 'RUNNING', $3, \
                 1, 3, NOW() - INTERVAL '30 seconds')",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Uuid, _>(workflow_exec_id)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("insert task");
    id
}

async fn task_state(conn: &mut AsyncPgConnection, task_id: Uuid) -> String {
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .load::<TaskStateRow>(conn)
        .await
        .expect("query")
        .into_iter()
        .next()
        .expect("row")
        .state
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: Uuid) -> String {
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id)
        .load::<ExecStateRow>(conn)
        .await
        .expect("query")
        .into_iter()
        .next()
        .expect("row")
        .state
}

#[derive(QueryableByName)]
struct TaskStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

#[derive(QueryableByName)]
struct ExecStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `reset_timed_out_workflow_task` reverts a RUNNING task to PENDING so the
/// next poll cycle can re-claim it without waiting for the orphan-reclaim
/// staleness window.
#[tokio::test]
async fn reset_reverts_running_task_to_pending() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");

    reset_timed_out_workflow_task(&pool, task_id, "worker-1").await;

    assert_eq!(
        task_state(&mut conn, task_id).await,
        "PENDING",
        "reset should move RUNNING → PENDING"
    );
    // Execution stays RUNNING (timeout below threshold doesn't fail it).
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
}

/// `reset_timed_out_workflow_task` is a no-op when the task is already
/// PENDING or was claimed by a different worker — the optimistic guard prevents
/// a stale reset from clobbering a fresh claim.
#[tokio::test]
async fn reset_is_idempotent_on_wrong_state_or_worker() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    // Wrong worker_id — should not transition.
    reset_timed_out_workflow_task(&pool, task_id, "worker-99").await;
    assert_eq!(
        task_state(&mut conn, task_id).await,
        "RUNNING",
        "wrong worker_id must not reset the task"
    );

    // Correct worker — transitions.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1").await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    // Second call on PENDING task — no crash, still PENDING.
    reset_timed_out_workflow_task(&pool, task_id, "worker-1").await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// `quarantine_workflow_task_timeout` moves the task to the dead-letter queue,
/// fails the owning execution, and emits the `workflow.task_timeout` +
/// `workflow.terminal` metrics.
#[tokio::test]
async fn quarantine_writes_dlq_and_fails_execution() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    let metrics = std::sync::Arc::new(RecordingMetrics::default());

    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id),
        "worker-1",
        3,  // new_strikes (= threshold)
        10, // timeout_secs
        "timeout_wf",
        "default",
        &*metrics,
    )
    .await;

    // Task row must be FAILED.
    assert_eq!(
        task_state(&mut conn, task_id).await,
        "FAILED",
        "timed-out task must be quarantined (FAILED)"
    );

    // Owning execution must be FAILED.
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "FAILED",
        "owning execution must be failed after quarantine"
    );

    // DLQ must have one entry.
    let dlq_count = dead_letter_count(&mut conn).await.expect("dlq count");
    assert_eq!(dlq_count, 1, "exactly one DLQ entry expected");

    // Terminal metric emitted.
    let has_terminal = {
        let terminal = metrics.terminal.lock().unwrap();
        terminal
            .iter()
            .any(|(wf, q, _)| wf == "timeout_wf" && q == "default")
    };
    assert!(
        has_terminal,
        "record_workflow_terminal should be called with (timeout_wf, default)"
    );
}

/// The DLQ entry carries a `WorkflowTaskTimeout` typed reason so operators
/// can distinguish this from poison-pill quarantines.
#[tokio::test]
async fn quarantine_writes_typed_dlq_reason() {
    let (mut conn, pool, _container) = setup().await;

    let exec_id = insert_running_workflow(&mut conn).await;
    let task_id = insert_running_workflow_task(&mut conn, exec_id, "worker-1").await;

    let metrics = RecordingMetrics::default();
    quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id),
        "worker-1",
        3,
        10,
        "timeout_wf",
        "default",
        &metrics,
    )
    .await;

    // Inspect the raw reason JSON stored in harvest_dead_letters.
    let rows: Vec<ReasonRow> = diesel::sql_query("SELECT error FROM harvest_dead_letters LIMIT 1")
        .load(&mut conn)
        .await
        .expect("query dlq");
    let reason_json = rows.into_iter().next().expect("dlq row").error;

    let reason: DeadLetterReason =
        serde_json::from_str(&reason_json).expect("reason must deserialize");
    assert!(
        matches!(
            reason,
            DeadLetterReason::WorkflowTaskTimeout {
                task_timeout_strikes: 3,
                timeout_secs: 10
            }
        ),
        "DLQ reason must be WorkflowTaskTimeout, got {reason:?}"
    );
}

/// `quarantine_workflow_task_timeout` with no `exec_id` still marks the task
/// FAILED and writes a DLQ entry, but does not try to fail any execution.
#[tokio::test]
async fn quarantine_with_no_exec_id_only_fails_task() {
    let (mut conn, pool, _container) = setup().await;

    // Task with no execution association.
    let task_id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (id, queue_name, task_type, input, state, worker_id, attempt, max_attempts, started_at) \
         VALUES ($1, 'default', 'workflow', '{}'::jsonb, 'RUNNING', 'worker-1', 1, 3, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("insert orphan task");

    let metrics = RecordingMetrics::default();
    quarantine_workflow_task_timeout(
        &pool, task_id, None, // no exec_id
        "worker-1", 3, 10, "unknown", "default", &metrics,
    )
    .await;

    assert_eq!(task_state(&mut conn, task_id).await, "FAILED");
    let dlq_count = dead_letter_count(&mut conn).await.expect("dlq count");
    assert_eq!(dlq_count, 1);
}
