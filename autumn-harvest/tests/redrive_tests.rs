#![cfg(feature = "db")]
// Test-code style lints (consistent with other integration test files in this crate).
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::significant_drop_tightening,
    clippy::too_many_lines
)]
//! DLQ redrive integration tests — issue #510.
//!
//! Exercises the redrive primitive end-to-end against a real Postgres container:
//!
//! - AC3/AC7 — a FAILED owning execution is reactivated to RUNNING, a single
//!   `WorkflowRedriven` event is appended (no prior event mutated), the DLQ row
//!   is deleted, and a fresh task is enqueued on the original queue.
//! - AC4 — redriving the same entry twice is an idempotent no-op (skipped).
//! - AC4 — a RUNNING owning execution is a skip no-op.
//! - AC7 — a COMPLETED/CANCELLED owning execution is rejected with a clear error
//!   and the row is left in place.
//! - AC6 — `max` caps the redriven set; `matched` vs `redriven` reconcile.
//! - AC2 — dry-run mutates nothing.
//! - filter dimensions (queue / error_contains / dead_letter_ids / time bounds).
//! - AC8 — the `harvest.dlq.redriven{queue, outcome}` metric is emitted.

use std::sync::Mutex;

use autumn_harvest::dlq::{
    NewDeadLetterEntry, RedriveFilter, RedriveOutcome, dead_letter, redrive_dead_letter,
    redrive_dead_letters,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::{Priority, StartWorkflowParams, store};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

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
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql")
);

// ── Metrics recorder capturing redrive outcomes ──────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    redriven: Mutex<Vec<(String, String)>>,
}

impl MetricsRecorder for RecordingMetrics {
    fn record_dlq_redriven(&self, queue: &str, outcome: &str) {
        self.redriven
            .lock()
            .unwrap()
            .push((queue.to_owned(), outcome.to_owned()));
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

// ── Seed helpers ─────────────────────────────────────────────────────────────

/// Start a real RUNNING workflow execution and return its id.
async fn start_running(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    queue: &str,
) -> ExecutionId {
    let started = autumn_harvest::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: serde_json::json!({"k": "v"}),
            parent_id: None,
            queue_name: queue,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
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
        },
    )
    .await
    .expect("start workflow");
    started.exec_id
}

/// Force an execution into a terminal state and append the matching terminal
/// event, mimicking what the quarantine paths do at DLQ time.
async fn seal_state(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    state: &str,
    terminal: WorkflowEvent,
) {
    let history = store::load_history(conn, exec_id).await.expect("history");
    store::append_events(conn, exec_id, &[terminal], history.next_event_id)
        .await
        .expect("append terminal");
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state=$1, completed_at=NOW(), error='boom' WHERE id=$2",
    )
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("seal state");

    if state == "FAILED" || state == "COMPLETED" || state == "CANCELLED" {
        autumn_harvest::queue::fail_open_tasks_for_execution(conn, exec_id, "boom")
            .await
            .expect("fail open tasks");
    }
}

/// Insert a workflow-task DLQ row for `exec_id` and return its id.
async fn insert_dlq(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    queue: &str,
    error: &str,
) -> Uuid {
    dead_letter(
        conn,
        &NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: queue.to_string(),
            task_type: "workflow".to_string(),
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: None,
            input: serde_json::json!({"k": "v"}),
            error: error.to_string(),
            attempts: 3,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("insert dlq")
}

/// Seed a FAILED execution whose history ends in WorkflowFailed plus a matching
/// DLQ row. Returns `(exec_id, dlq_id)`.
async fn seed_failed_with_dlq(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    queue: &str,
    error: &str,
) -> (ExecutionId, Uuid) {
    let exec_id = start_running(conn, workflow_name, workflow_id, queue).await;
    seal_state(
        conn,
        exec_id,
        "FAILED",
        WorkflowEvent::WorkflowFailed {
            error: error.to_string(),
        },
    )
    .await;
    let dlq_id = insert_dlq(conn, exec_id, queue, error).await;
    (exec_id, dlq_id)
}

// ── Small SQL probes ─────────────────────────────────────────────────────────

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    v: String,
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    diesel::sql_query("SELECT state AS v FROM harvest_workflow_executions WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<TextRow>(conn)
        .await
        .expect("state")
        .v
}

async fn count_events(conn: &mut AsyncPgConnection, exec_id: ExecutionId, ty: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id=$1 AND event_type=$2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(ty)
    .get_result::<CountRow>(conn)
    .await
    .expect("count events")
    .n
}

async fn count_pending_tasks(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue WHERE workflow_exec_id=$1 AND state='PENDING'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<CountRow>(conn)
    .await
    .expect("count tasks")
    .n
}

async fn dlq_exists(conn: &mut AsyncPgConnection, dlq_id: Uuid) -> bool {
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_dead_letters WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(dlq_id)
        .get_result::<CountRow>(conn)
        .await
        .expect("count dlq")
        .n
        > 0
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn redrive_failed_execution_reactivates_and_appends() {
    let (mut conn, _c) = setup_db().await;
    let (exec_id, dlq_id) = seed_failed_with_dlq(
        &mut conn,
        "onboarding",
        "wf-1",
        "default",
        "connection refused",
    )
    .await;

    let events_before = count_events(&mut conn, exec_id, "WorkflowFailed").await;
    assert_eq!(events_before, 1);

    let outcome = redrive_dead_letter(&mut conn, dlq_id, None, Some("fixed"))
        .await
        .expect("redrive");
    assert!(matches!(outcome, RedriveOutcome::Redriven(_)));

    // Execution reactivated; one WorkflowRedriven appended; original WorkflowFailed untouched.
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
    assert_eq!(
        count_events(&mut conn, exec_id, "WorkflowRedriven").await,
        1
    );
    assert_eq!(count_events(&mut conn, exec_id, "WorkflowFailed").await, 1);
    // DLQ row consumed; a fresh PENDING task enqueued on the original queue.
    assert!(!dlq_exists(&mut conn, dlq_id).await);
    assert_eq!(count_pending_tasks(&mut conn, exec_id).await, 1);
}

#[tokio::test]
async fn redrive_idempotent_second_call_skips() {
    let (mut conn, _c) = setup_db().await;
    let (exec_id, dlq_id) =
        seed_failed_with_dlq(&mut conn, "onboarding", "wf-2", "default", "boom").await;

    let first = redrive_dead_letter(&mut conn, dlq_id, None, None)
        .await
        .expect("first redrive");
    assert!(matches!(first, RedriveOutcome::Redriven(_)));

    // Second redrive of the same (now-deleted) row is an idempotent no-op.
    let second = redrive_dead_letter(&mut conn, dlq_id, None, None)
        .await
        .expect("second redrive");
    assert_eq!(second, RedriveOutcome::Skipped);

    // Exactly one WorkflowRedriven event and one pending task — no duplicate.
    assert_eq!(
        count_events(&mut conn, exec_id, "WorkflowRedriven").await,
        1
    );
    assert_eq!(count_pending_tasks(&mut conn, exec_id).await, 1);
}

#[tokio::test]
async fn redrive_running_execution_is_skip_noop() {
    let (mut conn, _c) = setup_db().await;
    // Execution stays RUNNING, but a stale DLQ row points at it.
    let exec_id = start_running(&mut conn, "onboarding", "wf-3", "default").await;
    let dlq_id = insert_dlq(&mut conn, exec_id, "default", "boom").await;

    let outcome = redrive_dead_letter(&mut conn, dlq_id, None, None)
        .await
        .expect("redrive");
    assert_eq!(outcome, RedriveOutcome::Skipped);

    // No reactivation event appended; stale row converged (deleted).
    assert_eq!(
        count_events(&mut conn, exec_id, "WorkflowRedriven").await,
        0
    );
    assert!(!dlq_exists(&mut conn, dlq_id).await);
}

#[tokio::test]
async fn redrive_completed_execution_is_rejected() {
    let (mut conn, _c) = setup_db().await;
    let exec_id = start_running(&mut conn, "onboarding", "wf-4", "default").await;
    seal_state(
        &mut conn,
        exec_id,
        "COMPLETED",
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"ok": true}),
        },
    )
    .await;
    let dlq_id = insert_dlq(&mut conn, exec_id, "default", "boom").await;

    let err = redrive_dead_letter(&mut conn, dlq_id, None, None)
        .await
        .expect_err("completed execution must be rejected");
    assert!(err.to_string().to_lowercase().contains("not resurrectable"));
    // Row left in place for the operator; execution untouched.
    assert!(dlq_exists(&mut conn, dlq_id).await);
    assert_eq!(execution_state(&mut conn, exec_id).await, "COMPLETED");
}

#[tokio::test]
async fn redrive_cancelled_execution_is_rejected() {
    let (mut conn, _c) = setup_db().await;
    let exec_id = start_running(&mut conn, "onboarding", "wf-5", "default").await;
    seal_state(
        &mut conn,
        exec_id,
        "CANCELLED",
        WorkflowEvent::WorkflowCancelled {
            reason: "stop".to_string(),
        },
    )
    .await;
    let dlq_id = insert_dlq(&mut conn, exec_id, "default", "boom").await;

    let err = redrive_dead_letter(&mut conn, dlq_id, None, None)
        .await
        .expect_err("cancelled execution must be rejected");
    assert!(err.to_string().to_lowercase().contains("not resurrectable"));
    assert!(dlq_exists(&mut conn, dlq_id).await);
}

#[tokio::test]
async fn bulk_redrive_respects_max_matched_vs_redriven() {
    let (mut conn, _c) = setup_db().await;
    for i in 0..5 {
        seed_failed_with_dlq(
            &mut conn,
            "batch_wf",
            &format!("wf-max-{i}"),
            "batchq",
            "downstream timeout",
        )
        .await;
    }
    let metrics = RecordingMetrics::default();
    let filter = RedriveFilter {
        queue: Some("batchq".to_string()),
        max: Some(2),
        ..Default::default()
    };
    let result = redrive_dead_letters(&mut conn, &filter, None, None, &metrics)
        .await
        .expect("bulk redrive");

    assert_eq!(
        result.matched, 5,
        "matched must reflect the full filter set"
    );
    assert_eq!(result.redriven, 2, "max caps the redriven set");
    assert_eq!(result.failed, 0);
    assert_eq!(result.ids.len(), 2);

    // Three rows remain to be redriven on a follow-up call.
    let remaining = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_dead_letters WHERE queue_name='batchq'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("count remaining")
    .n;
    assert_eq!(remaining, 3);
}

#[tokio::test]
async fn bulk_redrive_dry_run_mutates_nothing() {
    let (mut conn, _c) = setup_db().await;
    let (exec_id, dlq_id) =
        seed_failed_with_dlq(&mut conn, "dry_wf", "wf-dry", "dryq", "boom").await;

    let metrics = RecordingMetrics::default();
    let filter = RedriveFilter {
        queue: Some("dryq".to_string()),
        dry_run: true,
        ..Default::default()
    };
    let result = redrive_dead_letters(&mut conn, &filter, None, None, &metrics)
        .await
        .expect("dry run");

    assert!(result.dry_run);
    assert_eq!(result.matched, 1);
    assert_eq!(result.redriven, 0);
    assert_eq!(result.ids.len(), 1, "dry-run returns a bounded sample");
    // Nothing changed.
    assert_eq!(execution_state(&mut conn, exec_id).await, "FAILED");
    assert_eq!(
        count_events(&mut conn, exec_id, "WorkflowRedriven").await,
        0
    );
    assert!(dlq_exists(&mut conn, dlq_id).await);
    assert!(metrics.redriven.lock().unwrap().is_empty());
}

#[tokio::test]
async fn redrive_filter_dimensions_select_the_right_subset() {
    let (mut conn, _c) = setup_db().await;
    let (_e1, target) =
        seed_failed_with_dlq(&mut conn, "wfa", "wf-a", "queue-a", "connection refused 42").await;
    let (_e2, other) = seed_failed_with_dlq(&mut conn, "wfb", "wf-b", "queue-b", "disk full").await;

    let metrics = RecordingMetrics::default();

    // error_contains is case-insensitive substring → selects only the first row.
    let filter = RedriveFilter {
        error_contains: Some("CONNECTION".to_string()),
        ..Default::default()
    };
    let r = redrive_dead_letters(&mut conn, &filter, None, None, &metrics)
        .await
        .expect("redrive by error");
    assert_eq!(r.matched, 1);
    assert_eq!(r.redriven, 1);
    assert!(!dlq_exists(&mut conn, target).await);
    assert!(dlq_exists(&mut conn, other).await, "queue-b row untouched");
}

#[tokio::test]
async fn redrive_by_explicit_ids_targets_only_those_rows() {
    let (mut conn, _c) = setup_db().await;
    let (_e1, a) = seed_failed_with_dlq(&mut conn, "wf", "id-a", "q", "boom").await;
    let (_e2, b) = seed_failed_with_dlq(&mut conn, "wf", "id-b", "q", "boom").await;

    let metrics = RecordingMetrics::default();
    let filter = RedriveFilter {
        dead_letter_ids: Some(vec![a]),
        ..Default::default()
    };
    let r = redrive_dead_letters(&mut conn, &filter, None, None, &metrics)
        .await
        .expect("redrive by ids");
    assert_eq!(r.matched, 1);
    assert_eq!(r.redriven, 1);
    assert!(!dlq_exists(&mut conn, a).await);
    assert!(dlq_exists(&mut conn, b).await);
}

#[tokio::test]
async fn redrive_emits_metric_per_outcome() {
    let (mut conn, _c) = setup_db().await;
    // One redrivable (FAILED), one rejected (COMPLETED), on the same queue.
    seed_failed_with_dlq(&mut conn, "m_wf", "m-ok", "metricq", "boom").await;
    let bad_exec = start_running(&mut conn, "m_wf", "m-bad", "metricq").await;
    seal_state(
        &mut conn,
        bad_exec,
        "COMPLETED",
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!(null),
        },
    )
    .await;
    insert_dlq(&mut conn, bad_exec, "metricq", "boom").await;

    let metrics = RecordingMetrics::default();
    let filter = RedriveFilter {
        queue: Some("metricq".to_string()),
        ..Default::default()
    };
    let r = redrive_dead_letters(&mut conn, &filter, None, None, &metrics)
        .await
        .expect("bulk redrive");
    assert_eq!(r.redriven, 1);
    assert_eq!(r.failed, 1);

    let recorded = metrics.redriven.lock().unwrap();
    assert!(recorded.contains(&("metricq".to_string(), "redriven".to_string())));
    assert!(recorded.contains(&("metricq".to_string(), "failed".to_string())));
}
