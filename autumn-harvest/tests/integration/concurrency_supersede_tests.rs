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
//! Latest-wins concurrency (`on_conflict = "cancel_running"`) — issue #811.
//!
//! Drives the real start primitive against Postgres and asserts the invariant
//! the strategy exists to hold: **at most `limit` non-terminal runs per
//! `(workflow_name, concurrency_key)`, and the newest admission wins.**
//!
//! Coverage:
//! - **AC3** `limit = 1` — admitting run N cancels the incumbent atomically.
//! - **AC4** `limit = N > 1` — sheds the OLDEST runs until in-flight `< N`.
//! - **AC1/AC2** `Defer` (the default) is byte-for-byte unchanged — nothing
//!   is ever cancelled.
//! - **AC5** the superseded run reaches `CANCELLED` through the ordinary
//!   cooperative path, recording a `WorkflowCancelled` event — no new event
//!   variant.
//! - **AC6** deterministic tie-break: the *later-admitted* run survives.
//! - **AC7** the metric fires exactly once per superseded run.
//! - Cross-workflow isolation: a different workflow type sharing the same key
//!   string is never cancelled.
//! - **Success metric** — 100 rapid same-key starts leave exactly ONE
//!   non-terminal run (the last admitted) and 99 `CANCELLED`.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it, otherwise a fresh testcontainers Postgres 16 is booted.

use std::sync::{Arc, Mutex};

use autumn_harvest::StartWorkflowParams;
use autumn_harvest::concurrency::ConcurrencyOnConflict;
use autumn_harvest::execution::start_or_load_workflow_execution_with_metrics;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{
    ExecutionId, ShardId, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── Harness ────────────────────────────────────────────────────────────────

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

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB")
}

/// Scrub every execution + task row so a shared migrated DB stays isolated.
async fn scrub(conn: &mut AsyncPgConnection) {
    conn.batch_execute("DELETE FROM harvest_task_queue; DELETE FROM harvest_workflow_executions;")
        .await
        .expect("scrub");
}

/// A capturing recorder so the AC7 counter is asserted, not assumed.
#[derive(Default)]
struct CapturingMetrics {
    superseded: Mutex<Vec<String>>,
    cancelled_terminals: Mutex<Vec<String>>,
}

impl MetricsRecorder for CapturingMetrics {
    fn record_concurrency_superseded(&self, workflow: &str) {
        self.superseded.lock().unwrap().push(workflow.to_owned());
    }
    fn record_workflow_terminal(
        &self,
        workflow_name: &str,
        _queue: &str,
        outcome: autumn_harvest::telemetry::WorkflowStatus,
    ) {
        if matches!(
            outcome,
            autumn_harvest::telemetry::WorkflowStatus::Cancelled
        ) {
            self.cancelled_terminals
                .lock()
                .unwrap()
                .push(workflow_name.to_owned());
        }
    }
}

fn params<'a>(
    wf: &'a str,
    wf_id: &'a str,
    exec_id: ExecutionId,
    key: &str,
    limit: u32,
    on_conflict: ConcurrencyOnConflict,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: wf,
        workflow_id: wf_id,
        exec_id,
        input: serde_json::json!({ "doc_id": key }),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: Some(key.to_string()),
        concurrency_limit: Some(limit),
        concurrency_on_conflict: on_conflict,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Start one run through the REAL production wrapper.
///
/// Deliberately `start_or_load_workflow_execution_with_metrics` rather than the
/// lower-level `_collect`: it is the wrapper every HTTP/CLI start funnels
/// through, so the test exercises the emission plumbing (post-commit
/// `emit_start_cancel_metrics`) as well as the supersede itself — a regression
/// that dropped the counter at a consumer would still be caught.
async fn start_run(
    conn: &mut AsyncPgConnection,
    metrics: &CapturingMetrics,
    wf: &str,
    wf_id: &str,
    key: &str,
    limit: u32,
    on_conflict: ConcurrencyOnConflict,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution_with_metrics(
        conn,
        params(wf, wf_id, exec_id, key, limit, on_conflict),
        Some(metrics),
        None,
    )
    .await
    .expect("start must succeed");
    exec_id
}

async fn state_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .load(conn)
            .await
            .expect("load state");
    rows.into_iter()
        .next()
        .map(|r| r.state)
        .unwrap_or_else(|| panic!("execution {exec_id} must exist"))
}

async fn non_terminal_count(conn: &mut AsyncPgConnection, wf: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND state IN ('RUNNING', 'PAUSED')",
    )
    .bind::<Text, _>(wf)
    .load(conn)
    .await
    .expect("count");
    rows[0].n
}

async fn cancelled_event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_data->>'type' = 'WorkflowCancelled'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("count events");
    rows[0].n
}

// ── AC3: limit = 1 supersedes the incumbent ────────────────────────────────

#[tokio::test]
async fn cancel_running_limit_one_supersedes_the_incumbent() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let first = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-1",
        "doc-a",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(state_of(&mut conn, first).await, "RUNNING");
    assert!(
        metrics.superseded.lock().unwrap().is_empty(),
        "the first admission has nothing to supersede"
    );

    let second = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-2",
        "doc-a",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    // AC3 + AC6: the LATER-admitted run wins; the earlier is cancelled.
    assert_eq!(
        state_of(&mut conn, first).await,
        "CANCELLED",
        "the incumbent must be superseded by the newer admission"
    );
    assert_eq!(
        state_of(&mut conn, second).await,
        "RUNNING",
        "the newest admission must survive"
    );
    assert_eq!(
        non_terminal_count(&mut conn, "doc_index").await,
        1,
        "at most `limit` non-terminal runs per key"
    );

    // AC5: cooperative cancellation path — a WorkflowCancelled event, no new
    // event variant.
    assert_eq!(
        cancelled_event_count(&mut conn, first).await,
        1,
        "supersede must go through the ordinary cancellation path"
    );

    // AC7: exactly one supersede count, carrying the SUPERSEDED run's type.
    assert_eq!(
        metrics.superseded.lock().unwrap().as_slice(),
        &["doc_index"],
        "harvest.concurrency.superseded fires once per superseded run"
    );
    // A superseded run is still a cancelled terminal, so the pre-existing
    // terminal counter fires too — the new counter isolates the subset.
    assert_eq!(metrics.cancelled_terminals.lock().unwrap().len(), 1);
}

// ── AC4: limit = N > 1 sheds the OLDEST ────────────────────────────────────

#[tokio::test]
async fn cancel_running_limit_n_sheds_oldest_until_under_cap() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let mut runs = Vec::new();
    for i in 0..3 {
        runs.push(
            start_run(
                &mut conn,
                &metrics,
                "batch_job",
                &format!("run-{i}"),
                "tenant-a",
                3,
                ConcurrencyOnConflict::CancelRunning,
            )
            .await,
        );
        // Distinct started_at so "oldest" is unambiguous.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Three runs, cap 3 — nothing shed yet.
    assert_eq!(non_terminal_count(&mut conn, "batch_job").await, 3);
    assert!(metrics.superseded.lock().unwrap().is_empty());

    // The fourth admission pushes the population to 4 > 3, so exactly ONE (the
    // oldest) is shed.
    let fourth = start_run(
        &mut conn,
        &metrics,
        "batch_job",
        "run-3",
        "tenant-a",
        3,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, runs[0]).await,
        "CANCELLED",
        "the OLDEST run must be shed first"
    );
    for surviving in [runs[1], runs[2], fourth] {
        assert_eq!(
            state_of(&mut conn, surviving).await,
            "RUNNING",
            "only the oldest excess run is shed"
        );
    }
    assert_eq!(
        non_terminal_count(&mut conn, "batch_job").await,
        3,
        "post-admission population must respect the cap"
    );
    assert_eq!(metrics.superseded.lock().unwrap().len(), 1);
}

// ── AC1/AC2: Defer (the default) is unchanged ──────────────────────────────

#[tokio::test]
async fn defer_never_cancels_anything() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let first = start_run(
        &mut conn,
        &metrics,
        "legacy_flow",
        "run-1",
        "key-a",
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;
    let second = start_run(
        &mut conn,
        &metrics,
        "legacy_flow",
        "run-2",
        "key-a",
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;

    // Both rows exist and are non-terminal: the CAP is enforced later, at claim
    // time, by deferring the second task — not by cancelling the first.
    assert_eq!(state_of(&mut conn, first).await, "RUNNING");
    assert_eq!(state_of(&mut conn, second).await, "RUNNING");
    assert_eq!(cancelled_event_count(&mut conn, first).await, 0);
    assert!(
        metrics.superseded.lock().unwrap().is_empty(),
        "the default strategy must emit no supersede counts at all"
    );
    assert!(metrics.cancelled_terminals.lock().unwrap().is_empty());
}

// ── Cross-workflow isolation ───────────────────────────────────────────────

#[tokio::test]
async fn supersede_never_crosses_workflow_types_sharing_a_key() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    // A *different* workflow type that merely resolved the same key string,
    // and did not opt in to latest-wins.
    let bystander = start_run(
        &mut conn,
        &metrics,
        "other_flow",
        "run-x",
        "shared-key",
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;
    let first = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-1",
        "shared-key",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    let second = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-2",
        "shared-key",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, bystander).await,
        "RUNNING",
        "a workflow type that never opted in must never be superseded"
    );
    assert_eq!(state_of(&mut conn, first).await, "CANCELLED");
    assert_eq!(state_of(&mut conn, second).await, "RUNNING");
    assert_eq!(
        metrics.superseded.lock().unwrap().as_slice(),
        &["doc_index"],
        "only the opted-in type is ever counted"
    );
}

// ── Distinct keys never interfere ──────────────────────────────────────────

#[tokio::test]
async fn distinct_keys_are_independent() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let a = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-a",
        "doc-a",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    let b = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "run-b",
        "doc-b",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    assert_eq!(state_of(&mut conn, a).await, "RUNNING");
    assert_eq!(state_of(&mut conn, b).await, "RUNNING");
    assert!(metrics.superseded.lock().unwrap().is_empty());
    assert_eq!(non_terminal_count(&mut conn, "doc_index").await, 2);
}

// ── Success metric: 100 rapid same-key starts ──────────────────────────────

#[tokio::test]
async fn success_metric_hundred_rapid_starts_leave_exactly_one_survivor() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = Arc::new(CapturingMetrics::default());
    let mut ids = Vec::with_capacity(100);
    for i in 0..100 {
        let id = start_run(
            &mut conn,
            &metrics,
            "doc_index",
            &format!("burst-{i}"),
            "hot-doc",
            1,
            ConcurrencyOnConflict::CancelRunning,
        )
        .await;
        ids.push(id);

        // The invariant must hold after EVERY admission, not only at the end:
        // at no point may two runs be simultaneously non-terminal for the key.
        assert_eq!(
            non_terminal_count(&mut conn, "doc_index").await,
            1,
            "admission {i} left more than one non-terminal run for the key"
        );
    }

    let last = *ids.last().unwrap();
    assert_eq!(
        state_of(&mut conn, last).await,
        "RUNNING",
        "the LAST admitted run is the survivor (deterministic tie-break)"
    );
    for (i, id) in ids.iter().take(99).enumerate() {
        assert_eq!(
            state_of(&mut conn, *id).await,
            "CANCELLED",
            "run {i} must have been superseded"
        );
    }
    assert_eq!(
        metrics.superseded.lock().unwrap().len(),
        99,
        "exactly one supersede count per superseded run"
    );
}
