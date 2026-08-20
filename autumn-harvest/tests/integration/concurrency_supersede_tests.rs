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
use autumn_harvest::worker::DbPool;
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
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
    start_run_full(conn, metrics, wf, wf_id, key, limit, on_conflict)
        .await
        .0
}

/// Same as [`start_run`] but also reports whether the start CREATED a fresh
/// execution (`true`) or ATTACHED to an existing one (`false`).
async fn start_run_full(
    conn: &mut AsyncPgConnection,
    metrics: &CapturingMetrics,
    wf: &str,
    wf_id: &str,
    key: &str,
    limit: u32,
    on_conflict: ConcurrencyOnConflict,
) -> (ExecutionId, bool) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let started = start_or_load_workflow_execution_with_metrics(
        conn,
        params(wf, wf_id, exec_id, key, limit, on_conflict),
        Some(metrics),
        None,
    )
    .await
    .expect("start must succeed");
    (
        ExecutionId::from_uuid(started.exec_id.as_uuid()),
        started.created,
    )
}

/// Same as [`start_run`] but with an explicit reuse policy, so the
/// `replace_execution` admission arms can be driven (issue #811, Codex round 2).
async fn start_run_with_reuse(
    conn: &mut AsyncPgConnection,
    metrics: &CapturingMetrics,
    wf: &str,
    wf_id: &str,
    key: &str,
    limit: u32,
    on_conflict: ConcurrencyOnConflict,
    reuse_policy: WorkflowIdReusePolicy,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut p = params(wf, wf_id, exec_id, key, limit, on_conflict);
    p.reuse_policy = reuse_policy;
    let started = start_or_load_workflow_execution_with_metrics(conn, p, Some(metrics), None)
        .await
        .expect("start must succeed");
    ExecutionId::from_uuid(started.exec_id.as_uuid())
}

/// Force a run terminal so it becomes a replaceable prior for its own
/// `workflow_id` while leaving the key's other runs untouched.
async fn mark_terminal(conn: &mut AsyncPgConnection, exec_id: ExecutionId, state: &str) {
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET state = $1, completed_at = NOW() WHERE id = $2",
    )
    .bind::<Text, _>(state)
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("mark terminal");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'COMPLETED' WHERE workflow_exec_id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(conn)
    .await
    .expect("close tasks");
}

async fn mark_failed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    mark_terminal(conn, exec_id, "FAILED").await;
}

async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    mark_terminal(conn, exec_id, "COMPLETED").await;
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(12)
        .build()
        .expect("build pool")
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
        .map_or_else(|| panic!("execution {exec_id} must exist"), |r| r.state)
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

/// State of an execution's `task_type = 'workflow'` queue row, if any.
async fn workflow_task_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT state FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("load task state");
    rows.into_iter().next().map(|r| r.state)
}

/// The `reason` recorded on an execution's `WorkflowCancelled` event.
async fn cancelled_event_reason(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Option<String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        reason: Option<String>,
    }
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT event_data->'data'->>'reason' AS reason FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_data->>'type' = 'WorkflowCancelled' \
         ORDER BY id ASC LIMIT 1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .load(conn)
    .await
    .expect("load cancel reason");
    rows.into_iter().next().and_then(|r| r.reason)
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

    // The survivor must be able to actually RUN. A supersede pass that failed
    // (or that cancelled the survivor's OWN task row) would leave the execution
    // row `RUNNING` forever while the run hangs -- and every assertion above
    // would still pass. Assert the workflow task is claimable.
    assert_eq!(
        workflow_task_state(&mut conn, last).await.as_deref(),
        Some("PENDING"),
        "the survivor's workflow task must still be PENDING (claimable)"
    );
}

// ── AC6: admission order, NOT wall-clock ───────────────────────────────────

/// The distinguishing half of AC6.
///
/// Every other test in this file starts runs strictly sequentially, so
/// admission order and `started_at` order coincide — an implementation that
/// resolved the winner purely from wall-clock would pass them all. Here the
/// incumbent's `started_at` is pushed into the FUTURE before the second run is
/// admitted, so the two orders disagree: a wall-clock implementation would
/// keep the "newest" (future-stamped) incumbent and cancel the newcomer.
/// Latest-wins must still supersede the incumbent, because the newcomer is the
/// later ADMISSION.
#[tokio::test]
async fn later_admitted_run_wins_even_when_its_started_at_is_older() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let incumbent = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "wall-clock-1",
        "doc-42",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    // Make the incumbent look like the NEWEST run by wall clock.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET started_at = NOW() + INTERVAL '1 hour' \
         WHERE id = $1",
    )
    .bind::<SqlUuid, _>(incumbent.as_uuid())
    .execute(&mut conn)
    .await
    .expect("backdate");

    let newcomer = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "wall-clock-2",
        "doc-42",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "the earlier-ADMITTED run must be superseded even though its started_at is later"
    );
    assert_eq!(state_of(&mut conn, newcomer).await, "RUNNING");
    assert_eq!(non_terminal_count(&mut conn, "doc_index").await, 1);
    assert_eq!(metrics.superseded.lock().unwrap().len(), 1);
}

/// AC3's "**in the same shard-local serialized critical section**" + AC6 under
/// genuine contention: N same-key admissions racing on separate pooled
/// connections must converge to exactly ONE survivor with zero errors.
///
/// The advisory lock in `supersede_running_for_key` is what makes this hold; a
/// build without it would let two admissions each observe the other as absent
/// and leave two non-terminal runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admissions_converge_to_one_survivor() {
    let (url, _c) = setup_db().await;
    let pool = build_pool(&url);
    {
        let mut conn = pool.get().await.expect("pool conn");
        scrub(&mut conn).await;
    }

    let metrics = Arc::new(CapturingMetrics::default());
    let n = 10usize;
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let pool = pool.clone();
        let metrics = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.expect("pool conn");
            start_run(
                &mut conn,
                &metrics,
                "doc_index",
                &format!("race-{i}"),
                "contended-key",
                1,
                ConcurrencyOnConflict::CancelRunning,
            )
            .await
        }));
    }

    let mut ids = Vec::with_capacity(n);
    for h in handles {
        ids.push(h.await.expect("task join"));
    }

    let mut conn = pool.get().await.expect("pool conn");
    assert_eq!(
        non_terminal_count(&mut conn, "doc_index").await,
        1,
        "{n} concurrent same-key admissions must converge to exactly one survivor"
    );

    let mut cancelled = 0usize;
    let mut running = 0usize;
    for id in &ids {
        match state_of(&mut conn, *id).await.as_str() {
            "CANCELLED" => cancelled += 1,
            "RUNNING" => running += 1,
            other => panic!("unexpected state {other}"),
        }
    }
    assert_eq!(running, 1, "exactly one run survives");
    assert_eq!(cancelled, n - 1, "every other run is superseded");
    assert_eq!(
        metrics.superseded.lock().unwrap().len(),
        n - 1,
        "one supersede count per superseded run, even under contention"
    );
}

// ── AC3: PAUSED runs are part of the active set ────────────────────────────

/// `PAUSED` is in the candidate scan's active set (`SUSPENDED` is not a
/// persisted state). Dropping `'PAUSED'` from the scan passes every other test
/// in this file, so it is asserted directly.
#[tokio::test]
async fn paused_incumbent_is_superseded() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let incumbent = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "paused-1",
        "doc-paused",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    autumn_harvest::execution::pause_workflow_execution(
        &mut conn,
        incumbent,
        Some("operator hold"),
        "tester",
        &metrics,
    )
    .await
    .expect("pause must succeed");
    assert_eq!(state_of(&mut conn, incumbent).await, "PAUSED");

    let newcomer = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "paused-2",
        "doc-paused",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "a PAUSED incumbent is still an active run and must be superseded"
    );
    assert_eq!(state_of(&mut conn, newcomer).await, "RUNNING");
    assert_eq!(metrics.superseded.lock().unwrap().len(), 1);
}

// ── Attach must never supersede ────────────────────────────────────────────

/// Superseding is gated on `created == true`. An ATTACH admits nothing, so it
/// must cancel nothing — otherwise a duplicate start would cancel runs on
/// behalf of an admission that never happened.
#[tokio::test]
async fn attach_does_not_supersede() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let (first, created_first) = start_run_full(
        &mut conn,
        &metrics,
        "doc_index",
        "same-wf-id",
        "doc-attach",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert!(created_first, "the first start creates a fresh execution");

    // Same workflow_id + AllowDuplicate over a live prior => ATTACH.
    let (second, created_second) = start_run_full(
        &mut conn,
        &metrics,
        "doc_index",
        "same-wf-id",
        "doc-attach",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert!(!created_second, "the second start must ATTACH, not create");
    assert_eq!(second, first, "attach returns the existing execution");

    assert_eq!(
        state_of(&mut conn, first).await,
        "RUNNING",
        "an attach must never cancel the run it attached to"
    );
    assert!(
        metrics.superseded.lock().unwrap().is_empty(),
        "an attach admits nothing, so it supersedes nothing"
    );
    assert_eq!(non_terminal_count(&mut conn, "doc_index").await, 1);
}

// ── The superseded run's cancellation reason ───────────────────────────────

/// `SUPERSEDE_CANCEL_REASON` is the operator's only signal distinguishing a
/// latest-wins supersede from an ordinary operator cancel.
#[tokio::test]
async fn supersede_records_the_supersede_reason() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let metrics = CapturingMetrics::default();
    let incumbent = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "reason-1",
        "doc-reason",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    let _ = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "reason-2",
        "doc-reason",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;

    let reason = cancelled_event_reason(&mut conn, incumbent)
        .await
        .expect("a superseded run records a WorkflowCancelled reason");
    assert!(
        reason.contains(autumn_harvest::concurrency::SUPERSEDE_CANCEL_REASON),
        "cancel reason must name the supersede, got: {reason}"
    );
}

// ── SUPERSEDE_SCAN_LIMIT clamps one admission's shed count ─────────────────

/// The documented safety bound: one admission never cancels more than
/// `SUPERSEDE_SCAN_LIMIT` runs; the excess is shed by the NEXT admission, so
/// the population still converges. Removing the `.min(SUPERSEDE_SCAN_LIMIT)`
/// clamp (or flipping it to `max`) is undetectable without this.
#[tokio::test]
async fn scan_limit_clamps_shed_per_admission_and_still_converges() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let cap = autumn_harvest::concurrency::SUPERSEDE_SCAN_LIMIT;
    let backlog = cap + 2;

    // Build the backlog under Defer, which never cancels anything.
    let metrics = CapturingMetrics::default();
    for i in 0..backlog {
        let _ = start_run(
            &mut conn,
            &metrics,
            "doc_index",
            &format!("backlog-{i}"),
            "over-cap",
            1,
            ConcurrencyOnConflict::Defer,
        )
        .await;
    }
    assert_eq!(
        non_terminal_count(&mut conn, "doc_index").await,
        backlog as i64,
        "Defer must not cancel anything while the backlog is built"
    );

    // One latest-wins admission: sheds at most `cap`, leaving
    // backlog + 1 (self) - cap non-terminal.
    let _ = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "clamped-1",
        "over-cap",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(
        metrics.superseded.lock().unwrap().len(),
        cap,
        "one admission sheds at most SUPERSEDE_SCAN_LIMIT runs"
    );
    assert_eq!(
        non_terminal_count(&mut conn, "doc_index").await,
        (backlog + 1 - cap) as i64,
        "the excess is deliberately left for the next admission"
    );

    // The next admission finishes the job -- the population converges.
    let last = start_run(
        &mut conn,
        &metrics,
        "doc_index",
        "clamped-2",
        "over-cap",
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(
        non_terminal_count(&mut conn, "doc_index").await,
        1,
        "the population converges on the next admission"
    );
    assert_eq!(state_of(&mut conn, last).await, "RUNNING");
}

// ── Scheduler-tick supersede metrics (issue #811, Codex round 1, P2) ───────

/// A scheduled fire that supersedes must emit the counters too.
///
/// The scheduler resolves the declared `on_conflict` from the registered
/// `WorkflowInfo` and threads it into `StartWorkflowParams`, so a scheduled
/// tick genuinely *does* supersede an incumbent. But it called the
/// metrics-discarding `start_or_load_workflow_execution` wrapper, so neither
/// `harvest.concurrency.superseded` nor the cancelled terminal counter was
/// emitted for that shed run — AC7 says "once per superseded run", and a
/// scheduled fire is a superseding run like any other.
///
/// Drives the REAL public `tick_once` against a due schedule with a live
/// incumbent, so the fix has to be in the production plumbing rather than in a
/// test-only shim.
#[tokio::test]
async fn scheduled_tick_supersede_emits_the_counters() {
    use autumn_harvest::worker::HandlerRegistry;
    use autumn_harvest::{DagCatalog, SchedulerMonitor, WorkflowContext, tick_once};

    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let wf = "sched_latest_wins";
    let key = "tenant-sched";

    // A live incumbent on the key, admitted the ordinary way.
    let incumbent = start_run(
        &mut conn,
        &CapturingMetrics::default(),
        wf,
        "incumbent",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(state_of(&mut conn, incumbent).await, "RUNNING");

    // A due schedule for the same workflow type. The scheduler resolves the
    // key + strategy from the registered WorkflowInfo below.
    // `harvest_schedules.workflow_name` is globally unique, so clear any row a
    // prior run of this suite left behind in a reused database.
    diesel::sql_query("DELETE FROM harvest_schedules WHERE workflow_name = $1")
        .bind::<Text, _>(wf)
        .execute(&mut conn)
        .await
        .expect("clear stale schedule");
    let sched_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_schedules \
         (id, workflow_name, schedule_expr, timezone, catchup, max_active_runs, \
          is_paused, next_run_at, jitter_secs, overlap_policy, buffered_runs, \
          buffer_all_max, skip_policy, workflow_input) \
         VALUES ($1, $2, 'interval:60', 'UTC', false, 10, false, \
                 NOW() - INTERVAL '5 seconds', 0, 'skip', '[]'::jsonb, 100, 'skip', $3)",
    )
    .bind::<SqlUuid, _>(sched_id)
    .bind::<Text, _>(wf)
    .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({ "tenant_id": key }))
    .execute(&mut conn)
    .await
    .expect("insert schedule");

    drop(conn);

    fn noop<'a>(
        _ctx: &'a WorkflowContext,
        _input: serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
    > {
        Box::pin(async { Ok(serde_json::Value::Null) })
    }

    let metrics: Arc<CapturingMetrics> = Arc::new(CapturingMetrics::default());
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        ..Default::default()
    });

    let info = autumn_harvest::WorkflowInfo {
        name: "sched_latest_wins",
        module: "concurrency_supersede_tests",
        handler: noop,
        concurrency: Some(
            autumn_harvest::concurrency::ConcurrencyPolicy::new("input.tenant_id", 1)
                .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
        ),
        declared_activities: None,
        declared_children: None,
        mcp: false,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    };

    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![info],
        vec![],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));

    tick_once(
        build_pool(&url),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");

    let mut conn = connect(&url).await;

    // The scheduled fire superseded the incumbent...
    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "the scheduled fire must supersede the incumbent"
    );
    assert_eq!(
        non_terminal_count(&mut conn, wf).await,
        1,
        "exactly the scheduled run survives"
    );

    // ...and therefore must have counted it (AC7).
    assert_eq!(
        metrics.superseded.lock().unwrap().as_slice(),
        &[wf.to_string()],
        "a scheduled supersede must emit harvest.concurrency.superseded exactly once"
    );
    assert_eq!(
        metrics.cancelled_terminals.lock().unwrap().as_slice(),
        &[wf.to_string()],
        "a scheduled supersede must emit the cancelled terminal counter too"
    );
}

// ── Replacement admissions supersede too (issue #811, Codex round 2, P2) ──

/// A `replace_execution` admission is a fresh admission — it must shed too.
///
/// `AllowDuplicateFailedOnly` over a FAILED prior, and `TerminateIfRunning`
/// over a terminal prior, both seal THIS `workflow_id`'s own prior and insert a
/// fresh run. Sealing our own prior says nothing about a *different*
/// `workflow_id` holding the same concurrency key, so skipping the supersede
/// pass on those arms let a `limit = 1` group retain BOTH runs despite
/// `cancel_running`.
#[tokio::test]
async fn allow_duplicate_failed_only_replacement_sheds_other_runs_on_the_key() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let wf = "doc_index";
    let key = "replace-adfo";
    let metrics = CapturingMetrics::default();

    // `req-1` runs and then FAILS, so it is a terminal prior for its own
    // workflow_id and is out of the key's population.
    let first = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    mark_failed(&mut conn, first).await;

    // A DIFFERENT workflow_id becomes the live incumbent on the key.
    let incumbent = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-2",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(state_of(&mut conn, incumbent).await, "RUNNING");
    assert_eq!(non_terminal_count(&mut conn, wf).await, 1);

    metrics.superseded.lock().unwrap().clear();
    metrics.cancelled_terminals.lock().unwrap().clear();

    // Re-admitting `req-1` takes the `replace_execution` arm (its own prior is
    // FAILED). It is still a fresh admission into the key's population.
    let replacement = start_run_with_reuse(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "a replacement admission must shed the other run on the key"
    );
    assert_eq!(state_of(&mut conn, replacement).await, "RUNNING");
    assert_eq!(
        non_terminal_count(&mut conn, wf).await,
        1,
        "limit = 1 must hold across a replacement admission"
    );
    assert_eq!(
        *metrics.superseded.lock().unwrap(),
        vec![wf.to_string()],
        "the shed run must be counted"
    );
    assert_eq!(
        *metrics.cancelled_terminals.lock().unwrap(),
        vec![wf.to_string()]
    );
}

/// Same, for `TerminateIfRunning` over a terminal prior.
#[tokio::test]
async fn terminate_if_running_replacement_sheds_other_runs_on_the_key() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let wf = "doc_index";
    let key = "replace-tir";
    let metrics = CapturingMetrics::default();

    let first = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    mark_completed(&mut conn, first).await;

    let incumbent = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-2",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(non_terminal_count(&mut conn, wf).await, 1);

    metrics.superseded.lock().unwrap().clear();

    let replacement = start_run_with_reuse(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
        WorkflowIdReusePolicy::TerminateIfRunning,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "TerminateIfRunning over a terminal prior is still a fresh admission"
    );
    assert_eq!(state_of(&mut conn, replacement).await, "RUNNING");
    assert_eq!(non_terminal_count(&mut conn, wf).await, 1);
    assert_eq!(*metrics.superseded.lock().unwrap(), vec![wf.to_string()]);
}

/// A `Defer` replacement admission still cancels nothing (no regression).
#[tokio::test]
async fn defer_replacement_admission_sheds_nothing() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    let wf = "doc_index";
    let key = "replace-defer";
    let metrics = CapturingMetrics::default();

    let first = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;
    mark_failed(&mut conn, first).await;

    let incumbent = start_run(
        &mut conn,
        &metrics,
        wf,
        "req-2",
        key,
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;

    let _ = start_run_with_reuse(
        &mut conn,
        &metrics,
        wf,
        "req-1",
        key,
        1,
        ConcurrencyOnConflict::Defer,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
    )
    .await;

    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "RUNNING",
        "Defer must never cancel anything, replacement or not"
    );
    assert_eq!(non_terminal_count(&mut conn, wf).await, 2);
    assert!(metrics.superseded.lock().unwrap().is_empty());
}

// ── Completion-trigger supersede metrics (issue #811, Codex round 2, P2) ──

/// A completion-trigger target that supersedes must emit the counters too.
///
/// `evaluate_triggers_for_execution` propagates the target's declared
/// `on_conflict`, so a same-shard inline trigger start genuinely *does*
/// supersede an incumbent — but it called the metrics-discarding
/// `start_or_load_workflow_execution` wrapper, which drops the collected
/// cancellations. Neither `harvest.concurrency.superseded` nor the incumbent's
/// cancelled terminal was emitted even though a recorder is in scope, so AC7's
/// "once per superseded run" did not hold for trigger-created runs.
#[tokio::test]
async fn completion_trigger_supersede_emits_the_counters() {
    use autumn_harvest::completion_trigger::{
        GLOBAL_WORKFLOW_METADATA, TerminalState, WorkflowMetadata, evaluate_triggers_for_execution,
    };
    use autumn_harvest::concurrency::ConcurrencyPolicy;

    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;
    conn.batch_execute(
        "DELETE FROM harvest_completion_trigger_fires; DELETE FROM harvest_completion_triggers;",
    )
    .await
    .expect("scrub triggers");

    let source_wf = "trigger_source";
    let target_wf = "trigger_target";
    let key = "tenant-trigger";

    // The trigger path resolves routing through the process-global router.
    autumn_harvest::shard::install_global_router(autumn_harvest::shard::ShardRouter::single());

    // Publish the target's declared policy the way the worker does at startup,
    // so `evaluate_triggers_for_execution` resolves `cancel_running`.
    // Restored below so the process-global does not leak to sibling tests.
    // `WorkflowMetadata` is not `Clone`, so take the whole map out and put it
    // back at the end rather than cloning it.
    let previous = {
        let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
        lock.take()
    };
    {
        let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
        let mut map = std::collections::HashMap::new();
        map.insert(
            target_wf.to_string(),
            WorkflowMetadata {
                concurrency: Some(
                    ConcurrencyPolicy::new("input.doc_id", 1)
                        .with_on_conflict(ConcurrencyOnConflict::CancelRunning),
                ),
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                input_schema: None,
                sla: None,
                retry_policy: None,
            },
        );
        *lock = Some(map);
    }

    let trigger_id = uuid::Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_completion_triggers \
         (id, source_workflow_name, terminal_states, target_workflow_name, input_mapping, is_static) \
         VALUES ($1, $2, '[\"Completed\"]'::jsonb, $3, '{\"type\":\"Passthrough\"}'::jsonb, TRUE)",
    )
    .bind::<SqlUuid, _>(trigger_id)
    .bind::<Text, _>(source_wf)
    .bind::<Text, _>(target_wf)
    .execute(&mut conn)
    .await
    .expect("insert trigger");

    let metrics = CapturingMetrics::default();

    // A live incumbent of the TARGET workflow on the key.
    let incumbent = start_run(
        &mut conn,
        &metrics,
        target_wf,
        "incumbent-1",
        key,
        1,
        ConcurrencyOnConflict::CancelRunning,
    )
    .await;
    assert_eq!(state_of(&mut conn, incumbent).await, "RUNNING");

    // The source run, completing with an output that carries the same key so
    // the Passthrough mapping resolves `input.doc_id` to it.
    let source = start_run(
        &mut conn,
        &metrics,
        source_wf,
        "src-1",
        key,
        1,
        ConcurrencyOnConflict::Defer,
    )
    .await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
         SET state = 'COMPLETED', completed_at = NOW(), output = $1::jsonb WHERE id = $2",
    )
    .bind::<Text, _>(serde_json::json!({ "doc_id": key }).to_string())
    .bind::<SqlUuid, _>(source.as_uuid())
    .execute(&mut conn)
    .await
    .expect("complete source");

    metrics.superseded.lock().unwrap().clear();
    metrics.cancelled_terminals.lock().unwrap().clear();

    let deferred = evaluate_triggers_for_execution(
        &mut conn,
        source,
        TerminalState::Completed,
        Some(&metrics),
    )
    .await
    .expect("trigger evaluation must succeed");
    for start in deferred {
        start.spawn();
    }

    // The supersede itself must have happened...
    assert_eq!(
        state_of(&mut conn, incumbent).await,
        "CANCELLED",
        "the trigger-created target must supersede the incumbent"
    );
    assert_eq!(
        non_terminal_count(&mut conn, target_wf).await,
        1,
        "limit = 1 must hold for a trigger-created run"
    );
    // ...AND it must be counted. This is the assertion that was RED: pre-fix the
    // cancel above still happened, but the recorder saw nothing.
    assert_eq!(
        *metrics.superseded.lock().unwrap(),
        vec![target_wf.to_string()],
        "a completion-trigger supersede must emit harvest.concurrency.superseded"
    );
    assert_eq!(
        *metrics.cancelled_terminals.lock().unwrap(),
        vec![target_wf.to_string()],
        "the superseded run's cancelled terminal must be counted too"
    );

    if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
        *lock = previous;
    }
}
