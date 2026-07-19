#![cfg(feature = "db")]
//! Integration tests for the chain-scoped lifetime cap — issue #617.
//!
//! Distinct from #243's per-run `execution_timeout`, the chain cap is anchored at
//! the FIRST run's start and CARRIED VERBATIM across every continue-as-new, so a
//! runaway loop cannot escape it by continuing-as-new. Enforced by the existing
//! timeout scanner and the existing `WorkflowExecutionTimedOut` event.
//!
//! - AC2/AC7: a chain cap fires via the scanner where a per-run cap alone would
//!   not; the run terminates (`TIMED_OUT`) with `WorkflowExecutionTimedOut`.
//! - AC9: a continue-as-new successor carries `chain_deadline_at` VERBATIM.
//! - AC4: the builder ceiling doubles as a fleet-wide default.
//! - AC6: chain timeouts increment `harvest.workflow.chain_timeout`; per-run
//!   timeouts increment `harvest.workflow.timeout`.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::timeout;
use autumn_harvest::types::{
    ExecutionId, Priority, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{WorkflowContext, WorkflowInfo};
use chrono::{Duration as ChronoDuration, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

/// Insert a RUNNING execution row with a caller-chosen UNIQUE `workflow_name` and
/// `workflow_id`. The unique name lets scanner tests filter the metrics spy to
/// exactly their own row, so the GLOBAL `enforce_workflow_execution_timeouts`
/// scan — which times out every crossed-deadline RUNNING row in the shared CI
/// database, not just this one — cannot make assertions flaky.
async fn insert_named_running(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&autumn_harvest::models::NewWorkflowExecution {
            continued_from_exec_id: None,
            first_exec_id: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
            id: exec_id.as_uuid(),
            workflow_name,
            workflow_id,
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            sla_deadline_at: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            completion_callbacks: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
        })
        .execute(conn)
        .await
        .expect("insert named running execution");
    exec_id
}

/// Spy recorder counting both timeout counters so we can assert which one fired.
/// Assertions filter by `workflow_name` because the scanner is a GLOBAL scan.
#[derive(Default)]
struct TimeoutSpy {
    chain: Mutex<Vec<(String, String)>>,
    run: Mutex<Vec<(String, String)>>,
}
impl TimeoutSpy {
    fn chain_count_for(&self, name: &str) -> usize {
        self.chain
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .count()
    }
    fn run_count_for(&self, name: &str) -> usize {
        self.run
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .count()
    }
}
impl MetricsRecorder for TimeoutSpy {
    fn record_workflow_chain_timeout(&self, workflow_name: &str, queue: &str) {
        self.chain
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        self.run
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Directly set the chain columns on an existing execution row.
async fn set_chain_columns(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    chain_timeout: Option<ChronoDuration>,
    chain_deadline_at: Option<chrono::DateTime<Utc>>,
) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::chain_execution_timeout.eq(chain_timeout),
            harvest_workflow_executions::chain_deadline_at.eq(chain_deadline_at),
        ))
        .execute(conn)
        .await
        .expect("set chain columns");
}

/// Load the two chain columns for an execution.
async fn load_chain_columns(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (Option<ChronoDuration>, Option<chrono::DateTime<Utc>>) {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select((
            harvest_workflow_executions::chain_execution_timeout,
            harvest_workflow_executions::chain_deadline_at,
        ))
        .first(conn)
        .await
        .expect("load chain columns")
}

async fn load_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(conn)
        .await
        .expect("load state")
}

fn default_params(exec_id: ExecutionId, workflow_id: &str) -> StartWorkflowParams<'_> {
    StartWorkflowParams {
        workflow_name: "chain_cap_workflow",
        workflow_id,
        exec_id,
        input: serde_json::json!({}),
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
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

// ── AC2 / AC7: scanner fires on the chain cap where a per-run cap alone would
//    not — the essence of the "runaway loop cannot escape via CAN" guarantee.
#[tokio::test]
async fn chain_cap_fires_via_scanner_when_per_run_cap_would_not() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // Unique workflow_name so the GLOBAL scan's spy can be filtered to this row.
    let wf_name = format!("chain_scan_{}", Uuid::new_v4().simple());
    let exec_id = insert_named_running(&mut conn, &wf_name, "wf-1").await;
    // Per-run deadline is comfortably in the FUTURE (would never fire); chain
    // deadline already crossed. This models a run whose current attempt is fine
    // but whose whole continue-as-new chain has outlived its lifetime cap.
    let now = Utc::now();
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::deadline_at.eq(Some(now + ChronoDuration::hours(1))))
        .execute(&mut conn)
        .await
        .expect("set future per-run deadline");
    set_chain_columns(
        &mut conn,
        exec_id,
        Some(ChronoDuration::days(7)),
        Some(now - ChronoDuration::seconds(5)),
    )
    .await;

    let spy = TimeoutSpy::default();
    let n = timeout::enforce_workflow_execution_timeouts(&mut conn, &spy)
        .await
        .expect("scan");
    assert!(n >= 1, "at least the chain-expired run must be timed out");

    // State transitioned to TIMED_OUT.
    assert_eq!(load_state(&mut conn, exec_id).await, "TIMED_OUT");
    // WorkflowExecutionTimedOut appended (no new event variant — reuses #243's).
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionTimedOut { .. })),
        "must append WorkflowExecutionTimedOut"
    );
    // AC6: the CHAIN counter fired for THIS run, NOT the run counter.
    assert_eq!(
        spy.chain_count_for(&wf_name),
        1,
        "chain counter fires once for this run"
    );
    assert_eq!(
        spy.run_count_for(&wf_name),
        0,
        "the per-run timeout counter must NOT fire for a chain timeout"
    );
}

// ── Chain-only expiry (no per-run deadline configured) must not panic and must
//    still terminate the run.
#[tokio::test]
async fn chain_only_expiry_times_out_without_panicking() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = format!("chain_only_{}", Uuid::new_v4().simple());
    let exec_id = insert_named_running(&mut conn, &wf_name, "wf-1").await;
    // deadline_at stays NULL (no per-run cap); only the chain deadline is set.
    set_chain_columns(
        &mut conn,
        exec_id,
        Some(ChronoDuration::days(1)),
        Some(Utc::now() - ChronoDuration::seconds(1)),
    )
    .await;

    let spy = TimeoutSpy::default();
    let n = timeout::enforce_workflow_execution_timeouts(&mut conn, &spy)
        .await
        .expect("scan must not panic on a chain-only expiry");
    assert!(n >= 1);
    assert_eq!(load_state(&mut conn, exec_id).await, "TIMED_OUT");
    assert_eq!(spy.chain_count_for(&wf_name), 1);
}

// ── AC6: a per-run timeout increments the RUN counter, not the chain counter.
#[tokio::test]
async fn per_run_timeout_increments_run_counter_not_chain() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = format!("per_run_{}", Uuid::new_v4().simple());
    let exec_id = insert_named_running(&mut conn, &wf_name, "wf-1").await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(
            harvest_workflow_executions::deadline_at
                .eq(Some(Utc::now() - ChronoDuration::seconds(1))),
        )
        .execute(&mut conn)
        .await
        .expect("set past per-run deadline");
    // No chain deadline → the run cap fires.

    let spy = TimeoutSpy::default();
    timeout::enforce_workflow_execution_timeouts(&mut conn, &spy)
        .await
        .expect("scan");
    assert_eq!(
        spy.run_count_for(&wf_name),
        1,
        "run counter fires for this run"
    );
    assert_eq!(
        spy.chain_count_for(&wf_name),
        0,
        "chain counter must not fire for a per-run timeout"
    );
}

// ── Origin start anchors chain_deadline_at at start + chain_execution_timeout.
#[tokio::test]
async fn origin_start_anchors_chain_deadline() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new();
    let wid = format!("chain-origin-{}", Uuid::new_v4());
    let mut params = default_params(exec_id, &wid);
    params.chain_execution_timeout = Some(ChronoDuration::days(7));
    let before = Utc::now();
    start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start");
    let after = Utc::now();

    let (chain_timeout, chain_deadline) = load_chain_columns(&mut conn, exec_id).await;
    assert_eq!(chain_timeout, Some(ChronoDuration::days(7)));
    let dl = chain_deadline.expect("chain_deadline_at must be set");
    // chain_deadline_at ≈ started_at + 7d.
    assert!(dl >= before + ChronoDuration::days(7) - ChronoDuration::seconds(5));
    assert!(dl <= after + ChronoDuration::days(7) + ChronoDuration::seconds(5));
}

// ── inherited_chain_deadline_at is used VERBATIM (the workflow-retry #523 carry
//    mechanism) — NOT recomputed as now + timeout.
#[tokio::test]
async fn inherited_chain_deadline_is_used_verbatim() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // A deliberately PAST absolute deadline: proves it is NOT re-anchored to now.
    let frozen = Utc::now() - ChronoDuration::hours(3);
    let exec_id = ExecutionId::new();
    let wid = format!("chain-inherit-{}", Uuid::new_v4());
    let mut params = default_params(exec_id, &wid);
    params.chain_execution_timeout = Some(ChronoDuration::days(7));
    params.inherited_chain_deadline_at = Some(frozen);
    start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start");

    let (_timeout, chain_deadline) = load_chain_columns(&mut conn, exec_id).await;
    // TIMESTAMPTZ truncates sub-microsecond precision on the round-trip, so
    // compare within a tiny tolerance rather than exact equality.
    let dl = chain_deadline.expect("inherited chain deadline must be set");
    assert!(
        (dl - frozen).num_milliseconds().abs() < 10,
        "inherited chain deadline must be copied verbatim, not recomputed: got \
         {dl}, expected {frozen}"
    );
    // Falsify the re-anchor hypothesis: a re-anchored (now + 7d) deadline would be
    // far in the future; the verbatim value is 3h in the PAST.
    assert!(
        dl < Utc::now(),
        "a re-anchored now+7d deadline would be in the future; verbatim is 3h ago"
    );
}

// ── AC4: the builder ceiling doubles as a fleet-wide DEFAULT — a workflow that
//    declares NO chain cap still inherits the ceiling as its chain deadline.
#[tokio::test]
async fn ceiling_acts_as_fleet_wide_default() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new();
    let wid = format!("chain-ceiling-{}", Uuid::new_v4());
    let mut params = default_params(exec_id, &wid);
    // No workflow-declared chain cap, only the fleet-wide ceiling.
    params.chain_execution_timeout = None;
    params.max_workflow_chain_timeout_ceiling = Some(ChronoDuration::days(3));
    let before = Utc::now();
    start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start");

    let (chain_timeout, chain_deadline) = load_chain_columns(&mut conn, exec_id).await;
    assert_eq!(
        chain_timeout,
        Some(ChronoDuration::days(3)),
        "ceiling becomes the effective chain cap (fleet-wide default)"
    );
    let dl = chain_deadline.expect("ceiling-derived chain deadline");
    assert!(dl >= before + ChronoDuration::days(3) - ChronoDuration::seconds(5));
}

// ── AC4: when both are present, the effective chain cap is the MIN (ceiling caps).
#[tokio::test]
async fn ceiling_caps_a_larger_workflow_declared_chain_timeout() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new();
    let wid = format!("chain-cap-min-{}", Uuid::new_v4());
    let mut params = default_params(exec_id, &wid);
    params.chain_execution_timeout = Some(ChronoDuration::days(30)); // workflow asks 30d
    params.max_workflow_chain_timeout_ceiling = Some(ChronoDuration::days(7)); // ceiling 7d
    start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start");

    let (chain_timeout, _dl) = load_chain_columns(&mut conn, exec_id).await;
    assert_eq!(
        chain_timeout,
        Some(ChronoDuration::days(7)),
        "effective chain cap is min(workflow, ceiling)"
    );
}

/// A workflow that continues-as-new exactly once (phase init → next).
fn chain_can_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let phase = input
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if phase == "init" {
            let _ = ctx
                .continue_as_new(serde_json::json!({"phase": "done"}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

fn chain_can_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name: "e2e_test_workflow",
            module: "chain_timeout_tests",
            handler: chain_can_workflow,
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
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
        }],
        vec![],
    ))
}

// ── AC9 (the falsifiable success metric): a continue-as-new successor CARRIES
//    the chain deadline VERBATIM. A per-run cap alone would re-anchor on each CAN
//    (and so a runaway loop could evade it); the chain cap does not, so after any
//    number of continue-as-new hops the same absolute chain deadline persists.
#[tokio::test]
async fn continue_as_new_carries_chain_deadline_verbatim() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // Scrub the shared e2e identity so this test is isolated.
    for stmt in [
        "DELETE FROM harvest_events WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = 'e2e_test_workflow')",
        "DELETE FROM harvest_task_queue WHERE workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = 'e2e_test_workflow')",
        "DELETE FROM harvest_workflow_executions WHERE workflow_name = 'e2e_test_workflow'",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .expect("scrub");
    }

    let origin = insert_workflow_execution(&mut conn).await;
    // Stamp the origin's chain cap: a FUTURE absolute deadline so the successor
    // is not immediately timed out; we assert the successor copies it exactly.
    let frozen_chain_deadline = Utc::now() + ChronoDuration::hours(6);
    set_chain_columns(
        &mut conn,
        origin,
        Some(ChronoDuration::days(7)),
        Some(frozen_chain_deadline),
    )
    .await;
    enqueue_started_workflow_task(&mut conn, origin, serde_json::json!({"phase": "init"})).await;

    let worker = build_runtime_worker("worker-chain-can", 2, 1, chain_can_registry());
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let _sealed = wait_for_execution_state(&url, origin, "CONTINUED_AS_NEW").await;
    let history = load_history_from_url(&url, origin).await;
    let successor = history
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("origin history must contain WorkflowContinuedAsNew");
    let _done = wait_for_execution_state(&url, successor, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker join");

    let (succ_timeout, succ_deadline) = load_chain_columns(&mut conn, successor).await;
    assert_eq!(
        succ_timeout,
        Some(ChronoDuration::days(7)),
        "successor inherits the chain cap duration verbatim"
    );
    // The load truncates sub-microsecond precision on round-trip; compare within
    // a tiny tolerance rather than exact equality.
    let dl = succ_deadline.expect("successor chain deadline");
    let delta = (dl - frozen_chain_deadline).num_milliseconds().abs();
    assert!(
        delta < 1000,
        "successor's chain_deadline_at must equal the predecessor's absolute \
         value (carried verbatim, NOT recomputed as now + timeout): got {dl}, \
         expected {frozen_chain_deadline}"
    );
    // Falsify the re-anchor hypothesis: a re-anchored deadline would be ~now+7d,
    // far in the future; the verbatim value is only +6h from the origin start.
    assert!(
        dl < Utc::now() + ChronoDuration::days(1),
        "a re-anchored (now + 7d) deadline would be days out; verbatim is ~6h"
    );
}
