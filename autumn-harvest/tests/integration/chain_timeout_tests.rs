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
use autumn_harvest::execution::{
    SignalWithStartParams, StartWorkflowParams, pause_workflow_execution,
    resume_workflow_execution, signal_with_start_workflow_execution_with_metrics,
    start_or_load_workflow_execution,
};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::scheduler::{
    SchedulerMonitor, compile_dag_catalog, register_workflow_schedules, tick_once,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::timeout;
use autumn_harvest::types::{
    ExecutionId, Priority, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{
    ResetSignalReapplyPolicy, WorkflowContext, WorkflowInfo, WorkflowResetRequest,
    reset_workflow_execution,
};
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

// ─────────────────────────────────────────────────────────────────────────────
// Review-fix test pins (issue #617 code review): T1–T4, plus the FIX 2 fleet-wide
// ceiling tests for signal-with-start and the scheduler tick.
// ─────────────────────────────────────────────────────────────────────────────

/// Leak a UNIQUE `&'static str` workflow name so a worker-driven CAN chain and
/// the GLOBAL timeout scanner's metric spy can be isolated to this test's rows.
fn leaked_name(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// Build a `HandlerRegistry` for a single workflow with a caller-chosen name.
fn named_registry(name: &'static str, handler: WorkflowHandlerFn) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            mcp: false,
            name,
            module: "chain_timeout_tests",
            handler,
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

/// A workflow that continues-as-new until `count == 3`, then COMPLETES.
fn chain_can_3hop_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let count = input
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if count < 3 {
            let _ = ctx
                .continue_as_new(serde_json::json!({"count": count + 1}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

/// A workflow that continues-as-new until `count == 3`, then BLOCKS on a long
/// durable timer so the final successor stays RUNNING (never completes / CANs).
fn chain_can_3hop_block_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let count = input
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if count < 3 {
            let _ = ctx
                .continue_as_new(serde_json::json!({"count": count + 1}))
                .await;
            unreachable!("continue_as_new must not resolve");
        }
        // count == 3: park on a 1-hour durable timer (never fires in the test)
        // so this run stays RUNNING and is the "current" successor the chain
        // scanner can fire against.
        let _ = ctx.timer("chain_block", 3600).await;
        Ok(input)
    })
}

/// Walk one continue-as-new hop: wait for `exec_id` to reach `CONTINUED_AS_NEW`,
/// then return the successor recorded in its `WorkflowContinuedAsNew` event.
async fn walk_can_successor(url: &str, exec_id: ExecutionId) -> ExecutionId {
    let _ = wait_for_execution_state(url, exec_id, "CONTINUED_AS_NEW").await;
    let history = load_history_from_url(url, exec_id).await;
    history
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, .. } => Some(*new_exec_id),
            _ => None,
        })
        .expect("history must contain WorkflowContinuedAsNew")
}

// ── T1: pause/resume shifts the per-run `deadline_at` by the pause span but must
//    LEAVE `chain_deadline_at` UNCHANGED (absolute wall-clock; not suspended by
//    pause). A mutant adding `chain_deadline_at + pause_span` to resume fails this.
#[tokio::test]
async fn pause_resume_shifts_run_deadline_but_not_chain_deadline() {
    use autumn_harvest::schema::harvest_workflow_executions as e;

    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // A RUNNING execution with BOTH a per-run deadline and a chain deadline.
    let wf_name = format!("chain_pause_{}", Uuid::new_v4().simple());
    let exec_id = insert_named_running(&mut conn, &wf_name, "wf-1").await;

    let now = Utc::now();
    let run_deadline_before = now + ChronoDuration::minutes(30);
    let chain_deadline_before = now + ChronoDuration::days(7);
    diesel::update(e::table.find(exec_id.as_uuid()))
        .set(e::deadline_at.eq(Some(run_deadline_before)))
        .execute(&mut conn)
        .await
        .expect("set per-run deadline");
    set_chain_columns(
        &mut conn,
        exec_id,
        Some(ChronoDuration::days(7)),
        Some(chain_deadline_before),
    )
    .await;

    pause_workflow_execution(&mut conn, exec_id, Some("hold"), "oncall", &NoOpMetrics)
        .await
        .expect("pause should succeed");

    // Backdate `paused_at` 30 minutes so resume computes a deterministic span.
    let paused_at = now - ChronoDuration::minutes(30);
    diesel::update(e::table.find(exec_id.as_uuid()))
        .set(e::paused_at.eq(Some(paused_at)))
        .execute(&mut conn)
        .await
        .expect("backdate paused_at");

    resume_workflow_execution(&mut conn, exec_id, "oncall", &NoOpMetrics)
        .await
        .expect("resume should succeed");

    // Per-run deadline advanced by ~30 min pause span.
    let run_after: Option<chrono::DateTime<Utc>> = e::table
        .find(exec_id.as_uuid())
        .select(e::deadline_at)
        .first(&mut conn)
        .await
        .expect("load deadline_at");
    let run_after = run_after.expect("per-run deadline must remain set");
    let expected = run_deadline_before + ChronoDuration::minutes(30);
    assert!(
        (run_after - expected).num_seconds().abs() <= 5,
        "per-run deadline_at must advance by the pause span: after={run_after}, expected≈{expected}"
    );

    // Chain deadline UNCHANGED (absolute; not shifted by pause).
    let (_chain_timeout, chain_after) = load_chain_columns(&mut conn, exec_id).await;
    let chain_after = chain_after.expect("chain_deadline_at must remain set");
    assert!(
        (chain_after - chain_deadline_before)
            .num_milliseconds()
            .abs()
            < 10,
        "chain_deadline_at must NOT be shifted by pause/resume: after={chain_after}, \
         before={chain_deadline_before}"
    );
}

// ── T2: a reset FORK starts a fresh chain origin — `chain_deadline_at` is
//    RE-ANCHORED to the fork's own start (now + chain_execution_timeout), NOT
//    carried from the source's (already-past) value.
#[tokio::test]
async fn reset_re_anchors_chain_deadline_to_the_fork_start() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new();
    let wid = format!("chain-reset-{}", Uuid::new_v4());
    // Source has a 7d chain cap whose deadline is already 3h in the PAST.
    let mut params = default_params(exec_id, &wid);
    params.chain_execution_timeout = Some(ChronoDuration::days(7));
    start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start source");
    set_chain_columns(
        &mut conn,
        exec_id,
        Some(ChronoDuration::days(7)),
        Some(Utc::now() - ChronoDuration::hours(3)),
    )
    .await;

    let before = Utc::now();
    let res = reset_workflow_execution(
        &mut conn,
        exec_id,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "operator reset".to_string(),
            operator_id: "test-operator".to_string(),
            signal_reapply: ResetSignalReapplyPolicy::default(),
            allow_terminal_source: false,
            refuse_erased_source: false,
        },
        None,
    )
    .await
    .expect("reset should succeed");

    let (fork_timeout, fork_deadline) = load_chain_columns(&mut conn, res.new_exec_id).await;
    assert_eq!(
        fork_timeout,
        Some(ChronoDuration::days(7)),
        "fork carries the chain-cap DURATION"
    );
    let dl = fork_deadline.expect("fork chain deadline must be set");
    // Fresh anchor: ≈ now + 7d, definitely > now + 6d and in the FUTURE — NOT the
    // source's 3-hours-ago value (which a carry-instead-of-reanchor mutant yields).
    assert!(
        dl > before + ChronoDuration::days(6),
        "reset must RE-ANCHOR the chain deadline to the fork start (now + 7d), \
         not carry the source's past value: got {dl}"
    );
    assert!(
        dl > Utc::now(),
        "re-anchored fork deadline must be in the future"
    );
}

// ── FIX 2 (AC4): a signal-with-start of a workflow with NO chain attr but a
//    fleet-wide ceiling gets `chain_deadline_at ≈ start + ceiling`.
#[tokio::test]
async fn signal_with_start_applies_fleet_wide_chain_ceiling() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = format!("sws_chain_{}", Uuid::new_v4().simple());
    let wid = format!("sws-chain-{}", Uuid::new_v4());
    let exec_id = ExecutionId::new();
    let before = Utc::now();
    let outcome = signal_with_start_workflow_execution_with_metrics(
        &mut conn,
        SignalWithStartParams {
            workflow_name: &wf_name,
            workflow_id: &wid,
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            // No workflow-declared chain cap, only the fleet-wide ceiling.
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: Some(ChronoDuration::days(3)),
            concurrency_key: None,
            concurrency_limit: None,
            signal_name: "kick",
            signal_payload: serde_json::json!({"v": 1}),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            max_signal_payload_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            reject_fresh_if_debounced: false,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            workflow_info: None,
            start_source_override: None,
        },
        None,
        None,
    )
    .await
    .expect("signal-with-start should succeed");
    assert!(outcome.started_fresh, "must be a fresh start");

    let (chain_timeout, chain_deadline) = load_chain_columns(&mut conn, outcome.exec_id).await;
    assert_eq!(
        chain_timeout,
        Some(ChronoDuration::days(3)),
        "the fleet-wide ceiling becomes the effective chain cap (fleet-wide default)"
    );
    let dl = chain_deadline.expect("ceiling-derived chain deadline");
    assert!(
        dl >= before + ChronoDuration::days(3) - ChronoDuration::seconds(5),
        "chain_deadline_at ≈ start + ceiling: got {dl}"
    );
}

// ── FIX 2 (AC4): a scheduler-tick fire of a workflow with NO chain attr inherits
//    the fleet-wide chain ceiling threaded through the HandlerRegistry.
#[tokio::test]
async fn scheduler_tick_applies_fleet_wide_chain_ceiling() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // Unique workflow name so the fired execution is unambiguous.
    let wf_name = leaked_name("sched_chain");

    // Registry with the fleet-wide chain ceiling set (workflow declares no attr).
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![WorkflowInfo {
                mcp: false,
                name: wf_name,
                module: "chain_timeout_tests",
                handler: chain_can_3hop_workflow,
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
        )
        .with_max_workflow_chain_timeout(Some(std::time::Duration::from_secs(3 * 24 * 3600))),
    );

    // Register a due interval schedule for this workflow.
    let schedule = WorkflowSchedule::new(
        wf_name,
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    )
    .with_input(serde_json::json!({"count": 0}));
    register_workflow_schedules(&mut conn, std::slice::from_ref(&schedule))
        .await
        .expect("register schedule");
    // Force the schedule due.
    diesel::update(
        autumn_harvest::schema::harvest_schedules::table
            .filter(autumn_harvest::schema::harvest_schedules::workflow_name.eq(wf_name)),
    )
    .set(
        autumn_harvest::schema::harvest_schedules::next_run_at
            .eq(Some(Utc::now() - ChronoDuration::seconds(1))),
    )
    .execute(&mut conn)
    .await
    .expect("force due");

    let pool = build_test_pool(&url);
    let before = Utc::now();
    tick_once(
        pool,
        Arc::clone(&registry),
        Arc::new(compile_dag_catalog(vec![]).expect("empty dag catalog")),
        Arc::new(vec![schedule]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should succeed");

    // Find the fired execution for this unique workflow name.
    let row: Option<(Uuid, Option<ChronoDuration>, Option<chrono::DateTime<Utc>>)> =
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::chain_execution_timeout,
                harvest_workflow_executions::chain_deadline_at,
            ))
            .first(&mut conn)
            .await
            .optional()
            .expect("query fired execution");
    let (_id, chain_timeout, chain_deadline) =
        row.expect("scheduler tick must have fired an execution for this workflow");
    assert_eq!(
        chain_timeout,
        Some(ChronoDuration::days(3)),
        "scheduled run inherits the fleet-wide chain ceiling as its effective cap"
    );
    let dl = chain_deadline.expect("scheduled run chain deadline");
    assert!(
        dl >= before + ChronoDuration::days(3) - ChronoDuration::seconds(10),
        "scheduled run chain_deadline_at ≈ start + ceiling: got {dl}"
    );
}

// ── T4(a) (AC7): a worker drives ≥3 real continue-as-new hops; the 3rd successor
//    carries `chain_deadline_at` VERBATIM (across all 3 hops), NOT re-anchored.
#[tokio::test]
async fn three_hop_continue_as_new_carries_chain_deadline_verbatim() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = leaked_name("chain_can3");
    let origin = insert_named_running(&mut conn, wf_name, "chain-can3-1").await;
    // A FUTURE chain deadline so the hops complete without timing out.
    let frozen_chain_deadline = Utc::now() + ChronoDuration::hours(6);
    set_chain_columns(
        &mut conn,
        origin,
        Some(ChronoDuration::days(7)),
        Some(frozen_chain_deadline),
    )
    .await;
    enqueue_started_workflow_task(&mut conn, origin, serde_json::json!({"count": 0})).await;

    let worker = build_runtime_worker(
        "worker-chain-can3",
        2,
        1,
        named_registry(wf_name, chain_can_3hop_workflow),
    );
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Walk three real continue-as-new hops.
    let succ1 = walk_can_successor(&url, origin).await;
    let succ2 = walk_can_successor(&url, succ1).await;
    let succ3 = walk_can_successor(&url, succ2).await;
    let _done = wait_for_execution_state(&url, succ3, "COMPLETED").await;

    worker.shutdown();
    handle.await.expect("worker join");

    let (succ_timeout, succ_deadline) = load_chain_columns(&mut conn, succ3).await;
    assert_eq!(
        succ_timeout,
        Some(ChronoDuration::days(7)),
        "3rd successor carries the chain-cap DURATION verbatim"
    );
    let dl = succ_deadline.expect("3rd successor chain deadline");
    let delta = (dl - frozen_chain_deadline).num_milliseconds().abs();
    assert!(
        delta < 1000,
        "3rd successor chain_deadline_at must equal the origin's absolute value \
         (carried verbatim across 3 hops, NOT recomputed): got {dl}, expected {frozen_chain_deadline}"
    );
    // Falsify a re-anchor: now+7d would be days out; verbatim is ~6h from origin.
    assert!(
        dl < Utc::now() + ChronoDuration::days(1),
        "a re-anchored (now + 7d) deadline would be days out; verbatim is ~6h"
    );
}

// ── T4(b) (AC7 behavioral): with a SHORT per-run cap (re-anchored every hop, never
//    fires) and a chain deadline crossed after ≥3 hops, the CURRENT RUNNING
//    successor (not the origin) times out; the CHAIN counter fires, the RUN
//    counter does not.
#[tokio::test]
async fn chain_cap_fires_on_a_can_successor_not_the_origin() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = leaked_name("chain_can3b");
    let origin = insert_named_running(&mut conn, wf_name, "chain-can3b-1").await;
    // A FUTURE chain deadline so the 3 hops occur without timing out mid-drive.
    set_chain_columns(
        &mut conn,
        origin,
        Some(ChronoDuration::days(7)),
        Some(Utc::now() + ChronoDuration::hours(6)),
    )
    .await;
    enqueue_started_workflow_task(&mut conn, origin, serde_json::json!({"count": 0})).await;

    let worker = build_runtime_worker(
        "worker-chain-can3b",
        2,
        1,
        named_registry(wf_name, chain_can_3hop_block_workflow),
    );
    let pool = build_test_pool(&url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let succ1 = walk_can_successor(&url, origin).await;
    let succ2 = walk_can_successor(&url, succ1).await;
    let succ3 = walk_can_successor(&url, succ2).await;
    // succ3 blocks at count==3 → stays RUNNING (the "current successor").
    let _ = wait_for_execution_state(&url, succ3, "RUNNING").await;

    worker.shutdown();
    handle.await.expect("worker join");

    // Cross the chain deadline for succ3, keep its per-run deadline in the FUTURE
    // (so a per-run cap alone would NOT fire) — exactly the "chain fires where a
    // per-run cap re-anchors" scenario.
    set_chain_columns(
        &mut conn,
        succ3,
        Some(ChronoDuration::days(7)),
        Some(Utc::now() - ChronoDuration::seconds(5)),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(succ3.as_uuid()))
        .set(
            harvest_workflow_executions::deadline_at
                .eq(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .execute(&mut conn)
        .await
        .expect("set future per-run deadline on succ3");

    let spy = TimeoutSpy::default();
    timeout::enforce_workflow_execution_timeouts(&mut conn, &spy)
        .await
        .expect("scan");

    // The CURRENT successor timed out — not the origin (which is CONTINUED_AS_NEW).
    assert_eq!(load_state(&mut conn, succ3).await, "TIMED_OUT");
    assert_eq!(
        load_state(&mut conn, origin).await,
        "CONTINUED_AS_NEW",
        "the origin must stay CONTINUED_AS_NEW; only the current successor fires"
    );
    let history = store::load_history(&mut conn, succ3)
        .await
        .expect("history");
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowExecutionTimedOut { .. })),
        "successor must append WorkflowExecutionTimedOut"
    );
    // AC6: the CHAIN counter fired for this unique workflow, the RUN counter did not.
    assert_eq!(
        spy.chain_count_for(wf_name),
        1,
        "chain counter fires once for the successor"
    );
    assert_eq!(
        spy.run_count_for(wf_name),
        0,
        "the per-run timeout counter must NOT fire for a chain timeout"
    );
}
