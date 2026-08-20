#![cfg(feature = "db")]
//! Integration tests for DAG-level execution timeout and SLA — issue #743.
//!
//! Proves the whole pipeline end to end: a `DagInfo` with `execution_timeout`/
//! `sla` set -> `DagInfo::as_workflow_info()` -> the shadow `WorkflowInfo` ->
//! `StartWorkflowParams` (mirroring exactly how the scheduler tick resolves
//! these fields today for `#[workflow]`s, see
//! `scheduler::tick_one_workflow_schedule`) -> `deadline_at`/`sla_deadline_at`
//! on the execution row -> the EXISTING #243/#487 scanners
//! (`timeout::enforce_workflow_execution_timeouts`,
//! `timeout::enforce_workflow_sla_breaches`). No new scanner, no new event
//! variant, no new migration — issue #743 AC12.
//!
//! - AC1/AC3: a DAG-declared `execution_timeout` produces `deadline_at`, and a
//!   crossed deadline is caught by the existing scanner (`TIMED_OUT`, tagged
//!   `TimeoutType::WorkflowExecution` -- not the chain path).
//! - AC2/AC4: a DAG-declared `sla` produces `sla_deadline_at`, and a crossed
//!   SLA is caught by the existing scanner (metric exactly once, lifecycle
//!   unaffected -- the run can still reach `COMPLETED`).
//! - AC5: `sla` is clamped down to `execution_timeout` at start when it would
//!   otherwise exceed it (reuses the shared clamp logic in `execution.rs`).
//! - AC6: the fleet-wide `max_execution_timeout_ceiling` applies to a
//!   DAG-declared timeout identically to a `#[workflow]`-declared one.
//! - AC7: a DAG registered without the new attributes gets `None`/`NULL`
//!   deadlines end to end (zero regression).

use std::sync::{Arc, Mutex};

use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::scheduler::{
    SchedulerMonitor, compile_dag_catalog, register_workflow_schedules, tick_once,
    trigger_unified_dag,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::timeout;
use autumn_harvest::types::{
    ExecutionId, Priority, ShardId, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{DagBuilder, DagInfo, OverlapPolicy, WorkflowInfo};
use chrono::{Duration as ChronoDuration, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env};

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Build a `DagInfo` carrying the given `execution_timeout`/`sla`, with a
/// no-op builder and a trivial identity `workflow_handler` so
/// `as_workflow_info()` returns `Some` -- exactly the shape the `#[dag]`
/// macro's companion function produces (issue #743).
fn deadline_dag(
    name: &'static str,
    execution_timeout: Option<std::time::Duration>,
    sla: Option<std::time::Duration>,
) -> DagInfo {
    DagInfo {
        name,
        module: "tests",
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: None,
        builder: |_: &mut DagBuilder| {},
        workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
        jitter: std::time::Duration::ZERO,
        overlap_policy: OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
        execution_timeout,
        sla,
    }
}

/// Mirrors `scheduler::tick_one_workflow_schedule`'s resolution of a
/// `WorkflowInfo`'s `execution_timeout`/`sla` into `chrono::Duration` for
/// `StartWorkflowParams` -- the exact conversion a real scheduled DAG fire
/// performs.
fn resolve_deadlines(info: &WorkflowInfo) -> (Option<ChronoDuration>, Option<ChronoDuration>) {
    let execution_timeout = info
        .execution_timeout
        .and_then(|d| ChronoDuration::from_std(d).ok());
    let sla = info.sla.and_then(|d| ChronoDuration::from_std(d).ok());
    (execution_timeout, sla)
}

fn base_params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    execution_timeout: Option<ChronoDuration>,
    max_execution_timeout_ceiling: Option<ChronoDuration>,
    sla: Option<ChronoDuration>,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla,
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

#[derive(Queryable)]
struct DeadlineRow {
    state: String,
    deadline_at: Option<chrono::DateTime<Utc>>,
    sla_deadline_at: Option<chrono::DateTime<Utc>>,
    sla_breached: bool,
    error: Option<String>,
}

async fn load_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> DeadlineRow {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select((
            harvest_workflow_executions::state,
            harvest_workflow_executions::deadline_at,
            harvest_workflow_executions::sla_deadline_at,
            harvest_workflow_executions::sla_breached,
            harvest_workflow_executions::error,
        ))
        .first(conn)
        .await
        .expect("load deadline row")
}

async fn backdate_deadline_at(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(
            harvest_workflow_executions::deadline_at
                .eq(Some(Utc::now() - ChronoDuration::minutes(5))),
        )
        .execute(conn)
        .await
        .expect("backdate deadline_at");
}

async fn backdate_sla_deadline_at(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(
            harvest_workflow_executions::sla_deadline_at
                .eq(Some(Utc::now() - ChronoDuration::minutes(5))),
        )
        .execute(conn)
        .await
        .expect("backdate sla_deadline_at");
}

#[derive(Default)]
struct TimeoutSpy {
    run: Mutex<Vec<(String, String)>>,
}
impl MetricsRecorder for TimeoutSpy {
    fn record_workflow_timeout(&self, workflow_name: &str, queue: &str) {
        self.run
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

#[derive(Default)]
struct SlaSpy {
    breaches: Mutex<Vec<(String, String)>>,
}
impl MetricsRecorder for SlaSpy {
    fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
        self.breaches
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

/// Leak a UNIQUE `&'static str` DAG name so a scheduler-tick-driven or
/// directly-triggered DAG run's own row can be found unambiguously in the
/// shared CI database (`DagInfo::name` requires `&'static str`).
fn leaked_dag_name(prefix: &str) -> &'static str {
    Box::leak(unique_name(prefix).into_boxed_str())
}

// ── AC1 / AC2: DagInfo-declared timeout/sla translate into deadline columns ──

#[tokio::test]
async fn dag_declared_execution_timeout_and_sla_set_deadline_columns() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = unique_name("dag743_deadlines");
    let dag = deadline_dag(
        "nightly_deadline_etl",
        Some(std::time::Duration::from_secs(4 * 3600)),
        Some(std::time::Duration::from_secs(3600)),
    );
    let info = dag
        .as_workflow_info()
        .expect("workflow_handler is Some, so as_workflow_info must be Some");
    let (execution_timeout, sla) = resolve_deadlines(&info);

    let exec_id = ExecutionId::new();
    let before = Utc::now();
    let started = start_or_load_workflow_execution(
        &mut conn,
        base_params(&wf_name, "wf-1", exec_id, execution_timeout, None, sla),
        None,
    )
    .await
    .expect("start");
    assert!(started.created);
    let after = Utc::now();

    let row = load_row(&mut conn, exec_id).await;
    let deadline_at = row
        .deadline_at
        .expect("execution_timeout must set deadline_at");
    assert!(
        deadline_at >= before + ChronoDuration::hours(4)
            && deadline_at <= after + ChronoDuration::hours(4),
        "deadline_at must be started_at + 4h (the DagInfo's declared execution_timeout)"
    );
    let sla_deadline_at = row.sla_deadline_at.expect("sla must set sla_deadline_at");
    assert!(
        sla_deadline_at >= before + ChronoDuration::hours(1)
            && sla_deadline_at <= after + ChronoDuration::hours(1),
        "sla_deadline_at must be started_at + 1h (the DagInfo's declared sla)"
    );
}

// ── AC3: a crossed DAG-declared execution_timeout is caught by the EXISTING
//    enforce_workflow_execution_timeouts scanner -- no new scanner ───────────

#[tokio::test]
async fn dag_execution_timeout_scanner_transitions_expired_run_to_timed_out() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = unique_name("dag743_scanner");
    let dag = deadline_dag(
        "nightly_deadline_etl",
        Some(std::time::Duration::from_secs(3600)),
        None,
    );
    let info = dag.as_workflow_info().unwrap();
    let (execution_timeout, _sla) = resolve_deadlines(&info);

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(&wf_name, "wf-1", exec_id, execution_timeout, None, None),
        None,
    )
    .await
    .expect("start");

    // Simulate time passing without waiting an hour: the same mechanism a
    // real overrun uses (this is only what NOW() being later than the
    // deadline would look like).
    backdate_deadline_at(&mut conn, exec_id).await;

    let spy = TimeoutSpy::default();
    let n = timeout::enforce_workflow_execution_timeouts(&mut conn, &spy)
        .await
        .expect("scan");
    assert!(n >= 1, "at least this row must be caught by the scan");

    let row = load_row(&mut conn, exec_id).await;
    assert_eq!(
        row.state, "TIMED_OUT",
        "the existing #243 scanner must transition an expired DAG run to TIMED_OUT"
    );
    assert!(
        row.error
            .as_deref()
            .is_some_and(|e| e.contains("WorkflowExecution")),
        "must be classified via TimeoutType::WorkflowExecution (the per-run path), not the chain path"
    );
    assert!(
        spy.run.lock().unwrap().iter().any(|(n, _)| n == &wf_name),
        "harvest.workflow.timeout must fire for the DAG's shadow workflow_name"
    );
}

// ── AC4: a crossed DAG-declared sla is caught by the EXISTING
//    enforce_workflow_sla_breaches scanner exactly once, lifecycle unaffected

#[tokio::test]
async fn dag_sla_breach_emits_metric_exactly_once_without_terminating() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = unique_name("dag743_sla");
    let dag = deadline_dag(
        "nightly_deadline_etl",
        Some(std::time::Duration::from_secs(24 * 3600)),
        Some(std::time::Duration::from_secs(3600)),
    );
    let info = dag.as_workflow_info().unwrap();
    let (execution_timeout, sla) = resolve_deadlines(&info);

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(&wf_name, "wf-1", exec_id, execution_timeout, None, sla),
        None,
    )
    .await
    .expect("start");

    backdate_sla_deadline_at(&mut conn, exec_id).await;

    let spy = SlaSpy::default();
    let n1 = timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("first scan");
    assert!(n1 >= 1);

    let row = load_row(&mut conn, exec_id).await;
    assert!(row.sla_breached, "sla_breached must be set");
    assert_eq!(
        row.state, "RUNNING",
        "a soft SLA breach must never alter the run's lifecycle"
    );

    // Idempotent: a second scan does not re-fire the metric for this row.
    timeout::enforce_workflow_sla_breaches(&mut conn, &spy)
        .await
        .expect("second scan");
    let call_count = spy
        .breaches
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, _)| n == &wf_name)
        .count();
    assert_eq!(
        call_count, 1,
        "harvest.workflow.sla_breached must fire exactly once for this run"
    );

    // The breaching run can still reach COMPLETED normally.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::output.eq(Some(serde_json::json!({"ok": true}))),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .expect("complete");
    let row_after = load_row(&mut conn, exec_id).await;
    assert_eq!(row_after.state, "COMPLETED");
    assert!(
        row_after.sla_breached,
        "breach observation persists into the terminal record"
    );
}

// ── AC5: sla is clamped down to execution_timeout at start when it would
//    otherwise exceed it -- the same clamp `#[workflow]` reuses ─────────────

#[tokio::test]
async fn dag_sla_is_clamped_to_execution_timeout_at_start() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = unique_name("dag743_clamp");
    // sla (10h) is declared LARGER than execution_timeout (1h).
    let dag = deadline_dag(
        "nightly_deadline_etl",
        Some(std::time::Duration::from_secs(3600)),
        Some(std::time::Duration::from_secs(10 * 3600)),
    );
    let info = dag.as_workflow_info().unwrap();
    let (execution_timeout, sla) = resolve_deadlines(&info);

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(&wf_name, "wf-1", exec_id, execution_timeout, None, sla),
        None,
    )
    .await
    .expect("start");

    let row = load_row(&mut conn, exec_id).await;
    let deadline_at = row.deadline_at.expect("execution_timeout must be set");
    let sla_deadline_at = row.sla_deadline_at.expect("sla must be set");
    assert_eq!(
        sla_deadline_at, deadline_at,
        "AC5: an sla larger than execution_timeout must be clamped down to it at start"
    );
}

// ── AC6: the fleet-wide ceiling applies to a DAG-declared timeout
//    identically to a `#[workflow]`-declared one ─────────────────────────────

#[tokio::test]
async fn dag_and_workflow_execution_timeout_ceilings_apply_identically() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let ceiling = Some(ChronoDuration::hours(1));

    // A DAG declaring a 10h execution_timeout, capped by a 1h fleet ceiling.
    let dag_wf_name = unique_name("dag743_ceiling_dag");
    let dag = deadline_dag(
        "nightly_deadline_etl",
        Some(std::time::Duration::from_secs(10 * 3600)),
        None,
    );
    let info = dag.as_workflow_info().unwrap();
    let (dag_execution_timeout, _) = resolve_deadlines(&info);
    let dag_exec_id = ExecutionId::new();
    let dag_before = Utc::now();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(
            &dag_wf_name,
            "wf-1",
            dag_exec_id,
            dag_execution_timeout,
            ceiling,
            None,
        ),
        None,
    )
    .await
    .expect("start dag-derived");
    let dag_after = Utc::now();
    let dag_row = load_row(&mut conn, dag_exec_id).await;
    let dag_deadline_at = dag_row.deadline_at.expect("must be set");

    // A plain (non-DAG) start declaring the SAME 10h execution_timeout, capped
    // by the SAME 1h ceiling -- mirrors exactly how a `#[workflow(execution_timeout
    // = "10h")]` would resolve at start time.
    let wf_name = unique_name("dag743_ceiling_workflow");
    let wf_exec_id = ExecutionId::new();
    let wf_before = Utc::now();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(
            &wf_name,
            "wf-1",
            wf_exec_id,
            Some(ChronoDuration::hours(10)),
            ceiling,
            None,
        ),
        None,
    )
    .await
    .expect("start workflow-declared");
    let wf_after = Utc::now();
    let wf_row = load_row(&mut conn, wf_exec_id).await;
    let wf_deadline_at = wf_row.deadline_at.expect("must be set");

    // Both must be clamped to started_at + 1h (the ceiling), NOT + 10h.
    assert!(
        dag_deadline_at <= dag_before + ChronoDuration::hours(1) + ChronoDuration::seconds(5)
            && dag_deadline_at
                >= dag_before + ChronoDuration::hours(1) - ChronoDuration::seconds(5),
        "the DAG-declared 10h timeout must be clamped to the 1h ceiling"
    );
    assert!(
        wf_deadline_at <= wf_before + ChronoDuration::hours(1) + ChronoDuration::seconds(5)
            && wf_deadline_at >= wf_before + ChronoDuration::hours(1) - ChronoDuration::seconds(5),
        "the workflow-declared 10h timeout must be clamped to the 1h ceiling identically"
    );
    let dag_delta = dag_deadline_at - dag_before;
    let wf_delta = wf_deadline_at - wf_before;
    assert!(
        (dag_delta - wf_delta).num_seconds().abs() < 2,
        "the SAME ceiling clamp must apply identically regardless of DAG vs #[workflow] origin: dag_delta={dag_delta:?} wf_delta={wf_delta:?} dag_after={dag_after:?} wf_after={wf_after:?}"
    );
}

// ── AC7: a DAG without the new attributes gets null deadlines (zero
//    regression) ──────────────────────────────────────────────────────────

#[tokio::test]
async fn dag_without_deadline_attributes_has_null_deadline_columns() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let wf_name = unique_name("dag743_no_attrs");
    let dag = deadline_dag("plain_dag", None, None);
    let info = dag.as_workflow_info().unwrap();
    assert_eq!(info.execution_timeout, None);
    assert_eq!(info.sla, None);
    let (execution_timeout, sla) = resolve_deadlines(&info);

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        base_params(&wf_name, "wf-1", exec_id, execution_timeout, None, sla),
        None,
    )
    .await
    .expect("start");

    let row = load_row(&mut conn, exec_id).await;
    assert_eq!(
        row.deadline_at, None,
        "AC7: a DAG without execution_timeout must have a NULL deadline_at"
    );
    assert_eq!(
        row.sla_deadline_at, None,
        "AC7: a DAG without sla must have a NULL sla_deadline_at"
    );
    assert_eq!(row.state, "RUNNING");
}

// ─────────────────────────────────────────────────────────────────────────────
// Codex review (PR #1141) on issue #743: the 7 tests above prove the
// resolution mechanism (`DagInfo::as_workflow_info` -> `StartWorkflowParams`)
// works correctly in isolation, but 3 REAL production code paths that also
// start a scheduled/manual DAG run hardcoded `execution_timeout`/`sla`/the
// fleet-wide ceiling to `None` instead of reading them from the shadow
// `WorkflowInfo` the way `tick_one_workflow_schedule`'s NON-buffered branch
// already correctly does. These 3 tests drive the REAL entry points
// (`trigger_unified_dag`, `tick_once` via a buffered-drain fire, `tick_once`
// with a fleet-wide ceiling configured) rather than hand-mirroring scheduler
// resolution logic in the test itself -- exactly the gap that let these 3
// sites go unnoticed by the 7 tests above.
// ─────────────────────────────────────────────────────────────────────────────

fn dag_registry(dag: &DagInfo) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            dag.as_workflow_info()
                .expect("workflow_handler is Some, so as_workflow_info must be Some"),
        ],
        vec![],
    ))
}

async fn insert_manual_workflow_schedule(
    conn: &mut AsyncPgConnection,
    dag_name: &'static str,
    max_active_runs: u32,
) {
    let schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(std::time::Duration::from_secs(3600)),
        input: serde_json::Value::Null,
        catchup: false,
        max_active_runs,
        paused: false,
        queue_name: "default".to_string(),
        jitter: std::time::Duration::ZERO,
        overlap_policy: OverlapPolicy::BufferOne,
        buffer_all_max: 100,
        execution_timeout: None,
        chain_execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
        all_writable_shards: false,
    };
    register_workflow_schedules(conn, std::slice::from_ref(&schedule))
        .await
        .expect("register schedule");
}

async fn seed_buffered_run(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
    fire_at: chrono::DateTime<Utc>,
) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    diesel::update(dsl::harvest_schedules.filter(dsl::workflow_name.eq(dag_name)))
        .set(
            dsl::buffered_runs.eq(serde_json::Value::Array(vec![serde_json::Value::String(
                fire_at.to_rfc3339(),
            )])),
        )
        .execute(conn)
        .await
        .expect("seed buffered_runs");
}

async fn load_fired_deadline(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> (
    Uuid,
    Option<chrono::DateTime<Utc>>,
    Option<chrono::DateTime<Utc>>,
) {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::deadline_at,
            harvest_workflow_executions::sla_deadline_at,
        ))
        .first(conn)
        .await
        .optional()
        .expect("query fired execution")
        .expect("the tick must have fired an execution for this DAG")
}

// ── Finding #1: the manual/MCP trigger path must thread the DAG's declared
//    execution_timeout/sla instead of hardcoding StartWorkflowParams to None.

#[tokio::test]
async fn trigger_unified_dag_threads_dag_declared_execution_timeout_and_sla() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);

    let dag_name = leaked_dag_name("dag743_trigger");
    let dag = deadline_dag(
        dag_name,
        Some(std::time::Duration::from_secs(2 * 3600)),
        Some(std::time::Duration::from_secs(3600)),
    );
    let registry = dag_registry(&dag);

    let before = Utc::now();
    let started = trigger_unified_dag(
        pool,
        dag_name,
        None,
        ShardId::new(0),
        "default",
        None,
        None,
        None,
        &registry,
        StartSource::Api,
        None,
    )
    .await
    .expect("manual trigger must succeed");
    let after = Utc::now();

    let mut conn = connect(&url).await;
    let row = load_row(&mut conn, started.exec_id).await;
    let deadline_at = row.deadline_at.expect(
        "a manually-triggered DAG run must inherit its DAG-declared execution_timeout, \
         not hardcode None",
    );
    assert!(
        deadline_at >= before + ChronoDuration::hours(2)
            && deadline_at <= after + ChronoDuration::hours(2),
        "deadline_at must be started_at + 2h (the DagInfo's declared execution_timeout)"
    );
    let sla_deadline_at = row
        .sla_deadline_at
        .expect("a manually-triggered DAG run must inherit its DAG-declared sla");
    assert!(
        sla_deadline_at >= before + ChronoDuration::hours(1)
            && sla_deadline_at <= after + ChronoDuration::hours(1),
        "sla_deadline_at must be started_at + 1h (the DagInfo's declared sla)"
    );
}

// ── Finding #2: the scheduler's buffered-drain path (fired when
//    overlap_policy is buffer_one/buffer_all and a collision was queued)
//    must thread execution_timeout the same way the main tick path does.

#[tokio::test]
async fn scheduler_tick_buffered_drain_threads_dag_declared_execution_timeout() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let dag_name = leaked_dag_name("dag743_buffered");
    // `dag.schedule`/`dag.overlap_policy` must match the cadence and
    // overlap policy of the separately-registered `WorkflowSchedule` row
    // below (`Interval(3600s)` / `BufferOne`) -- `register_schedules_for_shard`'s
    // `upsert_schedule` (which runs on EVERY tick, before the buffered
    // drain) treats the DAG's own attributes as authoritative and
    // unconditionally overwrites `overlap_policy` from the stored row.
    // A mismatched `overlap_policy` (e.g. `deadline_dag`'s default `Skip`)
    // makes `is_buffering_policy = false`, which WIPES the just-seeded
    // `buffered_runs` back to `[]` regardless of whether the cadence
    // changed, before the drain ever queries it.
    let dag = DagInfo {
        schedule: Some(Schedule::Interval(std::time::Duration::from_secs(3600))),
        overlap_policy: OverlapPolicy::BufferOne,
        ..deadline_dag(
            dag_name,
            Some(std::time::Duration::from_secs(2 * 3600)),
            None,
        )
    };
    let registry = dag_registry(&dag);
    let catalog = compile_dag_catalog(vec![dag]).expect("compile dag catalog");

    insert_manual_workflow_schedule(&mut conn, dag_name, 1).await;
    // Directly seed a due buffered fire time -- bypassing the real
    // overlap-collision path (which requires a still-running prior execution)
    // since only the buffered-drain CODE PATH is under test here, not the
    // collision-detection logic that populates `buffered_runs` in production.
    seed_buffered_run(&mut conn, dag_name, Utc::now() - ChronoDuration::seconds(1)).await;

    let pool = build_test_pool(&url);
    let before = Utc::now();
    tick_once(
        pool,
        Arc::clone(&registry),
        Arc::new(catalog),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should succeed");

    let (_id, deadline_at, _sla_deadline_at) = load_fired_deadline(&mut conn, dag_name).await;
    let deadline_at = deadline_at.expect(
        "a buffered-drain-fired DAG run must inherit its DAG-declared execution_timeout, \
         not hardcode None",
    );
    assert!(
        deadline_at >= before + ChronoDuration::hours(2)
            && deadline_at <= before + ChronoDuration::hours(2) + ChronoDuration::seconds(10),
        "deadline_at must be ~started_at + 2h (the DagInfo's declared execution_timeout): got {deadline_at}"
    );
}

// ── Finding #3: the fleet-wide max_workflow_execution_timeout ceiling must
//    cap a scheduler-tick-fired DAG run identically to a manual/HTTP start.

#[tokio::test]
async fn scheduler_tick_applies_fleet_wide_execution_timeout_ceiling_to_dag() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let dag_name = leaked_dag_name("dag743_ceiling_tick");
    // Declares a 10h execution_timeout; the fleet-wide ceiling (1h) must win.
    // `dag.schedule` must match the separately-registered `WorkflowSchedule`
    // row's cadence below (also `Interval(3600s)`) -- see the identical note
    // in `scheduler_tick_buffered_drain_threads_dag_declared_execution_timeout`:
    // a mismatched cadence makes `register_schedules_for_shard`'s
    // `upsert_schedule` recompute (and NULL out) `next_run_at` from a `None`
    // schedule, so the forced-due `next_run_at` set below is wiped before
    // `tick_workflow_schedules` ever sees it as due.
    let dag = DagInfo {
        schedule: Some(Schedule::Interval(std::time::Duration::from_secs(3600))),
        ..deadline_dag(
            dag_name,
            Some(std::time::Duration::from_secs(10 * 3600)),
            None,
        )
    };
    let registry = Arc::new(
        HandlerRegistry::new(
            vec![
                dag.as_workflow_info()
                    .expect("workflow_handler is Some, so as_workflow_info must be Some"),
            ],
            vec![],
        )
        .with_max_workflow_execution_timeout(Some(std::time::Duration::from_secs(3600))),
    );
    let catalog = compile_dag_catalog(vec![dag]).expect("compile dag catalog");

    let schedule = WorkflowSchedule {
        workflow_name: dag_name.to_string(),
        dag_name: Some(dag_name.to_string()),
        schedule: Schedule::Interval(std::time::Duration::from_secs(3600)),
        input: serde_json::Value::Null,
        catchup: false,
        max_active_runs: 1,
        paused: false,
        queue_name: "default".to_string(),
        jitter: std::time::Duration::ZERO,
        overlap_policy: OverlapPolicy::Skip,
        buffer_all_max: 100,
        execution_timeout: None,
        chain_execution_timeout: None,
        calendar: None,
        skip_policy: autumn_harvest::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
        all_writable_shards: false,
    };
    register_workflow_schedules(&mut conn, std::slice::from_ref(&schedule))
        .await
        .expect("register schedule");
    // Force the schedule due.
    diesel::update(
        autumn_harvest::schema::harvest_schedules::table
            .filter(autumn_harvest::schema::harvest_schedules::workflow_name.eq(dag_name)),
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
        Arc::new(catalog),
        Arc::new(vec![schedule]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("scheduler tick should succeed");

    let (_id, deadline_at, _sla_deadline_at) = load_fired_deadline(&mut conn, dag_name).await;
    let deadline_at = deadline_at.expect("execution_timeout must be set (ceiling-derived)");
    assert!(
        deadline_at <= before + ChronoDuration::hours(1) + ChronoDuration::seconds(10)
            && deadline_at >= before + ChronoDuration::hours(1) - ChronoDuration::seconds(10),
        "the DAG's declared 10h timeout must be clamped to the 1h fleet-wide ceiling \
         for a scheduler-tick-fired run, exactly like a manual/HTTP start: got {deadline_at}"
    );
}
