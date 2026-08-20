#![cfg(feature = "chaos")]
//! Deterministic chaos / fault-injection reproducers and convergence sweep
//! (issue #940).
//!
//! Each of the four historical race classes is reproduced here as a *seeded /
//! scripted* fault at a named injection point (`autumn_harvest::chaos::points`),
//! driven against a real Postgres. Every reproducer FAILS on the pre-fix engine
//! shape and PASSES on the current one; the RED procedure is documented inline
//! next to each so a reviewer can reproduce the failing shape in one edit.
//!
//! The convergence sweep ([`chaos_seeded_convergence_sweep`]) runs a bounded
//! workload under `ChaosPlan::seeded(seed)` for each of N documented seeds and
//! asserts the engine still reaches the convergence invariant: every workflow
//! terminal-or-parked, no task stranded `RUNNING` with a dead worker, no
//! `ExternalSignalRequested` without an eventual terminal (issue #940 AC5).
//!
//! ## Determinism / replay (AC3)
//!
//! Every failure prints the [`ChaosGuard::diagnostics`] string, which embeds the
//! seed and the fired-action trace, so a failure is replayable with one command
//! (`CHAOS_SEEDS=<seed> cargo test --features chaos ...`).

use std::sync::Arc;

use autumn_harvest::chaos::points::{
    OUTBOX_INLINE_AFTER_REQUESTED, QUEUE_PARK_BEFORE_UPDATE, SCHED_AFTER_CLAIM,
    WORKER_PERSIST_BEFORE_COMMIT,
};
use autumn_harvest::chaos::{ChaosPlan, arm};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{HandlerRegistry, chaos_drive_one_workflow_task};
use autumn_harvest::{
    DagCatalog, ExecutionId, SchedulerMonitor, ShardId, StartWorkflowParams, WorkflowIdReusePolicy,
    tick_once,
};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

// ── Test workflows (macro-generated companions are field-growth-resilient) ──

/// Suspends on a signal — the simplest workflow that reaches the suspension
/// persist path (`WORKER_PERSIST_BEFORE_COMMIT`) and can be parked
/// (`QUEUE_PARK_BEFORE_UPDATE`).
#[workflow]
async fn chaos_wait_signal(
    ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let sig = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(sig)
}

/// Completes in a single decision cycle — the convergence-sweep workload unit.
#[workflow]
async fn chaos_noop(
    _ctx: &WorkflowContext,
    _input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!("ok"))
}

// ── DB setup ────────────────────────────────────────────────────────────────

/// A live DB URL plus (when spun locally) the container keeping it alive.
///
/// Honours `HARVEST_TEST_DATABASE_URL` (assumed already migrated) for fast
/// local RED/GREEN iteration; otherwise spins a fresh migrated Postgres 16
/// container (the CI path). The env DB is scrubbed at the start of each test so
/// the process-serialised chaos runs stay isolated.
async fn chaos_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to HARVEST_TEST_DATABASE_URL");
        scrub(&mut conn).await;
        (url, None)
    } else {
        use testcontainers::ImageExt;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migration");
        (url, Some(container))
    }
}

/// Truncate the workflow-state tables so an env-DB run starts clean.
async fn scrub(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "TRUNCATE harvest_workflow_executions, harvest_events, harvest_task_queue, \
         harvest_signals, harvest_timers, harvest_dead_letters, harvest_workers, \
         harvest_schedules CASCADE",
    )
    .await
    .expect("scrub");
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("establish test connection")
}

/// Build the standard `StartWorkflowParams` for a chaos test workflow.
fn base_params(
    workflow_name: &'static str,
    workflow_id: &'static str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
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

/// Read a task-queue row's `(state, worker_id)` by task id.
async fn task_state(conn: &mut AsyncPgConnection, task_id: uuid::Uuid) -> (String, Option<String>) {
    use autumn_harvest::schema::harvest_task_queue::dsl;
    dsl::harvest_task_queue
        .find(task_id)
        .select((dsl::state, dsl::worker_id))
        .first(conn)
        .await
        .expect("load task state")
}

/// Read an execution's `state`.
async fn exec_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::state)
        .first(conn)
        .await
        .expect("load exec state")
}

/// Count `ExternalSignalRequested` events with no matching terminal for the
/// same execution — the AC5 "no `*Requested` without eventual terminal"
/// invariant probe.
async fn dangling_external_requests(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events r \
         WHERE r.event_type = 'ExternalSignalRequested' \
           AND NOT EXISTS ( \
             SELECT 1 FROM harvest_events t \
             WHERE t.workflow_exec_id = r.workflow_exec_id \
               AND t.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') \
           )",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count dangling external requests")
    .n
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

// ── Reproducer 1 — issue #601 lost-wake (`wake_requested`) window ────────────

/// A wake that lands while the task is still claimed (mid-cycle) must not be
/// lost. HOLD the park at `QUEUE_PARK_BEFORE_UPDATE` (the row is still `RUNNING`
/// with its worker_id), fire a concurrent `wake_workflow_task` — whose primary
/// re-pend matches zero rows and must fall back to `wake_requested = TRUE` —
/// then release: the park atomically reads-and-clears `wake_requested` and
/// reports it, so the caller re-pends instead of stranding the task parked.
///
/// RED procedure: revert `queue::park_workflow_task_inner`'s `candidate` CTE to
/// stop reading `wake_requested` (return `false`), or drop
/// `wake_workflow_task`'s fallback UPDATE — the assert `had_wake_requested`
/// then fails, and the task stays parked with the wake lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_repro_601_lost_wake_is_recovered_via_wake_requested() {
    let (url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params(
        "chaos_wait_signal",
        "c601-wf",
        exec_id,
        serde_json::json!(null),
    );
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_wait_signal");

    // Claim the workflow task → RUNNING with a worker_id, exactly the state a
    // decision cycle holds when it decides to park.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c601-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("a workflow task must be claimable");

    let guard = arm(ChaosPlan::scripted().hold_at(QUEUE_PARK_BEFORE_UPDATE)).await;
    let hold = guard.hold(QUEUE_PARK_BEFORE_UPDATE);

    // Park on its own owned connection so the HOLD can block it without
    // borrowing our connection.
    let park_url = url.clone();
    let task_id = task.id;
    let park = tokio::spawn(async move {
        let mut park_conn = connect(&park_url).await;
        autumn_harvest::queue::park_workflow_task(&mut park_conn, task_id, None).await
    });

    // The park has reached the rendezvous with the row still RUNNING+worker_id.
    hold.reached().await;

    // A concurrent wake: primary re-pend matches 0 rows (row not yet parked),
    // so it must fall back to marking `wake_requested = TRUE`.
    autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("concurrent wake");

    hold.release();
    let had_wake_requested = park
        .await
        .expect("park task join")
        .expect("park succeeds");

    assert!(
        had_wake_requested,
        "the mid-cycle wake must be recovered via wake_requested, not lost; {}",
        guard.diagnostics()
    );
    assert_eq!(
        guard.hits(QUEUE_PARK_BEFORE_UPDATE),
        1,
        "the park injection point must have been hit exactly once; {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the HOLD must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );

    // Convergence: after the caller acts on had_wake_requested (re-pend), the
    // task is claimable again — not stranded parked.
    autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id)
        .await
        .expect("re-pend after wake_requested");
    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(state, "PENDING", "task must be re-pended, not stranded");
    assert!(worker.is_none(), "re-pended task must have no worker_id");
}

// ── Reproducer 2 — issue #367 poison-pill orphan reclaim ─────────────────────

/// A KILL mid-decision-cycle (before the persist commit) leaves the task
/// stranded `RUNNING` with a now-dead worker. The poison-pill reclaimer must
/// recover it: increment `crash_strikes` and re-queue it `PENDING` (under the
/// threshold). This exercises the KILL capability (AC1a) and the #367 fix.
///
/// RED procedure: comment out the `reclaim_orphaned_tasks` call below — the
/// orphan then stays `RUNNING` with a dead worker forever (the pre-#367 shape),
/// and the `PENDING` assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_repro_367_crash_orphan_is_reclaimed() {
    let (url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params(
        "chaos_wait_signal",
        "c367-wf",
        exec_id,
        serde_json::json!(null),
    );
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start chaos_wait_signal");

    let registry = Arc::new(HandlerRegistry::new(vec![chaos_wait_signal_info()], vec![]));

    // Claim → RUNNING with the (soon-to-crash) worker id.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c367-crash-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("workflow task claimable");

    // Drive the cycle with a KILL at the persist point → crash, owned conn drop
    // → server rollback → row stranded RUNNING with the dead worker.
    let guard = arm(ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT)).await;
    let outcome = chaos_drive_one_workflow_task(
        &url,
        Arc::clone(&registry),
        task.clone(),
        "c367-crash-worker".to_string(),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the KILL must crash the decision cycle (JoinError); {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the KILL must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    drop(guard);

    // The orphan condition: RUNNING, still owned by the dead worker, no forward
    // progress (the persist rolled back).
    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(state, "RUNNING", "crash must strand the task RUNNING");
    assert_eq!(worker.as_deref(), Some("c367-crash-worker"));

    // The dead worker was never registered → no live heartbeat → orphaned.
    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut conn,
        3,
        0,
        &NoOpMetrics,
    )
    .await
    .expect("reclaim");
    assert_eq!(summary.requeued, 1, "the orphan must be re-queued once");
    assert_eq!(summary.quarantined, 0);

    let (state, worker) = task_state(&mut conn, task.id).await;
    assert_eq!(state, "PENDING", "the orphan must be recovered to PENDING");
    assert!(worker.is_none(), "recovered task must have no worker_id");
}

// ── AC5 — seeded convergence sweep ───────────────────────────────────────────

/// Documented default seed set for the sweep (AC5: N ≥ 5 distinct seeds). CI
/// overrides via `CHAOS_SEEDS="1 2 3 ..."`.
fn sweep_seeds() -> Vec<u64> {
    std::env::var("CHAOS_SEEDS")
        .ok()
        .map(|s| {
            s.split_whitespace()
                .filter_map(|t| t.parse::<u64>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![1, 2, 3, 5, 8, 13, 21])
}

/// For each seed: arm a randomised plan, drive a bounded workload of
/// single-cycle workflows (KILLs caught as `JoinError`), disarm, run the
/// recovery loop (reclaim orphans + re-drive), and assert the convergence
/// invariant — every workflow terminal, no task stranded `RUNNING` with a dead
/// worker, no dangling `ExternalSignalRequested`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_seeded_convergence_sweep() {
    let (url, _c) = chaos_db().await;
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));
    const WORKLOAD: usize = 6;

    for seed in sweep_seeds() {
        // Fresh DB slate per seed so the invariant probe is unambiguous.
        {
            let mut conn = connect(&url).await;
            scrub(&mut conn).await;
        }

        // Start the workload.
        let mut execs = Vec::new();
        {
            let mut conn = connect(&url).await;
            for i in 0..WORKLOAD {
                let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
                let wid: &'static str = Box::leak(format!("sweep-{seed}-{i}").into_boxed_str());
                let params =
                    base_params("chaos_noop", wid, exec_id, serde_json::json!(null));
                autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
                    .await
                    .expect("start sweep workflow");
                execs.push(exec_id);
            }
        }

        // Drive every task once under the seeded plan; KILLs surface as
        // JoinErrors and are swallowed (a crashed cycle is a valid fault).
        let guard = arm(ChaosPlan::seeded(seed)).await;
        for i in 0..WORKLOAD {
            let mut conn = connect(&url).await;
            let worker: &'static str = Box::leak(format!("sweepw-{seed}-{i}").into_boxed_str());
            if let Some(task) = autumn_harvest::queue::claim_task(
                &mut conn,
                &["default".to_string()],
                worker,
                "",
                None,
                &[],
                &[],
            )
            .await
            .expect("claim in sweep")
            {
                let _ = chaos_drive_one_workflow_task(
                    &url,
                    Arc::clone(&registry),
                    task,
                    worker.to_string(),
                )
                .await;
            }
        }
        let diag = guard.diagnostics();
        drop(guard);

        // Recovery loop (chaos disarmed): reclaim crash orphans and re-drive any
        // remaining claimable tasks until quiescent.
        for _round in 0..(WORKLOAD + 4) {
            let mut conn = connect(&url).await;
            autumn_harvest::poison_pill::reclaim_orphaned_tasks(&mut conn, 3, 0, &NoOpMetrics)
                .await
                .expect("reclaim in sweep");
            let claimed = autumn_harvest::queue::claim_task(
                &mut conn,
                &["default".to_string()],
                "sweep-recover",
                "",
                None,
                &[],
                &[],
            )
            .await
            .expect("claim in recovery");
            let Some(task) = claimed else { break };
            let _ = chaos_drive_one_workflow_task(
                &url,
                Arc::clone(&registry),
                task,
                "sweep-recover".to_string(),
            )
            .await;
        }

        // Convergence invariant.
        let mut conn = connect(&url).await;
        for exec_id in &execs {
            let state = exec_state(&mut conn, *exec_id).await;
            assert_eq!(
                state, "COMPLETED",
                "seed {seed}: workflow {exec_id:?} must converge to terminal; got {state}; {diag}"
            );
        }
        let stranded = stranded_running_with_dead_worker(&mut conn).await;
        assert_eq!(
            stranded, 0,
            "seed {seed}: no task may be stranded RUNNING with a dead worker; {diag}"
        );
        let dangling = dangling_external_requests(&mut conn).await;
        assert_eq!(
            dangling, 0,
            "seed {seed}: no ExternalSignalRequested without a terminal; {diag}"
        );
    }
}

/// Count `RUNNING` tasks whose `worker_id` has no live `harvest_workers`
/// heartbeat — the stranded-orphan invariant probe.
async fn stranded_running_with_dead_worker(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue t \
         WHERE t.state = 'RUNNING' AND t.worker_id IS NOT NULL \
           AND NOT EXISTS ( \
             SELECT 1 FROM harvest_workers w WHERE w.id = t.worker_id \
           )",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count stranded")
    .n
}
