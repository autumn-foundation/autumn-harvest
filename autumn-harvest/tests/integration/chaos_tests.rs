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
use std::time::Duration;

use autumn_harvest::chaos::points::{
    OUTBOX_INLINE_AFTER_REQUESTED, QUEUE_PARK_BEFORE_UPDATE, SCHED_AFTER_CLAIM,
    WORKER_PERSIST_BEFORE_COMMIT,
};
use autumn_harvest::chaos::{ChaosPlan, arm};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry, chaos_drive_one_workflow_task};
use autumn_harvest::{
    DagCatalog, ExecutionId, SchedulerMonitor, ShardId, StartWorkflowParams, WorkflowIdReusePolicy,
    tick_once,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
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

/// Sends one external signal to the `target` execution named in its input, then
/// completes — the caller that reaches `persist_external_signal_inline` and so
/// the `OUTBOX_INLINE_AFTER_REQUESTED` window (issue #492 / AC4). The target is
/// passed as an `ExecutionId` string in `input["target"]`.
#[workflow]
async fn chaos_signal_external(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let target: ExecutionId = input["target"]
        .as_str()
        .ok_or("missing target")?
        .parse()
        .map_err(|e: uuid::Error| e.to_string())?;
    ctx.signal_external_workflow(target, "go", serde_json::json!({ "from": "chaos" }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!("signaled"))
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

/// A pooled `DbPool` for the scheduler tick (which takes a pool by value).
fn make_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Insert a due workflow schedule (fires 5 s ago) and return its id. Mirrors the
/// `scheduler_ha_tests` helper so the fired execution counts are comparable.
async fn insert_due_schedule(conn: &mut AsyncPgConnection, wf_name: &str) -> uuid::Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let now = Utc::now();
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(dsl::harvest_schedules)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

/// Count executions of a workflow type.
async fn exec_count(conn: &mut AsyncPgConnection, wf_name: &str) -> i64 {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    dsl::harvest_workflow_executions
        .filter(dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(conn)
        .await
        .expect("count executions")
}

/// Read a schedule's `(fire_claim_token, live)` where `live` is true iff the
/// claim is held and unexpired.
async fn schedule_claim(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> (Option<uuid::Uuid>, bool) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let (token, until): (Option<uuid::Uuid>, Option<chrono::DateTime<Utc>>) = dsl::harvest_schedules
        .find(id)
        .select((dsl::fire_claim_token, dsl::fire_claimed_until))
        .first(conn)
        .await
        .expect("load schedule claim");
    let live = token.is_some() && until.is_some_and(|u| u > Utc::now());
    (token, live)
}

/// Count events of a given `event_type` on one execution.
async fn event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId, event_type: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(event_type)
    .get_result::<CountRow>(conn)
    .await
    .expect("count events")
    .n
}

/// Count queued signal rows for a target execution — the immediate delivery
/// artifact (`harvest_signals`) the inline path writes via `send_signal_*`.
async fn signals_for(conn: &mut AsyncPgConnection, target: ExecutionId) -> i64 {
    use autumn_harvest::schema::harvest_signals::dsl;
    dsl::harvest_signals
        .filter(dsl::workflow_exec_id.eq(target.as_uuid()))
        .count()
        .get_result(conn)
        .await
        .expect("count signals")
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

// ── Reproducer 3 — issue #492 outbox vs inline external-signal persist ───────

/// A same-shard external signal appends `ExternalSignalRequested` and its
/// `ExternalSignalDelivered` terminal inside ONE transaction
/// (`persist_external_signal_inline`, the #492 fix). HOLD at
/// `OUTBOX_INLINE_AFTER_REQUESTED` — the `Requested` event is written but the
/// txn is uncommitted — and run the background outbox on a separate connection:
/// under READ COMMITTED it cannot observe the half-written request, so it
/// delivers nothing (returns 0) and the signal lands exactly once. Exercises the
/// ERROR-free HOLD/DELAY window (AC1b/AC1d shape) and the #492 fix (AC4).
///
/// RED procedure: remove the `conn.transaction(...)` wrapper in
/// `persist_external_signal_inline` so `ExternalSignalRequested` commits in its
/// own statement before the terminal. During the hold the outbox then sees a
/// committed `Requested`-without-terminal on a `RUNNING` execution and delivers
/// it a SECOND time — two `harvest_signals` rows for the target and a second
/// `ExternalSignalDelivered` — and the `== 1` / `outbox == 0` asserts fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_repro_492_outbox_cannot_double_deliver_inline_external_signal() {
    let (url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;

    // Target (same shard 0) — a live RUNNING execution to receive the signal.
    // Parked on its own queue so the `default` claim below can only pick the
    // caller (the target's task is never polled here).
    let target_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut target_params =
        base_params("chaos_wait_signal", "c492-target", target_id, serde_json::json!(null));
    target_params.queue_name = "chaos-target-q";
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, target_params, None)
        .await
        .expect("start target");

    // Caller (same shard 0) that signals the target.
    let caller_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_input = serde_json::json!({ "target": target_id.to_string() });
    autumn_harvest::execution::start_or_load_workflow_execution(
        &mut conn,
        base_params("chaos_signal_external", "c492-caller", caller_id, caller_input),
        None,
    )
    .await
    .expect("start caller");

    let registry = Arc::new(HandlerRegistry::new(
        vec![chaos_signal_external_info(), chaos_wait_signal_info()],
        vec![],
    ));

    // Claim the caller's workflow task for the drive worker.
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "c492-caller-worker",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("caller task claimable");
    assert_eq!(
        task.workflow_exec_id,
        Some(caller_id.as_uuid()),
        "must claim the caller task (target parks on its signal)"
    );

    // HOLD inside the inline persist txn, after Requested is appended.
    let guard = arm(ChaosPlan::scripted().hold_at(OUTBOX_INLINE_AFTER_REQUESTED)).await;
    let hold = guard.hold(OUTBOX_INLINE_AFTER_REQUESTED);

    let drive_url = url.clone();
    let drive_registry = Arc::clone(&registry);
    let drive = tokio::spawn(async move {
        chaos_drive_one_workflow_task(
            &drive_url,
            drive_registry,
            task,
            "c492-caller-worker".to_string(),
        )
        .await
    });

    hold.reached().await;

    // The outbox on a separate connection cannot see the uncommitted Requested —
    // the #492 invariant: zero deliveries during the open inline txn.
    let delivered = autumn_harvest::timeout::enforce_external_signals_outbox(
        &mut conn,
        &NoOpMetrics,
        Duration::from_secs(300),
        &None,
        &[],
    )
    .await
    .expect("outbox sweep");
    assert_eq!(
        delivered, 0,
        "the outbox must not observe (or double-deliver) the half-written inline \
         signal; {}",
        guard.diagnostics()
    );
    assert_eq!(
        signals_for(&mut conn, target_id).await,
        0,
        "no signal may be queued to the target while the inline txn is open; {}",
        guard.diagnostics()
    );

    hold.release();
    let outcome = drive.await.expect("drive join").expect("drive spawn join");
    outcome.expect("caller cycle must persist cleanly");

    assert_eq!(
        guard.hits(OUTBOX_INLINE_AFTER_REQUESTED),
        1,
        "the inline persist point must be hit exactly once; {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the HOLD must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    drop(guard);

    // Exactly-once: one Requested + one Delivered on the caller, one queued
    // signal on the target. A double-delivery would show 2 signals / 2 Delivered.
    assert_eq!(
        event_count(&mut conn, caller_id, "ExternalSignalRequested").await,
        1,
        "exactly one ExternalSignalRequested"
    );
    assert_eq!(
        event_count(&mut conn, caller_id, "ExternalSignalDelivered").await,
        1,
        "exactly one ExternalSignalDelivered"
    );
    assert_eq!(
        signals_for(&mut conn, target_id).await,
        1,
        "the signal must land on the target exactly once"
    );
    assert_eq!(
        dangling_external_requests(&mut conn).await,
        0,
        "no ExternalSignalRequested without a terminal"
    );
}

// ── Reproducer 4 — issue #350 schedule fire claim expiring mid-fire ──────────

/// A scheduler replica that crashes after winning the HA claim but before firing
/// must leave the slot fire-able by a healthy peer *exactly once*, and no peer
/// may double-fire while the claim is still live. KILL at `SCHED_AFTER_CLAIM`
/// (the claim UPDATE has committed in autocommit, the fire has not) via a spawned
/// `tick_once` → `JoinError`; the live claim then blocks a peer tick, and only
/// after the 30 s claim TTL expires does a peer fire the slot once (AC4).
///
/// RED procedure: remove the HA claim guard in
/// `claim_and_fire_workflow_schedule` (fire unconditionally, ignore
/// `fire_claim_token`/`fire_claimed_until`). The live-claim peer tick then fires
/// a SECOND time while the first fire is still in flight — the `exec_count == 0`
/// (live-claim) or final `== 1` (single fire) assertion fails with 2 executions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_repro_350_crashed_fire_claim_is_refired_exactly_once() {
    let (url, _c) = chaos_db().await;
    let wf = "chaos_sched_350";
    let sched_id = {
        let mut conn = connect(&url).await;
        insert_due_schedule(&mut conn, wf).await
    };
    let registry = Arc::new(HandlerRegistry::new(vec![chaos_noop_info()], vec![]));

    // Crash mid-fire: KILL after the claim commits, before the fire. The tick is
    // spawned so the panic surfaces as a JoinError instead of aborting the test.
    let guard = arm(ChaosPlan::scripted().kill_at(SCHED_AFTER_CLAIM)).await;
    let crash = tokio::spawn(tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    ))
    .await;
    assert!(
        crash.is_err(),
        "the KILL must crash the tick mid-fire (JoinError); {}",
        guard.diagnostics()
    );
    assert!(
        guard.actions_fired() >= 1,
        "the KILL must have fired (anti-vacuity); {}",
        guard.diagnostics()
    );
    drop(guard);

    // The crash left the claim held (committed in autocommit) with no fire.
    {
        let mut conn = connect(&url).await;
        let (token, live) = schedule_claim(&mut conn, sched_id).await;
        assert!(token.is_some(), "the HA claim must be committed by the crash");
        assert!(live, "the claim TTL must still be live right after the crash");
        assert_eq!(
            exec_count(&mut conn, wf).await,
            0,
            "the crash must have fired nothing"
        );
    }

    // A healthy peer tick while the claim is live must NOT double-fire.
    tick_once(
        make_pool(&url),
        Arc::clone(&registry),
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (live claim)");
    {
        let mut conn = connect(&url).await;
        assert_eq!(
            exec_count(&mut conn, wf).await,
            0,
            "a live claim must block the peer from firing"
        );
    }

    // Expire the claim (the 30 s TTL elapses) → a peer re-fires the slot ONCE.
    {
        let mut conn = connect(&url).await;
        diesel::sql_query(
            "UPDATE harvest_schedules SET fire_claimed_until = NOW() - INTERVAL '1 minute' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(sched_id)
        .execute(&mut conn)
        .await
        .expect("expire claim");
    }
    tick_once(
        make_pool(&url),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("peer tick (expired claim)");

    let mut conn = connect(&url).await;
    assert_eq!(
        exec_count(&mut conn, wf).await,
        1,
        "the crashed slot must be re-fired by a peer exactly once after the claim expires"
    );
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
/// heartbeat — the stranded-orphan invariant probe. Mirrors the poison-pill
/// reclaimer's own liveness definition (`orphaned_running_tasks_query`): a
/// worker is dead when no row carries its `worker_id` with a fresh heartbeat.
async fn stranded_running_with_dead_worker(conn: &mut AsyncPgConnection) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue t \
         WHERE t.state = 'RUNNING' AND t.worker_id IS NOT NULL \
           AND NOT EXISTS ( \
             SELECT 1 FROM harvest_workers w \
             WHERE w.worker_id = t.worker_id \
               AND w.last_heartbeat_at > NOW() - INTERVAL '30 seconds' \
           )",
    )
    .get_result::<CountRow>(conn)
    .await
    .expect("count stranded")
    .n
}
