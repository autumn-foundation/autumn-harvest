#![cfg(feature = "db")]
//! Worker-driven integration tests for cross-type continue-as-new — issue #803.
//!
//! `ctx.continue_as_new_as_type(target, input)` continues an entity run as a
//! *different* registered workflow type while keeping the same logical
//! `workflow_id`, so each lifecycle phase gets its own focused, replay-safe
//! handler instead of one ever-branching function.
//!
//! Coverage map (acceptance criteria from the issue):
//!
//! - **AC2** — the successor's `workflow_name` is the named type; the
//!   `workflow_id`, shard and queue are unchanged.
//! - **AC3** — the transition is recorded on the existing
//!   `WorkflowContinuedAsNew` event via the additive `new_workflow_type` field.
//! - **AC4** — the successor's `execution_timeout` / `sla` / concurrency /
//!   ops-metadata / retry policy come from the **new type's** `WorkflowInfo`,
//!   while the #617 chain cap is still carried verbatim (a type change must not
//!   be an escape hatch from a runaway-loop budget).
//! - **AC5** — continuing into an unregistered type fails the predecessor
//!   terminally and creates no successor.
//! - **AC7** — carryover is type-agnostic: unconsumed signals reassign to the
//!   cross-type successor and `last_completion_result` carries forward.
//! - **AC8** — the root-only guard is unchanged: a child cannot cross-type
//!   continue either.
//! - **Success metric** — after a transition, `signal_with_start` naming the
//!   *new* type attaches to the live successor rather than starting a duplicate.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it, otherwise a testcontainers instance is booted.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{
    SignalWithStartOutcome, SignalWithStartParams, StartWorkflowParams,
    signal_with_start_workflow_execution, start_or_load_workflow_execution,
};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::{harvest_signals, harvest_workflow_executions};
use autumn_harvest::types::{
    ExecutionId, Priority, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{WorkflowContext, WorkflowInfo};
use chrono::{Duration as ChronoDuration, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Leak a unique `&'static str` so each test owns an isolated workflow-type
/// namespace — the worker registry and several scanners are global.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// Phase 1: graduate into whatever type the input names, under the same
/// `workflow_id`. Parameterizing the target through the *input* (recorded
/// history) keeps the handler deterministic.
fn phase_one<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = input
            .get("next_type")
            .and_then(serde_json::Value::as_str)
            .expect("phase_one input must carry next_type")
            .to_string();
        ctx.continue_as_new_as_type(&target, serde_json::json!({"phase": "two"}))
            .await
            .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new_as_type must not resolve");
    })
}

/// Phase 2: a distinct handler that simply records which phase ran.
fn phase_two<'a>(
    _ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"ran": "phase_two", "input": input})) })
}

/// Phase 2 that parks on a signal, so a test can prove an external
/// `signal_with_start` attaches to the live successor.
fn phase_two_awaits_signal<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = ctx
            .wait_for_signal("upgrade")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"ran": "phase_two", "signal": payload}))
    })
}

fn wf(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "cross_type_continue_as_new_tests",
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
    }
}

fn registry(infos: Vec<WorkflowInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(infos, vec![]))
}

/// Start a root execution of `workflow_name`/`workflow_id` through the real
/// start path (so the row and its queue task are shaped exactly as production).
async fn start_root(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let params = StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id: ExecutionId::new(),
        input,
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
    };
    start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start root execution")
        .exec_id
}

/// Address an entity by `(workflow_name, workflow_id)` and deliver a signal,
/// starting a fresh run only if that pair has no live execution — the exact
/// call an external caller makes after a phase transition.
async fn signal_with_start(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    input: serde_json::Value,
    signal_payload: serde_json::Value,
) -> SignalWithStartOutcome {
    let params = SignalWithStartParams {
        workflow_name,
        workflow_id,
        exec_id: ExecutionId::new(),
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        signal_name: "upgrade",
        signal_payload,
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
    };
    signal_with_start_workflow_execution(conn, params)
        .await
        .expect("signal-with-start")
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

/// Pull the successor id (and the recorded target type) out of a sealed
/// predecessor's history.
async fn recorded_transition(url: &str, predecessor: ExecutionId) -> (ExecutionId, Option<String>) {
    load_history_from_url(url, predecessor)
        .await
        .events
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id,
                new_workflow_type,
                ..
            } => Some((*new_exec_id, new_workflow_type.clone())),
            _ => None,
        })
        .expect("predecessor history must contain WorkflowContinuedAsNew")
}

/// Open an explicit transaction on `conn` holding `exec_id`'s execution row,
/// so a concurrent writer of that row parks until `COMMIT` — the repo's
/// established choreography for reproducing a commit-window race
/// deterministically (see `pause_tests`).
///
/// `FOR NO KEY UPDATE`, not `FOR UPDATE`: the worker's seal is an ordinary
/// non-key `UPDATE` (which itself takes `FOR NO KEY UPDATE`) so it still
/// blocks, while a foreign-key check from an insert into `harvest_signals`
/// only needs `FOR KEY SHARE` — which `FOR UPDATE` would block and deadlock
/// this test against its own held lock.
/// Returns the holder's backend pid so [`wait_for_a_blocked_writer`] can
/// require that *this* connection is the blocker.
async fn hold_execution_row_lock(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }
    conn.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR NO KEY UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut *conn)
        .await
        .expect("lock execution row");
    let pid: Pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result(conn)
        .await
        .expect("read holder pid");
    pid.pid
}

/// Block until a backend is genuinely waiting on **`holder`'s** lock — the
/// precise signal that the worker's persist transaction has reached its seal
/// UPDATE (a fixed sleep would be a race in either direction).
///
/// Scoped to the holder's pid *and* the current database on purpose: an
/// unscoped `cardinality(pg_blocking_pids(pid)) > 0` is satisfied by **any**
/// blocked backend anywhere on the cluster — including a concurrent suite on
/// the shared test Postgres — which would release the lock before the worker
/// ever reached the seal and let the test pass vacuously.
async fn wait_for_a_blocked_writer(conn: &mut AsyncPgConnection, holder: i32) {
    #[derive(diesel::QueryableByName)]
    struct Blocked {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    for _ in 0..600 {
        let blocked: Blocked = diesel::sql_query(
            "SELECT count(*) AS n FROM pg_stat_activity \
             WHERE datname = current_database() AND $1 = ANY(pg_blocking_pids(pid))",
        )
        .bind::<diesel::sql_types::Integer, _>(holder)
        .get_result(conn)
        .await
        .expect("probe pg_stat_activity");
        if blocked.n > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the worker's persist transaction never blocked on the held row lock");
}

/// Run a worker until `predecessor` seals, then return the successor.
async fn drive_transition(
    url: &str,
    predecessor: ExecutionId,
    reg: Arc<HandlerRegistry>,
    worker_id: &str,
) -> (ExecutionId, Option<String>) {
    let worker = build_runtime_worker(worker_id, 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    let _sealed = wait_for_execution_state(url, predecessor, "CONTINUED_AS_NEW").await;
    let transition = recorded_transition(url, predecessor).await;
    worker.shutdown();
    handle.await.expect("worker join");
    transition
}

// ---------------------------------------------------------------------------
// AC2 / AC3 — the headline: the successor runs as the NAMED type, same identity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successor_runs_as_the_named_type_keeping_workflow_id_shard_and_queue() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;
    let before = load_execution(&mut conn, predecessor).await;

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let (successor, recorded_type) =
        drive_transition(&url, predecessor, reg, "w-803-identity").await;

    // AC3: the transition is recorded on the existing event.
    assert_eq!(
        recorded_type.as_deref(),
        Some(phase2),
        "the recorded WorkflowContinuedAsNew must name the target type"
    );

    // AC2: the successor IS the new type, under the SAME logical identity.
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.workflow_name, phase2,
        "successor runs as the new type"
    );
    assert_eq!(
        after.workflow_id, before.workflow_id,
        "the stable workflow_id must survive the transition"
    );
    assert_eq!(after.shard_id, before.shard_id, "same shard");
    assert_eq!(after.queue_name, before.queue_name, "same queue");
    assert_ne!(successor, predecessor, "a fresh execution id");

    // AC2: "its input is the provided payload" — `phase_one` hands the
    // transition `{"phase": "two"}`, which must be the successor's input on
    // both the row and its own `WorkflowStarted`.
    let expected_input = serde_json::json!({"phase": "two"});
    assert_eq!(
        after.input, expected_input,
        "the successor's input must be the payload passed to the transition"
    );

    // The successor's history starts clean — that is the point of the feature.
    let succ_history = load_history_from_url(&url, successor).await;
    match succ_history.events.as_slice().first() {
        Some(WorkflowEvent::WorkflowStarted { input, .. }) => assert_eq!(
            input, &expected_input,
            "the successor's WorkflowStarted must carry the provided payload"
        ),
        other => panic!("successor history must begin fresh with WorkflowStarted, got {other:?}"),
    }

    // The predecessor is sealed and released from the active-uniqueness slot.
    assert_eq!(
        load_execution(&mut conn, predecessor).await.state,
        "CONTINUED_AS_NEW"
    );
}

// ---------------------------------------------------------------------------
// AC4 — lifecycle defaults come from the NEW type; the #617 chain cap does not
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successor_resolves_lifecycle_defaults_from_the_new_type() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    // Give the PREDECESSOR row lifecycle values that differ from the target
    // type's declared defaults, plus a chain cap to pin the #617 interaction.
    let frozen_chain_deadline = Utc::now() + ChronoDuration::hours(6);
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set((
            harvest_workflow_executions::execution_timeout.eq(Some(ChronoDuration::seconds(600))),
            harvest_workflow_executions::sla.eq(Some(ChronoDuration::seconds(300))),
            harvest_workflow_executions::owner.eq(Some("growth-team")),
            harvest_workflow_executions::severity.eq(Some("SEV3")),
            harvest_workflow_executions::chain_execution_timeout.eq(Some(ChronoDuration::days(7))),
            harvest_workflow_executions::chain_deadline_at.eq(Some(frozen_chain_deadline)),
        ))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor lifecycle columns");

    // The TARGET type declares its own, different defaults.
    let mut target = wf(phase2, phase_two);
    target.execution_timeout = Some(std::time::Duration::from_secs(7_200));
    target.sla = Some(std::time::Duration::from_secs(1_800));
    target.owner = Some("billing-team");
    target.severity = Some("SEV1");

    let reg = registry(vec![wf(phase1, phase_one), target]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-803-defaults").await;
    let after = load_execution(&mut conn, successor).await;

    assert_eq!(
        after.execution_timeout,
        Some(ChronoDuration::seconds(7_200)),
        "execution_timeout must come from the NEW type (#243), not the predecessor's 600s"
    );
    assert_eq!(
        after.sla,
        Some(ChronoDuration::seconds(1_800)),
        "sla must come from the NEW type (#487), not the predecessor's 300s"
    );
    assert_eq!(
        after.owner.as_deref(),
        Some("billing-team"),
        "ops metadata follows the phase (#372) so alerts page the right team"
    );
    assert_eq!(after.severity.as_deref(), Some("SEV1"));

    // Per-run deadline re-anchored from the NEW type's budget.
    let deadline = after.deadline_at.expect("successor deadline_at");
    assert!(
        deadline > Utc::now() + ChronoDuration::seconds(7_000),
        "deadline must re-anchor to now + the new type's 7200s budget, got {deadline}"
    );

    // …but the CHAIN cap is carried verbatim: changing type is NOT an escape
    // hatch from the #617 runaway-loop budget.
    assert_eq!(
        after.chain_execution_timeout,
        Some(ChronoDuration::days(7)),
        "chain cap duration carried verbatim across a type change"
    );
    let chain_deadline = after
        .chain_deadline_at
        .expect("successor chain_deadline_at");
    assert!(
        (chain_deadline - frozen_chain_deadline)
            .num_milliseconds()
            .abs()
            < 1_000,
        "chain_deadline_at must be the predecessor's ABSOLUTE value ({frozen_chain_deadline}), \
         not re-anchored — got {chain_deadline}"
    );
}

/// A target type declaring no lifecycle defaults must CLEAR them rather than
/// silently inheriting the predecessor's — otherwise the successor runs under a
/// timeout its own type never declared.
#[tokio::test]
async fn successor_without_declared_defaults_does_not_inherit_the_predecessors() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("churned");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set((
            harvest_workflow_executions::execution_timeout.eq(Some(ChronoDuration::seconds(600))),
            harvest_workflow_executions::owner.eq(Some("growth-team")),
        ))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor");

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-803-cleared").await;
    let after = load_execution(&mut conn, successor).await;

    assert!(
        after.execution_timeout.is_none(),
        "a target declaring no timeout must not inherit the predecessor's"
    );
    assert!(after.deadline_at.is_none());
    assert!(
        after.owner.is_none(),
        "a target declaring no owner must not inherit the predecessor's"
    );
}

// ---------------------------------------------------------------------------
// AC2 — the successor's uniqueness slot
//
// A cross-type successor takes a DIFFERENT `(workflow_name, workflow_id)` slot
// than the predecessor, and sealing the predecessor does not free it. The two
// cases that slot can be in are pinned here end-to-end.
// ---------------------------------------------------------------------------

/// Park a prior run of the target type on the same `workflow_id` in `state`,
/// with no task-queue row, so it is a stable slot occupant the worker will
/// never claim and never transition.
async fn park_occupant(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let occupant = start_root(conn, workflow_name, workflow_id, serde_json::json!({})).await;
    diesel::sql_query("DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(occupant.as_uuid())
        .execute(conn)
        .await
        .expect("detach the occupant from the queue");
    let completed_at = autumn_harvest::erase::is_terminal_state(state).then(Utc::now);
    diesel::update(harvest_workflow_executions::table.find(occupant.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::completed_at.eq(completed_at),
        ))
        .execute(conn)
        .await
        .expect("park the occupant");
    occupant
}

/// A terminal prior run of the target phase (the win-back shape
/// `churned -> trial -> churned`) still *occupies* the successor's uniqueness
/// slot — the partial index excludes only `CONTINUED_AS_NEW` / `TERMINATED` —
/// and the transition is REJECTED rather than releasing it.
///
/// An earlier cut released the slot by re-stating that run `CONTINUED_AS_NEW`.
/// Two Codex P1s on PR #1159 showed that is unsafe: `CONTINUED_AS_NEW` is read
/// as a *link*, so `await_external_workflow` (#757) on the old id parks forever
/// and `/result` (#527) stops reporting its outcome; and a schedule-attributed
/// run dropped out of #488 carryover would roll an incremental cursor backward
/// (a rule `execution.rs`'s re-run path already spells out). A terminal run is
/// a durable record callers may hold an id for, so this path does not rewrite
/// it — the operator resets/erases it, or continues under a different
/// `workflow_id`.
#[tokio::test]
async fn a_terminal_prior_run_of_the_target_type_blocks_the_transition() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("churned");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    // The entity churned once before, was won back, and is now churning again.
    let occupant = park_occupant(&mut conn, phase2, &workflow_id, "COMPLETED").await;
    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let worker = build_runtime_worker("w-803-slot-terminal", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let failed = wait_for_execution_state(&url, predecessor, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let error = failed
        .error
        .expect("a terminal failure must carry an error");
    assert!(
        error.contains(&occupant.to_string()) && error.contains(phase2),
        "the failure must name the blocking execution and the target type, got {error}"
    );

    // No continue-as-new recorded.
    let history = load_history_from_url(&url, predecessor).await;
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "a blocked transition may not record a continue-as-new"
    );

    // The occupant's recorded outcome is UNTOUCHED — the whole point.
    let untouched = load_execution(&mut conn, occupant).await;
    assert_eq!(
        untouched.state, "COMPLETED",
        "a terminal run's recorded outcome must never be rewritten to make room"
    );

    // And no successor was created.
    let rows: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
        .filter(harvest_workflow_executions::workflow_name.eq(phase2))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count rows of the target type");
    assert_eq!(
        rows, 1,
        "only the pre-existing occupant may exist; no successor was created"
    );
}

/// Naming your OWN type is a supported request — unlike the legacy
/// `continue_as_new`, it re-resolves *that type's* declared defaults, and it is
/// what the natural typed form `continue_as_new_as(&own_info(), input)`
/// produces. The predecessor occupies the very slot it is about to vacate, so
/// the occupant check must not read it as blocking itself.
#[tokio::test]
async fn naming_the_current_type_continues_normally_instead_of_self_blocking() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase = leaked("trial_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    // `phase_one` continues into whatever `next_type` names — here, itself.
    let predecessor = start_root(
        &mut conn,
        phase,
        &workflow_id,
        serde_json::json!({"next_type": phase}),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set(harvest_workflow_executions::execution_timeout.eq(Some(ChronoDuration::seconds(600))))
        .execute(&mut conn)
        .await
        .expect("stamp a per-start override the type never declared");

    // Register the type WITHOUT the successor handler looping forever: the
    // successor's input is `{"phase": "two"}`, which `phase_one` would reject,
    // so drive only the transition and stop.
    let reg = registry(vec![wf(phase, phase_one)]);
    let (successor, recorded_type) = drive_transition(&url, predecessor, reg, "w-803-self").await;

    assert_eq!(
        recorded_type.as_deref(),
        Some(phase),
        "naming the current type must still be recorded — it is not normalized away"
    );
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(after.workflow_name, phase);
    assert_eq!(after.workflow_id, workflow_id);
    assert!(
        matches!(after.state.as_str(), "RUNNING" | "FAILED" | "COMPLETED"),
        "the successor must exist and be dispatchable, got {}",
        after.state
    );
    assert!(
        after.execution_timeout.is_none(),
        "naming a type explicitly asks for THAT type's declared defaults, so a \
         per-start override the type never declared is not carried"
    );

    let predecessor_row = load_execution(&mut conn, predecessor).await;
    assert_eq!(
        predecessor_row.state, "CONTINUED_AS_NEW",
        "the predecessor must seal normally, never fail as its own blocker"
    );
    assert!(
        predecessor_row.error.is_none(),
        "a self-named target must not be reported as a live-occupant collision"
    );
}

/// A *live* run of the target type already owns `(target, workflow_id)`.
/// Harvest admits exactly one active run per pair, so the transition must fail
/// the predecessor terminally with an actionable message — never cancel the
/// bystander, and never leave a half-written successor.
#[tokio::test]
async fn a_live_run_of_the_target_type_blocks_the_transition_terminally() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let occupant = park_occupant(&mut conn, phase2, &workflow_id, "RUNNING").await;
    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let worker = build_runtime_worker("w-803-slot-live", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let failed = wait_for_execution_state(&url, predecessor, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let error = failed
        .error
        .expect("a terminal failure must carry an error");
    assert!(
        error.contains(phase2) && error.contains("already has a live execution"),
        "the operator message must name the target phase and the collision, got: {error}"
    );

    // No continue-as-new recorded, and the bystander is untouched.
    let history = load_history_from_url(&url, predecessor).await;
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "a blocked transition may not record a continue-as-new"
    );
    assert_eq!(
        load_execution(&mut conn, occupant).await.state,
        "RUNNING",
        "the live occupant is a bystander — the transition must not cancel or seal it"
    );

    // Only the failed predecessor and the untouched occupant exist.
    let rows: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count rows");
    assert_eq!(rows, 2, "no successor may be created for a blocked slot");
}

// ---------------------------------------------------------------------------
// AC5 — an unregistered target fails terminally and creates NO successor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn continuing_into_an_unregistered_type_fails_terminally_without_a_successor() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let missing = leaked("never_registered");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": missing}),
    )
    .await;

    // Deliberately register ONLY phase 1.
    let reg = registry(vec![wf(phase1, phase_one)]);
    let worker = build_runtime_worker("w-803-unregistered", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let failed = wait_for_execution_state(&url, predecessor, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let error = failed
        .error
        .expect("a terminal failure must carry an error");
    assert!(
        error.contains(missing) && error.contains("not registered"),
        "the operator message must name the missing type and the reason, got: {error}"
    );

    // A terminal `WorkflowFailed` event, never a silent no-op.
    let history = load_history_from_url(&url, predecessor).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "the predecessor must be sealed with WorkflowFailed"
    );
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
        "no continue-as-new may be recorded when the target is unregistered"
    );

    // …and crucially, NO successor row exists under this workflow_id.
    let rows: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count rows");
    assert_eq!(
        rows, 1,
        "only the failed predecessor may exist — an undispatchable successor \
         must never be created"
    );
}

// ---------------------------------------------------------------------------
// AC7 — carryover is type-agnostic
// ---------------------------------------------------------------------------

/// Scheduled carryover (#488) and schedule lineage (#534) are copied onto the
/// successor by code that never consults the workflow type, so they must
/// survive a *cross-type* transition byte-for-byte.
#[tokio::test]
async fn scheduled_carryover_and_lineage_survive_a_type_change() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    let carried = stamp_type_agnostic_carryover(&mut conn, predecessor).await;
    let (schedule_id, slot) = (carried.schedule_id, carried.slot);

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-803-carryover").await;

    // The frozen carryover value (#488) rode across the type change.
    let succ_history = load_history_from_url(&url, successor).await;
    match succ_history.events.as_slice().first() {
        Some(WorkflowEvent::WorkflowStarted {
            last_completion_result,
            ..
        }) => assert_eq!(
            last_completion_result.as_ref(),
            Some(&serde_json::json!({"rows": 41})),
            "last_completion_result must carry across a type change"
        ),
        other => panic!("expected WorkflowStarted first, got {other:?}"),
    }

    // …and so did schedule lineage (#534): the continuation is still the same
    // logical scheduled run, in the same slot, even under a new type.
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(
        after.schedule_id,
        Some(schedule_id),
        "schedule lineage must survive a type change"
    );
    assert_eq!(
        after.scheduled_for.map(|s| s.timestamp_millis()),
        Some(slot.timestamp_millis()),
        "the logical slot must survive a type change"
    );

    // AC7 in full: every remaining type-agnostic column is byte-identical.
    assert_eq!(
        after.memo,
        Some(carried.memo),
        "memo must carry across a type change"
    );
    assert_eq!(
        after.search_attrs,
        Some(carried.search_attrs),
        "search attributes must carry across a type change (AC7 names them)"
    );
    assert_eq!(
        after.context_headers,
        Some(carried.headers),
        "context headers must carry across a type change"
    );
    assert_eq!(
        after.completion_callbacks,
        Some(carried.callbacks),
        "completion callbacks (#605) must carry across a type change"
    );
    assert_eq!(
        after.assigned_build_id.as_deref(),
        Some("build-2026.08.1"),
        "the assigned build id (#171) must carry — otherwise an incompatible \
         worker could claim the successor"
    );

    // Chain identity (#701): the successor points back at the predecessor and
    // the chain still names its original head.
    assert_eq!(
        after.continued_from_exec_id,
        Some(predecessor.as_uuid()),
        "the successor must link back to the run it continued"
    );
    assert_eq!(
        after.first_exec_id,
        Some(predecessor.as_uuid()),
        "a first transition makes the predecessor the chain head"
    );
}

/// A signal that lands **while the transitioning task is committing** is
/// reassigned to the successor rather than stranded on the sealed predecessor —
/// the `consumed = false` reassignment in `persist_workflow_continue_as_new`,
/// which is unconditional and therefore type-agnostic.
///
/// That window is the *only* one in which a signal is genuinely unconsumed at
/// transition time: every workflow-task pickup ingests all pending signals into
/// history first (`ingest_due_timers_and_signals` marks them `consumed`). It is
/// reproduced deterministically with the repo's established row-lock
/// choreography — hold `FOR UPDATE` on the predecessor so the worker's persist
/// transaction blocks on its seal UPDATE, insert the signal, then release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signal_landing_mid_transition_reassigns_to_the_cross_type_successor() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let mut lock_conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    // Hold the predecessor's row so the worker's persist transaction parks on
    // its seal UPDATE, *after* ingest has already run and found nothing pending.
    let holder_pid = hold_execution_row_lock(&mut lock_conn, predecessor).await;

    let reg = registry(vec![wf(phase1, phase_one), wf(phase2, phase_two)]);
    let worker = build_runtime_worker("w-803-midflight-signal", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    wait_for_a_blocked_writer(&mut conn, holder_pid).await;

    // The signal arrives in the window the reassignment exists for. Inserted
    // directly rather than through `send_signal`, whose target validation
    // (issue #753) takes the execution row `FOR UPDATE` — it would queue behind
    // the very lock this test is holding and deadlock. The row shape is what
    // matters here: an unconsumed signal against the predecessor.
    diesel::sql_query(
        "INSERT INTO harvest_signals (id, workflow_exec_id, signal_name, payload, consumed) \
         VALUES ($1, $2, 'late_arrival', '{\"n\": 1}'::jsonb, false)",
    )
    .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
    .bind::<diesel::sql_types::Uuid, _>(predecessor.as_uuid())
    .execute(&mut conn)
    .await
    .expect("queue an unconsumed signal mid-transition");

    lock_conn
        .batch_execute("COMMIT")
        .await
        .expect("release the row lock");

    let _sealed = wait_for_execution_state(&url, predecessor, "CONTINUED_AS_NEW").await;
    let (successor, _) = recorded_transition(&url, predecessor).await;
    worker.shutdown();
    handle.await.expect("worker join");

    let reassigned: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(successor.as_uuid()))
        .filter(harvest_signals::signal_name.eq("late_arrival"))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count signals");
    assert_eq!(
        reassigned, 1,
        "a signal landing mid-transition must follow the entity into the new \
         phase, not be stranded on the sealed predecessor"
    );
    let stranded: i64 = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(predecessor.as_uuid()))
        .filter(harvest_signals::consumed.eq(false))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count stranded signals");
    assert_eq!(
        stranded, 0,
        "nothing may be left unconsumed on the predecessor"
    );
}

// ---------------------------------------------------------------------------
// Success metric — an external signal-with-start attaches to the LIVE successor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn signal_with_start_naming_the_new_type_attaches_to_the_live_successor() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    let reg = registry(vec![
        wf(phase1, phase_one),
        wf(phase2, phase_two_awaits_signal),
    ]);
    let worker = build_runtime_worker("w-803-sws", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let _sealed = wait_for_execution_state(&url, predecessor, "CONTINUED_AS_NEW").await;
    let (successor, _) = recorded_transition(&url, predecessor).await;

    // The entity is now live as `phase2`. An external caller addressing it by
    // the CURRENT phase type + the stable workflow_id attaches to that run —
    // it does not start a duplicate.
    let outcome = signal_with_start(
        &mut conn,
        phase2,
        &workflow_id,
        serde_json::json!({"phase": "two"}),
        serde_json::json!({"plan": "enterprise"}),
    )
    .await;

    assert!(
        !outcome.started_fresh,
        "signal_with_start must ATTACH to the live successor, not start a duplicate"
    );
    assert_eq!(
        outcome.exec_id, successor,
        "it must attach to the exact successor the transition created"
    );

    // The signal unblocks the successor, proving the attachment is live.
    let done = wait_for_execution_state(&url, successor, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    let output = done.output.expect("successor output");
    assert_eq!(output["signal"]["plan"], "enterprise");
}

/// The documented **addressing consequence**: harvest's active-run identity is
/// `(workflow_name, workflow_id)`, so a caller still naming the OLD type after a
/// transition starts a *fresh* run of that old type. It coexists with the live
/// successor rather than attaching to it — the two occupy different uniqueness
/// slots. Pinned so the contract is explicit rather than folklore.
#[tokio::test]
async fn signal_with_start_naming_the_old_type_starts_a_separate_run() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
    )
    .await;

    let reg = registry(vec![
        wf(phase1, phase_one),
        wf(phase2, phase_two_awaits_signal),
    ]);
    let worker = build_runtime_worker("w-803-old-name", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let _sealed = wait_for_execution_state(&url, predecessor, "CONTINUED_AS_NEW").await;
    let (successor, _) = recorded_transition(&url, predecessor).await;
    worker.shutdown();
    handle.await.expect("worker join");

    let outcome = signal_with_start(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2}),
        serde_json::json!({}),
    )
    .await;

    assert!(
        outcome.started_fresh,
        "the old type's uniqueness slot was released by the transition, so a \
         caller naming it starts a fresh run"
    );
    assert_ne!(
        outcome.exec_id, successor,
        "that fresh run is NOT the live successor — address the entity by its \
         current phase type"
    );
}

// ---------------------------------------------------------------------------
// AC8 — root-only guard unchanged for the cross-type call too
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_child_workflow_cannot_cross_type_continue_either() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let child_type = leaked("child_phase_one");
    let target = leaked("child_phase_two");
    let workflow_id = format!("child-{}", Uuid::new_v4().simple());

    // A root row to parent the child, then the child itself.
    let parent = start_root(
        &mut conn,
        leaked("parent_holder"),
        &format!("parent-{}", Uuid::new_v4().simple()),
        serde_json::json!({}),
    )
    .await;
    let child = start_root(
        &mut conn,
        child_type,
        &workflow_id,
        serde_json::json!({"next_type": target}),
    )
    .await;
    diesel::update(harvest_workflow_executions::table.find(child.as_uuid()))
        .set(harvest_workflow_executions::parent_id.eq(Some(parent.as_uuid())))
        .execute(&mut conn)
        .await
        .expect("reparent the child");

    let reg = registry(vec![wf(child_type, phase_one), wf(target, phase_two)]);
    let worker = build_runtime_worker("w-803-child", 2, 1, reg);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
    let failed = wait_for_execution_state(&url, child, "FAILED").await;
    worker.shutdown();
    handle.await.expect("worker join");

    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|e| e.contains("child workflows")),
        "the root-only guard must still reject a cross-type continuation, got {:?}",
        failed.error
    );
}

/// Concurrency (#247) is re-resolved against the TARGET type's policy: a target
/// declaring none must clear the predecessor's key rather than keep governing
/// the successor under a policy that belongs to a different type.
#[tokio::test]
async fn successor_concurrency_key_is_resolved_from_the_new_type() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let phase1 = leaked("trial_subscription");
    let phase2 = leaked("paid_subscription");
    let workflow_id = format!("sub-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        phase1,
        &workflow_id,
        serde_json::json!({"next_type": phase2, "tenant_id": "acme"}),
    )
    .await;
    // Predecessor's task carries a key from phase 1's policy.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET concurrency_key = 'trial:acme', concurrency_cap = 3 \
         WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(predecessor.as_uuid())
    .execute(&mut conn)
    .await
    .expect("stamp predecessor task concurrency");

    // Phase 2 declares its OWN policy over a different field.
    let mut target = wf(phase2, phase_two);
    target.concurrency = Some(autumn_harvest::concurrency::ConcurrencyPolicy {
        key_expr: "phase",
        limit: 7,
    });

    let reg = registry(vec![wf(phase1, phase_one), target]);
    let (successor, _) = drive_transition(&url, predecessor, reg, "w-803-concurrency").await;

    let keys: TaskKeys = diesel::sql_query(
        "SELECT concurrency_key, concurrency_cap FROM harvest_task_queue WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(successor.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("load successor task");

    assert_eq!(
        keys.concurrency_key.as_deref(),
        Some("two"),
        "the key must be re-resolved from the NEW type's policy against the new \
         input, not carried from the predecessor's task"
    );
    assert_eq!(keys.concurrency_cap, Some(7));
}

/// A same-type `continue_as_new` still behaves exactly as before (AC1): the
/// successor keeps the predecessor's name and carries its lifecycle columns
/// verbatim, including an execution timeout the type itself never declared.
#[tokio::test]
async fn same_type_continue_as_new_is_unchanged() {
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let name = leaked("legacy_loop");
    let workflow_id = format!("loop-{}", Uuid::new_v4().simple());

    let predecessor = start_root(
        &mut conn,
        name,
        &workflow_id,
        serde_json::json!({"phase": "one"}),
    )
    .await;
    // A per-start override the TYPE never declares — it must be carried
    // verbatim, which is exactly what distinguishes the same-type path.
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set((
            harvest_workflow_executions::execution_timeout.eq(Some(ChronoDuration::seconds(900))),
            harvest_workflow_executions::owner.eq(Some("growth-team")),
        ))
        .execute(&mut conn)
        .await
        .expect("stamp predecessor");

    let reg = registry(vec![wf(name, same_type_loop)]);
    let (successor, recorded_type) =
        drive_transition(&url, predecessor, reg, "w-803-same-type").await;

    assert!(
        recorded_type.is_none(),
        "a same-type continuation records no target type (AC1/AC3)"
    );
    let after = load_execution(&mut conn, successor).await;
    assert_eq!(after.workflow_name, name);
    assert_eq!(
        after.execution_timeout,
        Some(ChronoDuration::seconds(900)),
        "the per-start override must still be carried verbatim"
    );
    assert_eq!(after.owner.as_deref(), Some("growth-team"));
}

/// Silence the unused-import lint when only a subset of the suite compiles.
#[allow(dead_code)]
const fn _unused(_p: Priority, _c: WorkflowIdConflictPolicy) {}

fn same_type_loop<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if input.get("phase").and_then(serde_json::Value::as_str) == Some("one") {
            ctx.continue_as_new(serde_json::json!({"phase": "two"}))
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("continue_as_new must not resolve");
        }
        Ok(input)
    })
}

/// The type-agnostic columns stamped onto a predecessor by
/// [`stamp_type_agnostic_carryover`], so the assertions can compare against the
/// exact values written.
struct CarriedColumns {
    schedule_id: Uuid,
    slot: chrono::DateTime<Utc>,
    memo: serde_json::Value,
    search_attrs: serde_json::Value,
    headers: serde_json::Value,
    callbacks: serde_json::Value,
}

/// Freeze a carryover value onto the predecessor's `WorkflowStarted` and stamp
/// every type-agnostic column a cross-type transition must copy verbatim.
///
/// These lines were *moved* by the #803 diff (the row literal was restructured
/// around the new `defaults` struct), so "unchanged code" is not a defence.
/// `assigned_build_id` is load-bearing in particular: a successor that lost it
/// could be claimed by an incompatible worker (#171).
async fn stamp_type_agnostic_carryover(
    conn: &mut AsyncPgConnection,
    predecessor: ExecutionId,
) -> CarriedColumns {
    let carried = CarriedColumns {
        schedule_id: Uuid::new_v4(),
        slot: Utc::now() - ChronoDuration::minutes(5),
        memo: serde_json::json!({"tier": "trial"}),
        search_attrs: serde_json::json!({"customer": "acme", "region": "eu"}),
        headers: serde_json::json!({"traceparent": "00-abc-def-01"}),
        callbacks: serde_json::json!([{"url": "https://example.test/hook"}]),
    };
    diesel::sql_query(
        "UPDATE harvest_events \
         SET event_data = jsonb_set(event_data, '{data,last_completion_result}', '{\"rows\": 41}') \
         WHERE workflow_exec_id = $1 AND event_id = 0",
    )
    .bind::<diesel::sql_types::Uuid, _>(predecessor.as_uuid())
    .execute(&mut *conn)
    .await
    .expect("stamp carryover");
    diesel::update(harvest_workflow_executions::table.find(predecessor.as_uuid()))
        .set((
            harvest_workflow_executions::schedule_id.eq(Some(carried.schedule_id)),
            harvest_workflow_executions::scheduled_for.eq(Some(carried.slot)),
            harvest_workflow_executions::memo.eq(Some(carried.memo.clone())),
            harvest_workflow_executions::search_attrs.eq(Some(carried.search_attrs.clone())),
            harvest_workflow_executions::context_headers.eq(Some(carried.headers.clone())),
            harvest_workflow_executions::completion_callbacks.eq(Some(carried.callbacks.clone())),
            harvest_workflow_executions::assigned_build_id.eq(Some("build-2026.08.1")),
        ))
        .execute(conn)
        .await
        .expect("stamp schedule lineage and type-agnostic carryover columns");
    carried
}

/// Row shape for the successor's re-resolved per-key concurrency (#247).
#[derive(diesel::QueryableByName)]
struct TaskKeys {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    concurrency_key: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    concurrency_cap: Option<i32>,
}
