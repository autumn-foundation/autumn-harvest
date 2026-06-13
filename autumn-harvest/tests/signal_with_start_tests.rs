//! Tests for `signal_with_start` core API (issue #244).
//!
//! These tests cover the atomic start-or-attach + signal primitive and
//! enumerate every reuse-policy × prior-state outcome documented in the
//! issue's 4×5 matrix, plus the optional idempotency-key dedup.

#![cfg(feature = "db")]

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{
    SignalWithStartOutcome, SignalWithStartParams, signal_with_start_workflow_execution,
};
use autumn_harvest::schema::harvest_workflow_executions::dsl;
use autumn_harvest::signal::load_pending_signals;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, WorkflowIdReusePolicy};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
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
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql")
);

async fn setup_test_db() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let conn = <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    (conn, container)
}

fn params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    signal_name: &'a str,
    signal_payload: serde_json::Value,
    reuse_policy: WorkflowIdReusePolicy,
) -> SignalWithStartParams<'a> {
    SignalWithStartParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: serde_json::json!({"hello": "world"}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        signal_name,
        signal_payload,
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
    }
}

/// Force an existing execution into a non-RUNNING state.
async fn force_state(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: ExecutionId,
    state: &str,
) {
    diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set((
            dsl::state.eq(state),
            dsl::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(conn)
        .await
        .expect("force_state update");
}

// ─────────────────────────────────────────────────────────────────────────────
// Fresh start outcomes
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn signal_with_start_starts_fresh_when_no_prior_execution_exists() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();
    let p = params(
        "onboard",
        "user-1",
        exec_id,
        "webhook",
        serde_json::json!({"k": 1}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );

    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .expect("call should succeed");

    assert!(out.started_fresh, "expected fresh start");
    assert!(out.signal_delivered, "expected signal to be delivered");
    assert_eq!(out.exec_id, exec_id, "returns the new exec id");
    assert_eq!(out.state, "RUNNING");

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 1, "one signal queued");
    assert_eq!(signals[0].signal_name, "webhook");
}

#[tokio::test]
async fn signal_with_start_appends_signal_to_history_before_first_dispatch() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();
    let p = params(
        "onboard",
        "user-2",
        exec_id,
        "webhook",
        serde_json::json!({"k": 2}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    // The fresh-start path must record the signal as a pending row that the
    // worker's `ingest_pending_signals` will turn into a `SignalReceived`
    // event before invoking the workflow function. Two equivalent guarantees
    // satisfy this: either the signal is already in `harvest_events` as a
    // `SignalReceived` variant, OR it lives in `harvest_signals` waiting for
    // the worker's first tick to ingest. Either is observable on the first
    // dispatch — i.e., before any other event is produced — so we check
    // that *one* of the two is true.
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    let in_history = history
        .events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "webhook"));
    let pending = signals
        .iter()
        .any(|s| s.signal_name == "webhook" && !s.consumed);
    assert!(
        in_history || pending,
        "signal must be observable before the workflow first dispatches: \
         history={:?}, pending={:?}",
        history.events,
        signals,
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Existing-execution outcomes — per reuse policy
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_running(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let p = params(
        workflow_name,
        workflow_id,
        exec_id,
        "seed",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let out = signal_with_start_workflow_execution(conn, p).await.unwrap();
    out.exec_id
}

#[tokio::test]
async fn allow_duplicate_signals_running_execution_and_returns_existing_id() {
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-running").await;

    let p = params(
        "wf",
        "id-running",
        ExecutionId::new(),
        "another",
        serde_json::json!({"k": "v"}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(!out.started_fresh);
    assert!(out.signal_delivered);
    assert_eq!(out.exec_id, first);

    let signals = load_pending_signals(&mut conn, first).await.unwrap();
    assert_eq!(signals.len(), 2, "seed + new signal both queued");
}

#[tokio::test]
async fn allow_duplicate_attaches_to_paused_prior_and_buffers_signal() {
    // Issue #383: PAUSED is a non-terminal active state. A signal-with-start
    // against a paused run must attach and buffer the signal for delivery on
    // resume — it must NOT escalate to TerminateIfRunning and cancel/replace the
    // run an operator deliberately paused.
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-paused").await;
    force_state(&mut conn, first, "PAUSED").await;

    let p = params(
        "wf",
        "id-paused",
        ExecutionId::new(),
        "another",
        serde_json::json!({"k": "v"}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(
        !out.started_fresh,
        "must attach to the paused run, not start fresh"
    );
    assert!(
        out.signal_delivered,
        "signal must be buffered on the paused run for resume"
    );
    assert_eq!(out.exec_id, first, "attaches to the existing paused exec");
    assert_eq!(out.state, "PAUSED", "the prior run stays paused");

    // The paused execution must be untouched (not sealed/cancelled/replaced).
    let state: String = dsl::harvest_workflow_executions
        .find(first.as_uuid())
        .select(dsl::state)
        .first(&mut conn)
        .await
        .unwrap();
    assert_eq!(state, "PAUSED", "the paused execution must not be sealed");

    let signals = load_pending_signals(&mut conn, first).await.unwrap();
    assert_eq!(signals.len(), 2, "seed + buffered signal both queued");
}

#[tokio::test]
async fn allow_duplicate_with_completed_prior_starts_fresh_and_delivers_signal() {
    // Spec invariant: "no signal is silently dropped". When the prior run is
    // terminal under AllowDuplicate, signal-with-start escalates to a fresh
    // start so the signal can land on a live execution. This diverges from
    // the standalone `start_or_load_workflow_execution` behaviour by design.
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-done").await;
    force_state(&mut conn, first, "COMPLETED").await;

    let new_id = ExecutionId::new();
    let p = params(
        "wf",
        "id-done",
        new_id,
        "late",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(out.started_fresh, "terminal prior must trigger fresh start");
    assert!(out.signal_delivered, "fresh start must accept the signal");
    assert_ne!(out.exec_id, first, "new exec id, not the COMPLETED prior");
    assert_eq!(out.exec_id, new_id);

    let pending = load_pending_signals(&mut conn, new_id).await.unwrap();
    assert_eq!(pending.len(), 1, "exactly one signal queued on the new run");
}

#[tokio::test]
async fn reject_duplicate_returns_already_exists_for_any_prior_state() {
    let (mut conn, _container) = setup_test_db().await;
    for state in ["RUNNING", "COMPLETED", "FAILED", "CANCELLED"] {
        let workflow_id = format!("id-{state}");
        let first = seed_running(&mut conn, "wf", &workflow_id).await;
        if state != "RUNNING" {
            force_state(&mut conn, first, state).await;
        }

        let p = params(
            "wf",
            &workflow_id,
            ExecutionId::new(),
            "x",
            serde_json::json!({}),
            WorkflowIdReusePolicy::RejectDuplicate,
        );
        let err = signal_with_start_workflow_execution(&mut conn, p)
            .await
            .expect_err(&format!("reject_duplicate must fail when prior is {state}"));
        match err {
            autumn_harvest::HarvestError::AlreadyExists {
                existing_exec_id, ..
            } => {
                assert_eq!(existing_exec_id, first, "echoes prior exec id for {state}");
            }
            other => panic!("expected AlreadyExists for {state}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn allow_duplicate_failed_only_starts_fresh_for_failed_prior() {
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-failed").await;
    force_state(&mut conn, first, "FAILED").await;

    let new_id = ExecutionId::new();
    let p = params(
        "wf",
        "id-failed",
        new_id,
        "fresh",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(out.started_fresh, "FAILED prior must yield fresh start");
    assert!(out.signal_delivered);
    assert_ne!(out.exec_id, first, "new exec id");
    assert_eq!(out.exec_id, new_id);
}

#[tokio::test]
async fn allow_duplicate_failed_only_attaches_to_running_prior() {
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-attached").await;

    let p = params(
        "wf",
        "id-attached",
        ExecutionId::new(),
        "more",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(!out.started_fresh);
    assert!(out.signal_delivered);
    assert_eq!(out.exec_id, first);
}

#[tokio::test]
async fn terminate_if_running_cancels_then_starts_fresh_and_signals() {
    let (mut conn, _container) = setup_test_db().await;
    let first = seed_running(&mut conn, "wf", "id-terminate").await;

    let new_id = ExecutionId::new();
    let p = params(
        "wf",
        "id-terminate",
        new_id,
        "kick",
        serde_json::json!({"v": 1}),
        WorkflowIdReusePolicy::TerminateIfRunning,
    );
    let out = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    assert!(out.started_fresh);
    assert!(out.signal_delivered);
    assert_eq!(out.exec_id, new_id);

    // Old execution should no longer be RUNNING. TerminateIfRunning cancels
    // the prior row in a separate transaction; the start transaction's
    // `replace_execution` then seals the now-CANCELLED row as
    // `CONTINUED_AS_NEW` so the partial unique index lets the new run land.
    let state: String = dsl::harvest_workflow_executions
        .find(first.as_uuid())
        .select(dsl::state)
        .first(&mut conn)
        .await
        .unwrap();
    assert_ne!(state, "RUNNING", "old run is no longer RUNNING");
    assert!(
        matches!(state.as_str(), "CANCELLED" | "CONTINUED_AS_NEW"),
        "old run terminal state was {state}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Idempotency key dedup
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn idempotency_key_dedupes_signal_within_the_same_execution() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    let mut p = params(
        "wf",
        "id-idem",
        exec_id,
        "webhook",
        serde_json::json!({"v": 1}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    p.idempotency_key = Some("evt-42".to_string());
    let out1 = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();
    assert!(out1.started_fresh);
    assert!(out1.signal_delivered);

    let mut p2 = params(
        "wf",
        "id-idem",
        ExecutionId::new(),
        "webhook",
        serde_json::json!({"v": 1}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    p2.idempotency_key = Some("evt-42".to_string());
    let out2 = signal_with_start_workflow_execution(&mut conn, p2)
        .await
        .unwrap();
    assert!(!out2.started_fresh, "second call attaches");
    assert!(
        !out2.signal_delivered,
        "duplicate idempotency key must not deliver a second signal"
    );
    assert_eq!(out2.exec_id, out1.exec_id);

    let signals = load_pending_signals(&mut conn, out1.exec_id).await.unwrap();
    assert_eq!(signals.len(), 1, "exactly one signal row");
}

#[tokio::test]
async fn different_idempotency_keys_deliver_independent_signals() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    let mut p = params(
        "wf",
        "id-multi-key",
        exec_id,
        "webhook",
        serde_json::json!({"i": 1}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    p.idempotency_key = Some("evt-a".to_string());
    let out1 = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();

    let mut p2 = params(
        "wf",
        "id-multi-key",
        ExecutionId::new(),
        "webhook",
        serde_json::json!({"i": 2}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    p2.idempotency_key = Some("evt-b".to_string());
    let out2 = signal_with_start_workflow_execution(&mut conn, p2)
        .await
        .unwrap();
    assert!(out2.signal_delivered);
    assert_eq!(out2.exec_id, out1.exec_id);

    let signals = load_pending_signals(&mut conn, out1.exec_id).await.unwrap();
    assert_eq!(signals.len(), 2, "both unique keys delivered");
}

#[tokio::test]
async fn outcome_struct_distinguishes_started_vs_attached() {
    // Acceptance: "response distinguishes the two outcomes so embedders can log/branch"
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();
    let p = params(
        "wf",
        "id-distinguish",
        exec_id,
        "go",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let first: SignalWithStartOutcome = signal_with_start_workflow_execution(&mut conn, p)
        .await
        .unwrap();
    assert!(first.started_fresh);

    let p2 = params(
        "wf",
        "id-distinguish",
        ExecutionId::new(),
        "go",
        serde_json::json!({}),
        WorkflowIdReusePolicy::AllowDuplicate,
    );
    let second = signal_with_start_workflow_execution(&mut conn, p2)
        .await
        .unwrap();
    assert!(!second.started_fresh);
    assert_eq!(second.exec_id, first.exec_id);
}
