#![cfg(feature = "db")]

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::signal::{
    load_pending_signals, mark_signals_consumed, send_signal, send_signal_idempotent,
};
use autumn_harvest::types::ExecutionId;
use diesel_async::RunQueryDsl;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
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
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
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
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql")
);

async fn setup_test_db() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(
        &database_url,
    )
    .await
    .expect("failed to connect to Postgres container");

    (conn, container)
}

#[tokio::test]
async fn test_send_and_load_signals() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    // Create workflow execution first (FK constraint)
    let new_exec = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "test_workflow",
        workflow_id: "test_id",
        run_id: exec_id.as_uuid(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&new_exec)
        .execute(&mut conn)
        .await
        .unwrap();

    // Load initial - should be empty
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert!(signals.is_empty());

    // Send a signal
    let payload = serde_json::json!({"key": "value"});
    send_signal(&mut conn, exec_id, "test_signal", payload.clone())
        .await
        .unwrap();

    // Load again
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].signal_name, "test_signal");
    assert_eq!(signals[0].payload, payload);
    assert!(!signals[0].consumed);
}

#[tokio::test]
async fn test_mark_signals_consumed() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    // Create workflow execution
    let new_exec = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "test_workflow",
        workflow_id: "test_id",
        run_id: exec_id.as_uuid(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&new_exec)
        .execute(&mut conn)
        .await
        .unwrap();

    // Send signals
    send_signal(&mut conn, exec_id, "sig1", serde_json::json!(1))
        .await
        .unwrap();
    send_signal(&mut conn, exec_id, "sig2", serde_json::json!(2))
        .await
        .unwrap();

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 2);

    // Mark first signal consumed
    let ids_to_mark = vec![signals[0].id];
    mark_signals_consumed(&mut conn, &ids_to_mark)
        .await
        .unwrap();

    // Load again
    let pending_signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(pending_signals.len(), 1);
    assert_eq!(pending_signals[0].id, signals[1].id);
    assert_eq!(pending_signals[0].signal_name, "sig2");

    let ids_to_mark = vec![];
    mark_signals_consumed(&mut conn, &ids_to_mark)
        .await
        .unwrap(); // Empty list test
    let pending_signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(pending_signals.len(), 1);
}

// ── Signal idempotency keys ───────────────────────────────────────────────

/// Insert a fresh RUNNING workflow execution and return its id.
async fn insert_running_execution(conn: &mut diesel_async::AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    // Unique workflow_id per execution: the active partial unique index on
    // (workflow_name, workflow_id) rejects a second RUNNING row otherwise.
    let workflow_id = exec_id.to_string();
    let new_exec = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "test_workflow",
        workflow_id: &workflow_id,
        run_id: exec_id.as_uuid(),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&new_exec)
        .execute(conn)
        .await
        .unwrap();
    exec_id
}

#[tokio::test]
async fn idempotent_signal_with_same_key_lands_exactly_once() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_running_execution(&mut conn).await;

    // Five duplicate deliveries carrying the same idempotency key — the
    // falsifiable success metric: the signal must land exactly once.
    let mut delivered_flags = Vec::new();
    for _ in 0..5 {
        let delivered = send_signal_idempotent(
            &mut conn,
            exec_id,
            "approval",
            serde_json::json!({"ok": true}),
            Some("evt_42"),
        )
        .await
        .unwrap();
        delivered_flags.push(delivered);
    }

    // Exactly one row queued.
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(
        signals.len(),
        1,
        "5 deliveries with the same key must queue exactly one signal row"
    );
    assert_eq!(signals[0].signal_name, "approval");

    // First call delivered (true); the remaining four deduped (false).
    assert_eq!(
        delivered_flags,
        vec![true, false, false, false, false],
        "only the first keyed delivery is fresh; the rest dedupe"
    );
}

#[tokio::test]
async fn idempotent_signal_with_distinct_keys_lands_each_time() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_running_execution(&mut conn).await;

    for i in 0..4 {
        let delivered = send_signal_idempotent(
            &mut conn,
            exec_id,
            "tick",
            serde_json::json!(i),
            Some(&format!("evt_{i}")),
        )
        .await
        .unwrap();
        assert!(delivered, "each distinct key is a fresh delivery");
    }

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(
        signals.len(),
        4,
        "four distinct keys must queue four signal rows"
    );
}

#[tokio::test]
async fn unkeyed_signal_preserves_at_least_once_behavior() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_running_execution(&mut conn).await;

    // No idempotency key: every delivery is a fresh row (legacy behavior).
    for _ in 0..3 {
        let delivered =
            send_signal_idempotent(&mut conn, exec_id, "ping", serde_json::json!(null), None)
                .await
                .unwrap();
        assert!(delivered, "unkeyed deliveries always report delivered=true");
    }

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(
        signals.len(),
        3,
        "three unkeyed deliveries must queue three rows (at-least-once)"
    );
}

#[tokio::test]
async fn idempotency_key_is_scoped_per_execution() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_a = insert_running_execution(&mut conn).await;
    let exec_b = insert_running_execution(&mut conn).await;

    // The same key on two different executions does NOT collide — dedupe is
    // scoped to (workflow_exec_id, idempotency_key).
    let a = send_signal_idempotent(
        &mut conn,
        exec_a,
        "sig",
        serde_json::json!(1),
        Some("shared"),
    )
    .await
    .unwrap();
    let b = send_signal_idempotent(
        &mut conn,
        exec_b,
        "sig",
        serde_json::json!(2),
        Some("shared"),
    )
    .await
    .unwrap();

    assert!(a && b, "same key on distinct executions both deliver");
    assert_eq!(
        load_pending_signals(&mut conn, exec_a).await.unwrap().len(),
        1
    );
    assert_eq!(
        load_pending_signals(&mut conn, exec_b).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn plain_send_signal_still_appends_each_call() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_running_execution(&mut conn).await;

    // The thin `send_signal` wrapper must be unchanged: no dedupe.
    send_signal(&mut conn, exec_id, "x", serde_json::json!(1))
        .await
        .unwrap();
    send_signal(&mut conn, exec_id, "x", serde_json::json!(2))
        .await
        .unwrap();

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 2);
}
