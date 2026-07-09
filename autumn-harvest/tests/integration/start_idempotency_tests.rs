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
//! Request-scoped start idempotency integration tests — issue #808.
//!
//! Verifies the core `start_or_load_workflow_execution_idempotent` primitive
//! against a real Postgres container:
//! - **Success metric** — 100 same-key reserves converge on exactly ONE
//!   execution with exactly ONE `WorkflowStarted` event; the rest dedup.
//! - **Dedup returns the same exec id** — a second same-key start (even with a
//!   different `workflow_id`) is a no-op returning the original run.
//! - **Distinct keys → distinct runs.**
//! - **Window reuse** — after the retention window elapses (created_at
//!   backdated), the same key reserves fresh and creates a NEW run.
//! - **Reserve rolls back if the start fails** — a `reject_duplicate` conflict
//!   leaves no idempotency claim behind so a retry can start fresh.
//! - **Defensive reclaim** — a claim pointing at a retention-deleted execution
//!   is reclaimed rather than wedging.

use autumn_harvest::StartWorkflowParams;
use autumn_harvest::error::HarvestError;
use autumn_harvest::execution::{
    IdempotentStartOutcome, start_or_load_workflow_execution_idempotent,
};
use autumn_harvest::start_idempotency::{
    DEFAULT_START_IDEMPOTENCY_WINDOW, StartIdempotencyReservation, reserve_start_idempotency,
};
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    "\n",
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260618000001_harvest_debounce/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    "\n",
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    "\n",
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    "\n",
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    "\n",
    // issue #808: the start-idempotency table under test.
    include_str!("../../migrations/20260709000000_harvest_start_idempotency/up.sql"),
);

async fn connect(url: &str) -> AsyncPgConnection {
    use diesel_async::AsyncConnection;
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test db")
}

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(INIT_SQL).await.expect("migrations");
    (conn, container)
}

fn params<'a>(
    wf: &'a str,
    wf_id: &'a str,
    exec_id: ExecutionId,
    reuse: WorkflowIdReusePolicy,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: wf,
        workflow_id: wf_id,
        exec_id,
        input: serde_json::json!({"n": 1}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: reuse,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
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
    }
}

const fn window_secs() -> f64 {
    DEFAULT_START_IDEMPOTENCY_WINDOW.as_secs_f64()
}

async fn scalar_i64(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(sql)
        .get_result::<N>(conn)
        .await
        .expect("scalar query")
        .n
}

async fn exec_count(conn: &mut AsyncPgConnection, wf: &str) -> i64 {
    scalar_i64(
        conn,
        &format!(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = '{wf}'"
        ),
    )
    .await
}

async fn workflow_started_events(conn: &mut AsyncPgConnection) -> i64 {
    scalar_i64(
        conn,
        "SELECT COUNT(*) AS n FROM harvest_events WHERE event_data->>'type' = 'WorkflowStarted'",
    )
    .await
}

async fn idem_claim_count(conn: &mut AsyncPgConnection, wf: &str, key: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_start_idempotency \
         WHERE workflow_name = $1 AND idempotency_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result::<N>(conn)
    .await
    .expect("claim count")
    .n
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// AC: a second start with the same key — even a different workflow_id —
/// deduplicates onto the FIRST execution (no second WorkflowStarted).
#[tokio::test]
async fn same_key_dedups_to_the_same_execution() {
    let (mut conn, _c) = setup_db().await;
    let wf = "order_flow";
    let key = "delivery-1";
    let e1 = ExecutionId::new_for_shard(ShardId::new(0));

    let first = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, "wid-a", e1, WorkflowIdReusePolicy::AllowDuplicate),
        key,
        window_secs(),
        None,
    )
    .await
    .expect("first start");
    let created_exec = match first {
        IdempotentStartOutcome::Started(s) => {
            assert!(s.created, "first same-key start creates the run");
            s.exec_id
        }
        IdempotentStartOutcome::Deduplicated { .. } => panic!("first start must not dedup"),
    };

    // Second call with the SAME key but a DIFFERENT workflow_id and a fresh
    // exec_id: must dedup onto the original run.
    let e2 = ExecutionId::new_for_shard(ShardId::new(0));
    let second = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, "wid-b", e2, WorkflowIdReusePolicy::AllowDuplicate),
        key,
        window_secs(),
        None,
    )
    .await
    .expect("second start");
    match second {
        IdempotentStartOutcome::Deduplicated { exec_id, .. } => {
            assert_eq!(exec_id, created_exec, "dedup returns the original exec id");
        }
        IdempotentStartOutcome::Started(_) => panic!("second same-key start must dedup"),
    }

    assert_eq!(exec_count(&mut conn, wf).await, 1, "exactly one execution");
    assert_eq!(
        workflow_started_events(&mut conn).await,
        1,
        "exactly one WorkflowStarted event"
    );
}

/// AC: distinct keys start distinct runs.
#[tokio::test]
async fn distinct_keys_start_distinct_runs() {
    let (mut conn, _c) = setup_db().await;
    let wf = "order_flow";
    for (i, key) in ["k1", "k2"].iter().enumerate() {
        let e = ExecutionId::new_for_shard(ShardId::new(0));
        let out = start_or_load_workflow_execution_idempotent(
            &mut conn,
            params(
                wf,
                &format!("wid-{i}"),
                e,
                WorkflowIdReusePolicy::AllowDuplicate,
            ),
            key,
            window_secs(),
            None,
        )
        .await
        .expect("start");
        assert!(matches!(out, IdempotentStartOutcome::Started(_)));
    }
    assert_eq!(exec_count(&mut conn, wf).await, 2, "two distinct runs");
    assert_eq!(workflow_started_events(&mut conn).await, 2);
}

/// Success metric: 100 same-key reserves converge on exactly ONE execution.
#[tokio::test]
async fn hundred_same_key_reserves_yield_one_execution() {
    let (mut conn, _c) = setup_db().await;
    let wf = "hammer";
    let key = "same";
    let mut created: Option<ExecutionId> = None;
    let mut dedups = 0;
    for i in 0..100 {
        let e = ExecutionId::new_for_shard(ShardId::new(0));
        let out = start_or_load_workflow_execution_idempotent(
            &mut conn,
            params(
                wf,
                &format!("wid-{i}"),
                e,
                WorkflowIdReusePolicy::AllowDuplicate,
            ),
            key,
            window_secs(),
            None,
        )
        .await
        .expect("start");
        match out {
            IdempotentStartOutcome::Started(s) => {
                assert!(created.is_none(), "only the first call creates");
                created = Some(s.exec_id);
            }
            IdempotentStartOutcome::Deduplicated { exec_id, .. } => {
                assert_eq!(
                    exec_id,
                    created.unwrap(),
                    "all dedups point at the first run"
                );
                dedups += 1;
            }
        }
    }
    assert_eq!(dedups, 99);
    assert_eq!(exec_count(&mut conn, wf).await, 1);
    assert_eq!(workflow_started_events(&mut conn).await, 1);
}

/// AC: after the retention window elapses (created_at backdated), the same key
/// reserves fresh and creates a NEW run.
#[tokio::test]
async fn key_is_reusable_after_the_window_elapses() {
    let (mut conn, _c) = setup_db().await;
    use diesel_async::RunQueryDsl;
    let wf = "cron_like";
    let key = "daily";

    let e1 = ExecutionId::new_for_shard(ShardId::new(0));
    let first = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, "wid-1", e1, WorkflowIdReusePolicy::AllowDuplicate),
        key,
        window_secs(),
        None,
    )
    .await
    .expect("first");
    assert!(matches!(first, IdempotentStartOutcome::Started(_)));

    // Backdate the claim two days into the past — beyond the 24h window.
    diesel::sql_query(
        "UPDATE harvest_start_idempotency SET created_at = now() - INTERVAL '2 days' \
         WHERE workflow_name = $1 AND idempotency_key = $2",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(&mut conn)
    .await
    .expect("backdate");

    let e2 = ExecutionId::new_for_shard(ShardId::new(0));
    let second = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, "wid-2", e2, WorkflowIdReusePolicy::AllowDuplicate),
        key,
        window_secs(),
        None,
    )
    .await
    .expect("second");
    match second {
        IdempotentStartOutcome::Started(s) => {
            assert!(s.created, "window-expired key starts a fresh run");
            assert_eq!(s.exec_id, e2);
        }
        IdempotentStartOutcome::Deduplicated { .. } => {
            panic!("window-expired key must NOT dedup")
        }
    }
    assert_eq!(exec_count(&mut conn, wf).await, 2, "two distinct runs");
}

/// AC: a start that fails after reserving rolls back the reservation so a retry
/// can start fresh (no orphaned claim).
#[tokio::test]
async fn reserve_rolls_back_when_the_start_fails() {
    let (mut conn, _c) = setup_db().await;
    let wf = "reject_flow";
    let key = "k-fail";
    let wid = "collide";

    // Pre-create a RUNNING execution for `wid` so a reject_duplicate start of the
    // same workflow_id conflicts.
    let pre = ExecutionId::new_for_shard(ShardId::new(0));
    let seeded = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, wid, pre, WorkflowIdReusePolicy::AllowDuplicate),
        "seed-key",
        window_secs(),
        None,
    )
    .await
    .expect("seed run");
    assert!(matches!(seeded, IdempotentStartOutcome::Started(_)));

    // Fresh idempotency key, but reject_duplicate on a colliding workflow_id →
    // reserve then AlreadyExists → rollback.
    let e = ExecutionId::new_for_shard(ShardId::new(0));
    let err = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, wid, e, WorkflowIdReusePolicy::RejectDuplicate),
        key,
        window_secs(),
        None,
    )
    .await;
    assert!(
        matches!(err, Err(HarvestError::AlreadyExists { .. })),
        "start conflicts under reject_duplicate: {err:?}"
    );
    assert_eq!(
        idem_claim_count(&mut conn, wf, key).await,
        0,
        "the reservation must have rolled back — no orphan claim"
    );
}

/// The defensive reclaim path: a claim pointing at a retention-deleted execution
/// is reclaimed rather than wedging a subsequent same-key start.
#[tokio::test]
async fn dangling_claim_is_reclaimed() {
    let (mut conn, _c) = setup_db().await;
    use diesel_async::RunQueryDsl;
    let wf = "reclaim_flow";
    let key = "orphan";

    let e1 = ExecutionId::new_for_shard(ShardId::new(0));
    let first = start_or_load_workflow_execution_idempotent(
        &mut conn,
        params(wf, "wid-1", e1, WorkflowIdReusePolicy::AllowDuplicate),
        key,
        window_secs(),
        None,
    )
    .await
    .expect("first");
    let orig = match first {
        IdempotentStartOutcome::Started(s) => s.exec_id,
        IdempotentStartOutcome::Deduplicated { .. } => panic!(),
    };

    // Simulate retention deleting the execution while the claim still points at
    // it (window not yet elapsed).
    diesel::sql_query("DELETE FROM harvest_events WHERE workflow_exec_id = $1")
        .bind::<diesel::sql_types::Uuid, _>(orig.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete events");
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(orig.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete exec");

    // Directly exercise the reserve reclaim path: within the window, a dangling
    // pointer must reclaim (Reserved) rather than return a Duplicate for a run
    // that no longer exists.
    let e2 = ExecutionId::new_for_shard(ShardId::new(0));
    let reservation = reserve_start_idempotency(&mut conn, wf, key, e2, 0, window_secs())
        .await
        .expect("reserve");
    assert_eq!(
        reservation,
        StartIdempotencyReservation::Reserved,
        "dangling claim reclaims to Reserved"
    );
    // The claim now points at the new exec id.
    let pointed: Uuid = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct R {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            workflow_exec_id: Uuid,
        }
        diesel::sql_query(
            "SELECT workflow_exec_id FROM harvest_start_idempotency \
             WHERE workflow_name = $1 AND idempotency_key = $2",
        )
        .bind::<diesel::sql_types::Text, _>(wf)
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<R>(&mut conn)
        .await
        .expect("pointer")
        .workflow_exec_id
    };
    assert_eq!(pointed, e2.as_uuid());
}
