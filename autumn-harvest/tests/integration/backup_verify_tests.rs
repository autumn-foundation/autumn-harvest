//! Post-restore resumability verification — DB integration tests (issue #943).
//!
//! These tests are the **correctness oracle** for `backup_verify::verify_restore`.
//! Each one seeds a freshly-migrated database with a deliberately-injected
//! incoherence class, runs the verifier, and asserts the report *detects that
//! exact class* — the issue's success metric ("detects 100% of a seeded set of
//! >= 5 incoherence classes").
//!
//! DB source of truth: `full_migrations_sql()` (the paved-path bundle) applied
//! to a fresh per-test database (when `HARVEST_TEST_DATABASE_URL` points at an
//! admin server) or a throwaway testcontainer (CI default).
#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements
)]

use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::backup_verify::{
    FindingClass, FindingSeverity, ShardTarget, VerifyOptions, VerifyStatus, verify_restore,
};
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn with_db_name(base: &str, db: &str) -> String {
    let (base, query) = base
        .split_once('?')
        .map_or((base, None), |(b, q)| (b, Some(q)));
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    query.map_or_else(
        || format!("{prefix}/{db}"),
        |q| format!("{prefix}/{db}?{q}"),
    )
}

/// Create an isolated, migrated database and return its URL.
async fn setup() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("bkverify_t_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create per-test database");
        let url = with_db_name(&admin_url, &db);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect fresh db");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migrations");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn exec_sql(conn: &mut AsyncPgConnection, sql: &str) {
    conn.batch_execute(sql).await.expect("seed sql");
}

/// Insert a RUNNING workflow execution with the given id on `shard_id`.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    shard_id: i32,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, state, input, started_at, shard_id, queue_name) \
         VALUES ($1, $2, $3, $4, '{}'::jsonb, NOW(), $5, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Text, _>(state)
    .bind::<diesel::sql_types::Integer, _>(shard_id)
    .execute(conn)
    .await
    .expect("seed execution");
}

async fn append_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_id: i32,
    event_type: &str,
    data: serde_json::Value,
) {
    let payload = json!({ "type": event_type, "data": data });
    diesel::sql_query(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ($1, $2, $3, $4, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(event_id)
    .bind::<diesel::sql_types::Text, _>(event_type)
    .bind::<diesel::sql_types::Jsonb, _>(payload)
    .execute(conn)
    .await
    .expect("append event");
}

fn one_shard(url: &str) -> Vec<ShardTarget> {
    vec![ShardTarget::new(0, url)]
}

fn opts() -> VerifyOptions {
    VerifyOptions::default().with_scratch_ack(true)
}

// ─────────────────────────── incoherence classes ───────────────────────────

/// Class 1: a RUNNING task row whose `worker_id` has no live heartbeat — the
/// canonical, EXPECTED post-restore artifact. Reclaimable, not a failure.
#[tokio::test]
async fn detects_dead_worker_running_task_as_reclaimable_not_a_failure() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "order_flow", "o-1", "RUNNING", 0).await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
             (id, workflow_exec_id, task_type, queue_name, state, worker_id, started_at, scheduled_at, input) \
             VALUES (gen_random_uuid(), '{}', 'workflow', 'default', 'RUNNING', 'dead-worker-1', NOW(), NOW(), '{{}}'::jsonb)",
            exec.as_uuid()
        ),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::DeadWorkerRunningTask),
        "expected dead-worker RUNNING task to be detected: {report:#?}"
    );
    assert_eq!(
        report.status,
        VerifyStatus::ResumableWithReclaim,
        "a dead-worker row is the NORMAL post-restore state and must not fail the drill"
    );
    assert_eq!(report.exit_code(), 0, "reclaimable must exit 0");
}

/// Class 2: a task-queue row whose owning execution row does not exist — a torn
/// restore. Incoherent: workers must NOT be started.
///
/// The live schema carries an FK from `harvest_task_queue.workflow_exec_id`, so
/// this state is only reachable when the restore path bypassed constraint
/// enforcement — which `pg_restore --data-only --disable-triggers` and
/// `COPY`-based restores routinely do. The test reproduces exactly that: drop
/// the constraint, load the row, and leave the constraint off (a restore that
/// forgot to re-validate). That is *why* the probe is worth running even though
/// a healthy schema forbids the state.
#[tokio::test]
async fn detects_dangling_task_execution_as_incoherent() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let orphan = Uuid::new_v4();
    exec_sql(
        &mut conn,
        "ALTER TABLE harvest_task_queue DROP CONSTRAINT harvest_task_queue_workflow_exec_id_fkey",
    )
    .await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
             (id, workflow_exec_id, task_type, queue_name, state, scheduled_at, input) \
             VALUES (gen_random_uuid(), '{orphan}', 'workflow', 'default', 'PENDING', NOW(), '{{}}'::jsonb)"
        ),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::DanglingTaskExecution),
        "expected dangling task->execution reference: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
    assert_eq!(report.exit_code(), 1, "incoherent must exit nonzero");
}

/// Class 3: an external signal/cancel request with no recorded terminal and a
/// target execution that does not exist on the shard that owns it.
#[tokio::test]
async fn detects_external_target_missing_as_incoherent() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, caller, "supervisor", "s-1", "RUNNING", 0).await;
    // NOTE: target execution is deliberately NOT seeded.

    append_event(
        &mut conn,
        caller,
        1,
        "WorkflowStarted",
        json!({ "input": {} }),
    )
    .await;
    append_event(
        &mut conn,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": Uuid::new_v4(),
            "target": target.to_string(),
            "signal_name": "go",
            "payload": {}
        }),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExternalTargetMissing),
        "expected missing external target: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// Class 4: an expired session lease (#606) — reclaimable by the session sweep.
#[tokio::test]
async fn detects_expired_session_lease_as_reclaimable() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "pipeline", "p-1", "RUNNING", 0).await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_sessions \
             (id, workflow_exec_id, host_worker_id, queue_name, state, created_at, expires_at) \
             VALUES (gen_random_uuid(), '{}', 'gone-worker', 'gpu', 'ACTIVE', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '5 minutes')",
            exec.as_uuid()
        ),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExpiredSessionLease),
        "expected expired session lease: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::ResumableWithReclaim);
}

/// Class 5: an INFLIGHT completion delivery (#605) — re-attempted after its
/// lease lapses; the receiver must dedupe on `delivery_id`.
#[tokio::test]
async fn detects_inflight_completion_delivery_as_reclaimable() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "order_flow", "o-9", "COMPLETED", 0).await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_completion_deliveries \
             (id, workflow_exec_id, shard_id, callback_index, workflow_name, workflow_id, \
              target_url, event_filter, terminal_state, payload, state, attempt, max_attempts, \
              retry_policy, next_attempt_at) \
             VALUES (gen_random_uuid(), '{}', 0, 0, 'order_flow', 'o-9', \
              'https://example.test/hook', '{{}}'::jsonb, 'completed', '{{}}'::jsonb, \
              'INFLIGHT', 1, 10, '{{}}'::jsonb, NOW() - INTERVAL '1 minute')",
            exec.as_uuid()
        ),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::InflightCompletionDelivery),
        "expected inflight completion delivery: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::ResumableWithReclaim);
}

/// A pristine restore with nothing in flight reports Clean and exits 0.
#[tokio::test]
async fn a_pristine_restore_is_clean() {
    let (url, _c) = setup().await;
    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;
    assert_eq!(report.status, VerifyStatus::Clean, "{report:#?}");
    assert_eq!(report.exit_code(), 0);
}

/// AC4: the verifier is strictly read-only — running it leaves the database
/// byte-identical.
#[tokio::test]
async fn verify_never_mutates_the_database() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "order_flow", "o-2", "RUNNING", 0).await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
             (id, workflow_exec_id, task_type, queue_name, state, worker_id, started_at, scheduled_at, input) \
             VALUES (gen_random_uuid(), '{}', 'workflow', 'default', 'RUNNING', 'dead-worker-2', NOW(), NOW(), '{{}}'::jsonb)",
            exec.as_uuid()
        ),
    )
    .await;

    // Seed every table a probe reads, so "untouched" is a claim about the
    // tables verify actually queries -- not a claim about three of them while
    // six went unexercised.
    let sched = uuid::Uuid::new_v4();
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_schedules \
             (id, workflow_name, schedule_expr, queue_name, workflow_input, is_paused, \
              fire_claim_token, fire_claimed_until) \
             VALUES ('{sched}', 'order_flow', '@every 60s', 'default', '{{}}'::jsonb, false, \
                     gen_random_uuid(), NOW() - INTERVAL '1 hour')"
        ),
    )
    .await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_sessions \
             (id, workflow_exec_id, host_worker_id, queue_name, state, expires_at) \
             VALUES (gen_random_uuid(), '{}', 'dead-worker-2', 'default', 'ACTIVE', \
                     NOW() - INTERVAL '1 hour')",
            exec.as_uuid()
        ),
    )
    .await;
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_completion_deliveries \
             (id, workflow_exec_id, shard_id, callback_index, workflow_name, workflow_id, \
              target_url, event_filter, terminal_state, payload, state, attempt, \
              max_attempts, retry_policy, next_attempt_at) \
             VALUES (gen_random_uuid(), '{}', 0, 0, 'order_flow', 'o-2', \
                     'https://example.invalid/hook', '{{}}'::jsonb, 'completed', \
                     '{{}}'::jsonb, 'INFLIGHT', 1, 10, '{{}}'::jsonb, NOW())",
            exec.as_uuid()
        ),
    )
    .await;

    #[derive(diesel::QueryableByName)]
    struct Snap {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        execs: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        tasks: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        events: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        task_states: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        exec_states: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        schedule_claims: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        session_states: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        delivery_states: String,
    }
    // `state` is the field every scanner would MUTATE, so a snapshot that
    // counts rows but never captures state cannot catch the regression this
    // test exists for: a probe that reclaims instead of reporting.
    let snap_sql = "SELECT \
        (SELECT COUNT(*) FROM harvest_workflow_executions) AS execs, \
        (SELECT COUNT(*) FROM harvest_task_queue) AS tasks, \
        (SELECT COUNT(*) FROM harvest_events) AS events, \
        (SELECT COALESCE(string_agg(state || ':' || COALESCE(worker_id, '-'), ',' ORDER BY id), '') \
           FROM harvest_task_queue) AS task_states, \
        (SELECT COALESCE(string_agg(state, ',' ORDER BY id), '') \
           FROM harvest_workflow_executions) AS exec_states, \
        (SELECT COALESCE(string_agg( \
             COALESCE(fire_claim_token::text, '-') || ':' || \
             COALESCE(fire_claimed_until::text, '-'), ',' ORDER BY id), '') \
           FROM harvest_schedules) AS schedule_claims, \
        (SELECT COALESCE(string_agg(state, ',' ORDER BY id), '') \
           FROM harvest_sessions) AS session_states, \
        (SELECT COALESCE(string_agg(state || ':' || attempt::text, ',' ORDER BY id), '') \
           FROM harvest_completion_deliveries) AS delivery_states";

    let before: Snap = diesel::sql_query(snap_sql)
        .get_result(&mut conn)
        .await
        .expect("before snapshot");

    let _ = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    let after: Snap = diesel::sql_query(snap_sql)
        .get_result(&mut conn)
        .await
        .expect("after snapshot");

    assert_eq!(
        before.execs, after.execs,
        "execution rows must be untouched"
    );
    assert_eq!(before.tasks, after.tasks, "task rows must be untouched");
    assert_eq!(before.events, after.events, "event rows must be untouched");
    assert_eq!(
        before.task_states, after.task_states,
        "the verifier must never reclaim, only report"
    );
    assert_eq!(
        before.exec_states, after.exec_states,
        "no execution may be sealed TIMED_OUT by a read-only drill"
    );
    assert_eq!(
        before.schedule_claims, after.schedule_claims,
        "no schedule claim may be stolen by a read-only drill"
    );
    assert_eq!(
        before.session_states, after.session_states,
        "no session may be marked BROKEN by a read-only drill"
    );
    assert_eq!(
        before.delivery_states, after.delivery_states,
        "no completion delivery may be re-attempted by a read-only drill"
    );
    // And the run must have genuinely LOOKED -- an all-clean report here would
    // mean the assertions above passed vacuously.
    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;
    assert!(
        report.all_findings().count() > 0,
        "the seeded fixtures must have produced findings; otherwise \
         `untouched` proves nothing: {report:?}"
    );
}

/// AC4, mechanically: the read-only session pin is a *server*-enforced
/// guarantee, not a promise about our own SQL. Prove Postgres rejects a write
/// with SQLSTATE 25006 once the pin the verifier issues is in effect.
#[tokio::test]
async fn the_read_only_session_pin_makes_postgres_reject_writes() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    diesel::sql_query(autumn_harvest::backup_verify::READ_ONLY_SESSION_SQL)
        .execute(&mut conn)
        .await
        .expect("pin the session read-only");

    let err = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, state, input) \
         VALUES (gen_random_uuid(), 'x', 'x', 'RUNNING', '{}'::jsonb)",
    )
    .execute(&mut conn)
    .await
    .expect_err("a write on a read-only session must be refused by the server");

    let msg = err.to_string();
    assert!(
        msg.contains("read-only transaction"),
        "expected SQLSTATE 25006 (read-only transaction), got: {msg}"
    );
}

/// AC3: cross-shard skew — a parent's recorded child terminal is missing on the
/// child's own shard (that shard was restored to an earlier point).
#[tokio::test]
async fn detects_cross_shard_child_terminal_rolled_back() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let parent = ExecutionId::new_for_shard(ShardId::new(0));
    let child = ExecutionId::new_for_shard(ShardId::new(1));

    seed_execution(&mut a, parent, "parent_flow", "pf-1", "RUNNING", 0).await;
    append_event(&mut a, parent, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        parent,
        2,
        "ChildWorkflowStarted",
        json!({ "child_id": child.to_string(), "workflow_name": "child_flow", "input": {} }),
    )
    .await;
    // The parent believes the child COMPLETED...
    append_event(
        &mut a,
        parent,
        3,
        "ChildWorkflowCompleted",
        json!({ "child_id": child.to_string(), "output": {} }),
    )
    .await;

    // ...but shard 1 was restored to an earlier point where it is still RUNNING.
    seed_execution(&mut b, child, "child_flow", "cf-1", "RUNNING", 1).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ChildTerminalRolledBack),
        "expected rolled-back child terminal: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// The success metric names "cross-shard missing child" explicitly, and it is a
/// DIFFERENT invariant from a rolled-back terminal: the parent is still awaiting
/// a child whose execution row is simply absent from the shard that owns it.
#[tokio::test]
async fn detects_cross_shard_child_execution_missing() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;

    let parent = ExecutionId::new_for_shard(ShardId::new(0));
    let child = ExecutionId::new_for_shard(ShardId::new(1));

    seed_execution(&mut a, parent, "parent_flow", "pf-2", "RUNNING", 0).await;
    append_event(&mut a, parent, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        parent,
        2,
        "ChildWorkflowStarted",
        json!({ "child_id": child.to_string(), "workflow_name": "child_flow", "input": {} }),
    )
    .await;
    // No terminal recorded: the parent is still awaiting the child. Shard 1 is
    // restored to a point BEFORE the child was ever created, so it has no row.

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ChildExecutionMissing),
        "an awaited child absent from its own shard is incoherent: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// A torn claim pair (`fire_claim_token` set, `fire_claimed_until` NULL) is
/// PERMANENTLY wedged, not merely expired: the scheduler's claim predicate is
/// `fire_claim_token IS NULL OR fire_claimed_until < NOW()`, which such a row
/// satisfies neither half of. Reporting it as reclaimable would tell an
/// operator to start workers on a schedule that will never fire again.
#[tokio::test]
async fn a_torn_schedule_claim_is_incoherent_not_reclaimable() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;
    exec_sql(
        &mut conn,
        "INSERT INTO harvest_schedules \
         (id, workflow_name, schedule_expr, queue_name, workflow_input, is_paused, \
          fire_claim_token, fire_claimed_until) \
         VALUES (gen_random_uuid(), 'order_flow', '@every 60s', 'default', '{}'::jsonb, false, \
                 gen_random_uuid(), NULL)",
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;
    assert!(
        report.detected(FindingClass::WedgedScheduleClaim),
        "a torn claim pair must be detected: {report:#?}"
    );
    assert!(
        !report.detected(FindingClass::ExpiredScheduleClaim),
        "a NULL fire_claimed_until is not an EXPIRED claim: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// The session reclaim scanner does NOT break a session whose only qualifying
/// reason is an elapsed lease while a member task is still `RUNNING`. Mirror
/// that, or the report over-reports work the fleet will never actually do.
#[tokio::test]
async fn an_expired_lease_with_a_running_member_and_a_live_host_is_not_reported() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "order_flow", "o-live", "RUNNING", 0).await;
    // A genuinely LIVE host: fresh heartbeat, Active.
    exec_sql(
        &mut conn,
        "INSERT INTO harvest_workers \
         (worker_id, queues, shard_assignments, max_concurrency, in_flight_count, \
          status, last_heartbeat_at, started_at, host) \
         VALUES ('live-worker', '[\"default\"]'::jsonb, '[0]'::jsonb, 4, 0, \
                 'Active', NOW(), NOW(), 'test-host')",
    )
    .await;
    let session = uuid::Uuid::new_v4();
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_sessions \
             (id, workflow_exec_id, host_worker_id, queue_name, state, expires_at) \
             VALUES ('{session}', '{}', 'live-worker', 'default', 'ACTIVE', \
                     NOW() - INTERVAL '1 hour')",
            exec.as_uuid()
        ),
    )
    .await;
    // ...and a member still in flight: independent proof of progress.
    exec_sql(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
             (id, workflow_exec_id, task_type, queue_name, state, worker_id, started_at, \
              scheduled_at, input, session_id) \
             VALUES (gen_random_uuid(), '{}', 'activity', 'default', 'RUNNING', 'live-worker', \
                     NOW(), NOW(), '{{}}'::jsonb, '{session}')",
            exec.as_uuid()
        ),
    )
    .await;

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;
    assert!(
        !report.detected(FindingClass::ExpiredSessionLease),
        "the scanner suppresses this case; the report must too: {report:#?}"
    );
}

/// An unreachable shard must report `Unavailable` (exit 2) — never a false
/// "clean", because coherence could not be determined.
#[tokio::test]
async fn an_unreachable_shard_is_undetermined_not_clean() {
    let (url, _c) = setup().await;
    let targets = vec![
        ShardTarget::new(0, &url),
        ShardTarget::new(1, "postgres://nobody@127.0.0.1:1/does_not_exist"),
    ];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;
    assert_eq!(report.status, VerifyStatus::Unavailable, "{report:#?}");
    assert_eq!(report.exit_code(), 2);
}

/// The most dangerous possible false-clean: a "restore" that produced an
/// **unmigrated** (empty) database. Every probe errors on a missing table, so a
/// naive report finds nothing wrong — and would tell an operator to start
/// workers against a database with no harvest schema at all.
///
/// Must report `Unavailable` (exit 2), never a pass.
#[tokio::test]
async fn an_unmigrated_restore_is_undetermined_never_a_pass() {
    // Deliberately does NOT run `full_migrations_sql()`.
    let admin = std::env::var("HARVEST_TEST_DATABASE_URL").ok();
    let (url, _c) = if let Some(admin_url) = admin {
        let mut admin_conn = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
        let db = format!("bkverify_bare_{}_{}", std::process::id(), n);
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin_conn)
            .await
            .expect("create bare database");
        (with_db_name(&admin_url, &db), None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres start");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };

    let report = verify_restore(&one_shard(&url), &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ProbeFailed),
        "an unmigrated database must be reported as un-probeable: {report:#?}"
    );
    assert_eq!(
        report.status,
        VerifyStatus::Unavailable,
        "a database with no harvest schema must NEVER read as a pass"
    );
    assert_eq!(report.exit_code(), 2, "undetermined must exit 2, not 0");
    assert_eq!(
        FindingClass::ProbeFailed.severity(),
        FindingSeverity::Undetermined
    );
}

// ---------------------------------------------------------------------------
// Codex round 1 regressions
// ---------------------------------------------------------------------------

/// Codex round 1, P1. A caller that recorded `ExternalCancelDelivered` asserts
/// the target is terminal. If the target's shard was restored to a point where
/// it is still RUNNING, resuming silently loses an effect the caller believes
/// already happened — the external-request analogue of a rolled-back child
/// terminal. Before the fix a *resolved* request was dropped from adjudication
/// entirely and the drill exited 0.
#[tokio::test]
async fn detects_delivered_external_cancel_rolled_back_on_the_target_shard() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let cancel_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-1", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalCancelRequested",
        json!({ "cancel_id": cancel_id.to_string(), "target": target.to_string() }),
    )
    .await;
    // The caller believes the cancel LANDED.
    append_event(
        &mut a,
        caller,
        3,
        "ExternalCancelDelivered",
        json!({ "cancel_id": cancel_id.to_string() }),
    )
    .await;

    // ...but shard 1 was restored to before the cancellation.
    seed_execution(&mut b, target, "fulfillment", "ff-1", "RUNNING", 1).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExternalEffectRolledBack),
        "expected a lost external cancel effect: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// The mirror of the test above: the same delivered cancel against a target
/// that IS terminal is coherent and must produce no finding. Without this
/// control, "flag every delivered request" would pass the test above.
#[tokio::test]
async fn a_delivered_external_cancel_whose_target_is_terminal_is_clean() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let cancel_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-2", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalCancelRequested",
        json!({ "cancel_id": cancel_id.to_string(), "target": target.to_string() }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalCancelDelivered",
        json!({ "cancel_id": cancel_id.to_string() }),
    )
    .await;

    seed_execution(&mut b, target, "fulfillment", "ff-2", "CANCELLED", 1).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "a delivered cancel against a terminal target is coherent: {report:#?}"
    );
}

/// A *failed* external terminal applied no effect, so there is nothing to
/// adjudicate on the target shard and no finding may be raised — the fix must
/// not turn every resolved request into a check.
#[tokio::test]
async fn a_failed_external_request_asserts_no_effect_on_the_target() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let cancel_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-3", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalCancelRequested",
        json!({ "cancel_id": cancel_id.to_string(), "target": target.to_string() }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalCancelFailed",
        json!({ "cancel_id": cancel_id.to_string(), "reason_code": "target_unknown" }),
    )
    .await;

    // Target still RUNNING -- which for a FAILED cancel is entirely expected.
    seed_execution(&mut b, target, "fulfillment", "ff-3", "RUNNING", 1).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "a failed request applied no effect: {report:#?}"
    );
    assert!(
        !report.detected(FindingClass::PendingExternalRequest),
        "a failed request is not pending either: {report:#?}"
    );
}

/// A delivered SIGNAL asserts a `harvest_signals` row or a `SignalReceived`
/// event on the target. Neither present means the target was restored to
/// before the delivery.
#[tokio::test]
async fn detects_delivered_external_signal_rolled_back_on_the_target_shard() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-4", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "approve",
            "payload": {}
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    // Target exists but carries no trace of the signal.
    seed_execution(&mut b, target, "review", "rv-1", "RUNNING", 1).await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExternalEffectRolledBack),
        "expected a lost external signal effect: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// The control for the signal case: a queued `harvest_signals` row on the
/// target is exactly the durable trace the delivery asserts, so it is clean.
#[tokio::test]
async fn a_delivered_external_signal_with_a_queued_row_is_clean() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-5", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "approve",
            "payload": {}
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    seed_execution(&mut b, target, "review", "rv-2", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
             VALUES (gen_random_uuid(), '{target}', 'approve', '{{}}'::jsonb, NOW(), false)"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "a queued signal row is the trace the delivery asserts: {report:#?}"
    );
}

/// Codex round 1, P2. An undecodable child/external `event_data` row may be the
/// very reference that would have exposed a missing target, so dropping it can
/// turn an incoherent restore into a resumable verdict. Fail closed: the whole
/// cross-shard check becomes Undetermined.
#[tokio::test]
async fn an_undecodable_reference_event_is_undetermined_not_silently_dropped() {
    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let owner = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, owner, "parent_flow", "pf-x", "RUNNING", 0).await;
    append_event(
        &mut conn,
        owner,
        1,
        "WorkflowStarted",
        json!({ "input": {} }),
    )
    .await;
    // Selected by the reference scan (matching `event_type`) but structurally
    // undecodable into a `WorkflowEvent`.
    append_event(
        &mut conn,
        owner,
        2,
        "ChildWorkflowStarted",
        json!({ "not_a_child_id": 42 }),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ProbeFailed),
        "an undecodable reference event must fail closed: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Unavailable);
}

/// Codex round 1, P1 — the *mapping*, driven end to end.
///
/// A registered handler that returns `Err` against a NON-terminal recorded
/// history is not a clean replay: the history carries no terminal failure, so
/// the deployed code is erroring where the live run had not, and resuming
/// fails the run immediately. Before the fix the status was collapsed to a
/// divergent/clean bool, so this counted as `clean` and the drill exited 0.
#[tokio::test]
async fn a_replayed_workflow_failure_is_not_counted_clean() {
    use std::future::Future;
    use std::pin::Pin;

    fn always_errors<'a>(
        _ctx: &'a autumn_harvest::WorkflowContext,
        _input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move { Err("the redeployed handler rejects this input".to_string()) })
    }

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "redeployed_flow", "rf-1", "RUNNING", 0).await;
    // `timestamp` is REQUIRED by the real `WorkflowStarted` variant. The other
    // tests in this file omit it because they register no handler and so never
    // decode; these two actually replay, so the event must be genuine or the
    // assertions below pass vacuously on an `unreadable` history.
    append_event(
        &mut conn,
        exec,
        1,
        "WorkflowStarted",
        json!({ "input": {}, "timestamp": chrono::Utc::now().to_rfc3339() }),
    )
    .await;

    let replayer = WorkflowReplayer::new().register_fn("redeployed_flow", always_errors);
    let report = verify_restore(&one_shard(&url), &opts(), &replayer).await;

    assert!(
        report.replay_verified,
        "the handler IS registered, so replay really ran: {report:#?}"
    );
    assert_eq!(
        report.replay.failed, 1,
        "a workflow error must be counted as a replay FAILURE: {report:#?}"
    );
    assert_eq!(
        report.replay.clean, 0,
        "and must NOT be folded into `clean`: {report:#?}"
    );
    assert!(
        report.detected(FindingClass::ReplayWorkflowFailed),
        "expected a ReplayWorkflowFailed finding: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
    assert_eq!(report.exit_code(), 1, "must not exit 0");
}

/// The control: the same registered handler succeeding is genuinely clean, so
/// "always report a failure" would not pass the test above.
#[tokio::test]
async fn a_replayed_workflow_that_succeeds_is_still_clean() {
    use std::future::Future;
    use std::pin::Pin;

    fn always_ok<'a>(
        _ctx: &'a autumn_harvest::WorkflowContext,
        _input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(serde_json::Value::Null) })
    }

    let (url, _c) = setup().await;
    let mut conn = connect(&url).await;

    let exec = ExecutionId::new_for_shard(ShardId::new(0));
    seed_execution(&mut conn, exec, "healthy_flow", "hf-1", "RUNNING", 0).await;
    append_event(
        &mut conn,
        exec,
        1,
        "WorkflowStarted",
        json!({ "input": {}, "timestamp": chrono::Utc::now().to_rfc3339() }),
    )
    .await;

    let replayer = WorkflowReplayer::new().register_fn("healthy_flow", always_ok);
    let report = verify_restore(&one_shard(&url), &opts(), &replayer).await;

    assert_eq!(report.replay.failed, 0, "{report:#?}");
    assert!(
        !report.detected(FindingClass::ReplayWorkflowFailed),
        "{report:#?}"
    );
}

// ─────────────── Codex round 2: signal effect identity + chain ───────────────

/// Codex round 2, P1. Continue-as-new REASSIGNS unconsumed `harvest_signals`
/// rows to the successor (`worker.rs`), and an unconsumed signal produces no
/// `SignalReceived` event — so after the transition the ORIGINAL target row
/// carries no row and no event. Checking the target alone reports a perfectly
/// healthy restore as rolled back and exits 1, telling an operator not to start
/// workers. Follow the successor chain.
#[tokio::test]
async fn a_signal_forwarded_by_continue_as_new_is_found_on_the_successor() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let successor = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-can", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "approve",
            "payload": {}
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    // The target continued-as-new and the engine moved the still-unconsumed
    // signal row onto the successor. The target itself is left with nothing.
    seed_execution(&mut b, target, "review", "rv-can", "CONTINUED_AS_NEW", 1).await;
    seed_execution(&mut b, successor, "review", "rv-can-2", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "UPDATE harvest_workflow_executions \
             SET continued_from_exec_id = '{target}' WHERE id = '{successor}'"
        ),
    )
    .await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
             VALUES (gen_random_uuid(), '{successor}', 'approve', '{{}}'::jsonb, NOW(), false)"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "the signal was forwarded to the continue-as-new successor, not lost: {report:#?}"
    );
    assert_ne!(
        report.status,
        VerifyStatus::Incoherent,
        "a healthy restore must not be reported incoherent: {report:#?}"
    );
}

/// The retry path is BROADER than continue-as-new: `forward_signals_to_retry_attempt`
/// (issue #843) moves the WHOLE mailbox and re-arms `consumed`, so the original
/// target is left with zero rows even for signals it had already consumed.
#[tokio::test]
async fn a_signal_forwarded_to_a_retry_attempt_is_found_on_the_successor() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let attempt2 = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-retry", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "approve",
            "payload": {}
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    seed_execution(&mut b, target, "review", "rv-retry", "FAILED", 1).await;
    seed_execution(&mut b, attempt2, "review", "rv-retry-2", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "UPDATE harvest_workflow_executions \
             SET retry_of_exec_id = '{target}' WHERE id = '{attempt2}'"
        ),
    )
    .await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
             VALUES (gen_random_uuid(), '{attempt2}', 'approve', '{{}}'::jsonb, NOW(), false)"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "the mailbox was forwarded to the retry attempt, not lost: {report:#?}"
    );
    assert_ne!(report.status, VerifyStatus::Incoherent);
}

/// Codex round 2, P1. A repeated channel name made the name-level EXISTS match
/// an OLDER delivery, so a rollback that dropped only the newer one read clean.
/// With an `idempotency_key` the delivery has a real identity, so the check is
/// exact: a target carrying only the earlier key is definitively rolled back.
#[tokio::test]
async fn a_keyed_signal_is_matched_exactly_not_by_channel_name() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-key", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "tick",
            "payload": {},
            "idempotency_key": "tick-2"
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    // The target kept the FIRST "tick" but the restore dropped the second.
    // A name-level check would match `tick-1` and wrongly report clean.
    seed_execution(&mut b, target, "review", "rv-key", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed, idempotency_key) \
             VALUES (gen_random_uuid(), '{target}', 'tick', '{{}}'::jsonb, NOW(), true, 'tick-1')"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExternalEffectRolledBack),
        "the keyed delivery tick-2 is absent; matching tick-1 by name is a false clean: {report:#?}"
    );
    assert_eq!(report.status, VerifyStatus::Incoherent);
}

/// The control for the keyed case: the exact key is present, so the check is
/// definitive and the report is clean — no advisory, no finding.
#[tokio::test]
async fn a_keyed_signal_whose_exact_key_is_present_is_definitively_clean() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-key-ok", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "tick",
            "payload": {},
            "idempotency_key": "tick-2"
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    seed_execution(&mut b, target, "review", "rv-key-ok", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed, idempotency_key) \
             VALUES (gen_random_uuid(), '{target}', 'tick', '{{}}'::jsonb, NOW(), true, 'tick-2')"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "the exact key is present: {report:#?}"
    );
    assert!(
        !report.detected(FindingClass::ExternalEffectUnverifiable),
        "a keyed delivery is exactly checkable, never merely unverifiable: {report:#?}"
    );
}

/// An UNKEYED delivery whose channel name is present cannot be proven to be
/// THIS delivery — the engine persists no per-delivery identity for it. That is
/// a permanent precision limit of the data model, so it is surfaced as an
/// advisory: honest, but never escalating a healthy restore to exit 1.
#[tokio::test]
async fn an_unkeyed_signal_with_name_evidence_is_advisory_not_clean_and_not_incoherent() {
    let (url_a, _ca) = setup().await;
    let (url_b, _cb) = setup().await;
    let mut a = connect(&url_a).await;
    let mut b = connect(&url_b).await;

    let caller = ExecutionId::new_for_shard(ShardId::new(0));
    let target = ExecutionId::new_for_shard(ShardId::new(1));
    let signal_id = uuid::Uuid::new_v4();

    seed_execution(&mut a, caller, "supervisor", "sup-unkeyed", "RUNNING", 0).await;
    append_event(&mut a, caller, 1, "WorkflowStarted", json!({ "input": {} })).await;
    append_event(
        &mut a,
        caller,
        2,
        "ExternalSignalRequested",
        json!({
            "signal_id": signal_id.to_string(),
            "target": target.to_string(),
            "signal_name": "approve",
            "payload": {}
        }),
    )
    .await;
    append_event(
        &mut a,
        caller,
        3,
        "ExternalSignalDelivered",
        json!({ "signal_id": signal_id.to_string() }),
    )
    .await;

    seed_execution(&mut b, target, "review", "rv-unkeyed", "RUNNING", 1).await;
    exec_sql(
        &mut b,
        &format!(
            "INSERT INTO harvest_signals \
             (id, workflow_exec_id, signal_name, payload, received_at, consumed) \
             VALUES (gen_random_uuid(), '{target}', 'approve', '{{}}'::jsonb, NOW(), false)"
        ),
    )
    .await;

    let targets = vec![ShardTarget::new(0, &url_a), ShardTarget::new(1, &url_b)];
    let report = verify_restore(&targets, &opts(), &WorkflowReplayer::new()).await;

    assert!(
        report.detected(FindingClass::ExternalEffectUnverifiable),
        "an unkeyed delivery is not provable from a name match: {report:#?}"
    );
    assert!(
        !report.detected(FindingClass::ExternalEffectRolledBack),
        "name evidence exists, so this is not a definitive rollback: {report:#?}"
    );
    assert_ne!(
        report.status,
        VerifyStatus::Incoherent,
        "an advisory must never escalate the verdict: {report:#?}"
    );
}
