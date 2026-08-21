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
    }
    let snap_sql = "SELECT \
        (SELECT COUNT(*) FROM harvest_workflow_executions) AS execs, \
        (SELECT COUNT(*) FROM harvest_task_queue) AS tasks, \
        (SELECT COUNT(*) FROM harvest_events) AS events, \
        (SELECT COALESCE(string_agg(state || ':' || COALESCE(worker_id, '-'), ',' ORDER BY id), '') \
           FROM harvest_task_queue) AS task_states";

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
