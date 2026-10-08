#![cfg(feature = "db")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
//! Transactional workflow start — issue #763.
//!
//! Proves `WorkflowHandleClient::start_workflow_transactional` closes the
//! dual-write gap between an embedding app's own domain write and starting a
//! workflow to react to it: the `WorkflowStarted` event, the execution row,
//! and the initial dispatchable task-queue row all live only inside whatever
//! transaction is open on the caller's connection, so they commit or roll
//! back together with the caller's own domain write.
//!
//! Structure, cheapest-and-most-decisive first:
//!   1. Atomicity (AC2) — commit makes everything visible; rollback leaves
//!      nothing, including the fault-injection sweep (AC5) and a dedicated
//!      test for the two-phase `TerminateExisting` pre-check-cancel
//!      interaction with an *outer* rollback.
//!   2. Start-semantics parity (AC3) — `WorkflowInfo` defaults (execution
//!      timeout, SLA, concurrency key — the issue text names all three
//!      explicitly), id-reuse / conflict policy, idempotency-key dedup
//!      (including its interaction with a rollback), input-schema validation
//!      aborting without writing, the debounce/batch/throttle rejection (all
//!      three disjuncts, independently), and the admission-gate (issue #618)
//!      interaction.
//!   3. Sharding (AC4) — lands on the shard backing the connection, and a
//!      single-shard deployment needs no `.with_shard(...)` call at all.
//!   4. Connection discipline — what happens when the caller passes a bare
//!      (non-transaction-wrapped) connection, since the atomicity guarantee
//!      is entirely dependent on the caller actually opening a transaction.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (each test creates its own fresh, uniquely-named
//! database); otherwise a fresh testcontainers Postgres is booted per test
//! with the full migration bundle.
//!
//! `--test-threads=1` (see `docs/transactional-start.md`'s "Testing" section
//! for the exact run command) is a *hard requirement*, not caution: every
//! test in this file builds its own `WorkflowHandleClient`/`ShardedDbPool`,
//! and `ShardedDbPool::single()`/`::from_map()` write to the process-wide
//! `GLOBAL_SHARDED_POOL`/`GLOBAL_SHARD_ROUTER` statics as a side effect of
//! construction (read by `DeferredTriggerStart::spawn()`, used from
//! `TransactionalStartOutcome::finish()`, and by background scanners).
//! Running this file's tests in parallel within one process would have them
//! clobber each other's global pool/router registration. The admission-gate
//! test below additionally arms the process-global `GLOBAL_ADMISSION_GATE_CACHE`
//! for its duration — safe under `--test-threads=1` because it always
//! disarms the gate before returning, matching every other gate-mutating
//! test in this crate.

use autumn_harvest::admission_gate::{
    AdmissionGate, AdmissionGateCache, AdmissionGateId, GateScope, set_global_admission_gate_cache,
};
use autumn_harvest::event_batch::BatchPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::types::{
    ExecutionId, ShardId, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{TransactionalStartOptions, TransactionalStartOutcome};

use chrono::Utc;
use diesel::sql_types::{BigInt, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixture workflows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    amount_cents: i64,
}

/// Carries a `WorkflowInfo`-declared execution timeout, SLA, and a per-key
/// concurrency policy so the transactional-start default-resolution path
/// (AC3) has something real to resolve. The real issue #763 text names all
/// three (`execution_timeout`, `sla`, concurrency key) explicitly — `sla`
/// was a coverage gap flagged in review (C1) until it was declared here.
#[workflow(
    execution_timeout = "1h",
    sla = "30m",
    concurrency(key = "input.order_id", limit = 5)
)]
async fn t763_order_workflow(_ctx: &WorkflowContext, _input: OrderInput) -> Result<(), String> {
    Ok(())
}

/// No declared policies — the "everything defaults" control workflow.
#[workflow]
async fn t763_plain_workflow(_ctx: &WorkflowContext, _input: OrderInput) -> Result<(), String> {
    Ok(())
}

/// Carries a published input schema (issue #373) so the pre-write validation
/// abort path (AC3) is exercisable.
#[workflow]
async fn t763_schema_checked_workflow(
    _ctx: &WorkflowContext,
    _input: OrderInput,
) -> Result<(), String> {
    Ok(())
}

fn order_input_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount_cents": {"type": "integer"}
        },
        "required": ["order_id", "amount_cents"]
    })
}

/// Configured with a debounce policy — must be rejected by the transactional
/// start guard (a deferred admission cannot return an `ExecutionId`
/// synchronously) rather than silently doing the wrong thing.
#[workflow(debounce(key = "input.order_id", window = "10s"))]
async fn t763_debounced_workflow(_ctx: &WorkflowContext, _input: OrderInput) -> Result<(), String> {
    Ok(())
}

/// Configured with a batch policy — one of the three independent disjuncts in
/// `validate_transactional_start_request`'s deferred-admission guard
/// (`debounce.is_some() || batch.is_some() || throttle.is_some()`). Review
/// finding H4: only the `debounce` disjunct had a test; a copy-paste error or
/// refactor silently dropping this or the throttle check would otherwise go
/// undetected.
#[workflow]
async fn t763_batched_workflow(_ctx: &WorkflowContext, _input: OrderInput) -> Result<(), String> {
    Ok(())
}

/// Configured with a throttle policy — the third disjunct of the same guard
/// (see `t763_batched_workflow`'s doc comment, review finding H4).
#[workflow]
async fn t763_throttled_workflow(_ctx: &WorkflowContext, _input: OrderInput) -> Result<(), String> {
    Ok(())
}

/// A dedicated target for the admission-gate behavioral test (review finding
/// M6) — kept structurally isolated from every other fixture so a
/// `GateScope::WorkflowName` gate armed against it can never affect an
/// unrelated test in this suite, even though `--test-threads=1` already
/// serializes every test here (see the module doc comment's "Execution" note
/// below the `--test-threads=1` explanation).
#[workflow]
async fn t763_gate_target_workflow(
    _ctx: &WorkflowContext,
    _input: OrderInput,
) -> Result<(), String> {
    Ok(())
}

fn all_test_workflows() -> Vec<autumn_harvest::info::WorkflowInfo> {
    vec![
        t763_order_workflow_info(),
        t763_plain_workflow_info(),
        t763_schema_checked_workflow_info().with_input_schema_fn(order_input_schema),
        t763_debounced_workflow_info(),
        t763_batched_workflow_info().with_batch(BatchPolicy {
            key_expr: "input.order_id".to_string(),
            max_size: 10,
            max_wait: Duration::from_secs(30),
        }),
        t763_throttled_workflow_info().with_throttle(
            ThrottlePolicy::from_rate_str("10/m", None, Some("input.order_id"), None)
                .expect("valid throttle rate spec"),
        ),
        t763_gate_target_workflow_info(),
    ]
}

// ---------------------------------------------------------------------------
// DB setup helpers (mirrors the established `test_init_sql()` +
// per-test-database convention used throughout this test suite).
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

fn rewrite_pg_db(base: &str, db: &str) -> String {
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let rest = &base[after_scheme..];
    let (authority, tail) = rest
        .find('/')
        .map_or((rest, ""), |i| (&rest[..i], &rest[i + 1..]));
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{}{}/{}{}", &base[..after_scheme], authority, db, query)
}

/// Creates one fresh, uniquely-named database (either off
/// `HARVEST_TEST_DATABASE_URL` or inside a fresh testcontainers Postgres),
/// migrated with the full bundle, plus a throwaway `t763_orders` domain
/// table simulating the embedding app's own write.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest763_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&base_url)
            .await
            .expect("failed to connect to HARVEST_TEST_DATABASE_URL base");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("failed to create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&new_url)
            .await
            .expect("failed to connect to per-test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("failed to apply migrations to per-test database");
        create_domain_table(&mut conn).await;
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to containerized database");
    create_domain_table(&mut conn).await;
    (database_url, Some(container))
}

async fn create_domain_table(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS t763_orders (id TEXT PRIMARY KEY, exec_id UUID NOT NULL)",
    )
    .await
    .expect("failed to create t763_orders domain table");
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("test pool build failed")
}

async fn connect(database_url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client")
}

/// A single-shard client, matching AC4's "single-shard deployment needs no
/// caller changes beyond passing the connection" — `.with_shard(...)` is
/// never called in any test using this helper.
fn single_shard_client(database_url: &str) -> WorkflowHandleClient {
    WorkflowHandleClient::single(build_pool(database_url), database_url.to_string())
        .with_workflows(all_test_workflows())
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count_rows(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .expect("count query")
        .n
}

async fn domain_row_count(conn: &mut AsyncPgConnection, order_id: &str) -> i64 {
    diesel::sql_query("SELECT COUNT(*) AS n FROM t763_orders WHERE id = $1")
        .bind::<Text, _>(order_id)
        .get_result::<CountRow>(conn)
        .await
        .expect("domain row count query")
        .n
}

async fn execution_row_count(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id = $1",
    )
    .bind::<Text, _>(workflow_id)
    .get_result::<CountRow>(conn)
    .await
    .expect("execution row count query")
    .n
}

#[derive(diesel::QueryableByName)]
struct ExecIdRow {
    #[diesel(sql_type = SqlUuid)]
    exec_id: Uuid,
}

/// Reads back the (patched-in) `exec_id` column on the `t763_orders` domain
/// row — used by the domain-first ordering tests to prove the row ends up
/// referencing the REAL exec id, never a leftover placeholder.
async fn order_row_exec_id(conn: &mut AsyncPgConnection, order_id: &str) -> Uuid {
    diesel::sql_query("SELECT exec_id FROM t763_orders WHERE id = $1")
        .bind::<Text, _>(order_id)
        .get_result::<ExecIdRow>(conn)
        .await
        .expect("domain row query")
        .exec_id
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct StateRow {
        #[diesel(sql_type = Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result::<StateRow>(conn)
        .await
        .expect("state query")
        .state
}

/// Reads back the persisted `shard_id` column on `harvest_workflow_executions`
/// — used to prove a start actually PERSISTED the shard it claims to be on
/// (issue #763 Codex review, "Default transactional starts to the actual
/// shard"), since `ExecutionId::shard()` alone only proves what got ENCODED
/// into the id, not what `StartWorkflowParams::shard_id()` chose to write to
/// the row.
async fn persisted_shard_id(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct ShardIdRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        shard_id: i32,
    }
    diesel::sql_query("SELECT shard_id FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result::<ShardIdRow>(conn)
        .await
        .expect("shard_id query")
        .shard_id
}

async fn workflow_started_event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = 'WorkflowStarted'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<CountRow>(conn)
    .await
    .expect("event count query")
    .n
}

/// Counts `WorkflowCancelled` events for an execution — used to prove
/// `execution::inline_cancel` genuinely ran during a `TerminateExisting`
/// collision, even though the row's FINAL `state` column is subsequently
/// overwritten to `CONTINUED_AS_NEW` by the `replace_execution` call that
/// immediately follows it inside the same active-conflict branch (see
/// `finish_after_commit_dispatches_diagnostics_for_a_terminate_existing_collision`).
async fn workflow_cancelled_event_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_type = 'WorkflowCancelled'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<CountRow>(conn)
    .await
    .expect("event count query")
    .n
}

async fn task_queue_row_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<CountRow>(conn)
    .await
    .expect("task queue count query")
    .n
}

/// Reads back the `queue_name` column on the initial dispatchable
/// `harvest_task_queue` row for a transactionally-started workflow — used by
/// the "Carry the default queue on the handle client" review-finding tests
/// below to prove which queue a start with no explicit
/// `TransactionalStartOptions::queue_name` override actually resolved to.
async fn task_queue_row_queue_name(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    #[derive(diesel::QueryableByName)]
    struct QueueNameRow {
        #[diesel(sql_type = Text)]
        queue_name: String,
    }
    diesel::sql_query(
        "SELECT queue_name FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<QueueNameRow>(conn)
    .await
    .expect("task queue row queue_name query")
    .queue_name
}

#[derive(diesel::QueryableByName)]
struct ExecutionColumns {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Interval>)]
    execution_timeout: Option<chrono::Duration>,
    // `sla`/`sla_deadline_at` (issue #487) — the second and third defaults
    // AC3's own issue text names explicitly alongside `execution_timeout`;
    // see `honors_workflow_info_execution_timeout_and_concurrency_key_defaults`
    // below (review finding C1).
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Interval>)]
    sla: Option<chrono::Duration>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    sla_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    start_source: Option<String>,
}

async fn load_execution_columns(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> ExecutionColumns {
    diesel::sql_query(
        "SELECT execution_timeout, sla, sla_deadline_at, start_source \
         FROM harvest_workflow_executions WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<ExecutionColumns>(conn)
    .await
    .expect("execution columns query")
}

/// The per-key concurrency policy (issue #247) is enforced by the initial
/// dispatchable `WORKFLOW` task-queue row (`concurrency_key`/`concurrency_cap`
/// columns, consulted by the claim-time advisory lock) — not by any column on
/// `harvest_workflow_executions` itself.
async fn load_task_concurrency(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> (Option<String>, Option<i32>) {
    #[derive(diesel::QueryableByName)]
    struct TaskConcurrencyColumns {
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        concurrency_key: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
        concurrency_cap: Option<i32>,
    }
    let row = diesel::sql_query(
        "SELECT concurrency_key, concurrency_cap FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .get_result::<TaskConcurrencyColumns>(conn)
    .await
    .expect("task concurrency columns query");
    (row.concurrency_key, row.concurrency_cap)
}

// ---------------------------------------------------------------------------
// 1. Atomicity (AC1/AC2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_makes_workflow_and_domain_row_atomically_visible() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);
    let order_id = "order-commit-1".to_string();

    // `outcome` (not just its `exec_id`) escapes the closure so `finish()` can
    // be called *after* the transaction has actually committed, matching the
    // documented usage contract exactly ("call this only after the caller's
    // outer transaction has committed"). `deferred` is empty for this fixture
    // (a plain fresh start, no `Terminate`-resolving collision), so `finish()`
    // is a documented no-op here; see
    // `finish_after_commit_dispatches_diagnostics_for_a_terminate_existing_collision`
    // for a case where it does real work.
    let outcome: TransactionalStartOutcome = Box::pin(
        conn.transaction::<TransactionalStartOutcome, HarvestError, _>({
            let client = client.clone();
            let order_id = order_id.clone();
            async move |conn| {
                let outcome = client
                    .start_workflow_transactional(
                        conn,
                        "t763_order_workflow",
                        &order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 500}),
                        TransactionalStartOptions::new(),
                    )
                    .await?;
                assert!(outcome.created, "fresh start must report created = true");
                diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
                    .bind::<Text, _>(order_id.clone())
                    .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
                    .execute(conn)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;
                Ok(outcome)
            }
        }),
    )
    .await
    .expect("transaction must commit");
    let exec_id = outcome.exec_id;
    outcome.finish().await;

    // Both the domain row and the full workflow-start footprint (execution
    // row, WorkflowStarted event, and the initial dispatchable task) are
    // visible from a completely fresh connection.
    let mut fresh = connect(&database_url).await;
    assert_eq!(domain_row_count(&mut fresh, &order_id).await, 1);
    assert_eq!(execution_row_count(&mut fresh, &order_id).await, 1);
    assert_eq!(workflow_started_event_count(&mut fresh, exec_id).await, 1);
    assert_eq!(task_queue_row_count(&mut fresh, exec_id).await, 1);

    // The run's recorded provenance (issue #740) is the new `Transactional`
    // source, distinguishing it from a plain HTTP start, a scheduler tick, etc.
    let cols = load_execution_columns(&mut fresh, exec_id).await;
    assert_eq!(
        cols.start_source.as_deref(),
        Some(StartSource::Transactional.as_str())
    );
}

#[tokio::test]
async fn rollback_leaves_neither_domain_row_nor_workflow() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);
    let order_id = "order-rollback-1".to_string();

    let mut observed_exec_id: Option<ExecutionId> = None;
    let result: Result<(), HarvestError> = Box::pin(conn.transaction::<(), HarvestError, _>({
        let client = client.clone();
        let order_id = order_id.clone();
        let observed = &mut observed_exec_id;
        async move |conn| {
            let outcome = client
                .start_workflow_transactional(
                    conn,
                    "t763_order_workflow",
                    &order_id,
                    serde_json::json!({"order_id": order_id, "amount_cents": 500}),
                    TransactionalStartOptions::new(),
                )
                .await?;
            *observed = Some(outcome.exec_id);
            diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
                .bind::<Text, _>(order_id.clone())
                .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            // Simulate a downstream domain-write failure forcing a rollback of
            // everything staged so far, including the workflow start.
            Err(HarvestError::Config("simulated downstream failure".into()))
        }
    }))
    .await;

    assert!(result.is_err(), "the outer transaction must fail");
    let exec_id = observed_exec_id.expect("exec id was assigned before the forced failure");

    let mut fresh = connect(&database_url).await;
    assert_eq!(domain_row_count(&mut fresh, &order_id).await, 0);
    assert_eq!(execution_row_count(&mut fresh, &order_id).await, 0);
    assert_eq!(workflow_started_event_count(&mut fresh, exec_id).await, 0);
    assert_eq!(task_queue_row_count(&mut fresh, exec_id).await, 0);
}

/// Fault-injection sweep (AC5, success metric): randomizes the "crash point"
/// (equivalent, from Postgres's ACID guarantee, to a literal process kill —
/// the client connection drops or issues no COMMIT, so the backend aborts the
/// open transaction exactly as it would for an actual crash) across >= 500
/// iterations, and after each iteration asserts the domain row, the execution
/// row, the `WorkflowStarted` event, AND the initial dispatchable task-queue
/// row are either ALL present or ALL absent — never a partial, orphaned state
/// in any of the four artifacts, in either direction.
///
/// Ordering is deliberately NOT independently randomized here (an earlier
/// revision of this test carried a `domain_write_first` flag that computed a
/// value but never actually took a different code path — a real,
/// since-fixed review finding, H1). The reason a second ordering doesn't
/// belong in *this* loop is structural, not an oversight: `place_order_after`
/// in `docs/transactional-start.md`/`examples/transactional_start_order.rs`
/// — the API's own canonical usage pattern — has the domain write reference
/// `outcome.exec_id` as a foreign key, which can only exist *after* the start
/// call returns it. `atomicity_holds_when_domain_row_is_written_before_the_start`
/// below covers the one *legitimate* alternative ordering (a domain row
/// written first with a placeholder, patched with the real id once the start
/// call returns it) as its own focused, deterministic test rather than folding
/// a synthetic reordering into this randomized loop.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn fault_injection_zero_orphans_across_five_hundred_randomized_crash_points() {
    const ITERATIONS: usize = 500;

    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);

    let mut rng_state: u64 = 0x763_763_763_763;
    let mut next_u64 = move || {
        // xorshift64* — deterministic, seedable, dependency-free.
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    let mut both_present = 0usize;
    let mut both_absent = 0usize;

    for i in 0..ITERATIONS {
        let order_id = format!("order-fault-{i}");
        // 4 crash points: commit (no crash), crash before start, crash after
        // start (before the domain write), crash after the domain write
        // (before commit).
        let crash_point = next_u64() % 4;

        let mut conn = connect(&database_url).await;
        let mut observed_exec_id: Option<ExecutionId> = None;
        // Captures the outcome so `finish()` can be called *after* the
        // transaction resolves — never from inside the still-open closure,
        // matching `TransactionalStartOutcome::finish`'s documented contract
        // exactly ("call this only after the caller's outer transaction has
        // committed"). Left `None` on every crash path (the closure returns
        // `Err` before reaching the point where this is set), so `finish()`
        // is correctly never called for a rolled-back attempt — dropping the
        // outcome there is the documented-safe no-op.
        let mut pending_finish: Option<TransactionalStartOutcome> = None;

        let result: Result<(), HarvestError> = Box::pin(conn.transaction::<(), HarvestError, _>({
            let client = client.clone();
            let order_id = order_id.clone();
            let observed = &mut observed_exec_id;
            let pending = &mut pending_finish;
            async move |conn| {
                if crash_point == 1 {
                    return Err(HarvestError::Config("crash before start".into()));
                }

                let outcome = client
                    .start_workflow_transactional(
                        conn,
                        "t763_order_workflow",
                        &order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await?;
                *observed = Some(outcome.exec_id);
                if crash_point == 2 {
                    return Err(HarvestError::Config("crash after start".into()));
                }
                diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
                    .bind::<Text, _>(order_id.clone())
                    .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
                    .execute(conn)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;
                if crash_point == 3 {
                    return Err(HarvestError::Config("crash after domain write".into()));
                }
                *pending = Some(outcome);
                Ok(())
            }
        }))
        .await;

        // Strictly after the transaction has resolved, and only on the
        // committed path — `pending_finish` is `Some` exactly when
        // `crash_point == 0` (no crash) and the transaction therefore
        // committed.
        if result.is_ok()
            && let Some(outcome) = pending_finish
        {
            outcome.finish().await;
        }

        let mut fresh = connect(&database_url).await;
        let domain_present = domain_row_count(&mut fresh, &order_id).await == 1;
        let workflow_present = execution_row_count(&mut fresh, &order_id).await == 1;
        let (events_present, task_present) = match observed_exec_id {
            Some(exec_id) => (
                workflow_started_event_count(&mut fresh, exec_id).await == 1,
                task_queue_row_count(&mut fresh, exec_id).await == 1,
            ),
            None => (false, false),
        };

        assert_eq!(
            domain_present, workflow_present,
            "iteration {i} (crash_point={crash_point}): \
             domain_present={domain_present} workflow_present={workflow_present} — orphan detected"
        );
        if workflow_present {
            assert!(
                events_present,
                "iteration {i}: workflow row exists without its WorkflowStarted event"
            );
            // AC2/AC5's atomicity promise names the "initial dispatchable
            // task-queue row" as a fourth artifact staged in the same
            // transaction — review finding H2: an earlier revision of this
            // loop watched only the domain row, the execution row, and the
            // WorkflowStarted event, so a future regression that moved task
            // enqueueing outside the transaction boundary would have gone
            // undetected here even though the two simpler one-shot tests
            // above already check it.
            assert!(
                task_present,
                "iteration {i}: workflow row exists without its dispatchable task-queue row"
            );
        }

        if result.is_ok() {
            assert!(
                domain_present && workflow_present,
                "iteration {i}: commit must persist both"
            );
            both_present += 1;
        } else {
            assert!(
                !domain_present && !workflow_present,
                "iteration {i}: rollback must persist neither"
            );
            both_absent += 1;
        }
    }

    assert_eq!(both_present + both_absent, ITERATIONS);
    assert!(
        both_present > 0,
        "sanity: at least one commit path must have run"
    );
    assert!(
        both_absent > 0,
        "sanity: at least one crash path must have run"
    );
}

/// The one legitimate alternative to "start first, then insert a domain row
/// referencing `outcome.exec_id`": a domain row inserted FIRST with a
/// placeholder foreign key, then patched with the real `exec_id` once the
/// start call returns it — all inside the same transaction (review finding
/// H1's replacement for the dead `domain_write_first` flag). Proves atomicity
/// holds for this ordering too, with its own focused, deterministic
/// commit/rollback coverage rather than folding a synthetic reordering into
/// the randomized loop above.
#[tokio::test]
async fn atomicity_holds_when_domain_row_is_written_before_the_start() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);
    let order_id = "order-domain-first-1".to_string();

    let outcome: TransactionalStartOutcome = Box::pin(
        conn.transaction::<TransactionalStartOutcome, HarvestError, _>({
            let client = client.clone();
            let order_id = order_id.clone();
            async move |conn| {
                // 1. Placeholder row — the exec id genuinely does not
                //    exist yet at this point.
                diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
                    .bind::<Text, _>(&order_id)
                    .bind::<SqlUuid, _>(Uuid::nil())
                    .execute(conn)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;

                // 2. Stage the workflow start on the SAME transaction.
                let outcome = client
                    .start_workflow_transactional(
                        conn,
                        "t763_order_workflow",
                        &order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 500}),
                        TransactionalStartOptions::new(),
                    )
                    .await?;

                // 3. Patch the placeholder with the real id, still inside
                //    the same open transaction.
                diesel::sql_query("UPDATE t763_orders SET exec_id = $1 WHERE id = $2")
                    .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
                    .bind::<Text, _>(&order_id)
                    .execute(conn)
                    .await
                    .map_err(autumn_harvest::error::database_error)?;

                Ok(outcome)
            }
        }),
    )
    .await
    .expect("transaction must commit");
    let exec_id = outcome.exec_id;
    outcome.finish().await;

    let mut fresh = connect(&database_url).await;
    assert_eq!(domain_row_count(&mut fresh, &order_id).await, 1);
    assert_eq!(execution_row_count(&mut fresh, &order_id).await, 1);
    assert_eq!(workflow_started_event_count(&mut fresh, exec_id).await, 1);
    assert_eq!(task_queue_row_count(&mut fresh, exec_id).await, 1);

    // The patched row references the REAL exec id, never the placeholder.
    let patched_exec_id = order_row_exec_id(&mut fresh, &order_id).await;
    assert_eq!(patched_exec_id, exec_id.as_uuid());
}

/// The mirror-image rollback for the domain-write-first ordering above: a
/// forced failure *after* the start call but *before* the patch-in UPDATE
/// must leave neither the placeholder domain row nor the workflow — the
/// placeholder row was staged inside the same still-open transaction as the
/// start, so both roll back together.
#[tokio::test]
async fn rollback_after_domain_first_write_leaves_neither_placeholder_nor_workflow() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);
    let order_id = "order-domain-first-rollback-1".to_string();

    let mut observed_exec_id: Option<ExecutionId> = None;
    let result: Result<(), HarvestError> = Box::pin(conn.transaction::<(), HarvestError, _>({
        let client = client.clone();
        let order_id = order_id.clone();
        let observed = &mut observed_exec_id;
        async move |conn| {
            diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
                .bind::<Text, _>(&order_id)
                .bind::<SqlUuid, _>(Uuid::nil())
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;

            let outcome = client
                .start_workflow_transactional(
                    conn,
                    "t763_order_workflow",
                    &order_id,
                    serde_json::json!({"order_id": order_id, "amount_cents": 500}),
                    TransactionalStartOptions::new(),
                )
                .await?;
            *observed = Some(outcome.exec_id);

            // Simulate a crash/failure before the patch-in UPDATE runs.
            Err(HarvestError::Config(
                "simulated failure before patch".into(),
            ))
        }
    }))
    .await;

    assert!(result.is_err());
    let exec_id = observed_exec_id.expect("exec id was assigned before the forced failure");

    let mut fresh = connect(&database_url).await;
    assert_eq!(domain_row_count(&mut fresh, &order_id).await, 0);
    assert_eq!(execution_row_count(&mut fresh, &order_id).await, 0);
    assert_eq!(workflow_started_event_count(&mut fresh, exec_id).await, 0);
    assert_eq!(task_queue_row_count(&mut fresh, exec_id).await, 0);
}

// ---------------------------------------------------------------------------
// 2. Start-semantics parity (AC3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn honors_workflow_info_execution_timeout_and_concurrency_key_defaults() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_order_workflow",
                    "order-defaults-1",
                    serde_json::json!({"order_id": "order-defaults-1", "amount_cents": 42}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("start must succeed");

    let mut fresh = connect(&database_url).await;
    let cols = load_execution_columns(&mut fresh, outcome.exec_id).await;
    assert!(
        cols.execution_timeout.is_some(),
        "the declared #[workflow(execution_timeout = \"1h\")] must be applied"
    );
    // AC3's own issue text names `sla` explicitly alongside `execution_timeout`
    // and concurrency key — review finding C1: this was previously untested
    // even though `resolve_transactional_start_defaults` computes it.
    assert!(
        cols.sla.is_some(),
        "the declared #[workflow(sla = \"30m\")] must be applied"
    );
    assert!(
        cols.sla_deadline_at.is_some(),
        "a declared SLA must resolve to a computed sla_deadline_at (started_at + sla)"
    );

    let (concurrency_key, concurrency_cap) =
        load_task_concurrency(&mut fresh, outcome.exec_id).await;
    assert_eq!(
        concurrency_key.as_deref(),
        Some("order-defaults-1"),
        "the declared concurrency(key = \"input.order_id\") must resolve against the input"
    );
    assert_eq!(
        concurrency_cap,
        Some(5),
        "the declared concurrency(limit = 5) must be applied to the dispatch task"
    );
}

#[tokio::test]
async fn plain_workflow_with_no_declared_policies_gets_no_defaults() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-plain-1",
                    serde_json::json!({"order_id": "order-plain-1", "amount_cents": 1}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("start must succeed");

    let mut fresh = connect(&database_url).await;
    let cols = load_execution_columns(&mut fresh, outcome.exec_id).await;
    assert!(cols.execution_timeout.is_none());
    assert!(cols.sla.is_none(), "no declared SLA must resolve to none");
    assert!(cols.sla_deadline_at.is_none());

    let (concurrency_key, concurrency_cap) =
        load_task_concurrency(&mut fresh, outcome.exec_id).await;
    assert!(concurrency_key.is_none());
    assert!(concurrency_cap.is_none());
}

/// Codex review (issue #763), "Carry the default queue on the handle
/// client": the falsifying proof. `resolve_transactional_queue_name` used to
/// fall back to the process-global `completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE`
/// static when a start supplied no explicit `TransactionalStartOptions::queue_name`
/// — a static that is write-once-if-unset and first-writer-wins for the whole
/// process, so it goes stale the moment a SECOND, differently-configured
/// runtime is built in the same process after an earlier one already
/// initialized it (exactly the finding's named "second runtime after an
/// earlier one initialized the global" scenario).
///
/// This test reproduces that scenario directly: it pre-poisons the global
/// with a queue name that belongs to nobody real (`stale-global-queue`,
/// simulating an unrelated earlier runtime), then builds a client carrying
/// its OWN, different queue list via `.with_queues(...)` (mirroring how the
/// plugin's production wiring in `autumn-harvest-plugin/src/plugin.rs` now
/// threads `runtime.api_runtime().queues()` through). A start with no
/// explicit per-call override must land on the CLIENT's own configured queue
/// — never the stale global, and never the literal `"default"`.
///
/// Under the pre-fix code this assertion fails (it resolves to
/// `stale-global-queue`, the poisoned global), which is exactly why this is
/// the decisive regression test for the fix rather than merely exercising
/// the new code path.
#[tokio::test]
async fn queue_defaults_to_the_clients_own_configured_queue_not_a_stale_process_global() {
    let (database_url, _container) = setup_database().await;

    // Simulate an earlier, unrelated runtime having already initialized the
    // process-global default queue static to a value that belongs to no real
    // worker fleet in this test.
    {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE
            .write()
            .expect("global default queue lock must not be poisoned");
        *lock = Some("stale-global-queue".to_string());
    }

    let client =
        single_shard_client(&database_url).with_queues(["priority-orders", "fallback-queue"]);
    let mut conn = connect(&database_url).await;

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-client-queue-1",
                    serde_json::json!({"order_id": "order-client-queue-1", "amount_cents": 1}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("start must succeed");

    // Restore the global immediately so this poisoning cannot leak into any
    // other test in the same `--test-threads=1` process.
    {
        let mut lock = autumn_harvest::completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE
            .write()
            .expect("global default queue lock must not be poisoned");
        *lock = None;
    }

    let mut fresh = connect(&database_url).await;
    let resolved_queue = task_queue_row_queue_name(&mut fresh, outcome.exec_id).await;
    assert_eq!(
        resolved_queue, "priority-orders",
        "the start must resolve to the client's own first configured queue, never the stale \
         process global (\"stale-global-queue\") and never the literal \"default\""
    );
}

/// Companion to the test above: with no `.with_queues(...)` call at all (the
/// plain `single_shard_client` helper every other test in this file uses),
/// a start with no explicit per-call override must still fall through to the
/// literal `"default"` — the same final fallback the HTTP
/// `POST /workflows/{name}/start` route uses. Guards the innermost fallback
/// tier against an accidental future regression now that the process-global
/// fallback tier has been removed entirely.
#[tokio::test]
async fn queue_falls_back_to_the_literal_default_with_no_configured_queues() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let mut conn = connect(&database_url).await;

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-default-queue-1",
                    serde_json::json!({"order_id": "order-default-queue-1", "amount_cents": 1}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("start must succeed");

    let mut fresh = connect(&database_url).await;
    let resolved_queue = task_queue_row_queue_name(&mut fresh, outcome.exec_id).await;
    assert_eq!(resolved_queue, "default");
}

#[tokio::test]
async fn reject_duplicate_reuse_policy_rejects_a_second_start() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);

    let start_once = |workflow_id: &'static str| {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            Box::pin(conn.transaction::<_, HarvestError, _>(async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        workflow_id,
                        serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                        TransactionalStartOptions::new()
                            .with_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate),
                    )
                    .await
            }))
            .await
        }
    };

    let first = start_once("order-reject-1").await;
    assert!(first.is_ok(), "first start must succeed");

    let second = start_once("order-reject-1").await;
    assert!(
        matches!(second, Err(HarvestError::AlreadyExists { .. })),
        "second start under RejectDuplicate must be rejected, got {second:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, "order-reject-1").await, 1);
}

#[tokio::test]
async fn conflict_policy_use_existing_attaches_without_a_second_workflow_started() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);

    let start_once = || {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            Box::pin(conn.transaction::<_, HarvestError, _>(async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        "order-attach-1",
                        serde_json::json!({"order_id": "order-attach-1", "amount_cents": 1}),
                        TransactionalStartOptions::new()
                            .with_conflict_policy(WorkflowIdConflictPolicy::UseExisting),
                    )
                    .await
            }))
            .await
        }
    };

    let first = start_once().await.expect("first start must succeed");
    assert!(first.created);
    let second = start_once()
        .await
        .expect("second start must attach, not error");
    assert!(
        !second.created,
        "attaching to a live prior must report created = false"
    );
    assert_eq!(first.exec_id, second.exec_id);

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, "order-attach-1").await, 1);
    assert_eq!(
        workflow_started_event_count(&mut fresh, first.exec_id).await,
        1,
        "attaching must not append a second WorkflowStarted event"
    );
}

/// `WorkflowIdConflictPolicy::TerminateExisting` against a still-active prior
/// (no worker is running in this test, so the first start never progresses
/// past `RUNNING`) forces `inline_cancel`, which durably cancels the prior
/// AND populates `TransactionalStartOutcome::deferred.checks` with a real,
/// non-empty entry (`(exec_id, workflow_name)`) — unlike every other test in
/// this suite, whose `deferred` is always empty. Calling `finish()`
/// *strictly after* the transaction commits must not panic or hang, proving
/// both that the documented post-commit usage genuinely works end to end,
/// and — as a regression guard — that it correctly resolves a pool
/// connection for a *single-shard* client (`self.shard == ShardId::UNENCODED`,
/// which `ShardedDbPool::exact_pool_for` cannot resolve; see
/// `TransactionalStartOutcome::finish`'s use of `pool_for` instead).
#[tokio::test]
async fn finish_after_commit_dispatches_diagnostics_for_a_terminate_existing_collision() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let workflow_id = "order-terminate-existing-1";

    let start = |conflict: Option<WorkflowIdConflictPolicy>| {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            let outcome: TransactionalStartOutcome = Box::pin(
                conn.transaction::<TransactionalStartOutcome, HarvestError, _>(async move |conn| {
                    let mut options = TransactionalStartOptions::new();
                    if let Some(policy) = conflict {
                        options = options.with_conflict_policy(policy);
                    }
                    client
                        .start_workflow_transactional(
                            conn,
                            "t763_plain_workflow",
                            workflow_id,
                            serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                            options,
                        )
                        .await
                }),
            )
            .await
            .expect("start must succeed");
            outcome
        }
    };

    let first = start(None).await;
    assert!(first.created);

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_state(&mut fresh, first.exec_id).await,
        "RUNNING",
        "no worker is polling in this test, so the prior stays active"
    );

    let second = start(Some(WorkflowIdConflictPolicy::TerminateExisting)).await;
    assert!(
        second.created,
        "TerminateExisting must start a fresh execution, not attach"
    );
    assert_ne!(first.exec_id, second.exec_id);

    // `execution::inline_cancel` genuinely ran on the prior — a durable
    // `WorkflowCancelled` event was appended — proving `deferred.checks` is
    // non-empty for this outcome (asserted structurally as "does not panic"
    // below, since the field is private to the crate's own module). The
    // row's FINAL `state` column is then sealed to `CONTINUED_AS_NEW` by the
    // `replace_execution` call that unconditionally follows `inline_cancel`
    // in the same `ActiveConflictBehavior::Terminate` branch (see
    // `execution.rs`; this is the general terminate-and-replace mechanism
    // shared with the `TerminateIfRunning` reuse policy, not something
    // `start_workflow_transactional` introduces) — so the *event log* records
    // the cancellation while the *state column* reads CONTINUED_AS_NEW.
    assert_eq!(
        workflow_cancelled_event_count(&mut fresh, first.exec_id).await,
        1,
        "inline_cancel must have appended exactly one WorkflowCancelled event"
    );
    assert_eq!(
        execution_state(&mut fresh, first.exec_id).await,
        "CONTINUED_AS_NEW"
    );

    // The whole point of this test: call `finish()` strictly after the
    // transaction that produced it has already committed (proven above by
    // `second`/the cancellation both being independently visible from a
    // fresh connection), with a genuinely non-empty `deferred.checks`.
    second.finish().await;
}

/// Regression guard for the `WorkflowIdConflictPolicy::TerminateExisting`
/// path's atomicity. Unlike the HTTP start route (which runs a separate,
/// unlocked pre-check step for the native `TerminateIfRunning` reuse policy
/// before its own locked start transaction), `start_workflow_transactional`
/// has no such separate step: the ENTIRE collision resolution for the prior
/// execution — `execution::inline_cancel` followed by `replace_execution` —
/// runs inside `start_or_load_workflow_execution_collect`'s own nested
/// `conn.transaction()` call, on the SAME caller-owned `conn` (`diesel_async`
/// issues a `SAVEPOINT`, not a fresh `BEGIN`, when `conn` is already inside
/// an open transaction) — so none of it is independently durable. If the
/// caller's OUTER transaction subsequently rolls back (a downstream failure
/// *after* `start_workflow_transactional` has already returned `Ok`), that
/// whole resolution must roll back right along with the creation of the
/// fresh one — leaving the prior back in `RUNNING`, with no successor ever
/// having existed. A version of `inline_cancel`/`replace_execution` that
/// instead opened a genuinely independent top-level transaction (bypassing
/// the caller's own `conn`) would durably strand the prior sealed
/// `CONTINUED_AS_NEW` (with a stray `WorkflowCancelled` event in its
/// history) with no successor even after this outer rollback; this test
/// catches that class of regression.
#[tokio::test]
async fn outer_rollback_after_a_terminate_existing_collision_undoes_the_cancellation_too() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let workflow_id = "order-terminate-existing-rollback-1";

    // Start the first execution and commit — this one must survive.
    let mut conn = connect(&database_url).await;
    let first: TransactionalStartOutcome = Box::pin(
        conn.transaction::<TransactionalStartOutcome, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        workflow_id,
                        serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }),
    )
    .await
    .expect("first start must commit");
    assert!(first.created);
    let first_exec_id = first.exec_id;

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_state(&mut fresh, first_exec_id).await, "RUNNING");

    // Now attempt a TerminateExisting collision, but fail the OUTER
    // transaction *after* `start_workflow_transactional` has already
    // returned `Ok` — i.e. after the internal pre-check-cancel SAVEPOINT
    // has been released and the fresh execution row has been staged.
    let mut conn2 = connect(&database_url).await;
    let result: Result<(), HarvestError> = Box::pin(conn2.transaction::<(), HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            let outcome = client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    workflow_id,
                    serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                    TransactionalStartOptions::new()
                        .with_conflict_policy(WorkflowIdConflictPolicy::TerminateExisting),
                )
                .await?;
            assert!(
                outcome.created,
                "TerminateExisting must resolve to a fresh execution before the forced failure"
            );
            assert_ne!(outcome.exec_id, first_exec_id);

            // Simulate a downstream failure in the caller's own code, *after*
            // the start (and the cancellation it performed) has already been
            // staged in this same outer transaction.
            Err(HarvestError::Config(
                "simulated failure after terminate-existing collision".into(),
            ))
        }
    }))
    .await;

    assert!(
        result.is_err(),
        "the outer transaction must have rolled back"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_state(&mut fresh, first_exec_id).await,
        "RUNNING",
        "the prior's cancellation must be undone along with the rest of the \
         rolled-back outer transaction — it must not be left stranded in the \
         terminal state (CONTINUED_AS_NEW) that `replace_execution` would \
         have sealed it into had the outer transaction committed"
    );
    assert_eq!(
        workflow_cancelled_event_count(&mut fresh, first_exec_id).await,
        0,
        "the WorkflowCancelled event inline_cancel staged must be rolled \
         back too, not left stranded in a RUNNING execution's history"
    );
}

/// Regression guard for a Codex review finding on PR #1148 ("Require a real
/// outer transaction"): the module doc comment's Section 4 above explicitly
/// documents (and the two tests immediately above already exercise, always
/// via an outer `conn.transaction()` wrapper) that
/// `start_workflow_transactional` permits a caller to pass a genuinely
/// *bare*, non-transaction-wrapped `conn`. The non-keyed branch tells
/// `start_or_load_workflow_execution_collect` `in_outer_transaction = true`,
/// which lets a `TerminateExisting`/`TerminateIfRunning` pre-check
/// cancellation defer its follow-up dispatch on the assumption that
/// *something* will revert the cancellation if the replacement start
/// subsequently fails. On a genuinely bare connection there was previously
/// nothing to make that true: a replacement-start failure (forced here via
/// an oversized input, so `execution::replace_execution`'s payload-cap check
/// fails *after* `execution::inline_cancel` has already durably cancelled
/// the prior in the same statement sequence) would leave the prior
/// permanently cancelled with no successor and no way to recover the lost
/// follow-up dispatch — the whole call returns `Err`, so there is no
/// `TransactionalStartOutcome.deferred` to recover it from either.
///
/// The fix wraps the whole non-keyed call to
/// `start_or_load_workflow_execution_collect` in its own nested
/// `conn.transaction()` (mirroring the keyed branch,
/// `start_workflow_transactional_idempotent`, which already did this
/// before this PR) — `diesel_async` issues a genuine top-level `BEGIN` when
/// `conn` has no already-open transaction, so the cancel-then-replace
/// sequence becomes atomic on its own, without requiring the caller to have
/// opened one. This test proves that atomicity holds on a bare connection,
/// not just on one already wrapped by the caller.
///
/// **This is Harvest's own *internal* multi-statement atomicity — not the
/// same thing as dual-write atomicity with a caller's own domain write.**
/// See `bare_connection_commits_the_start_immediately_no_dual_write_atomicity`
/// immediately below for the boundary of that distinction (the subject of a
/// later Codex finding on this same fix, "Require an outer transaction for
/// atomic starts").
#[tokio::test]
async fn bare_connection_terminate_existing_collision_reverts_cancellation_on_replacement_failure()
{
    let (database_url, _container) = setup_database().await;
    // The cap must comfortably exceed the first (legitimate) start's ~59-byte
    // serialized payload but sit well under the second (deliberately
    // oversized) call's padded payload below.
    let client = single_shard_client(&database_url).with_max_workflow_input_bytes(100);
    let workflow_id = "order-bare-conn-terminate-1";

    // Start the first execution normally, via a transaction wrapper (how it
    // starts is irrelevant to what this test proves — only the SECOND call
    // below, on a genuinely bare connection, matters).
    let mut setup_conn = connect(&database_url).await;
    let first: TransactionalStartOutcome = Box::pin(
        setup_conn.transaction::<TransactionalStartOutcome, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        workflow_id,
                        serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }),
    )
    .await
    .expect("first start must commit");
    assert!(first.created);
    let first_exec_id = first.exec_id;

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_state(&mut fresh, first_exec_id).await, "RUNNING");

    // The whole point of this test: a BARE connection with no
    // `conn.transaction()` wrapper at all — exactly what the module doc's
    // Section 4 documents as permitted, and what this call is documented to
    // make safe entirely on its own.
    let mut bare_conn = connect(&database_url).await;
    let oversized_input = serde_json::json!({
        "order_id": workflow_id,
        "amount_cents": 1,
        "padding": "x".repeat(200),
    });
    let result = client
        .start_workflow_transactional(
            &mut bare_conn,
            "t763_plain_workflow",
            workflow_id,
            oversized_input,
            TransactionalStartOptions::new()
                .with_conflict_policy(WorkflowIdConflictPolicy::TerminateExisting),
        )
        .await;

    assert!(
        matches!(result, Err(HarvestError::PayloadTooLarge { .. })),
        "expected the oversized replacement input to fail with \
         PayloadTooLarge, got {result:?}"
    );

    let mut verify = connect(&database_url).await;
    assert_eq!(
        execution_state(&mut verify, first_exec_id).await,
        "RUNNING",
        "the prior must NOT be left durably cancelled on a bare connection \
         when the replacement start subsequently fails — the nested \
         transaction this call wraps itself in must have reverted \
         inline_cancel's work too, exactly as it would have if the caller \
         had wrapped the call in their own transaction"
    );
    assert_eq!(
        workflow_cancelled_event_count(&mut verify, first_exec_id).await,
        0,
        "inline_cancel's WorkflowCancelled event must be rolled back, not \
         left stranded against a still-RUNNING execution"
    );
    assert_eq!(
        execution_row_count(&mut verify, workflow_id).await,
        1,
        "no replacement execution row may have been left behind either"
    );
}

/// Codex review (issue #763), "Require an outer transaction for atomic
/// starts": documents and proves the precise boundary of the atomicity
/// guarantee for a genuinely bare (non-transaction-wrapped) connection.
///
/// The test immediately above proves Harvest's own *internal*
/// multi-statement sequence (a `TerminateExisting` collision's
/// cancel-then-replace pair) stays atomic as a unit on a bare connection —
/// but that is a narrower guarantee than this call's headline promise
/// ("commits or rolls back atomically with the caller's own domain write").
/// On a bare connection, `diesel-async`'s nested-transaction machinery has
/// nothing to nest *into*: it issues a real, top-level `BEGIN ... COMMIT` of
/// its own that fully completes *before* `start_workflow_transactional`
/// returns (confirmed against `diesel-async 0.9.2`'s own transaction manager,
/// which branches on `TransactionManagerStatus::transaction_depth()` —
/// `None` on a bare connection issues `BEGIN`/`COMMIT`; `Some(_)` on an
/// already-open transaction issues `SAVEPOINT`/`RELEASE SAVEPOINT` instead).
/// There is nothing left "open" for a caller write performed afterward on
/// that same connection to share a rollback boundary with.
///
/// This proves that boundary concretely: after calling
/// `start_workflow_transactional` on a bare connection, the started
/// execution's row and `WorkflowStarted` event are ALREADY visible from a
/// completely independent connection — durably committed — even though this
/// test itself never issues an explicit `COMMIT`. A caller's own domain
/// write, made afterward on that same bare connection, subsequently failing
/// does NOT roll the workflow start back — proving a bare connection does
/// not provide dual-write atomicity. See "Connection discipline" in
/// `docs/transactional-start.md` for the corrected usage guidance (the fix
/// for this finding was a documentation correction — see the doc comment on
/// `WorkflowHandleClient::start_workflow_transactional` — not a runtime
/// behavior change: rejecting bare connections outright would break the
/// legitimate narrower use case the test above already relies on and
/// documents).
#[tokio::test]
async fn bare_connection_commits_the_start_immediately_no_dual_write_atomicity() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let workflow_id = "order-bare-conn-no-atomicity-1";

    let mut bare_conn = connect(&database_url).await;
    let outcome = client
        .start_workflow_transactional(
            &mut bare_conn,
            "t763_plain_workflow",
            workflow_id,
            serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
            TransactionalStartOptions::new(),
        )
        .await
        .expect("bare-connection start must succeed");
    assert!(outcome.created);

    // Not a single explicit COMMIT has been issued by this test on
    // `bare_conn` -- yet the workflow start is already fully durable, visible
    // from a completely independent connection. There is nothing left
    // "pending" for a caller write made afterward on `bare_conn` to share an
    // atomic rollback boundary with.
    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, workflow_id).await, 1);
    assert_eq!(
        workflow_started_event_count(&mut fresh, outcome.exec_id).await,
        1
    );
    assert_eq!(
        execution_state(&mut fresh, outcome.exec_id).await,
        "RUNNING"
    );

    // Simulate the caller's OWN domain write, on the SAME bare connection,
    // subsequently failing -- a primary-key violation on a second insert of
    // the same row is the simplest reliable way to force one. If a bare
    // connection provided dual-write atomicity, this failure would have to
    // roll the workflow start back too; it does not, because the start
    // already committed before this test ever touched `bare_conn` again.
    diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
        .bind::<Text, _>(workflow_id.to_string())
        .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
        .execute(&mut bare_conn)
        .await
        .expect("first domain insert on the bare connection must succeed");
    let duplicate_insert_result =
        diesel::sql_query("INSERT INTO t763_orders (id, exec_id) VALUES ($1, $2)")
            .bind::<Text, _>(workflow_id.to_string())
            .bind::<SqlUuid, _>(outcome.exec_id.as_uuid())
            .execute(&mut bare_conn)
            .await;
    assert!(
        duplicate_insert_result.is_err(),
        "the duplicate-primary-key insert must fail"
    );

    let mut verify = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut verify, workflow_id).await,
        1,
        "the workflow start is UNAFFECTED by the caller's own later write \
         failing on the same bare connection -- it was already committed"
    );
    assert_eq!(
        workflow_started_event_count(&mut verify, outcome.exec_id).await,
        1,
        "the WorkflowStarted event is likewise unaffected"
    );
}

/// Codex review (issue #763), "Validate transactional idempotency keys":
/// unlike the plain HTTP start route
/// (`autumn-harvest-plugin::api::validate_start_idempotency_key`), the
/// transactional-start client accepted ANY string as an idempotency key with
/// no trim/empty/length validation — an accidental empty or whitespace-only
/// key became a real `(workflow_name, "")` claim in `harvest_start_idempotency`
/// that would silently dedupe every OTHER unrelated transactional start for
/// the same workflow against each other. This proves an empty key is now
/// rejected before anything is written, matching the HTTP route.
#[tokio::test]
async fn empty_idempotency_key_is_rejected_writes_nothing() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let mut conn = connect(&database_url).await;

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        "order-empty-idem-key-1",
                        serde_json::json!({"order_id": "order-empty-idem-key-1", "amount_cents": 1}),
                        TransactionalStartOptions::new().with_idempotency_key(""),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for an empty idempotency_key, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-empty-idem-key-1").await,
        0,
        "an empty idempotency_key must write no execution row"
    );
    assert_eq!(
        count_rows(
            &mut fresh,
            "SELECT COUNT(*) AS n FROM harvest_start_idempotency \
             WHERE workflow_name = 't763_plain_workflow'"
        )
        .await,
        0,
        "an empty idempotency_key must claim no idempotency row either — this \
         is exactly the bug: an unvalidated empty key becomes a real, shared \
         claim that would dedupe unrelated starts against each other"
    );
}

/// Sibling to the empty-key case: a whitespace-only key must be treated the
/// same as empty (rejected), not accepted as a distinct, non-empty-looking
/// key that happens to be all spaces.
#[tokio::test]
async fn whitespace_only_idempotency_key_is_rejected_writes_nothing() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let mut conn = connect(&database_url).await;

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        "order-whitespace-idem-key-1",
                        serde_json::json!({
                            "order_id": "order-whitespace-idem-key-1",
                            "amount_cents": 1
                        }),
                        TransactionalStartOptions::new().with_idempotency_key("   \t  "),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for a whitespace-only idempotency_key, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-whitespace-idem-key-1").await,
        0
    );
}

/// A key that survives trimming but is absurdly long must be rejected with a
/// clean, typed error — not left to abort the caller's own transaction with a
/// raw Postgres error when the `(workflow_name, idempotency_key)` composite
/// PRIMARY KEY on `harvest_start_idempotency` exceeds btree's index tuple
/// size limit.
#[tokio::test]
async fn oversized_idempotency_key_is_rejected_writes_nothing() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let mut conn = connect(&database_url).await;
    let oversized_key = "k".repeat(513); // MAX_START_IDEMPOTENCY_KEY_LEN is 512

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        "order-oversized-idem-key-1",
                        serde_json::json!({
                            "order_id": "order-oversized-idem-key-1",
                            "amount_cents": 1
                        }),
                        TransactionalStartOptions::new().with_idempotency_key(oversized_key),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for an oversized idempotency_key, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-oversized-idem-key-1").await,
        0
    );
}

/// A key exactly AT the cap must still be accepted — the cap rejects
/// strictly-over, not at-or-over, mirroring the HTTP route's own boundary.
#[tokio::test]
async fn idempotency_key_at_exactly_the_length_cap_is_accepted() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let mut conn = connect(&database_url).await;
    let key_at_cap = "k".repeat(512);

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        let key_at_cap = key_at_cap.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-cap-idem-key-1",
                    serde_json::json!({"order_id": "order-cap-idem-key-1", "amount_cents": 1}),
                    TransactionalStartOptions::new().with_idempotency_key(key_at_cap),
                )
                .await
        }
    }))
    .await
    .expect("a key exactly at the length cap must be accepted");

    assert!(outcome.created);
}

/// A key with leading/trailing whitespace must be TRIMMED before use, and the
/// trimmed form must be what's actually stored/matched — proven by starting
/// once with a padded key and once with its already-trimmed equivalent, and
/// asserting they dedupe to the SAME execution (if trimming didn't happen,
/// `"  key  "` and `"key"` would be treated as two distinct keys and would
/// NOT dedupe, landing two separate executions instead of one).
#[tokio::test]
async fn idempotency_key_is_trimmed_before_use() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);

    let start_with_key = |workflow_id: &'static str, key: &'static str| {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            Box::pin(conn.transaction::<_, HarvestError, _>(async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        workflow_id,
                        serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                        TransactionalStartOptions::new().with_idempotency_key(key),
                    )
                    .await
            }))
            .await
        }
    };

    let first = start_with_key("order-trim-idem-a", "  trimmed-key-xyz  ")
        .await
        .expect("padded-key start must succeed");
    let second = start_with_key("order-trim-idem-b", "trimmed-key-xyz")
        .await
        .expect("already-trimmed-key start must dedupe, not error");

    assert_eq!(
        first.exec_id, second.exec_id,
        "the padded key and its trimmed equivalent must dedupe to ONE \
         execution, proving the key was actually trimmed before being used \
         as the idempotency claim"
    );
    assert!(first.created);
    assert!(!second.created);

    let mut fresh = connect(&database_url).await;
    let total = count_rows(
        &mut fresh,
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id LIKE 'order-trim-idem-%'",
    )
    .await;
    assert_eq!(total, 1, "exactly one execution row must exist");
}

#[tokio::test]
async fn idempotency_key_deduplicates_two_committed_starts() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let key = "checkout-req-abc123";

    let start_once = |workflow_id: &'static str| {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            Box::pin(conn.transaction::<_, HarvestError, _>(async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        workflow_id,
                        serde_json::json!({"order_id": workflow_id, "amount_cents": 1}),
                        TransactionalStartOptions::new().with_idempotency_key(key),
                    )
                    .await
            }))
            .await
        }
    };

    // Two *different* workflow_ids carrying the *same* idempotency key must
    // converge on one execution — the key, not the id, is authoritative.
    let first = start_once("order-idem-a")
        .await
        .expect("first keyed start must succeed");
    let second = start_once("order-idem-b")
        .await
        .expect("second keyed start must dedupe, not error");

    assert_eq!(
        first.exec_id, second.exec_id,
        "same key must dedupe to one execution"
    );
    assert!(first.created);
    assert!(
        !second.created,
        "the deduped retry must report created = false"
    );

    let mut fresh = connect(&database_url).await;
    let total = count_rows(
        &mut fresh,
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id LIKE 'order-idem-%'",
    )
    .await;
    assert_eq!(total, 1, "exactly one execution row must exist");
}

/// Review finding M3: the idempotency-key **reservation itself** — not just
/// the execution it protects — must be undone by an outer rollback, exactly
/// like every other write this API stages
/// (`start_or_load_workflow_execution_idempotent` reserves the claim and
/// performs the start inside one transaction nested on the caller's own
/// `conn`). A caller retry after a genuine crash/rollback must NOT find its
/// key permanently "burned" with no execution to show for it.
#[tokio::test]
async fn idempotency_key_is_not_burned_by_a_rolled_back_start() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);
    let key = "checkout-req-rollback-retry-1";
    let order_id = "order-idem-rollback-1";

    // First attempt: the caller's transaction fails AFTER the keyed start
    // has already returned `Ok`, simulating a crash before the caller's own
    // commit.
    let mut conn = connect(&database_url).await;
    let first_attempt: Result<(), HarvestError> =
        Box::pin(conn.transaction::<(), HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                let outcome = client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 1}),
                        TransactionalStartOptions::new().with_idempotency_key(key),
                    )
                    .await?;
                assert!(outcome.created, "the first attempt must resolve fresh");
                Err(HarvestError::Config(
                    "simulated crash before the caller's own commit".into(),
                ))
            }
        }))
        .await;
    assert!(first_attempt.is_err());

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, order_id).await,
        0,
        "the rolled-back attempt must have left no execution row behind"
    );

    // Retry with the SAME idempotency key, in a NEW transaction that
    // actually commits — must succeed as a fresh start, not be permanently
    // rejected as an already-consumed key.
    let mut conn2 = connect(&database_url).await;
    let retry: TransactionalStartOutcome = Box::pin(
        conn2.transaction::<TransactionalStartOutcome, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 1}),
                        TransactionalStartOptions::new().with_idempotency_key(key),
                    )
                    .await
            }
        }),
    )
    .await
    .expect("retry with the same key after a rollback must succeed");

    assert!(
        retry.created,
        "the retry must create a genuinely fresh execution, not be treated \
         as an already-consumed idempotency key"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, order_id).await, 1);
    assert_eq!(
        workflow_started_event_count(&mut fresh, retry.exec_id).await,
        1
    );
}

// ---------------------------------------------------------------------------
// Committed-replay probe ordering & configured window (PR review findings).
//
// A code-review pass on this PR found two real bugs in `start_workflow_transactional`:
// (1) the idempotency-key dedup check ran *after* the fresh-start-only
//     validation (schema, debounce/batch/throttle rejection), so a retry of
//     an already-committed keyed start could be spuriously rejected by a
//     rule that only makes sense for a genuinely fresh admission if that
//     rule had changed (or newly applied) since the original delivery; and
// (2) the reserve step hardcoded `DEFAULT_START_IDEMPOTENCY_WINDOW` (24h)
//     instead of the client's configured
//     `WorkflowHandleClient::with_start_idempotency_window`, silently
//     ignoring an operator's shorter (or longer) configured window.
//
// Both mirror the already-established issue #808 invariant the HTTP start
// route enforces (`probe_committed_start_replay` runs before every
// fresh-start-only rejection, and reads `api_state.start_idempotency_window()`
// rather than the hardcoded default) -- these tests prove the in-process
// `start_workflow_transactional` path now matches it.
// ---------------------------------------------------------------------------

/// Finding (1): a retry of an already-committed keyed start must dedupe even
/// when the target workflow's published input schema has *tightened* since
/// the original delivery. Two separate clients simulate the drift: the first
/// (used for the original delivery) has no schema attached to
/// `t763_plain_workflow`; the second (used for the retry) attaches a schema
/// the very same raw input would fail if validation genuinely ran. Both
/// point at the same database and register the same handler function under
/// the same name, so the `(workflow_name, idempotency_key)` dedup key
/// resolves identically for both.
#[tokio::test]
async fn committed_replay_dedupes_despite_a_schema_that_tightened_since_the_original_delivery() {
    let (database_url, _container) = setup_database().await;
    let key = "checkout-schema-drift-1";
    let order_id = "order-schema-drift-1";
    // Missing "amount_cents" -- `order_input_schema()` would reject this if
    // it were ever checked against it.
    let input = serde_json::json!({"order_id": order_id});

    let lenient_client =
        WorkflowHandleClient::single(build_pool(&database_url), database_url.clone())
            .with_workflows(vec![t763_plain_workflow_info()]);

    let mut conn = connect(&database_url).await;
    let first = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = lenient_client.clone();
        let input = input.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    order_id,
                    input,
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect("first (schema-free) keyed start must succeed");
    assert!(first.created);

    let strict_client =
        WorkflowHandleClient::single(build_pool(&database_url), database_url.clone())
            .with_workflows(vec![
                t763_plain_workflow_info().with_input_schema_fn(order_input_schema),
            ]);

    let mut conn2 = connect(&database_url).await;
    let retry = Box::pin(conn2.transaction::<_, HarvestError, _>({
        let client = strict_client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    order_id,
                    input,
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect(
        "the retry must dedupe to the already-committed run, not be rejected by a schema \
         that only exists on the retrying client -- a schema/input mismatch on a duplicate \
         must never surface as InputValidationFailed",
    );

    assert_eq!(
        retry.exec_id, first.exec_id,
        "the retry must resolve to the SAME execution as the original delivery"
    );
    assert!(
        !retry.created,
        "the retry must report created = false (it is a replay, not a fresh start)"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, order_id).await,
        1,
        "exactly one execution row must exist"
    );
}

/// Finding (1), the debounce disjunct: a retry of an already-committed keyed
/// start must dedupe even when the target workflow has *gained* a debounce
/// policy since the original delivery -- the debounce/batch/throttle
/// deferred-admission rejection only applies to a genuinely fresh start.
#[tokio::test]
async fn committed_replay_dedupes_despite_a_debounce_policy_gained_since_the_original_delivery() {
    let (database_url, _container) = setup_database().await;
    let key = "checkout-debounce-drift-1";
    let order_id = "order-debounce-drift-1";
    let input = serde_json::json!({"order_id": order_id, "amount_cents": 1});

    let no_policy_client =
        WorkflowHandleClient::single(build_pool(&database_url), database_url.clone())
            .with_workflows(vec![t763_plain_workflow_info()]);

    let mut conn = connect(&database_url).await;
    let first = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = no_policy_client.clone();
        let input = input.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    order_id,
                    input,
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect("first (policy-free) keyed start must succeed");
    assert!(first.created);

    let now_debounced_client =
        WorkflowHandleClient::single(build_pool(&database_url), database_url.clone())
            .with_workflows(vec![t763_plain_workflow_info().with_debounce(
                autumn_harvest::debounce::DebouncePolicy {
                    key_expr: "input.order_id",
                    window: Duration::from_secs(10),
                    max_wait: None,
                },
            )]);

    let mut conn2 = connect(&database_url).await;
    let retry = Box::pin(conn2.transaction::<_, HarvestError, _>({
        let client = now_debounced_client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    order_id,
                    input,
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect(
        "the retry must dedupe to the already-committed run, not be rejected by the \
         debounce deferred-admission guard, which only applies to a genuinely fresh start",
    );

    assert_eq!(retry.exec_id, first.exec_id);
    assert!(!retry.created);

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, order_id).await, 1);
}

/// Finding (2): the idempotency dedup window must be the CLIENT's configured
/// [`WorkflowHandleClient::with_start_idempotency_window`], not the hardcoded
/// `DEFAULT_START_IDEMPOTENCY_WINDOW` (24h). Proven by backdating the
/// reservation 5 minutes -- past a short 60-second configured window, but
/// still well inside the 24h default -- and asserting the retry is treated
/// as EXPIRED (a genuinely fresh execution), which could only happen if the
/// short window is actually consulted; under the pre-fix hardcoded default
/// this same backdate would still be "live" and the retry would wrongly
/// dedupe.
///
/// The two calls use **different `workflow_id`s** (mirroring
/// `idempotency_key_deduplicates_two_committed_starts`) so the ONLY possible
/// source of a dedupe is the idempotency-key reservation itself, never the
/// orthogonal `workflow_id`-based reuse-policy collision (which would
/// legitimately attach to a still-non-terminal first execution regardless of
/// the idempotency window, since no worker runs in this test to complete
/// it) -- that confound would otherwise mask whichever window is actually
/// consulted.
#[tokio::test]
async fn keyed_retry_honors_the_clients_configured_idempotency_window_not_the_hardcoded_default() {
    let (database_url, _container) = setup_database().await;
    let key = "checkout-window-1";
    let first_workflow_id = "order-window-a";
    let retry_workflow_id = "order-window-b";

    let short_window_client =
        WorkflowHandleClient::single(build_pool(&database_url), database_url.clone())
            .with_workflows(vec![t763_plain_workflow_info()])
            .with_start_idempotency_window(Duration::from_secs(60));
    assert_eq!(
        short_window_client.start_idempotency_window(),
        Duration::from_secs(60)
    );

    let mut conn = connect(&database_url).await;
    let first = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = short_window_client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    first_workflow_id,
                    serde_json::json!({"order_id": first_workflow_id, "amount_cents": 1}),
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect("first keyed start must succeed");
    assert!(first.created);

    // Backdate the reservation 5 minutes -- past the configured 60-second
    // window, but still well inside the hardcoded 24h default. If the
    // hardcoded default were still consulted (the bug this test guards
    // against), the retry below would wrongly dedupe to `first` even though
    // it targets a completely different `workflow_id`.
    let mut admin = connect(&database_url).await;
    diesel::sql_query(
        "UPDATE harvest_start_idempotency SET created_at = now() - interval '5 minutes' \
         WHERE workflow_name = $1 AND idempotency_key = $2",
    )
    .bind::<Text, _>("t763_plain_workflow")
    .bind::<Text, _>(key)
    .execute(&mut admin)
    .await
    .expect("backdate must succeed");

    let mut conn2 = connect(&database_url).await;
    let retry = Box::pin(conn2.transaction::<_, HarvestError, _>({
        let client = short_window_client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    retry_workflow_id,
                    serde_json::json!({"order_id": retry_workflow_id, "amount_cents": 1}),
                    TransactionalStartOptions::new().with_idempotency_key(key),
                )
                .await
        }
    }))
    .await
    .expect("retry past the configured window must still succeed, as a fresh start");

    assert_ne!(
        retry.exec_id, first.exec_id,
        "the retry must be treated as a genuinely new start once the CLIENT's configured \
         (short) window has elapsed -- it must not dedupe under the hardcoded 24h default"
    );
    assert!(
        retry.created,
        "an expired-window retry must create a fresh execution, not attach"
    );
    assert_eq!(
        retry.workflow_id, retry_workflow_id,
        "the fresh execution must carry the retry's own workflow_id, proving it was not an \
         attach to the first execution under a different guise"
    );

    let mut fresh = connect(&database_url).await;
    let total = count_rows(
        &mut fresh,
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id LIKE 'order-window-%'",
    )
    .await;
    assert_eq!(
        total, 2,
        "two independent execution rows must exist once the short window expired"
    );
}

#[tokio::test]
async fn schema_validation_failure_aborts_without_writing_anything() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_schema_checked_workflow",
                        "order-schema-1",
                        // Missing the required "amount_cents" field, and
                        // "order_id" has the wrong type — must be rejected by
                        // the published schema before any DB write.
                        serde_json::json!({"order_id": 12345}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::InputValidationFailed { .. })),
        "expected InputValidationFailed, got {result:?}"
    );
    if let Err(HarvestError::InputValidationFailed { violations }) = result {
        assert!(!violations.is_empty());
    }

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, "order-schema-1").await, 0);
}

#[tokio::test]
async fn debounced_workflow_is_rejected_because_admission_cannot_be_deferred() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_debounced_workflow",
                        "order-debounced-1",
                        serde_json::json!({"order_id": "order-debounced-1", "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for a debounced workflow, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-debounced-1").await,
        0
    );
}

/// Review finding H4: the deferred-admission guard is
/// `debounce.is_some() || batch.is_some() || throttle.is_some()` — a single
/// disjunct test (debounce, above) cannot catch a refactor that silently
/// drops the batch or throttle arm. This covers the batch disjunct.
#[tokio::test]
async fn batched_workflow_is_rejected_because_admission_cannot_be_deferred() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_batched_workflow",
                        "order-batched-1",
                        serde_json::json!({"order_id": "order-batched-1", "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for a batched workflow, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(execution_row_count(&mut fresh, "order-batched-1").await, 0);
}

/// Review finding H4, throttle disjunct — see
/// `batched_workflow_is_rejected_because_admission_cannot_be_deferred`'s doc
/// comment.
#[tokio::test]
async fn throttled_workflow_is_rejected_because_admission_cannot_be_deferred() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_throttled_workflow",
                        "order-throttled-1",
                        serde_json::json!({"order_id": "order-throttled-1", "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection for a throttled workflow, got {result:?}"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-throttled-1").await,
        0
    );
}

#[tokio::test]
async fn unregistered_workflow_name_is_rejected() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "totally_unregistered_workflow",
                        "order-unreg-1",
                        serde_json::json!({}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(matches!(result, Err(HarvestError::Config(_))));

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-unreg-1").await,
        0,
        "an unregistered workflow name must be rejected BEFORE any database \
         write — matching every other rejection test in this suite"
    );
}

/// Review finding M6: the transactional-start path threads the resolved
/// gate decision through `GateMode::CheckCached` (see
/// `docs/transactional-start.md`'s "Start-semantics parity" table) — this is
/// the one behavioral test in the suite that actually arms a gate and
/// proves (a) a matching `GateScope::WorkflowName` gate blocks the
/// transactional start with `HarvestError::AdmissionBlocked`, writing
/// nothing; (b) an UNRELATED workflow name is unaffected by the same
/// still-armed gate, proving the scope is honored and not accidentally
/// fleet-wide; and (c) disarming the gate immediately restores normal
/// admission. Uses the dedicated `t763_gate_target_workflow` fixture, never
/// used by any other test in this suite, so this is the ONLY test whose
/// outcome depends on the process-global `GLOBAL_ADMISSION_GATE_CACHE` —
/// see the module doc comment's "Execution" note.
#[tokio::test]
async fn admission_gate_blocks_a_matching_workflow_name_and_spares_others() {
    let (database_url, _container) = setup_database().await;
    let client = single_shard_client(&database_url);

    let start = |workflow_name: &'static str, order_id: &'static str| {
        let client = client.clone();
        let database_url = database_url.clone();
        async move {
            let mut conn = connect(&database_url).await;
            Box::pin(conn.transaction::<_, HarvestError, _>(async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        workflow_name,
                        order_id,
                        serde_json::json!({"order_id": order_id, "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }))
            .await
        }
    };

    let cache = Arc::new(AdmissionGateCache::new());
    cache.refresh(vec![AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope: GateScope::WorkflowName("t763_gate_target_workflow".to_string()),
        reason: "t763-gate-test-incident".to_string(),
        message: None,
        created_by: "test".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }]);
    set_global_admission_gate_cache(Some(Arc::clone(&cache)));

    let blocked = start("t763_gate_target_workflow", "order-gate-blocked-1").await;
    assert!(
        matches!(blocked, Err(HarvestError::AdmissionBlocked { .. })),
        "expected AdmissionBlocked for a gated workflow name, got {blocked:?}"
    );

    let spared = start("t763_plain_workflow", "order-gate-spared-1").await;
    assert!(
        spared.is_ok(),
        "an unrelated workflow name must not be blocked by a \
         WorkflowName-scoped gate targeting a different name, got {spared:?}"
    );

    set_global_admission_gate_cache(None);

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-gate-blocked-1").await,
        0,
        "a gate-blocked start must write nothing"
    );
    assert_eq!(
        execution_row_count(&mut fresh, "order-gate-spared-1").await,
        1
    );

    // With the gate disarmed, the previously-blocked workflow name now
    // starts normally — and reuses the SAME order_id, directly proving the
    // earlier blocked attempt left no partial/conflicting state behind.
    let now_allowed = start("t763_gate_target_workflow", "order-gate-blocked-1").await;
    assert!(
        now_allowed.is_ok(),
        "disarming the gate must restore normal admission, got {now_allowed:?}"
    );
}

// ---------------------------------------------------------------------------
// 3. Sharding (AC4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lands_on_the_shard_backing_the_connection_never_the_other_shard() {
    let (url_shard0, _container_a) = setup_database().await;
    let (url_shard1, _container_b) = setup_database().await;
    let pool0 = build_pool(&url_shard0);
    let pool1 = build_pool(&url_shard1);

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), pool0);
    shard_pools.insert(ShardId::new(1), pool1.clone());
    let sharded_pool = ShardedDbPool::from_map(shard_pools, ShardId::new(0));

    let client = WorkflowHandleClient::new(
        sharded_pool,
        router,
        [
            (ShardId::new(0), url_shard0.clone()),
            (ShardId::new(1), url_shard1.clone()),
        ],
    )
    .with_workflows(all_test_workflows());

    // The caller's connection is to SHARD 1's database.
    let mut shard1_conn = connect(&url_shard1).await;
    let outcome = Box::pin(shard1_conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-shard-1",
                    serde_json::json!({"order_id": "order-shard-1", "amount_cents": 1}),
                    TransactionalStartOptions::new().with_shard(ShardId::new(1)),
                )
                .await
        }
    }))
    .await
    .expect("start on shard 1 must succeed");

    assert_eq!(
        outcome.exec_id.shard(),
        ShardId::new(1),
        "the assigned exec_id must decode to shard 1"
    );

    // The execution row physically exists on shard 1's database...
    let mut fresh_shard1 = connect(&url_shard1).await;
    assert_eq!(
        execution_row_count(&mut fresh_shard1, "order-shard-1").await,
        1
    );

    // ...and does NOT exist on shard 0's database — never a cross-shard write.
    let mut fresh_shard0 = connect(&url_shard0).await;
    assert_eq!(
        execution_row_count(&mut fresh_shard0, "order-shard-1").await,
        0
    );
}

/// A multi-shard client cannot infer which shard the caller's connection
/// belongs to, so omitting `.with_shard(...)` must be rejected rather than
/// silently minting an execution id whose *encoded* shard disagrees with
/// wherever the row actually landed (which would make it unreachable by any
/// later exec-id-routed lookup — signal, cancel, describe, ...).
#[tokio::test]
async fn multi_shard_client_without_with_shard_is_rejected_writes_nothing() {
    let (url_shard0, _container_a) = setup_database().await;
    let (url_shard1, _container_b) = setup_database().await;
    let pool0 = build_pool(&url_shard0);
    let pool1 = build_pool(&url_shard1);

    let router = ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(ShardId::new(0), pool0);
    shard_pools.insert(ShardId::new(1), pool1);
    let sharded_pool = ShardedDbPool::from_map(shard_pools, ShardId::new(0));

    let client = WorkflowHandleClient::new(
        sharded_pool,
        router,
        [
            (ShardId::new(0), url_shard0.clone()),
            (ShardId::new(1), url_shard1.clone()),
        ],
    )
    .with_workflows(all_test_workflows());

    let mut shard1_conn = connect(&url_shard1).await;
    let result: Result<TransactionalStartOutcome, HarvestError> =
        Box::pin(shard1_conn.transaction::<_, HarvestError, _>({
            let client = client.clone();
            async move |conn| {
                client
                    .start_workflow_transactional(
                        conn,
                        "t763_plain_workflow",
                        "order-no-shard-specified",
                        serde_json::json!({"order_id": "order-no-shard-specified", "amount_cents": 1}),
                        TransactionalStartOptions::new(),
                    )
                    .await
            }
        }))
        .await;

    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "expected Config rejection when a multi-shard client omits .with_shard(...), got {result:?}"
    );

    let mut fresh_shard0 = connect(&url_shard0).await;
    let mut fresh_shard1 = connect(&url_shard1).await;
    assert_eq!(
        execution_row_count(&mut fresh_shard0, "order-no-shard-specified").await,
        0
    );
    assert_eq!(
        execution_row_count(&mut fresh_shard1, "order-no-shard-specified").await,
        0
    );
}

#[tokio::test]
async fn single_shard_deployment_needs_no_with_shard_call() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    // `single_shard_client` never registers more than one shard, and this
    // test never calls `.with_shard(...)` — mirroring AC4's "single-shard
    // deployment needs no caller changes beyond passing the connection".
    let client = single_shard_client(&database_url);

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-single-shard-1",
                    serde_json::json!({"order_id": "order-single-shard-1", "amount_cents": 1}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("single-shard start must succeed with no shard option");

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        execution_row_count(&mut fresh, "order-single-shard-1").await,
        1
    );
    // An un-pinned transactional start mints its `ExecutionId` with the
    // caller's client's CONCRETELY resolved default shard (issue #763 Codex
    // review, "Default transactional starts to the actual shard") — never the
    // bare `UNENCODED` sentinel, even in the single-shard case. This client's
    // router is `ShardRouter::single()`, whose `default_shard()` is
    // `ShardId::new(0)`, so this is byte-for-byte the "pre-sharding runtime"
    // shard for a single-shard deployment. The point is that a single-shard
    // deployment whose one shard is numbered something OTHER than 0 (see
    // `unpinned_start_on_a_non_default_single_shard_client_encodes_that_shard_not_zero`
    // below) now correctly encodes ITS shard instead of leaving the id
    // unencoded and letting the row's persisted `shard_id` column silently
    // default to 0 regardless — a single-shard deployment still needs no
    // shard-aware code at all, matching AC4.
    assert_eq!(outcome.exec_id.shard(), ShardId::new(0));
}

/// Codex review (issue #763), "Default transactional starts to the actual
/// shard": a client built from a genuinely single-shard `ShardedDbPool`/
/// `ShardRouter` pair whose one shard is NOT numbered 0 (carved out of a
/// larger topology, or simply configured that way) must have an un-pinned
/// start encode THAT shard — both in the returned `exec_id` and in the row's
/// persisted `shard_id` column — never the bare `UNENCODED` sentinel and
/// never a hardcoded 0. Before the fix, `StartWorkflowParams::shard_id()`
/// (which persists the column every shard-scoped admission gate and the
/// idempotency purge scanner reads) hardcoded 0 for an unencoded id
/// regardless of the actual configured default shard, silently mislabeling
/// this write's shard.
#[tokio::test]
async fn unpinned_start_on_a_non_default_single_shard_client_encodes_that_shard_not_zero() {
    let (database_url, _container) = setup_database().await;
    let shard = ShardId::new(5);
    let pool = build_pool(&database_url);
    let router = ShardRouter::new(vec![shard], vec![shard], shard);
    let mut shard_pools = std::collections::BTreeMap::new();
    shard_pools.insert(shard, pool);
    let sharded_pool = ShardedDbPool::from_map(shard_pools, shard);

    let client = WorkflowHandleClient::new(sharded_pool, router, [(shard, database_url.clone())])
        .with_workflows(all_test_workflows());

    let mut conn = connect(&database_url).await;
    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-single-nondefault-shard-1",
                    serde_json::json!({
                        "order_id": "order-single-nondefault-shard-1",
                        "amount_cents": 1
                    }),
                    // Deliberately NO `.with_shard(...)` — the whole point is
                    // that `pools.len() == 1` lets the caller omit it, and
                    // the fallback must still resolve to shard 5, not 0.
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("single-shard (non-default-numbered) start must succeed with no shard option");

    assert_eq!(
        outcome.exec_id.shard(),
        shard,
        "the returned exec_id must encode this client's actual shard (5), \
         not the UNENCODED sentinel and not shard 0"
    );

    let mut fresh = connect(&database_url).await;
    assert_eq!(
        persisted_shard_id(&mut fresh, outcome.exec_id).await,
        5,
        "the row's persisted shard_id column must also read 5, not 0 — this \
         is what every shard-scoped admission gate and the idempotency purge \
         scanner actually consult"
    );
}

// ---------------------------------------------------------------------------
// finish()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn finish_is_a_safe_no_op_when_no_follow_ups_were_produced() {
    let (database_url, _container) = setup_database().await;
    let mut conn = connect(&database_url).await;
    let client = single_shard_client(&database_url);

    let outcome = Box::pin(conn.transaction::<_, HarvestError, _>({
        let client = client.clone();
        async move |conn| {
            client
                .start_workflow_transactional(
                    conn,
                    "t763_plain_workflow",
                    "order-finish-1",
                    serde_json::json!({"order_id": "order-finish-1", "amount_cents": 1}),
                    TransactionalStartOptions::new(),
                )
                .await
        }
    }))
    .await
    .expect("start must succeed");

    // Must not panic or hang for the overwhelmingly common empty-follow-up case.
    outcome.finish().await;
}
