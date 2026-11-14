#![cfg(feature = "db")]
// Issue #751: workflow_id-targeted in-workflow signal and cancel primitives.
//
// This file proves the DB-level behaviour of two new primitives end to end:
// `WorkflowContext::signal_external_workflow_by_id[_with_idempotency]` and
// `request_cancel_external_workflow_by_id`. It also covers the resolver
// functions under them (`signal::resolve_and_signal_by_workflow_id`,
// `execution::resolve_and_cancel_by_workflow_id`).
//
// Structure, cheapest-and-most-decisive first:
//   1. Resolver-level tests (no worker needed) prove the outcome matrix
//      (`ByIdSignalOutcome` / `ByIdCancelOutcome`). They include a
//      lock-choreographed proof of the exact continue-as-new race window
//      (AC2/AC3 "closed by construction").
//   2. Randomized race tests (AC3 headline: N >= 100 randomized interleavings,
//      0 misdeliveries) use concurrent tokio tasks. Each races a real
//      continue-as-new against a real by-id delivery attempt.
//   3. End-to-end tests through a real `Worker` prove the public
//      `WorkflowContext` API. They cover inline dispatch, the outbox scanner,
//      cross-shard routing, the grace window, and self-targeting rejection.

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{self, ByIdCancelOutcome};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::schema::harvest_workflow_executions_ext;
use autumn_harvest::signal::{self, ByIdSignalOutcome};
use autumn_harvest::store;
use autumn_harvest::table_split::SetExt;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

static TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn setup_test_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        use diesel_async::SimpleAsyncConnection;
        let db_name = format!("harvest751_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&base_url)
            .await
            .expect("failed to connect to HARVEST_TEST_DATABASE_URL base");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("failed to create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&new_url)
            .await
            .expect("failed to connect to per-test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("failed to apply migrations to per-test database");
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
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

    (database_url, Some(container))
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("failed to build test pool")
}

async fn connect(database_url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client")
}

async fn load_execution_from_url(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(database_url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

/// Insert a bare `RUNNING` execution row for resolver-level tests, with no
/// worker or handler.
async fn insert_running_row(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    exec_id: ExecutionId,
) {
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: exec_id.shard().as_i32(),
        input: serde_json::json!({}).into(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
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
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert running row");
}

/// Atomically seal `pred` to `CONTINUED_AS_NEW` and insert a fresh `RUNNING`
/// successor under the same `(workflow_name, workflow_id)`.
///
/// The real continue-as-new path gives the same guarantee: the predecessor
/// seal and the successor insert commit in ONE transaction. The "closed by
/// construction" race-safety claim rests on it. A concurrent resolver sees
/// only the whole pre-image or post-image, never a torn state.
///
/// The seal requires `pred` to be `RUNNING` at UPDATE time, as in production.
/// The SAME decision cycle that processes `pred` issues its continue-as-new.
/// So it never overwrites an external terminal transition, such as a
/// concurrent cancel, that reached `pred` first. An unguarded `UPDATE` would
/// let this helper cause a "misdelivery" that the real engine does not have.
/// Returns whether the seal, and so the successor insert, happened.
async fn continue_as_new_atomically(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    pred: ExecutionId,
    succ: ExecutionId,
) -> bool {
    Box::pin(conn.transaction(async |tx| {
        let sealed = diesel::update(
            harvest_workflow_executions::table
                .find(pred.as_uuid())
                .filter(harvest_workflow_executions::state.eq("RUNNING")),
        )
        .set(harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"))
        .execute(tx)
        .await
        .map_err(autumn_harvest::error::database_error)?;
        if sealed == 0 {
            // `pred` is no longer RUNNING, for example after a concurrent
            // cancel. A real continue-as-new stops here too.
            return Ok::<bool, HarvestError>(false);
        }

        let row = NewWorkflowExecution {
            quota_key: None,
            continued_from_exec_id: Some(pred.as_uuid()),
            first_exec_id: Some(pred.as_uuid()),
            id: succ.as_uuid(),
            workflow_name,
            workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: succ.shard().as_i32(),
            input: serde_json::json!({}).into(),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
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
            origin: None,
            completion_callbacks: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
            tenant: None,
        };
        diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .execute(tx)
            .await
            .map_err(autumn_harvest::error::database_error)?;

        Ok::<bool, HarvestError>(true)
    }))
    .await
    .expect("continue-as-new transaction should commit")
}

async fn load_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(conn)
        .await
        .expect("execution row should exist")
}

async fn hold_execution_row_lock(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    conn.batch_execute("BEGIN")
        .await
        .expect("begin should succeed");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .expect("lock execution row");
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Resolver-level tests (no worker needed)
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn resolver_signal_delivers_to_running_predecessor() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let pred = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-1", pred).await;

    let outcome = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-1",
        "ping",
        serde_json::json!({"hello": "world"}),
        None,
    )
    .await
    .expect("resolve+signal should succeed");
    assert_eq!(outcome, ByIdSignalOutcome::Delivered);

    let pending = signal::load_pending_signals(&conn, pred)
        .await
        .expect("load pending signals");
    assert_eq!(pending.len(), 1, "signal must be queued against pred");
}

#[tokio::test]
async fn resolver_signal_delivers_to_successor_after_can() {
    // AC2: resolution uses the run current AT DELIVERY TIME, not the run that
    // existed when the caller first formed its intent.
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let pred = ExecutionId::new_for_shard(ShardId::new(0));
    let succ = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-2", pred).await;
    continue_as_new_atomically(&mut conn, "resolver_wf", "res-2", pred, succ).await;

    let outcome = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-2",
        "ping",
        serde_json::json!({}),
        None,
    )
    .await
    .expect("resolve+signal should succeed");
    assert_eq!(outcome, ByIdSignalOutcome::Delivered);

    let pending_pred = signal::load_pending_signals(&conn, pred)
        .await
        .expect("load pending signals for pred");
    assert!(
        pending_pred.is_empty(),
        "the stale predecessor must never receive the signal"
    );
    let pending_succ = signal::load_pending_signals(&conn, succ)
        .await
        .expect("load pending signals for succ");
    assert_eq!(
        pending_succ.len(),
        1,
        "the live successor must receive the signal"
    );
}

#[tokio::test]
async fn resolver_signal_no_run_found_ever_existed() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let outcome = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "never-existed",
        "ping",
        serde_json::json!({}),
        None,
    )
    .await
    .expect("resolve+signal should succeed");
    assert_eq!(outcome, ByIdSignalOutcome::NoRunFound);
}

#[tokio::test]
async fn resolver_signal_terminal_current_run_is_not_running() {
    // AC4: a signal to a workflow_id whose CURRENT run is terminal is a
    // genuine failure (never silently dropped, never success).
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-3", exec_id).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .expect("seal completed");

    let outcome = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-3",
        "ping",
        serde_json::json!({}),
        None,
    )
    .await
    .expect("resolve+signal should succeed");
    assert_eq!(outcome, ByIdSignalOutcome::NotRunning);

    let pending = signal::load_pending_signals(&conn, exec_id)
        .await
        .expect("load pending signals");
    assert!(pending.is_empty(), "no signal should ever be queued");
}

#[tokio::test]
async fn resolver_signal_keyed_retry_after_target_went_terminal_is_delivered_not_not_running() {
    // Regression (PR #1145, issue #751). In the cross-shard outbox path, the
    // TARGET-shard signal insert can commit while the CALLER-shard transaction
    // fails before it appends `ExternalSignalDelivered`. The outbox then
    // retries the same keyed request. Suppose the target completes before that
    // retry. An `is_terminal_state` check BEFORE delivery would then report
    // `NotRunning` for a signal that already landed. That check denies the
    // "insert before validate" dedup of `send_signal_idempotent` (issue #521)
    // any chance to see the key collision. A keyed retry that already landed
    // must dedupe to `Delivered`, whatever the target's current state.
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-keyed-1", exec_id).await;

    // Attempt 1: delivers successfully while the target is still RUNNING.
    let first = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-keyed-1",
        "ping",
        serde_json::json!({"n": 1}),
        Some("retry-key-1"),
    )
    .await
    .expect("first attempt should succeed");
    assert_eq!(first, ByIdSignalOutcome::Delivered);

    // The target processes the signal and completes before the caller-side
    // outbox retry runs.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .expect("seal completed");

    // Attempt 2 (the retry): the SAME idempotency key, now against a
    // terminal current run. Must report Delivered (already landed), never
    // NotRunning.
    let retry = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-keyed-1",
        "ping",
        serde_json::json!({"n": 1}),
        Some("retry-key-1"),
    )
    .await
    .expect("retry should succeed");
    assert_eq!(
        retry,
        ByIdSignalOutcome::Delivered,
        "a keyed retry that already landed must dedupe to Delivered even after \
         the target has since gone terminal"
    );

    let pending = signal::load_pending_signals(&conn, exec_id)
        .await
        .expect("load pending signals");
    assert_eq!(
        pending.len(),
        1,
        "exactly one signal must ever be queued for this key -- no duplicate insert"
    );
}

#[tokio::test]
async fn resolver_signal_keyed_retry_after_continue_as_new_does_not_double_deliver() {
    // Regression (PR #1145, issue #751). A cross-shard delivery commits on run
    // A, but the caller-side outcome append rolls back. A then continues as new
    // to B before the retry. A naive retry resolves to B, the new current run,
    // and attempts a FRESH insert there. The dedup of `send_signal_idempotent`
    // is scoped to (workflow_exec_id, idempotency_key). So a key used against
    // A's exec_id does not collide against B's. That yields TWO SignalReceived
    // events for one logical request. It breaks the exactly-once contract of
    // `_with_idempotency`. The dedup identity must survive execution rotation.
    // A call to `execution::lookup_idempotent_signal_dedupe` before a fresh
    // delivery closes this. `signal_with_start` uses the same lookup (issue
    // #244), scoped to (workflow_name, workflow_id, idempotency_key).
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let pred = ExecutionId::new_for_shard(ShardId::new(0));
    let succ = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-keyed-2", pred).await;

    // Attempt 1: delivers successfully to pred while it is still RUNNING.
    let first = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-keyed-2",
        "ping",
        serde_json::json!({"n": 1}),
        Some("retry-key-2"),
    )
    .await
    .expect("first attempt should succeed");
    assert_eq!(first, ByIdSignalOutcome::Delivered);

    // pred rotates to succ before the caller-side outbox retry runs.
    assert!(continue_as_new_atomically(&mut conn, "resolver_wf", "res-keyed-2", pred, succ).await);

    // Attempt 2 (the retry): the SAME idempotency key. Must recognise the
    // key as already delivered (to pred) and report Delivered WITHOUT
    // inserting a second, duplicate signal against succ.
    let retry = signal::resolve_and_signal_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "res-keyed-2",
        "ping",
        serde_json::json!({"n": 1}),
        Some("retry-key-2"),
    )
    .await
    .expect("retry should succeed");
    assert_eq!(
        retry,
        ByIdSignalOutcome::Delivered,
        "a keyed retry must recognise a delivery already recorded against a \
         predecessor execution in the same (workflow_name, workflow_id) chain"
    );

    let pending_succ = signal::load_pending_signals(&conn, succ)
        .await
        .expect("load pending signals for succ");
    assert!(
        pending_succ.is_empty(),
        "the retry must NOT insert a second, duplicate signal against the \
         continue-as-new successor -- the request was already fulfilled \
         against the predecessor"
    );
}

#[tokio::test]
async fn resolver_picks_most_recent_of_multiple_terminal_runs() {
    // No other test here has more than one terminal row per (workflow_name,
    // workflow_id). This proves that the live SQL `ORDER BY started_at DESC`
    // fallback picks the most-recently STARTED terminal row. It must not pick
    // the most-recently-inserted one. A check of the pure `select_resolved_run`
    // merge function against fabricated structs cannot prove that.
    //
    // Only ONE row per (workflow_name, workflow_id) may ever hold a state
    // OUTSIDE {CONTINUED_AS_NEW, TERMINATED}. That is the DB-level partial
    // unique index every OTHER helper in this file already relies on. So the
    // test models multiple terminal candidates as production does: a
    // continue-as-new chain that ends in one real terminal row. Each
    // predecessor in it is released to CONTINUED_AS_NEW. The chain goes in
    // with a DELIBERATELY non-chronological started_at order. The OLDEST
    // inserted row gets the LARGEST started_at. So a resolver that used
    // insertion or row-id order instead of `started_at` picks the wrong row.
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let now = chrono::Utc::now();
    let workflow_id = "res-multi-terminal";

    let predecessor_a = ExecutionId::new_for_shard(ShardId::new(0)); // inserted 1st, started_at newest -- must win
    let predecessor_b = ExecutionId::new_for_shard(ShardId::new(0)); // inserted 2nd, started_at middle
    let terminus = ExecutionId::new_for_shard(ShardId::new(0)); // inserted 3rd (last), started_at oldest

    insert_running_row(&mut conn, "resolver_wf", workflow_id, predecessor_a).await;
    diesel::update(harvest_workflow_executions::table.find(predecessor_a.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::started_at.eq(now - chrono::Duration::hours(1)),
        ))
        .execute(&mut conn)
        .await
        .expect("release predecessor_a from the active-uniqueness index");

    insert_running_row(&mut conn, "resolver_wf", workflow_id, predecessor_b).await;
    diesel::update(harvest_workflow_executions::table.find(predecessor_b.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::started_at.eq(now - chrono::Duration::hours(3)),
        ))
        .execute(&mut conn)
        .await
        .expect("release predecessor_b from the active-uniqueness index");

    insert_running_row(&mut conn, "resolver_wf", workflow_id, terminus).await;
    diesel::update(harvest_workflow_executions::table.find(terminus.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::started_at.eq(now - chrono::Duration::hours(5)),
        ))
        .execute(&mut conn)
        .await
        .expect("seal the chain terminus (inserted last, started_at earliest)");

    let resolved =
        execution::resolve_execution_id_by_workflow_id(&mut conn, "resolver_wf", workflow_id)
            .await
            .expect("resolve should succeed")
            .expect("a terminal run must resolve");

    assert_eq!(
        resolved.exec_id, predecessor_a,
        "resolver must pick the terminal row with the most recent started_at \
         (predecessor_a, CONTINUED_AS_NEW, -1h) -- not the FAILED terminus \
         (-5h, inserted last) or the middle predecessor (-3h)"
    );
    assert_eq!(resolved.state, "CONTINUED_AS_NEW");
}

/// Issue #1317: a reconciled `MIGRATED` seal
/// (`migrated_run_terminal_at` set) no longer represents anything current
/// for this business key. It must be excluded from the terminal fallback
/// outright, not merely lose an ordinary `started_at` tie-break. The seal
/// can be the ONLY row for this key on this shard, with no newer run to
/// naturally outrank it.
#[tokio::test]
async fn resolver_never_returns_a_reconciled_seal() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let workflow_id = "res-reconciled-seal-only";
    let seal = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", workflow_id, seal).await;
    diesel::update(harvest_workflow_executions::table.find(seal.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("MIGRATED"),
            SetExt(harvest_workflow_executions_ext::migrated_to_shard.eq(1)),
            SetExt(harvest_workflow_executions_ext::migrated_at.eq(chrono::Utc::now())),
            harvest_workflow_executions::migrated_run_terminal_at.eq(chrono::Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .expect("seed a reconciled seal as the only row for this key");

    let resolved =
        execution::resolve_execution_id_by_workflow_id(&mut conn, "resolver_wf", workflow_id)
            .await
            .expect("resolve should succeed");

    assert!(
        resolved.is_none(),
        "a reconciled seal must never be returned, even as the sole \
         candidate on this shard; got {resolved:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolver_signal_races_to_successor_exactly_mid_delivery() {
    // Deterministic, lock-choreographed proof of the race window (AC3). Hold
    // the predecessor row lock, so the resolve step of
    // `resolve_and_signal_by_workflow_id` sees it RUNNING. Queue the real
    // continue-as-new behind the lock. Release the lock so the CAN commits,
    // then let the delivery attempt proceed. Its internal check must detect
    // the mid-flight seal and report `RacedToSuccessor`. It must never report
    // a stale `Delivered` against the superseded predecessor.
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    let mut conn_lock = connect(&url).await;
    let mut conn_can = connect(&url).await;

    let pred = ExecutionId::new_for_shard(ShardId::new(0));
    let succ = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "res-race-1", pred).await;

    hold_execution_row_lock(&mut conn_lock, pred).await;

    let can_handle = tokio::spawn(async move {
        continue_as_new_atomically(&mut conn_can, "resolver_wf", "res-race-1", pred, succ).await;
    });
    // Give the CAN's UPDATE a moment to queue behind the held lock.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resolve_handle = tokio::spawn(async move {
        signal::resolve_and_signal_by_workflow_id(
            &mut conn,
            "resolver_wf",
            "res-race-1",
            "ping",
            serde_json::json!({}),
            None,
        )
        .await
    });
    // Give the resolve step time to run (it sees pred RUNNING, since the CAN
    // is still blocked on the row lock) before releasing the lock.
    tokio::time::sleep(Duration::from_millis(100)).await;
    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release lock");

    can_handle.await.expect("can task should not panic");
    let outcome = resolve_handle
        .await
        .expect("resolve task should not panic")
        .expect("resolve+signal should not error");

    assert_eq!(
        outcome,
        ByIdSignalOutcome::RacedToSuccessor,
        "a delivery attempt that resolved to a run which then raced to \
         CONTINUED_AS_NEW before delivery must report RacedToSuccessor, \
         never a false Delivered"
    );

    let verify_conn = connect(&url).await;
    let pending_pred = signal::load_pending_signals(&verify_conn, pred)
        .await
        .expect("load pending signals for pred");
    assert!(
        pending_pred.is_empty(),
        "no signal must ever land on the superseded predecessor"
    );
}

#[tokio::test]
async fn resolver_cancel_delivers_to_running_predecessor() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "can-res-1", exec_id).await;

    let outcome =
        execution::resolve_and_cancel_by_workflow_id(&mut conn, "resolver_wf", "can-res-1", "op")
            .await
            .expect("resolve+cancel should succeed");
    assert!(matches!(outcome, ByIdCancelOutcome::Cancelled { .. }));

    let state = load_state(&mut conn, exec_id).await;
    assert_eq!(state, "CANCELLED");
}

#[tokio::test]
async fn resolver_cancel_already_terminal_current_run_is_noop() {
    // AC5: a cancel to a workflow_id whose current run is already terminal is
    // a no-op success. Its goal, "the run is not active", is already met.
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "can-res-2", exec_id).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .expect("seal completed");

    let outcome =
        execution::resolve_and_cancel_by_workflow_id(&mut conn, "resolver_wf", "can-res-2", "op")
            .await
            .expect("resolve+cancel should succeed");
    assert!(matches!(outcome, ByIdCancelOutcome::AlreadyTerminal));

    let state = load_state(&mut conn, exec_id).await;
    assert_eq!(state, "COMPLETED", "the terminal state must be untouched");
}

#[tokio::test]
async fn resolver_cancel_no_run_found_ever_existed() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;

    let outcome = execution::resolve_and_cancel_by_workflow_id(
        &mut conn,
        "resolver_wf",
        "never-existed-cancel",
        "op",
    )
    .await
    .expect("resolve+cancel should succeed");
    assert!(matches!(outcome, ByIdCancelOutcome::NoRunFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolver_cancel_races_to_successor_exactly_mid_delivery() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let mut conn = connect(&url).await;
    let mut conn_lock = connect(&url).await;
    let mut conn_can = connect(&url).await;

    let pred = ExecutionId::new_for_shard(ShardId::new(0));
    let succ = ExecutionId::new_for_shard(ShardId::new(0));
    insert_running_row(&mut conn, "resolver_wf", "can-race-1", pred).await;

    hold_execution_row_lock(&mut conn_lock, pred).await;

    let can_handle = tokio::spawn(async move {
        continue_as_new_atomically(&mut conn_can, "resolver_wf", "can-race-1", pred, succ).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resolve_handle = tokio::spawn(async move {
        execution::resolve_and_cancel_by_workflow_id(&mut conn, "resolver_wf", "can-race-1", "op")
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    conn_lock
        .batch_execute("COMMIT")
        .await
        .expect("release lock");

    can_handle.await.expect("can task should not panic");
    let outcome = resolve_handle
        .await
        .expect("resolve task should not panic")
        .expect("resolve+cancel should not error");

    assert!(
        matches!(outcome, ByIdCancelOutcome::RacedToSuccessor),
        "a cancel attempt that resolved to a run which then raced to \
         CONTINUED_AS_NEW before delivery must report RacedToSuccessor, got {outcome:?}"
    );

    let mut verify_conn = connect(&url).await;
    let pred_state = load_state(&mut verify_conn, pred).await;
    assert_eq!(
        pred_state, "CONTINUED_AS_NEW",
        "the predecessor must be left exactly as the CAN sealed it"
    );
    let succ_state = load_state(&mut verify_conn, succ).await;
    assert_eq!(
        succ_state, "RUNNING",
        "the live successor must never be cancelled by a stale request"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Randomized continue-as-new race tests (AC3 headline: N >= 100
//    randomized interleavings, 0 misdeliveries)
// ─────────────────────────────────────────────────────────────────────────

const RACE_TRIALS: usize = 150;
const RACE_CONCURRENCY: usize = 15;
const RACE_DELAY_MICROS_MAX: u64 = 2000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn randomized_signal_by_id_continue_as_new_race_zero_misdeliveries() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(RACE_CONCURRENCY));

    // Insert all N predecessor rows up front on one shared connection. Then
    // race each pair's CAN against its by-id resolve+signal, with a random
    // micro-delay before each side. Real Tokio scheduling against the SAME
    // live Postgres then decides which side wins the commit race per trial.
    let mut setup_conn = connect(&url).await;
    let mut pairs = Vec::with_capacity(RACE_TRIALS);
    for i in 0..RACE_TRIALS {
        let wf_id = format!("race-sig-{i}");
        let pred = ExecutionId::new_for_shard(ShardId::new(0));
        let succ = ExecutionId::new_for_shard(ShardId::new(0));
        insert_running_row(&mut setup_conn, "race_sig_wf", &wf_id, pred).await;
        pairs.push((wf_id, pred, succ));
    }
    drop(setup_conn);

    let mut handles = Vec::with_capacity(RACE_TRIALS);
    for (wf_id, pred, succ) in pairs {
        let url = url.clone();
        let sem = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore not closed");

            let can_delay = Duration::from_micros(rand::random::<u64>() % RACE_DELAY_MICROS_MAX);
            let resolve_delay =
                Duration::from_micros(rand::random::<u64>() % RACE_DELAY_MICROS_MAX);

            let mut conn_can = connect(&url).await;
            let mut conn_resolve = connect(&url).await;
            let wf_id_can = wf_id.clone();
            let wf_id_resolve = wf_id.clone();

            let can_task = tokio::spawn(async move {
                tokio::time::sleep(can_delay).await;
                continue_as_new_atomically(&mut conn_can, "race_sig_wf", &wf_id_can, pred, succ)
                    .await
            });
            let resolve_task = tokio::spawn(async move {
                tokio::time::sleep(resolve_delay).await;
                let outcome = signal::resolve_and_signal_by_workflow_id(
                    &mut conn_resolve,
                    "race_sig_wf",
                    &wf_id_resolve,
                    "ping",
                    serde_json::json!({}),
                    None,
                )
                .await;
                (outcome, conn_resolve)
            });

            // Only this CAN mutates `pred`'s state in the signal race.
            // Signaling only inserts into `harvest_signals`. So the CAN's
            // `WHERE state = 'RUNNING'` guard is never declined here. Assert
            // that invariant explicitly. A future signal-delivery change that
            // touches execution state would otherwise break, unseen, the
            // assumption that `succ` always exists.
            let can_succeeded = can_task.await.expect("can task should not panic");
            assert!(
                can_succeeded,
                "the continue-as-new must always succeed in this scenario: \
                 nothing else touches pred's state while signaling"
            );
            let (outcome, mut conn_resolve) =
                resolve_task.await.expect("resolve task should not panic");
            let outcome = outcome.expect("resolve+signal should not error");

            // If the delivery attempt raced, retry now. Both tasks were
            // awaited above, so the CAN has committed. This proves eventual,
            // exactly-once delivery under the "leave pending, retry" contract
            // of `RacedToSuccessor`.
            let retried = if matches!(outcome, ByIdSignalOutcome::RacedToSuccessor) {
                Some(
                    signal::resolve_and_signal_by_workflow_id(
                        &mut conn_resolve,
                        "race_sig_wf",
                        &wf_id,
                        "ping",
                        serde_json::json!({}),
                        None,
                    )
                    .await
                    .expect("retry after RacedToSuccessor should not error"),
                )
            } else {
                None
            };

            (pred, succ, outcome, retried)
        }));
    }

    let verify_conn = connect(&url).await;
    let mut delivered_to_pred = 0usize;
    let mut delivered_to_succ = 0usize;
    let mut raced_then_retried = 0usize;
    for (i, h) in handles.into_iter().enumerate() {
        let (pred, succ, outcome, retried) = h.await.expect("trial task should not panic");
        let pred_pending = signal::load_pending_signals(&verify_conn, pred)
            .await
            .expect("load pred pending signals");
        let succ_pending = signal::load_pending_signals(&verify_conn, succ)
            .await
            .expect("load succ pending signals");

        match outcome {
            ByIdSignalOutcome::Delivered => {
                assert_eq!(
                    pred_pending.len() + succ_pending.len(),
                    1,
                    "trial {i}: exactly one signal must be queued total on a \
                     Delivered outcome — never zero (lost), never more than \
                     one (duplicated)"
                );
                if pred_pending.is_empty() {
                    delivered_to_succ += 1;
                } else {
                    delivered_to_pred += 1;
                }
            }
            ByIdSignalOutcome::RacedToSuccessor => {
                // The spawned trial task has ALREADY run its inline retry
                // before it returned. So `succ_pending`, loaded before this
                // match, may already show the post-retry delivery. Assert
                // only the invariant that holds whatever the retry timing:
                // the sealed predecessor NEVER receives a signal. The
                // `succ_pending_after` check below verifies exactly-once
                // delivery on the successor.
                assert!(
                    pred_pending.is_empty(),
                    "trial {i}: RacedToSuccessor must never queue a signal \
                     against the sealed predecessor"
                );
                raced_then_retried += 1;
                assert!(
                    matches!(retried, Some(ByIdSignalOutcome::Delivered)),
                    "trial {i}: retrying after RacedToSuccessor must deliver, \
                     got {retried:?}"
                );
                let pred_pending_after = signal::load_pending_signals(&verify_conn, pred)
                    .await
                    .expect("load pred pending signals after retry");
                let succ_pending_after = signal::load_pending_signals(&verify_conn, succ)
                    .await
                    .expect("load succ pending signals after retry");
                assert!(
                    pred_pending_after.is_empty(),
                    "trial {i}: the retry must never land on the sealed \
                     predecessor"
                );
                assert_eq!(
                    succ_pending_after.len(),
                    1,
                    "trial {i}: the retry must deliver exactly once to the \
                     live successor"
                );
            }
            other => panic!("trial {i}: unexpected outcome in race trial: {other:?}"),
        }
    }

    println!(
        "signal-by-id race distribution over {RACE_TRIALS} trials: \
         delivered_to_pred={delivered_to_pred} delivered_to_succ={delivered_to_succ} \
         raced_then_retried={raced_then_retried}"
    );
    assert_eq!(
        delivered_to_pred + delivered_to_succ + raced_then_retried,
        RACE_TRIALS,
        "every trial must resolve to exactly one accounted-for outcome"
    );
    // The success metric names randomized interleavings, not a specific mix.
    // Require that the fixture exercised BOTH sides of the "who commits
    // first" race, so this is not a vacuously-passing test.
    assert!(
        delivered_to_pred > 0,
        "fixture must exercise resolve-wins-the-race at least once"
    );
    assert!(
        raced_then_retried > 0 || delivered_to_succ > 0,
        "fixture must exercise can-wins-the-race at least once (either as \
         RacedToSuccessor or as a direct successor delivery)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn randomized_cancel_by_id_continue_as_new_race_zero_misdeliveries() {
    let _guard = TEST_MUTEX.lock().await;
    let (url, _c) = setup_test_database_url().await;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(RACE_CONCURRENCY));

    let mut setup_conn = connect(&url).await;
    let mut pairs = Vec::with_capacity(RACE_TRIALS);
    for i in 0..RACE_TRIALS {
        let wf_id = format!("race-can-{i}");
        let pred = ExecutionId::new_for_shard(ShardId::new(0));
        let succ = ExecutionId::new_for_shard(ShardId::new(0));
        insert_running_row(&mut setup_conn, "race_can_wf", &wf_id, pred).await;
        pairs.push((wf_id, pred, succ));
    }
    drop(setup_conn);

    let mut handles = Vec::with_capacity(RACE_TRIALS);
    for (wf_id, pred, succ) in pairs {
        let url = url.clone();
        let sem = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore not closed");

            let can_delay = Duration::from_micros(rand::random::<u64>() % RACE_DELAY_MICROS_MAX);
            let resolve_delay =
                Duration::from_micros(rand::random::<u64>() % RACE_DELAY_MICROS_MAX);

            let mut conn_can = connect(&url).await;
            let mut conn_resolve = connect(&url).await;
            let wf_id_can = wf_id.clone();
            let wf_id_resolve = wf_id.clone();

            let can_task = tokio::spawn(async move {
                tokio::time::sleep(can_delay).await;
                continue_as_new_atomically(&mut conn_can, "race_can_wf", &wf_id_can, pred, succ)
                    .await
            });
            let resolve_task = tokio::spawn(async move {
                tokio::time::sleep(resolve_delay).await;
                let outcome = execution::resolve_and_cancel_by_workflow_id(
                    &mut conn_resolve,
                    "race_can_wf",
                    &wf_id_resolve,
                    "op",
                )
                .await;
                (outcome, conn_resolve)
            });

            // Unlike the signal race, the cancel race can decline the CAN. A
            // winning `resolve_and_cancel_by_workflow_id` commits `pred` to
            // CANCELLED. The CAN's guarded `WHERE state = 'RUNNING'` UPDATE
            // then matches 0 rows, and `succ`'s row is NEVER inserted. So
            // capture the observed outcome here. Do not re-derive it later
            // from the row of `succ`, which may not exist.
            let can_succeeded = can_task.await.expect("can task should not panic");
            let (outcome, mut conn_resolve) =
                resolve_task.await.expect("resolve task should not panic");
            let outcome = outcome.expect("resolve+cancel should not error");

            let retried = if matches!(outcome, ByIdCancelOutcome::RacedToSuccessor) {
                Some(
                    execution::resolve_and_cancel_by_workflow_id(
                        &mut conn_resolve,
                        "race_can_wf",
                        &wf_id,
                        "op",
                    )
                    .await
                    .expect("retry after RacedToSuccessor should not error"),
                )
            } else {
                None
            };

            (pred, succ, can_succeeded, outcome, retried)
        }));
    }

    let mut verify_conn = connect(&url).await;
    let mut cancelled_pred_directly = 0usize;
    let mut cancelled_succ_directly = 0usize;
    let mut cancelled_succ_after_retry = 0usize;
    for (i, h) in handles.into_iter().enumerate() {
        let (pred, succ, can_succeeded, outcome, retried) =
            h.await.expect("trial task should not panic");

        match outcome {
            ByIdCancelOutcome::Cancelled { .. } => {
                // Branch on the observed `can_succeeded`, not on row state.
                // A declined CAN never inserts the row of `succ`, so
                // `load_state(&mut verify_conn, succ)` would then panic.
                let pred_state = load_state(&mut verify_conn, pred).await;
                if can_succeeded {
                    // The CAN committed. A direct `Cancelled` outcome is then
                    // reachable only when the resolve step ran entirely after
                    // the CAN's commit and saw `succ` as current. That resolve
                    // never passed through the `RacedToSuccessor` window.
                    assert_eq!(
                        pred_state, "CONTINUED_AS_NEW",
                        "trial {i}: once the CAN succeeds, pred must be \
                         sealed and never touched by a subsequent cancel"
                    );
                    let succ_state = load_state(&mut verify_conn, succ).await;
                    assert_eq!(
                        succ_state, "CANCELLED",
                        "trial {i}: a direct Cancelled outcome after a \
                         successful CAN must have landed on the live \
                         successor"
                    );
                    cancelled_succ_directly += 1;
                } else {
                    // The CAN was declined. Only the cancel's own write on
                    // pred's row causes that, because the CAN's guarded
                    // UPDATE then matches 0 rows. So pred is the cancelled run.
                    assert_eq!(
                        pred_state, "CANCELLED",
                        "trial {i}: when the CAN is declined, the cancel \
                         that declined it must be the one that landed on \
                         pred — a genuine misdelivery otherwise"
                    );
                    cancelled_pred_directly += 1;
                }
            }
            ByIdCancelOutcome::RacedToSuccessor => {
                assert!(
                    can_succeeded,
                    "trial {i}: RacedToSuccessor can only occur once the \
                     CAN has actually sealed the predecessor"
                );
                let pred_state = load_state(&mut verify_conn, pred).await;
                assert_eq!(
                    pred_state, "CONTINUED_AS_NEW",
                    "trial {i}: RacedToSuccessor must leave the predecessor \
                     exactly as the CAN sealed it, never cancelled"
                );
                // The spawned trial task has ALREADY run its inline retry, so
                // `succ`'s state here shows the post-retry cancellation. Only
                // the settled invariant, checked via `retried` and
                // `succ_state_after` below, means anything here.
                match retried {
                    Some(ByIdCancelOutcome::Cancelled { .. }) => {}
                    other => panic!(
                        "trial {i}: retry after RacedToSuccessor must \
                         cancel, got {other:?}"
                    ),
                }
                let succ_state_after = load_state(&mut verify_conn, succ).await;
                assert_eq!(
                    succ_state_after, "CANCELLED",
                    "trial {i}: the retry must cancel the live successor"
                );
                cancelled_succ_after_retry += 1;
            }
            other => panic!("trial {i}: unexpected outcome in race trial: {other:?}"),
        }
    }

    println!(
        "cancel-by-id race distribution over {RACE_TRIALS} trials: \
         cancelled_pred_directly={cancelled_pred_directly} \
         cancelled_succ_directly={cancelled_succ_directly} \
         cancelled_succ_after_retry={cancelled_succ_after_retry}"
    );
    assert_eq!(
        cancelled_pred_directly + cancelled_succ_directly + cancelled_succ_after_retry,
        RACE_TRIALS,
        "every trial must resolve to exactly one accounted-for outcome"
    );
    assert!(
        cancelled_pred_directly > 0,
        "fixture must exercise resolve-wins-the-race at least once"
    );
    assert!(
        cancelled_succ_after_retry > 0 || cancelled_succ_directly > 0,
        "fixture must exercise can-wins-the-race at least once (either as \
         RacedToSuccessor-then-retry or as a direct successor cancel)"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 3. End-to-end tests through a real `Worker`. They prove the public
//    `WorkflowContext` API (`signal_external_workflow_by_id[_with_idempotency]`,
//    `request_cancel_external_workflow_by_id`) through the whole pipeline.
//    That covers inline dispatch, the outbox scanner, cross-shard routing, the
//    grace window, and self-targeting rejection.
// ─────────────────────────────────────────────────────────────────────────

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "workflow_id_targeted_tests",
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

fn default_start_params(
    exec_id: ExecutionId,
    workflow_name: &'static str,
    workflow_id: &'static str,
    input: serde_json::Value,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        exec_id,
        workflow_name,
        workflow_id,
        input: input.into(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        sla: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::default(),
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        workflow_retry_source: autumn_harvest::workflow_retry::RetryPolicySource::Type,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
        fairness_key: None,
        tenant: None,
    }
}

async fn e2e_load_execution(database_url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect fresh Postgres client for execution query");
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("failed to reload workflow execution")
}

// Target that parks on a plain signal wait and echoes the delivered payload.
fn by_id_signal_target_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload: serde_json::Value = ctx
            .receive_signal("ping")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signalled", "payload": payload}))
    })
}

// Completes immediately — used as an "already terminal by the time the
// caller acts" target (AC5).
fn by_id_instant_complete_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({"status": "done"})) })
}

// Target that continues-as-new exactly once. The successor then parks on the
// same plain signal wait as `by_id_signal_target_workflow`. AC2 end to end:
// resolution must follow to the live successor.
fn by_id_can_target_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let is_predecessor = input.get("phase").and_then(|v| v.as_str()) == Some("predecessor");
        if is_predecessor {
            ctx.continue_as_new(serde_json::json!({"phase": "successor"}))
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("continue_as_new parks forever; this line is never reached");
        }
        let payload: serde_json::Value = ctx
            .receive_signal("ping")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"status": "signalled", "payload": payload}))
    })
}

// Caller that signals a target addressed purely by `(workflow_name,
// workflow_id)`. It returns the typed outcome or reason_code in its output
// instead of an error. So the test can assert a terminal COMPLETED state
// either way.
fn by_id_signaller_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let workflow_name = input["workflow_name"]
            .as_str()
            .ok_or("missing workflow_name")?
            .to_string();
        let workflow_id = input["workflow_id"]
            .as_str()
            .ok_or("missing workflow_id")?
            .to_string();
        match ctx
            .signal_external_workflow_by_id(
                &workflow_name,
                &workflow_id,
                "ping",
                serde_json::json!({"hello": "world"}),
            )
            .await
        {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => {
                Ok(serde_json::json!({"result": "failed", "reason_code": reason_code}))
            }
            Err(other) => Err(other.to_string()),
        }
    })
}

// Same shape as `by_id_signaller_workflow`, but targets ITSELF. It proves
// self-targeting rejection (AC6) end to end through the real worker. The
// rejection itself needs no DB round trip.
fn by_id_self_signaller_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let name = ctx.workflow_type();
        let id = ctx.workflow_id();
        match ctx
            .signal_external_workflow_by_id(name, id, "ping", serde_json::json!({}))
            .await
        {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => {
                Ok(serde_json::json!({"result": "failed", "reason_code": reason_code}))
            }
            Err(other) => Err(other.to_string()),
        }
    })
}

// Caller that cancels a target addressed by `(workflow_name, workflow_id)`.
fn by_id_canceller_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let workflow_name = input["workflow_name"]
            .as_str()
            .ok_or("missing workflow_name")?
            .to_string();
        let workflow_id = input["workflow_id"]
            .as_str()
            .ok_or("missing workflow_id")?
            .to_string();
        match ctx
            .request_cancel_external_workflow_by_id(&workflow_name, &workflow_id)
            .await
        {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => {
                Ok(serde_json::json!({"result": "failed", "reason_code": reason_code}))
            }
            Err(other) => Err(other.to_string()),
        }
    })
}

// Self-cancel counterpart of `by_id_self_signaller_workflow` (AC6).
fn by_id_self_canceller_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let name = ctx.workflow_type();
        let id = ctx.workflow_id();
        match ctx.request_cancel_external_workflow_by_id(name, id).await {
            Ok(()) => Ok(serde_json::json!({"result": "delivered"})),
            Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => {
                Ok(serde_json::json!({"result": "failed", "reason_code": reason_code}))
            }
            Err(other) => Err(other.to_string()),
        }
    })
}

/// Build a single-shard test worker registered with `infos`, as in
/// `cross_workflow_*_tests.rs`.
///
/// It always installs a clean single-shard global router first.
/// `GLOBAL_SHARD_ROUTER` is a process-global static, shared by every test in
/// this test binary. An earlier test that installed another topology would
/// otherwise corrupt `WorkflowId` resolution for the by-id deliveries here.
fn build_e2e_worker(
    pool: &DbPool,
    infos: Vec<WorkflowInfo>,
    worker_id: &str,
    grace_window: Option<Duration>,
) -> Arc<Worker> {
    autumn_harvest::shard::install_global_router(autumn_harvest::shard::ShardRouter::single());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    let mut builder = HarvestBuilder::new()
        .workflows(infos)
        .worker(WorkerConfig::default());
    if let Some(window) = grace_window {
        builder = builder.unknown_target_grace_window(window);
    }
    let built = builder.build();

    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);

    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn e2e_assert_has_event(events: &[WorkflowEvent], name: &str) {
    assert!(
        events.iter().any(|e| e.type_name() == name),
        "expected event {name} in history; got: {:?}",
        events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );
}

fn e2e_assert_no_event(events: &[WorkflowEvent], name: &str) {
    assert!(
        !events.iter().any(|e| e.type_name() == name),
        "expected NO {name} in history (self-targeting must be rejected before \
         any event append -- AC6); got: {:?}",
        events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );
}

// 1. Same-shard signal-by-id reaches the running target and delivers the
//    payload (AC1 baseline sanity through the real worker + public API).
#[tokio::test]
async fn worker_signal_external_workflow_by_id_reaches_running_target() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = build_e2e_worker(
        &pool,
        vec![
            wf_info("by_id_signal_target_workflow", by_id_signal_target_workflow),
            wf_info("by_id_signaller_workflow", by_id_signaller_workflow),
        ],
        "worker-e2e-sig-1",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_signal_target_workflow",
            "e2e-sig-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_signaller_workflow",
            "e2e-sig-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_signal_target_workflow",
                "workflow_id": "e2e-sig-target-1",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            let target = e2e_load_execution(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED" && target.state == "COMPLETED" {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflows should reach terminal state within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"}))
    );
    assert_eq!(
        target_final.output,
        Some(serde_json::json!({
            "status": "signalled",
            "payload": {"hello": "world"}
        }))
    );

    let caller_history = store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    e2e_assert_has_event(&caller_history.events, "ExternalSignalDelivered");

    worker.shutdown();
    let _ = handle.await;
}

// 2. AC2 end-to-end: the target continues-as-new once BEFORE the signaller
//    ever runs; resolution must land on the live successor, never the
//    already-sealed predecessor.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn worker_signal_external_workflow_by_id_reaches_live_successor_after_continue_as_new() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = build_e2e_worker(
        &pool,
        vec![
            wf_info("by_id_can_target_workflow", by_id_can_target_workflow),
            wf_info("by_id_signaller_workflow", by_id_signaller_workflow),
        ],
        "worker-e2e-sig-2",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_can_target_workflow",
            "e2e-can-target-1",
            serde_json::json!({"phase": "predecessor"}),
        ),
        None,
    )
    .await
    .unwrap();

    // Wait until the predecessor has continued-as-new AND the successor is
    // parked, then start the signaller. This isolates the property under
    // test, that resolution finds the live successor, from race timing. The
    // resolver-level and randomized tests above cover race timing.
    let successor_exec_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(Some(resolved)) = execution::resolve_execution_id_by_workflow_id(
                &mut conn,
                "by_id_can_target_workflow",
                "e2e-can-target-1",
            )
            .await
                && resolved.exec_id != target_exec_id
                && resolved.state == "RUNNING"
            {
                break resolved.exec_id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should continue-as-new within timeout");
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_signaller_workflow",
            "e2e-can-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_can_target_workflow",
                "workflow_id": "e2e-can-target-1",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let (caller_final, predecessor_final, successor_final) =
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let caller = e2e_load_execution(&database_url, caller_exec_id).await;
                let predecessor = e2e_load_execution(&database_url, target_exec_id).await;
                let successor = e2e_load_execution(&database_url, successor_exec_id).await;
                if caller.state == "COMPLETED" && successor.state == "COMPLETED" {
                    break (caller, predecessor, successor);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("workflows should reach terminal state within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"}))
    );
    assert_eq!(
        predecessor_final.state, "CONTINUED_AS_NEW",
        "the sealed predecessor must never be touched by the signal"
    );
    assert_eq!(
        successor_final.output,
        Some(serde_json::json!({
            "status": "signalled",
            "payload": {"hello": "world"}
        })),
        "the LIVE successor — not the predecessor — must receive the payload"
    );

    worker.shutdown();
    let _ = handle.await;
}

// 3. Same-shard cancel-by-id reaches the running target.
#[tokio::test]
async fn worker_request_cancel_external_workflow_by_id_reaches_running_target() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = build_e2e_worker(
        &pool,
        vec![
            wf_info("by_id_signal_target_workflow", by_id_signal_target_workflow),
            wf_info("by_id_canceller_workflow", by_id_canceller_workflow),
        ],
        "worker-e2e-can-1",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_signal_target_workflow",
            "e2e-can-target-2",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_canceller_workflow",
            "e2e-can-caller-2",
            serde_json::json!({
                "workflow_name": "by_id_signal_target_workflow",
                "workflow_id": "e2e-can-target-2",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            let target = e2e_load_execution(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED"
                && (target.state == "CANCELLED" || target.state == "FAILED")
            {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("workflows should reach terminal state within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"}))
    );
    assert!(
        target_final.state == "CANCELLED" || target_final.state == "FAILED",
        "target should be CANCELLED or FAILED, got {}",
        target_final.state
    );

    let caller_history = store::load_history(&mut conn, caller_exec_id)
        .await
        .unwrap();
    e2e_assert_has_event(&caller_history.events, "ExternalCancelDelivered");

    worker.shutdown();
    let _ = handle.await;
}

// 4. AC4 end-to-end: a signal to a never-started `workflow_id` is never
//    silently dropped. It fails with a typed `target_unknown` reason once the
//    grace window elapses. The test shortens that window for speed.
#[tokio::test]
async fn worker_signal_by_id_fails_target_unknown_after_grace_window() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = build_e2e_worker(
        &pool,
        vec![wf_info(
            "by_id_signaller_workflow",
            by_id_signaller_workflow,
        )],
        "worker-e2e-grace-1",
        Some(Duration::from_millis(200)),
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_signaller_workflow",
            "e2e-grace-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_signal_target_workflow",
                "workflow_id": "e2e-grace-never-existed",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let caller_final = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("caller should complete (having caught the typed failure) within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "failed", "reason_code": "target_unknown"})),
        "a workflow_id with no live run must eventually surface a typed \
         target_unknown failure, never silently dropped"
    );

    worker.shutdown();
    let _ = handle.await;
}

// 5. AC5 end-to-end: a cancel to a `workflow_id` whose current run is
//    already terminal is a no-op success. It never fails and never mutates
//    the terminal target.
#[tokio::test]
async fn worker_cancel_by_id_already_terminal_is_noop_success() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let caller_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let worker = build_e2e_worker(
        &pool,
        vec![
            wf_info(
                "by_id_instant_complete_workflow",
                by_id_instant_complete_workflow,
            ),
            wf_info("by_id_canceller_workflow", by_id_canceller_workflow),
        ],
        "worker-e2e-noop-1",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_instant_complete_workflow",
            "e2e-noop-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    // Wait until the target is confirmed terminal BEFORE starting the
    // canceller, isolating the "already terminal" case from the race-timing
    // concerns already covered elsewhere.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = e2e_load_execution(&database_url, target_exec_id).await;
            if target.state == "COMPLETED" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("target should complete within timeout");

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_canceller_workflow",
            "e2e-noop-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_instant_complete_workflow",
                "workflow_id": "e2e-noop-target-1",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let caller_final = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            if caller.state == "COMPLETED" {
                break caller;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("caller should complete within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"})),
        "a cancel whose target is already terminal is a no-op SUCCESS, \
         never a failure"
    );

    let target_after = e2e_load_execution(&database_url, target_exec_id).await;
    assert_eq!(
        target_after.state, "COMPLETED",
        "the already-terminal target must never be mutated by the cancel"
    );
    assert_eq!(
        target_after.output,
        Some(serde_json::json!({"status": "done"})),
        "the target's own recorded output must be untouched"
    );

    worker.shutdown();
    let _ = handle.await;
}

// 6/7 shared setup: pick two shard ids so the by-id rendezvous hash of the
// target's business key resolves to a DIFFERENT shard than the caller's. A
// real deployment's `start_or_load` places the target the same way. So the
// outbox's same-pool/cross-pool check really exercises the cross-pool branch.
fn e2e_cross_shard_ids(
    router: &autumn_harvest::shard::ShardRouter,
    target_workflow_name: &str,
    target_workflow_id: &str,
) -> (ExecutionId, ExecutionId) {
    let target_shard = router.pick_for_new_workflow(target_workflow_name, target_workflow_id);
    let caller_shard = if target_shard == ShardId::new(0) {
        ShardId::new(1)
    } else {
        ShardId::new(0)
    };
    (
        ExecutionId::new_for_shard(target_shard),
        ExecutionId::new_for_shard(caller_shard),
    )
}

// 6. AC8 end-to-end: signal-by-id delivered via the cross-shard outbox when
//    the target's rendezvous-hashed shard differs from the caller's.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn worker_cross_shard_signal_external_workflow_by_id() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let router = autumn_harvest::shard::ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    autumn_harvest::shard::install_global_router(router.clone());

    let (target_exec_id, caller_exec_id) = e2e_cross_shard_ids(
        &router,
        "by_id_signal_target_workflow",
        "cross-sig-target-1",
    );
    assert_ne!(
        target_exec_id.shard(),
        caller_exec_id.shard(),
        "test setup must guarantee a genuine cross-shard pairing"
    );

    // Both logical shards point at the same physical Postgres, as in
    // `test_cross_shard_cancel_via_outbox`. The test proves that the OUTBOX
    // routing decision (same-pool vs cross-pool, driven by
    // `external_target_owning_shard`) takes the cross-pool branch. It does
    // not need physically distinct databases.
    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), pool.clone());
    pools.insert(ShardId::new(1), pool.clone());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info("by_id_signal_target_workflow", by_id_signal_target_workflow),
            wf_info("by_id_signaller_workflow", by_id_signaller_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-e2e-cross-sig-1".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    // The outbox scanner (`enforce_external_signals_outbox`) filters candidate
    // *caller* executions by `execs.shard_id = ANY(shard_assignments)`,
    // defaulting to `[0]` when empty (see `timeout.rs`). Both mock shards are
    // physically the same Postgres instance here. But the caller's
    // `ExecutionId` is genuinely encoded with shard 0 or 1, whichever is NOT
    // the target's rendezvous-hashed shard. So the single worker must be
    // assigned both shards. Otherwise it never sees a caller row encoded on the
    // shard it is not watching, and the request stays pending forever.
    runtime_config.shard_assignments = vec![ShardId::new(0), ShardId::new(1)];
    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_signal_target_workflow",
            "cross-sig-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_signaller_workflow",
            "cross-sig-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_signal_target_workflow",
                "workflow_id": "cross-sig-target-1",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            let target = e2e_load_execution(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED" && target.state == "COMPLETED" {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .expect("cross-shard signal-by-id should complete within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"}))
    );
    assert_eq!(
        target_final.output,
        Some(serde_json::json!({
            "status": "signalled",
            "payload": {"hello": "world"}
        }))
    );

    worker.shutdown();
    let _ = handle.await;
}

// 7. AC8 end-to-end: cancel-by-id via the cross-shard outbox.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn worker_cross_shard_cancel_external_workflow_by_id() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let router = autumn_harvest::shard::ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    autumn_harvest::shard::install_global_router(router.clone());

    let (target_exec_id, caller_exec_id) = e2e_cross_shard_ids(
        &router,
        "by_id_signal_target_workflow",
        "cross-can-target-1",
    );
    assert_ne!(
        target_exec_id.shard(),
        caller_exec_id.shard(),
        "test setup must guarantee a genuine cross-shard pairing"
    );

    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), pool.clone());
    pools.insert(ShardId::new(1), pool.clone());
    let _sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info("by_id_signal_target_workflow", by_id_signal_target_workflow),
            wf_info("by_id_canceller_workflow", by_id_canceller_workflow),
        ])
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "worker-e2e-cross-can-1".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    // See the identical comment in `worker_cross_shard_signal_external_workflow_by_id`.
    // The outbox scanner filters caller executions by `shard_assignments`. That
    // must cover both mock shards, or a caller encoded on shard 1 is never
    // scanned.
    runtime_config.shard_assignments = vec![ShardId::new(0), ShardId::new(1)];
    let worker =
        Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            target_exec_id,
            "by_id_signal_target_workflow",
            "cross-can-target-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            caller_exec_id,
            "by_id_canceller_workflow",
            "cross-can-caller-1",
            serde_json::json!({
                "workflow_name": "by_id_signal_target_workflow",
                "workflow_id": "cross-can-target-1",
            }),
        ),
        None,
    )
    .await
    .unwrap();

    let (caller_final, target_final) = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let caller = e2e_load_execution(&database_url, caller_exec_id).await;
            let target = e2e_load_execution(&database_url, target_exec_id).await;
            if caller.state == "COMPLETED"
                && (target.state == "CANCELLED" || target.state == "FAILED")
            {
                break (caller, target);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .expect("cross-shard cancel-by-id should complete within timeout");

    assert_eq!(
        caller_final.output,
        Some(serde_json::json!({"result": "delivered"}))
    );
    assert!(
        target_final.state == "CANCELLED" || target_final.state == "FAILED",
        "target should be CANCELLED or FAILED, got {}",
        target_final.state
    );

    worker.shutdown();
    let _ = handle.await;
}

/// A minimal [`MetricsRecorder`] that only captures
/// `record_workflow_unfinished_handlers` calls, for asserting the exact
/// `(workflow_name, count)` the deferred unfinished-handler check reports.
#[derive(Default)]
struct UnfinishedHandlerRecorder {
    calls: std::sync::Mutex<Vec<(String, u64)>>,
}

impl autumn_harvest::telemetry::MetricsRecorder for UnfinishedHandlerRecorder {
    fn record_workflow_unfinished_handlers(&self, workflow_name: &str, _kind: &str, count: u64) {
        self.calls
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), count));
    }
}

// 7b. Issue #751: a cross-shard cancel's deferred
//     unfinished-update-handler check must run against the connection that
//     actually owns the checked execution id. It must not run unconditionally
//     against the caller's own shard connection. Otherwise the check silently
//     sees zero events, because the target's rows are not on that physical
//     database. It then never reports a genuinely unfinished update handler.
//
//     Unlike the other `worker_cross_shard_*` tests here, this test uses TWO
//     physically separate databases: two independent
//     `setup_test_database_url()` calls. The bug shows only when the wrong
//     connection cannot see the target's rows at all. One Postgres shared by
//     two logical shard ids cannot reproduce that, because every row lives in
//     the one database.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cross_shard_cancel_reports_unfinished_handlers_on_the_target_shard() {
    let _guard = TEST_MUTEX.lock().await;
    let (url_a, _container_a) = setup_test_database_url().await;
    let (url_b, _container_b) = setup_test_database_url().await;
    let pool_a = build_test_pool(&url_a);
    let pool_b = build_test_pool(&url_b);

    let router = autumn_harvest::shard::ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    autumn_harvest::shard::install_global_router(router.clone());

    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), pool_a.clone());
    pools.insert(ShardId::new(1), pool_b.clone());
    let sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let (target_exec_id, caller_exec_id) =
        e2e_cross_shard_ids(&router, "cross_can_unfinished_target", "cchu-target-1");
    assert_ne!(
        target_exec_id.shard(),
        caller_exec_id.shard(),
        "test setup must guarantee a genuine cross-shard pairing"
    );

    // Seed the target on ITS OWN (physically separate) shard's database:
    // RUNNING, with an admitted update handler that never resolved.
    let mut target_conn = sharded_pool
        .pool_for(target_exec_id.shard())
        .get()
        .await
        .unwrap();
    insert_running_row(
        &mut target_conn,
        "cross_can_unfinished_target",
        "cchu-target-1",
        target_exec_id,
    )
    .await;
    let update_id = autumn_harvest::types::UpdateId::new();
    store::append_events(
        &mut target_conn,
        target_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "approve".to_string(),
                input: serde_json::Value::Null,
                timestamp: chrono::Utc::now(),
            },
        ],
        1,
    )
    .await
    .expect("seed target history");

    // Seed the caller on its own shard's database with a pending
    // `ExternalCancelRequested` targeting the business key above -- exactly
    // the durable row `enforce_external_cancels_outbox` scans for.
    let mut caller_conn = sharded_pool
        .pool_for(caller_exec_id.shard())
        .get()
        .await
        .unwrap();
    insert_running_row(
        &mut caller_conn,
        "cross_can_unfinished_caller",
        "cchu-caller-1",
        caller_exec_id,
    )
    .await;
    let cancel_id = autumn_harvest::types::ExternalCancelId::new();
    store::append_events(
        &mut caller_conn,
        caller_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: autumn_harvest::types::ExternalTarget::WorkflowId {
                    workflow_name: "cross_can_unfinished_target".to_string(),
                    workflow_id: "cchu-target-1".to_string(),
                },
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");

    let recorder = Arc::new(UnfinishedHandlerRecorder::default());
    let processed = autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        recorder.as_ref(),
        Duration::from_secs(60),
        &Some(sharded_pool.clone()),
        &[caller_exec_id.shard()],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("cancel outbox sweep should succeed");
    // The sweep that ends a live run cancels and runs its follow-ups, and
    // leaves the terminal event to the next fan-out (issue #1313). The
    // deferred check under test belongs to the sweep that cancelled, so it has
    // already run here.
    assert_eq!(
        processed, 0,
        "the cancelling sweep resolves nothing yet, by design"
    );

    // The target must actually be CANCELLED on its own database.
    let target_after = harvest_workflow_executions::table
        .find(target_exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut target_conn)
        .await
        .expect("load target after cancel");
    assert_eq!(target_after.state, "CANCELLED");

    // The regression: the deferred unfinished-handler check must run against
    // the TARGET's shard and report the unfinished `approve` update. On the
    // caller's physically separate connection it would see zero events.
    let calls = recorder.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![("cross_can_unfinished_target".to_string(), 1)],
        "the unfinished-handler check must run against the target's own \
         shard and report exactly the one still-admitted update handler; \
         got: {calls:?}"
    );

    let processed = autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        recorder.as_ref(),
        Duration::from_secs(60),
        &Some(sharded_pool),
        &[caller_exec_id.shard()],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("second cancel outbox sweep should succeed");
    assert_eq!(
        processed, 1,
        "exactly one cancel-by-id delivery should be processed"
    );
}

/// A minimal [`MetricsRecorder`] for the cross-pool cancel-by-id follow-ups.
///
/// It captures the deferred unfinished-update-handler check, the
/// cancel-terminal metric, and the later, caller-side "external cancel sent"
/// metric. Test 7c uses it to prove that the terminal-shard follow-ups are
/// recorded immediately. That is before the caller-side outer transaction
/// has any chance to fail and discard them.
#[derive(Default)]
struct CrossPoolFollowupRecorder {
    unfinished_handler: std::sync::Mutex<Vec<(String, u64)>>,
    terminal: std::sync::Mutex<Vec<(String, String, autumn_harvest::telemetry::WorkflowStatus)>>,
    external_cancel_sent: std::sync::Mutex<Vec<(String, Option<String>)>>,
}

impl autumn_harvest::telemetry::MetricsRecorder for CrossPoolFollowupRecorder {
    fn record_workflow_unfinished_handlers(&self, workflow_name: &str, _kind: &str, count: u64) {
        self.unfinished_handler
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), count));
    }

    fn record_workflow_terminal(
        &self,
        workflow_name: &str,
        queue: &str,
        outcome: autumn_harvest::telemetry::WorkflowStatus,
    ) {
        self.terminal
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), queue.to_string(), outcome));
    }

    fn record_external_cancel_sent(&self, outcome: &str, reason_code: Option<&str>) {
        self.external_cancel_sent
            .lock()
            .unwrap()
            .push((outcome.to_string(), reason_code.map(str::to_string)));
    }
}

// 7c. Issue #751: a cross-shard cancel's target-side
//     commit is real and irrevocable the instant `attempt_cancel_delivery`
//     returns for the cross-pool branch. It runs on a *fresh* connection
//     with its own top-level `conn.transaction(...)` wrap, not a savepoint on
//     the caller's outer transaction. Suppose the accumulated follow-ups
//     (unfinished-handler check, terminal metric, deferred trigger starts)
//     wait for the *caller-side* outer commit. An unrelated failure of that
//     transaction then discards them permanently. One example is an error
//     from `store::append_events` or `queue::wake_workflow_task`. This test
//     forces it with an undecodable third row in the caller's history. A
//     retry sees the target as `AlreadyTerminal`, which produces no new
//     checks or metrics.
//
//     This test proves the fix. The target-shard follow-ups must be recorded
//     right after the target-side commit, on the still-open target
//     connection. They must not wait for the failing caller-side transaction.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cross_shard_cancel_target_followups_survive_a_failed_caller_side_commit() {
    let _guard = TEST_MUTEX.lock().await;
    let (url_a, _container_a) = setup_test_database_url().await;
    let (url_b, _container_b) = setup_test_database_url().await;
    let pool_a = build_test_pool(&url_a);
    let pool_b = build_test_pool(&url_b);

    let router = autumn_harvest::shard::ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    );
    autumn_harvest::shard::install_global_router(router.clone());

    let mut pools = std::collections::BTreeMap::new();
    pools.insert(ShardId::new(0), pool_a.clone());
    pools.insert(ShardId::new(1), pool_b.clone());
    let sharded_pool = autumn_harvest::shard::ShardedDbPool::from_map(pools, ShardId::new(0));

    let (target_exec_id, caller_exec_id) =
        e2e_cross_shard_ids(&router, "cross_can_followup_target", "ccfu-target-1");
    assert_ne!(
        target_exec_id.shard(),
        caller_exec_id.shard(),
        "test setup must guarantee a genuine cross-shard pairing"
    );

    // Seed the target on its own, physically separate shard database. It is
    // RUNNING, with an admitted update handler that never resolved. So the
    // deferred unfinished-handler check has something to report.
    let mut target_conn = sharded_pool
        .pool_for(target_exec_id.shard())
        .get()
        .await
        .unwrap();
    insert_running_row(
        &mut target_conn,
        "cross_can_followup_target",
        "ccfu-target-1",
        target_exec_id,
    )
    .await;
    let update_id = autumn_harvest::types::UpdateId::new();
    store::append_events(
        &mut target_conn,
        target_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "approve".to_string(),
                input: serde_json::Value::Null,
                timestamp: chrono::Utc::now(),
            },
        ],
        1,
    )
    .await
    .expect("seed target history");

    // Seed the caller with a pending `ExternalCancelRequested` for the
    // business key above, PLUS a deliberately undecodable third event row.
    // Its `event_type` is wrong, so the outbox scan (which filters on
    // `event_type = 'ExternalCancelRequested'`) still finds row #2. Its
    // `event_data` is malformed. So the LATER full-history
    // `lock_workflow_execution_and_load_history` call fails while it decodes
    // the row. That call runs only *after* the cross-pool target commit. This
    // reproduces a caller-side failure after a successful target-side commit,
    // with no timing or race dependency.
    let mut caller_conn = sharded_pool
        .pool_for(caller_exec_id.shard())
        .get()
        .await
        .unwrap();
    insert_running_row(
        &mut caller_conn,
        "cross_can_followup_caller",
        "ccfu-caller-1",
        caller_exec_id,
    )
    .await;
    let cancel_id = autumn_harvest::types::ExternalCancelId::new();
    store::append_events(
        &mut caller_conn,
        caller_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: autumn_harvest::types::ExternalTarget::WorkflowId {
                    workflow_name: "cross_can_followup_target".to_string(),
                    workflow_id: "ccfu-target-1".to_string(),
                },
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");
    diesel::insert_into(autumn_harvest::schema::harvest_events::table)
        .values(&autumn_harvest::models::NewHarvestEvent {
            workflow_exec_id: caller_exec_id.as_uuid(),
            event_id: 3,
            event_type: "SomeBogusUnknownVariant",
            event_data: serde_json::json!({"type": "SomeBogusUnknownVariant", "data": {}}),
        })
        .execute(&mut caller_conn)
        .await
        .expect("seed deliberately undecodable third row");

    let recorder = Arc::new(CrossPoolFollowupRecorder::default());
    // Sweep 1 does the cross-pool cancel and its follow-ups. It appends no
    // terminal event, because a cancel that ends a live run leaves that to the
    // next fan-out (issue #1313). So it never reaches the undecodable row.
    autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        recorder.as_ref(),
        Duration::from_secs(60),
        &Some(sharded_pool.clone()),
        &[caller_exec_id.shard()],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("the cancelling sweep should succeed");

    // Sweep 2 loads the caller's history to append the terminal event, and
    // fails on that row. The follow-ups under test belong to sweep 1 and are
    // already recorded, which is exactly what this failure must not undo.
    let outcome = autumn_harvest::timeout::enforce_external_cancels_outbox(
        &mut caller_conn,
        recorder.as_ref(),
        Duration::from_secs(60),
        &Some(sharded_pool),
        &[caller_exec_id.shard()],
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;

    // The caller-side outer transaction must genuinely fail -- proving the
    // failure mode this test reproduces is real, not accidentally absorbed.
    assert!(
        outcome.is_err(),
        "expected the caller-side outer transaction to fail while decoding \
         the deliberately undecodable third event row; got: {outcome:?}"
    );

    // The target must still be durably CANCELLED on its own database. The
    // cross-pool commit happened independently. The later caller-side
    // failure must not roll it back.
    let target_after = harvest_workflow_executions::table
        .find(target_exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut target_conn)
        .await
        .expect("load target after cancel");
    assert_eq!(
        target_after.state, "CANCELLED",
        "the target-side commit must be irrevocable regardless of what \
         happens later on the unrelated caller-side transaction"
    );

    // The regression: the deferred unfinished-handler check and the
    // cancel-terminal metric must be recorded right after the target-side
    // commit. Deferred past the failing caller-side transaction, they would
    // be lost permanently.
    let unfinished_calls = recorder.unfinished_handler.lock().unwrap().clone();
    assert_eq!(
        unfinished_calls,
        vec![("cross_can_followup_target".to_string(), 1)],
        "the unfinished-handler check must be recorded immediately, before \
         the failing caller-side commit, not discarded with it; \
         got: {unfinished_calls:?}"
    );
    let terminal_calls = recorder.terminal.lock().unwrap().clone();
    assert_eq!(
        terminal_calls,
        vec![(
            "cross_can_followup_target".to_string(),
            "default".to_string(),
            autumn_harvest::telemetry::WorkflowStatus::Cancelled,
        )],
        "the cancel-terminal metric must be recorded immediately, before \
         the failing caller-side commit, not discarded with it; \
         got: {terminal_calls:?}"
    );

    // Only the caller side calls `record_external_cancel_sent`, after the
    // failing history load and append. It must NOT have fired. That confirms
    // execution never reached that later step.
    let external_cancel_sent_calls = recorder.external_cancel_sent.lock().unwrap().clone();
    assert!(
        external_cancel_sent_calls.is_empty(),
        "record_external_cancel_sent must not fire once the caller-side \
         history load/append has failed; got: {external_cancel_sent_calls:?}"
    );
}

// 7d. Issue #751: the same-pool decision of the deferred unfinished-handler
//     check must compare *resolved connection pools*, not raw `ShardId`s. A
//     legacy, pre-sharding caller execution carries the `ShardId::UNENCODED`
//     sentinel. `exact_pool_for_execution` resolves it to the default shard's
//     pool via its own fallback. A raw `exec_id.shard() == caller_shard`
//     comparison instead sees an unencoded caller and an *encoded*
//     default-shard target as different shards. The same pool serves both.
//     The misclassification takes the "cross-shard" branch. It then asks that
//     pool for a SECOND connection while the first (`conn`) is still checked
//     out. That self-deadlocks under the documented pool-size-1
//     configuration, which this routing exists to protect (issue #688's
//     precedent).
//
//     This test uses a size-1 connection pool and bounds the call with a
//     timeout. So the pre-fix deadlock shows deterministically as a timeout,
//     not an indefinite hang.
#[tokio::test]
async fn cross_shard_cancel_deferred_check_does_not_deadlock_on_unencoded_caller_id() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;

    // A genuinely size-1 pool: any second `.get()` while the first
    // connection is still checked out would block forever without the fix.
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
    let pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(1)
        .build()
        .expect("failed to build size-1 test pool");

    let sharded_pool = autumn_harvest::shard::ShardedDbPool::single(pool.clone());

    // The caller carries the legacy `ShardId::UNENCODED` sentinel --
    // `exact_pool_for_execution` resolves this to the default shard's pool.
    let caller_exec_id = ExecutionId::new();
    assert!(
        caller_exec_id.shard().is_unencoded(),
        "test setup must produce a genuinely unencoded caller id"
    );
    // The target is explicitly encoded for shard 0 -- the same physical pool
    // the caller's sentinel falls back to under `ShardedDbPool::single`.
    let target_exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let mut conn = pool.get().await.unwrap();
    insert_running_row(
        &mut conn,
        "round5_target",
        "round5-target-1",
        target_exec_id,
    )
    .await;
    let update_id = autumn_harvest::types::UpdateId::new();
    store::append_events(
        &mut conn,
        target_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "approve".to_string(),
                input: serde_json::Value::Null,
                timestamp: chrono::Utc::now(),
            },
        ],
        1,
    )
    .await
    .expect("seed target history");

    insert_running_row(
        &mut conn,
        "round5_caller",
        "round5-caller-1",
        caller_exec_id,
    )
    .await;
    let cancel_id = autumn_harvest::types::ExternalCancelId::new();
    store::append_events(
        &mut conn,
        caller_exec_id,
        &[
            WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: autumn_harvest::types::ExternalTarget::ExecutionId(target_exec_id),
            },
        ],
        1,
    )
    .await
    .expect("seed caller history");

    // `conn` is the only connection of the size-1 pool. It stays checked out
    // for the whole outbox sweep below. A routing decision that wrongly asks
    // the same pool for a second connection then self-deadlocks.
    let recorder = Arc::new(UnfinishedHandlerRecorder::default());
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        autumn_harvest::timeout::enforce_external_cancels_outbox(
            &mut conn,
            recorder.as_ref(),
            Duration::from_secs(60),
            &Some(sharded_pool),
            &[caller_exec_id.shard()],
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        ),
    )
    .await;

    let processed = result
        .expect("must not deadlock: enforce_external_cancels_outbox timed out")
        .expect("cancel outbox sweep should succeed");
    assert_eq!(
        processed, 1,
        "exactly one cancel-by-id delivery should be processed"
    );

    let target_after = harvest_workflow_executions::table
        .find(target_exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load target after cancel");
    assert_eq!(target_after.state, "CANCELLED");

    let calls = recorder.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![("round5_target".to_string(), 1)],
        "the unfinished-handler check must still run (via the reused, \
         same-pool connection) even though the caller's id is unencoded; \
         got: {calls:?}"
    );
}

// 8. AC6 end-to-end: signalling own `(workflow_name, workflow_id)` is
//    rejected immediately, with no DB round trip for the rejection. The
//    workflow completes cleanly and surfaces the typed `self_signal` reason.
#[tokio::test]
async fn worker_self_signal_by_id_rejected_end_to_end() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let worker = build_e2e_worker(
        &pool,
        vec![wf_info(
            "by_id_self_signaller_workflow",
            by_id_self_signaller_workflow,
        )],
        "worker-e2e-self-sig-1",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            exec_id,
            "by_id_self_signaller_workflow",
            "e2e-self-sig-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    let final_exec = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let exec = e2e_load_execution(&database_url, exec_id).await;
            if exec.state == "COMPLETED" {
                break exec;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("self-signalling workflow should complete quickly (no DB round trip needed)");

    assert_eq!(
        final_exec.output,
        Some(serde_json::json!({"result": "failed", "reason_code": "self_signal"}))
    );

    // AC6: self-targeting is rejected BEFORE any event append -- assert this
    // directly against the recorded history, not just the workflow's output.
    let mut conn = pool.get().await.unwrap();
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    e2e_assert_no_event(&history.events, "ExternalSignalRequested");
    e2e_assert_no_event(&history.events, "ExternalSignalDelivered");
    e2e_assert_no_event(&history.events, "ExternalSignalFailed");

    worker.shutdown();
    let _ = handle.await;
}

// 9. AC6 end-to-end: cancelling own `(workflow_name, workflow_id)` is
//    rejected immediately with the typed `self_cancel` reason.
#[tokio::test]
async fn worker_self_cancel_by_id_rejected_end_to_end() {
    let _guard = TEST_MUTEX.lock().await;
    let (database_url, _container) = setup_test_database_url().await;
    let pool = build_test_pool(&database_url);

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let worker = build_e2e_worker(
        &pool,
        vec![wf_info(
            "by_id_self_canceller_workflow",
            by_id_self_canceller_workflow,
        )],
        "worker-e2e-self-can-1",
        None,
    );
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let handle = tokio::spawn(async move {
        runner.run(&pool_for_run).await;
    });

    let mut conn = pool.get().await.unwrap();
    start_or_load_workflow_execution(
        &mut conn,
        default_start_params(
            exec_id,
            "by_id_self_canceller_workflow",
            "e2e-self-can-1",
            serde_json::json!({}),
        ),
        None,
    )
    .await
    .unwrap();

    let final_exec = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let exec = e2e_load_execution(&database_url, exec_id).await;
            if exec.state == "COMPLETED" {
                break exec;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("self-cancelling workflow should complete quickly (no DB round trip needed)");

    assert_eq!(
        final_exec.output,
        Some(serde_json::json!({"result": "failed", "reason_code": "self_cancel"}))
    );

    // AC6: self-targeting is rejected BEFORE any event append -- assert this
    // directly against the recorded history, not just the workflow's output.
    let mut conn = pool.get().await.unwrap();
    let history = store::load_history(&mut conn, exec_id).await.unwrap();
    e2e_assert_no_event(&history.events, "ExternalCancelRequested");
    e2e_assert_no_event(&history.events, "ExternalCancelDelivered");
    e2e_assert_no_event(&history.events, "ExternalCancelFailed");

    worker.shutdown();
    let _ = handle.await;
}
