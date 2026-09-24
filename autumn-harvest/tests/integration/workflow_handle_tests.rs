//! `WorkflowHandle` request/response embedding tests.

use std::time::Duration;

use autumn_harvest::error::{HarvestError, TimeoutType};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{
    ExecutionId, Priority, StartWorkflowParams, WorkflowHandleClient, WorkflowIdReusePolicy,
    WorkflowResult, WorkflowResultState, start_or_load_workflow_execution,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_database_url() -> (String, ContainerAsync<Postgres>) {
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
        .expect("container postgres port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
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

/// Dual-mode database URL: a throwaway database against
/// `HARVEST_TEST_DATABASE_URL` when set, otherwise a fresh testcontainers
/// Postgres. Returns `None` in the container slot for the throwaway-database
/// path since there is no container to keep alive.
async fn setup_isolated_database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    use diesel_async::SimpleAsyncConnection;
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest1317_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&base_url)
            .await
            .expect("connect to HARVEST_TEST_DATABASE_URL base");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&new_url)
            .await
            .expect("connect to per-test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations to per-test database");
        return (new_url, None);
    }
    let (url, container) = setup_database_url().await;
    (url, Some(container))
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("test pool should build")
}

async fn start_running_workflow(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> autumn_harvest::StartedWorkflowExecution {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "echo",
            workflow_id: "echo-1",
            exec_id,
            input: serde_json::json!({"name": "Mina"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
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
        },
        None,
    )
    .await
    .expect("workflow should start")
}

async fn mark_completed(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    use autumn_harvest::WorkflowEvent;
    use autumn_harvest::error::HarvestError;
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let output = serde_json::json!({"ok": true});
    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        let output = output.clone();
        autumn_harvest::store::append_events(
            conn,
            exec_id,
            &[WorkflowEvent::WorkflowCompleted {
                output: output.clone(),
            }],
            1,
        )
        .await?;

        diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
            .set((
                dsl::state.eq("COMPLETED"),
                dsl::output.eq(Some(output)),
                dsl::completed_at.eq(Some(chrono::Utc::now())),
            ))
            .execute(conn)
            .await
            .map_err(autumn_harvest::error::database_error)?;
        Ok(())
    }))
    .await
    .expect("workflow row should complete");
}

#[tokio::test]
async fn workflow_event_listener_receives_append_notification() {
    let (database_url, _container) = setup_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let _started = start_running_workflow(&mut conn, exec_id).await;
    let mut listener = autumn_harvest::notify::WorkflowEventListener::connect(&database_url)
        .await
        .expect("listener should connect");

    mark_completed(&mut conn, exec_id).await;

    let outcome = tokio::time::timeout(Duration::from_secs(2), listener.wait_for_notification())
        .await
        .expect("listener should receive notification")
        .expect("notification payload should parse");

    assert!(matches!(
        outcome,
        autumn_harvest::notify::WorkflowEventWaitOutcome::Notification(payload)
            if payload.workflow_exec_id == exec_id.as_uuid()
                && payload.last_event_type == "WorkflowCompleted"
    ));
}

#[tokio::test]
async fn result_raw_with_timeout_returns_timeout_for_running_workflow() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let error = client
        .handle(started.exec_id)
        .result_raw_with_timeout(Duration::from_millis(50))
        .await
        .expect_err("running workflow should time out");

    assert!(matches!(
        error,
        HarvestError::Timeout {
            timeout_type: TimeoutType::ScheduleToClose,
            task_name
        } if task_name == "echo"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_raw_wakes_on_harvest_events_notification() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::new(
        ShardedDbPool::single(pool),
        ShardRouter::single(),
        [(autumn_harvest::ShardId::new(0), database_url.clone())],
    );
    let handle = client.handle(started.exec_id);

    let waiter = tokio::spawn(async move { handle.result_raw().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    mark_completed(&mut conn, exec_id).await;

    let result = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("handle should wake after completion notification")
        .expect("waiter task should not panic")
        .expect("result should be successful");

    assert_eq!(result, serde_json::json!({"ok": true}));
}

/// Issue #1317 review, P1: a listener stays bound to whichever shard it
/// resolved at connect time. If the execution migrates to a different
/// shard while `result_raw` is waiting, the old connection stays healthy,
/// with no `ChannelClosed`. Nothing wakes it again unless it rebinds on
/// its own.
///
/// This models the RESULT of a migration directly: copy the row onto the
/// new shard, seal the old one with a forwarding pointer. It skips the
/// full staging/verification/cutover machinery, which is orthogonal to
/// the listener-rebind behavior under test.
#[tokio::test]
async fn result_raw_rebinds_its_listener_after_a_migration() {
    #[derive(diesel::QueryableByName)]
    struct JsonRow {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        payload: serde_json::Value,
    }

    let (url_a, _container_a) = setup_isolated_database_url().await;
    let (url_b, _container_b) = setup_isolated_database_url().await;
    let pool_a = build_pool(&url_a);
    let pool_b = build_pool(&url_b);
    let mut conn_a = <AsyncPgConnection as AsyncConnection>::establish(&url_a)
        .await
        .expect("connect a");
    let mut conn_b = <AsyncPgConnection as AsyncConnection>::establish(&url_b)
        .await
        .expect("connect b");

    let shard_a = autumn_harvest::ShardId::new(0);
    let shard_b = autumn_harvest::ShardId::new(1);
    let exec_id = ExecutionId::new_for_shard(shard_a);
    let started = start_running_workflow(&mut conn_a, exec_id).await;

    let pools: std::collections::BTreeMap<_, _> =
        [(shard_a, pool_a), (shard_b, pool_b)].into_iter().collect();
    let sharded_pool = ShardedDbPool::from_map(pools, shard_a);
    let router = ShardRouter::new(vec![shard_a, shard_b], vec![shard_a, shard_b], shard_a);
    let client = WorkflowHandleClient::new(
        sharded_pool,
        router,
        [(shard_a, url_a.clone()), (shard_b, url_b.clone())],
    );
    let handle = client.handle(started.exec_id);

    let waiter = tokio::spawn(async move { handle.result_raw().await });
    // Give the waiter time to connect its listener to shard A first.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let row: JsonRow = diesel::sql_query(
        "SELECT to_jsonb(e) AS payload FROM harvest_workflow_executions e WHERE e.id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn_a)
    .await
    .expect("read the row to copy");

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             SELECT * FROM jsonb_populate_record( \
                 NULL::harvest_workflow_executions, \
                 $1::jsonb || jsonb_build_object('shard_id', $2::int))",
    )
    .bind::<diesel::sql_types::Jsonb, _>(&row.payload)
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_b)
    .await
    .expect("copy the row onto shard B");

    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
            SET state = 'MIGRATED', migrated_to_shard = $2, migrated_at = NOW() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_a)
    .await
    .expect("seal shard A's row with a forwarding pointer");

    // Give the shard-change check a chance to run against a listener still
    // bound to shard A, exactly as it would find a real in-flight migration.
    tokio::time::sleep(Duration::from_millis(200)).await;
    mark_completed(&mut conn_b, exec_id).await;

    let result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect(
            "the handle must rebind to shard B and wake on its notification, \
             not hang past the timeout waiting on shard A's stale listener",
        )
        .expect("waiter task should not panic")
        .expect("result should be successful");

    assert_eq!(result, serde_json::json!({"ok": true}));
}

/// Issue #1317 review, P1 follow-up (companion to the test above).
/// `result_raw_with_timeout` connects its listener once. Before this fix,
/// it waited the entire caller-supplied timeout on it. A migration
/// landing mid-wait would go unnoticed until that timeout elapsed, which
/// can be minutes or hours. The 30-second timeout used here would fail
/// this test's own 10-second outer timeout if the fix regressed.
#[tokio::test]
async fn result_raw_with_timeout_rebinds_its_listener_after_a_migration() {
    #[derive(diesel::QueryableByName)]
    struct JsonRow {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        payload: serde_json::Value,
    }

    let (url_a, _container_a) = setup_isolated_database_url().await;
    let (url_b, _container_b) = setup_isolated_database_url().await;
    let pool_a = build_pool(&url_a);
    let pool_b = build_pool(&url_b);
    let mut conn_a = <AsyncPgConnection as AsyncConnection>::establish(&url_a)
        .await
        .expect("connect a");
    let mut conn_b = <AsyncPgConnection as AsyncConnection>::establish(&url_b)
        .await
        .expect("connect b");

    let shard_a = autumn_harvest::ShardId::new(0);
    let shard_b = autumn_harvest::ShardId::new(1);
    let exec_id = ExecutionId::new_for_shard(shard_a);
    let started = start_running_workflow(&mut conn_a, exec_id).await;

    let pools: std::collections::BTreeMap<_, _> =
        [(shard_a, pool_a), (shard_b, pool_b)].into_iter().collect();
    let sharded_pool = ShardedDbPool::from_map(pools, shard_a);
    let router = ShardRouter::new(vec![shard_a, shard_b], vec![shard_a, shard_b], shard_a);
    let client = WorkflowHandleClient::new(
        sharded_pool,
        router,
        [(shard_a, url_a.clone()), (shard_b, url_b.clone())],
    );
    let handle = client.handle(started.exec_id);

    let waiter = tokio::spawn(async move {
        handle
            .result_raw_with_timeout(Duration::from_secs(30))
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let row: JsonRow = diesel::sql_query(
        "SELECT to_jsonb(e) AS payload FROM harvest_workflow_executions e WHERE e.id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn_a)
    .await
    .expect("read the row to copy");

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             SELECT * FROM jsonb_populate_record( \
                 NULL::harvest_workflow_executions, \
                 $1::jsonb || jsonb_build_object('shard_id', $2::int))",
    )
    .bind::<diesel::sql_types::Jsonb, _>(&row.payload)
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_b)
    .await
    .expect("copy the row onto shard B");

    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
            SET state = 'MIGRATED', migrated_to_shard = $2, migrated_at = NOW() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_a)
    .await
    .expect("seal shard A's row with a forwarding pointer");

    tokio::time::sleep(Duration::from_millis(200)).await;
    mark_completed(&mut conn_b, exec_id).await;

    let result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect(
            "the handle must rebind to shard B and wake on its notification, \
             not wait out the full 30s caller timeout on shard A's stale listener",
        )
        .expect("waiter task should not panic")
        .expect("result should be successful");

    assert_eq!(result, serde_json::json!({"ok": true}));
}

/// Issue #1317 review, P1 follow-up (companion to the tests above):
/// `result_snapshot_with_wait` has the same single-connect listener
/// pattern. A 30-second wait timeout would fail this test's own 10-second
/// outer timeout if the fix regressed.
#[tokio::test]
async fn result_snapshot_with_wait_rebinds_its_listener_after_a_migration() {
    #[derive(diesel::QueryableByName)]
    struct JsonRow {
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        payload: serde_json::Value,
    }

    let (url_a, _container_a) = setup_isolated_database_url().await;
    let (url_b, _container_b) = setup_isolated_database_url().await;
    let pool_a = build_pool(&url_a);
    let pool_b = build_pool(&url_b);
    let mut conn_a = <AsyncPgConnection as AsyncConnection>::establish(&url_a)
        .await
        .expect("connect a");
    let mut conn_b = <AsyncPgConnection as AsyncConnection>::establish(&url_b)
        .await
        .expect("connect b");

    let shard_a = autumn_harvest::ShardId::new(0);
    let shard_b = autumn_harvest::ShardId::new(1);
    let exec_id = ExecutionId::new_for_shard(shard_a);
    let started = start_running_workflow(&mut conn_a, exec_id).await;

    let pools: std::collections::BTreeMap<_, _> =
        [(shard_a, pool_a), (shard_b, pool_b)].into_iter().collect();
    let sharded_pool = ShardedDbPool::from_map(pools, shard_a);
    let router = ShardRouter::new(vec![shard_a, shard_b], vec![shard_a, shard_b], shard_a);
    let client = WorkflowHandleClient::new(
        sharded_pool,
        router,
        [(shard_a, url_a.clone()), (shard_b, url_b.clone())],
    );
    let handle = client.handle(started.exec_id);

    let waiter = tokio::spawn(async move {
        handle
            .result_snapshot_with_wait(Duration::from_secs(30))
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let row: JsonRow = diesel::sql_query(
        "SELECT to_jsonb(e) AS payload FROM harvest_workflow_executions e WHERE e.id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn_a)
    .await
    .expect("read the row to copy");

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             SELECT * FROM jsonb_populate_record( \
                 NULL::harvest_workflow_executions, \
                 $1::jsonb || jsonb_build_object('shard_id', $2::int))",
    )
    .bind::<diesel::sql_types::Jsonb, _>(&row.payload)
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_b)
    .await
    .expect("copy the row onto shard B");

    diesel::sql_query(
        "UPDATE harvest_workflow_executions \
            SET state = 'MIGRATED', migrated_to_shard = $2, migrated_at = NOW() \
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Integer, _>(shard_b.as_i32())
    .execute(&mut conn_a)
    .await
    .expect("seal shard A's row with a forwarding pointer");

    tokio::time::sleep(Duration::from_millis(200)).await;
    mark_completed(&mut conn_b, exec_id).await;

    let result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect(
            "the handle must rebind to shard B and wake on its notification, \
             not wait out the full 30s caller timeout on shard A's stale listener",
        )
        .expect("waiter task should not panic")
        .expect("wait should not fail");

    let snapshot = result.expect("workflow must be terminal, not a 204-style miss");
    assert_eq!(snapshot.state, WorkflowResultState::Completed);
}

#[tokio::test]
async fn result_snapshot_with_wait_returns_none_for_running_workflow() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let result = client
        .handle(started.exec_id)
        .result_snapshot_with_wait(Duration::from_millis(50))
        .await
        .expect("wait should not fail");

    assert!(
        result.is_none(),
        "running workflow should produce 204-style miss"
    );
}

#[test]
fn workflow_result_from_terminal_execution_is_compact() {
    let response = WorkflowResult::completed(
        WorkflowResultState::Completed,
        serde_json::json!({"value": 42}),
        Some("2026-05-09T12:00:00Z".parse().expect("timestamp")),
    );
    let json = serde_json::to_value(response).expect("serializable result response");

    assert_eq!(json["state"], "completed");
    assert_eq!(json["output"], serde_json::json!({"value": 42}));
    assert!(
        json.get("history").is_none(),
        "result response must stay compact"
    );
}

#[tokio::test]
async fn handle_terminate_seals_terminated_and_result_surfaces_terminated() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);
    let handle = client.handle(started.exec_id);

    let outcome = handle
        .terminate("wedged by handle")
        .await
        .expect("terminate must succeed");
    assert_eq!(outcome.state, "TERMINATED");
    assert!(outcome.newly_cancelled);

    // A result-awaiting caller observes HarvestError::Terminated (distinct from
    // a cooperative cancel and from a failure).
    let error = handle
        .result_raw()
        .await
        .expect_err("terminated workflow surfaces an error");
    assert!(
        matches!(error, HarvestError::Terminated(reason) if reason.contains("wedged by handle")),
        "expected HarvestError::Terminated carrying the reason"
    );
}

#[tokio::test]
async fn handle_terminate_is_idempotent_on_terminal() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    mark_completed(&mut conn, started.exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let outcome = client
        .handle(started.exec_id)
        .terminate("too late")
        .await
        .expect("terminate must not error on a terminal run");
    assert!(
        !outcome.newly_cancelled,
        "terminating a terminal run is a non-mutating no-op"
    );
    assert_eq!(outcome.state, "COMPLETED");
}

#[tokio::test]
async fn typed_handle_terminate_delegates_to_inner() {
    use autumn_harvest::TypedWorkflowHandle;

    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);
    let typed: TypedWorkflowHandle<serde_json::Value> =
        TypedWorkflowHandle::new(client.handle(started.exec_id));

    let outcome = typed
        .terminate("typed kill")
        .await
        .expect("typed terminate must succeed");
    assert_eq!(outcome.state, "TERMINATED");
    assert!(outcome.newly_cancelled);
}

#[tokio::test]
async fn handle_cancel_seals_cancelled() {
    let (database_url, _container) = setup_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let client = WorkflowHandleClient::single(pool, database_url);

    let outcome = client
        .handle(started.exec_id)
        .cancel("graceful by handle")
        .await
        .expect("cancel must succeed");
    assert_eq!(outcome.state, "CANCELLED");
    assert!(outcome.newly_cancelled);
}

// ── Issue #1717: TLS listeners and the polling fallback ──────────────────

/// A notification URL that refuses every connection.
const UNREACHABLE_NOTIFY_URL: &str = "postgres://postgres:postgres@127.0.0.1:1/postgres";

/// Start a running workflow and complete it after a short delay.
///
/// The client notifies through `notify_url`, not through `database_url`.
async fn complete_later_with_notify_url(
    notify_url: &str,
) -> (
    autumn_harvest::WorkflowHandle,
    tokio::task::JoinHandle<()>,
    Option<ContainerAsync<Postgres>>,
) {
    let (database_url, container) = setup_isolated_database_url().await;
    let pool = build_pool(&database_url);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("postgres connection");
    let exec_id = ExecutionId::new_for_shard(autumn_harvest::ShardId::new(0));
    let started = start_running_workflow(&mut conn, exec_id).await;
    let handle = WorkflowHandleClient::single(pool, notify_url).handle(started.exec_id);
    let completer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        mark_completed(&mut conn, exec_id).await;
    });
    (handle, completer, container)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_raw_polls_when_the_listener_cannot_connect() {
    let (handle, completer, _container) =
        complete_later_with_notify_url(UNREACHABLE_NOTIFY_URL).await;

    let result = tokio::time::timeout(Duration::from_secs(10), handle.result_raw())
        .await
        .expect("polling must see the completion")
        .expect("a dead listener must not fail the wait");

    assert_eq!(result, serde_json::json!({"ok": true}));
    completer.await.expect("completer should not panic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_raw_with_timeout_polls_when_the_listener_cannot_connect() {
    let (handle, completer, _container) =
        complete_later_with_notify_url(UNREACHABLE_NOTIFY_URL).await;

    let result = tokio::time::timeout(
        Duration::from_secs(20),
        handle.result_raw_with_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("the wait must end")
    .expect("a dead listener must not fail the wait");

    assert_eq!(result, serde_json::json!({"ok": true}));
    completer.await.expect("completer should not panic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_snapshot_with_wait_polls_when_the_listener_cannot_connect() {
    let (handle, completer, _container) =
        complete_later_with_notify_url(UNREACHABLE_NOTIFY_URL).await;

    let snapshot = tokio::time::timeout(
        Duration::from_secs(20),
        handle.result_snapshot_with_wait(Duration::from_secs(10)),
    )
    .await
    .expect("the wait must end")
    .expect("a dead listener must not fail the wait")
    .expect("polling must see the completion");

    assert_eq!(snapshot.state, WorkflowResultState::Completed);
    completer.await.expect("completer should not panic");
}

/// A failed `sslmode=require` listener must name the TLS cause.
///
/// `tokio_postgres` shows only "error performing TLS handshake". The cause is
/// in `source()`. The unit test `sslmode_require_starts_a_tls_handshake` in
/// `notify.rs` proves that `require` selects the rustls connector.
#[tokio::test]
async fn listener_with_sslmode_require_names_the_tls_cause() {
    let (database_url, _container) = setup_isolated_database_url().await;
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let require_url = format!("{database_url}{separator}sslmode=require");

    let Err(error) = autumn_harvest::notify::WorkflowEventListener::connect(&require_url).await
    else {
        // The server has a trusted certificate. No failure to inspect.
        return;
    };
    let message = error.to_string();

    assert!(
        message.contains("server does not support TLS") || message.contains("certificate"),
        "the error must name the TLS cause: {message}"
    );
}
