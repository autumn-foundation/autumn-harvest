#![cfg(feature = "db")]
//! Issue #1360: `batch::run_executor_once` self-deadlock on a small shard pool.
//!
//! The executor permanently retains up to two connections from a shard's pool
//! while that shard has an open batch job:
//!
//! 1. `run_executor_once` checks out a connection to list open jobs, then
//!    keeps it while it processes every job on that shard. `process_job`
//!    needs its own connection from the same pool right away.
//! 2. Inside `process_job`, the connection that claims the job (`owning_conn`)
//!    stays checked out across the whole per-target dispatch loop. Each
//!    target concurrently needs its own connection from `dispatch_pool_for`.
//!    On a single-shard deployment that pool is the same one.
//!
//! Neither connection is released until the whole function returns. A shard
//! pool at or below the retained count (1 or 2) deadlocks forever:
//! `deadpool` has no acquisition timeout configured anywhere in this crate.
//!
//! Each test bounds the call in [`tokio::time::timeout`] so a regression
//! fails fast instead of hanging the suite.

use autumn_harvest::batch::{
    BatchAction, BatchExecutorConfig, BatchFilter, BatchSubmission, get_batch_job,
    run_executor_once, submit_batch_job,
};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;

use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::json;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const SHARD: ShardId = ShardId::new(0);

/// One throwaway, migrated Postgres database for one test.
async fn setup_isolated_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_batch_exhaustion_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        diesel_async::SimpleAsyncConnection::batch_execute(
            &mut admin,
            &format!("CREATE DATABASE \"{db_name}\""),
        )
        .await
        .expect("create throwaway database");
        let url = swap_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        diesel_async::SimpleAsyncConnection::batch_execute(
            &mut conn,
            &autumn_harvest::test_init_sql(),
        )
        .await
        .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

/// Swap the database-name path segment of a Postgres URL.
fn swap_database(base: &str, db: &str) -> String {
    let prefix = base.rsplit_once('/').map_or(base, |(p, _)| p);
    format!("{prefix}/{db}")
}

fn build_pool_with_max_size(url: &str, max_size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("build test pool")
}

/// Insert one bare `RUNNING` execution row (default state). No history is
/// needed: `cancel_workflow_execution` decides on the row's `state` column.
async fn insert_running_execution(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(SHARD);
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "pool_exhaustion_probe",
        workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: SHARD.as_i32(),
        input: json!({}),
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
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert probe execution");
    exec_id
}

async fn state_of(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use diesel::QueryDsl;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first(conn)
        .await
        .expect("execution row must exist")
}

/// One `RUNNING` execution plus one open `Cancel` batch job that matches it,
/// on a single-shard `ShardedDbPool` built at `max_size`.
struct Fixture {
    sharded_pool: ShardedDbPool,
    exec_id: ExecutionId,
    job_id: Uuid,
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn build_fixture(max_size: usize) -> Fixture {
    let (url, container) = setup_isolated_db().await;
    let pool = build_pool_with_max_size(&url, max_size);
    let sharded_pool = ShardedDbPool::single(pool.clone());

    let mut conn = pool.get().await.expect("setup connection");
    let exec_id = insert_running_execution(&mut conn, "pool-exhaustion").await;
    let job_id = submit_batch_job(
        &mut conn,
        BatchSubmission {
            action: BatchAction::Cancel,
            filter: BatchFilter {
                states: vec!["RUNNING".to_string()],
                ..BatchFilter::default()
            },
            signal_name: None,
            signal_payload: None,
            idempotency_key: None,
            created_by: None,
        },
    )
    .await
    .expect("submit batch job");
    drop(conn);

    Fixture {
        sharded_pool,
        exec_id,
        job_id,
        url,
        _container: container,
    }
}

#[tokio::test]
async fn run_executor_once_does_not_self_deadlock_a_capacity_one_shard_pool() {
    // Root cause #1: `run_executor_once` holds its job-listing connection
    // while `process_job` needs a second one from the same pool. A pool of
    // one connection cannot hand out that second connection.
    let fixture = build_fixture(1).await;

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_executor_once(&fixture.sharded_pool, &BatchExecutorConfig::default()),
    )
    .await;

    let outcome = result.expect(
        "run_executor_once must not hang: the job-listing connection must be \
         released before process_job needs a second one from the same pool",
    );
    outcome.expect("executor tick must succeed");

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&fixture.url)
        .await
        .expect("connect for assertions");
    let job = get_batch_job(&mut conn, fixture.job_id)
        .await
        .expect("load batch job")
        .expect("batch job row exists");
    assert_eq!(job.status, "Completed");
    assert_eq!(job.completed, 1);
    assert_eq!(job.failed, 0);
    assert_eq!(state_of(&mut conn, fixture.exec_id).await, "CANCELLED");
}

#[tokio::test]
async fn run_executor_once_does_not_self_deadlock_a_capacity_two_shard_pool() {
    // Root cause #2: `process_job` holds `owning_conn` (acquired to claim the
    // job) across the per-target dispatch loop. On a single-shard deployment,
    // a pool of two connections is already full: the job-listing connection
    // and `owning_conn` fill it. Nothing is left for the dispatch loop.
    let fixture = build_fixture(2).await;

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        run_executor_once(&fixture.sharded_pool, &BatchExecutorConfig::default()),
    )
    .await;

    let outcome = result.expect(
        "run_executor_once must not hang: owning_conn must not be held across \
         the per-target dispatch loop's own connection checkouts",
    );
    outcome.expect("executor tick must succeed");

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&fixture.url)
        .await
        .expect("connect for assertions");
    let job = get_batch_job(&mut conn, fixture.job_id)
        .await
        .expect("load batch job")
        .expect("batch job row exists");
    assert_eq!(job.status, "Completed");
    assert_eq!(job.completed, 1);
    assert_eq!(job.failed, 0);
    assert_eq!(state_of(&mut conn, fixture.exec_id).await, "CANCELLED");
}
