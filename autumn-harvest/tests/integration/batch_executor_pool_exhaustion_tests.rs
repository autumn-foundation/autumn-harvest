#![cfg(feature = "db")]
//! Snag finding: `batch::run_executor_once` deadlocks itself against its own
//! shard pool. Issue #1360 filed this as an unconfirmed, unreproduced risk
//! in the `#1350` shard-6 outbox-deadlock changelog. This test confirms it.
//!
//! Two stacked instances share one root cause. Each holds a checked-out
//! connection across an `.await`. Each `.await` then needs a second
//! connection from the identical pool.
//!
//! 1. `run_executor_once` checks out a connection to list open jobs on a
//!    shard. It holds that connection, unused, for the entire processing of
//!    every job on that shard. `process_job` immediately needs its own
//!    connection (`owning_shard_pool.get()`) from the same pool.
//! 2. Inside `process_job`, `owning_conn` claims the job's lease first. It
//!    stays checked out across the whole per-target dispatch loop. The
//!    function needs it again afterward, for `record_progress` and
//!    `mark_completed`. Meanwhile each dispatched target concurrently needs
//!    its own connection via `dispatch_pool_for(..).get()`. For a
//!    single-shard deployment, or two shards aliased to one physical pool
//!    (the `#1350` topology), that is the identical pool.
//!
//! Fixing only #1 is not sufficient. The test below still hangs with just
//! that instance patched, because #2 alone reproduces it on a single shard.
//! A correct fix needs `owning_conn` re-acquired at each of its three use
//! sites: claim, each `record_progress` call, and `mark_completed`. Holding
//! one connection for the whole function is the current, broken shape.
//! Re-acquiring at three sites touches enough call sites to read as a small
//! design change, not a one-line patch. This report ships without a fix for
//! that reason.
//!
//! On a pool with no free capacity left, the second acquisition can never
//! succeed. The first connection never releases until processing finishes,
//! and processing never finishes. `deadpool` has no acquisition timeout
//! configured anywhere in this codebase. That same precondition made the
//! `#1350` outbox bug a permanent hang rather than a slow retry. This is a
//! genuine, permanent self-deadlock, not a transient stall.
//!
//! This bug is reachable through the real production entry point.
//! `autumn_harvest_plugin::runner::BatchRuntime::spawn` calls
//! `run_executor_once` in a bare loop with no timeout. `BatchRuntime::
//! shutdown` then awaits that same stuck task, with no abort fallback. A
//! hang here therefore wedges the whole process's graceful shutdown too. It
//! triggers whenever a shard's pool has no spare connection at the moment a
//! batch job is open. The simplest case, reproduced here, is a pool of
//! size one.
//!
//! `#[ignore]` applies because this is a confirmed hang, not a flake.
//! Running it un-ignored would reproduce the "one test parks a CI job for
//! hours" failure mode that `#1350` already caused once. This test bounds
//! the hang to 15 seconds via `tokio::time::timeout`, so it fails fast
//! instead. A hang is still not something the default suite should carry.
//! Run it explicitly to reproduce:
//! `HARVEST_TEST_DATABASE_URL=... cargo test --features db,testing -- \
//! --ignored run_executor_once_deadlocks_on_a_pool_with_no_spare_connection`.
//! It fails 3 out of 3 in local verification.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::batch::{BatchAction, BatchExecutorConfig, BatchFilter, run_executor_once};
use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;

use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// One physically distinct, migrated database. Mirrors
/// `shard_placement_by_id_tests::setup_one_database`: prefers
/// `HARVEST_TEST_DATABASE_URL` (a real Postgres already reachable in this
/// environment) over a testcontainers container so the test runs without
/// Docker.
/// Swap the database-name path segment of a Postgres URL, preserving any
/// query string. Mirrors `shard_placement_by_id_tests::rewrite_pg_db`: a
/// naive `rsplit_once('/')` would drop options like `?sslmode=require` from
/// `HARVEST_TEST_DATABASE_URL`, breaking the per-test database connection on
/// a server that requires them.
fn rewrite_pg_db(base: &str, db: &str) -> String {
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let rest = &base[after_scheme..];
    let (authority, tail) = rest
        .find('/')
        .map_or((rest, ""), |i| (&rest[..i], &rest[i + 1..]));
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{}{}/{}{}", &base[..after_scheme], authority, db, query)
}

async fn setup_one_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(base_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest1360_{}", uuid::Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&base_url)
            .await
            .expect("connect to HARVEST_TEST_DATABASE_URL");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create per-test database");
        let new_url = rewrite_pg_db(&base_url, &db_name);
        let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&new_url)
            .await
            .expect("connect to per-test database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("apply migrations");
        return (new_url, None);
    }

    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("connect to container database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migrations");
    (url, Some(container))
}

fn build_pool_with_max_size(database_url: &str, max_size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(max_size)
        .build()
        .expect("failed to build test pool")
}

async fn insert_running_row(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "snag_1360_wf",
        workflow_id: "snag-1360",
        run_id: uuid::Uuid::new_v4(),
        shard_id: exec_id.shard().as_i32(),
        input: serde_json::json!({}),
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
        .expect("insert running execution row");
}

async fn load_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

/// Reproduction test.
///
/// A single-shard deployment has a pool at capacity: `max_size` 1. This is
/// the simplest case of "no spare connection." One open batch job matches
/// one execution. This permanently hangs `run_executor_once`. No pool
/// aliasing, no rebalancing, and no concurrent load are required.
#[tokio::test]
#[ignore = "confirmed permanent hang (issue #1360), bounded to 15s here; not \
            safe for the default suite until the deadlock is fixed — see \
            module docs"]
async fn run_executor_once_deadlocks_on_a_pool_with_no_spare_connection() {
    let (url, _container) = setup_one_database().await;

    // The pool holds exactly one connection.
    //
    // `run_executor_once` needs a connection to list open jobs.
    // `process_job` needs a second, concurrently held, connection from the
    // same pool. It uses that second connection to claim and record the
    // job.
    //
    // A size-one pool is the minimal reproduction case. The real-world
    // trigger is a pool that concurrent load has merely exhausted, at the
    // moment the executor tick fires. A permanently-size-one pool is not
    // required in production.
    let pool = build_pool_with_max_size(&url, 1);
    let sharded = ShardedDbPool::single(pool.clone());

    let exec_id = ExecutionId::new_for_shard(sharded.iter_shards().next().unwrap().0);
    {
        let mut seed = pool.get().await.expect("seed connection");
        insert_running_row(&mut seed, exec_id).await;

        autumn_harvest::batch::submit_batch_job(
            &mut seed,
            autumn_harvest::batch::BatchSubmission {
                action: BatchAction::Cancel,
                filter: BatchFilter {
                    states: vec!["RUNNING".to_string()],
                    workflow_name: None,
                    search_attrs: vec![],
                },
                signal_name: None,
                signal_payload: None,
                idempotency_key: None,
                created_by: None,
            },
        )
        .await
        .expect("submit batch job");
        // `seed` is dropped here, returning the pool's only connection.
    }

    let config = BatchExecutorConfig {
        concurrency: 8,
        metrics: Arc::new(NoOpMetrics),
    };

    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        run_executor_once(&sharded, &config),
    )
    .await;

    // Unwrap both result layers. A bounded acquisition timeout added later
    // could turn the hang into a fast `Err` instead. This test must reject
    // that outcome too, not just the absence of a 15-second wait.
    match outcome {
        Err(elapsed) => panic!(
            "run_executor_once must not hang ({elapsed}): it needs only one connection at a \
             time, but held onto the job-listing connection for the whole tick instead of \
             releasing it before process_job claims its own (issue #1360). A batch \
             executor tick that hangs forever also hangs `BatchRuntime::shutdown` \
             (runner.rs awaits the same JoinHandle with no abort fallback), so one \
             exhausted shard pool wedges the whole process's shutdown path too."
        ),
        Ok(Err(error)) => panic!("run_executor_once must succeed, not fail: {error}"),
        Ok(Ok(())) => {}
    }

    let mut verify = pool.get().await.expect("verify connection");
    assert_eq!(
        load_execution(&mut verify, exec_id).await.state,
        "CANCELLED",
        "the batch job must actually cancel its matched target, not merely \
         return without hanging"
    );
}
