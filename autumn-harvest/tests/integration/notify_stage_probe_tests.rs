#![cfg(feature = "db")]
//! A workflow stages its NOTIFY notes without a probe round trip (issue #2125).
//!
//! Each staged note needs the transaction id and the database fingerprint of
//! its write. The write that stages the note returns them. A separate
//! `SELECT txid_current() AS txid, ...` probe is then not necessary.
//!
//! The test runs one 3-activity workflow through a worker. It counts the
//! probe calls in `pg_stat_statements` of a fresh database.
//! `HARVEST_TEST_DATABASE_URL` is an admin URL of a superuser on a server that
//! preloads `pg_stat_statements`. Without it the test starts a container.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use super::integration_e2e::{
    build_test_pool, runtime_config, spawn_test_worker, wait_for_execution_state,
};

const QUEUE: &str = "stage-probe";
const WORKFLOW: &str = "stage_probe_three_steps";
const ACTIVITY: &str = "stage_probe_step";

/// Matches the stage probe, in and out of a diesel transaction.
const PROBE_PATTERN: &str = "SELECT txid_current%AS txid, current_database()%";

/// Matches a write that returns its stamp.
const STAMPED_PATTERN: &str = "INSERT INTO%RETURNING txid_current()%";

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn step(_ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// The shape of the e2e throughput workload: three activities in sequence.
fn three_steps(ctx: &WorkflowContext, _input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        for n in 0..3 {
            ctx.execute_activity_raw(ACTIVITY, json!({ "step": n }), QUEUE)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(json!({ "ok": true }))
    })
}

fn workflow_info(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "notify_stage_probe_tests",
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

fn activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "notify_stage_probe_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(QUEUE),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: step,
        input_schema: None,
        output_schema: None,
    }
}

fn worker() -> Arc<Worker> {
    let registry = Arc::new(HandlerRegistry::new(
        vec![workflow_info(WORKFLOW, three_steps)],
        vec![activity_info(ACTIVITY)],
    ));
    let mut config = runtime_config("stage-probe-worker", 4, 4, Duration::from_secs(30));
    config.queues = vec![QUEUE.to_string()];
    // Scanner election and the heartbeat write on a timer. Keep them out of
    // the count.
    config.scanner.elect = false;
    config.worker_heartbeat_interval = Duration::from_secs(600);
    Arc::new(Worker::new(config, registry).expect("the worker builds"))
}

async fn setup_server() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    // `with_cmd` replaces the image command, so repeat `fsync=off`.
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd([
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "fsync=off",
        ])
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// The admin URL with its database name replaced. A query string stays.
fn db_url(admin_url: &str, name: &str) -> String {
    let (base, query) = admin_url
        .split_once('?')
        .map_or((admin_url, None), |(b, q)| (b, Some(q)));
    let (prefix, _) = base.rsplit_once('/').expect("the URL has a db segment");
    query.map_or_else(
        || format!("{prefix}/{name}"),
        |q| format!("{prefix}/{name}?{q}"),
    )
}

#[derive(diesel::QueryableByName)]
struct Calls {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
}

/// The calls of statements that match `pattern` in the current database.
async fn calls_like(conn: &mut AsyncPgConnection, pattern: &str) -> i64 {
    diesel::sql_query(
        "SELECT COALESCE(sum(calls), 0)::bigint AS calls FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND query LIKE $1",
    )
    .bind::<diesel::sql_types::Text, _>(pattern)
    .get_result::<Calls>(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
    .calls
}

#[tokio::test]
async fn a_three_activity_workflow_sends_no_stage_probe() {
    let (admin_url, _container) = setup_server().await;
    let name = format!("stage_probe_{}", uuid::Uuid::new_v4().simple());
    let mut admin = AsyncPgConnection::establish(&admin_url)
        .await
        .expect("connect");
    admin
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .expect("create the database");
    let url = db_url(&admin_url, &name);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply the migration bundle");
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("the test needs a superuser");

    let pool = build_test_pool(&url);
    assert!(
        autumn_harvest::notify::register_pool(&pool)
            .wait_ready(Duration::from_secs(10))
            .await,
        "the sender must be ready before the start"
    );
    let worker = worker();
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .execute(&mut conn)
    .await
    .expect("reset the statement counters");

    let exec_id = ExecutionId::new();
    {
        let mut start_conn = pool.get().await.expect("pool connection");
        let params = StartWorkflowParams::new(WORKFLOW, "stage-probe-1", exec_id, json!({}), QUEUE);
        start_or_load_workflow_execution(&mut start_conn, params, None)
            .await
            .expect("start the workflow");
    }
    wait_for_execution_state(&url, exec_id, "COMPLETED").await;
    // Read before the shutdown, so its writes stay out of the count.
    let probes = calls_like(&mut conn, PROBE_PATTERN).await;
    let stamped = calls_like(&mut conn, STAMPED_PATTERN).await;
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;

    drop(conn);
    drop(pool);
    let _ = admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
        .await;
    // A stale sender also sends no probe, because the fallback runs. The
    // stamped writes show that the sender was ready.
    assert!(
        stamped >= 10,
        "the writes must return their stamps: {stamped}"
    );
    assert_eq!(
        probes, 0,
        "each staged note must reuse the stamp of its own write"
    );
}
