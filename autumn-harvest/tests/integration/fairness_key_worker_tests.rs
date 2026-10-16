#![cfg(feature = "db")]

//! Fairness keys through a real worker (issue #1976).
//!
//! - A start's fairness key reaches the run's workflow task, its activities
//!   and its child workflows.
//! - A worker with fairness keys on serves tenant B through tenant A's flood.
//!   The same flood holds B to the end with fairness keys off.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::fairness_keys::list_fairness_state;
use autumn_harvest::prelude::*;
use autumn_harvest::worker::{HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};

use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_test_pool, runtime_config, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// Runs one activity, then one child workflow.
#[workflow]
async fn fair_parent_wf(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let queue = ctx.queue_name().to_string();
    ctx.execute_activity_raw("fair_step", serde_json::json!({}), &queue)
        .await
        .map_err(|e| e.to_string())?;
    if input["child"].as_bool().unwrap_or(false) {
        ctx.spawn_child_workflow_raw("fair_leaf_wf", serde_json::json!({}))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(serde_json::json!({}))
}

/// Runs one activity.
#[workflow]
async fn fair_leaf_wf(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    let queue = ctx.queue_name().to_string();
    ctx.execute_activity_raw("fair_step", serde_json::json!({}), &queue)
        .await
        .map_err(|e| e.to_string())
}

/// A short unit of work, so the worker's one slot is the bottleneck.
#[activity(start_to_close = "60s")]
async fn fair_step(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = (ctx, input);
    tokio::time::sleep(Duration::from_millis(15)).await;
    Ok(serde_json::json!({}))
}

/// One task row of the propagation check.
#[derive(diesel::QueryableByName)]
struct Row {
    #[diesel(sql_type = diesel::sql_types::Text)]
    task_type: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    fairness_key: Option<String>,
}

/// A count.
#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![fair_parent_wf_info(), fair_leaf_wf_info()],
        activities![fair_step],
    ))
}

fn fresh_queue(tag: &str) -> String {
    format!("fairw-{tag}-{}", Uuid::new_v4().simple())
}

/// Start one `fair_parent_wf` run on `queue_name` with `key`.
async fn start(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    key: &str,
    child: bool,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = format!("fairw-{}", exec_id.as_uuid());
    let mut params = StartWorkflowParams::new(
        "fair_parent_wf",
        &workflow_id,
        exec_id,
        serde_json::json!({ "child": child }),
        queue_name,
    );
    params.fairness_key = Some(key.to_owned());
    start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start the run");
    exec_id
}

fn start_worker(
    queue_name: &str,
    fairness: bool,
    pool: &autumn_harvest::worker::DbPool,
) -> (Arc<Worker>, tokio::task::JoinHandle<()>) {
    let mut config = runtime_config(
        &format!("fairw-{}", Uuid::new_v4().simple()),
        1,
        1,
        Duration::from_secs(10),
    );
    config.queues = vec![queue_name.to_owned()];
    config.fairness_keys = fairness;
    config.sticky_timeout = Duration::ZERO;
    let worker = Arc::new(Worker::new(config, registry()).expect("worker builds"));
    let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
    (worker, handle)
}

/// A start's key reaches every task of the run and of its child.
#[tokio::test]
async fn start_key_reaches_activities_and_children() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let queue_name = fresh_queue("prop");
    let exec_id = start(&mut conn, &queue_name, "tenant-x", true).await;

    let pool = build_test_pool(&url);
    let (worker, handle) = start_worker(&queue_name, true, &pool);
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(60))
        .await;
    worker.shutdown();
    let _ = handle.await;

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT task_type, fairness_key FROM harvest_task_queue WHERE queue_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&queue_name)
    .load(&mut conn)
    .await
    .unwrap();
    let count = |kind: &str| rows.iter().filter(|r| r.task_type == kind).count();
    assert!(
        count("workflow") >= 2,
        "parent and child workflow tasks exist"
    );
    assert!(
        count("activity") >= 2,
        "parent and child activity tasks exist"
    );
    for row in &rows {
        assert_eq!(
            row.fairness_key.as_deref(),
            Some("tenant-x"),
            "a {} task lost the key",
            row.task_type
        );
    }

    // Every claim charged the one key, so it is the only state row.
    let keys: Vec<String> = list_fairness_state(&mut conn, &queue_name)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.fairness_key)
        .collect();
    assert_eq!(
        keys,
        ["tenant-x"],
        "workflow, activity and child claims share the key"
    );
}

/// Start a flood from tenant A and one run from tenant B, then run one
/// worker. Returns how many A runs were complete when B completed.
async fn a_runs_done_before_b(fairness: bool) -> usize {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let queue_name = fresh_queue(if fairness { "on" } else { "off" });

    let mut a_runs = Vec::new();
    for _ in 0..20 {
        a_runs.push(start(&mut conn, &queue_name, "tenant-a", false).await);
    }
    let b = start(&mut conn, &queue_name, "tenant-b", false).await;

    let pool = build_test_pool(&url);
    let (worker, handle) = start_worker(&queue_name, fairness, &pool);
    wait_for_execution_state_with_timeout(&url, b, "COMPLETED", Duration::from_secs(120)).await;

    let ids: Vec<Uuid> = a_runs.iter().map(ExecutionId::as_uuid).collect();
    // Count the A runs that completed before B. Counting after B completes
    // would also count runs that finish while this query runs.
    let done: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions a \
         WHERE a.id = ANY($1) AND a.state = 'COMPLETED' \
           AND a.completed_at < (SELECT completed_at FROM harvest_workflow_executions WHERE id = $2)",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(&ids)
    .bind::<diesel::sql_types::Uuid, _>(b.as_uuid())
    .get_result(&mut conn)
    .await
    .unwrap();
    worker.shutdown();
    let _ = handle.await;
    usize::try_from(done.n).unwrap()
}

/// With fairness keys on, B completes before most of A's flood.
#[tokio::test]
async fn worker_with_fairness_keys_serves_tenant_b_through_a_flood() {
    let done = a_runs_done_before_b(true).await;
    eprintln!("fairness keys on: B completed after {done} of 20 A runs");
    assert!(
        done <= 5,
        "B waited for {done} of 20 A runs with fairness keys on"
    );
}

/// Control: with fairness keys off, the same flood holds B to the end.
#[tokio::test]
async fn worker_without_fairness_keys_holds_tenant_b_behind_the_flood() {
    let done = a_runs_done_before_b(false).await;
    eprintln!("fairness keys off: B completed after {done} of 20 A runs");
    assert!(
        done >= 15,
        "B passed {done} of 20 A runs with fairness keys off"
    );
}
