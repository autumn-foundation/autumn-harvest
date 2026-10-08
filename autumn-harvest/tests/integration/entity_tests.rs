#![cfg(feature = "db")]
//! Keyed entity on a real Postgres (issue #1975).
//!
//! Each test drives an `autumn_harvest::entity` loop through real workers.
//!
//! - A worker dies in the middle of an operation. A second worker takes
//!   over. The state holds every operation once.
//! - Two clients that send to one key reach one run.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, Entity, EntityCheckpoint, EntityMessage};
use autumn_harvest::prelude::*;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StickyRoutingConfig, TypedSignalWithStartOptions};
use diesel_async::{AsyncConnection, AsyncPgConnection};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

/// When set, the `slow` op hangs in its activity, as on a worker that dies.
static HANG_SLOW: AtomicBool = AtomicBool::new(false);

/// How many times each op started its activity.
static RUNS: Mutex<BTreeMap<String, u32>> = Mutex::new(BTreeMap::new());

fn runs_of(op: &str) -> u32 {
    RUNS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(op)
        .copied()
        .unwrap_or(0)
}

/// One step of an op. The `slow` op hangs while [`HANG_SLOW`] is set.
#[activity(
    start_to_close = "2s",
    retry = RetryPolicy::fixed(5, Duration::from_millis(100))
)]
async fn entity_crash_step(_ctx: &ActivityContext, op: String) -> Result<String, String> {
    *RUNS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(op.clone())
        .or_default() += 1;
    if op == "slow" && HANG_SLOW.load(Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    Ok(op)
}

/// An entity whose state is the list of applied ops.
#[workflow]
async fn entity_crash_log(
    ctx: &WorkflowContext,
    input: EntityCheckpoint<Vec<String>>,
) -> Result<Vec<String>, String> {
    let queue = ctx.queue_name().to_string();
    Entity::new(ctx, input)
        .run(|mut log: Vec<String>, op: String| {
            let queue = queue.clone();
            async move {
                let done: String = ctx
                    .execute_activity_with_opts(
                        &entity_crash_step_info(),
                        op,
                        Some(&queue),
                        None,
                        None,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                log.push(done);
                Ok(log)
            }
        })
        .await
        .map_err(|e| e.to_string())
}

fn unique_id(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// A worker with sticky routing off, so a peer takes over at once.
fn build_worker(queue: &str, worker_id: &str) -> Arc<Worker> {
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    });
    let built = HarvestBuilder::new()
        .workflows(workflows![entity_crash_log])
        .activities(activities![entity_crash_step])
        .worker(config.with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Send one message to the entity `key` with signal-with-start.
async fn send(
    conn: &mut AsyncPgConnection,
    client: &WorkflowHandleClient,
    queue: &str,
    key: &str,
    message: EntityMessage<String>,
    idempotency_key: &str,
) -> ExecutionId {
    EntityCrashLogStub::signal_with_start(
        conn,
        client,
        key,
        EntityCheckpoint::<Vec<String>>::default(),
        ENTITY_OP_SIGNAL,
        message,
        TypedSignalWithStartOptions {
            queue_name: Some(queue.to_string()),
            idempotency_key: Some(idempotency_key.to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("signal with start")
    .exec_id()
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// AC: per-key handlers run one at a time, and state survives a worker
/// crash. Worker A dies while op `slow` runs its activity. Worker B retries
/// the activity, applies the later ops, and the final state holds each op
/// once. A resent op with the same idempotency key applies once.
#[tokio::test]
async fn entity_state_survives_a_worker_crash_in_the_middle_of_an_op() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique_id("entity-q");
    let key = unique_id("session");

    let worker_a = build_worker(&queue, &unique_id("entity-a"));
    let handle_a = spawn(&worker_a, &pool);

    let exec = send(
        &mut conn,
        &client,
        &queue,
        &key,
        EntityMessage::op("a".into()),
        "op-a",
    )
    .await;
    wait_until("op a", || runs_of("a") == 1).await;

    // Worker A takes op `slow` and hangs in its activity, then dies.
    HANG_SLOW.store(true, Ordering::SeqCst);
    let again = send(
        &mut conn,
        &client,
        &queue,
        &key,
        EntityMessage::op("slow".into()),
        "op-slow",
    )
    .await;
    assert_eq!(again, exec, "one key reaches one run");
    wait_until("op slow to start", || runs_of("slow") == 1).await;
    handle_a.abort();
    let _ = handle_a.await;
    HANG_SLOW.store(false, Ordering::SeqCst);

    // Later ops queue behind `slow`. The resend of `a` is a duplicate.
    send(
        &mut conn,
        &client,
        &queue,
        &key,
        EntityMessage::op("c".into()),
        "op-c",
    )
    .await;
    send(
        &mut conn,
        &client,
        &queue,
        &key,
        EntityMessage::op("a".into()),
        "op-a",
    )
    .await;

    let worker_b = build_worker(&queue, &unique_id("entity-b"));
    let handle_b = spawn(&worker_b, &pool);
    wait_until("op c", || runs_of("c") == 1).await;
    send(
        &mut conn,
        &client,
        &queue,
        &key,
        EntityMessage::delete(),
        "op-delete",
    )
    .await;

    let execution =
        wait_for_execution_state_with_timeout(&url, exec, "COMPLETED", Duration::from_secs(30))
            .await;
    let log: Vec<String> =
        serde_json::from_value(execution.output.expect("final state")).expect("decode state");
    assert_eq!(
        log,
        vec!["a", "slow", "c"],
        "each op applies once, in order"
    );
    assert_eq!(runs_of("a"), 1, "replay does not run a recorded op again");
    assert_eq!(runs_of("c"), 1);
    assert_eq!(runs_of("slow"), 2, "the crashed attempt is retried once");

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// An entity with no activity. It records each op in its state.
#[workflow]
async fn entity_checkpoint_log(
    ctx: &WorkflowContext,
    input: EntityCheckpoint<Vec<String>>,
) -> Result<Vec<String>, String> {
    Entity::new(ctx, input)
        .run(|mut log: Vec<String>, op: String| async move {
            log.push(op);
            Ok(log)
        })
        .await
        .map_err(|e| e.to_string())
}

/// A worker whose history threshold forces a checkpoint every few ops.
fn build_checkpoint_worker(queue: &str) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(workflows![entity_checkpoint_log])
        .history_continue_as_new_threshold(4)
        .worker(WorkerConfig::default().with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = unique_id("entity-ckpt");
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

#[derive(diesel::QueryableByName)]
struct RunRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    output: Option<serde_json::Value>,
}

async fn runs_of_key(conn: &mut AsyncPgConnection, key: &str) -> Vec<RunRow> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "SELECT state, output FROM harvest_workflow_executions \
         WHERE workflow_name = 'entity_checkpoint_log' AND workflow_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .load(conn)
    .await
    .expect("load runs")
}

/// AC: state and waiting ops survive continue-as-new. Six ops arrive before
/// the first task, so all six are in history when the first checkpoint
/// fires. The drain carries the other five into the next run.
#[tokio::test]
async fn entity_ops_survive_history_checkpoints() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique_id("entity-ckpt-q");
    let key = unique_id("ledger");

    let send_to = |n: usize| (format!("op-{n}"), EntityMessage::op(format!("op-{n}")));
    for n in 0..6 {
        let (id, message) = send_to(n);
        EntityCheckpointLogStub::signal_with_start(
            &mut conn,
            &client,
            &key,
            EntityCheckpoint::<Vec<String>>::default(),
            ENTITY_OP_SIGNAL,
            message,
            TypedSignalWithStartOptions {
                queue_name: Some(queue.clone()),
                idempotency_key: Some(id),
                ..Default::default()
            },
        )
        .await
        .expect("send op");
    }

    let worker = build_checkpoint_worker(&queue);
    let handle = spawn(&worker, &pool);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let runs = runs_of_key(&mut conn, &key).await;
        let checkpoints = runs
            .iter()
            .filter(|r| r.state == "CONTINUED_AS_NEW")
            .count();
        if checkpoints >= 1 && runs.iter().any(|r| r.state == "RUNNING") {
            break;
        }
        assert!(Instant::now() < deadline, "the entity took no checkpoints");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    EntityCheckpointLogStub::signal_with_start(
        &mut conn,
        &client,
        &key,
        EntityCheckpoint::<Vec<String>>::default(),
        ENTITY_OP_SIGNAL,
        EntityMessage::<String>::delete(),
        TypedSignalWithStartOptions {
            queue_name: Some(queue.clone()),
            idempotency_key: Some("delete".to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("send delete");

    let deadline = Instant::now() + Duration::from_secs(30);
    let output = loop {
        let runs = runs_of_key(&mut conn, &key).await;
        if let Some(done) = runs.into_iter().find(|r| r.state == "COMPLETED") {
            break done.output.expect("final state");
        }
        assert!(Instant::now() < deadline, "the entity did not complete");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let log: Vec<String> = serde_json::from_value(output).expect("decode state");
    let expected: Vec<String> = (0..6).map(|n| format!("op-{n}")).collect();
    assert_eq!(
        log, expected,
        "every op applies once, in order, across checkpoints"
    );
    let runs = runs_of_key(&mut conn, &key).await;
    assert!(
        runs.iter().any(|r| r.state == "CONTINUED_AS_NEW"),
        "the threshold forces at least one checkpoint"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}
