#![cfg(feature = "db")]
//! Keyed entity on a real Postgres (issue #1975).
//!
//! Each test drives an `autumn_harvest::entity` loop through real workers.
//!
//! - A worker stops in the middle of an operation. A second worker takes
//!   over. A later operation waits for the first one. The state holds every
//!   operation once.
//! - Two clients that send to one new key at the same time reach one run.
//! - Operations sent while checkpoints happen all apply once, in order.
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
    build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// When set, the `crash-slow` op hangs in its activity, as on a dead worker.
static HANG_SLOW: AtomicBool = AtomicBool::new(false);

/// How many times each op started its activity. Op names are unique for
/// each test.
static RUNS: Mutex<BTreeMap<String, u32>> = Mutex::new(BTreeMap::new());

fn runs_of(op: &str) -> u32 {
    RUNS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(op)
        .copied()
        .unwrap_or(0)
}

/// One step of an op. The `crash-slow` op hangs while [`HANG_SLOW`] is set.
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
    if op == "crash-slow" && HANG_SLOW.load(Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    Ok(op)
}

/// An entity whose state is the list of applied ops. Each op runs one
/// activity on the run's own queue.
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

fn unique_id(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// A worker with sticky routing off, so a peer takes over at once. A
/// history threshold of `threshold` events forces frequent checkpoints.
fn build_worker(queue: &str, worker_id: &str, threshold: Option<u64>) -> Arc<Worker> {
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    });
    let mut builder = HarvestBuilder::new()
        .workflows(workflows![entity_crash_log, entity_checkpoint_log])
        .activities(activities![entity_crash_step]);
    if let Some(threshold) = threshold {
        builder = builder.history_continue_as_new_threshold(threshold);
    }
    let built = builder.worker(config.with_queues([queue])).build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    spawn_test_worker(Arc::clone(worker), pool.clone())
}

/// Where a message goes: the client, the queue and the entity key.
struct Target<'a> {
    client: &'a WorkflowHandleClient,
    queue: &'a str,
    key: &'a str,
}

/// Send one message to a crash-log or a checkpoint-log entity.
async fn send(
    conn: &mut AsyncPgConnection,
    to: &Target<'_>,
    checkpoint_entity: bool,
    message: EntityMessage<String>,
    idempotency_key: &str,
) -> ExecutionId {
    let opts = TypedSignalWithStartOptions {
        queue_name: Some(to.queue.to_string()),
        idempotency_key: Some(idempotency_key.to_string()),
        ..Default::default()
    };
    let start = EntityCheckpoint::<Vec<String>>::default();
    let handle = if checkpoint_entity {
        EntityCheckpointLogStub::signal_with_start(
            conn,
            to.client,
            to.key,
            start,
            ENTITY_OP_SIGNAL,
            message,
            opts,
        )
        .await
    } else {
        EntityCrashLogStub::signal_with_start(
            conn,
            to.client,
            to.key,
            start,
            ENTITY_OP_SIGNAL,
            message,
            opts,
        )
        .await
    };
    handle.expect("signal with start").exec_id()
}

fn op(name: &str) -> EntityMessage<String> {
    EntityMessage::op(name.to_string())
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(diesel::QueryableByName)]
struct RunRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    output: Option<serde_json::Value>,
}

/// Every run of the entity `key` of type `workflow_name`.
async fn runs_of_key(conn: &mut AsyncPgConnection, workflow_name: &str, key: &str) -> Vec<RunRow> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "SELECT state, output FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND workflow_id = $2",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(key)
    .load(conn)
    .await
    .expect("load runs")
}

/// The output of the completed run of `key`, once it exists.
async fn final_log(conn: &mut AsyncPgConnection, workflow_name: &str, key: &str) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let runs = runs_of_key(conn, workflow_name, key).await;
        if let Some(done) = runs.into_iter().find(|r| r.state == "COMPLETED") {
            let output = done.output.expect("final state");
            return serde_json::from_value(output).expect("decode state");
        }
        assert!(Instant::now() < deadline, "the entity did not complete");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// AC: per-key handlers run one at a time, and state survives a worker
/// crash. Worker A hangs in op `slow`. Op `c` waits behind it. Worker A then
/// stops: its loop ends and its pool closes, so it writes nothing more. The
/// hung attempt times out (`start_to_close`), and worker B retries it. The
/// final state holds each op once. A resent op with the same idempotency
/// key applies once.
#[tokio::test]
async fn entity_state_survives_a_worker_crash_in_the_middle_of_an_op() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let pool_a = build_test_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique_id("entity-q");
    let key = unique_id("session");
    let to = Target {
        client: &client,
        queue: &queue,
        key: &key,
    };

    let worker_a = build_worker(&queue, &unique_id("entity-a"), None);
    let handle_a = spawn(&worker_a, &pool_a);

    let exec = send(&mut conn, &to, false, op("crash-a"), "op-a").await;
    wait_until("op a", || runs_of("crash-a") == 1).await;

    // Worker A takes op `slow` and hangs in its activity.
    HANG_SLOW.store(true, Ordering::SeqCst);
    let again = send(&mut conn, &to, false, op("crash-slow"), "op-slow").await;
    assert_eq!(again, exec, "one key reaches one run");
    wait_until("op slow to start", || runs_of("crash-slow") == 1).await;

    // Op `c` arrives while `slow` runs. It must not start.
    send(&mut conn, &to, false, op("crash-c"), "op-c").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(runs_of("crash-c"), 0, "op c waits behind op slow");

    // Worker A dies.
    handle_a.abort();
    let _ = handle_a.await;
    worker_a.shutdown();
    pool_a.close();
    HANG_SLOW.store(false, Ordering::SeqCst);

    // The resend of `a` is a duplicate.
    send(&mut conn, &to, false, op("crash-a"), "op-a").await;

    let worker_b = build_worker(&queue, &unique_id("entity-b"), None);
    let handle_b = spawn(&worker_b, &pool);
    wait_until("op c", || runs_of("crash-c") == 1).await;
    send(&mut conn, &to, false, EntityMessage::delete(), "op-delete").await;

    let execution =
        wait_for_execution_state_with_timeout(&url, exec, "COMPLETED", Duration::from_secs(30))
            .await;
    let log: Vec<String> =
        serde_json::from_value(execution.output.expect("final state")).expect("decode state");
    assert_eq!(
        log,
        vec!["crash-a", "crash-slow", "crash-c"],
        "each op applies once, in order"
    );
    assert_eq!(
        runs_of("crash-a"),
        1,
        "replay does not run a recorded op again"
    );
    assert_eq!(runs_of("crash-c"), 1);
    assert!(
        runs_of("crash-slow") >= 2,
        "the hung attempt is retried on worker B"
    );

    worker_b.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle_b).await;
}

/// AC: one key has one run. Two clients send the first ops to a new key at
/// the same time. Both reach the same run, and both ops apply once.
#[tokio::test]
async fn two_clients_that_race_on_a_new_key_reach_one_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique_id("entity-race-q");
    let key = unique_id("race");
    let to = Target {
        client: &client,
        queue: &queue,
        key: &key,
    };
    let mut first = AsyncPgConnection::establish(&url).await.expect("connect");
    let mut second = AsyncPgConnection::establish(&url).await.expect("connect");

    let (one, two) = tokio::join!(
        send(&mut first, &to, true, op("race-x"), "race-x"),
        send(&mut second, &to, true, op("race-y"), "race-y"),
    );
    assert_eq!(one, two, "both clients reach one run");

    let worker = build_worker(&queue, &unique_id("entity-race"), None);
    let handle = spawn(&worker, &pool);
    send(
        &mut first,
        &to,
        true,
        EntityMessage::delete(),
        "race-delete",
    )
    .await;

    let mut log = final_log(&mut first, "entity_checkpoint_log", &key).await;
    log.sort();
    assert_eq!(log, vec!["race-x", "race-y"], "each op applies once");

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// AC: state and ops survive continue-as-new. A history threshold of four
/// events forces checkpoints while thirty ops arrive. Some ops are in
/// history at a checkpoint and ride in the input. Others arrive during the
/// transition and move to the next run. A duplicate sent after a
/// checkpoint applies once.
#[tokio::test]
async fn entity_ops_survive_history_checkpoints() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let client = WorkflowHandleClient::single(pool.clone(), url.clone());
    let queue = unique_id("entity-ckpt-q");
    let key = unique_id("ledger");
    let to = Target {
        client: &client,
        queue: &queue,
        key: &key,
    };

    let worker = build_worker(&queue, &unique_id("entity-ckpt"), Some(4));
    let handle = spawn(&worker, &pool);
    let names: Vec<String> = (0..30).map(|n| format!("op-{n:02}")).collect();
    for name in &names {
        send(&mut conn, &to, true, op(name), name).await;
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let runs = runs_of_key(&mut conn, "entity_checkpoint_log", &key).await;
        if runs.iter().any(|r| r.state == "CONTINUED_AS_NEW") {
            break;
        }
        assert!(Instant::now() < deadline, "the entity took no checkpoints");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    send(&mut conn, &to, true, op(&names[0]), &names[0]).await;
    send(&mut conn, &to, true, EntityMessage::delete(), "delete").await;

    let log = final_log(&mut conn, "entity_checkpoint_log", &key).await;
    assert_eq!(
        log, names,
        "every op applies once, in send order, across checkpoints"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}
