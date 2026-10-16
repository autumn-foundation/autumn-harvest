#![cfg(feature = "db")]
//! A signal staged during a run's final decision (issue #2079).
//!
//! The task ingests the pending signals at its start. A signal that arrives
//! later stays unconsumed. A run that then completes or fails must not seal
//! over it. The engine runs the task again, so the body sees the signal.
//!
//! Each workflow body sends its own late signal on its first pass. That
//! signal lands after the task start and before the completion.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::TypedSignalWithStartOptions;
use autumn_harvest::prelude::*;
use autumn_harvest::signal::send_signal;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

const LATE: &str = "late";

/// The database URL of each run, keyed by workflow id.
static DB_URLS: Mutex<BTreeMap<String, String>> = Mutex::new(BTreeMap::new());

/// The workflow ids that already sent their late signal.
static SENT: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// Send the late signal once per workflow id, on the first body pass.
async fn send_late_signal_once(ctx: &WorkflowContext) {
    let info = ctx.info();
    let first = SENT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(info.workflow_id.clone());
    if !first {
        return;
    }
    let url = DB_URLS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&info.workflow_id)
        .cloned()
        .expect("database url");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    send_signal(
        &mut conn,
        info.execution_id,
        LATE,
        serde_json::json!("late-1"),
    )
    .await
    .expect("send late signal");
}

/// Sends a late signal, then completes with every `late` signal it can read.
#[workflow]
async fn late_signal_reader(ctx: &WorkflowContext, _input: String) -> Result<Vec<String>, String> {
    send_late_signal_once(ctx).await;
    ctx.drain_signals::<String>(LATE).map_err(|e| e.to_string())
}

/// Sends a late signal, then completes and never reads it.
#[workflow]
async fn late_signal_ignorer(ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    send_late_signal_once(ctx).await;
    Ok("done".to_string())
}

/// Sends a late signal, then fails unless it can read that signal.
#[workflow]
async fn late_signal_failer(ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    send_late_signal_once(ctx).await;
    let seen = ctx
        .drain_signals::<String>(LATE)
        .map_err(|e| e.to_string())?;
    if seen.is_empty() {
        return Err("failed before the late signal".to_string());
    }
    Err(format!("failed after {}", seen.join(",")))
}

fn unique_id(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

fn build_worker(queue: &str) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(workflows![
            late_signal_reader,
            late_signal_ignorer,
            late_signal_failer
        ])
        .worker(WorkerConfig::default().with_queues([queue]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = unique_id("late-signal-worker");
    runtime_config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

/// The started run and its worker.
struct Started {
    url: String,
    exec_id: ExecutionId,
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

/// Start `key` with a `kick` signal and run it on a fresh worker.
macro_rules! start_run {
    ($stub:ident, $key:expr) => {{
        let (url, container) = setup_test_database_url_or_env().await;
        let pool: DbPool = build_test_pool(&url);
        let client = WorkflowHandleClient::single(pool.clone(), url.clone());
        let queue = unique_id("late-signal-q");
        DB_URLS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert($key.to_string(), url.clone());
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        let opts = TypedSignalWithStartOptions {
            queue_name: Some(queue.clone()),
            ..Default::default()
        };
        let handle = $stub::signal_with_start(
            &mut conn,
            &client,
            $key,
            String::new(),
            "kick",
            "go".to_string(),
            opts,
        )
        .await
        .expect("signal with start");
        let worker = build_worker(&queue);
        let task = spawn_test_worker(Arc::clone(&worker), pool);
        (
            Started {
                url,
                exec_id: handle.exec_id(),
                worker,
                handle: task,
            },
            container,
        )
    }};
}

impl Started {
    async fn stop(self) {
        self.worker.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.handle).await;
    }
}

#[derive(diesel::QueryableByName)]
struct SignalRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    consumed: bool,
}

/// The `consumed` flag of every `late` signal on the run.
async fn late_signals(url: &str, exec_id: ExecutionId) -> Vec<bool> {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    diesel::sql_query(
        "SELECT consumed FROM harvest_signals \
         WHERE workflow_exec_id = $1 AND signal_name = $2",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(LATE)
    .load::<SignalRow>(&mut conn)
    .await
    .expect("load signals")
    .into_iter()
    .map(|row| row.consumed)
    .collect()
}

/// AC: a signal staged after the task start and before the completion is
/// delivered. The run does not seal until the body has read it.
#[tokio::test]
async fn a_late_signal_reaches_the_body_before_the_run_completes() {
    let key = unique_id("late-reader");
    let (run, _container) = start_run!(LateSignalReaderStub, &key);

    let done = wait_for_execution_state_with_timeout(
        &run.url,
        run.exec_id,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        done.output.as_ref().map(std::string::ToString::to_string),
        Some(r#"["late-1"]"#.to_string()),
        "the output holds the late signal"
    );
    assert_eq!(late_signals(&run.url, run.exec_id).await, vec![true]);
    run.stop().await;
}

/// AC: a body that ignores late signals still completes on the next pass.
/// The signal is in history, so no row stays unconsumed.
#[tokio::test]
async fn a_body_that_ignores_a_late_signal_still_completes() {
    let key = unique_id("late-ignorer");
    let (run, _container) = start_run!(LateSignalIgnorerStub, &key);

    wait_for_execution_state_with_timeout(
        &run.url,
        run.exec_id,
        "COMPLETED",
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(late_signals(&run.url, run.exec_id).await, vec![true]);
    run.stop().await;
}

/// AC: a run that fails does not seal over a late signal either.
#[tokio::test]
async fn a_late_signal_reaches_the_body_before_the_run_fails() {
    let key = unique_id("late-failer");
    let (run, _container) = start_run!(LateSignalFailerStub, &key);

    let failed = wait_for_execution_state_with_timeout(
        &run.url,
        run.exec_id,
        "FAILED",
        Duration::from_secs(30),
    )
    .await;
    assert!(
        failed
            .error
            .as_deref()
            .is_some_and(|e| e.contains("failed after late-1")),
        "the failure came after the body read the late signal: {:?}",
        failed.error
    );
    assert_eq!(late_signals(&run.url, run.exec_id).await, vec![true]);
    run.stop().await;
}
