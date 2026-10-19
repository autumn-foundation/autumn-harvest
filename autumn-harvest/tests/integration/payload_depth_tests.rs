#![cfg(feature = "db")]

//! Deep activity results and heartbeats against a real worker (issue #2225).
//!
//! `serde_json` reads at most 128 levels. A result nested past that limit made
//! every load of the run history fail. The worker now turns a result nested
//! past `MAX_PAYLOAD_DEPTH` into a non-retryable `PayloadTooDeep` failure.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, or let the suite
//! start a container. The tests share the `default` queue, so a lock runs them
//! one at a time.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::RetryPolicy;
use autumn_harvest::builder::{DEFAULT_MAX_ACTIVITY_INPUT_BYTES, DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES};
use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::payload_depth::{MAX_PAYLOAD_DEPTH, PAYLOAD_TOO_DEEP_ERROR_TYPE};
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use autumn_harvest::store;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task, insert_named_execution,
    setup_test_database_url_or_env, spawn_test_worker, wait_for_execution_state_with_timeout,
};

/// The depth that issue #2225 reports as unreadable.
const ISSUE_DEPTH: usize = 126;

/// Runs the tests one at a time. They share one queue and one table.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

type BoxFut<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A value of `depth` nested one-element arrays around a leaf.
fn nested(depth: usize, leaf: Value) -> Value {
    let mut value = leaf;
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    value
}

/// The nesting depth of a chain of one-element arrays.
fn chain_depth(mut value: &Value) -> usize {
    let mut depth = 0;
    while let Value::Array(items) = value {
        depth += 1;
        // `RunQueryDsl` is in scope, so call the slice method by path.
        match <[Value]>::first(items) {
            Some(inner) => value = inner,
            None => break,
        }
    }
    depth
}

/// An in-memory blob store that keeps every blob.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    puts: AtomicUsize,
}

impl MemStore {
    fn puts(&self) -> usize {
        AtomicUsize::load(&self.puts, Ordering::SeqCst)
    }
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "mem"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let n = self.puts.fetch_add(1, Ordering::SeqCst);
        let key = format!("blob-{n}");
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), bytes.to_vec());
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().unwrap().get(key).cloned();
        let key = key.to_string();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("missing {key}"))) })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs.lock().unwrap().remove(key);
        Box::pin(async move { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// Activities and the workflow.
// ---------------------------------------------------------------------------

/// Input: `{ "depth", "pad"? }`. Returns a chain of `depth` arrays. A `pad`
/// adds a string of that many bytes at the leaf.
fn deep_value(input: &Value) -> Value {
    let depth = usize::try_from(input["depth"].as_u64().unwrap_or(0)).unwrap_or(0);
    let pad = usize::try_from(input["pad"].as_u64().unwrap_or(0)).unwrap_or(0);
    nested(depth, json!("p".repeat(pad)))
}

/// A regular activity that returns a deep value.
fn deep_result<'a>(_ctx: &'a ActivityContext, input: Value) -> BoxFut<'a> {
    Box::pin(async move { Ok(deep_value(&input)) })
}

/// A local activity that returns a deep value.
fn deep_local<'a>(_ctx: &'a ActivityContext, input: Value) -> BoxFut<'a> {
    Box::pin(async move { Ok(deep_value(&input)) })
}

/// A transactional activity. It writes a row, then returns a deep value.
fn deep_transactional<'a>(ctx: &'a ActivityContext, input: Value) -> BoxFut<'a> {
    Box::pin(async move {
        let tag = input["tag"].as_str().unwrap_or("none").to_string();
        let value = deep_value(&input);
        ctx.run_transactional(move |conn| {
            Box::pin(async move {
                diesel::sql_query("INSERT INTO depth_probe_rows (tag) VALUES ($1)")
                    .bind::<Text, _>(tag)
                    .execute(conn)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(value)
            })
        })
        .await
    })
}

/// Attempt 1 sends a good checkpoint, then a deep one, and fails. Attempt 2
/// returns the checkpoint that it finds.
fn heartbeat_probe<'a>(ctx: &'a ActivityContext, _input: Value) -> BoxFut<'a> {
    Box::pin(async move {
        if ctx.attempt() > 1 {
            let found: Option<Value> = ctx.heartbeat_details().map_err(|e| e.to_string())?;
            return Ok(json!({ "found": found }));
        }
        ctx.heartbeat(json!({ "step": 1 }))
            .await
            .map_err(|e| e.to_string())?;
        // The flusher writes once a second.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        ctx.heartbeat(nested(ISSUE_DEPTH, json!(1)))
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        Err("retry on the next attempt".to_string())
    })
}

/// Input: `{ "activity", "local"?, "depth"?, "pad"?, "tag"? }`. The workflow
/// completes with a summary of what the activity gave it.
fn probe_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> BoxFut<'a> {
    Box::pin(async move {
        let activity = input["activity"]
            .as_str()
            .unwrap_or("deep_result")
            .to_string();
        let retry = Some(RetryPolicy::fixed(3, Duration::from_millis(10)));
        let result = if input["local"].as_bool() == Some(true) {
            ctx.execute_local_activity_raw(&activity, input.clone(), retry, None)
                .await
        } else {
            let queue = ctx.queue_name().to_string();
            ctx.execute_activity_raw_with_opts(&activity, input.clone(), &queue, retry, None)
                .await
        };
        match result {
            Ok(value) => {
                Ok(json!({ "ok": true, "depth": chain_depth(&value), "value": value.get("found") }))
            }
            Err(error) => Ok(json!({
                "ok": false,
                "error_type": error.activity_error_type(),
                "message": error.to_string(),
            })),
        }
    })
}

fn activity(name: &'static str, handler: ActivityHandlerFn, is_local: bool) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "payload_depth_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
        input_schema: None,
        output_schema: None,
    }
}

fn workflow() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "depth_probe",
        module: "payload_depth_tests",
        handler: probe_workflow as WorkflowHandlerFn,
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

/// A registry with every probe activity.
///
/// `result_cap` is the global result byte cap. `0` means no cap. `offload`
/// adds an offloader with the given threshold.
fn registry(result_cap: u64, offload: Option<(Arc<MemStore>, u64)>) -> Arc<HandlerRegistry> {
    let registry = HandlerRegistry::new(
        vec![workflow()],
        vec![
            activity("deep_result", deep_result, false),
            activity("deep_local", deep_local, true),
            activity("deep_transactional", deep_transactional, false),
            activity("heartbeat_probe", heartbeat_probe, false),
        ],
    )
    .with_payload_caps(
        DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
        autumn_harvest::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
        result_cap,
        DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
    );
    Arc::new(match offload {
        Some((store, threshold)) => registry.with_payload_offloader(Some(Arc::new(
            PayloadOffloader::new(store, threshold, Arc::new(NoOpMetrics)),
        ))),
        None => registry,
    })
}

// ---------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------

struct Run {
    database_url: String,
    conn: AsyncPgConnection,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

impl Run {
    async fn new() -> Self {
        let (database_url, container) = setup_test_database_url_or_env().await;
        let mut conn = AsyncPgConnection::establish(&database_url)
            .await
            .expect("connect");
        conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS depth_probe_rows (id BIGSERIAL PRIMARY KEY, tag TEXT NOT NULL)",
        )
        .await
        .expect("create depth_probe_rows");
        Self {
            database_url,
            conn,
            _container: container,
        }
    }

    /// Run one `depth_probe` workflow on a worker with `registry`, and return
    /// the run id and the workflow output. The run must complete.
    async fn complete(
        &mut self,
        registry: Arc<HandlerRegistry>,
        input: Value,
    ) -> (ExecutionId, Value) {
        let worker = build_runtime_worker("worker-depth-probe", 2, 4, registry);
        let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&self.database_url));
        let workflow_id: &'static str =
            Box::leak(format!("depth-{}", uuid::Uuid::new_v4()).into_boxed_str());
        let exec_id =
            insert_named_execution(&mut self.conn, "depth_probe", workflow_id, input.clone()).await;
        enqueue_started_workflow_task(&mut self.conn, exec_id, input).await;
        let execution = wait_for_execution_state_with_timeout(
            &self.database_url,
            exec_id,
            "COMPLETED",
            Duration::from_secs(30),
        )
        .await;
        worker.shutdown();
        let _ = handle.await;
        (
            exec_id,
            execution.output.expect("the workflow has an output"),
        )
    }

    async fn history(&mut self, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
        store::load_history(&mut self.conn, exec_id)
            .await
            .expect("the history must load")
            .events
    }
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

// Characterization: the hazard that issue #2225 reports. A stored value
// nested 126 levels cannot be read back, and a value at the cap can.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_read_limit_sits_above_the_cap() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    for (depth, readable) in [(MAX_PAYLOAD_DEPTH, true), (ISSUE_DEPTH, false)] {
        let workflow_id: &'static str =
            Box::leak(format!("depth-read-{}", uuid::Uuid::new_v4()).into_boxed_str());
        let exec_id =
            insert_named_execution(&mut run.conn, "depth_probe", workflow_id, json!({})).await;
        store::append_events(
            &mut run.conn,
            exec_id,
            &[WorkflowEvent::ActivityCompleted {
                activity_id: autumn_harvest::types::ActivityExecId::new(),
                output: nested(depth, json!(1)),
            }],
            0,
        )
        .await
        .expect("the write has no depth limit");
        let loaded = store::load_history(&mut run.conn, exec_id).await;
        assert_eq!(
            loaded.is_ok(),
            readable,
            "a result nested {depth} levels: readable = {readable}"
        );
    }
}

// Issue #2225, acceptance 1: the workflow sees a `PayloadTooDeep` failure and
// the run completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_nested_126_levels_becomes_a_payload_too_deep_failure() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let (exec_id, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                None,
            ),
            json!({ "activity": "deep_result", "depth": ISSUE_DEPTH }),
        )
        .await;
    assert_eq!(output["ok"], json!(false), "output: {output}");
    assert_eq!(output["error_type"], json!(PAYLOAD_TOO_DEEP_ERROR_TYPE));

    let history = run.history(exec_id).await;
    let failed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(failed, 1, "one terminal failure, and no retry: {history:?}");
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the deep result is never stored"
    );
}

// The check does not depend on the byte cap. An uncapped worker refuses too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uncapped_worker_refuses_a_deep_result() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let (_, output) = run
        .complete(
            registry(0, None),
            json!({ "activity": "deep_result", "depth": ISSUE_DEPTH }),
        )
        .await;
    assert_eq!(
        output["error_type"],
        json!(PAYLOAD_TOO_DEEP_ERROR_TYPE),
        "output: {output}"
    );
}

// Issue #2225, acceptance 2: a result at the cap round-trips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_at_the_cap_round_trips() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let (exec_id, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                None,
            ),
            json!({ "activity": "deep_result", "depth": MAX_PAYLOAD_DEPTH }),
        )
        .await;
    assert_eq!(output["ok"], json!(true), "output: {output}");
    assert_eq!(output["depth"], json!(MAX_PAYLOAD_DEPTH));
    let history = run.history(exec_id).await;
    let stored = history.iter().find_map(|e| match e {
        WorkflowEvent::ActivityCompleted { output, .. } => Some(chain_depth(output)),
        _ => None,
    });
    assert_eq!(stored, Some(MAX_PAYLOAD_DEPTH));
}

// A result that the offloader would move to the store is refused first. No
// blob of a value that no one can read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deep_result_is_refused_before_the_offloader_sees_it() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let store = Arc::new(MemStore::default());
    let (_, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                Some((Arc::clone(&store), 1024)),
            ),
            json!({ "activity": "deep_result", "depth": ISSUE_DEPTH, "pad": 4096 }),
        )
        .await;
    assert_eq!(
        output["error_type"],
        json!(PAYLOAD_TOO_DEEP_ERROR_TYPE),
        "output: {output}"
    );
    assert_eq!(store.puts(), 0, "the offloader stored no blob");
}

// A local activity ends on its first attempt, even with a retry policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deep_local_result_fails_on_the_first_attempt() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let (exec_id, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                None,
            ),
            json!({ "activity": "deep_local", "local": true, "depth": ISSUE_DEPTH }),
        )
        .await;
    assert_eq!(output["ok"], json!(false), "output: {output}");
    let history = run.history(exec_id).await;
    let failed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::LocalActivityFailed { .. }))
        .count();
    assert_eq!(failed, 1, "no retry follows: {history:?}");
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityExhausted { attempt: 1, .. }))
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "the deep result is never stored"
    );
}

// A transactional activity rolls its writes back. Nothing commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deep_transactional_result_rolls_back_the_user_write() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let tag = format!("deep-{}", uuid::Uuid::new_v4());
    let (_, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                None,
            ),
            json!({ "activity": "deep_transactional", "depth": ISSUE_DEPTH, "tag": tag }),
        )
        .await;
    assert_eq!(
        output["error_type"],
        json!(PAYLOAD_TOO_DEEP_ERROR_TYPE),
        "output: {output}"
    );
    let rows =
        diesel::sql_query("SELECT COUNT(*)::bigint AS n FROM depth_probe_rows WHERE tag = $1")
            .bind::<Text, _>(&tag)
            .get_result::<Count>(&mut run.conn)
            .await
            .expect("count rows");
    assert_eq!(rows.n, 0, "the user write rolled back");
}

// A deep heartbeat is dropped. The activity stays alive, the last good
// checkpoint stays, and the next claim of the task reads it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deep_heartbeat_keeps_the_last_good_checkpoint() {
    let _serial = SERIAL.lock().await;
    let mut run = Run::new().await;
    let (_, output) = run
        .complete(
            registry(
                autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                None,
            ),
            json!({ "activity": "heartbeat_probe" }),
        )
        .await;
    assert_eq!(output["ok"], json!(true), "output: {output}");
    assert_eq!(output["value"], json!({ "step": 1 }));
}
