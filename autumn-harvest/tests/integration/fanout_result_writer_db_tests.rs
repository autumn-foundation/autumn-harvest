#![cfg(feature = "db")]

//! Fan-out result writer and tolerance against a real worker (issue #1986).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, or let the suite
//! start a container.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::error::HarvestError;
use autumn_harvest::fan_out::{FailureTolerance, FanOutItem, FanOutOptions, FanOutResults};
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString as _};
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use autumn_harvest::store;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use diesel::sql_types::{BigInt, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task, insert_named_execution,
    setup_test_database_url_or_env, spawn_test_worker, wait_for_execution_state_with_timeout,
};

/// A content-addressed in-memory store.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemStore {
    fn contains(&self, key: &str) -> bool {
        self.blobs.lock().unwrap().contains_key(key)
    }
}

/// One store for every worker in this file.
///
/// The tests share the `default` queue, so a worker can run another test's
/// activity. A shared store keeps each blob where its test looks for it.
fn shared_store() -> Arc<MemStore> {
    static STORE: std::sync::OnceLock<Arc<MemStore>> = std::sync::OnceLock::new();
    Arc::clone(STORE.get_or_init(|| Arc::new(MemStore::default())))
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "mem"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key: String = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
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

/// Item `i` returns a string of `size` bytes that starts with `i`.
/// Items listed in `fail` fail without a retry.
fn make_blob<'a>(
    _ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let i = input["i"].as_u64().unwrap_or(0);
        if input["fail"].as_bool() == Some(true) {
            return Err(ActivityFailure::non_retryable("Boom", format!("item {i}")).into_error_payload());
        }
        let size = usize::try_from(input["size"].as_u64().unwrap_or(0)).unwrap_or(0);
        let mut body = format!("{i}:");
        body.push_str(&"x".repeat(size.saturating_sub(body.len())));
        Ok(json!(body))
    })
}

/// Input: `{ "n", "size", "writer", "count"?, "fail"?: [indices] }`.
fn fan_out_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = input["n"].as_u64().unwrap_or(0);
        let failing: Vec<u64> = input["fail"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default();
        let activities: Vec<_> = (0..n)
            .map(|i| {
                let item = json!({
                    "i": i,
                    "size": input["size"],
                    "fail": failing.contains(&i),
                });
                ("make_blob".to_string(), item, "default".to_string())
            })
            .collect();
        let mut options = FanOutOptions::new().max_in_flight(50);
        if input["writer"].as_bool() == Some(true) {
            options = options.write_results();
        }
        if let Some(k) = input["count"].as_u64() {
            options = options.tolerate(FailureTolerance::Count(usize::try_from(k).unwrap()));
        }
        let results = match ctx.execute_activity_fan_out_raw_with(activities, &options).await {
            Ok(results) => results,
            Err(error @ HarvestError::FanOutFailureThresholdExceeded { .. }) => {
                return Err(error.to_string());
            }
            Err(error) => return Err(error.to_string()),
        };
        let stored = results
            .items()
            .iter()
            .filter(|item| matches!(item, FanOutItem::Stored(_)))
            .count();
        // Return a small summary. The manifest goes to the next step, not to
        // the output, so the output does not hide the history measure.
        Ok(json!({
            "stored": stored,
            "failed": results.failed_count(),
            "first": results.items().first(),
        }))
    })
}

fn registry(store: Arc<MemStore>) -> Arc<HandlerRegistry> {
    let workflow = WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "writer_fan_out",
        module: "fanout_result_writer_db_tests",
        handler: fan_out_workflow as WorkflowHandlerFn,
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
    };
    let activity = ActivityInfo {
        name: "make_blob",
        module: "fanout_result_writer_db_tests",
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
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: make_blob,
    };
    // A 1 MiB threshold keeps the plain control run inline.
    let offloader = PayloadOffloader::new(store, 1024 * 1024, Arc::new(NoOpMetrics));
    Arc::new(
        HandlerRegistry::new(vec![workflow], vec![activity])
            .with_payload_offloader(Some(Arc::new(offloader))),
    )
}

#[derive(diesel::QueryableByName)]
struct Bytes {
    #[diesel(sql_type = BigInt)]
    bytes: i64,
}

/// Stored bytes of the run's history. `kind` limits the sum to one event type.
async fn history_bytes(conn: &mut AsyncPgConnection, exec_id: ExecutionId, kind: &str) -> i64 {
    diesel::sql_query(
        "SELECT COALESCE(SUM(pg_column_size(event_data)), 0)::bigint AS bytes \
         FROM harvest_events WHERE workflow_exec_id = $1 \
         AND ($2 = '' OR event_data->>'type' = $2)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(kind)
    .get_result::<Bytes>(conn)
    .await
    .expect("measure history bytes")
    .bytes
}

/// Run one fan-out to a terminal state and return the run id and the state.
async fn run_fan_out(
    database_url: &str,
    conn: &mut AsyncPgConnection,
    workflow_id: &'static str,
    input: Value,
    state: &str,
) -> (ExecutionId, autumn_harvest::models::WorkflowExecution) {
    let exec_id = insert_named_execution(conn, "writer_fan_out", workflow_id, input.clone()).await;
    enqueue_started_workflow_task(conn, exec_id, input).await;
    let execution =
        wait_for_execution_state_with_timeout(database_url, exec_id, state, Duration::from_secs(120))
            .await;
    (exec_id, execution)
}

/// Done when #2: with a result writer, the history bytes per item do not
/// depend on the width or on the result size.
///
/// Each item still records its activity events. Those have a fixed size. The
/// result bytes go to the store, so a wider fan-out adds only that fixed size
/// per item.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn result_writer_keeps_history_bytes_per_item_fixed() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let store = shared_store();
    let worker = build_runtime_worker("worker-fan-out-writer", 4, 50, registry(Arc::clone(&store)));
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));

    let (narrow, narrow_run) = run_fan_out(
        &database_url,
        &mut conn,
        "writer-narrow",
        json!({ "n": 20, "size": 1024, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (wide, wide_run) = run_fan_out(
        &database_url,
        &mut conn,
        "writer-wide",
        json!({ "n": 400, "size": 1024, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (large, _) = run_fan_out(
        &database_url,
        &mut conn,
        "writer-large-results",
        json!({ "n": 20, "size": 65536, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (plain, _) = run_fan_out(
        &database_url,
        &mut conn,
        "plain-large-results",
        json!({ "n": 20, "size": 65536 }),
        "COMPLETED",
    )
    .await;

    assert_eq!(narrow_run.output.as_ref().unwrap()["stored"], json!(20));
    assert_eq!(wide_run.output.as_ref().unwrap()["stored"], json!(400));

    // Result bytes per item are fixed: the same for 20 or 400 items, and for
    // 1 KiB or 64 KiB results.
    let per_item = |total: i64, n: i64| total / n;
    let narrow_item = per_item(history_bytes(&mut conn, narrow, "ActivityCompleted").await, 20);
    let wide_item = per_item(history_bytes(&mut conn, wide, "ActivityCompleted").await, 400);
    let large_item = per_item(history_bytes(&mut conn, large, "ActivityCompleted").await, 20);
    let plain_item = per_item(history_bytes(&mut conn, plain, "ActivityCompleted").await, 20);
    assert!(narrow_item <= 512, "a reference is small: {narrow_item} bytes");
    assert!(
        (narrow_item - wide_item).abs() <= 8,
        "width must not change bytes per item: {narrow_item} vs {wide_item}"
    );
    assert!(
        (narrow_item - large_item).abs() <= 8,
        "result size must not change bytes per item: {narrow_item} vs {large_item}"
    );
    assert!(
        plain_item > 10 * large_item,
        "the control run keeps results inline: {plain_item} vs {large_item}"
    );

    // The whole history grows by a fixed amount per item.
    let narrow_total = history_bytes(&mut conn, narrow, "").await;
    let wide_total = history_bytes(&mut conn, wide, "").await;
    let large_total = history_bytes(&mut conn, large, "").await;
    let marginal = (wide_total - narrow_total) / 380;
    assert!(
        marginal <= 2048,
        "each added item costs a fixed {marginal} bytes, not its result"
    );
    assert!(
        (large_total - narrow_total).abs() <= 20 * 16,
        "64 KiB results must not grow history: {large_total} vs {narrow_total}"
    );

    // Each result is one blob with a GC reference, and it reads back.
    let refs = store::load_payload_refs(&mut conn, wide).await.unwrap();
    assert_eq!(refs.len(), 400, "each blob has a harvest_payload_refs row");
    assert!(
        refs.iter().all(|r| store.contains(&r.blob_key)),
        "each reference names a blob in the store"
    );
    let first: FanOutItem<Value> =
        serde_json::from_value(wide_run.output.unwrap()["first"].clone()).unwrap();
    let FanOutItem::Stored(stored) = first else {
        panic!("expected a stored item, got {first:?}");
    };
    let value = stored
        .fetch(store.as_ref(), &PayloadCodecs::default())
        .await
        .unwrap();
    assert_eq!(value.as_str().unwrap().len(), 1024);
    assert!(value.as_str().unwrap().starts_with("0:"));

    worker.shutdown();
    handle.await.expect("worker joins");
}

/// Done when #1 against a real worker: a tolerance of 1 completes with one
/// failure and fails with two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tolerance_completes_at_n_and_fails_at_n_plus_one_on_a_worker() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let worker = build_runtime_worker("worker-fan-out-tolerance", 4, 50, registry(shared_store()));
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));

    let (_, within) = run_fan_out(
        &database_url,
        &mut conn,
        "tolerance-within",
        json!({ "n": 5, "size": 8, "count": 1, "fail": [3] }),
        "COMPLETED",
    )
    .await;
    assert_eq!(within.output.unwrap()["failed"], json!(1));

    let (_, beyond) = run_fan_out(
        &database_url,
        &mut conn,
        "tolerance-beyond",
        json!({ "n": 5, "size": 8, "count": 1, "fail": [1, 3] }),
        "FAILED",
    )
    .await;
    let error = beyond.error.unwrap_or_default();
    assert!(
        error.contains("more than 1 of 5 items failed"),
        "got {error}"
    );

    worker.shutdown();
    handle.await.expect("worker joins");
}

/// The manifest type is serializable, so a workflow can hand it on.
#[test]
fn manifest_round_trips_through_json() {
    let results: FanOutResults<Value> = serde_json::from_value(json!({
        "items": [{ "value": 1 }, { "failed": "x" }],
        "tolerated": 1,
    }))
    .unwrap();
    assert_eq!(results.failed_count(), 1);
    assert_eq!(serde_json::to_value(&results).unwrap()["tolerated"], json!(1));
}
