#![cfg(feature = "db")]

//! Fan-out result writer and tolerance against a real worker (issue #1986).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, or let the suite
//! start a container.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::error::HarvestError;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString as _};
use autumn_harvest::fan_out::{
    FailureTolerance, FanOutItem, FanOutOptions, FanOutResults, StoredResult,
};
use autumn_harvest::info::{ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
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

/// Lower-case hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// A content-addressed in-memory store that counts its calls.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    puts: AtomicUsize,
    gets: AtomicUsize,
}

impl MemStore {
    fn blob(&self, key: &str) -> Option<Vec<u8>> {
        self.blobs.lock().unwrap().get(key).cloned()
    }
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "mem"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let key = hex(&Sha256::digest(bytes));
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), bytes.to_vec());
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let found = self.blob(key);
        let key = key.to_string();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("missing {key}"))) })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs.lock().unwrap().remove(key);
        Box::pin(async move { Ok(()) })
    }
}

/// A test cipher: XOR with one byte. It makes the stored bytes differ from
/// the plaintext, which is all the codec check needs.
#[derive(Debug)]
struct XorCodec(u8);

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

fn xor_codecs() -> PayloadCodecs {
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(XorCodec(0x5a)));
    codecs
}

/// Item `i` returns a string of `size` bytes that starts with `i:`.
/// Items listed in `fail` fail without a retry.
fn make_blob<'a>(
    _ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let i = input["i"].as_u64().unwrap_or(0);
        if let Some(ms) = input["sleep_ms"].as_u64() {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if input["fail"].as_bool() == Some(true) {
            return Err(
                ActivityFailure::non_retryable("Boom", format!("item {i}")).into_error_payload()
            );
        }
        let size = usize::try_from(input["size"].as_u64().unwrap_or(0)).unwrap_or(0);
        // Hash output does not compress, so Postgres stores the full size.
        let mut body = format!("{i}:");
        let mut block = Sha256::digest(body.as_bytes());
        while body.len() < size {
            block = Sha256::digest(block);
            body.push_str(&hex(&block));
        }
        body.truncate(size.max(2));
        Ok(json!(body))
    })
}

/// `make_blob`, committed through `run_transactional`.
fn make_blob_in_a_transaction<'a>(
    ctx: &'a ActivityContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let value = make_blob(ctx, input).await?;
        ctx.run_transactional(move |_conn| Box::pin(async move { Ok(value) }))
            .await
    })
}

/// Input: `{ "n", "size", "writer", "activity"?, "count"?, "fail"?: [indices] }`.
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
                    "sleep_ms": input["sleep_ms"][i.to_string()],
                });
                let activity = input["activity"].as_str().unwrap_or("make_blob");
                (activity.to_string(), item, "default".to_string())
            })
            .collect();
        let window = usize::try_from(input["w"].as_u64().unwrap_or(50)).unwrap_or(50);
        let mut options = FanOutOptions::new().with_max_in_flight(window);
        if input["writer"].as_bool() == Some(true) {
            options = options.with_result_writer(true);
        }
        if let Some(k) = input["count"].as_u64() {
            options = options.with_tolerance(FailureTolerance::Count(usize::try_from(k).unwrap()));
        }
        let results = match ctx
            .execute_activity_fan_out_raw_with(activities, &options)
            .await
        {
            Ok(results) => results,
            Err(error @ HarvestError::FanOutFailureThresholdExceeded { .. }) => {
                if input["catch"].as_bool() == Some(true) {
                    return Ok(json!({ "caught": error.to_string() }));
                }
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

fn registry(store: Arc<MemStore>, threshold: u64) -> Arc<HandlerRegistry> {
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
    let activity = |name, handler| ActivityInfo {
        name,
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
        handler,
    };
    let offloader = PayloadOffloader::new(store, threshold, Arc::new(NoOpMetrics));
    Arc::new(
        HandlerRegistry::new(
            vec![workflow],
            vec![
                activity("make_blob", make_blob),
                activity("make_blob_in_a_transaction", make_blob_in_a_transaction),
            ],
        )
        .with_payload_offloader(Some(Arc::new(offloader)))
        .with_payload_codecs(xor_codecs()),
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
    // A unique id per run, so a rerun on a shared database does not collide.
    let workflow_id: &'static str =
        Box::leak(format!("{workflow_id}-{}", uuid::Uuid::new_v4()).into_boxed_str());
    let exec_id = insert_named_execution(conn, "writer_fan_out", workflow_id, input.clone()).await;
    enqueue_started_workflow_task(conn, exec_id, input).await;
    let execution = wait_for_execution_state_with_timeout(
        database_url,
        exec_id,
        state,
        Duration::from_secs(120),
    )
    .await;
    (exec_id, execution)
}

/// Bytes per event of one type, and the number of such events.
async fn per_event(conn: &mut AsyncPgConnection, exec_id: ExecutionId, kind: &str) -> (i64, i64) {
    let history = store::load_history_with_codecs(conn, exec_id, &xor_codecs())
        .await
        .expect("load history");
    let n = history
        .events
        .iter()
        .filter(|e| e.type_name() == kind)
        .count();
    let n = i64::try_from(n).unwrap();
    (history_bytes(conn, exec_id, kind).await / n.max(1), n)
}

fn stored_items(execution: &autumn_harvest::models::WorkflowExecution) -> Value {
    execution.output.as_ref().unwrap()["stored"].clone()
}

/// Every scenario runs on one worker, one after another. The tests share the
/// `default` queue, so a second worker with other codecs could take these
/// rows. The worker encrypts every payload with a test codec.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn fan_out_options_on_a_real_worker() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let store = Arc::new(MemStore::default());
    let worker = build_runtime_worker(
        "worker-fan-out-options",
        4,
        50,
        // A 1 MiB threshold keeps the plain control run inline.
        registry(Arc::clone(&store), 1024 * 1024),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    let db = database_url.as_str();

    // ── Done when #2: the result writer keeps bytes per item fixed ──────────
    let (narrow, narrow_run) = run_fan_out(
        db,
        &mut conn,
        "narrow",
        json!({ "n": 20, "size": 1024, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (wide, wide_run) = run_fan_out(
        db,
        &mut conn,
        "wide",
        json!({ "n": 400, "size": 1024, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (large, _) = run_fan_out(
        db,
        &mut conn,
        "large",
        json!({ "n": 20, "size": 65536, "writer": true }),
        "COMPLETED",
    )
    .await;
    let (plain, _) = run_fan_out(
        db,
        &mut conn,
        "plain",
        json!({ "n": 20, "size": 65536 }),
        "COMPLETED",
    )
    .await;
    assert_eq!(stored_items(&narrow_run), json!(20));
    assert_eq!(stored_items(&wide_run), json!(400));

    let (narrow_item, narrow_n) = per_event(&mut conn, narrow, "ActivityCompleted").await;
    let (wide_item, wide_n) = per_event(&mut conn, wide, "ActivityCompleted").await;
    let (large_item, large_n) = per_event(&mut conn, large, "ActivityCompleted").await;
    let (plain_item, _) = per_event(&mut conn, plain, "ActivityCompleted").await;
    assert_eq!(
        (narrow_n, wide_n, large_n),
        (20, 400, 20),
        "one completion per item"
    );
    assert!(
        narrow_item <= 512,
        "a reference is small: {narrow_item} bytes"
    );
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
    // The other event types do not depend on the result either.
    for kind in ["ActivityScheduled", "ActivityStarted"] {
        let (n_bytes, _) = per_event(&mut conn, narrow, kind).await;
        let (w_bytes, _) = per_event(&mut conn, wide, kind).await;
        let (l_bytes, _) = per_event(&mut conn, large, kind).await;
        assert!(
            (n_bytes - w_bytes).abs() <= 8 && (n_bytes - l_bytes).abs() <= 8,
            "{kind} bytes per item: {n_bytes}, {w_bytes}, {l_bytes}"
        );
    }
    let marginal = (history_bytes(&mut conn, wide, "").await
        - history_bytes(&mut conn, narrow, "").await)
        / 380;
    assert!(
        marginal <= 1024,
        "each added item costs a fixed {marginal} bytes"
    );
    eprintln!(
        "bytes per completed item: 20 x 1 KiB = {narrow_item}, 400 x 1 KiB = {wide_item}, \
         20 x 64 KiB = {large_item}, inline 20 x 64 KiB = {plain_item}; \
         whole history per added item = {marginal}"
    );

    // One blob and one reference row per item. Replay fetched no blob.
    let refs = store::load_payload_refs(&mut conn, wide).await.unwrap();
    assert_eq!(refs.len(), 400, "each blob has a harvest_payload_refs row");
    assert!(refs.iter().all(|r| store.blob(&r.blob_key).is_some()));
    assert_eq!(
        AtomicUsize::load(&store.puts, Ordering::SeqCst),
        440,
        "one upload per stored item"
    );
    assert_eq!(
        AtomicUsize::load(&store.gets, Ordering::SeqCst),
        0,
        "replay reads no blob"
    );

    // ── Codec: the blob holds ciphertext, and `fetch` decodes it ───────────
    let first: FanOutItem<Value> =
        serde_json::from_value(wide_run.output.clone().unwrap()["first"].clone()).unwrap();
    let FanOutItem::Stored(stored) = first else {
        panic!("expected a stored item, got {first:?}");
    };
    let value = stored.fetch(store.as_ref(), &xor_codecs()).await.unwrap();
    let text = value.as_str().unwrap().to_string();
    assert_eq!(text.len(), 1024);
    assert!(text.starts_with("0:"));
    let raw = store.blob(&stored.key).unwrap();
    assert!(
        !raw.windows(64).any(|w| w == &text.as_bytes()[2..66]),
        "the blob must not hold the plaintext"
    );
    let plain_codecs = stored
        .fetch(store.as_ref(), &PayloadCodecs::default())
        .await;
    assert!(
        plain_codecs.map_or(true, |v| v != value),
        "default codecs must not decode it"
    );

    // ── `fetch` rejects a reference whose blob does not match ──────────────
    let tampered = StoredResult {
        len: stored.len + 1,
        ..stored.clone()
    };
    assert!(tampered.fetch(store.as_ref(), &xor_codecs()).await.is_err());

    // ── Writer, window and tolerance together ───────────────────────────────
    let puts_before = AtomicUsize::load(&store.puts, Ordering::SeqCst);
    let (mixed, mixed_run) = run_fan_out(
        db,
        &mut conn,
        "mixed",
        json!({ "n": 60, "w": 10, "size": 64, "writer": true, "count": 2, "fail": [3, 41] }),
        "COMPLETED",
    )
    .await;
    let output = mixed_run.output.unwrap();
    assert_eq!(output["stored"], json!(58));
    assert_eq!(output["failed"], json!(2));
    assert_eq!(
        store::load_payload_refs(&mut conn, mixed)
            .await
            .unwrap()
            .len(),
        58
    );
    assert_eq!(
        AtomicUsize::load(&store.puts, Ordering::SeqCst) - puts_before,
        58,
        "no blob for a failed item"
    );

    // ── A transactional activity honours the writer ────────────────────────
    let puts_before = AtomicUsize::load(&store.puts, Ordering::SeqCst);
    let (tx_run, tx_out) = run_fan_out(
        db,
        &mut conn,
        "transactional",
        json!({ "n": 5, "size": 4096, "writer": true, "activity": "make_blob_in_a_transaction" }),
        "COMPLETED",
    )
    .await;
    assert_eq!(
        stored_items(&tx_out),
        json!(5),
        "every transactional result is stored"
    );
    assert_eq!(
        store::load_payload_refs(&mut conn, tx_run)
            .await
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        AtomicUsize::load(&store.puts, Ordering::SeqCst) - puts_before,
        5
    );
    let (tx_item, _) = per_event(&mut conn, tx_run, "ActivityCompleted").await;
    assert!(
        tx_item <= 512,
        "a transactional reference is small: {tx_item} bytes"
    );

    // ── A caught stop cancels a slot still in flight ────────────────────────
    let (caught, caught_run) = run_fan_out(
        db,
        &mut conn,
        "caught-stop",
        json!({ "n": 2, "size": 8, "fail": [0], "sleep_ms": { "1": 3000 }, "catch": true }),
        "COMPLETED",
    )
    .await;
    assert!(
        caught_run.output.unwrap()["caught"]
            .as_str()
            .unwrap()
            .contains("more than 0 of 2")
    );
    // Wait past the slow item. Its late result must not land after the end.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let history = store::load_history_with_codecs(&mut conn, caught, &xor_codecs())
        .await
        .expect("load history");
    let last = history.events.last().unwrap();
    assert_eq!(
        last.type_name(),
        "WorkflowCompleted",
        "history must end at the terminal: {last:?}"
    );
    let failures = history
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                autumn_harvest::event::WorkflowEvent::ActivityFailed { .. }
            )
        })
        .count();
    assert_eq!(failures, 2, "item 0 failed and item 1 was cancelled");

    // ── Done when #1 on a worker: N completes, N+1 fails ────────────────────
    let (_, within) = run_fan_out(
        db,
        &mut conn,
        "within",
        json!({ "n": 5, "size": 8, "count": 1, "fail": [3] }),
        "COMPLETED",
    )
    .await;
    assert_eq!(within.output.unwrap()["failed"], json!(1));
    let (_, beyond) = run_fan_out(
        db,
        &mut conn,
        "beyond",
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

    // ── A zero offload threshold does not offload the reference again ──────
    let worker = build_runtime_worker(
        "worker-fan-out-threshold-zero",
        4,
        50,
        registry(Arc::clone(&store), 0),
    );
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&database_url));
    let puts_before = AtomicUsize::load(&store.puts, Ordering::SeqCst);
    let (zero, _) = run_fan_out(
        db,
        &mut conn,
        "threshold-zero",
        json!({ "n": 3, "size": 64, "writer": true }),
        "COMPLETED",
    )
    .await;
    let refs = store::load_payload_refs(&mut conn, zero).await.unwrap();
    let completed_refs = AtomicUsize::load(&store.puts, Ordering::SeqCst) - puts_before;
    worker.shutdown();
    handle.await.expect("worker joins");
    // Each item writes its result once. The workflow input and output are
    // offloaded too, under a zero threshold, so count only item blobs.
    let history = store::load_history_with_codecs(&mut conn, zero, &xor_codecs())
        .await
        .expect("load history");
    let stored = history
        .events
        .iter()
        .filter(|e| {
            matches!(e, autumn_harvest::event::WorkflowEvent::ActivityCompleted { output, .. }
            if StoredResult::from_recorded_value(output).is_some())
        })
        .count();
    assert_eq!(
        stored, 3,
        "each completion records a reference, not an offload envelope"
    );
    assert!(
        completed_refs >= 3 && refs.len() == completed_refs,
        "every upload has one reference row: {completed_refs} uploads, {} rows",
        refs.len()
    );
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
    assert_eq!(results.tolerated(), 1);
    assert_eq!(
        serde_json::to_value(&results).unwrap()["tolerated"],
        json!(1)
    );
}
