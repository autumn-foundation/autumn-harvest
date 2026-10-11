#![cfg(feature = "db")]
//! Durable workflow output streams (issue #1974).
//!
//! Store tests prove the storage contract: offset order, keep-first dedup,
//! the per-execution cap, batch inserts, cascade and erasure. Worker tests
//! prove the chain from `ctx.publish_durable_progress` to the table.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! tests start a testcontainers Postgres with the full migration bundle.

use autumn_harvest::WorkflowInfo;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::store::{self, DURABLE_STREAM_TRUNCATION_OFFSET, DurableStreamChunk};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use chrono::Utc;
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Insert a minimal RUNNING execution row, so the chunk FK resolves.
async fn insert_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
             (id, workflow_name, workflow_id, state, input, started_at, queue_name, shard_id)
         VALUES ($1, $2, $2 || '-' || $1::text, 'RUNNING', '{}'::jsonb, NOW(), 'default', 0)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(name)
    .execute(conn)
    .await
    .expect("insert execution");
}

const fn chunk(offset: i64, value: Value) -> DurableStreamChunk {
    DurableStreamChunk {
        offset,
        chunk: value,
    }
}

async fn read_after(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    after: Option<i64>,
    limit: i64,
) -> Vec<(i64, Value)> {
    store::load_stream_chunks(conn, exec_id, after, limit)
        .await
        .expect("load chunks")
        .into_iter()
        .map(|row| (row.stream_offset, row.chunk))
        .collect()
}

async fn read_all(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<(i64, Value)> {
    read_after(conn, exec_id, None, 1_000_000).await
}

// ── Store contract ──────────────────────────────────────────────────────────

#[tokio::test]
async fn chunks_read_back_in_offset_order() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_order").await;

    let written = store::append_stream_chunks(
        &mut conn,
        exec_id,
        &[
            chunk(1, json!("b")),
            chunk(0, json!("a")),
            chunk(2, json!("c")),
        ],
        100,
    )
    .await
    .expect("append");
    assert_eq!(written, 3);
    assert_eq!(
        read_all(&mut conn, exec_id).await,
        vec![(0, json!("a")), (1, json!("b")), (2, json!("c"))]
    );
}

#[tokio::test]
async fn a_re_driven_append_keeps_the_first_content() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_dedup").await;

    store::append_stream_chunks(&mut conn, exec_id, &[chunk(0, json!("first"))], 100)
        .await
        .expect("append");
    let written = store::append_stream_chunks(
        &mut conn,
        exec_id,
        &[chunk(0, json!("second")), chunk(1, json!("next"))],
        100,
    )
    .await
    .expect("re-driven append");
    assert_eq!(written, 1, "only the new offset is inserted");
    assert_eq!(
        read_all(&mut conn, exec_id).await,
        vec![(0, json!("first")), (1, json!("next"))],
        "the first stored content is canonical"
    );
}

#[tokio::test]
async fn load_after_an_offset_returns_only_later_chunks_in_pages() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_page").await;
    let chunks: Vec<_> = (0..10).map(|i| chunk(i, json!(i))).collect();
    store::append_stream_chunks(&mut conn, exec_id, &chunks, 100)
        .await
        .expect("append");

    assert_eq!(
        read_after(&mut conn, exec_id, Some(6), 2).await,
        vec![(7, json!(7)), (8, json!(8))]
    );
    assert_eq!(
        read_after(&mut conn, exec_id, Some(9), 100).await,
        Vec::<(i64, Value)>::new()
    );
}

#[tokio::test]
async fn chunks_are_scoped_to_their_own_execution() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let a = ExecutionId::new();
    let b = ExecutionId::new();
    insert_execution(&mut conn, a, "stream_a").await;
    insert_execution(&mut conn, b, "stream_b").await;
    store::append_stream_chunks(&mut conn, a, &[chunk(0, json!("a"))], 100)
        .await
        .expect("append a");
    store::append_stream_chunks(&mut conn, b, &[chunk(0, json!("b"))], 100)
        .await
        .expect("append b");
    assert_eq!(read_all(&mut conn, a).await, vec![(0, json!("a"))]);
    assert_eq!(read_all(&mut conn, b).await, vec![(0, json!("b"))]);
}

#[tokio::test]
async fn the_cap_drops_the_newest_and_records_one_terminal_marker() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_cap").await;

    let first: Vec<_> = (0..3).map(|i| chunk(i, json!(i))).collect();
    let written = store::append_stream_chunks(&mut conn, exec_id, &first, 2)
        .await
        .expect("append over cap");
    assert_eq!(written, 2);
    // A later batch, also over the cap, adds no row and no second marker.
    store::append_stream_chunks(&mut conn, exec_id, &[chunk(3, json!(3))], 2)
        .await
        .expect("append after cap");
    // A raised cap does not reopen the gate. The stored rows stay a prefix.
    let reopened = store::append_stream_chunks(&mut conn, exec_id, &[chunk(4, json!(4))], 50)
        .await
        .expect("append with raised cap");
    assert_eq!(reopened, 0);

    let rows = read_all(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 3, "two chunks and one marker: {rows:?}");
    assert_eq!(rows[..2], [(0, json!(0)), (1, json!(1))]);
    assert_eq!(rows[2].0, DURABLE_STREAM_TRUNCATION_OFFSET);
    assert_eq!(rows[2].1["_harvest_stream_truncated"], json!(true));
    assert_eq!(rows[2].1["max_chunks"], json!(2));
}

#[tokio::test]
async fn a_re_driven_batch_at_the_cap_is_not_a_truncation() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_cap_redrive").await;
    let batch: Vec<_> = (0..2).map(|i| chunk(i, json!(i))).collect();
    store::append_stream_chunks(&mut conn, exec_id, &batch, 2)
        .await
        .expect("append");
    store::append_stream_chunks(&mut conn, exec_id, &batch, 2)
        .await
        .expect("re-drive");
    assert_eq!(
        read_all(&mut conn, exec_id).await,
        vec![(0, json!(0)), (1, json!(1))],
        "a re-driven batch at exactly the cap drops nothing, so no marker"
    );
}

#[tokio::test]
async fn the_cap_holds_when_stored_offsets_are_not_a_prefix() {
    // A call that fails to serialize leaves its offset unused, so stored
    // offsets can have holes. Rows inside a batch's range then still count.
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_cap_holes").await;
    store::append_stream_chunks(&mut conn, exec_id, &[chunk(2, json!(2))], 2)
        .await
        .expect("append offset 2");
    let batch: Vec<_> = (0..3).map(|i| chunk(i, json!(i))).collect();
    store::append_stream_chunks(&mut conn, exec_id, &batch, 2)
        .await
        .expect("append over the cap");

    let rows = read_all(&mut conn, exec_id).await;
    let real = rows
        .iter()
        .filter(|(o, _)| *o != DURABLE_STREAM_TRUNCATION_OFFSET)
        .count();
    assert_eq!(real, 2, "never more real rows than the cap: {rows:?}");
    assert_eq!(
        rows.last().map(|(o, _)| *o),
        Some(DURABLE_STREAM_TRUNCATION_OFFSET),
        "a dropped chunk is visible as the marker: {rows:?}"
    );
}

#[tokio::test]
async fn one_append_can_exceed_the_bind_parameter_limit() {
    // Postgres allows 65,535 bind parameters in one statement. Three
    // parameters for each row would fail near 21,845 rows without batching.
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_big").await;
    let chunks: Vec<_> = (0..30_000).map(|i| chunk(i, json!(i))).collect();
    let written = store::append_stream_chunks(&mut conn, exec_id, &chunks, 100_000)
        .await
        .expect("a large batch must not exceed the bind limit");
    assert_eq!(written, 30_000);
}

#[tokio::test]
async fn a_failed_append_stores_no_chunk() {
    // The inline worker paths call the store on a bare connection. A failure
    // in a later batch must not leave the earlier batches committed.
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_atomic").await;
    let trigger = format!("fail_stream_{}", exec_id.as_uuid().simple());
    conn.batch_execute(&format!(
        "CREATE FUNCTION {trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.workflow_exec_id = '{id}' AND NEW.stream_offset = 1500 THEN \
           RAISE EXCEPTION 'injected failure'; END IF; RETURN NEW; END $$; \
         CREATE TRIGGER {trigger} BEFORE INSERT ON harvest_stream_chunks \
           FOR EACH ROW EXECUTE FUNCTION {trigger}();",
        id = exec_id.as_uuid()
    ))
    .await
    .expect("install failing trigger");

    let chunks: Vec<_> = (0..2_000).map(|i| chunk(i, json!(i))).collect();
    let result = store::append_stream_chunks(&mut conn, exec_id, &chunks, 100_000).await;
    conn.batch_execute(&format!(
        "DROP TRIGGER {trigger} ON harvest_stream_chunks; DROP FUNCTION {trigger}();"
    ))
    .await
    .expect("drop trigger");

    assert!(result.is_err(), "the second batch fails");
    assert_eq!(
        read_all(&mut conn, exec_id).await.len(),
        0,
        "the first batch must roll back with the second"
    );
}

#[tokio::test]
async fn appending_nothing_is_a_no_op() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_empty").await;
    let written = store::append_stream_chunks(&mut conn, exec_id, &[], 100)
        .await
        .expect("append nothing");
    assert_eq!(written, 0);
}

#[tokio::test]
async fn chunks_never_outlive_their_execution() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_cascade").await;
    store::append_stream_chunks(&mut conn, exec_id, &[chunk(0, json!("bye"))], 100)
        .await
        .expect("append");
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete execution");
    assert_eq!(read_all(&mut conn, exec_id).await, Vec::new());
}

#[tokio::test]
async fn erase_workflow_payloads_deletes_stream_chunks_and_reports_the_count() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_erase").await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
            SET state = 'COMPLETED', completed_at = NOW()
          WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seal execution");
    store::append_stream_chunks(
        &mut conn,
        exec_id,
        &[
            chunk(0, json!("alice@example.com")),
            chunk(1, json!("4111")),
        ],
        100,
    )
    .await
    .expect("append");

    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gdpr-req-2")
        .await
        .expect("erase a terminal execution");
    assert_eq!(outcome.stream_chunks_deleted, 2, "{outcome:?}");
    assert_eq!(read_all(&mut conn, exec_id).await, Vec::new());
}

/// An append after an erasure stores nothing. A stale inline write must not
/// restore author output that the erasure destroyed.
#[tokio::test]
async fn an_append_after_an_erasure_stores_nothing() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "stream_erased").await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
            SET state = 'TERMINATED', completed_at = NOW()
          WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("terminate");
    autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gdpr-req-3")
        .await
        .expect("erase");

    let stored = store::append_stream_chunks(
        &mut conn,
        exec_id,
        &[chunk(0, json!("alice@example.com"))],
        100,
    )
    .await
    .expect("append");

    assert_eq!(stored, 0);
    assert_eq!(read_all(&mut conn, exec_id).await, Vec::new());
}

/// An append that races an erasure waits for it, then stores nothing.
#[tokio::test]
async fn an_append_that_races_an_erasure_waits_and_stores_nothing() {
    let (url, _c) = setup_database().await;
    let mut eraser = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut eraser, exec_id, "stream_race").await;

    // Hold the erasure gate lock, as `erase_workflow_payloads` does.
    eraser.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(&mut eraser)
        .await
        .expect("lock");

    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        let mut conn = AsyncPgConnection::establish(&writer_url)
            .await
            .expect("connect");
        store::append_stream_chunks(&mut conn, exec_id, &[chunk(0, json!("secret"))], 100)
            .await
            .expect("append")
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !writer.is_finished(),
        "the append must wait for the erasure"
    );

    diesel::sql_query(
        "UPDATE harvest_workflow_executions
            SET state = 'TERMINATED', completed_at = NOW(),
                input = jsonb_build_object($2::text, true)
          WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(autumn_harvest::erase::ERASURE_TOMBSTONE_KEY)
    .execute(&mut eraser)
    .await
    .expect("tombstone");
    eraser.batch_execute("COMMIT").await.expect("commit");

    assert_eq!(writer.await.expect("writer"), 0);
    assert_eq!(read_all(&mut eraser, exec_id).await, Vec::new());
}

// ── Worker end to end ───────────────────────────────────────────────────────

/// Publishes two durable chunks, parks on a timer, then publishes two more.
/// The second cycle replays the first two calls.
fn durable_stream_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.publish_durable_progress(json!({"token": "Hello"}))
            .map_err(|e| e.to_string())?;
        ctx.publish_durable_progress(json!({"token": ","}))
            .map_err(|e| e.to_string())?;
        ctx.timer("stream_gap", 1)
            .await
            .map_err(|e| e.to_string())?;
        ctx.publish_durable_progress(json!({"token": " world"}))
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"ephemeral": true}))
            .map_err(|e| e.to_string())?;
        ctx.publish_durable_progress(json!({"token": "!"}))
            .map_err(|e| e.to_string())?;
        Ok(json!({"ok": true}))
    })
}

fn workflow_info(name: &'static str, handler: autumn_harvest::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "durable_stream_tests",
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

/// Seed a RUNNING execution and a claimable task, then run a real worker
/// until the execution completes.
async fn run_workflow(
    database_url: &str,
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    handler: autumn_harvest::WorkflowHandlerFn,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    insert_execution(conn, exec_id, workflow_name).await;
    let input = json!({});
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params).await.expect("enqueue");

    let registry = HandlerRegistry::new(vec![workflow_info(workflow_name, handler)], vec![]);
    let worker = crate::integration_e2e::build_runtime_worker(
        &format!("stream-worker-{workflow_name}"),
        2,
        2,
        Arc::new(registry),
    );
    let pool = crate::integration_e2e::build_test_pool(database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&pool).await;
    });

    let url = database_url.to_string();
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let execution = crate::integration_e2e::load_execution_from_url(&url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    worker.shutdown();
    handle.await.expect("worker task joins");
    completed.expect("workflow completes within the timeout");
    exec_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_stores_each_durable_chunk_once_at_a_contiguous_offset() {
    let (database_url, _container) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");
    let exec_id = run_workflow(
        &database_url,
        &mut conn,
        "durable_stream_wf",
        durable_stream_workflow,
    )
    .await;

    assert_eq!(
        read_all(&mut conn, exec_id).await,
        vec![
            (0, json!({"token": "Hello"})),
            (1, json!({"token": ","})),
            (2, json!({"token": " world"})),
            (3, json!({"token": "!"})),
        ],
        "each durable chunk once, at offsets 0..4; the ephemeral chunk is not stored"
    );

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the run spans two cycles, so cycle 2 replays the first calls"
    );
    assert!(
        !format!("{:?}", history.events).contains("world"),
        "a durable chunk never enters harvest_events"
    );
}

// ── Wake channel ────────────────────────────────────────────────────────────

/// A committed wake reaches the listener. A rolled-back wake does not.
/// Several wakes merge into one.
#[tokio::test]
async fn the_listener_wakes_on_commit_only_and_merges_wakes() {
    use autumn_harvest::notify::{DurableStreamListener, DurableStreamWait};

    let (url, _c) = setup_database().await;
    let exec_id = ExecutionId::new();
    let listener = DurableStreamListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let rolled_back = Box::pin(conn.transaction::<(), autumn_harvest::HarvestError, _>(
        async |c| {
            autumn_harvest::notify::notify_durable_stream(c, exec_id.as_uuid()).await?;
            Err(autumn_harvest::HarvestError::Database("roll back".into()))
        },
    ))
    .await;
    assert!(rolled_back.is_err());
    assert_eq!(
        listener.wait_timeout(Duration::from_millis(300)).await,
        DurableStreamWait::TimedOut,
        "a rolled-back cycle must not wake a reader"
    );

    // Three wakes in one transaction reach the listener as one. Postgres
    // merges equal notifications of a transaction. The post-commit sender
    // merges per channel too. Separate transactions can each wake the
    // reader, so the test does not use them.
    Box::pin(
        conn.transaction::<(), autumn_harvest::HarvestError, _>(async |c| {
            for _ in 0..3 {
                autumn_harvest::notify::notify_durable_stream(c, exec_id.as_uuid()).await?;
            }
            Ok(())
        }),
    )
    .await
    .expect("commit");
    assert_eq!(
        listener.wait_timeout(Duration::from_secs(5)).await,
        DurableStreamWait::Woken
    );
    assert_eq!(
        listener.wait_timeout(Duration::from_millis(500)).await,
        DurableStreamWait::TimedOut,
        "three wakes in one commit must merge into one"
    );
}

/// SQL that installs a `pg_notify` that always fails (issue #1796 pattern).
const FAILING_NOTIFY_SQL: &str = "CREATE SCHEMA IF NOT EXISTS notify_fail; \
    CREATE OR REPLACE FUNCTION notify_fail.pg_notify(text, text) RETURNS void \
    LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'pg_notify is disabled on this session'; END $$;";

/// The wake is optional. A failed `pg_notify` must not fail the chunk write,
/// as for the other post-commit wakes (issue #1796).
#[tokio::test]
async fn a_failed_wake_never_fails_the_chunk_write() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(FAILING_NOTIFY_SQL)
        .await
        .expect("failing pg_notify");
    conn.batch_execute("SET search_path = notify_fail, pg_catalog, public")
        .await
        .expect("search_path");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "failed_wake").await;

    let result = Box::pin(
        conn.transaction::<(), autumn_harvest::HarvestError, _>(async |c| {
            store::append_stream_chunks(c, exec_id, &[chunk(0, json!("a"))], 10).await?;
            autumn_harvest::notify::notify_durable_stream(c, exec_id.as_uuid()).await
        }),
    )
    .await;

    result.expect("the chunk write must commit when the wake fails");
    let stored = store::load_stream_chunks(&mut conn, exec_id, None, 10)
        .await
        .expect("load");
    assert_eq!(stored.len(), 1);
}

/// With a registered pool, the post-commit sender sends the wake after the
/// write commits, outside the write transaction.
#[tokio::test]
async fn a_registered_pool_wakes_the_reader_after_commit() {
    use autumn_harvest::notify::{DurableStreamListener, DurableStreamWait};
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let (url, _c) = setup_database().await;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool: autumn_harvest::worker::DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("pool");
    let sink = autumn_harvest::notify::register_pool(&pool);
    assert!(
        sink.wait_ready(Duration::from_secs(10)).await,
        "sender ready"
    );
    let exec_id = ExecutionId::new();
    let listener = DurableStreamListener::connect(&url, exec_id.as_uuid())
        .await
        .expect("listen");

    let mut conn = pool.get().await.expect("conn");
    Box::pin(
        conn.transaction::<(), autumn_harvest::HarvestError, _>(async |c| {
            autumn_harvest::notify::notify_durable_stream(c, exec_id.as_uuid()).await
        }),
    )
    .await
    .expect("commit");

    assert_eq!(
        listener.wait_timeout(Duration::from_secs(5)).await,
        DurableStreamWait::Woken
    );
}
