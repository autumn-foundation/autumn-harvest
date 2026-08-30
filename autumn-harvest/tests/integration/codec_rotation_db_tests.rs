#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

//! DB-backed integration tests for payload-codec key rotation and the lazy
//! re-encryption sweep — issue #948.
//!
//! # AC coverage map
//!
//! - **AC1** (`kid` in the envelope; kid-less rows resolve to the legacy key
//!   id) — [`a_kidless_pre_upgrade_row_is_swept_onto_the_active_key`] proves the
//!   stored-bytes half end to end; the envelope shape itself is pinned by
//!   `payload_codec.rs`'s own unit tests.
//! - **AC2** (a flip takes effect for all new writes, no restart window) —
//!   [`new_writes_land_under_the_new_key_immediately_after_a_flip`].
//! - **AC3** (decode resolves any registered key; mixed histories replay) —
//!   [`a_mixed_key_history_loads_transparently`].
//! - **AC4** (batched, rate-limitable, idempotent, resumable via a durable
//!   per-shard cursor) — [`the_sweep_is_batched_and_resumes_from_its_cursor`],
//!   [`a_zero_batch_size_disables_the_sweep`],
//!   [`re_running_the_sweep_rewrites_nothing`],
//!   [`flipping_the_active_key_starts_a_fresh_pass`].
//! - **AC5** (replay fidelity across the in-place mutation) —
//!   [`replay_fidelity_is_byte_identical_across_a_sweep`], plus
//!   [`a_stale_read_can_never_overwrite_a_committed_erasure`] for the CAS guard
//!   that keeps exception #3 from resurrecting what exception #2 destroyed.
//! - **AC6** (fail-closed retirement gate) —
//!   [`retirement_is_refused_while_rows_remain_and_succeeds_at_zero`] and
//!   [`retirement_fails_closed_on_an_unreachable_shard`].
//! - **AC7** (per-shard rows-remaining per key id; the sweep metric) —
//!   [`rotation_progress_reports_rows_per_key_id_and_the_cursor`] and
//!   [`the_sweep_records_the_reencrypted_metric`]. The HTTP route itself is
//!   covered in the plugin crate's `codec_rotation_admin_integration.rs`.
//! - **AC8** (composition with offload / erasure) —
//!   [`offload_envelopes_and_tombstones_survive_a_sweep_untouched`].
//!
//! Runs against `HARVEST_TEST_DATABASE_URL` when set (each test gets its own
//! throwaway database, because the rotation census is shard-wide by design),
//! otherwise against a per-test Postgres container.

use std::sync::{Arc, Mutex};

use autumn_harvest::codec_rotation::{
    load_shard_rotation_progress, retire_codec_key, sweep_codec_reencryption_once,
};
use autumn_harvest::erase::erasure_tombstone;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::{
    CODEC_ENVELOPE_KID_KEY, CODEC_LEGACY_KEY_ID, CodecError, PayloadCodec, PayloadCodecs,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{ExecutionId, ShardId};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── codecs ───────────────────────────────────────────────────────────────────

/// Two instances differ only in key material — exactly the shape rotation has
/// to cope with, and exactly why a key id cannot live in `codec_id`.
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

/// Counts `record_codec_reencrypted` calls so AC7's metric can be asserted
/// without a Prometheus scrape.
#[derive(Default)]
struct CountingMetrics {
    reencrypted: Mutex<Vec<(String, u64)>>,
}

impl MetricsRecorder for CountingMetrics {
    fn record_codec_reencrypted(&self, shard: &str, count: u64) {
        self.reencrypted
            .lock()
            .unwrap()
            .push((shard.to_string(), count));
    }
}

// ── harness ──────────────────────────────────────────────────────────────────

/// A migrated Postgres that no other test shares.
///
/// The rotation census counts every `harvest_events` row on the shard — that is
/// the point of it — so these tests cannot share a database with anything else.
async fn setup_isolated_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_codec_rot_{}", Uuid::new_v4().simple());
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("HARVEST_TEST_DATABASE_URL must be reachable");
        admin
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .expect("create throwaway database");
        let url = swap_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to throwaway database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("apply migrations");
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

/// Replace the database component of a `postgres://` URL.
fn swap_database(url: &str, db_name: &str) -> String {
    let (base, _) = url.split_once('?').unwrap_or((url, ""));
    let cut = base.rfind('/').expect("a postgres URL has a database path");
    format!("{}/{db_name}", &base[..cut])
}

fn build_pool(url: &str) -> autumn_harvest::worker::DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build pool")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn insert_execution(conn: &mut AsyncPgConnection, name: &str) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions;
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: &Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
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
        .expect("insert execution");
    exec_id
}

/// Append `events` encoded under `key_id`, restoring the previously active key
/// so a test can compose a genuinely mixed-key history.
async fn append_under_key(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    exec_id: ExecutionId,
    key_id: &str,
    start_id: i32,
    events: &[WorkflowEvent],
) {
    let restore = codecs.active_key_id();
    codecs.set_active_key(key_id).expect("activate for fixture");
    store::append_events_with_codecs(conn, exec_id, events, start_id, codecs)
        .await
        .expect("append events");
    codecs.set_active_key(&restore).expect("restore active key");
}

fn started(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn completed(output: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowCompleted { output }
}

/// A registry holding `k1` (outgoing) and `k2` (incoming), active on `k1`.
fn two_key_registry() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1", Arc::new(XorCodec(0x11)))
        .expect("register k1");
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k1").expect("activate k1");
    codecs
}

async fn raw_event_data(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<Value> {
    use autumn_harvest::schema::harvest_events;
    harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::event_data)
        .load::<Value>(conn)
        .await
        .expect("load raw events")
}

async fn cursor_row_count(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let row: Count =
        diesel::sql_query("SELECT COUNT(*)::BIGINT AS n FROM harvest_codec_rotation_cursor")
            .get_result(conn)
            .await
            .expect("count cursor rows");
    row.n
}

fn kid_of(event_data: &Value, field: &str) -> Option<String> {
    event_data["data"][field][CODEC_ENVELOPE_KID_KEY]
        .as_str()
        .map(str::to_string)
}

// ── AC1 / AC4: the sweep converts stored history ─────────────────────────────

#[tokio::test]
async fn a_kidless_pre_upgrade_row_is_swept_onto_the_active_key() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    // A pre-#948 deployment: one codec, registered under the legacy key id, so
    // its envelopes carry no `kid` at all.
    let codecs = PayloadCodecs::default();
    codecs
        .register_key(CODEC_LEGACY_KEY_ID, Arc::new(XorCodec(0x11)))
        .expect("register legacy");
    let exec_id = insert_execution(&mut conn, "rotate_me").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        CODEC_LEGACY_KEY_ID,
        0,
        &[started(json!({"user": "alice"}))],
    )
    .await;
    let before = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&before[0], "input"),
        None,
        "the fixture really is a kid-less pre-upgrade row"
    );

    // Rotate onto k2 and sweep.
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");
    codecs.set_active_key("k2").expect("activate k2");
    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 1);
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&after[0], "input"), Some("k2".to_string()));
    // And the plaintext survived the trip.
    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("load history");
    match &history.events[0] {
        WorkflowEvent::WorkflowStarted { input, .. } => {
            assert_eq!(*input, json!({"user": "alice"}));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn new_writes_land_under_the_new_key_immediately_after_a_flip() {
    // AC2: no restart-ordering window. The registry clone handed to the write
    // path was taken BEFORE the flip.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let captured_at_boot = codecs.clone();

    let exec_id = insert_execution(&mut conn, "flip").await;
    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[started(json!({"n": 1}))],
        0,
        &captured_at_boot,
    )
    .await
    .expect("append pre-flip");

    codecs.set_active_key("k2").expect("flip to k2");

    store::append_events_with_codecs(
        &mut conn,
        exec_id,
        &[completed(json!({"n": 2}))],
        1,
        &captured_at_boot,
    )
    .await
    .expect("append post-flip");

    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
    assert_eq!(
        kid_of(&rows[1], "output"),
        Some("k2".to_string()),
        "a write through a pre-flip clone must still use the new key"
    );
}

#[tokio::test]
async fn a_mixed_key_history_loads_transparently() {
    // AC3.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "mixed").await;

    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"first": true}))],
    )
    .await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k2",
        1,
        &[completed(json!({"second": true}))],
    )
    .await;

    let history = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("mixed-key history must load");
    assert_eq!(history.events.len(), 2);
    match (&history.events[0], &history.events[1]) {
        (
            WorkflowEvent::WorkflowStarted { input, .. },
            WorkflowEvent::WorkflowCompleted { output, .. },
        ) => {
            assert_eq!(*input, json!({"first": true}));
            assert_eq!(*output, json!({"second": true}));
        }
        other => panic!("unexpected history: {other:?}"),
    }
}

#[tokio::test]
async fn the_sweep_is_batched_and_resumes_from_its_cursor() {
    // AC4: bounded per call, and the durable cursor makes the next call pick up
    // exactly where the last one stopped.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "batched").await;
    let events: Vec<WorkflowEvent> = (0..5).map(|i| started(json!({ "i": i }))).collect();
    append_under_key(&mut conn, &codecs, exec_id, "k1", 0, &events).await;
    codecs.set_active_key("k2").expect("flip");

    let first = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 1");
    assert_eq!(first, 2, "the batch limit really bounds the work");

    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    let cursor = progress.cursor.expect("a cursor row exists after a batch");
    assert!(cursor.last_event_id > 0);
    assert_eq!(cursor.rows_reencrypted, 2);
    assert_eq!(progress.rows_by_key_id.get("k1"), Some(&3));
    assert_eq!(progress.rows_by_key_id.get("k2"), Some(&2));

    let second = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 2");
    let third = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 2, &NoOpMetrics)
        .await
        .expect("batch 3");
    assert_eq!(
        second + third,
        3,
        "the remaining rows convert across batches"
    );

    let done = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(done.rows_remaining(), 0);
    assert_eq!(done.rows_by_key_id.get("k2"), Some(&5));
}

#[tokio::test]
async fn a_zero_batch_size_disables_the_sweep() {
    // AC4: rate-limitable, down to "off", with no redeploy.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "throttled").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 0, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0);
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
}

#[tokio::test]
async fn re_running_the_sweep_rewrites_nothing() {
    // AC4: idempotent.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "idempotent").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep 1"),
        1
    );
    let after_first = raw_event_data(&mut conn, exec_id).await;

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep 2"),
        0
    );
    assert_eq!(
        raw_event_data(&mut conn, exec_id).await,
        after_first,
        "a re-run leaves the stored bytes byte-identical"
    );
}

#[tokio::test]
async fn flipping_the_active_key_starts_a_fresh_pass() {
    // AC4: the cursor is keyed on (shard, active_key_id), so a second rotation
    // rescans from the start with no reset step to forget.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    codecs
        .register_key("k3", Arc::new(XorCodec(0x33)))
        .expect("register k3");
    let exec_id = insert_execution(&mut conn, "twice").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    codecs.set_active_key("k2").expect("flip to k2");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep onto k2"),
        1
    );

    codecs.set_active_key("k3").expect("flip to k3");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep onto k3"),
        1,
        "the second rotation must rescan from the start of the shard"
    );
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k3".to_string()));
}

// ── AC5: the fidelity proof behind sanctioned exception #3 ───────────────────

#[tokio::test]
async fn replay_fidelity_is_byte_identical_across_a_sweep() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "fidelity_workflow").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[
            started(json!({"user": "alice", "amounts": [1, 2, 3]})),
            completed(json!({"ok": true, "nested": {"k": null}})),
        ],
    )
    .await;

    let before = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("history before");
    let before_json = serde_json::to_string(&before.events).expect("serialize before");
    let report_before = WorkflowReplayer::new()
        .register_fn("fidelity_workflow", |_ctx, input| {
            Box::pin(async move { Ok(json!({"ok": true, "nested": {"k": null}, "echo": input})) })
        })
        .replay_from_events(before.events.clone())
        .await;
    assert!(
        matches!(report_before.status, ReplayStatus::ReplaySucceeded),
        "pre-sweep replay must succeed:\n{report_before}"
    );

    codecs.set_active_key("k2").expect("flip");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        2,
        "the sweep really did rewrite the stored bytes"
    );

    let after = store::load_history_with_codecs(&mut conn, exec_id, &codecs)
        .await
        .expect("history after");
    let after_json = serde_json::to_string(&after.events).expect("serialize after");
    assert_eq!(
        before_json, after_json,
        "the DECODED history must be byte-identical across the in-place mutation"
    );

    let report_after = WorkflowReplayer::new()
        .register_fn("fidelity_workflow", |_ctx, input| {
            Box::pin(async move { Ok(json!({"ok": true, "nested": {"k": null}, "echo": input})) })
        })
        .replay_from_events(after.events)
        .await;
    assert!(
        matches!(report_after.status, ReplayStatus::ReplaySucceeded),
        "post-sweep replay must succeed:\n{report_after}"
    );
}

#[tokio::test]
async fn an_erasure_tombstone_committed_before_the_sweep_is_never_overwritten() {
    // The ordinary (non-racing) half: a row already tombstoned carries no
    // ciphertext, so the sweep skips it outright. The racing half — a sweep
    // that read the row BEFORE the tombstone committed — is
    // [`a_stale_read_can_never_overwrite_a_committed_erasure`] below.
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "raced").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"ssn": "123-45-6789"}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    // Simulate the interleaving: the row changes under the sweep between its
    // read and its write. Tombstoning it directly is exactly what erase.rs does.
    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut tombstoned: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    tombstoned["data"]["input"] = erasure_tombstone();
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&tombstoned))
        .execute(&mut conn)
        .await
        .expect("tombstone");

    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0, "there is nothing left to rotate on a tombstone");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"],
        erasure_tombstone(),
        "the erasure tombstone must survive the sweep"
    );
}

#[tokio::test]
async fn a_stale_read_can_never_overwrite_a_committed_erasure() {
    // The compare-and-swap guard itself, exercised directly. This is the
    // interleaving the batch-oriented sweep entry point cannot express: the
    // sweep reads a row, a PII erasure (#495) tombstones it and COMMITS, and
    // only then does the sweep try to write its re-encrypted copy back.
    // Without the CAS that write would resurrect payload data the erasure had
    // just destroyed — the P1 this design exists to foreclose.
    use autumn_harvest::codec_rotation::{compare_and_swap_event, reencrypt_event_payload_fields};
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "cas").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"ssn": "123-45-6789"}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");

    // 1. The sweep reads the row and prepares its re-encrypted copy.
    let stale: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("stale read");
    let mut candidate = stale.clone();
    let outcome = reencrypt_event_payload_fields(&codecs, &mut candidate).expect("reencrypt");
    assert!(outcome.changed(), "the sweep really did produce a rewrite");

    // 2. An erasure tombstones the row and commits, under the sweep.
    let mut tombstoned = stale.clone();
    tombstoned["data"]["input"] = erasure_tombstone();
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&tombstoned))
        .execute(&mut conn)
        .await
        .expect("erasure commits");

    // 3. The sweep's write must lose.
    let swapped = compare_and_swap_event(&mut conn, row_id, &stale, &candidate)
        .await
        .expect("cas");

    assert!(!swapped, "a stale compare-and-swap must not take effect");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"],
        erasure_tombstone(),
        "the erasure must survive; ciphertext must never be resurrected"
    );
}

// ── AC8: composition with offload and erasure ────────────────────────────────

#[tokio::test]
async fn offload_envelopes_and_tombstones_survive_a_sweep_untouched() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "composed").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;

    let ids: Vec<i64> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::id)
        .load(&mut conn)
        .await
        .expect("ids");

    // Row 0's payload becomes an offload reference envelope; row 1's becomes an
    // erasure tombstone.
    let offload_envelope = json!({
        "_harvest_offload_envelope": 1,
        "store_id": "mem",
        "key": "blob/abc",
        "len": 4096,
        "checksum": "deadbeef",
    });
    for (row_id, field, replacement) in [
        (ids[0], "input", offload_envelope.clone()),
        (ids[1], "output", erasure_tombstone()),
    ] {
        let mut data: Value = harvest_events::table
            .find(row_id)
            .select(harvest_events::event_data)
            .first(&mut conn)
            .await
            .expect("row");
        data["data"][field] = replacement;
        diesel::update(harvest_events::table.find(row_id))
            .set(harvest_events::event_data.eq(&data))
            .execute(&mut conn)
            .await
            .expect("update");
    }

    codecs.set_active_key("k2").expect("flip");
    let swept = sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    assert_eq!(swept, 0, "neither field carries rotatable ciphertext");
    let after = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        after[0]["data"]["input"], offload_envelope,
        "the offload reference envelope is passed through, never double-encrypted"
    );
    assert_eq!(after[1]["data"]["output"], erasure_tombstone());
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "rows with no ciphertext must not block retirement forever"
    );
}

// ── AC6: the fail-closed retirement gate ─────────────────────────────────────

#[tokio::test]
async fn retirement_is_refused_while_rows_remain_and_succeeds_at_zero() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "retire").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let shards = [ShardId::new(0)];

    let err = retire_codec_key(&sharded, &shards, &codecs, "k1")
        .await
        .expect_err("retirement must be refused while a row remains");
    match err {
        HarvestError::CodecKeyRetirementBlocked { key_id, remaining } => {
            assert_eq!(key_id, "k1");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].shard_id, 0);
            assert_eq!(remaining[0].rows, 1, "the error names the remaining count");
            assert!(remaining[0].reachable);
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(
        codecs.codec_for_key("k1").is_some(),
        "a refused retirement must not drop the key"
    );

    sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
        .await
        .expect("sweep");

    retire_codec_key(&sharded, &shards, &codecs, "k1")
        .await
        .expect("retirement must succeed at exactly zero remaining rows");
    assert!(codecs.codec_for_key("k1").is_none());
}

#[tokio::test]
async fn retirement_fails_closed_on_an_unreachable_shard() {
    // AC6: an unreachable shard blocks retirement — it is never read as zero.
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    // Shard 0 is reachable and empty; shard 7 has no pool in this process.
    let shards = [ShardId::new(0), ShardId::new(7)];

    let err = retire_codec_key(&sharded, &shards, &codecs, "k1")
        .await
        .expect_err("an unreadable shard must block retirement");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].shard_id, 7);
            assert!(!remaining[0].reachable);
            assert_eq!(
                remaining[0].rows, 0,
                "unknown is reported as 0-but-unreachable"
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());
}

#[tokio::test]
async fn retirement_with_no_shards_to_inspect_is_refused() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(&sharded, &[], &codecs, "k1")
        .await
        .expect_err("proving nothing must not be treated as proving zero");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

#[tokio::test]
async fn the_active_key_can_never_be_retired() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k1")
        .await
        .expect_err("k1 is active");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

// ── AC7: progress reporting and the metric ───────────────────────────────────

#[tokio::test]
async fn rotation_progress_reports_rows_per_key_id_and_the_cursor() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "progress").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k2",
        2,
        &[WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::event::SideEffectKind::Custom,
            name: Some("x".to_string()),
            value: json!({"c": 3}),
        }],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");

    assert_eq!(progress.active_key_id, "k2");
    assert_eq!(progress.rows_by_key_id.get("k1"), Some(&2));
    assert_eq!(progress.rows_by_key_id.get("k2"), Some(&1));
    assert_eq!(progress.rows_remaining(), 2);
    assert!(
        progress.cursor.is_none(),
        "no cursor row exists before the first batch"
    );
}

#[tokio::test]
async fn the_sweep_records_the_reencrypted_metric() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "metered").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1})), completed(json!({"b": 2}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    let metrics = CountingMetrics::default();
    sweep_codec_reencryption_once(&mut conn, 3, &codecs, 100, &metrics)
        .await
        .expect("sweep");

    let recorded = metrics.reencrypted.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec![("3".to_string(), 2u64)],
        "harvest.codec.reencrypted is labelled by shard and counts swept rows"
    );
}

#[tokio::test]
async fn a_near_envelope_is_neither_counted_nor_swept() {
    // The census SQL mirrors `codec_envelope_parts` exactly: a four-key object
    // whose fourth key is not a string `kid` is not an envelope, in Postgres
    // just as in Rust. If the two drifted, the retirement gate would either
    // block forever or open early.
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "near").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    // Business data that merely *looks* like an envelope.
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 2,
        "codec_id": "xor",
        "data": "AAAA",
        "something_else": true,
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "a near-envelope must not be counted by the census"
    );
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0,
        "and must not be swept either"
    );
}

/// The SQL census must agree with Rust that a four-key **version 1** value is
/// plaintext, not an envelope — otherwise it would count business data that the
/// sweep can never convert and block retirement forever.
#[tokio::test]
async fn a_four_key_version_1_payload_is_not_counted_by_the_census() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "v1_business").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    // Exactly what a pre-#948 identity deployment could legitimately have
    // stored as business plaintext.
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 1,
        "codec_id": "xor",
        "data": "AAAA",
        "kid": "k1",
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(
        progress.rows_remaining(),
        0,
        "four-key version-1 plaintext must not be counted: {:?}",
        progress.rows_by_key_id
    );
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0,
        "and must not be rewritten"
    );
}

#[tokio::test]
async fn a_registry_with_no_keyed_codecs_sweeps_nothing_and_writes_no_cursor() {
    // The zero-overhead default: an un-rotated deployment pays nothing.
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(XorCodec(0x11)));
    let exec_id = insert_execution(&mut conn, "unrotated").await;
    store::append_events_with_codecs(&mut conn, exec_id, &[started(json!({"a": 1}))], 0, &codecs)
        .await
        .expect("append");
    let before = raw_event_data(&mut conn, exec_id).await;

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep"),
        0
    );
    assert_eq!(raw_event_data(&mut conn, exec_id).await, before);
    // The early return happens before any bookkeeping, so the sweep leaves no
    // trace at all — the observable half of "not one statement issued".
    assert_eq!(
        cursor_row_count(&mut conn).await,
        0,
        "an un-rotated deployment must not even create a cursor row"
    );
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert!(
        progress.rows_by_key_id.is_empty(),
        "the admin read must not run the census when rotation was never adopted"
    );
}

/// A rotation that is later ROLLED BACK must rescan the shard.
///
/// The cursor deliberately carries the target key id as a column rather than as
/// part of its key: resuming a rolled-back-to key's own already-completed pass
/// would skip every row written under the key being rolled back FROM, and leave
/// that key permanently unretirable.
#[tokio::test]
async fn rolling_back_to_a_previous_key_rescans_the_shard() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "rollback").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    // Forward: k1 -> k2, pass completes.
    codecs.set_active_key("k2").expect("flip to k2");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("forward sweep"),
        1
    );
    assert!(
        load_shard_rotation_progress(&mut conn, 0, &codecs)
            .await
            .expect("progress")
            .cursor
            .expect("cursor")
            .completed_at
            .is_some(),
        "the forward pass completed"
    );

    // Roll back: k2 -> k1. The k1 pass must start over, not resume.
    codecs.set_active_key("k1").expect("roll back to k1");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("rollback sweep"),
        1,
        "a rollback must rescan the shard, not resume the old k1 cursor"
    );
    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(kid_of(&rows[0], "input"), Some("k1".to_string()));
    assert_eq!(
        load_shard_rotation_progress(&mut conn, 0, &codecs)
            .await
            .expect("progress")
            .rows_remaining(),
        0
    );
}

/// A row the pass could not convert must not be abandoned behind the cursor.
///
/// The failure this guards is a two-phase rollout that activates the new key
/// before every process has the outgoing key registered: without the
/// unresolved-row accounting the pass would log each undecodable row, march the
/// cursor to the end of the shard, stamp itself complete, and leave those rows
/// on the retired key forever — with a manual `DELETE` on the cursor table as
/// the only recovery.
#[tokio::test]
async fn an_unconvertible_row_is_retried_once_its_key_comes_back() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;

    // Written under a key the sweeping process does not (yet) know.
    let writer = PayloadCodecs::default();
    writer
        .register_key("k0", Arc::new(XorCodec(0x00)))
        .expect("register k0");
    let exec_id = insert_execution(&mut conn, "late_key").await;
    append_under_key(
        &mut conn,
        &writer,
        exec_id,
        "k0",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k2", Arc::new(XorCodec(0x22)))
        .expect("register k2");

    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep with the key missing"),
        0
    );
    let stalled = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress")
        .cursor
        .expect("cursor");
    assert!(
        stalled.completed_at.is_none(),
        "a pass that left a row unconverted must NOT report itself complete"
    );
    assert_eq!(
        stalled.last_event_id, 0,
        "and must rewind so the row gets another attempt"
    );

    // The operator puts the key back, exactly as the runbook says.
    codecs
        .register_key("k0", Arc::new(XorCodec(0x00)))
        .expect("re-register k0");
    assert_eq!(
        sweep_codec_reencryption_once(&mut conn, 0, &codecs, 100, &NoOpMetrics)
            .await
            .expect("sweep after re-registering"),
        1,
        "the previously-unconvertible row must be picked up with no manual intervention"
    );
    let done = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert_eq!(done.rows_remaining(), 0);
    assert!(done.cursor.expect("cursor").completed_at.is_some());
}

/// The third documented fail-closed path: the shard is reachable but its census
/// errors. An unreadable shard is a blocker, never a zero.
#[tokio::test]
async fn retirement_fails_closed_when_a_shards_census_errors() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");

    // Make the census fail on an otherwise-reachable shard.
    conn.batch_execute("DROP TABLE harvest_events CASCADE")
        .await
        .expect("drop events table");

    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);
    let err = retire_codec_key(&sharded, &[ShardId::new(0)], &codecs, "k1")
        .await
        .expect_err("a failed census must block retirement");
    match err {
        HarvestError::CodecKeyRetirementBlocked { remaining, .. } => {
            assert_eq!(remaining.len(), 1);
            assert!(!remaining[0].reachable);
            assert!(
                remaining[0]
                    .reason
                    .as_deref()
                    .is_some_and(|r| r.contains("census")),
                "the error must say why: {:?}",
                remaining[0].reason
            );
        }
        other => panic!("expected CodecKeyRetirementBlocked, got {other:?}"),
    }
    assert!(codecs.codec_for_key("k1").is_some());
}

/// Retirement must refuse a shard list that omits a shard this process can see:
/// an omitted shard is never censused, so `Ok` for it would be vacuous.
#[tokio::test]
async fn retirement_refuses_a_shard_list_that_omits_a_known_shard() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    codecs.set_active_key("k2").expect("flip");
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    // Shard 0 exists in the pool but is not in the supplied list.
    let err = retire_codec_key(&sharded, &[ShardId::new(9)], &codecs, "k1")
        .await
        .expect_err("an incomplete shard list must block retirement");
    assert!(
        matches!(err, HarvestError::CodecKeyRetirementBlocked { .. }),
        "{err:?}"
    );
}

/// Retiring a key that was never registered must not report a vacuous success.
#[tokio::test]
async fn retirement_refuses_an_unregistered_key_id() {
    let (url, _c) = setup_isolated_db().await;
    let codecs = two_key_registry();
    let pool = build_pool(&url);
    let sharded = ShardedDbPool::single(pool);

    let err = retire_codec_key(&sharded, &[ShardId::new(0)], &codecs, "never-registered")
        .await
        .expect_err("an unregistered key proves nothing");
    assert!(matches!(err, HarvestError::Config(_)), "{err:?}");
}

/// AC4's "resident of the existing scanner cadence" half: the sweep must
/// actually run from `enforce_timeouts_once`, not only when called directly.
#[tokio::test]
async fn the_sweep_runs_as_a_resident_of_the_timeout_scanner() {
    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "scanner_resident").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;
    codecs.set_active_key("k2").expect("flip");

    autumn_harvest::timeout::enforce_timeouts_once(
        &mut conn,
        &NoOpMetrics,
        std::time::Duration::from_secs(5),
        &None,
        &[],
        None,
        None,
        60,
        &codecs,
        100,
    )
    .await
    .expect("timeout tick");

    let rows = raw_event_data(&mut conn, exec_id).await;
    assert_eq!(
        kid_of(&rows[0], "input"),
        Some("k2".to_string()),
        "the scanner tick must drive the sweep"
    );
}

/// A `kid` read back out of STORAGE is untrusted input.
///
/// On a deployment with no non-identity codec, a caller's workflow input is
/// stored verbatim — so envelope-shaped input carrying an arbitrary `kid` would
/// otherwise inject an attacker-chosen, unbounded key into the rotation census
/// and keep `rows_remaining` permanently non-zero, denying the retirement
/// procedure outright.
#[tokio::test]
async fn a_crafted_key_id_in_stored_input_is_not_counted() {
    use autumn_harvest::schema::harvest_events;

    let (url, _c) = setup_isolated_db().await;
    let mut conn = connect(&url).await;
    let codecs = two_key_registry();
    let exec_id = insert_execution(&mut conn, "crafted").await;
    append_under_key(
        &mut conn,
        &codecs,
        exec_id,
        "k1",
        0,
        &[started(json!({"a": 1}))],
    )
    .await;

    let row_id: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(harvest_events::id)
        .first(&mut conn)
        .await
        .expect("row id");
    let mut data: Value = harvest_events::table
        .find(row_id)
        .select(harvest_events::event_data)
        .first(&mut conn)
        .await
        .expect("row");
    data["data"]["input"] = json!({
        "_harvest_codec_envelope": 2,
        "codec_id": "xor",
        "data": "AAAA",
        "kid": "A".repeat(4096),
    });
    diesel::update(harvest_events::table.find(row_id))
        .set(harvest_events::event_data.eq(&data))
        .execute(&mut conn)
        .await
        .expect("update");

    codecs.set_active_key("k2").expect("flip");
    let progress = load_shard_rotation_progress(&mut conn, 0, &codecs)
        .await
        .expect("progress");
    assert!(
        progress.rows_by_key_id.keys().all(|k| k.len() <= 64),
        "an over-long crafted key id must never reach the census: {:?}",
        progress.rows_by_key_id
    );
    assert_eq!(
        progress.rows_remaining(),
        0,
        "crafted input must not be able to hold the retirement gate open"
    );
}
