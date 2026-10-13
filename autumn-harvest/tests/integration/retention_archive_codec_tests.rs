#![cfg(feature = "db")]
//! Retention archival with a payload codec on (issue #1983).
//!
//! Retention loaded the history with the identity-only codec registry. With a
//! real codec, that load failed on the first envelope. Retention then skipped
//! the run on every tick, and the archiver never got a document.
//!
//! These tests drive one real retention tick. They assert two things. The
//! archiver gets the document. Each payload field in it is still a codec
//! envelope, so the archive holds ciphertext.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise a
//! testcontainers Postgres starts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::history_export::HistoryExportDocument;
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture, is_offload_envelope,
};
use autumn_harvest::retention::{
    ArchiverFuture, HistoryArchiver, RetentionConfig, RetentionRuntime,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::Utc;
use diesel::sql_types::{Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A string that must never appear in an archive when the codec is on.
const MARKER: &str = "PLAINTEXT-MARKER-1983";

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
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

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_workflow_executions",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

/// Insert a COMPLETED execution that finished two days ago.
async fn insert_completed(conn: &mut AsyncPgConnection) -> ExecutionId {
    let two_days_ago = Utc::now() - chrono::Duration::days(2);
    let id = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at, completed_at)
         VALUES ($1, $2, 0, 'COMPLETED', '{}'::jsonb, $3, $3)
         RETURNING id",
    )
    .bind::<Text, _>("codec_wf")
    .bind::<Text, _>("codec-1")
    .bind::<Timestamptz, _>(two_days_ago)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id;
    ExecutionId::from_uuid(id)
}

fn aead_codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let codec = AeadCodec::new("k-1983", &DataKey::generate()).expect("codec");
    codecs
        .register_key("k-1983", Arc::new(codec))
        .expect("register key");
    codecs.set_active_key("k-1983").expect("activate key");
    codecs
}

fn history(input_blob: &str) -> Vec<WorkflowEvent> {
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({ "secret": MARKER, "blob": input_blob }),
            timestamp: Utc::now() - chrono::Duration::days(2),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "secret": MARKER }),
        },
    ]
}

#[derive(Default)]
struct CapturingArchiver {
    docs: Mutex<Vec<HistoryExportDocument>>,
}

impl HistoryArchiver for CapturingArchiver {
    fn archive(&self, doc: &HistoryExportDocument) -> ArchiverFuture<'_> {
        self.docs.lock().unwrap().push(doc.clone());
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl PayloadStore for MemStore {
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = format!("k{}", self.blobs.lock().unwrap().len());
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
        Box::pin(async { Ok(()) })
    }
}

/// Run one retention tick and return the shard-0 deleted count.
async fn run_one_tick(
    pool: DbPool,
    archiver: Arc<dyn HistoryArchiver>,
    offloader: Option<Arc<PayloadOffloader>>,
) -> usize {
    let config = RetentionConfig {
        max_age_secs: Some(86_400),
        audit_retention_days: 0,
        schedule_decision_retention_days: 0,
        ..RetentionConfig::default()
    };
    let runtime = RetentionRuntime::spawn(
        ShardedDbPool::single(pool),
        config,
        Arc::new(NoOpMetrics),
        Some(archiver),
        offloader,
    )
    .expect("retention runtime should spawn when enabled");
    runtime.run_now();
    let mut deleted = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            deleted = Some(r.deleted_count);
            break;
        }
    }
    runtime.shutdown();
    deleted.expect("retention tick did not report a result in time")
}

/// Assert that every payload field in the archive is a codec envelope.
fn assert_archive_is_ciphertext(doc: &HistoryExportDocument) {
    assert_eq!(doc.events.len(), 2, "both events are archived");
    let input = &doc.events[0]["data"]["input"];
    let output = &doc.events[1]["data"]["output"];
    assert!(is_codec_envelope(input), "input is ciphertext: {input}");
    assert!(is_codec_envelope(output), "output is ciphertext: {output}");
    assert!(!is_offload_envelope(input), "offloaded input is inflated");
    let text = serde_json::to_string(doc).expect("serialize");
    assert!(!text.contains(MARKER), "no plaintext in the archive");
}

#[tokio::test]
async fn retention_archive_keeps_codec_ciphertext() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;
    let exec_id = insert_completed(&mut conn).await;
    let codecs = aead_codecs();
    autumn_harvest::store::append_events_with_codecs(&mut conn, exec_id, &history("x"), 0, &codecs)
        .await
        .expect("append encoded history");

    let archiver = Arc::new(CapturingArchiver::default());
    let deleted = run_one_tick(
        build_pool(&url),
        Arc::clone(&archiver) as Arc<dyn HistoryArchiver>,
        None,
    )
    .await;

    assert_eq!(deleted, 1, "the run is archived and deleted");
    let docs = archiver.docs.lock().unwrap().clone();
    assert_eq!(docs.len(), 1, "the archiver got one document");
    assert_eq!(docs[0].execution_id, exec_id);
    assert_archive_is_ciphertext(&docs[0]);
}

#[tokio::test]
async fn retention_archive_inflates_offloaded_ciphertext() {
    let (url, _container) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    scrub(&mut conn).await;
    let exec_id = insert_completed(&mut conn).await;
    let codecs = aead_codecs();
    let store = Arc::new(MemStore::default());
    let offloader = Arc::new(PayloadOffloader::new(
        Arc::clone(&store) as Arc<dyn PayloadStore>,
        1024,
        Arc::new(NoOpMetrics),
    ));
    autumn_harvest::store::append_events_offloaded_with_codecs(
        &mut conn,
        exec_id,
        &history(&"Q".repeat(8 * 1024)),
        0,
        Some(&offloader),
        &codecs,
    )
    .await
    .expect("append encoded and offloaded history");
    assert!(
        !store.blobs.lock().unwrap().is_empty(),
        "the large input is offloaded"
    );

    let archiver = Arc::new(CapturingArchiver::default());
    let deleted = run_one_tick(
        build_pool(&url),
        Arc::clone(&archiver) as Arc<dyn HistoryArchiver>,
        Some(offloader),
    )
    .await;

    assert_eq!(deleted, 1, "the run is archived and deleted");
    let docs = archiver.docs.lock().unwrap().clone();
    assert_eq!(docs.len(), 1, "the archiver got one document");
    assert_archive_is_ciphertext(&docs[0]);
}
