#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::history_export::{
    HistoryExportDocument, HistoryExportRequest, HistoryPayloadPolicy, export_history,
};
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::payload_store::PayloadStore;
use autumn_harvest::retention::{ArchiveFetchError, HistoryArchiver};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::WorkflowEvent;

use super::{MemoryBackend, ObjectBackend, ObjectHistoryArchiver, ObjectPayloadStore};

const MARKER: &str = "PLAINTEXT-MARKER-1983";

pub(crate) fn sample_doc(execution_id: ExecutionId) -> HistoryExportDocument {
    export_history(HistoryExportRequest {
        workflow_name: "archived_wf".to_string(),
        workflow_id: Some(format!("wf-{MARKER}")),
        queue_name: Some("default".to_string()),
        execution_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events: vec![WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "secret": MARKER }),
        }],
        exported_at: chrono::Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(usize::MAX),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
    })
    .unwrap()
}

pub(crate) fn aead_codecs(key_id: &str) -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let codec = AeadCodec::new(key_id, &DataKey::generate()).unwrap();
    codecs.register_key(key_id, Arc::new(codec)).unwrap();
    codecs.set_active_key(key_id).unwrap();
    codecs
}

fn json(doc: &HistoryExportDocument) -> serde_json::Value {
    serde_json::to_value(doc).unwrap()
}

#[tokio::test]
async fn payload_store_round_trips() {
    let backend = Arc::new(MemoryBackend::default());
    let store = ObjectPayloadStore::new(Arc::clone(&backend));
    let key = store.put(b"hello").await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), b"hello");
}

#[tokio::test]
async fn payload_store_is_content_addressed_under_the_prefix() {
    let backend = Arc::new(MemoryBackend::default());
    let store = ObjectPayloadStore::new(Arc::clone(&backend)).with_prefix("blobs/");
    let first = store.put(b"same").await.unwrap();
    let second = store.put(b"same").await.unwrap();
    let other = store.put(b"other").await.unwrap();
    assert_eq!(first, second, "identical bytes share a key");
    assert_ne!(first, other, "different bytes get different keys");
    assert!(first.starts_with("blobs/"), "key uses the prefix: {first}");
    assert_eq!(first.len(), "blobs/".len() + 64, "prefix plus SHA-256 hex");
    assert!(backend.get(&first).await.unwrap().is_some());
}

#[tokio::test]
async fn payload_store_get_of_a_missing_key_is_an_error() {
    let store = ObjectPayloadStore::new(Arc::new(MemoryBackend::default()));
    let err = store.get("missing").await.unwrap_err();
    assert!(err.0.contains("missing"), "error names the key: {err}");
}

#[tokio::test]
async fn payload_store_delete_is_idempotent() {
    let store = ObjectPayloadStore::new(Arc::new(MemoryBackend::default()));
    let key = store.put(b"bytes").await.unwrap();
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.is_err(), "the blob is gone");
}

#[test]
fn payload_store_id_defaults_to_the_trait_default() {
    let store = ObjectPayloadStore::new(Arc::new(MemoryBackend::default()));
    assert_eq!(store.store_id(), "default");
    let store = store.with_store_id("s3-main");
    assert_eq!(store.store_id(), "s3-main");
}

#[tokio::test]
async fn archiver_round_trips_a_document() {
    let backend = Arc::new(MemoryBackend::default());
    let archiver = ObjectHistoryArchiver::new(Arc::clone(&backend)).with_prefix("history/");
    let id = ExecutionId::new();
    let doc = sample_doc(id);
    archiver.archive(&doc).await.unwrap();

    let key = archiver.object_key(&id);
    assert_eq!(key, format!("history/{id}.json"));
    assert!(backend.get(&key).await.unwrap().is_some());

    let fetched = archiver.fetch(&id).await.unwrap().expect("archived");
    assert_eq!(json(&fetched), json(&doc));
}

#[tokio::test]
async fn archiver_overwrites_a_repeated_archive() {
    let archiver = ObjectHistoryArchiver::new(Arc::new(MemoryBackend::default()));
    let id = ExecutionId::new();
    archiver.archive(&sample_doc(id)).await.unwrap();
    let doc = sample_doc(id);
    archiver.archive(&doc).await.unwrap();
    let fetched = archiver.fetch(&id).await.unwrap().expect("archived");
    assert_eq!(json(&fetched), json(&doc));
}

#[tokio::test]
async fn archiver_fetch_of_a_missing_run_is_none() {
    let archiver = ObjectHistoryArchiver::new(Arc::new(MemoryBackend::default()));
    assert!(archiver.fetch(&ExecutionId::new()).await.unwrap().is_none());
}

#[tokio::test]
async fn archiver_rejects_an_object_for_another_run() {
    let backend = Arc::new(MemoryBackend::default());
    let archiver = ObjectHistoryArchiver::new(Arc::clone(&backend));
    let id = ExecutionId::new();
    let other = sample_doc(ExecutionId::new());
    let bytes = serde_json::to_vec(&other).unwrap();
    backend
        .put(&archiver.object_key(&id), bytes, "application/json")
        .await
        .unwrap();
    let err = archiver.fetch(&id).await.unwrap_err();
    assert!(matches!(err, ArchiveFetchError::Backend(_)), "{err}");
}

#[tokio::test]
async fn archiver_without_codecs_uploads_plain_json() {
    let backend = Arc::new(MemoryBackend::default());
    let archiver = ObjectHistoryArchiver::new(Arc::clone(&backend));
    let id = ExecutionId::new();
    archiver.archive(&sample_doc(id)).await.unwrap();
    let raw = backend.get(&archiver.object_key(&id)).await.unwrap().unwrap();
    let text = String::from_utf8(raw).unwrap();
    assert!(text.contains(MARKER), "codec off: the object is plain JSON");
}

#[tokio::test]
async fn archiver_with_codecs_uploads_ciphertext() {
    let backend = Arc::new(MemoryBackend::default());
    let archiver =
        ObjectHistoryArchiver::new(Arc::clone(&backend)).with_codecs(aead_codecs("k-1983"));
    let id = ExecutionId::new();
    let doc = sample_doc(id);
    archiver.archive(&doc).await.unwrap();

    let raw = backend.get(&archiver.object_key(&id)).await.unwrap().unwrap();
    let text = String::from_utf8(raw.clone()).unwrap();
    assert!(!text.contains(MARKER), "codec on: no plaintext in the object");
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert!(is_codec_envelope(&value), "the object is a codec envelope");

    let fetched = archiver.fetch(&id).await.unwrap().expect("archived");
    assert_eq!(json(&fetched), json(&doc), "fetch decodes the envelope");
}

#[tokio::test]
async fn archiver_fetch_without_the_key_is_an_error() {
    let backend = Arc::new(MemoryBackend::default());
    let writer =
        ObjectHistoryArchiver::new(Arc::clone(&backend)).with_codecs(aead_codecs("k-a"));
    let id = ExecutionId::new();
    writer.archive(&sample_doc(id)).await.unwrap();

    let reader = ObjectHistoryArchiver::new(Arc::clone(&backend)).with_codecs(aead_codecs("k-b"));
    let err = reader.fetch(&id).await.unwrap_err();
    assert!(matches!(err, ArchiveFetchError::Backend(_)), "{err}");
    let plain = ObjectHistoryArchiver::new(backend);
    assert!(plain.fetch(&id).await.is_err(), "no codecs: cannot decode");
}
