//! GCS adapters against fake-gcs-server (issue #1983).
//!
//! Needs Docker. The suite starts `fsouza/fake-gcs-server`.
//!
//! ```sh
//! cargo test -p autumn-harvest-plugin --features gcs --test object_store_gcs_emulator
//! ```
#![cfg(feature = "gcs")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "support/object_store_e2e.rs"]
mod object_store_e2e;

use std::sync::Arc;

use autumn_harvest::aead_codec::{AeadCodec, DataKey};
use autumn_harvest::history_export::{
    HistoryExportRequest, HistoryPayloadPolicy, export_history,
};
use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::payload_store::{PayloadOffloader, PayloadStore};
use autumn_harvest::retention::HistoryArchiver;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::WorkflowEvent;
use autumn_harvest_plugin::object_store::gcs::{GcsBackend, NoAuth};
use autumn_harvest_plugin::object_store::{ObjectHistoryArchiver, ObjectPayloadStore};
use object_store_e2e::MARKER;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// The fake-gcs-server image. CI pre-pulls this exact tag.
const GCS_IMAGE: &str = "fsouza/fake-gcs-server";
const GCS_TAG: &str = "1.56.1";
const BUCKET: &str = "harvest-test";

struct FakeGcs {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    http: reqwest::Client,
}

async fn fake_gcs() -> FakeGcs {
    let container = GenericImage::new(GCS_IMAGE, GCS_TAG)
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_either_std("server started"))
        .with_cmd(["-scheme", "http", "-port", "4443"])
        .start()
        .await
        .expect("start fake-gcs-server");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(4443).await.unwrap();
    let endpoint = format!("http://{host}:{port}");
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{endpoint}/storage/v1/b"))
        .header("content-type", "application/json")
        .body(format!(r#"{{"name":"{BUCKET}"}}"#))
        .send()
        .await
        .expect("create bucket");
    assert!(response.status().is_success(), "create the test bucket");
    FakeGcs {
        _container: container,
        endpoint,
        http,
    }
}

/// Read an object with a plain HTTP call, not with the adapter under test.
async fn raw_object(gcs: &FakeGcs, key: &str) -> Vec<u8> {
    let name = key.replace('/', "%2F");
    let response = gcs
        .http
        .get(format!("{}/storage/v1/b/{BUCKET}/o/{name}?alt=media", gcs.endpoint))
        .send()
        .await
        .expect("get object");
    assert!(response.status().is_success(), "object {key} exists");
    response.bytes().await.unwrap().to_vec()
}

fn backend(gcs: &FakeGcs) -> Arc<GcsBackend> {
    Arc::new(GcsBackend::new(BUCKET, NoAuth).with_endpoint(&gcs.endpoint))
}

fn aead_codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let codec = AeadCodec::new("k-gcs", &DataKey::generate()).unwrap();
    codecs.register_key("k-gcs", Arc::new(codec)).unwrap();
    codecs.set_active_key("k-gcs").unwrap();
    codecs
}

#[tokio::test]
async fn gcs_payload_store_round_trips() {
    let gcs = fake_gcs().await;
    let store = ObjectPayloadStore::new(backend(&gcs)).with_prefix("blobs/");
    let key = store.put(b"hello gcs").await.unwrap();
    assert!(key.starts_with("blobs/"));
    assert_eq!(store.get(&key).await.unwrap(), b"hello gcs");
    assert_eq!(raw_object(&gcs, &key).await, b"hello gcs");
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.is_err(), "a deleted blob is gone");
}

#[tokio::test]
async fn gcs_offloaded_blob_is_ciphertext_with_the_codec_on() {
    let gcs = fake_gcs().await;
    let codecs = aead_codecs();
    let store = Arc::new(ObjectPayloadStore::new(backend(&gcs)));
    let offloader = PayloadOffloader::new(store, 64, Arc::new(NoOpMetrics));
    let event = WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({ "secret": MARKER, "pad": "x".repeat(512) }),
    };
    let mut value = codecs.encode_event(&event).unwrap();
    let refs = offloader.offload_event_value(&mut value).await.unwrap();
    assert_eq!(refs.len(), 1, "the large output is offloaded");

    let raw = raw_object(&gcs, &refs[0].blob_key).await;
    let text = String::from_utf8(raw.clone()).unwrap();
    assert!(!text.contains(MARKER), "the uploaded blob is ciphertext");
    let blob: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert!(is_codec_envelope(&blob), "the blob is a codec envelope");

    offloader.inflate_event_value(&mut value).await.unwrap();
    let decoded = codecs.decode_event(value).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(event).unwrap()
    );
}

#[tokio::test]
async fn gcs_archiver_uploads_ciphertext_and_fetches_it_back() {
    let gcs = fake_gcs().await;
    let archiver = ObjectHistoryArchiver::new(backend(&gcs))
        .with_prefix("history/")
        .with_codecs(aead_codecs());
    let id = ExecutionId::new();
    let doc = export_history(HistoryExportRequest {
        workflow_name: "archived_wf".to_string(),
        workflow_id: Some(format!("wf-{MARKER}")),
        queue_name: None,
        execution_id: id,
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
    .unwrap();
    archiver.archive(&doc).await.unwrap();

    let raw = raw_object(&gcs, &format!("history/{id}.json")).await;
    let text = String::from_utf8(raw.clone()).unwrap();
    assert!(!text.contains(MARKER), "the archive object is ciphertext");
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert!(is_codec_envelope(&value), "the object is a codec envelope");

    let fetched = archiver.fetch(&id).await.unwrap().expect("archived");
    assert_eq!(
        serde_json::to_value(fetched).unwrap(),
        serde_json::to_value(doc).unwrap()
    );
    assert!(archiver.fetch(&ExecutionId::new()).await.unwrap().is_none());
}

#[tokio::test]
async fn gcs_retention_archive_reads_back_through_api_and_vantage() {
    let gcs = fake_gcs().await;
    object_store_e2e::retention_archive_reads_back_through_api_and_vantage(backend(&gcs)).await;
}
