//! S3 adapters against a MinIO emulator (issue #1983).
//!
//! Needs Docker. The suite starts `pgsty/minio`, the maintained community
//! build of MinIO. The official MinIO images are no longer published.
//!
//! ```sh
//! cargo test -p autumn-harvest-plugin --features s3 --test object_store_s3_minio
//! ```
#![cfg(feature = "s3")]
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
use autumn_harvest_plugin::object_store::s3::{S3Backend, aws_sdk_s3};
use autumn_harvest_plugin::object_store::{ObjectHistoryArchiver, ObjectPayloadStore};
use object_store_e2e::MARKER;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// The MinIO image. CI pre-pulls this exact tag.
const MINIO_IMAGE: &str = "pgsty/minio";
const MINIO_TAG: &str = "RELEASE.2026-08-04T00-00-00Z";
const USER: &str = "harvest";
const PASSWORD: &str = "harvest-secret";
const BUCKET: &str = "harvest-test";

struct Minio {
    _container: ContainerAsync<GenericImage>,
    client: aws_sdk_s3::Client,
}

async fn minio() -> Minio {
    let container = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_either_std("API:"))
        .with_env_var("MINIO_ROOT_USER", USER)
        .with_env_var("MINIO_ROOT_PASSWORD", PASSWORD)
        .with_cmd(["server", "/data"])
        .start()
        .await
        .expect("start MinIO");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(format!("http://{host}:{port}"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            USER, PASSWORD, None, None, "minio",
        ))
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);
    // MinIO can log "API:" a moment before it accepts requests.
    let mut created = false;
    for _ in 0..50 {
        if client.create_bucket().bucket(BUCKET).send().await.is_ok() {
            created = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(created, "create the test bucket");
    Minio {
        _container: container,
        client,
    }
}

/// Read an object with the AWS client, not with the adapter under test.
async fn raw_object(client: &aws_sdk_s3::Client, key: &str) -> Vec<u8> {
    let output = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .expect("get object");
    output.body.collect().await.unwrap().into_bytes().to_vec()
}

fn backend(minio: &Minio) -> Arc<S3Backend> {
    Arc::new(S3Backend::new(minio.client.clone(), BUCKET))
}

fn aead_codecs() -> PayloadCodecs {
    let codecs = PayloadCodecs::default();
    let codec = AeadCodec::new("k-s3", &DataKey::generate()).unwrap();
    codecs.register_key("k-s3", Arc::new(codec)).unwrap();
    codecs.set_active_key("k-s3").unwrap();
    codecs
}

#[tokio::test]
async fn s3_payload_store_round_trips() {
    let minio = minio().await;
    let store = ObjectPayloadStore::new(backend(&minio)).with_prefix("blobs/");
    let key = store.put(b"hello s3").await.unwrap();
    assert!(key.starts_with("blobs/"));
    assert_eq!(store.get(&key).await.unwrap(), b"hello s3");
    assert_eq!(raw_object(&minio.client, &key).await, b"hello s3");
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(store.get(&key).await.is_err(), "a deleted blob is gone");
}

#[tokio::test]
async fn s3_offloaded_blob_is_ciphertext_with_the_codec_on() {
    let minio = minio().await;
    let codecs = aead_codecs();
    let store = Arc::new(ObjectPayloadStore::new(backend(&minio)));
    let offloader = PayloadOffloader::new(store, 64, Arc::new(NoOpMetrics));
    let event = WorkflowEvent::WorkflowCompleted {
        output: serde_json::json!({ "secret": MARKER, "pad": "x".repeat(512) }),
    };
    let mut value = codecs.encode_event(&event).unwrap();
    let refs = offloader.offload_event_value(&mut value).await.unwrap();
    assert_eq!(refs.len(), 1, "the large output is offloaded");

    let raw = raw_object(&minio.client, &refs[0].blob_key).await;
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
async fn s3_archiver_uploads_ciphertext_and_fetches_it_back() {
    let minio = minio().await;
    let archiver = ObjectHistoryArchiver::new(backend(&minio))
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

    let raw = raw_object(&minio.client, &format!("history/{id}.json")).await;
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
async fn s3_retention_archive_reads_back_through_api_and_vantage() {
    let minio = minio().await;
    object_store_e2e::retention_archive_reads_back_through_api_and_vantage(backend(&minio)).await;
}
