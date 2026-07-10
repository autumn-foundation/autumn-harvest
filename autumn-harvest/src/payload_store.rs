//! Large-payload offloading to external storage via claim-check (issue #524).
//!
//! **Why does this exist?**
//! `harvest_events` must stay small for fast, cheap deterministic replay, so the
//! payload-size cap (issue #252) *rejects* anything over the limit. That blocks a
//! legitimate class of workloads — document processing, media pipelines, RAG
//! ingestion, report generation — where a single step genuinely needs to hand a
//! multi-megabyte blob to the next step.
//!
//! The claim-check pattern turns that "reject" into a graceful "offload": when a
//! payload-bearing field is larger than a configurable threshold, harvest writes
//! the bytes to an **embedder-supplied** [`PayloadStore`] and keeps only a small,
//! self-describing **reference envelope** (store id, content key, byte length,
//! content checksum) inline in the event. On read/replay the bytes are fetched
//! back and the original value is reconstructed byte-for-byte.
//!
//! Harvest core ships **no** concrete S3/GCS client — the embedder owns the
//! backend, preserving the Postgres-only engine boundary. The reference envelope
//! rides inside the existing payload JSON exactly as the encryption codec's
//! output does, so **no new `WorkflowEvent` variant** is introduced and the
//! append-only / adjacently-tagged-JSON contracts are untouched.
//!
//! ## Composition with [`PayloadCodec`](crate::payload_codec::PayloadCodec)
//!
//! Offload composes **after** codec encode (encrypt-then-offload) on write, and
//! the inverse on read (fetch-then-decode). The offload envelope is keyed on a
//! distinct `_harvest_offload_envelope` discriminator so it never collides with
//! the codec's `_harvest_codec_envelope`.

use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{HarvestError, HarvestResult};
use crate::telemetry::MetricsRecorder;

/// Payload-bearing field keys carried inside an event's `data` object.
///
/// Mirrors the set transformed by
/// [`PayloadCodecs`](crate::payload_codec::PayloadCodecs) so offload applies
/// uniformly to every payload boundary: workflow input/output, activity
/// input/output, signal payload, query/update args & results, child-workflow
/// input/output, side-effect values, and scheduled carryover.
pub(crate) const PAYLOAD_FIELD_KEYS: [&str; 6] = [
    "input",
    "output",
    "payload",
    "details",
    "value",
    "last_completion_result",
];

/// Discriminator key marking an offload reference envelope.
const OFFLOAD_ENVELOPE_KEY: &str = "_harvest_offload_envelope";

/// Future returned by [`PayloadStore`] methods.
pub type PayloadStoreFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, PayloadStoreError>> + Send + 'a>>;

/// Error returned by a [`PayloadStore`] backend operation.
#[derive(Debug, thiserror::Error)]
#[error("payload store error: {0}")]
pub struct PayloadStoreError(pub String);

/// An embedder-supplied content-addressed blob store for offloaded payloads.
///
/// Implementations provide durable external storage (S3, GCS, a filesystem,
/// etc.). Harvest never ships a concrete backend — registering one via
/// [`HarvestBuilder::payload_store`](crate::builder::HarvestBuilder::payload_store)
/// is what enables offloading.
///
/// Contract:
/// - [`put`](PayloadStore::put) writes `bytes` and returns an opaque **key** that
///   [`get`](PayloadStore::get) can later resolve to the exact same bytes.
///   Implementations are encouraged (but not required) to be content-addressed
///   so identical bytes map to identical keys.
/// - [`get`](PayloadStore::get) returns the bytes previously stored under `key`.
/// - [`delete`](PayloadStore::delete) removes the blob; called by the retention
///   sweep once no execution references the key.
pub trait PayloadStore: Send + Sync + 'static {
    /// A stable identifier for this store, recorded in every reference envelope
    /// so a read can tell which backend a blob lives in. Defaults to
    /// `"default"`.
    #[allow(clippy::unnecessary_literal_bound)]
    fn store_id(&self) -> &str {
        "default"
    }

    /// Write `bytes` to the store and return a key that resolves back to them.
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String>;

    /// Fetch the bytes previously stored under `key`.
    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>>;

    /// Delete the blob stored under `key`.
    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()>;
}

/// A reference to an offloaded blob, recorded per execution so the retention
/// sweep can garbage-collect blobs no longer referenced by any execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadedRef {
    /// Opaque key returned by [`PayloadStore::put`].
    pub blob_key: String,
    /// Identifier of the store the blob lives in.
    pub store_id: String,
    /// Byte length of the offloaded payload.
    pub byte_len: u64,
}

/// Decoded contents of an offload reference envelope.
struct OffloadEnvelope {
    store_id: String,
    key: String,
    len: u64,
    checksum: String,
}

/// Drives offload-on-write and inflate-on-read against a configured
/// [`PayloadStore`].
///
/// Constructed by the builder when a store is registered and carried on the
/// worker's `HandlerRegistry` (and threaded to retention) so every payload-write
/// and history-read path can compose offload with the codec pipeline.
#[derive(Clone)]
pub struct PayloadOffloader {
    store: Arc<dyn PayloadStore>,
    threshold: u64,
    store_id: String,
    metrics: Arc<dyn MetricsRecorder>,
}

impl std::fmt::Debug for PayloadOffloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadOffloader")
            .field("store_id", &self.store_id)
            .field("threshold", &self.threshold)
            .finish_non_exhaustive()
    }
}

impl PayloadOffloader {
    /// Create an offloader over `store`, offloading any payload-bearing field
    /// whose serialized length exceeds `threshold` bytes.
    #[must_use]
    pub fn new(
        store: Arc<dyn PayloadStore>,
        threshold: u64,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let store_id = store.store_id().to_string();
        Self {
            store,
            threshold,
            store_id,
            metrics,
        }
    }

    /// The byte threshold at or below which payloads stay inline.
    #[must_use]
    pub const fn threshold(&self) -> u64 {
        self.threshold
    }

    /// The configured store's identifier.
    #[must_use]
    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    /// Borrow the underlying store (used by the retention sweep to delete blobs).
    #[must_use]
    pub fn store(&self) -> &Arc<dyn PayloadStore> {
        &self.store
    }

    /// Offload any over-threshold payload field inside a serialized event's
    /// `data` object, replacing each with a reference envelope in place.
    ///
    /// Operates on the **already codec-encoded** event value, so offload
    /// composes after [`PayloadCodec::encode`](crate::payload_codec::PayloadCodec::encode).
    /// Fields that are already offload envelopes (carry-forward / idempotent
    /// re-persist) are left untouched. Returns the set of blobs created so the
    /// caller can record per-execution references.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::PayloadOffload`] if a `put` fails, or
    /// [`HarvestError::Serialization`] if a field cannot be serialized.
    pub async fn offload_event_value(&self, value: &mut Value) -> HarvestResult<Vec<OffloadedRef>> {
        let mut refs = Vec::new();
        let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
            return Ok(refs);
        };
        for key in PAYLOAD_FIELD_KEYS {
            let Some(field) = data.get_mut(key) else {
                continue;
            };
            if field.is_null() || is_offload_envelope(field) {
                continue;
            }
            let bytes = serde_json::to_vec(field)?;
            if bytes.len() as u64 <= self.threshold {
                continue;
            }
            let byte_len = bytes.len() as u64;
            let checksum = hex_sha256(&bytes);
            let blob_key = self
                .store
                .put(&bytes)
                .await
                .map_err(|e| HarvestError::PayloadOffload(e.0))?;
            *field = build_offload_envelope(&self.store_id, &blob_key, byte_len, &checksum);
            self.metrics
                .record_payload_offloaded(key, &self.store_id, byte_len);
            refs.push(OffloadedRef {
                blob_key,
                store_id: self.store_id.clone(),
                byte_len,
            });
        }
        Ok(refs)
    }

    /// Reconstruct any offloaded payload field inside a serialized event's
    /// `data` object by fetching from the store, replacing each envelope with
    /// the original value in place.
    ///
    /// Runs **before** [`PayloadCodec::decode`](crate::payload_codec::PayloadCodec::decode)
    /// so the inverse ordering of write (encode-then-offload) holds. The fetched
    /// bytes are checksum-verified against the recorded envelope; a mismatch is a
    /// hard error rather than silent corruption.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::PayloadOffload`] on a `get` failure, an unknown
    /// `store_id`, or a checksum mismatch.
    pub async fn inflate_event_value(&self, value: &mut Value) -> HarvestResult<()> {
        let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
            return Ok(());
        };
        for key in PAYLOAD_FIELD_KEYS {
            let Some(field) = data.get_mut(key) else {
                continue;
            };
            let Some(env) = parse_offload_envelope(field) else {
                continue;
            };
            if env.store_id != self.store_id {
                return Err(HarvestError::PayloadOffload(format!(
                    "offloaded payload references unknown store '{}' (configured store is '{}')",
                    env.store_id, self.store_id
                )));
            }
            let started = std::time::Instant::now();
            let bytes = self
                .store
                .get(&env.key)
                .await
                .map_err(|e| HarvestError::PayloadOffload(e.0))?;
            self.metrics
                .record_payload_offload_fetch(&self.store_id, started.elapsed().as_secs_f64());
            let actual = hex_sha256(&bytes);
            if actual != env.checksum {
                return Err(HarvestError::PayloadOffload(format!(
                    "checksum mismatch for offloaded key '{}' (expected {}, got {})",
                    env.key, env.checksum, actual
                )));
            }
            if bytes.len() as u64 != env.len {
                return Err(HarvestError::PayloadOffload(format!(
                    "length mismatch for offloaded key '{}' (expected {} bytes, got {})",
                    env.key,
                    env.len,
                    bytes.len()
                )));
            }
            *field = serde_json::from_slice(&bytes)?;
        }
        Ok(())
    }
}

/// Recognise an offload reference envelope (without fetching) and extract its
/// blob reference.
///
/// Used by carry-forward (continue-as-new) to record a new reference to an
/// already-offloaded blob without re-uploading.
#[must_use]
pub fn extract_offload_ref(field: &Value) -> Option<OffloadedRef> {
    parse_offload_envelope(field).map(|env| OffloadedRef {
        blob_key: env.key,
        store_id: env.store_id,
        byte_len: env.len,
    })
}

/// Scan a serialized event's `data` object for offload reference envelopes and
/// return their blob references. Used to discover an execution's blobs.
#[must_use]
pub fn refs_in_event_value(value: &Value) -> Vec<OffloadedRef> {
    let Some(data) = value.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    PAYLOAD_FIELD_KEYS
        .iter()
        .filter_map(|key| data.get(*key).and_then(extract_offload_ref))
        .collect()
}

fn is_offload_envelope(field: &Value) -> bool {
    field
        .as_object()
        .is_some_and(|obj| obj.get(OFFLOAD_ENVELOPE_KEY).and_then(Value::as_i64) == Some(1))
}

fn parse_offload_envelope(field: &Value) -> Option<OffloadEnvelope> {
    let obj = field.as_object()?;
    if obj.get(OFFLOAD_ENVELOPE_KEY).and_then(Value::as_i64) != Some(1) {
        return None;
    }
    Some(OffloadEnvelope {
        store_id: obj.get("store_id").and_then(Value::as_str)?.to_string(),
        key: obj.get("key").and_then(Value::as_str)?.to_string(),
        len: obj.get("len").and_then(Value::as_u64)?,
        checksum: obj.get("checksum").and_then(Value::as_str)?.to_string(),
    })
}

fn build_offload_envelope(store_id: &str, key: &str, len: u64, checksum: &str) -> Value {
    serde_json::json!({
        OFFLOAD_ENVELOPE_KEY: 1,
        "store_id": store_id,
        "key": key,
        "len": len,
        "checksum": checksum,
    })
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory content-addressed store with put/get/delete spies.
    #[derive(Default)]
    struct MemStore {
        id: String,
        blobs: Mutex<HashMap<String, Vec<u8>>>,
        puts: AtomicUsize,
        gets: AtomicUsize,
        deletes: AtomicUsize,
    }

    impl MemStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                id: "mem".to_string(),
                ..Default::default()
            })
        }
    }

    impl PayloadStore for MemStore {
        fn store_id(&self) -> &str {
            &self.id
        }
        fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            // Content-addressed key.
            let key = format!("sha256/{}", hex_sha256(bytes));
            self.blobs
                .lock()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            Box::pin(async move { Ok(key) })
        }
        fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let found = self.blobs.lock().unwrap().get(key).cloned();
            let key = key.to_string();
            Box::pin(
                async move { found.ok_or_else(|| PayloadStoreError(format!("missing key {key}"))) },
            )
        }
        fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            self.blobs.lock().unwrap().remove(key);
            Box::pin(async move { Ok(()) })
        }
    }

    fn offloader(store: Arc<MemStore>, threshold: u64) -> PayloadOffloader {
        PayloadOffloader::new(store, threshold, Arc::new(crate::telemetry::NoOpMetrics))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn event_with_output(output: Value) -> Value {
        serde_json::json!({ "type": "WorkflowCompleted", "data": { "output": output } })
    }

    #[tokio::test]
    async fn offload_then_inflate_round_trips_byte_identical() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let original = serde_json::json!({ "blob": "x".repeat(10_000) });
        let mut event = event_with_output(original.clone());

        let refs = off.offload_event_value(&mut event).await.unwrap();
        assert_eq!(refs.len(), 1, "one field offloaded");
        // Inline representation is now a tiny reference envelope.
        let env = &event["data"]["output"];
        assert_eq!(env[OFFLOAD_ENVELOPE_KEY], 1);
        assert_eq!(env["store_id"], "mem");
        assert!(env.get("checksum").is_some());
        assert!(
            serde_json::to_vec(&event).unwrap().len() < 4096,
            "stored event stays small (success metric: < 4 KB)"
        );

        off.inflate_event_value(&mut event).await.unwrap();
        assert_eq!(event["data"]["output"], original, "100% byte fidelity");
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn small_field_stays_inline() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 1024);
        let mut event = event_with_output(serde_json::json!({ "n": 1 }));
        let refs = off.offload_event_value(&mut event).await.unwrap();
        assert!(refs.is_empty(), "below threshold: not offloaded");
        assert_eq!(event["data"]["output"], serde_json::json!({ "n": 1 }));
        assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn threshold_boundary_is_exclusive() {
        let store = MemStore::new();
        // A bare JSON string serializes to len = content + 2 quote bytes.
        let payload = serde_json::json!("a".repeat(30));
        let exact = serde_json::to_vec(&payload).unwrap().len() as u64; // 32
        // At threshold == exact -> stays inline (uses <=).
        let mut e1 = event_with_output(payload.clone());
        assert!(
            offloader(store.clone(), exact)
                .offload_event_value(&mut e1)
                .await
                .unwrap()
                .is_empty()
        );
        // One below -> offloaded.
        let mut e2 = event_with_output(payload);
        assert_eq!(
            offloader(store.clone(), exact - 1)
                .offload_event_value(&mut e2)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn already_offloaded_field_is_not_reuploaded() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let mut event = event_with_output(serde_json::json!({ "blob": "y".repeat(5_000) }));
        off.offload_event_value(&mut event).await.unwrap();
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        // Re-running offload on an already-enveloped value must not re-upload
        // (carry-forward / idempotent re-persist).
        let refs = off.offload_event_value(&mut event).await.unwrap();
        assert!(refs.is_empty());
        assert_eq!(store.puts.load(Ordering::SeqCst), 1, "no second put");
    }

    #[tokio::test]
    async fn checksum_mismatch_is_detected() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let mut event = event_with_output(serde_json::json!("z".repeat(1_000)));
        off.offload_event_value(&mut event).await.unwrap();
        // Corrupt the stored blob under its key.
        let key = event["data"]["output"]["key"].as_str().unwrap().to_string();
        store
            .blobs
            .lock()
            .unwrap()
            .insert(key, b"tampered".to_vec());
        let err = off.inflate_event_value(&mut event).await.unwrap_err();
        assert!(matches!(err, HarvestError::PayloadOffload(_)));
        assert!(err.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn extract_offload_ref_recognises_envelope_without_fetch() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let mut event = event_with_output(serde_json::json!("w".repeat(1_000)));
        let refs = off.offload_event_value(&mut event).await.unwrap();
        let extracted = extract_offload_ref(&event["data"]["output"]).unwrap();
        assert_eq!(extracted, refs[0]);
        // No get performed.
        assert_eq!(store.gets.load(Ordering::SeqCst), 0);
        // refs_in_event_value finds the same.
        assert_eq!(refs_in_event_value(&event), refs);
    }

    #[tokio::test]
    async fn composes_after_codec_envelope() {
        // Simulate codec-encoded output (the codec's 3-key envelope), then
        // offload it: the offload envelope must wrap the codec envelope, and
        // inflate must restore the codec envelope verbatim (so a later
        // codec.decode can run).
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let codec_value = serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "reverse",
            "data": "QUJD".repeat(2_000),
        });
        let mut event = event_with_output(codec_value.clone());
        off.offload_event_value(&mut event).await.unwrap();
        assert_eq!(event["data"]["output"][OFFLOAD_ENVELOPE_KEY], 1);
        off.inflate_event_value(&mut event).await.unwrap();
        assert_eq!(event["data"]["output"], codec_value);
    }

    #[tokio::test]
    async fn unknown_store_id_on_inflate_errors() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let mut event = event_with_output(serde_json::json!("q".repeat(1_000)));
        off.offload_event_value(&mut event).await.unwrap();
        // Rewrite the envelope's store_id to a store we don't have.
        event["data"]["output"]["store_id"] = serde_json::json!("other");
        let err = off.inflate_event_value(&mut event).await.unwrap_err();
        assert!(err.to_string().contains("unknown store"));
    }

    #[tokio::test]
    async fn multiple_payload_fields_offloaded_independently() {
        let store = MemStore::new();
        let off = offloader(store.clone(), 16);
        let mut event = serde_json::json!({
            "type": "ActivityCompleted",
            "data": {
                "input": "i".repeat(1_000),
                "output": "o".repeat(1_000),
                "small": "stays",
            }
        });
        let refs = off.offload_event_value(&mut event).await.unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(store.puts.load(Ordering::SeqCst), 2);
        off.inflate_event_value(&mut event).await.unwrap();
        assert_eq!(event["data"]["input"], serde_json::json!("i".repeat(1_000)));
        assert_eq!(
            event["data"]["output"],
            serde_json::json!("o".repeat(1_000))
        );
    }

    #[test]
    fn is_offload_envelope_recognizes_envelope() {
        let valid = serde_json::json!({
            OFFLOAD_ENVELOPE_KEY: 1,
            "store_id": "s3",
            "key": "123",
            "len": 100,
            "checksum": "abc"
        });
        assert!(is_offload_envelope(&valid));

        let invalid_val = serde_json::json!({
            OFFLOAD_ENVELOPE_KEY: 2,
        });
        assert!(!is_offload_envelope(&invalid_val));

        let missing_key = serde_json::json!({
            "store_id": "s3"
        });
        assert!(!is_offload_envelope(&missing_key));

        let not_object = serde_json::json!("string");
        assert!(!is_offload_envelope(&not_object));
    }

    #[test]
    fn parse_offload_envelope_extracts_correctly() {
        let valid = serde_json::json!({
            OFFLOAD_ENVELOPE_KEY: 1,
            "store_id": "s3",
            "key": "123",
            "len": 100,
            "checksum": "abc"
        });
        let env = parse_offload_envelope(&valid).unwrap();
        assert_eq!(env.store_id, "s3");
        assert_eq!(env.key, "123");
        assert_eq!(env.len, 100);
        assert_eq!(env.checksum, "abc");

        let missing_store = serde_json::json!({
            OFFLOAD_ENVELOPE_KEY: 1,
            "key": "123",
            "len": 100,
            "checksum": "abc"
        });
        assert!(parse_offload_envelope(&missing_store).is_none());

        let invalid_type = serde_json::json!({
            OFFLOAD_ENVELOPE_KEY: 1,
            "store_id": 123,
            "key": "123",
            "len": 100,
            "checksum": "abc"
        });
        assert!(parse_offload_envelope(&invalid_type).is_none());
    }

    #[test]
    fn build_offload_envelope_constructs_valid_json() {
        let env = build_offload_envelope("gcs", "xyz", 42, "def");
        assert!(is_offload_envelope(&env));
        let parsed = parse_offload_envelope(&env).unwrap();
        assert_eq!(parsed.store_id, "gcs");
        assert_eq!(parsed.key, "xyz");
        assert_eq!(parsed.len, 42);
        assert_eq!(parsed.checksum, "def");
    }
}
