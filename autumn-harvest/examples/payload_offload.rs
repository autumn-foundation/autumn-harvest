#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Large-payload offloading via claim-check — issue #524.
//!
//! Demonstrates how an embedder enables transparent offloading of oversized
//! payloads to external storage by implementing **one trait** ([`PayloadStore`])
//! and adding **one builder call** ([`HarvestBuilder::payload_store`]) — with no
//! change to any workflow or activity function signature.
//!
//! ## The problem
//!
//! A legitimately large payload — a scraped HTML document, an ML feature vector,
//! a generated PDF — cannot durably flow through a workflow: the #252 size cap
//! *rejects* anything over the limit (to keep `harvest_events` small and replay
//! fast), and `PayloadCodec` is a synchronous in-place transform that can't do
//! async I/O against external storage.
//!
//! ## The solution
//!
//! Register a [`PayloadStore`] (e.g. S3, GCS, a filesystem — harvest core ships
//! none, preserving the Postgres-only boundary) and an offload threshold. Any
//! payload-bearing field larger than the threshold is written to the store and
//! replaced inline with a small, checksummed **reference envelope**. Replay
//! fetches the blob back and reconstructs the exact original bytes.
//!
//! ```no_run
//! use autumn_harvest::builder::HarvestBuilder;
//! # fn build(store: impl autumn_harvest::payload_store::PayloadStore) {
//! let builder = HarvestBuilder::new()
//!     .payload_store(store)                       // implement PayloadStore
//!     .payload_offload_threshold(256 * 1024);     // offload anything > 256 KiB
//! # let _ = builder;
//! # }
//! ```
//!
//! Run with: `cargo run --example payload_offload`

use std::collections::HashMap;
use std::sync::Mutex;

use autumn_harvest::payload_store::{PayloadStore, PayloadStoreError, PayloadStoreFuture};

/// A trivial in-memory content-addressed store. A real embedder would back this
/// with S3/GCS/etc. — `put` uploads bytes and returns a key, `get` downloads
/// them, `delete` removes the blob during retention GC.
#[derive(Default)]
pub struct InMemoryStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl PayloadStore for InMemoryStore {
    fn store_id(&self) -> &str {
        "in-memory"
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        // Content-addressed: identical bytes map to identical keys, so a
        // continue-as-new / child carry-forward never re-uploads.
        let key = format!("blob/{:x}", seahash_like(bytes));
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), bytes.to_vec());
        Box::pin(async move { Ok(key) })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().unwrap().get(key).cloned();
        let key = key.to_string();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("no blob at {key}"))) })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs.lock().unwrap().remove(key);
        Box::pin(async move { Ok(()) })
    }
}

/// A tiny non-cryptographic hash so the example has no extra deps.
fn seahash_like(bytes: &[u8]) -> u64 {
    let mut h: u64 = 1469598103934665603;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(1099511628211);
    }
    h
}

fn main() {
    // Register the store + threshold on the builder. Workflows and activities
    // are written exactly as before — offloading is fully transparent.
    let builder = autumn_harvest::builder::HarvestBuilder::new()
        .payload_store(InMemoryStore::default())
        .payload_offload_threshold(256 * 1024);
    let built = builder.build();

    println!(
        "PayloadStore registered: offloader present = {}",
        built.payload_offloader().is_some()
    );
    if let Some(off) = built.payload_offloader() {
        println!("offload threshold = {} bytes", off.threshold());
        println!("store id = {}", off.store_id());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::payload_store::PayloadOffloader;
    use autumn_harvest::telemetry::NoOpMetrics;
    use std::sync::Arc;

    #[tokio::test]
    async fn store_round_trips_bytes() {
        let store = Arc::new(InMemoryStore::default());
        let key = store.put(b"hello world").await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), b"hello world");
        store.delete(&key).await.unwrap();
        assert!(store.get(&key).await.is_err());
    }

    #[tokio::test]
    async fn offloader_replaces_large_field_with_envelope() {
        let store = Arc::new(InMemoryStore::default());
        let off = PayloadOffloader::new(store, 16, Arc::new(NoOpMetrics));
        let mut event = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": { "output": "z".repeat(10_000) }
        });
        let refs = off.offload_event_value(&mut event).await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(event["data"]["output"]["_harvest_offload_envelope"], 1);
        off.inflate_event_value(&mut event).await.unwrap();
        assert_eq!(
            event["data"]["output"],
            serde_json::json!("z".repeat(10_000))
        );
    }
}
