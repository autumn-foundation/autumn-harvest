//! Object-store adapters for `PayloadStore` and `HistoryArchiver` (issue #1983).
//!
//! [`ObjectBackend`] is a small seam over one bucket: put, get and delete by
//! key. Two adapters sit on top of it:
//!
//! - [`ObjectPayloadStore`] implements
//!   [`PayloadStore`](autumn_harvest::payload_store::PayloadStore) for
//!   claim-check offload. Keys are the SHA-256 of the bytes.
//! - [`ObjectHistoryArchiver`] implements
//!   [`HistoryArchiver`](autumn_harvest::retention::HistoryArchiver). It
//!   writes one object per run and reads it back for the management API.
//!
//! Backends: [`s3::S3Backend`] (feature `s3`) and [`gcs::GcsBackend`]
//! (feature `gcs`). [`MemoryBackend`] keeps objects in memory, for tests.
//!
//! ## Codec
//!
//! The offloader encodes a payload with the codec before it calls `put`. So
//! with a codec on, an offloaded blob is a codec envelope (ciphertext).
//!
//! Retention gives the archiver payload fields in their stored form. With a
//! codec on, they are codec envelopes too. [`ObjectHistoryArchiver::with_codecs`]
//! also encodes the whole document, so its metadata is ciphertext as well.
//! Use the builder's `payload_codecs()`, so that both use the same keys.
//!
//! ```text
//! let codecs = builder.payload_codecs().clone();
//! let backend = Arc::new(S3Backend::new(s3_client, "harvest-archive"));
//! let builder = builder
//!     .payload_store(ObjectPayloadStore::new(Arc::clone(&backend)).with_prefix("blobs/"))
//!     .history_archiver(
//!         ObjectHistoryArchiver::new(backend)
//!             .with_prefix("history/")
//!             .with_codecs(codecs),
//!     );
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use autumn_harvest::history_export::HistoryExportDocument;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::{PayloadStore, PayloadStoreError, PayloadStoreFuture};
use autumn_harvest::retention::{
    ArchiveFetchError, ArchiveFetchFuture, ArchiverFuture, HistoryArchiver,
};
use autumn_harvest::types::ExecutionId;
use sha2::{Digest, Sha256};

#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(feature = "s3")]
pub mod s3;

/// The default read limit of [`ObjectHistoryArchiver::fetch`]: 64 MiB.
///
/// A read holds the object, its parsed form and the response in memory at the
/// same time. The limit keeps one read from exhausting the process.
pub const DEFAULT_MAX_FETCH_BYTES: u64 = 64 * 1024 * 1024;

/// Future returned by [`ObjectBackend`] methods.
pub type ObjectFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ObjectStoreError>> + Send + 'a>>;

/// Error returned by an [`ObjectBackend`].
#[derive(Debug, thiserror::Error)]
#[error("object store error: {0}")]
pub struct ObjectStoreError(pub String);

/// One bucket in an object store.
///
/// Contract:
/// - [`put`](ObjectBackend::put) writes or overwrites the object at `key`.
/// - [`get`](ObjectBackend::get) returns `Ok(None)` when no object is at `key`.
/// - [`delete`](ObjectBackend::delete) succeeds when no object is at `key`.
pub trait ObjectBackend: Send + Sync + 'static {
    /// Write `bytes` to `key`.
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> ObjectFuture<'a, ()>;

    /// Read the object at `key`.
    fn get<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, Option<Vec<u8>>>;

    /// Delete the object at `key`.
    fn delete<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, ()>;

    /// Read the object at `key`, or fail when it is over `max_bytes`.
    ///
    /// The default reads the whole object and then checks its length. A
    /// backend that knows the size first should override this, so that it
    /// refuses a large object before it reads the body.
    fn get_bounded<'a>(
        &'a self,
        key: &'a str,
        max_bytes: u64,
    ) -> ObjectFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let found = self.get(key).await?;
            if let Some(bytes) = &found {
                check_size(key, bytes.len() as u64, max_bytes)?;
            }
            Ok(found)
        })
    }
}

/// Fail when an object of `size` bytes is over `max_bytes`.
pub(crate) fn check_size(key: &str, size: u64, max_bytes: u64) -> Result<(), ObjectStoreError> {
    if size > max_bytes {
        return Err(ObjectStoreError(format!(
            "object {key} is {size} bytes, over the read limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

/// An in-memory [`ObjectBackend`], for tests and local development.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    fn objects(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<u8>>> {
        self.objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ObjectBackend for MemoryBackend {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        _content_type: &'a str,
    ) -> ObjectFuture<'a, ()> {
        self.objects().insert(key.to_string(), bytes);
        Box::pin(async { Ok(()) })
    }

    fn get<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, Option<Vec<u8>>> {
        let found = self.objects().get(key).cloned();
        Box::pin(async move { Ok(found) })
    }

    fn delete<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, ()> {
        self.objects().remove(key);
        Box::pin(async { Ok(()) })
    }
}

/// A [`PayloadStore`] over an [`ObjectBackend`].
///
/// The key of a blob is `{prefix}{sha256-hex}`, so identical bytes share one
/// object. Retention deletes a blob only when no run refers to its key.
pub struct ObjectPayloadStore<B> {
    backend: Arc<B>,
    prefix: String,
    store_id: String,
}

impl<B: ObjectBackend> ObjectPayloadStore<B> {
    /// Create a store with no key prefix and the store id `"default"`.
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            prefix: String::new(),
            store_id: "default".to_string(),
        }
    }

    /// Put each blob key under `prefix`, for example `"blobs/"`.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Set the store id that each reference envelope records.
    ///
    /// A reference can only inflate through a store with the same id. Keep
    /// the id of the store that wrote the existing references.
    #[must_use]
    pub fn with_store_id(mut self, store_id: impl Into<String>) -> Self {
        self.store_id = store_id.into();
        self
    }
}

impl<B: ObjectBackend> PayloadStore for ObjectPayloadStore<B> {
    fn store_id(&self) -> &str {
        &self.store_id
    }

    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = format!("{}{}", self.prefix, hex_sha256(bytes));
        let bytes = bytes.to_vec();
        Box::pin(async move {
            self.backend
                .put(&key, bytes, "application/octet-stream")
                .await
                .map_err(|err| PayloadStoreError(err.0))?;
            Ok(key)
        })
    }

    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let key = key.to_string();
        Box::pin(async move {
            self.backend
                .get(&key)
                .await
                .map_err(|err| PayloadStoreError(err.0))?
                .ok_or_else(|| PayloadStoreError(format!("blob {key} is missing")))
        })
    }

    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        let key = key.to_string();
        Box::pin(async move {
            self.backend
                .delete(&key)
                .await
                .map_err(|err| PayloadStoreError(err.0))
        })
    }
}

/// A [`HistoryArchiver`] over an [`ObjectBackend`].
///
/// The key of a run is `{prefix}{execution_id}.json`. A second archive of the
/// same run overwrites the object.
pub struct ObjectHistoryArchiver<B> {
    backend: Arc<B>,
    prefix: String,
    codecs: Option<PayloadCodecs>,
    max_fetch_bytes: u64,
}

impl<B: ObjectBackend> ObjectHistoryArchiver<B> {
    /// Create an archiver with no key prefix and no document codec.
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            prefix: String::new(),
            codecs: None,
            max_fetch_bytes: DEFAULT_MAX_FETCH_BYTES,
        }
    }

    /// Put each object key under `prefix`, for example `"history/"`.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Encode the whole document with `codecs` before upload.
    ///
    /// The archiver encodes the document under the active key. A fetch needs
    /// that key.
    /// Keep a retired key registered while its archives must stay readable.
    #[must_use]
    pub fn with_codecs(mut self, codecs: PayloadCodecs) -> Self {
        self.codecs = Some(codecs);
        self
    }

    /// Refuse to read an archive object over `max_bytes`.
    ///
    /// The default is [`DEFAULT_MAX_FETCH_BYTES`]. The limit applies to the
    /// read path only. Retention can still archive a larger run.
    #[must_use]
    pub const fn with_max_fetch_bytes(mut self, max_bytes: u64) -> Self {
        self.max_fetch_bytes = max_bytes;
        self
    }

    /// The object key for `execution_id`.
    #[must_use]
    pub fn object_key(&self, execution_id: &ExecutionId) -> String {
        format!("{}{execution_id}.json", self.prefix)
    }

    fn encode(&self, doc: &HistoryExportDocument) -> Result<Vec<u8>, String> {
        let value = serde_json::to_value(doc).map_err(|err| err.to_string())?;
        let value = match &self.codecs {
            Some(codecs) => codecs
                .encode_payload(&value)
                .map_err(|err| err.to_string())?,
            None => value,
        };
        serde_json::to_vec(&value).map_err(|err| err.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<HistoryExportDocument, String> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
        let value = match &self.codecs {
            Some(codecs) => codecs
                .decode_payload(&value)
                .map_err(|err| err.to_string())?,
            None => value,
        };
        serde_json::from_value(value).map_err(|err| err.to_string())
    }
}

impl<B: ObjectBackend> HistoryArchiver for ObjectHistoryArchiver<B> {
    fn archive(&self, doc: &HistoryExportDocument) -> ArchiverFuture<'_> {
        let key = self.object_key(&doc.execution_id);
        let encoded = self.encode(doc);
        Box::pin(async move {
            let bytes = encoded?;
            self.backend.put(&key, bytes, "application/json").await?;
            Ok(())
        })
    }

    fn fetch(&self, execution_id: &ExecutionId) -> ArchiveFetchFuture<'_> {
        let key = self.object_key(execution_id);
        let execution_id = *execution_id;
        Box::pin(async move {
            let Some(bytes) = self
                .backend
                .get_bounded(&key, self.max_fetch_bytes)
                .await
                .map_err(|err| ArchiveFetchError::Backend(Box::new(err)))?
            else {
                return Ok(None);
            };
            let doc = self
                .decode(&bytes)
                .map_err(|err| ArchiveFetchError::Backend(format!("{key}: {err}").into()))?;
            if doc.execution_id != execution_id {
                return Err(ArchiveFetchError::Backend(
                    format!("{key} holds the archive of run {}", doc.execution_id).into(),
                ));
            }
            Ok(Some(doc))
        })
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests;
