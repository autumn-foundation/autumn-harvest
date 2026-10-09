//! A cross-run cache of model answers (issue #1998).
//!
//! Replay reads an answer back from the history of one run. This cache serves
//! an identical request from another run, so a retry, a fork or an evaluation
//! run does not pay again.
//!
//! The cache is opt-in. Install it with
//! [`AgentHarness::response_cache`](crate::AgentHarness::response_cache).
//! A hit is the result of the `agent_model_turn` activity, so history records
//! it like any other answer. Replay reads history and never reads the cache.
//!
//! The answer goes to a [`PayloadStore`], encoded by [`PayloadCodecs`]. A
//! [`CacheIndex`] maps each cache key to the key of its blob.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use autumn_harvest::payload_codec::{PayloadCodecs, is_codec_envelope};
use autumn_harvest::payload_store::PayloadStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{AgentError, ErrorKind};
use crate::message::{ChatMessage, ContentPart, StopReason, TokenUsage, ToolDefinition};
use crate::model::{BoxFuture, ChatRequest, ChatResponse};

/// The default capacity of an [`InMemoryCacheIndex`], in entries.
pub const DEFAULT_INDEX_CAPACITY: usize = 10_000;

/// The version of the key and entry format. A new format gives new keys.
const FORMAT_VERSION: u32 = 1;

/// Who may share a cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CacheScope {
    /// Only runs of the same tenant share an entry. A run with no tenant
    /// shares only with other runs that have no tenant.
    #[default]
    Tenant,
    /// Every run shares every entry, whatever its tenant. Use it only for
    /// prompts that hold no tenant data.
    Shared,
}

/// The tenant that a key belongs to.
///
/// The source of the tenant is part of the key. A declared tenant never
/// shares an entry with a verified tenant of the same name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyTenant<'a> {
    /// A run with no tenant.
    None,
    /// The tenant that the engine verified for the run (issue #1977).
    Verified(&'a str),
    /// The tenant that the task names. The caller that starts the run
    /// writes it.
    Declared(&'a str),
}

/// The key of one cache entry: a SHA-256 hash, as lowercase hex.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CacheKey(String);

impl CacheKey {
    /// The key as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Maps a [`CacheKey`] to the key of its blob in the [`PayloadStore`].
///
/// The index holds two opaque keys and no prompt or answer.
pub trait CacheIndex: Send + Sync + std::fmt::Debug {
    /// The blob key stored for `key`, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the index cannot read.
    fn get<'a>(&'a self, key: &'a CacheKey) -> BoxFuture<'a, Result<Option<String>, AgentError>>;

    /// Store `blob` for `key`. Returns each blob key that no entry holds
    /// after the write: an entry it replaced, or one it evicted. The cache
    /// deletes those blobs.
    ///
    /// # Errors
    ///
    /// Returns an error when the index cannot write.
    fn put<'a>(
        &'a self,
        key: &'a CacheKey,
        blob: String,
    ) -> BoxFuture<'a, Result<Vec<String>, AgentError>>;
}

/// A [`CacheIndex`] in process memory, with a capacity.
///
/// The index loses its entries when the process stops. Their blobs then
/// stay in the store, so give the store its own expiry. Each worker has its
/// own index, so a run on another worker misses. When the index is full, a new
/// entry evicts the oldest one.
pub struct InMemoryCacheIndex {
    capacity: usize,
    state: Mutex<IndexState>,
}

#[derive(Default)]
struct IndexState {
    entries: HashMap<CacheKey, String>,
    order: VecDeque<CacheKey>,
}

impl std::fmt::Debug for InMemoryCacheIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self.state.lock().map_or(0, |state| state.entries.len());
        f.debug_struct("InMemoryCacheIndex")
            .field("capacity", &self.capacity)
            .field("entries", &entries)
            .finish()
    }
}

impl Default for InMemoryCacheIndex {
    fn default() -> Self {
        Self::new(DEFAULT_INDEX_CAPACITY)
    }
}

impl InMemoryCacheIndex {
    /// An index that holds at most `capacity` entries. A capacity of 0
    /// holds one entry.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(IndexState::default()),
        }
    }

    /// The number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }

    /// Whether the index has no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl CacheIndex for InMemoryCacheIndex {
    fn get<'a>(&'a self, key: &'a CacheKey) -> BoxFuture<'a, Result<Option<String>, AgentError>> {
        let blob = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .get(key)
            .cloned();
        Box::pin(std::future::ready(Ok(blob)))
    }

    fn put<'a>(
        &'a self,
        key: &'a CacheKey,
        blob: String,
    ) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let IndexState { entries, order } = &mut *state;
        let mut dropped = Vec::new();
        if let Some(old) = entries.insert(key.clone(), blob.clone()) {
            // A replaced entry moves to the back, as a new one would.
            order.retain(|held| held != key);
            if old != blob {
                dropped.push(old);
            }
        }
        order.push_back(key.clone());
        while entries.len() > self.capacity {
            let Some(oldest) = order.pop_front() else {
                break;
            };
            dropped.extend(entries.remove(&oldest));
        }
        drop(state);
        Box::pin(std::future::ready(Ok(dropped)))
    }
}

/// The cross-run cache of model answers.
///
/// Clones share the store, the index and the codec registry.
///
/// ```
/// # fn demo(
/// #     model: std::sync::Arc<dyn autumn_harvest_agent::AgentModel>,
/// #     store: std::sync::Arc<dyn autumn_harvest::payload_store::PayloadStore>,
/// #     codecs: autumn_harvest::payload_codec::PayloadCodecs,
/// # ) {
/// use std::sync::Arc;
/// use std::time::Duration;
/// use autumn_harvest_agent::{AgentHarness, InMemoryCacheIndex, ResponseCache};
///
/// let cache = ResponseCache::new(store, Arc::new(InMemoryCacheIndex::default()), codecs)
///     .max_age(Duration::from_secs(24 * 3600));
/// let harness = AgentHarness::new(model).response_cache(cache);
/// # let _ = harness;
/// # }
/// ```
#[derive(Clone)]
pub struct ResponseCache {
    store: Arc<dyn PayloadStore>,
    index: Arc<dyn CacheIndex>,
    codecs: PayloadCodecs,
    scope: CacheScope,
    namespace: String,
    max_age: Option<Duration>,
}

impl std::fmt::Debug for ResponseCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseCache")
            .field("store", &self.store.store_id())
            .field("index", &self.index)
            .field("scope", &self.scope)
            .field("max_age", &self.max_age)
            .finish_non_exhaustive()
    }
}

impl ResponseCache {
    /// A cache over `store` and `index`, scoped per tenant.
    ///
    /// Pass a clone of the codec registry that the engine uses, for example
    /// `HarvestBuilder::payload_codecs().clone()` after you add the codecs.
    /// Clones share their keys, so a key rotation reaches the cache. An
    /// active codec encrypts every entry. A registry with no active codec
    /// stores entries in clear.
    #[must_use]
    pub fn new(
        store: Arc<dyn PayloadStore>,
        index: Arc<dyn CacheIndex>,
        codecs: PayloadCodecs,
    ) -> Self {
        Self {
            store,
            index,
            codecs,
            scope: CacheScope::Tenant,
            namespace: String::new(),
            max_age: None,
        }
    }

    /// Set who may share an entry. The default is [`CacheScope::Tenant`].
    #[must_use]
    pub const fn scope(mut self, scope: CacheScope) -> Self {
        self.scope = scope;
        self
    }

    /// Mix `namespace` into every key.
    ///
    /// Change it to make every old entry a miss. The old blobs stay in the
    /// store until the index drops them. Keep it secret, so that a reader of
    /// the index cannot test a guessed prompt against a key.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Treat an entry older than `max_age` as a miss.
    ///
    /// It limits what the cache serves, not what the store keeps. Give the
    /// store an expiry of at least `max_age` too.
    #[must_use]
    pub const fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// The key of `request` for `model_id`, as seen by `tenant`.
    ///
    /// The key is the SHA-256 hash of one JSON document. The document holds
    /// the namespace, the scope, the tenant and its source, the model id,
    /// the output cap, the temperature, the tools and the messages. Under
    /// [`CacheScope::Shared`] it holds no tenant.
    ///
    /// # Errors
    ///
    /// Returns a `Decode` error when the request does not serialize.
    pub fn key(
        &self,
        tenant: KeyTenant<'_>,
        model_id: &str,
        request: &ChatRequest,
    ) -> Result<CacheKey, AgentError> {
        let tenant = match self.scope {
            CacheScope::Tenant => tenant,
            CacheScope::Shared => KeyTenant::None,
        };
        let scope = match self.scope {
            CacheScope::Tenant => "tenant",
            CacheScope::Shared => "shared",
        };
        let (tenant_source, tenant) = match tenant {
            KeyTenant::None => ("none", None),
            KeyTenant::Verified(name) => ("verified", Some(name)),
            KeyTenant::Declared(name) => ("declared", Some(name)),
        };
        let material = KeyMaterial {
            v: FORMAT_VERSION,
            namespace: &self.namespace,
            scope,
            tenant_source,
            tenant,
            model: model_id,
            max_tokens: request.max_tokens,
            // The bits keep the value exact. They also keep `None` apart
            // from a NaN, which JSON writes as `null`.
            temperature_bits: request.temperature.map(f32::to_bits),
            tools: &request.tools,
            messages: &request.messages,
        };
        let bytes = serde_json::to_vec(&material)
            .map_err(|err| AgentError::new(ErrorKind::Decode, format!("cache key: {err}")))?;
        Ok(CacheKey(hex(&Sha256::digest(&bytes))))
    }

    /// The cached answer for `key`, if any.
    ///
    /// The answer keeps the token usage of the call that stored it.
    ///
    /// # Errors
    ///
    /// Returns an error when the index or the store cannot read, or the
    /// entry does not decode. The harness treats each error as a miss.
    pub async fn lookup(&self, key: &CacheKey) -> Result<Option<ChatResponse>, AgentError> {
        let Some(blob) = self.index.get(key).await? else {
            return Ok(None);
        };
        let bytes = self
            .store
            .get(&blob)
            .await
            .map_err(|err| unavailable(&err))?;
        let entry = self.decode(&bytes)?;
        if entry.v != FORMAT_VERSION || entry.key != key.as_str() {
            tracing::warn!(
                key = key.as_str(),
                "the response cache index points at an entry of another key; it is a miss"
            );
            return Ok(None);
        }
        if self.is_expired(entry.stored_at, now_millis()) {
            return Ok(None);
        }
        Ok(Some(ChatResponse {
            content: entry.content,
            stop_reason: entry.stop_reason,
            usage: entry.usage,
        }))
    }

    /// Store `response` under `key`.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry does not encode, or the store or the
    /// index cannot write.
    pub async fn store(&self, key: &CacheKey, response: &ChatResponse) -> Result<(), AgentError> {
        self.store_at(key, response, now_millis()).await
    }

    /// [`store`](Self::store) with an explicit write time.
    async fn store_at(
        &self,
        key: &CacheKey,
        response: &ChatResponse,
        stored_at: u64,
    ) -> Result<(), AgentError> {
        let entry = Entry {
            v: FORMAT_VERSION,
            key: key.as_str().to_owned(),
            stored_at,
            content: response.content.clone(),
            stop_reason: response.stop_reason,
            usage: response.usage,
        };
        let bytes = self.encode(&entry)?;
        let blob = self
            .store
            .put(&bytes)
            .await
            .map_err(|err| unavailable(&err))?;
        let dropped = match self.index.put(key, blob.clone()).await {
            Ok(dropped) => dropped,
            Err(err) => {
                // No entry holds the new blob, so the cache deletes it.
                self.delete(&blob).await;
                return Err(err);
            }
        };
        for old in dropped.iter().filter(|old| **old != blob) {
            self.delete(old).await;
        }
        Ok(())
    }

    /// The codec envelope of `entry`, as bytes.
    fn encode(&self, entry: &Entry) -> Result<Vec<u8>, AgentError> {
        let value = serde_json::to_value(entry).map_err(|_| shape_error())?;
        let encoded = self
            .codecs
            .encode_payload(&value)
            .map_err(|err| AgentError::new(ErrorKind::Config, format!("cache encode: {err}")))?;
        serde_json::to_vec(&encoded).map_err(|_| shape_error())
    }

    /// The entry inside the codec envelope `bytes`.
    ///
    /// The errors use fixed text. A serde error can quote the decoded
    /// answer, and the harness logs each error.
    fn decode(&self, bytes: &[u8]) -> Result<Entry, AgentError> {
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| shape_error())?;
        // With an active codec, a plain entry is not one this cache wrote.
        if self
            .codecs
            .active_codec_id()
            .is_some_and(|id| id != "identity")
            && !is_codec_envelope(&value)
        {
            return Err(AgentError::new(
                ErrorKind::Decode,
                "response cache entry is not a codec envelope",
            ));
        }
        let decoded = self
            .codecs
            .decode_payload(&value)
            .map_err(|err| AgentError::new(ErrorKind::Decode, format!("cache decode: {err}")))?;
        serde_json::from_value(decoded).map_err(|_| shape_error())
    }

    /// Whether an entry written at `stored_at` is out of date at `now`.
    ///
    /// The check is symmetric. A clock that runs ahead cannot write an entry
    /// that lives longer than `max_age` past its write.
    fn is_expired(&self, stored_at: u64, now: u64) -> bool {
        self.max_age.is_some_and(|max_age| {
            let max_age = u64::try_from(max_age.as_millis()).unwrap_or(u64::MAX);
            now.abs_diff(stored_at) > max_age
        })
    }

    /// Delete a blob that no entry holds. A failure leaves an orphan, so it
    /// is only a warning.
    async fn delete(&self, blob: &str) {
        if let Err(err) = self.store.delete(blob).await {
            tracing::warn!(error = %err, "the response cache could not delete an unused blob");
        }
    }
}

/// The document that [`ResponseCache::key`] hashes.
#[derive(Serialize)]
struct KeyMaterial<'a> {
    v: u32,
    namespace: &'a str,
    scope: &'static str,
    tenant_source: &'static str,
    tenant: Option<&'a str>,
    model: &'a str,
    max_tokens: Option<u32>,
    temperature_bits: Option<u32>,
    tools: &'a [ToolDefinition],
    messages: &'a [ChatMessage],
}

/// One stored answer. It repeats its key, so a read can check it. It holds
/// no prompt.
#[derive(Serialize, Deserialize)]
struct Entry {
    v: u32,
    key: String,
    /// Milliseconds since the Unix epoch.
    stored_at: u64,
    content: Vec<ContentPart>,
    stop_reason: StopReason,
    usage: TokenUsage,
}

/// Milliseconds since the Unix epoch. A clock before the epoch reads 0.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// `bytes` as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn unavailable(err: &impl std::fmt::Display) -> AgentError {
    AgentError::new(ErrorKind::Unavailable, format!("response cache: {err}"))
}

fn shape_error() -> AgentError {
    AgentError::new(
        ErrorKind::Decode,
        "response cache entry does not have the expected shape",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use autumn_harvest::aead_codec::{AeadCodec, DataKey};
    use autumn_harvest::payload_codec::is_codec_envelope;
    use autumn_harvest::payload_store::{PayloadStoreError, PayloadStoreFuture};
    use serde_json::json;

    use super::*;
    use crate::message::ToolDefinition;
    use crate::message::{ChatMessage, ChatRole, ContentPart, StopReason, TokenUsage};

    /// A [`PayloadStore`] in memory.
    #[derive(Default)]
    struct MemStore {
        blobs: Mutex<HashMap<String, Vec<u8>>>,
        next: AtomicUsize,
    }

    impl MemStore {
        fn blobs(&self) -> Vec<Vec<u8>> {
            self.blobs.lock().unwrap().values().cloned().collect()
        }

        fn overwrite(&self, key: &str, bytes: Vec<u8>) {
            self.blobs.lock().unwrap().insert(key.to_owned(), bytes);
        }
    }

    impl PayloadStore for MemStore {
        fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
            let key = format!("blob-{}", self.next.fetch_add(1, Ordering::SeqCst));
            self.blobs
                .lock()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            Box::pin(std::future::ready(Ok(key)))
        }

        fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
            let found = self.blobs.lock().unwrap().get(key).cloned();
            Box::pin(std::future::ready(
                found.ok_or_else(|| PayloadStoreError(format!("no blob {key}"))),
            ))
        }

        fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
            self.blobs.lock().unwrap().remove(key);
            Box::pin(std::future::ready(Ok(())))
        }
    }

    fn request(text: &str) -> ChatRequest {
        ChatRequest {
            messages: vec![ChatMessage::text(ChatRole::User, text)],
            tools: Vec::new(),
            max_tokens: Some(64),
            temperature: Some(0.0),
        }
    }

    fn response(text: &str) -> ChatResponse {
        ChatResponse {
            content: vec![ContentPart::Text(text.to_owned())],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::new(10, 5),
        }
    }

    fn cache_over(store: &Arc<MemStore>, index: Arc<dyn CacheIndex>) -> ResponseCache {
        let store: Arc<dyn PayloadStore> = Arc::clone(store) as _;
        ResponseCache::new(store, index, PayloadCodecs::default())
    }

    fn cache() -> (ResponseCache, Arc<MemStore>) {
        let store = Arc::new(MemStore::default());
        (
            cache_over(&store, Arc::new(InMemoryCacheIndex::default())),
            store,
        )
    }

    #[test]
    fn identical_requests_share_a_key() {
        let (cache, _) = cache();
        let one = cache
            .key(KeyTenant::Declared("acme"), "m-1", &request("hi"))
            .unwrap();
        let two = cache
            .key(KeyTenant::Declared("acme"), "m-1", &request("hi"))
            .unwrap();
        assert_eq!(one, two);
        assert_eq!(one.as_str().len(), 64);
        assert!(one.as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn each_part_of_the_request_changes_the_key() {
        let (cache, _) = cache();
        let base = cache.key(KeyTenant::None, "m-1", &request("hi")).unwrap();
        let mut tools = request("hi");
        tools.tools.push(ToolDefinition {
            name: "t".into(),
            description: "d".into(),
            input_schema: json!({}),
        });
        let mut cap = request("hi");
        cap.max_tokens = Some(65);
        let mut warm = request("hi");
        warm.temperature = Some(0.5);
        let mut unset = request("hi");
        unset.temperature = None;
        let others = [
            cache.key(KeyTenant::None, "m-2", &request("hi")).unwrap(),
            cache.key(KeyTenant::None, "m-1", &request("ho")).unwrap(),
            cache.key(KeyTenant::None, "m-1", &tools).unwrap(),
            cache.key(KeyTenant::None, "m-1", &cap).unwrap(),
            cache.key(KeyTenant::None, "m-1", &warm).unwrap(),
            cache.key(KeyTenant::None, "m-1", &unset).unwrap(),
            cache
                .namespace("v2")
                .key(KeyTenant::None, "m-1", &request("hi"))
                .unwrap(),
        ];
        for other in &others {
            assert_ne!(&base, other);
        }
    }

    #[test]
    fn the_default_scope_separates_tenants() {
        let (cache, _) = cache();
        let a = cache
            .key(KeyTenant::Declared("a"), "m", &request("hi"))
            .unwrap();
        let b = cache
            .key(KeyTenant::Declared("b"), "m", &request("hi"))
            .unwrap();
        let none = cache.key(KeyTenant::None, "m", &request("hi")).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, none);
        assert_ne!(b, none);
    }

    #[test]
    fn a_declared_tenant_never_shares_a_verified_key() {
        let (cache, _) = cache();
        let verified = cache
            .key(KeyTenant::Verified("acme"), "m", &request("hi"))
            .unwrap();
        let declared = cache
            .key(KeyTenant::Declared("acme"), "m", &request("hi"))
            .unwrap();
        assert_ne!(verified, declared);
        let shared = cache.scope(CacheScope::Shared);
        assert_eq!(
            shared
                .key(KeyTenant::Verified("acme"), "m", &request("hi"))
                .unwrap(),
            shared
                .key(KeyTenant::Declared("b"), "m", &request("hi"))
                .unwrap(),
        );
    }

    #[test]
    fn the_shared_scope_ignores_the_tenant() {
        let (cache, _) = cache();
        let shared = cache.clone().scope(CacheScope::Shared);
        let a = shared
            .key(KeyTenant::Declared("a"), "m", &request("hi"))
            .unwrap();
        assert_eq!(
            a,
            shared
                .key(KeyTenant::Declared("b"), "m", &request("hi"))
                .unwrap()
        );
        assert_eq!(a, shared.key(KeyTenant::None, "m", &request("hi")).unwrap());
        assert_ne!(a, cache.key(KeyTenant::None, "m", &request("hi")).unwrap());
    }

    #[tokio::test]
    async fn a_stored_answer_comes_back() {
        let (cache, _) = cache();
        let key = cache
            .key(KeyTenant::Declared("a"), "m", &request("hi"))
            .unwrap();
        assert_eq!(cache.lookup(&key).await.unwrap(), None);
        cache.store(&key, &response("hello")).await.unwrap();
        assert_eq!(cache.lookup(&key).await.unwrap(), Some(response("hello")));
    }

    #[tokio::test]
    async fn an_entry_under_another_key_is_a_miss() {
        let store = Arc::new(MemStore::default());
        let index = Arc::new(InMemoryCacheIndex::default());
        let cache = cache_over(&store, Arc::clone(&index) as _);
        let a = cache
            .key(KeyTenant::Declared("a"), "m", &request("hi"))
            .unwrap();
        let b = cache
            .key(KeyTenant::Declared("b"), "m", &request("hi"))
            .unwrap();
        cache.store(&a, &response("for a")).await.unwrap();
        // A faulty index points b at the blob of a.
        let blob = index.get(&a).await.unwrap().unwrap();
        index.put(&b, blob).await.unwrap();
        assert_eq!(cache.lookup(&b).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_entry_older_than_max_age_is_a_miss() {
        let (cache, _) = cache();
        let cache = cache.max_age(Duration::from_secs(60));
        let key = cache.key(KeyTenant::None, "m", &request("hi")).unwrap();
        let old = now_millis() - 61_000;
        cache.store_at(&key, &response("old"), old).await.unwrap();
        assert_eq!(cache.lookup(&key).await.unwrap(), None);
        let ahead = now_millis() + 61_000;
        cache
            .store_at(&key, &response("ahead"), ahead)
            .await
            .unwrap();
        assert_eq!(cache.lookup(&key).await.unwrap(), None);
        let fresh = now_millis() - 59_000;
        cache
            .store_at(&key, &response("fresh"), fresh)
            .await
            .unwrap();
        assert_eq!(cache.lookup(&key).await.unwrap(), Some(response("fresh")));
    }

    #[tokio::test]
    async fn an_active_codec_encrypts_the_entry() {
        let codecs = PayloadCodecs::default();
        AeadCodec::new("k1", &DataKey::generate())
            .unwrap()
            .register_with(&codecs)
            .unwrap();
        codecs.set_active_key("k1").unwrap();
        let store = Arc::new(MemStore::default());
        let cache = ResponseCache::new(
            Arc::clone(&store) as _,
            Arc::new(InMemoryCacheIndex::default()),
            codecs,
        );
        let key = cache
            .key(KeyTenant::Declared("a"), "m", &request("hi"))
            .unwrap();
        cache
            .store(&key, &response("the secret answer"))
            .await
            .unwrap();

        let blobs = store.blobs();
        assert_eq!(blobs.len(), 1);
        let raw = String::from_utf8_lossy(&blobs[0]);
        assert!(!raw.contains("the secret answer"), "{raw}");
        let stored: serde_json::Value = serde_json::from_slice(&blobs[0]).unwrap();
        assert!(is_codec_envelope(&stored), "{stored}");
        assert_eq!(
            cache.lookup(&key).await.unwrap(),
            Some(response("the secret answer"))
        );
    }

    #[tokio::test]
    async fn an_entry_without_its_codec_key_does_not_decode() {
        let codecs = PayloadCodecs::default();
        AeadCodec::new("k1", &DataKey::generate())
            .unwrap()
            .register_with(&codecs)
            .unwrap();
        codecs.set_active_key("k1").unwrap();
        let store = Arc::new(MemStore::default());
        let index: Arc<dyn CacheIndex> = Arc::new(InMemoryCacheIndex::default());
        let writer = ResponseCache::new(Arc::clone(&store) as _, Arc::clone(&index), codecs);
        let key = writer.key(KeyTenant::None, "m", &request("hi")).unwrap();
        writer.store(&key, &response("hello")).await.unwrap();

        let reader = cache_over(&store, index);
        assert!(reader.lookup(&key).await.is_err());
    }

    #[tokio::test]
    async fn an_active_codec_refuses_a_plain_entry() {
        let store = Arc::new(MemStore::default());
        let index: Arc<dyn CacheIndex> = Arc::new(InMemoryCacheIndex::default());
        let plain = cache_over(&store, Arc::clone(&index));
        let key = plain.key(KeyTenant::None, "m", &request("hi")).unwrap();
        plain.store(&key, &response("planted")).await.unwrap();

        let codecs = PayloadCodecs::default();
        AeadCodec::new("k1", &DataKey::generate())
            .unwrap()
            .register_with(&codecs)
            .unwrap();
        codecs.set_active_key("k1").unwrap();
        let sealed = ResponseCache::new(Arc::clone(&store) as _, index, codecs);
        assert!(sealed.lookup(&key).await.is_err());
    }

    #[tokio::test]
    async fn a_bad_entry_error_quotes_no_content() {
        let store = Arc::new(MemStore::default());
        let index = Arc::new(InMemoryCacheIndex::default());
        let cache = cache_over(&store, Arc::clone(&index) as _);
        let key = cache.key(KeyTenant::None, "m", &request("hi")).unwrap();
        cache.store(&key, &response("hello")).await.unwrap();
        let blob = index.get(&key).await.unwrap().unwrap();
        let bad = json!({"v": 1, "key": key.as_str(), "content": "the secret"});
        store.overwrite(&blob, serde_json::to_vec(&bad).unwrap());
        let err = cache.lookup(&key).await.unwrap_err();
        assert!(!err.to_string().contains("the secret"), "{err}");
    }

    #[tokio::test]
    async fn a_corrupt_blob_does_not_decode() {
        let store = Arc::new(MemStore::default());
        let index = Arc::new(InMemoryCacheIndex::default());
        let cache = cache_over(&store, Arc::clone(&index) as _);
        let key = cache.key(KeyTenant::None, "m", &request("hi")).unwrap();
        cache.store(&key, &response("hello")).await.unwrap();
        let blob = index.get(&key).await.unwrap().unwrap();
        store.overwrite(&blob, b"not json".to_vec());
        assert!(cache.lookup(&key).await.is_err());
    }

    #[tokio::test]
    async fn the_index_evicts_the_oldest_entry() {
        let index = InMemoryCacheIndex::new(2);
        let key = |n: u8| CacheKey(format!("{n:064x}"));
        let none: [String; 0] = [];
        assert_eq!(index.put(&key(1), "b1".into()).await.unwrap(), none);
        assert_eq!(index.put(&key(2), "b2".into()).await.unwrap(), none);
        assert_eq!(index.put(&key(3), "b3".into()).await.unwrap(), ["b1"]);
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(&key(1)).await.unwrap(), None);
        assert_eq!(index.get(&key(3)).await.unwrap(), Some("b3".into()));
        // A replaced entry gives back its old blob and keeps the count.
        assert_eq!(index.put(&key(3), "b4".into()).await.unwrap(), ["b3"]);
        assert_eq!(index.len(), 2);
        assert_eq!(index.put(&key(4), "b5".into()).await.unwrap(), ["b2"]);
    }

    #[tokio::test]
    async fn the_cache_deletes_the_blobs_the_index_drops() {
        let store = Arc::new(MemStore::default());
        let cache = cache_over(&store, Arc::new(InMemoryCacheIndex::new(1)));
        let one = cache.key(KeyTenant::None, "m", &request("one")).unwrap();
        let two = cache.key(KeyTenant::None, "m", &request("two")).unwrap();
        cache.store(&one, &response("1")).await.unwrap();
        cache.store(&one, &response("1 again")).await.unwrap();
        cache.store(&two, &response("2")).await.unwrap();
        assert_eq!(store.blobs().len(), 1);
        assert_eq!(cache.lookup(&one).await.unwrap(), None);
        assert_eq!(cache.lookup(&two).await.unwrap(), Some(response("2")));
    }
}
