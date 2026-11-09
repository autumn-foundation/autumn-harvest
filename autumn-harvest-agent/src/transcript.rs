//! Record only the messages that each model turn adds (issue #2264).
//!
//! Each model turn used to record the whole transcript in its activity
//! input, so a run's history grew with the square of its turn count. A turn
//! now records a [`PrefixRef`] and the messages after that prefix. The
//! model-turn activity rebuilds the window from a [`TranscriptStore`]. The
//! window that reaches the model is the same, so the response cache key and
//! provider prompt caching do not change.
//!
//! A store holds windows outside history. On a miss, such as on another
//! worker or after a restart, the turn fails with [`PREFIX_MISSING`] before
//! any model call. The workflow then sends the turn again in the full form.
//! So a store can lose any window, and the run stays correct.
//!
//! The digest of a window is a chain over its messages. The digest of a
//! longer window follows from a shorter one and the added messages, so the
//! workflow hashes each message once.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::AgentError;
use crate::message::{ChatMessage, SessionId};
use crate::model::BoxFuture;

/// The activity error type of a turn whose prefix the store does not hold.
pub const PREFIX_MISSING: &str = "TranscriptPrefixMissing";

/// The patch marker of the delta form. A run recorded before it sends the
/// full form.
pub(crate) const TRANSCRIPT_DELTA_PATCH: &str = "agent.transcript-delta";

/// The default byte bound of [`InMemoryTranscriptStore`]: 64 MiB.
pub const DEFAULT_STORE_BYTES: usize = 64 * 1024 * 1024;

/// The start of every digest chain.
const SEED: &[u8] = b"autumn-harvest.transcript.v1";

/// A reference to the first messages of a window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixRef {
    /// The chained digest of the prefix, in hex.
    pub digest: String,
    /// The number of messages in the prefix.
    pub len: u32,
}

/// The digest of `added` after the window whose digest is `prev`. `None`
/// starts a new chain.
#[must_use]
pub fn chain(prev: Option<&str>, added: &[ChatMessage]) -> String {
    let mut digest = prev.map_or_else(|| hex(&Sha256::digest(SEED)), str::to_owned);
    for message in added {
        // A message holds strings, numbers and maps only, so it encodes.
        let bytes = serde_json::to_vec(message).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(digest.as_bytes());
        hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(&bytes);
        digest = hex(&hasher.finalize());
    }
    digest
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// The default number of windows that a durable store keeps for one run.
/// A later turn names the newest one. A retried turn names the one before.
pub const DEFAULT_KEEP_PER_RUN: u32 = 4;

/// The default age limit of a window in a durable store: 7 days.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_hours(7 * 24);

/// The run and the session of the turn that keeps a window.
///
/// An erase of the session deletes the window. A durable agent sets the
/// session of each turn to its agent id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WindowOwner<'a> {
    /// The run id of the turn.
    pub run: &'a str,
    /// The session of the turn, if it has one.
    pub session: Option<&'a SessionId>,
}

impl<'a> WindowOwner<'a> {
    /// The owner of a window of the run `run` in `session`.
    #[must_use]
    pub const fn new(run: &'a str, session: Option<&'a SessionId>) -> Self {
        Self { run, session }
    }
}

/// Where the model-turn activity keeps the windows that later turns name.
///
/// A store may drop any window at any time. A shared store lets a delta hit
/// on every worker. A read checks the digest of the window, so a changed
/// window is a miss.
///
/// A store holds transcript text outside history and outside the payload
/// codecs. The reset of a durable agent calls [`erase`](Self::erase) with
/// the agent id, so `harvest agents erase` reaches each window of the
/// agent. The crate ships [`InMemoryTranscriptStore`] and the durable
/// stores of [`stores`](crate::stores).
pub trait TranscriptStore: Send + Sync + std::fmt::Debug {
    /// The window with the digest `digest`, if the store holds it. A store
    /// that cannot read returns `None`, and the turn goes in the full form.
    fn get<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Option<Vec<ChatMessage>>>;

    /// Keep `window` under `digest` for `owner`. A store that cannot write
    /// keeps nothing, and a later turn misses.
    fn put<'a>(
        &'a self,
        owner: WindowOwner<'a>,
        digest: &'a str,
        window: &'a [ChatMessage],
    ) -> BoxFuture<'a, ()>;

    /// Delete every window that `session` keeps, and count them.
    ///
    /// The default returns `None`: the store cannot erase by session, and
    /// the app must clear it itself. The erase report then shows no count.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot delete.
    fn erase<'a>(
        &'a self,
        session: &'a SessionId,
    ) -> BoxFuture<'a, Result<Option<u64>, AgentError>> {
        let _ = session;
        Box::pin(std::future::ready(Ok(None)))
    }
}

/// An in-process [`TranscriptStore`] with a byte bound.
///
/// It drops the oldest window first. A restart empties it, so the next turn
/// of each run sends the full form once.
#[derive(Debug)]
pub struct InMemoryTranscriptStore {
    max_bytes: usize,
    inner: Mutex<Windows>,
}

/// One held window and the sessions that keep it.
#[derive(Debug)]
struct Held {
    window: Vec<ChatMessage>,
    size: usize,
    /// `None` stands for a turn with no session.
    owners: BTreeSet<Option<SessionId>>,
}

#[derive(Debug, Default)]
struct Windows {
    by_digest: HashMap<String, Held>,
    order: VecDeque<String>,
    bytes: usize,
}

impl Windows {
    fn remove(&mut self, digest: &str) {
        if let Some(held) = self.by_digest.remove(digest) {
            self.bytes -= held.size;
        }
    }
}

impl Default for InMemoryTranscriptStore {
    fn default() -> Self {
        Self::with_max_bytes(DEFAULT_STORE_BYTES)
    }
}

impl InMemoryTranscriptStore {
    /// A store that keeps at most `max_bytes` of encoded messages.
    #[must_use]
    pub fn with_max_bytes(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(Windows::default()),
        }
    }

    fn windows(&self) -> std::sync::MutexGuard<'_, Windows> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn put_now(&self, owner: WindowOwner<'_>, digest: &str, window: &[ChatMessage]) {
        let size = usize::try_from(crate::bounds::json_len(&window)).unwrap_or(usize::MAX);
        if size > self.max_bytes {
            return;
        }
        let mut windows = self.windows();
        if let Some(held) = windows.by_digest.get_mut(digest) {
            held.owners.insert(owner.session.cloned());
            return;
        }
        while windows.bytes + size > self.max_bytes {
            let Some(oldest) = windows.order.pop_front() else {
                break;
            };
            windows.remove(&oldest);
        }
        windows.bytes += size;
        windows.order.push_back(digest.to_owned());
        windows.by_digest.insert(
            digest.to_owned(),
            Held {
                window: window.to_vec(),
                size,
                owners: BTreeSet::from([owner.session.cloned()]),
            },
        );
    }

    fn erase_now(&self, session: &SessionId) -> u64 {
        let mut windows = self.windows();
        let key = Some(session.clone());
        let mut erased = 0_u64;
        let mut orphans = Vec::new();
        for (digest, held) in &mut windows.by_digest {
            if held.owners.remove(&key) {
                erased += 1;
                if held.owners.is_empty() {
                    orphans.push(digest.clone());
                }
            }
        }
        for digest in &orphans {
            windows.remove(digest);
        }
        windows
            .order
            .retain(|digest| !orphans.iter().any(|orphan| orphan == digest));
        erased
    }
}

impl TranscriptStore for InMemoryTranscriptStore {
    fn get<'a>(&'a self, digest: &'a str) -> BoxFuture<'a, Option<Vec<ChatMessage>>> {
        let window = self
            .windows()
            .by_digest
            .get(digest)
            .map(|held| held.window.clone());
        Box::pin(std::future::ready(window))
    }

    fn put<'a>(
        &'a self,
        owner: WindowOwner<'a>,
        digest: &'a str,
        window: &'a [ChatMessage],
    ) -> BoxFuture<'a, ()> {
        self.put_now(owner, digest, window);
        Box::pin(std::future::ready(()))
    }

    fn erase<'a>(
        &'a self,
        session: &'a SessionId,
    ) -> BoxFuture<'a, Result<Option<u64>, AgentError>> {
        let erased = self.erase_now(session);
        Box::pin(std::future::ready(Ok(Some(erased))))
    }
}

/// The last window that a run sent, so the next turn can name it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Sent {
    /// The chained digest of the window.
    pub digest: String,
    /// The number of messages in the window.
    pub len: usize,
}

impl Sent {
    /// The sent window of `window`.
    #[must_use]
    pub fn of(window: &[ChatMessage]) -> Self {
        Self {
            digest: chain(None, window),
            len: window.len(),
        }
    }

    /// Whether `window` still starts with this sent window. It hashes the
    /// prefix, so call it once per segment, not once per turn.
    #[must_use]
    pub fn is_prefix_of(&self, window: &[ChatMessage]) -> bool {
        window.len() >= self.len && chain(None, &window[..self.len]) == self.digest
    }

    /// The sent window after the turn that sent `window`. Within a segment
    /// a window only grows, so this hashes the added messages only.
    #[must_use]
    pub fn after(&self, window: &[ChatMessage]) -> Self {
        Self {
            digest: chain(Some(&self.digest), &window[self.len..]),
            len: window.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::message::ChatRole;

    fn messages(n: usize) -> Vec<ChatMessage> {
        (0..n)
            .map(|i| ChatMessage::text(ChatRole::User, format!("message {i}")))
            .collect()
    }

    #[test]
    fn the_chain_composes() {
        let all = messages(5);
        let head = chain(None, &all[..2]);
        assert_eq!(chain(Some(&head), &all[2..]), chain(None, &all));
        assert_ne!(chain(None, &all[..4]), chain(None, &all));
    }

    #[test]
    fn a_sent_window_follows_the_growth_of_a_segment() {
        let all = messages(6);
        let sent = Sent::of(&all[..3]);
        assert!(sent.is_prefix_of(&all));
        assert_eq!(sent.after(&all), Sent::of(&all));
        let mut changed = all.clone();
        changed[1] = ChatMessage::text(ChatRole::User, "edited");
        assert!(
            !sent.is_prefix_of(&changed),
            "an edited prefix is not a prefix"
        );
        assert!(
            !Sent::of(&all).is_prefix_of(&all[..2]),
            "a shorter window is not"
        );
    }

    #[tokio::test]
    async fn a_window_over_the_bound_is_not_kept() {
        let store = InMemoryTranscriptStore::with_max_bytes(10);
        store
            .put(WindowOwner::new("r", None), "big", &messages(3))
            .await;
        assert!(store.get("big").await.is_none());
    }

    #[tokio::test]
    async fn an_erase_removes_only_the_windows_of_its_session() {
        let store = InMemoryTranscriptStore::default();
        let (gone, kept) = (SessionId::new("a/1"), SessionId::new("a/2"));
        let window = messages(2);
        store
            .put(WindowOwner::new("r1", Some(&gone)), "own", &window)
            .await;
        store
            .put(WindowOwner::new("r1", Some(&gone)), "shared", &window)
            .await;
        store
            .put(WindowOwner::new("r2", Some(&kept)), "shared", &window)
            .await;
        assert_eq!(store.erase(&gone).await.unwrap(), Some(2));
        assert!(store.get("own").await.is_none());
        assert_eq!(store.get("shared").await, Some(window));
        assert_eq!(store.erase(&gone).await.unwrap(), Some(0));
        assert_eq!(store.erase(&kept).await.unwrap(), Some(1));
        assert!(store.get("shared").await.is_none(), "no session holds it");
    }

    #[tokio::test]
    async fn an_erase_frees_the_bytes_of_the_window() {
        let window = messages(2);
        let size = usize::try_from(crate::bounds::json_len(&window)).unwrap_or(usize::MAX);
        let store = InMemoryTranscriptStore::with_max_bytes(size);
        let session = SessionId::new("a/1");
        store
            .put(WindowOwner::new("r", Some(&session)), "one", &window)
            .await;
        store.erase(&session).await.unwrap();
        store.put(WindowOwner::new("r", None), "two", &window).await;
        assert_eq!(store.windows().bytes, size, "the erase freed its bytes");
    }
}
