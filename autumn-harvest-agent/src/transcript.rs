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

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::message::ChatMessage;

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

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Where the model-turn activity keeps the windows that later turns name.
///
/// A store may drop any window at any time. A shared store, such as one in
/// Redis, lets a delta hit on every worker. A read checks the digest of the
/// window, so a changed window is a miss.
///
/// A store holds transcript text outside history and outside the payload
/// codecs. Erasure does not reach it, so the app clears its own store.
pub trait TranscriptStore: Send + Sync + std::fmt::Debug {
    /// The window with the digest `digest`, if the store holds it.
    fn get(&self, digest: &str) -> Option<Vec<ChatMessage>>;

    /// Keep `window` under `digest`.
    fn put(&self, digest: &str, window: &[ChatMessage]);
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

#[derive(Debug, Default)]
struct Windows {
    by_digest: HashMap<String, (Vec<ChatMessage>, usize)>,
    order: VecDeque<String>,
    bytes: usize,
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
}

impl TranscriptStore for InMemoryTranscriptStore {
    fn get(&self, digest: &str) -> Option<Vec<ChatMessage>> {
        self.windows()
            .by_digest
            .get(digest)
            .map(|(window, _)| window.clone())
    }

    fn put(&self, digest: &str, window: &[ChatMessage]) {
        let size = usize::try_from(crate::bounds::json_len(&window)).unwrap_or(usize::MAX);
        if size > self.max_bytes {
            return;
        }
        let mut windows = self.windows();
        if windows.by_digest.contains_key(digest) {
            return;
        }
        while windows.bytes + size > self.max_bytes {
            let Some(oldest) = windows.order.pop_front() else {
                break;
            };
            if let Some((_, freed)) = windows.by_digest.remove(&oldest) {
                windows.bytes -= freed;
            }
        }
        windows.bytes += size;
        windows.order.push_back(digest.to_owned());
        windows
            .by_digest
            .insert(digest.to_owned(), (window.to_vec(), size));
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

    #[test]
    fn a_window_over_the_bound_is_not_kept() {
        let store = InMemoryTranscriptStore::with_max_bytes(10);
        store.put("big", &messages(3));
        assert!(store.get("big").is_none());
    }
}
