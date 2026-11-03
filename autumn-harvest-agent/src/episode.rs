//! The episode log of a durable agent (issue #2454).
//!
//! An episode records one op that ended. It holds the run id, the answer
//! cut to a bound, the stop, the recorded time and the flow labels of the
//! agent. The log lives in the scope `agent:<id>:episodes` of an
//! [`EpisodeStore`].
//!
//! The setting is off by default. An operator turns it on for a kind with
//! [`AgentProfile::episodes`](crate::AgentProfile::episodes). The workflow
//! [`agent_consolidate`](crate::consolidate::agent_consolidate) turns the
//! unconsolidated episodes into facts in the memory of the agent.
//!
//! Each append carries an [`EditKey`], and a store applies a key once. A
//! retried activity therefore appends once. [`EpisodeLog`] holds the rules,
//! so every store applies them the same way.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use crate::error::AgentError;
use crate::memory::{EditKey, MemoryScope};
use crate::model::BoxFuture;
use crate::types::AgentStop;

/// The default bound on the episodes of one agent.
pub const DEFAULT_MAX_EPISODES: u32 = 200;

/// The default bound on the text of one episode, in bytes.
pub const DEFAULT_MAX_TEXT_BYTES: u32 = 1_000;

/// The default bound on the episodes that one pass reads.
pub const DEFAULT_MAX_PASS_EPISODES: u32 = 20;

/// The default memory block of the facts.
pub const DEFAULT_FACTS_BLOCK: &str = "memory";

/// The most append keys and pass keys that one log keeps. A retry comes
/// soon after its first attempt, so an old key is safe to forget.
pub const MAX_KEPT_KEYS: usize = 4_096;

/// The most derived fact entries that one log keeps.
pub const MAX_DERIVED_FACTS: usize = 1_024;

/// One op that ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Episode {
    /// The id: the run id of the segment, `<incarnation>:s<n>`.
    pub id: String,
    /// The answer of the op, cut to the bound of the kind.
    pub text: String,
    /// How the op ended.
    pub stop: AgentStop,
    /// The recorded time of the op end, in epoch milliseconds.
    pub at_ms: i64,
    /// The flow labels of the agent at the op end.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub labels: BTreeSet<String>,
}

/// When a consolidation pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ConsolidationTrigger {
    /// An engine schedule starts each pass. The agent starts none. See
    /// [`consolidate::schedule`](crate::consolidate::schedule).
    #[default]
    Schedule,
    /// The agent runs a pass as a child workflow when an append leaves this
    /// many unconsolidated episodes or more. The op waits for the pass.
    Every {
        /// The number of unconsolidated episodes. 0 never fires.
        episodes: u32,
    },
}

/// The episode setting of one kind of agent.
///
/// It needs `AgentMemory::Store`, because the facts go into the memory
/// scope of the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EpisodeSettings {
    /// The most episodes that the log keeps. The oldest goes first. 0
    /// counts as 1.
    pub max_episodes: u32,
    /// The bound on the text of one episode, in bytes.
    pub max_text_bytes: u32,
    /// When a pass runs.
    pub trigger: ConsolidationTrigger,
    /// The harness model of a pass. `None` takes the profile model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The memory block that gets the facts.
    pub facts_block: String,
    /// The most episodes that one pass reads, oldest first. 0 counts as 1.
    pub max_pass_episodes: u32,
}

impl Default for EpisodeSettings {
    fn default() -> Self {
        Self {
            max_episodes: DEFAULT_MAX_EPISODES,
            max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
            trigger: ConsolidationTrigger::Schedule,
            model: None,
            facts_block: DEFAULT_FACTS_BLOCK.to_owned(),
            max_pass_episodes: DEFAULT_MAX_PASS_EPISODES,
        }
    }
}

impl EpisodeSettings {
    /// Keep at most `n` episodes.
    #[must_use]
    pub const fn max_episodes(mut self, n: u32) -> Self {
        self.max_episodes = n;
        self
    }

    /// Run a pass after every `n` unconsolidated episodes.
    #[must_use]
    pub const fn every(mut self, n: u32) -> Self {
        self.trigger = ConsolidationTrigger::Every { episodes: n };
        self
    }

    /// Run each pass on the harness model `name`.
    #[must_use]
    pub fn model(mut self, name: impl Into<String>) -> Self {
        self.model = Some(name.into());
        self
    }

    /// Write the facts into the memory block `label`.
    #[must_use]
    pub fn facts_block(mut self, label: impl Into<String>) -> Self {
        self.facts_block = label.into();
        self
    }

    /// Cut the text of each episode to `bytes`.
    #[must_use]
    pub const fn max_text_bytes(mut self, bytes: u32) -> Self {
        self.max_text_bytes = bytes;
        self
    }

    /// Read at most `n` episodes in one pass.
    #[must_use]
    pub const fn max_pass_episodes(mut self, n: u32) -> Self {
        self.max_pass_episodes = n;
        self
    }
}

/// One episode in the log, with its consolidation flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEpisode {
    /// The episode.
    #[serde(flatten)]
    pub episode: Episode,
    /// Whether a pass turned it into facts.
    #[serde(default)]
    pub consolidated: bool,
}

/// What one append did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Appended {
    /// The unconsolidated episodes after the append.
    pub pending: u64,
    /// The episodes that the append evicted. A retry evicts none.
    pub evicted: u64,
}

/// What one pass reports to the log.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PassMark {
    /// The key of the pass. A log applies a key once.
    pub key: EditKey,
    /// The ids of the episodes that the pass read.
    pub episodes: Vec<String>,
    /// The `dropped` count that the pass read.
    pub dropped: u64,
    /// The fact entries that the pass wrote.
    pub facts: Vec<String>,
}

/// What one clear removed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Cleared {
    /// The episodes removed, consolidated ones included.
    pub episodes: u64,
    /// The fact entries that passes derived from them.
    pub derived: Vec<String>,
}

/// The counts of one episode reset (issue #2454).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EpisodeReset {
    /// The episodes removed.
    pub episodes: u64,
    /// The derived facts that the memory held. The memory reset removes
    /// them.
    pub facts: u64,
}

/// The episode log of one scope, and the rules of each change.
///
/// A store keeps one log per scope. It calls these methods so that the
/// rules are the same in every store. The log serialises, so a store can
/// keep it as one value.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EpisodeLog {
    /// The episodes, oldest first.
    episodes: VecDeque<StoredEpisode>,
    /// Unconsolidated episodes that an eviction removed since the last
    /// pass read the count.
    pub dropped: u64,
    /// The fact entries that passes wrote, oldest first.
    derived: VecDeque<String>,
    /// The applied keys, oldest first.
    keys: VecDeque<String>,
}

impl EpisodeLog {
    /// The number of episodes, consolidated ones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.episodes.len()
    }

    /// Whether the log holds no episode.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.episodes.is_empty()
    }

    /// Every episode, oldest first.
    pub fn episodes(&self) -> impl Iterator<Item = &Episode> {
        self.episodes.iter().map(|stored| &stored.episode)
    }

    /// Every episode with its flag, oldest first.
    pub fn stored(&self) -> impl Iterator<Item = &StoredEpisode> {
        self.episodes.iter()
    }

    /// The fact entries that passes wrote.
    pub fn derived(&self) -> impl Iterator<Item = &str> {
        self.derived.iter().map(String::as_str)
    }

    /// The oldest `limit` unconsolidated episodes. A `limit` of 0 counts
    /// as 1.
    #[must_use]
    pub fn pending(&self, limit: usize) -> Vec<Episode> {
        self.episodes
            .iter()
            .filter(|stored| !stored.consolidated)
            .take(limit.max(1))
            .map(|stored| stored.episode.clone())
            .collect()
    }

    /// The unconsolidated episodes.
    #[must_use]
    pub fn pending_len(&self) -> u64 {
        self.episodes
            .iter()
            .filter(|stored| !stored.consolidated)
            .count() as u64
    }

    /// Append `episode` once for `key`, and evict the oldest episodes past
    /// `max_episodes`.
    ///
    /// An evicted unconsolidated episode adds one to [`Self::dropped`]. A
    /// key that the log applied already changes nothing.
    pub fn append(&mut self, key: &EditKey, episode: Episode, max_episodes: u32) -> Appended {
        if self.applied(key) {
            return Appended {
                pending: self.pending_len(),
                evicted: 0,
            };
        }
        let len = self.episodes.len() as u64;
        let evicted = crate::bounds::episode_evictions(len, u64::from(max_episodes));
        for _ in 0..evicted {
            if let Some(old) = self.episodes.pop_front()
                && !old.consolidated
            {
                self.dropped = self.dropped.saturating_add(1);
            }
        }
        self.episodes.push_back(StoredEpisode {
            episode,
            consolidated: false,
        });
        self.remember(key);
        Appended {
            pending: self.pending_len(),
            evicted,
        }
    }

    /// Apply one pass once for its key.
    ///
    /// The episodes that the pass read are consolidated. The count that it
    /// read leaves [`Self::dropped`], so an eviction during the pass stays
    /// counted. Its facts join the derived entries.
    pub fn mark(&mut self, mark: &PassMark) {
        if self.applied(&mark.key) {
            return;
        }
        let read: BTreeSet<&str> = mark.episodes.iter().map(String::as_str).collect();
        for stored in &mut self.episodes {
            if read.contains(stored.episode.id.as_str()) {
                stored.consolidated = true;
            }
        }
        self.dropped = self.dropped.saturating_sub(mark.dropped);
        for fact in &mark.facts {
            if !self.derived.contains(fact) {
                self.derived.push_back(fact.clone());
            }
        }
        while self.derived.len() > MAX_DERIVED_FACTS {
            self.derived.pop_front();
        }
        self.remember(&mark.key);
    }

    /// What a clear of this log removes.
    #[must_use]
    pub fn into_cleared(self) -> Cleared {
        Cleared {
            episodes: self.episodes.len() as u64,
            derived: self.derived.into(),
        }
    }

    fn applied(&self, key: &EditKey) -> bool {
        self.keys.iter().any(|kept| kept == key.as_str())
    }

    fn remember(&mut self, key: &EditKey) {
        self.keys.push_back(key.as_str().to_owned());
        while self.keys.len() > MAX_KEPT_KEYS {
            self.keys.pop_front();
        }
    }
}

/// Storage for episode logs.
///
/// Call the methods of [`EpisodeLog`] so that the rules are the same in
/// every store. Each method is one transaction.
pub trait EpisodeStore: Send + Sync + std::fmt::Debug {
    /// The log of `scope`. A new scope has an empty log.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot read.
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>>;

    /// Append one episode with [`EpisodeLog::append`].
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot write.
    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>>;

    /// Apply one pass with [`EpisodeLog::mark`].
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot write.
    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<(), AgentError>>;

    /// Remove the log of `scope`, and return what it held.
    ///
    /// A retry with the same `key` returns the first result, so a retried
    /// reset reports the same counts.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot write.
    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>>;
}

/// An [`EpisodeStore`] in process memory. Its contents go when the process
/// stops, so use it for tests and demos only.
///
/// Its `Debug` output shows only the number of scopes, never an episode.
#[derive(Default)]
pub struct InMemoryEpisodeStore {
    state: Mutex<EpisodeState>,
}

#[derive(Default)]
struct EpisodeState {
    logs: HashMap<MemoryScope, EpisodeLog>,
    /// The last clear of each scope, with its key.
    cleared: HashMap<MemoryScope, (EditKey, Cleared)>,
}

impl std::fmt::Debug for InMemoryEpisodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scopes = self.state.lock().map_or(0, |state| state.logs.len());
        f.debug_struct("InMemoryEpisodeStore")
            .field("scopes", &scopes)
            .finish_non_exhaustive()
    }
}

impl InMemoryEpisodeStore {
    fn with_state<T>(&self, f: impl FnOnce(&mut EpisodeState) -> T) -> T {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut state)
    }
}

impl EpisodeStore for InMemoryEpisodeStore {
    fn load<'a>(&'a self, scope: &'a MemoryScope) -> BoxFuture<'a, Result<EpisodeLog, AgentError>> {
        let log = self.with_state(|state| state.logs.get(scope).cloned().unwrap_or_default());
        Box::pin(std::future::ready(Ok(log)))
    }

    fn append<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
        episode: Episode,
        max_episodes: u32,
    ) -> BoxFuture<'a, Result<Appended, AgentError>> {
        let appended = self.with_state(|state| {
            state
                .logs
                .entry(scope.clone())
                .or_default()
                .append(key, episode, max_episodes)
        });
        Box::pin(std::future::ready(Ok(appended)))
    }

    fn mark<'a>(
        &'a self,
        scope: &'a MemoryScope,
        mark: PassMark,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.with_state(|state| state.logs.entry(scope.clone()).or_default().mark(&mark));
        Box::pin(std::future::ready(Ok(())))
    }

    fn clear<'a>(
        &'a self,
        scope: &'a MemoryScope,
        key: &'a EditKey,
    ) -> BoxFuture<'a, Result<Cleared, AgentError>> {
        let cleared = self.with_state(|state| {
            if let Some((last, cleared)) = state.cleared.get(scope)
                && last == key
            {
                return cleared.clone();
            }
            let cleared = state.logs.remove(scope).unwrap_or_default().into_cleared();
            state
                .cleared
                .insert(scope.clone(), (key.clone(), cleared.clone()));
            cleared
        });
        Box::pin(std::future::ready(Ok(cleared)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn episode(id: &str) -> Episode {
        Episode {
            id: id.to_owned(),
            text: format!("text of {id}"),
            stop: AgentStop::Completed,
            at_ms: 1,
            labels: BTreeSet::new(),
        }
    }

    fn key(text: &str) -> EditKey {
        EditKey::new(text)
    }

    #[test]
    fn an_append_applies_its_key_once() {
        let mut log = EpisodeLog::default();
        let first = log.append(&key("k1"), episode("e1"), 10);
        let retry = log.append(&key("k1"), episode("e1"), 10);
        assert_eq!(
            first,
            Appended {
                pending: 1,
                evicted: 0
            }
        );
        assert_eq!(
            retry,
            Appended {
                pending: 1,
                evicted: 0
            }
        );
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn an_eviction_of_an_unconsolidated_episode_counts_as_dropped() {
        let mut log = EpisodeLog::default();
        log.append(&key("k1"), episode("e1"), 2);
        log.append(&key("k2"), episode("e2"), 2);
        let third = log.append(&key("k3"), episode("e3"), 2);
        assert_eq!(third.evicted, 1);
        assert_eq!(log.dropped, 1);
        let ids: Vec<&str> = log.episodes().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["e2", "e3"]);
    }

    #[test]
    fn an_eviction_of_a_consolidated_episode_drops_nothing() {
        let mut log = EpisodeLog::default();
        log.append(&key("k1"), episode("e1"), 1);
        log.mark(&PassMark {
            key: key("p1"),
            episodes: vec!["e1".into()],
            ..PassMark::default()
        });
        log.append(&key("k2"), episode("e2"), 1);
        assert_eq!(log.dropped, 0);
    }

    #[test]
    fn a_mark_applies_once_and_keeps_a_later_drop() {
        let mut log = EpisodeLog {
            dropped: 3,
            ..EpisodeLog::default()
        };
        log.append(&key("k1"), episode("e1"), 10);
        let mark = PassMark {
            key: key("p1"),
            episodes: vec!["e1".into()],
            dropped: 2,
            facts: vec!["a b c [episodes: e1]".into()],
        };
        log.mark(&mark);
        log.mark(&mark);
        assert_eq!(log.dropped, 1, "a drop after the read stays counted");
        assert_eq!(log.pending(10).len(), 0);
        assert_eq!(log.derived().count(), 1);
    }

    #[test]
    fn pending_reads_the_oldest_first_within_the_limit() {
        let mut log = EpisodeLog::default();
        for id in ["e1", "e2", "e3"] {
            log.append(&key(id), episode(id), 10);
        }
        let ids: Vec<String> = log.pending(2).into_iter().map(|e| e.id).collect();
        assert_eq!(ids, ["e1", "e2"]);
        assert_eq!(log.pending(0).len(), 1, "0 counts as 1");
    }

    #[test]
    fn the_log_forgets_only_old_keys() {
        let mut log = EpisodeLog::default();
        for n in 0..=MAX_KEPT_KEYS {
            log.append(&key(&format!("k{n}")), episode(&format!("e{n}")), 1);
        }
        assert_eq!(log.keys.len(), MAX_KEPT_KEYS);
        assert!(log.applied(&key(&format!("k{MAX_KEPT_KEYS}"))));
    }

    #[tokio::test]
    async fn a_retried_clear_returns_its_first_counts() {
        let store = InMemoryEpisodeStore::default();
        let scope = MemoryScope::new("agent:a/b:episodes");
        store
            .append(&scope, &key("k1"), episode("e1"), 10)
            .await
            .unwrap();
        let first = store.clear(&scope, &key("r1")).await.unwrap();
        let retry = store.clear(&scope, &key("r1")).await.unwrap();
        assert_eq!(first.episodes, 1);
        assert_eq!(retry, first);
        let next = store.clear(&scope, &key("r2")).await.unwrap();
        assert_eq!(next.episodes, 0);
    }

    #[tokio::test]
    async fn debug_output_shows_no_episode() {
        let store = InMemoryEpisodeStore::default();
        let scope = MemoryScope::new("s");
        store
            .append(&scope, &key("k"), episode("secret plan"), 10)
            .await
            .unwrap();
        let debug = format!("{store:?}");
        assert!(!debug.contains("secret"), "{debug}");
        assert!(debug.contains("scopes: 1"), "{debug}");
    }

    #[test]
    fn the_settings_decode_from_an_empty_object() {
        let settings: EpisodeSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(settings, EpisodeSettings::default());
        assert_eq!(settings.trigger, ConsolidationTrigger::Schedule);
    }
}
