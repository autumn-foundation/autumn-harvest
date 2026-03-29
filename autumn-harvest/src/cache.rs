//! LRU cache for suspended workflow states.
//!
//! When a workflow suspends, its replay position and sequence counters are
//! cached so that subsequent replay attempts can skip already-processed events.
//! This avoids re-replaying the entire history from scratch on every wake-up.
//!
//! The cache uses a fixed maximum size with LRU eviction -- when the cache is
//! full, the least-recently-used entry is evicted to make room.
//!
//! This module is pure data structure logic and does NOT require the `db` feature.

use lru::LruCache;
use std::num::NonZeroUsize;
use uuid::Uuid;

/// Cached state for a suspended workflow execution.
///
/// Stores the replay cursor position and sequence counters so that
/// subsequent replay attempts can fast-forward past already-processed events.
#[derive(Debug, Clone)]
pub struct CachedWorkflowState {
    /// Number of events already processed in the history.
    pub replay_position: usize,
    /// Next activity sequence number to assign.
    pub next_activity_seq: u32,
    /// Next timer sequence number to assign.
    pub next_timer_seq: u32,
}

/// LRU cache mapping workflow execution IDs to their cached replay state.
///
/// Thread-safety: this cache is NOT `Sync` — it should be owned by a single
/// worker task (or wrapped in a `Mutex` if shared).
pub struct WorkflowCache {
    inner: LruCache<Uuid, CachedWorkflowState>,
}

impl WorkflowCache {
    /// Create a new cache with the given maximum number of entries.
    ///
    /// # Panics
    ///
    /// Panics if `max_size` is zero.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        let cap = NonZeroUsize::new(max_size).expect("cache max_size must be > 0");
        Self {
            inner: LruCache::new(cap),
        }
    }

    /// Insert or update a cached workflow state.
    ///
    /// If the cache is full, the least-recently-used entry is evicted.
    pub fn insert(&mut self, exec_id: Uuid, state: CachedWorkflowState) {
        self.inner.put(exec_id, state);
    }

    /// Look up a cached workflow state, marking it as recently used.
    ///
    /// Returns `None` if the execution ID is not in the cache.
    #[must_use]
    pub fn get(&mut self, exec_id: &Uuid) -> Option<&CachedWorkflowState> {
        self.inner.get(exec_id)
    }

    /// Remove a cached workflow state, returning it if present.
    pub fn remove(&mut self, exec_id: &Uuid) -> Option<CachedWorkflowState> {
        self.inner.pop(exec_id)
    }

    /// Returns the number of entries currently in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the cache contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl std::fmt::Debug for WorkflowCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowCache")
            .field("len", &self.inner.len())
            .field("cap", &self.inner.cap())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(pos: usize) -> CachedWorkflowState {
        CachedWorkflowState {
            replay_position: pos,
            next_activity_seq: 0,
            next_timer_seq: 0,
        }
    }

    #[test]
    fn cache_stores_and_retrieves() {
        let mut cache = WorkflowCache::new(10);
        let id = Uuid::new_v4();
        let state = CachedWorkflowState {
            replay_position: 5,
            next_activity_seq: 3,
            next_timer_seq: 1,
        };

        cache.insert(id, state);

        let retrieved = cache.get(&id).expect("should find cached state");
        assert_eq!(retrieved.replay_position, 5);
        assert_eq!(retrieved.next_activity_seq, 3);
        assert_eq!(retrieved.next_timer_seq, 1);

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }

    #[test]
    fn cache_evicts_lru() {
        let mut cache = WorkflowCache::new(2);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        cache.insert(id1, make_state(1));
        cache.insert(id2, make_state(2));

        // Cache is full (size 2). Inserting a third should evict id1 (LRU).
        cache.insert(id3, make_state(3));

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&id1).is_none(), "id1 should have been evicted");
        assert!(cache.get(&id2).is_some(), "id2 should still be present");
        assert!(cache.get(&id3).is_some(), "id3 should be present");
    }

    #[test]
    fn cache_remove_returns_entry() {
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();

        cache.insert(id, make_state(10));
        let removed = cache.remove(&id);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().replay_position, 10);
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_get_missing_returns_none() {
        let mut cache = WorkflowCache::new(5);
        assert!(cache.get(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn cache_lru_access_updates_recency() {
        let mut cache = WorkflowCache::new(2);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        cache.insert(id1, make_state(1));
        cache.insert(id2, make_state(2));

        // Access id1 to make it recently used (id2 is now LRU).
        let _ = cache.get(&id1);

        // Insert id3 -- should evict id2 (LRU), not id1.
        cache.insert(id3, make_state(3));

        assert!(
            cache.get(&id1).is_some(),
            "id1 should still be present (recently accessed)"
        );
        assert!(
            cache.get(&id2).is_none(),
            "id2 should have been evicted (LRU)"
        );
        assert!(cache.get(&id3).is_some(), "id3 should be present");
    }
}
