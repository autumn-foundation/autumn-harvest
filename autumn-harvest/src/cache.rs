//! LRU cache for suspended workflow states.
//!
//! When a workflow suspends, its full event history and the next DB event-id
//! are cached keyed by execution UUID. On the next task for that execution, the
//! worker fetches only *delta* events (new timer firings and signals appended
//! since the last suspension) and prepends the cached snapshot, avoiding a full
//! history reload from Postgres.
//!
//! This is the in-process companion to the Postgres-level sticky routing
//! mechanism: sticky routing keeps follow-up tasks on the owning worker
//! (issue #235); this cache ensures that when a task does land on the
//! owning worker the benefit is a cheap delta load rather than a full load.
//!
//! The cache uses a fixed maximum size with LRU eviction — when the cache is
//! full, the least-recently-used entry is evicted. Evicted entries cause the
//! next task to fall back to a full history load (cold path), which is always
//! correct — it is just slower.
//!
//! This module is pure data structure logic and does NOT require the `db` feature.

use lru::LruCache;
use std::num::NonZeroUsize;
use uuid::Uuid;

use crate::event::WorkflowEvent;

/// Cached state for a suspended workflow execution.
///
/// The worker inserts an entry here after each suspension and uses it on the
/// next task for the same execution to avoid a full history reload.
#[derive(Debug, Clone)]
pub struct CachedWorkflowState {
    /// All events present in the history at the point of the last suspension.
    ///
    /// On a cache hit the worker fetches only events with
    /// `event_id >= next_event_id` (the delta since the last suspension),
    /// then prepends this snapshot to reconstruct the full history for the
    /// executor without reading the old events from Postgres again.
    pub events: Vec<WorkflowEvent>,

    /// The `next_event_id` at the time of the last suspension.
    ///
    /// Delta queries use `WHERE event_id >= next_event_id` to load only the
    /// events appended after the last suspension (timer firings, signals).
    pub next_event_id: i32,
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
    /// If `max_size` is zero, it defaults to a capacity of 1.
    /// The maximum size is also capped at 1,000,000 entries to prevent OOM
    /// conditions when large, arbitrary sizes are requested.
    ///
    /// # Panics
    ///
    /// Panics if the clamped capacity somehow becomes zero (which should be
    /// mathematically impossible due to the clamping bounds).
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(10);
    /// assert_eq!(cache.len(), 0);
    /// ```
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        // Havoc prevention: Cap max capacity to prevent OOM, and floor at 1 to prevent panic.
        let safe_size = max_size.clamp(1, 1_000_000);
        let cap = NonZeroUsize::new(safe_size).expect("clamp ensures size >= 1");
        Self {
            inner: LruCache::new(cap),
        }
    }

    /// Insert or update a cached workflow state.
    ///
    /// If the cache is full, the least-recently-used entry is evicted.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use autumn_harvest::cache::{WorkflowCache, CachedWorkflowState};
    ///
    /// let mut cache = WorkflowCache::new(10);
    /// let state = CachedWorkflowState { events: vec![], next_event_id: 10 };
    /// cache.insert(Uuid::new_v4(), state);
    /// ```
    pub fn insert(&mut self, exec_id: Uuid, state: CachedWorkflowState) {
        self.inner.put(exec_id, state);
    }

    /// Look up a cached workflow state, marking it as recently used.
    ///
    /// Returns `None` if the execution ID is not in the cache.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let mut cache = WorkflowCache::new(10);
    /// assert!(cache.get(&Uuid::new_v4()).is_none());
    /// ```
    #[must_use]
    pub fn get(&mut self, exec_id: &Uuid) -> Option<&CachedWorkflowState> {
        self.inner.get(exec_id)
    }

    /// Remove a cached workflow state, returning it if present.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let mut cache = WorkflowCache::new(10);
    /// let id = Uuid::new_v4();
    /// assert!(cache.remove(&id).is_none());
    /// ```
    pub fn remove(&mut self, exec_id: &Uuid) -> Option<CachedWorkflowState> {
        self.inner.pop(exec_id)
    }

    /// Returns the number of entries currently in the cache.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(10);
    /// assert_eq!(cache.len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the cache contains no entries.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(10);
    /// assert!(cache.is_empty());
    /// ```
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

    fn make_state(next_event_id: i32) -> CachedWorkflowState {
        CachedWorkflowState {
            events: vec![],
            next_event_id,
        }
    }

    #[test]
    fn cache_stores_and_retrieves() {
        let mut cache = WorkflowCache::new(10);
        let id = Uuid::new_v4();
        let state = CachedWorkflowState {
            events: vec![],
            next_event_id: 5,
        };

        cache.insert(id, state);

        let retrieved = cache.get(&id).expect("should find cached state");
        assert_eq!(retrieved.next_event_id, 5);
        assert!(retrieved.events.is_empty());

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
        assert_eq!(
            removed
                .expect("removed entry should not be None")
                .next_event_id,
            10
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_get_missing_returns_none() {
        let mut cache = WorkflowCache::new(5);
        assert!(cache.get(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn cache_handles_zero_size_safely() {
        let cache = WorkflowCache::new(0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn cache_handles_max_size_without_oom() {
        let cache = WorkflowCache::new(usize::MAX);
        assert_eq!(cache.len(), 0);
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

    #[test]
    fn cached_state_stores_events_and_next_event_id() {
        use crate::event::WorkflowEvent;
        use chrono::Utc;
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();

        let state = CachedWorkflowState {
            events: vec![WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            next_event_id: 42,
        };
        cache.insert(id, state);

        let retrieved = cache.get(&id).expect("should be present");
        assert_eq!(retrieved.next_event_id, 42);
        assert_eq!(retrieved.events.len(), 1);
    }
}
