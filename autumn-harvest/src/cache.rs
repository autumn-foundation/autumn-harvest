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
//! An entry can also hold the suspended workflow itself (issue #1798). A warm
//! task then resumes the parked future with the delta events and does not
//! replay history. See [`crate::resident`]. The worker takes an entry on a
//! hit and puts it back only after a suspension commits.
//!
//! This module is pure data structure logic and does NOT require the `db` feature.

use lru::LruCache;
use std::num::NonZeroUsize;
use uuid::Uuid;

use crate::event::WorkflowEvent;
use crate::resident::{NotKept, ResidentWorkflow};

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

/// Stored history bytes for events with `event_id < through` (issue #1804).
///
/// The worker keeps this mark with the cache entry. On a cache hit it sums
/// only the events at or after `through`. This keeps the byte check off the
/// full history on the warm path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryBytesMark {
    /// Sum of `pg_column_size(event_data)` below `through`.
    pub(crate) bytes: u64,
    /// First event id that `bytes` does not include.
    pub(crate) through: i32,
    /// Incremental sums since the last full sum. The worker re-sums the
    /// full history when this reaches its interval.
    pub(crate) warm_steps: u32,
}

/// One cache entry: the event snapshot, the resident workflow if any, and
/// the stored-history byte mark if any.
struct CacheEntry {
    state: CachedWorkflowState,
    resident: Option<ResidentWorkflow>,
    /// Why the suspension did not stay resident (issue #2007).
    // Only the `db` worker reads the reason.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    not_kept: Option<NotKept>,
    // Only the `db` worker reads the mark.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    history_bytes: Option<HistoryBytesMark>,
}

/// An entry that [`WorkflowCache::take_with_history_bytes`] removed.
#[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
pub(crate) struct TakenEntry {
    pub(crate) state: CachedWorkflowState,
    pub(crate) resident: Option<ResidentWorkflow>,
    /// Why the suspension did not stay resident (issue #2007).
    pub(crate) not_kept: Option<NotKept>,
    pub(crate) history_bytes: Option<HistoryBytesMark>,
}

/// The entries that [`WorkflowCache::close`] removed (issue #1798).
///
/// Dropping the value drops each parked handler future and its context.
pub(crate) struct ClosedEntries(#[allow(dead_code)] LruCache<Uuid, CacheEntry>);

impl ClosedEntries {
    /// The number of entries that the cache held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

/// LRU cache mapping workflow execution IDs to their cached replay state.
///
/// Thread-safety: this cache is NOT `Sync` — it should be owned by a single
/// worker task (or wrapped in a `Mutex` if shared).
pub struct WorkflowCache {
    inner: LruCache<Uuid, CacheEntry>,
    resident_enabled: bool,
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
        let cap = NonZeroUsize::new(safe_size).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: LruCache::new(cap),
            resident_enabled: true,
        }
    }

    /// Sets whether entries keep the suspended workflow resident (issue
    /// #1798). On by default.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(10).with_resident(false);
    /// assert!(!cache.resident_enabled());
    /// ```
    #[must_use]
    pub const fn with_resident(mut self, enabled: bool) -> Self {
        self.resident_enabled = enabled;
        self
    }

    /// Whether entries keep the suspended workflow resident (issue #1798).
    #[must_use]
    pub const fn resident_enabled(&self) -> bool {
        self.resident_enabled
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
        let _displaced = self.insert_resident(exec_id, state, None);
    }

    /// Inserts a snapshot with the resident workflow of its suspension
    /// (issue #1798).
    ///
    /// When resident state is off, `resident` is not stored. An existing
    /// entry with a later `next_event_id` stays, because a later decision
    /// wrote it. Returns the entry that this call displaced or refused, or
    /// the evicted LRU entry, so the caller can drop it outside any lock.
    pub(crate) fn insert_resident(
        &mut self,
        exec_id: Uuid,
        state: CachedWorkflowState,
        resident: Option<ResidentWorkflow>,
    ) -> Option<(CachedWorkflowState, Option<ResidentWorkflow>)> {
        self.insert_resident_with_history_bytes(exec_id, state, resident, None, None)
    }

    /// [`Self::insert_resident`] that also stores the stored-history byte
    /// mark of the snapshot (issue #1804). `not_kept` says why the
    /// suspension did not stay resident (issue #2007).
    pub(crate) fn insert_resident_with_history_bytes(
        &mut self,
        exec_id: Uuid,
        state: CachedWorkflowState,
        resident: Option<ResidentWorkflow>,
        not_kept: Option<NotKept>,
        history_bytes: Option<HistoryBytesMark>,
    ) -> Option<(CachedWorkflowState, Option<ResidentWorkflow>)> {
        let resident = resident.filter(|_| self.resident_enabled);
        if self
            .inner
            .peek(&exec_id)
            .is_some_and(|existing| existing.state.next_event_id > state.next_event_id)
        {
            return Some((state, resident));
        }
        self.inner
            .push(
                exec_id,
                CacheEntry {
                    state,
                    resident,
                    not_kept,
                    history_bytes,
                },
            )
            .map(|(_, entry)| (entry.state, entry.resident))
    }

    /// Removes an entry and returns its snapshot and resident workflow
    /// (issue #1798).
    ///
    /// The worker takes the entry on a hit, so no other task can resume the
    /// same parked future.
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
    pub(crate) fn take(
        &mut self,
        exec_id: &Uuid,
    ) -> Option<(CachedWorkflowState, Option<ResidentWorkflow>)> {
        self.take_with_history_bytes(exec_id)
            .map(|entry| (entry.state, entry.resident))
    }

    /// [`Self::take`] that also returns the stored-history byte mark of the
    /// entry (issue #1804).
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
    pub(crate) fn take_with_history_bytes(&mut self, exec_id: &Uuid) -> Option<TakenEntry> {
        self.inner.pop(exec_id).map(|entry| TakenEntry {
            state: entry.state,
            resident: entry.resident,
            not_kept: entry.not_kept,
            history_bytes: entry.history_bytes,
        })
    }

    /// Closes the cache when the worker stops (issue #1798).
    ///
    /// The method removes every entry and stops resident capture. A task
    /// that outlives the shutdown drain then cannot park a future again. The
    /// caller drops the returned entries outside the cache lock.
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker closes the cache.
    #[must_use = "drop the closed entries outside the cache lock"]
    pub(crate) fn close(&mut self) -> ClosedEntries {
        self.resident_enabled = false;
        let empty = LruCache::new(self.inner.cap());
        ClosedEntries(std::mem::replace(&mut self.inner, empty))
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
        self.inner.get(exec_id).map(|entry| &entry.state)
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
        self.inner.pop(exec_id).map(|entry| entry.state)
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
            .field("resident_enabled", &self.resident_enabled)
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
    fn cache_keeps_history_bytes_mark_with_its_entry() {
        // Issue #1804: the byte mark lives and dies with its entry.
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        let mark = HistoryBytesMark {
            bytes: 1_234,
            through: 7,
            warm_steps: 0,
        };

        let _ = cache.insert_resident_with_history_bytes(id, make_state(7), None, None, Some(mark));
        let taken = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken.history_bytes, Some(mark), "the take returns the mark");
        assert!(
            cache.take_with_history_bytes(&id).is_none(),
            "the take removes the entry"
        );

        cache.insert(id, make_state(8));
        let taken = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken.history_bytes, None, "a plain insert stores no mark");
    }

    #[test]
    fn an_entry_keeps_why_its_suspension_was_not_resident() {
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        let _ = cache.insert_resident_with_history_bytes(
            id,
            make_state(7),
            None,
            Some(NotKept::MultiAwait),
            None,
        );
        let taken = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken.not_kept, Some(NotKept::MultiAwait));

        cache.insert(id, make_state(8));
        let taken = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken.not_kept, None, "a plain insert stores no reason");
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

    /// Waits for one signal.
    fn signal_workflow(
        ctx: &crate::context::WorkflowContext,
        _input: serde_json::Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + '_>,
    > {
        Box::pin(async move { ctx.wait_for_signal("go").await.map_err(|e| e.to_string()) })
    }

    /// A resident workflow parked on a signal wait.
    async fn resident() -> ResidentWorkflow {
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];
        let (_outcome, resident) = crate::resident::start(
            crate::types::ExecutionId::new(),
            history,
            signal_workflow,
            serde_json::Value::Null,
        )
        .await;
        resident.expect("a signal wait stays resident")
    }

    #[tokio::test]
    async fn take_removes_the_entry_with_its_resident_workflow() {
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        cache.insert_resident(id, make_state(7), Some(resident().await));

        let (state, live) = cache.take(&id).expect("entry is present");
        assert_eq!(state.next_event_id, 7);
        assert!(live.is_some(), "the resident workflow comes with the entry");
        assert!(cache.take(&id).is_none(), "a take removes the entry");
    }

    #[tokio::test]
    async fn close_releases_every_entry_and_stops_resident_capture() {
        let mut cache = WorkflowCache::new(5);
        cache.insert_resident(Uuid::new_v4(), make_state(3), Some(resident().await));
        cache.insert(Uuid::new_v4(), make_state(5));

        let closed = cache.close();
        assert_eq!(closed.len(), 2, "close hands back every entry");
        drop(closed);
        assert!(cache.is_empty(), "a closed cache holds no entry");
        assert!(!cache.resident_enabled());

        // A task that outlives the shutdown drain cannot park a future again.
        let id = Uuid::new_v4();
        cache.insert_resident(id, make_state(7), Some(resident().await));
        let (_, live) = cache.take(&id).expect("the snapshot is kept");
        assert!(live.is_none(), "a closed cache keeps no resident workflow");
    }

    #[test]
    fn a_later_snapshot_is_not_replaced_by_an_earlier_one() {
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        cache.insert(id, make_state(9));
        let refused = cache.insert_resident(id, make_state(4), None);

        assert_eq!(refused.map(|(state, _)| state.next_event_id), Some(4));
        assert_eq!(cache.get(&id).map(|state| state.next_event_id), Some(9));
    }

    #[tokio::test]
    async fn disabled_resident_state_keeps_only_the_snapshot() {
        let mut cache = WorkflowCache::new(5).with_resident(false);
        let id = Uuid::new_v4();
        cache.insert_resident(id, make_state(7), Some(resident().await));

        let (state, live) = cache.take(&id).expect("entry is present");
        assert_eq!(state.next_event_id, 7);
        assert!(
            live.is_none(),
            "a disabled cache must drop the resident workflow"
        );
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
