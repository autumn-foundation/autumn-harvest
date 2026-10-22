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

use crate::decision_owner::OwnerKey;
use crate::event::WorkflowEvent;
use crate::resident::Residency;

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

/// One cache entry: the event snapshot, the resident state of its
/// suspension, and the stored-history byte mark if any.
struct CacheEntry {
    state: CachedWorkflowState,
    resident: Residency,
    // Only the `db` worker reads the mark.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    history_bytes: Option<HistoryBytesMark>,
    /// The shard and decision partition of the execution, if the caller
    /// knows them (issue #2251). A fenced owner drops each entry of its
    /// partition. Only the db-gated worker reads the key.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    owner_key: Option<OwnerKey>,
}

/// The entries that [`WorkflowCache::close`] (issue #1798) or
/// [`WorkflowCache::evict_partition`] (issue #2251) removed.
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
        let _displaced = self.insert_resident(exec_id, state, Residency::Unknown);
    }

    /// Inserts a snapshot with the resident state of its suspension
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
        resident: Residency,
    ) -> Option<(CachedWorkflowState, Residency)> {
        self.insert_resident_with_history_bytes(exec_id, state, resident, None, None)
    }

    /// [`Self::insert_resident`] that also stores the stored-history byte
    /// mark of the snapshot (issue #1804) and the decision partition of the
    /// execution (issue #2251).
    pub(crate) fn insert_resident_with_history_bytes(
        &mut self,
        exec_id: Uuid,
        state: CachedWorkflowState,
        resident: Residency,
        history_bytes: Option<HistoryBytesMark>,
        owner_key: Option<OwnerKey>,
    ) -> Option<(CachedWorkflowState, Residency)> {
        let resident = if self.resident_enabled {
            resident
        } else {
            Residency::Unknown
        };
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
                    history_bytes,
                    owner_key,
                },
            )
            .map(|(_, entry)| (entry.state, entry.resident))
    }

    /// Removes an entry and returns its snapshot and resident state
    /// (issue #1798).
    ///
    /// The worker takes the entry on a hit, so no other task can resume the
    /// same parked future.
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
    pub(crate) fn take(&mut self, exec_id: &Uuid) -> Option<(CachedWorkflowState, Residency)> {
        self.take_with_history_bytes(exec_id)
            .map(|(state, resident, _)| (state, resident))
    }

    /// [`Self::take`] that also returns the stored-history byte mark of the
    /// entry (issue #1804).
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
    pub(crate) fn take_with_history_bytes(
        &mut self,
        exec_id: &Uuid,
    ) -> Option<(CachedWorkflowState, Residency, Option<HistoryBytesMark>)> {
        self.take_entry(exec_id)
            .map(|(state, resident, history_bytes, _)| (state, resident, history_bytes))
    }

    /// [`Self::take_with_history_bytes`] that also returns the owner key of
    /// the entry (issue #2253). A decision of another key must not use the
    /// entry: the run moved to another database.
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker takes entries.
    pub(crate) fn take_entry(
        &mut self,
        exec_id: &Uuid,
    ) -> Option<(
        CachedWorkflowState,
        Residency,
        Option<HistoryBytesMark>,
        Option<OwnerKey>,
    )> {
        self.inner.pop(exec_id).map(|entry| {
            (
                entry.state,
                entry.resident,
                entry.history_bytes,
                entry.owner_key,
            )
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

    /// Removes every entry of the partition `key` (issue #2251).
    ///
    /// A fenced owner calls this when it unloads the partition. A new
    /// tenure then starts cold, as rule R4 of the lease ADR requires. The
    /// caller drops the returned entries outside the cache lock, because
    /// each one can hold a parked handler future.
    #[must_use = "drop the evicted entries outside the cache lock"]
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker unloads a partition.
    pub(crate) fn evict_partition(&mut self, key: OwnerKey) -> ClosedEntries {
        let doomed: Vec<Uuid> = self
            .inner
            .iter()
            .filter(|(_, entry)| entry.owner_key == Some(key))
            .map(|(id, _)| *id)
            .collect();
        let cap = NonZeroUsize::new(doomed.len()).unwrap_or(NonZeroUsize::MIN);
        let mut evicted = LruCache::new(cap);
        for id in doomed {
            if let Some(entry) = self.inner.pop(&id) {
                evicted.push(id, entry);
            }
        }
        ClosedEntries(evicted)
    }

    /// Drop every entry of each partition in `keys`, in one pass (issue
    /// #2252).
    ///
    /// The owner loop drops many partitions at once on a release or a
    /// switch-off. One pass keeps the cache lock short.
    #[must_use = "drop the evicted entries outside the cache lock"]
    #[cfg_attr(not(feature = "db"), allow(dead_code))] // Only the db-gated worker unloads a partition.
    pub(crate) fn evict_partitions(&mut self, keys: &[OwnerKey]) -> ClosedEntries {
        let keys: std::collections::HashSet<OwnerKey> = keys.iter().copied().collect();
        let doomed: Vec<Uuid> = self
            .inner
            .iter()
            .filter(|(_, entry)| entry.owner_key.is_some_and(|k| keys.contains(&k)))
            .map(|(id, _)| *id)
            .collect();
        let cap = NonZeroUsize::new(doomed.len()).unwrap_or(NonZeroUsize::MIN);
        let mut evicted = LruCache::new(cap);
        for id in doomed {
            if let Some(entry) = self.inner.pop(&id) {
                evicted.push(id, entry);
            }
        }
        ClosedEntries(evicted)
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

    /// Whether the cache holds an entry for `exec_id`. The check does not
    /// change the LRU order.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use autumn_harvest::cache::WorkflowCache;
    ///
    /// let cache = WorkflowCache::new(10);
    /// assert!(!cache.contains(&Uuid::new_v4()));
    /// ```
    #[must_use]
    pub fn contains(&self, exec_id: &Uuid) -> bool {
        self.inner.contains(exec_id)
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
    use crate::resident::ResidentWorkflow;

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

        let _ = cache.insert_resident_with_history_bytes(
            id,
            make_state(7),
            Residency::Unknown,
            Some(mark),
            None,
        );
        let (_, _, taken) = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken, Some(mark), "the take returns the mark");
        assert!(
            cache.take_with_history_bytes(&id).is_none(),
            "the take removes the entry"
        );

        cache.insert(id, make_state(8));
        let (_, _, taken) = cache.take_with_history_bytes(&id).expect("entry");
        assert_eq!(taken, None, "a plain insert stores no mark");
    }

    /// Issue #2253: a take returns the owner key, so a decision can refuse
    /// an entry of another database. `contains` keeps the entry.
    #[test]
    fn take_entry_returns_the_owner_key() {
        use crate::decision_owner::DecisionPartition;
        use crate::types::ShardId;
        let key = OwnerKey {
            shard: ShardId::new(1),
            partition: DecisionPartition::new(3).expect("in range"),
        };
        let mut cache = WorkflowCache::new(4);
        let id = Uuid::new_v4();
        let _ = cache.insert_resident_with_history_bytes(
            id,
            make_state(2),
            Residency::Unknown,
            None,
            Some(key),
        );
        assert!(cache.contains(&id), "contains keeps the entry");
        let (_, _, _, taken) = cache.take_entry(&id).expect("entry");
        assert_eq!(taken, Some(key));
        assert!(!cache.contains(&id), "a take removes the entry");
    }

    #[test]
    fn evict_partitions_drops_each_listed_partition_in_one_pass() {
        use crate::decision_owner::DecisionPartition;
        use crate::types::ShardId;
        let key = |p| OwnerKey {
            shard: ShardId::new(0),
            partition: DecisionPartition::new(p).expect("in range"),
        };
        let mut cache = WorkflowCache::new(8);
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        for (id, p) in ids.iter().zip([1, 2, 3]) {
            let _ = cache.insert_resident_with_history_bytes(
                *id,
                make_state(1),
                Residency::Unknown,
                None,
                Some(key(p)),
            );
        }
        let evicted = cache.evict_partitions(&[key(1), key(3)]);
        assert_eq!(evicted.len(), 2);
        drop(evicted);
        assert!(cache.get(&ids[0]).is_none());
        assert!(cache.get(&ids[1]).is_some());
        assert!(cache.get(&ids[2]).is_none());
    }

    #[test]
    fn evict_partition_drops_only_that_partition() {
        use crate::decision_owner::DecisionPartition;
        use crate::types::ShardId;
        let key = |shard, n| OwnerKey {
            shard: ShardId::new(shard),
            partition: DecisionPartition::new(n).expect("in range"),
        };
        let mut cache = WorkflowCache::new(8);
        let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        let (first, second, other, untagged, remote) = (ids[0], ids[1], ids[2], ids[3], ids[4]);
        let tags = [
            (first, Some(key(0, 3))),
            (second, Some(key(0, 3))),
            (other, Some(key(0, 4))),
            (untagged, None),
            (remote, Some(key(1, 3))),
        ];
        for (id, partition) in tags {
            let _ = cache.insert_resident_with_history_bytes(
                id,
                make_state(1),
                Residency::Unknown,
                None,
                partition,
            );
        }
        let evicted = cache.evict_partition(key(0, 3));
        assert_eq!(evicted.len(), 2);
        assert!(cache.get(&first).is_none() && cache.get(&second).is_none());
        assert!(cache.get(&other).is_some(), "another partition stays");
        assert!(cache.get(&untagged).is_some(), "an untagged entry stays");
        assert!(
            cache.get(&remote).is_some(),
            "partition 3 of another database stays"
        );
        assert_eq!(cache.evict_partition(key(0, 9)).len(), 0);
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
        cache.insert_resident(id, make_state(7), Residency::Parked(resident().await));

        let (state, live) = cache.take(&id).expect("entry is present");
        assert_eq!(state.next_event_id, 7);
        assert!(
            live.is_parked(),
            "the resident workflow comes with the entry"
        );
        assert!(cache.take(&id).is_none(), "a take removes the entry");
    }

    #[tokio::test]
    async fn close_releases_every_entry_and_stops_resident_capture() {
        let mut cache = WorkflowCache::new(5);
        cache.insert_resident(
            Uuid::new_v4(),
            make_state(3),
            Residency::Parked(resident().await),
        );
        cache.insert(Uuid::new_v4(), make_state(5));

        let closed = cache.close();
        assert_eq!(closed.len(), 2, "close hands back every entry");
        drop(closed);
        assert!(cache.is_empty(), "a closed cache holds no entry");
        assert!(!cache.resident_enabled());

        // A task that outlives the shutdown drain cannot park a future again.
        let id = Uuid::new_v4();
        cache.insert_resident(id, make_state(7), Residency::Parked(resident().await));
        let (_, live) = cache.take(&id).expect("the snapshot is kept");
        assert!(
            !live.is_parked(),
            "a closed cache keeps no resident workflow"
        );
    }

    #[test]
    fn an_entry_keeps_the_miss_reason_of_its_suspension() {
        // Issue #2007: the next decision reports why it replays.
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        cache.insert_resident(
            id,
            make_state(7),
            Residency::Missed(crate::telemetry::ResidentOutcome::Race),
        );
        let (_, residency) = cache.take(&id).expect("entry is present");
        assert!(
            matches!(
                residency,
                Residency::Missed(crate::telemetry::ResidentOutcome::Race)
            ),
            "{residency:?}"
        );
    }

    #[test]
    fn a_later_snapshot_is_not_replaced_by_an_earlier_one() {
        let mut cache = WorkflowCache::new(5);
        let id = Uuid::new_v4();
        cache.insert(id, make_state(9));
        let refused = cache.insert_resident(id, make_state(4), Residency::Unknown);

        assert_eq!(refused.map(|(state, _)| state.next_event_id), Some(4));
        assert_eq!(cache.get(&id).map(|state| state.next_event_id), Some(9));
    }

    #[tokio::test]
    async fn disabled_resident_state_keeps_only_the_snapshot() {
        let mut cache = WorkflowCache::new(5).with_resident(false);
        let id = Uuid::new_v4();
        cache.insert_resident(id, make_state(7), Residency::Parked(resident().await));

        let (state, live) = cache.take(&id).expect("entry is present");
        assert_eq!(state.next_event_id, 7);
        assert!(
            !live.is_parked(),
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
