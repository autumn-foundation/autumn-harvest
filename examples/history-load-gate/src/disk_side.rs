//! The owner disk cache side of the warm-up (issue #2258).
//!
//! It drives the real `OwnerDiskCache`. A store seals and writes a whole
//! history, as the first decision after a restart does. A read opens the
//! file and checks the last event against Postgres, as a memory miss does.

use std::sync::Arc;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::cache::CachedWorkflowState;
use autumn_harvest::decision_owner::{DecisionPartition, OwnerEpoch, OwnerKey};
use autumn_harvest::owner_disk_cache::{DiskLoad, DiskSlot, DiskStore, OwnerDiskCache, event_hash};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use diesel_async::AsyncPgConnection;

use crate::BoxError;

/// The partitions that the runs spread over. An owner of a 512-partition
/// database with 8 owners holds 64.
pub const PARTITIONS: u16 = 64;

/// A disk cache in its own temporary directory.
pub struct DiskSide {
    cache: Arc<OwnerDiskCache>,
    _dir: tempfile::TempDir,
}

impl DiskSide {
    /// Open a disk cache in a new temporary directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or the cache cannot be made.
    pub fn open() -> Result<Self, BoxError> {
        let dir = tempfile::tempdir()?;
        // The cache makes this subdirectory with the mode `0700`.
        let cache = Arc::new(OwnerDiskCache::open(dir.path().join("cache"))?);
        Ok(Self { cache, _dir: dir })
    }

    /// The slot of the run at `index`.
    ///
    /// # Panics
    ///
    /// Never: the partition is below [`PARTITIONS`].
    #[must_use]
    pub fn slot(index: usize) -> DiskSlot {
        let partition = u16::try_from(index % usize::from(PARTITIONS)).unwrap_or_default();
        DiskSlot {
            key: OwnerKey {
                shard: ShardId::UNENCODED,
                partition: DecisionPartition::new(partition).expect("below the partition count"),
            },
            epoch: OwnerEpoch::new(1),
            codec_generation: 0,
        }
    }

    /// Store `events` for the run `exec` at `index`. It blocks on disk I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when the store does not write the run.
    pub fn store(
        &self,
        index: usize,
        exec: ExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Result<(), BoxError> {
        let state = CachedWorkflowState {
            next_event_id: i32::try_from(events.len())?,
            events,
        };
        match self
            .cache
            .store(&Self::slot(index), exec.as_uuid(), &state, || true)
        {
            DiskStore::Written { .. } => Ok(()),
            other => Err(format!("the disk store gave {other:?}").into()),
        }
    }

    /// Read the run `exec` at `index`, check its last event against
    /// Postgres and load the Postgres delta. Returns the event count.
    ///
    /// # Errors
    ///
    /// Returns an error on a miss, a failed check or a failed query.
    pub async fn read(
        &self,
        conn: &mut AsyncPgConnection,
        index: usize,
        exec: ExecutionId,
    ) -> Result<usize, BoxError> {
        let cache = Arc::clone(&self.cache);
        let slot = Self::slot(index);
        let loaded = tokio::task::spawn_blocking(move || cache.load(&slot, exec.as_uuid())).await?;
        let DiskLoad::Hit(entry) = loaded else {
            return Err(format!("the disk read gave {}", loaded.outcome()).into());
        };
        let anchor = entry.state.next_event_id - 1;
        let codecs = PayloadCodecs::default();
        let event = store::load_event_inflated(conn, exec, anchor, &codecs, None)
            .await?
            .ok_or("the anchor event is missing")?;
        if event_hash(&event)? != entry.last_event_hash {
            return Err("the anchor check failed".into());
        }
        // A disk hit then loads the delta, as a memory hit does.
        let delta = store::load_history_since_inflated(
            conn,
            exec,
            entry.state.next_event_id,
            &codecs,
            None,
        )
        .await?;
        Ok(entry.state.events.len() + delta.events.len())
    }

    /// The cache, for a blocking task.
    #[must_use]
    pub fn cache(&self) -> Arc<OwnerDiskCache> {
        Arc::clone(&self.cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_history_loads_back_from_the_file() {
        let side = DiskSide::open().expect("open");
        let exec = ExecutionId::new();
        let history = crate::seed::history(21, 32);
        side.store(3, exec, history).expect("store");
        let DiskLoad::Hit(entry) = side.cache().load(&DiskSide::slot(3), exec.as_uuid()) else {
            panic!("a stored history is a hit");
        };
        assert_eq!(entry.state.events.len(), 21);
    }

    #[test]
    fn runs_spread_over_the_partitions() {
        let first = DiskSide::slot(0).key.partition;
        let wrapped = DiskSide::slot(usize::from(PARTITIONS)).key.partition;
        assert_eq!(first, wrapped);
        assert_ne!(first, DiskSide::slot(1).key.partition);
    }
}
