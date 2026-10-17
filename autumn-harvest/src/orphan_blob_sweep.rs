//! Sweep of payload blobs that a rolled-back write uploaded (issue #2123).
//!
//! A write uploads its blobs before it opens its transaction (issue #954). A
//! rollback leaves those blobs in the store with no `harvest_payload_refs` row.
//! Retention finds blobs only through those rows. This pass finds them through
//! the store listing instead. See `DESIGN-2123.md`.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::payload_store::{PayloadOffloader, PayloadStore, StoredBlob};
use crate::retention::{MIN_ORPHAN_BLOB_GRACE, RetentionConfig};
use crate::shard::ShardedDbPool;
use crate::telemetry::MetricsRecorder;

/// Listing pages that one tick reads.
const MAX_PAGES_PER_TICK: usize = 10;

/// The keys of `blobs` that are older than `cutoff` and that no database
/// references.
pub fn select_orphans(
    blobs: &[StoredBlob],
    cutoff: DateTime<Utc>,
    referenced: &HashSet<String>,
) -> Vec<String> {
    blobs
        .iter()
        .filter(|blob| blob.created_at < cutoff && !referenced.contains(&blob.key))
        .map(|blob| blob.key.clone())
        .collect()
}

/// The keys of `keys` that a `harvest_payload_refs` row names on any database.
///
/// A database that cannot answer fails the call. Its references are unknown,
/// so the caller must not delete.
async fn referenced_keys(
    pools: &ShardedDbPool,
    keys: &[String],
) -> Result<HashSet<String>, String> {
    let mut referenced = HashSet::new();
    for (pool, _) in pools.pool_groups() {
        let mut conn = crate::replication::fenced_checkout(pool)
            .await
            .map_err(|err| err.to_string())?;
        referenced.extend(
            crate::store::batch_blob_keys_still_referenced(&mut conn, keys)
                .await
                .map_err(|err| err.to_string())?,
        );
    }
    Ok(referenced)
}

/// What one page of the sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
struct PageOutcome {
    found: u64,
    deleted: u64,
    delete_failures: u64,
}

/// Find and delete the orphans of one listing page.
async fn sweep_page(
    store: &dyn PayloadStore,
    pools: &ShardedDbPool,
    blobs: &[StoredBlob],
    cutoff: DateTime<Utc>,
    dry_run: bool,
    shutdown: &CancellationToken,
) -> Result<PageOutcome, String> {
    // A store can ignore `created_before`, so filter again here.
    let old_keys: Vec<String> = blobs
        .iter()
        .filter(|blob| blob.created_at < cutoff)
        .map(|blob| blob.key.clone())
        .collect();
    if old_keys.is_empty() {
        return Ok(PageOutcome::default());
    }
    let referenced = referenced_keys(pools, &old_keys).await?;
    let mut orphans = select_orphans(blobs, cutoff, &referenced);
    if !dry_run && !orphans.is_empty() {
        // A writer can commit a reference after the first check. Check again
        // just before the delete to keep that window short.
        let referenced = referenced_keys(pools, &orphans).await?;
        orphans.retain(|key| !referenced.contains(key));
    }
    let mut outcome = PageOutcome {
        found: orphans.len() as u64,
        ..PageOutcome::default()
    };
    if dry_run {
        return Ok(outcome);
    }
    for key in &orphans {
        if shutdown.is_cancelled() {
            break;
        }
        match store.delete(key).await {
            Ok(()) => outcome.deleted += 1,
            Err(err) => {
                outcome.delete_failures += 1;
                tracing::warn!(blob_key = %key, error = %err, "harvest orphan-blob delete failed");
            }
        }
    }
    Ok(outcome)
}

/// The state that the orphan sweep keeps between ticks.
#[derive(Debug, Default)]
pub struct OrphanBlobSweeper {
    /// The listing cursor. `None` starts at the first key.
    cursor: Option<String>,
    /// Set when the store cannot list. The sweep then stays off.
    unsupported: bool,
}

impl OrphanBlobSweeper {
    /// Run one tick of the sweep.
    ///
    /// The pass is best-effort. A failed page is logged and retried on the
    /// next tick. Shutdown takes effect between pages and between deletes.
    pub async fn run_pass(
        &mut self,
        pools: &ShardedDbPool,
        config: &RetentionConfig,
        offloader: Option<&PayloadOffloader>,
        metrics: &dyn MetricsRecorder,
        shutdown: &CancellationToken,
    ) {
        let (Some(offloader), Some(grace)) = (offloader, config.orphan_blob_grace()) else {
            return;
        };
        if self.unsupported {
            return;
        }
        // `spawn` does not call `validate`, so the pass checks the bound itself.
        // A grace below the floor could delete a blob that a write is about to
        // reference.
        let cutoff = (MIN_ORPHAN_BLOB_GRACE..=crate::retention::MAX_MAX_AGE)
            .contains(&grace)
            .then(|| chrono::Duration::from_std(grace).ok())
            .flatten()
            .and_then(|grace| Utc::now().checked_sub_signed(grace));
        let Some(cutoff) = cutoff else {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                "harvest orphan-blob sweep skipped: orphan_blob_grace is out of range"
            );
            return;
        };
        let store = offloader.store().as_ref();
        for _ in 0..MAX_PAGES_PER_TICK {
            if shutdown.is_cancelled() {
                return;
            }
            let limit = config.batch_size.max(1);
            let page = match store.list(self.cursor.as_deref(), cutoff, limit).await {
                Ok(Some(page)) => page,
                Ok(None) => {
                    self.unsupported = true;
                    tracing::info!(
                        "harvest orphan-blob sweep is off: the payload store cannot list"
                    );
                    return;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "harvest orphan-blob listing failed");
                    return;
                }
            };
            match sweep_page(store, pools, &page.blobs, cutoff, config.dry_run, shutdown).await {
                Ok(outcome) => {
                    if outcome != PageOutcome::default() {
                        metrics.record_orphan_blob_sweep(
                            outcome.found,
                            outcome.deleted,
                            outcome.delete_failures,
                        );
                        tracing::info!(
                            found = outcome.found,
                            deleted = outcome.deleted,
                            delete_failures = outcome.delete_failures,
                            dry_run = config.dry_run,
                            "harvest orphan-blob sweep page"
                        );
                    }
                }
                Err(error) => {
                    // The cursor stays, so the next tick retries this page.
                    tracing::warn!(error = %error, "harvest orphan-blob sweep page failed");
                    return;
                }
            }
            self.cursor = page.next;
            if self.cursor.is_none() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(key: &str, age_hours: i64) -> StoredBlob {
        StoredBlob {
            key: key.to_string(),
            created_at: Utc::now() - chrono::Duration::hours(age_hours),
        }
    }

    #[test]
    fn an_old_unreferenced_blob_is_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let orphans = select_orphans(&[blob("a", 48)], cutoff, &HashSet::new());
        assert_eq!(orphans, vec!["a".to_string()]);
    }

    #[test]
    fn a_referenced_blob_is_never_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let referenced = HashSet::from(["a".to_string()]);
        let orphans = select_orphans(&[blob("a", 48), blob("b", 48)], cutoff, &referenced);
        assert_eq!(orphans, vec!["b".to_string()]);
    }

    #[test]
    fn a_blob_younger_than_the_cutoff_is_never_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let orphans = select_orphans(&[blob("young", 1)], cutoff, &HashSet::new());
        assert!(orphans.is_empty(), "a store may ignore the filter");
    }

    #[test]
    fn a_blob_created_exactly_at_the_cutoff_is_kept() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let at = StoredBlob {
            key: "edge".to_string(),
            created_at: cutoff,
        };
        assert!(select_orphans(&[at], cutoff, &HashSet::new()).is_empty());
    }
}
