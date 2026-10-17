//! Sweep of payload blobs that a rolled-back write uploaded (issue #2123).
//!
//! A write uploads its blobs before it opens its transaction (issue #954). A
//! rollback leaves those blobs in the store with no `harvest_payload_refs` row.
//! Retention finds blobs only through those rows. This pass finds them through
//! the store listing instead. See `DESIGN-2123.md`.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::payload_store::{PayloadOffloader, PayloadStore, PayloadStoreFuture, StoredBlob};
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

/// Why a store call failed.
enum CallError {
    /// The process is shutting down.
    Shutdown,
    /// The store did not answer in time. The store is probably hung.
    TimedOut(String),
    /// The store or a database returned an error.
    Failed(String),
}

impl CallError {
    fn into_message(self) -> String {
        match self {
            Self::Shutdown => "shutdown".to_string(),
            Self::TimedOut(message) | Self::Failed(message) => message,
        }
    }
}

/// What one page of the sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
struct PageOutcome {
    found: u64,
    deleted: u64,
    delete_failures: u64,
    first_failure: Option<String>,
    /// A store call timed out. The pass stops, so a hung store costs one
    /// timeout and not one for each blob.
    timed_out: bool,
}

/// The inputs that one sweep page shares.
struct PageSweep<'a> {
    store: &'a dyn PayloadStore,
    pools: &'a ShardedDbPool,
    cutoff: DateTime<Utc>,
    dry_run: bool,
    /// The longest that one store call may take.
    call_timeout: Duration,
    shutdown: &'a CancellationToken,
}

impl PageSweep<'_> {
    /// Run a store call. It fails on timeout and on shutdown, so a hung store
    /// cannot hold the tick or its fence guards.
    async fn call<T>(&self, call: PayloadStoreFuture<'_, T>) -> Result<T, CallError> {
        tokio::select! {
            () = self.shutdown.cancelled() => Err(CallError::Shutdown),
            result = tokio::time::timeout(self.call_timeout, call) => match result {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(err)) => Err(CallError::Failed(err.to_string())),
                Err(_) => Err(CallError::TimedOut(format!(
                    "timed out after {}s",
                    self.call_timeout.as_secs()
                ))),
            },
        }
    }

    /// Whether the store still lists `key` as older than the cutoff.
    ///
    /// A writer that uploads the same key again resets its creation time. The
    /// page holds the old time. This read happens just before the delete, so
    /// it sees a re-upload that happened since the page was listed.
    async fn still_old(&self, after: Option<&str>, key: &str) -> Result<bool, CallError> {
        let page = self.call(self.store.list(after, self.cutoff, 1)).await?;
        Ok(page
            .and_then(|page| page.blobs.into_iter().next())
            .is_some_and(|blob| blob.key == key && blob.created_at < self.cutoff))
    }

    /// Find and delete the orphans of one listing page.
    ///
    /// `after` is the cursor that produced `blobs`.
    async fn sweep(
        &self,
        after: Option<&str>,
        blobs: &[StoredBlob],
    ) -> Result<PageOutcome, String> {
        // A store can ignore `created_before`, so filter again here.
        let old_keys: Vec<String> = blobs
            .iter()
            .filter(|blob| blob.created_at < self.cutoff)
            .map(|blob| blob.key.clone())
            .collect();
        if old_keys.is_empty() {
            return Ok(PageOutcome::default());
        }
        let referenced = referenced_keys(self.pools, &old_keys).await?;
        let orphans = select_orphans(blobs, self.cutoff, &referenced);
        let mut outcome = PageOutcome {
            found: orphans.len() as u64,
            ..PageOutcome::default()
        };
        if self.dry_run {
            return Ok(outcome);
        }
        for key in &orphans {
            if self.shutdown.is_cancelled() {
                break;
            }
            // The listing of this key precedes it in the page, or is the cursor.
            let before = old_keys
                .iter()
                .position(|k| k == key)
                .and_then(|at| at.checked_sub(1))
                .map(|at| old_keys[at].as_str())
                .or(after);
            match self.delete_if_orphan(before, key).await {
                Ok(true) => outcome.deleted += 1,
                Ok(false) => {}
                // Shutdown is not a store failure.
                Err(CallError::Shutdown) => break,
                Err(err) => {
                    let timed_out = matches!(err, CallError::TimedOut(_));
                    let message = err.into_message();
                    outcome.delete_failures += 1;
                    tracing::debug!(blob_key = %key, error = %message, "harvest orphan-blob delete failed");
                    outcome.first_failure.get_or_insert(message);
                    if timed_out {
                        outcome.timed_out = true;
                        break;
                    }
                }
            }
        }
        Ok(outcome)
    }

    /// Delete `key` when it is still an orphan. Returns `false` when it is not.
    ///
    /// Two checks run just before the delete. A writer can commit a reference
    /// after the page check. A writer can also upload the key again, which
    /// resets its creation time. Neither check closes the window completely,
    /// because a delete is not conditional. The window is one store round trip.
    async fn delete_if_orphan(&self, before: Option<&str>, key: &str) -> Result<bool, CallError> {
        let one = [key.to_string()];
        if !referenced_keys(self.pools, &one)
            .await
            .map_err(CallError::Failed)?
            .is_empty()
        {
            return Ok(false);
        }
        if !self.still_old(before, key).await? {
            return Ok(false);
        }
        self.call(self.store.delete(key)).await?;
        Ok(true)
    }
}

/// Record the metrics and logs of one swept page.
fn report_page(outcome: &PageOutcome, dry_run: bool, metrics: &dyn MetricsRecorder) {
    if *outcome != PageOutcome::default() {
        metrics.record_orphan_blob_sweep(outcome.found, outcome.deleted, outcome.delete_failures);
        tracing::info!(
            found = outcome.found,
            deleted = outcome.deleted,
            delete_failures = outcome.delete_failures,
            dry_run,
            "harvest orphan-blob sweep page"
        );
    }
    if let Some(error) = &outcome.first_failure {
        tracing::warn!(
            delete_failures = outcome.delete_failures,
            first_error = %error,
            "harvest orphan-blob delete failed"
        );
    }
}

/// The state that the orphan sweep keeps between ticks.
#[derive(Debug, Default)]
pub struct OrphanBlobSweeper {
    /// The listing cursor. `None` starts at the first key.
    cursor: Option<String>,
    /// Set when the store cannot list. The sweep then stays off.
    unsupported: bool,
    /// Set after the sweep logs that it is on.
    announced: bool,
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
        let sweep = PageSweep {
            store: offloader.store().as_ref(),
            pools,
            cutoff,
            dry_run: config.dry_run,
            call_timeout: config.archival_timeout(),
            shutdown,
        };
        let limit = config.batch_size.max(1);
        for _ in 0..MAX_PAGES_PER_TICK {
            if shutdown.is_cancelled() {
                return;
            }
            let after = self.cursor.clone();
            let listed = sweep
                .call(sweep.store.list(after.as_deref(), cutoff, limit))
                .await
                .map_err(CallError::into_message);
            let mut page = match listed {
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
            if !self.announced {
                self.announced = true;
                tracing::info!(
                    store_id = offloader.store_id(),
                    grace_secs = grace.as_secs(),
                    dry_run = config.dry_run,
                    "harvest orphan-blob sweep is on"
                );
            }
            // A store must honour `limit`. Do not trust it.
            page.blobs.truncate(limit);
            match sweep.sweep(after.as_deref(), &page.blobs).await {
                Ok(outcome) => {
                    report_page(&outcome, config.dry_run, metrics);
                    // A hung store ends the pass. The page keeps its cursor.
                    if outcome.timed_out {
                        return;
                    }
                }
                Err(error) => {
                    // The cursor stays, so the next tick retries this page.
                    tracing::warn!(error = %error, "harvest orphan-blob sweep page failed");
                    return;
                }
            }
            // A page that shutdown cut short keeps its cursor.
            if shutdown.is_cancelled() {
                return;
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
        assert_eq!(
            select_orphans(&[at], cutoff, &HashSet::new()),
            Vec::<String>::new()
        );
    }
}
