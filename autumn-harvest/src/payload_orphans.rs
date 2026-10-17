//! Sweep of payload blobs that no execution references (issue #2123).
//!
//! A write uploads its offloaded payloads before it opens its transaction.
//! A rollback after the upload leaves blobs with no `harvest_payload_refs` row.
//! Retention finds blobs only through those rows, so it never deletes them.
//!
//! **Why not delete on rollback?** A content-addressed store can give the same
//! key to a committed run. A concurrent writer can commit a reference to that
//! key just after any check. The sweep closes the race with a grace period:
//! a blob that a writer just uploaded is too young to delete.
//!
//! **Safety rules.**
//! - The sweep deletes a blob only when it is older than the grace period.
//!   It checks the age itself and does not trust the store to filter.
//! - It deletes a blob only when no shard references it. It checks every
//!   physical database.
//! - A failed reference check stops the sweep. It never deletes on a guess.
//! - The store must hold only harvest blobs. See [`PayloadStore::list`].
//!
//! **Limits.**
//! - The sweep checks only the databases in its own pool set. Give every
//!   retention process the full shard list. A ref on an unlisted database is
//!   invisible.
//! - A re-upload of identical bytes after the listing can race a plain
//!   delete. [`PayloadStore::delete_if_older`] closes the race with an atomic
//!   conditional delete. A store without it keeps the race open for one page of
//!   reference checks and deletes. The `list` contract requires `put` to
//!   refresh `created_at`, which closes the race for earlier uploads.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::error::HarvestResult;
use crate::payload_store::{PayloadStore, StoredBlob};
use crate::shard::ShardedDbPool;

/// Settings for one [`sweep_orphaned_blobs`] call.
#[derive(Debug, Clone)]
pub struct OrphanSweepOptions {
    /// A blob younger than this is never deleted.
    pub grace: Duration,
    /// The most pages one call reads.
    pub page_budget: usize,
    /// Count orphans and delete nothing.
    pub dry_run: bool,
    /// Where the previous call stopped, or `None` to start from the top.
    pub cursor: Option<String>,
}

/// What one [`sweep_orphaned_blobs`] call did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrphanSweepReport {
    /// Blobs that were old enough and that no shard references.
    pub found: u64,
    /// Blobs the call deleted.
    pub deleted: u64,
    /// Blobs whose delete failed.
    pub failed: u64,
    /// Where the next call resumes, or `None` when the listing is complete.
    pub next_cursor: Option<String>,
    /// The error that stopped the call early.
    pub error: Option<String>,
}

/// The result of [`sweep_orphaned_blobs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanSweep {
    /// The store cannot list, so the sweep is off for it.
    Unsupported,
    /// The sweep ran.
    Swept(OrphanSweepReport),
}

/// Delete the blobs in `store` that no execution on any shard references.
///
/// Reads at most [`OrphanSweepOptions::page_budget`] pages. Resume from
/// [`OrphanSweepReport::next_cursor`] until it is `None`. A cancelled
/// `shutdown` stops the call between pages.
pub async fn sweep_orphaned_blobs(
    store: &dyn PayloadStore,
    pools: &ShardedDbPool,
    options: &OrphanSweepOptions,
    shutdown: &CancellationToken,
) -> OrphanSweep {
    let mut report = OrphanSweepReport {
        next_cursor: options.cursor.clone(),
        ..OrphanSweepReport::default()
    };
    let Some(cutoff) = chrono::Duration::from_std(options.grace)
        .ok()
        .and_then(|grace| Utc::now().checked_sub_signed(grace))
    else {
        report.error = Some("the grace period is out of range".to_owned());
        return OrphanSweep::Swept(report);
    };
    for _ in 0..options.page_budget {
        if shutdown.is_cancelled() {
            break;
        }
        let Some(listing) = store.list("", cutoff, report.next_cursor.as_deref()) else {
            return OrphanSweep::Unsupported;
        };
        let page = match listing.await {
            Ok(page) => page,
            Err(error) => {
                report.error = Some(error.to_string());
                return OrphanSweep::Swept(report);
            }
        };
        if let Err(error) =
            sweep_page(store, pools, cutoff, &page.blobs, options, &mut report).await
        {
            report.error = Some(error.to_string());
            return OrphanSweep::Swept(report);
        }
        report.next_cursor = page.next;
        if report.next_cursor.is_none() {
            break;
        }
    }
    OrphanSweep::Swept(report)
}

/// Sweep one page. A reference check that fails returns an error before any
/// delete, so a page is deleted whole or not at all.
async fn sweep_page(
    store: &dyn PayloadStore,
    pools: &ShardedDbPool,
    cutoff: DateTime<Utc>,
    blobs: &[StoredBlob],
    options: &OrphanSweepOptions,
    report: &mut OrphanSweepReport,
) -> HarvestResult<()> {
    let old: Vec<String> = blobs
        .iter()
        .filter(|blob| blob.created_at < cutoff)
        .map(|blob| blob.key.clone())
        .collect();
    let referenced = referenced_on_any_shard(pools, &old).await?;
    for key in old.iter().filter(|key| !referenced.contains(*key)) {
        report.found += 1;
        if options.dry_run {
            continue;
        }
        // A conditional delete skips a blob that a writer re-uploaded after
        // the listing. A store without one falls back to a plain delete.
        let outcome = match store.delete_if_older(key, cutoff) {
            Some(delete) => delete.await,
            None => store.delete(key).await.map(|()| true),
        };
        match outcome {
            Ok(true) => report.deleted += 1,
            Ok(false) => {}
            Err(error) => {
                report.failed += 1;
                tracing::warn!(blob_key = %key, error = %error, "harvest orphan blob delete failed");
            }
        }
    }
    Ok(())
}

/// The subset of `keys` that a `harvest_payload_refs` row names on any
/// physical database. Aliased shards share a database, so one check covers
/// them.
async fn referenced_on_any_shard(
    pools: &ShardedDbPool,
    keys: &[String],
) -> HarvestResult<HashSet<String>> {
    let mut referenced = HashSet::new();
    if keys.is_empty() {
        return Ok(referenced);
    }
    for (pool, _shards) in pools.pool_groups() {
        let mut conn = crate::replication::fenced_checkout(pool).await?;
        referenced.extend(crate::store::batch_blob_keys_still_referenced(&mut conn, keys).await?);
    }
    Ok(referenced)
}
