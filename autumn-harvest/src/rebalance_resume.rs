//! Automatic resume of a shard rebalance that stalls after its cutover
//! (issue #1839).
//!
//! A shard migration seals the source in one commit, the cutover. A second
//! step on the target, the activation, makes the run claimable again. A crash
//! between the two leaves the run claimable on neither shard. Before issue
//! #1839, only a manual `harvest shard rebalance-resume` closed that gap.
//!
//! This scanner closes it automatically. Each worker runs one scanner per
//! assigned shard, unless `ScannerConfig::rebalance_resume_enabled` is
//! `false`. A pass calls
//! [`crate::shard_rebalance::resume_stalled_cutovers`], which settles each
//! `COMMITTED` record older than a grace period and writes a
//! `shard.rebalance.auto_resume` audit row.
//!
//! The scanner does not use a scanner lease. The pass claims each record in
//! one statement, so many replicas can run it. The claim reads the partial
//! index of unsettled records. That index is empty when no rebalance runs.
//! During a drain it also holds the records before the cutover, which the
//! claim reads and skips.

use std::time::Duration;

/// Settings for one rebalance-resume scanner.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RebalanceResumeConfig {
    /// Time between passes. Kept within
    /// [`crate::scanner_lease::MIN_SCANNER_INTERVAL`] and
    /// [`crate::scanner_lease::MAX_SCANNER_INTERVAL`].
    pub interval: Duration,
    /// Age of a `COMMITTED` record before the scanner settles it. Raised to
    /// at least [`crate::scanner_lease::MIN_REBALANCE_STALL_AFTER`].
    pub stall_after: Duration,
    /// Most records that one pass settles. At least 1.
    pub batch_size: u32,
    /// Random spread of each sleep, as a fraction of the interval. Clamped
    /// like [`crate::scanner_lease::ScannerConfig::jitter`].
    pub jitter: f64,
}

/// Default for [`RebalanceResumeConfig::batch_size`].
pub const DEFAULT_REBALANCE_RESUME_BATCH_SIZE: u32 = 100;

impl RebalanceResumeConfig {
    /// The settings that a worker derives from its scanner config.
    #[must_use]
    pub const fn from_scanner_config(scanner: &crate::scanner_lease::ScannerConfig) -> Self {
        Self {
            interval: scanner.rebalance_resume_interval,
            stall_after: scanner.rebalance_stall_after,
            batch_size: DEFAULT_REBALANCE_RESUME_BATCH_SIZE,
            jitter: scanner.jitter,
        }
    }
}

/// Spawn the rebalance-resume scanner for one source shard.
///
/// `pool` must reach every shard that a migration from `shard` can target.
/// The loop stops when `cancel` fires, also during a pass. A dropped pass is
/// safe: each step is idempotent, and an open transaction makes the pool
/// discard its connection.
#[cfg(feature = "db")]
#[must_use = "dropping the handle detaches the scanner; join it at shutdown"]
pub fn spawn_rebalance_resume_scanner(
    pool: crate::shard::ShardedDbPool,
    shard: crate::types::ShardId,
    config: RebalanceResumeConfig,
    cancel: tokio_util::sync::CancellationToken,
    telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
) -> tokio::task::JoinHandle<()> {
    let interval = crate::scanner_lease::scanner_interval(config.interval);
    let limit = i64::from(config.batch_size.max(1));
    // Register before the first tick, so `scanner_liveness` expects this loop
    // and gives it boot grace (issue #797). The longest sleep is the period.
    let owner = crate::scanner_health::register_scanner_for_shard(
        &*telemetry.metrics,
        crate::scanner_health::Scanner::RebalanceResume,
        crate::scanner_lease::max_jittered_interval(interval, config.jitter),
        Some(shard),
    );
    crate::dispatch::spawn_bound(async move {
        loop {
            // Jitter keeps the replicas of a restarted fleet out of step.
            let sleep =
                crate::scanner_lease::jittered_interval(interval, config.jitter, rand::random());
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(sleep) => {}
            }

            // Selected against `cancel` (issue #1426). A pool can have no
            // checkout timeout, so a full pool could block shutdown here.
            let pass = tokio::select! {
                () = cancel.cancelled() => break,
                result = crate::shard_rebalance::resume_stalled_cutovers(
                    &pool,
                    shard,
                    config.stall_after,
                    limit,
                ) => result,
            };
            match pass {
                Ok(outcomes) => {
                    for outcome in &outcomes {
                        report(shard, outcome);
                    }
                }
                Err(error) => {
                    tracing::error!(
                        shard = shard.as_i32(),
                        error = %error,
                        "rebalance-resume pass failed"
                    );
                }
            }

            crate::scanner_health::record_scanner_tick(&*telemetry.metrics, owner);
        }
        // A graceful stop removes this loop from the expected set. A panic
        // skips this line, so a panicked loop ages into `Wedged`.
        crate::scanner_health::deregister_scanner(owner);
    })
}

/// Log one settled or failed record.
#[cfg(feature = "db")]
fn report(shard: crate::types::ShardId, outcome: &crate::shard_rebalance::MigrationOutcome) {
    use crate::shard_rebalance::MigrationOutcome;
    match outcome {
        MigrationOutcome::Migrated { execution_id, .. } => tracing::warn!(
            shard = shard.as_i32(),
            execution_id = %execution_id,
            "resumed a shard migration that stalled after its cutover"
        ),
        MigrationOutcome::Aborted {
            execution_id,
            reason,
        } => tracing::error!(
            shard = shard.as_i32(),
            execution_id = %execution_id,
            reason = %reason,
            "could not resume a shard migration that stalled after its cutover"
        ),
        MigrationOutcome::WouldMigrate { .. } | MigrationOutcome::Skipped { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner_lease::{
        DEFAULT_REBALANCE_RESUME_INTERVAL, DEFAULT_REBALANCE_STALL_AFTER, ScannerConfig,
    };

    #[test]
    fn the_worker_settings_come_from_the_scanner_config() {
        let defaults = RebalanceResumeConfig::from_scanner_config(&ScannerConfig::default());
        assert_eq!(defaults.interval, DEFAULT_REBALANCE_RESUME_INTERVAL);
        assert_eq!(defaults.stall_after, DEFAULT_REBALANCE_STALL_AFTER);
        assert_eq!(defaults.batch_size, DEFAULT_REBALANCE_RESUME_BATCH_SIZE);

        let tuned = RebalanceResumeConfig::from_scanner_config(&ScannerConfig {
            rebalance_resume_interval: Duration::from_millis(200),
            rebalance_stall_after: Duration::ZERO,
            ..ScannerConfig::default()
        });
        assert_eq!(tuned.interval, Duration::from_millis(200));
        assert_eq!(tuned.stall_after, Duration::ZERO);
    }

    #[test]
    fn the_grace_period_has_a_floor() {
        use crate::scanner_lease::{MIN_REBALANCE_STALL_AFTER, rebalance_stall_after};
        assert_eq!(
            rebalance_stall_after(Duration::ZERO),
            MIN_REBALANCE_STALL_AFTER
        );
        assert_eq!(
            rebalance_stall_after(DEFAULT_REBALANCE_STALL_AFTER),
            DEFAULT_REBALANCE_STALL_AFTER
        );
    }

    #[test]
    fn the_scanner_is_on_by_default() {
        assert!(ScannerConfig::default().rebalance_resume_enabled);
    }

    #[test]
    fn the_default_grace_outlasts_several_passes() {
        // A live CLI activates within milliseconds. The grace period must
        // leave it many passes to do so before the scanner acts.
        assert!(DEFAULT_REBALANCE_STALL_AFTER >= DEFAULT_REBALANCE_RESUME_INTERVAL * 3);
    }
}
