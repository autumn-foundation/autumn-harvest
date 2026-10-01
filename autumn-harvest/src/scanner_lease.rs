//! One active background scanner per shard (issue #1795).

use std::time::Duration;

/// How replicas share one per-shard background scanner.
#[derive(Debug, Clone, PartialEq)]
pub struct ScannerCoordination {
    /// The lease holder id, unique per worker. `None` turns election off.
    pub holder: Option<String>,
    /// How long a lease lasts without renewal.
    pub lease_ttl: Duration,
    /// Random spread of each sleep, as a fraction of the interval.
    pub jitter: f64,
}
