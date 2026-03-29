//! Database pool configuration with separate pools and shared ceiling.
//!
//! Design Decision DD-2: Two `deadpool` instances — one for the web server,
//! one for Harvest workers — share a `max_total_connections` ceiling so the
//! combined pool sizes never exceed Postgres `max_connections`.
//!
//! If the sum of requested sizes exceeds the ceiling, both are scaled down
//! proportionally, with any remainder awarded to the web pool (HTTP latency
//! is more user-visible than worker throughput).

use crate::error::{HarvestError, HarvestResult};

/// Configuration for the Harvest worker database pool.
///
/// The web pool size is configured separately (via `autumn-web`); this struct
/// controls the worker side and the shared ceiling that constrains both.
#[derive(Debug, Clone)]
pub struct HarvestPoolConfig {
    /// Number of connections reserved for Harvest workers.
    pub worker_pool_size: usize,
    /// Maximum combined connections across both pools.
    ///
    /// Default: 95 (i.e. `pg max_connections` 100 minus 5 for superuser/admin).
    pub max_total_connections: usize,
}

impl Default for HarvestPoolConfig {
    fn default() -> Self {
        Self {
            worker_pool_size: 10,
            max_total_connections: 95,
        }
    }
}

impl HarvestPoolConfig {
    /// Validate the pool configuration against a given web pool size.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if:
    /// - `worker_pool_size` is zero
    /// - `web_pool_size` is zero
    ///
    /// Logs a warning (via `tracing`) if the combined sizes exceed the ceiling
    /// but does not reject — [`compute_pool_sizes`] will scale them down.
    pub fn validate(&self, web_pool_size: usize) -> HarvestResult<()> {
        if self.worker_pool_size == 0 {
            return Err(HarvestError::Config(
                "worker_pool_size must be at least 1".into(),
            ));
        }
        if web_pool_size == 0 {
            return Err(HarvestError::Config(
                "web_pool_size must be at least 1".into(),
            ));
        }

        let combined = web_pool_size + self.worker_pool_size;
        if combined > self.max_total_connections {
            tracing::warn!(
                web_pool_size,
                worker_pool_size = self.worker_pool_size,
                max_total_connections = self.max_total_connections,
                "combined pool sizes ({combined}) exceed ceiling; pools will be scaled down"
            );
        }

        Ok(())
    }
}

/// Scale two pool sizes so their sum does not exceed `ceiling`.
///
/// If `requested_web + requested_worker <= ceiling`, both values are returned
/// unchanged. Otherwise they are scaled down proportionally, with any integer
/// remainder awarded to the web pool (prioritising HTTP latency).
///
/// Both returned values are guaranteed to be at least 1.
#[must_use]
pub fn compute_pool_sizes(
    requested_web: usize,
    requested_worker: usize,
    ceiling: usize,
) -> (usize, usize) {
    // Guarantee at least 1 each — clamp inputs.
    let requested_web = requested_web.max(1);
    let requested_worker = requested_worker.max(1);
    let ceiling = ceiling.max(2); // need room for at least 1 + 1

    let combined = requested_web + requested_worker;
    if combined <= ceiling {
        return (requested_web, requested_worker);
    }

    // Scale proportionally using integer arithmetic to avoid cast warnings.
    // worker gets floor(ceiling * requested_worker / combined), minimum 1.
    let mut scaled_worker = (ceiling * requested_worker / combined).max(1);
    // web gets the rest, minimum 1.
    let mut scaled_web = ceiling.saturating_sub(scaled_worker).max(1);

    // If rounding pushed us over ceiling, trim worker (prioritise web).
    if scaled_web + scaled_worker > ceiling {
        scaled_worker = ceiling.saturating_sub(scaled_web).max(1);
        scaled_web = ceiling.saturating_sub(scaled_worker);
    }

    (scaled_web, scaled_worker)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_config_default_values() {
        let cfg = HarvestPoolConfig::default();
        assert_eq!(cfg.worker_pool_size, 10);
        assert_eq!(cfg.max_total_connections, 95);
    }

    #[test]
    fn pool_config_validates_ceiling() {
        let cfg = HarvestPoolConfig {
            worker_pool_size: 10,
            max_total_connections: 95,
        };
        // combined = 20 + 10 = 30 < 95 => ok
        assert!(cfg.validate(20).is_ok());
    }

    #[test]
    fn pool_config_rejects_zero_worker_pool() {
        let cfg = HarvestPoolConfig {
            worker_pool_size: 0,
            max_total_connections: 95,
        };
        let err = cfg.validate(20).unwrap_err();
        assert!(err.to_string().contains("worker_pool_size"));
    }

    #[test]
    fn pool_config_rejects_zero_web_pool() {
        let cfg = HarvestPoolConfig::default();
        let err = cfg.validate(0).unwrap_err();
        assert!(err.to_string().contains("web_pool_size"));
    }

    #[test]
    fn pool_sizes_respect_ceiling() {
        // 60 + 60 = 120 > 100 ceiling => must scale down
        let (web, worker) = compute_pool_sizes(60, 60, 100);
        assert!(
            web + worker <= 100,
            "combined {web} + {worker} = {} exceeds ceiling 100",
            web + worker
        );
        assert!(web >= 1);
        assert!(worker >= 1);
    }

    #[test]
    fn pool_sizes_unchanged_under_ceiling() {
        let (web, worker) = compute_pool_sizes(20, 10, 95);
        assert_eq!(web, 20);
        assert_eq!(worker, 10);
    }

    #[test]
    fn pool_sizes_remainder_goes_to_web() {
        // 70 + 30 = 100, ceiling = 50 => scale down
        // ratio = 0.5 => web ~35, worker ~15, but floor could leave remainder
        let (web, worker) = compute_pool_sizes(70, 30, 50);
        assert!(web + worker <= 50);
        assert!(web >= 1);
        assert!(worker >= 1);
        // web should get at least as much proportion as worker
        assert!(web >= worker, "web ({web}) should be >= worker ({worker})");
    }

    #[test]
    fn pool_sizes_extreme_imbalance() {
        // 1 web + 99 worker, ceiling 10
        let (web, worker) = compute_pool_sizes(1, 99, 10);
        assert!(web + worker <= 10);
        assert!(web >= 1);
        assert!(worker >= 1);
    }

    #[test]
    fn pool_sizes_exact_fit() {
        let (web, worker) = compute_pool_sizes(50, 50, 100);
        assert_eq!(web, 50);
        assert_eq!(worker, 50);
    }

    #[test]
    fn pool_sizes_minimum_guarantee() {
        // Both request 0 => clamped to 1 each, ceiling 2
        let (web, worker) = compute_pool_sizes(0, 0, 2);
        assert!(web >= 1);
        assert!(worker >= 1);
    }
}
