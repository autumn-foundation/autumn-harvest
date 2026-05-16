//! Unit tests for the sticky cross-worker routing feature (issue #235).
//!
//! These tests verify the public API surface introduced for opt-in sticky routing:
//! `StickyRoutingConfig`, `WorkerConfig::with_sticky_routing`, the two new metric
//! constants, and the corresponding `MetricsRecorder` trait methods.
//!
//! All tests here are pure-logic unit tests — no database required.

use autumn_harvest::builder::{StickyRoutingConfig, WorkerConfig};
use autumn_harvest::telemetry::{
    METRIC_WORKFLOW_CACHE_HIT, METRIC_WORKFLOW_CACHE_MISS, MetricsRecorder, NoOpMetrics,
};
use std::time::Duration;

// ---------------------------------------------------------------------------
// StickyRoutingConfig
// ---------------------------------------------------------------------------

#[test]
fn sticky_routing_config_can_be_constructed() {
    let config = StickyRoutingConfig {
        lease_ttl: Duration::from_secs(10),
    };
    assert_eq!(config.lease_ttl, Duration::from_secs(10));
}

#[test]
fn sticky_routing_config_zero_lease_ttl_is_valid() {
    let config = StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    };
    assert!(config.lease_ttl.is_zero());
}

// ---------------------------------------------------------------------------
// WorkerConfig default + with_sticky_routing
// ---------------------------------------------------------------------------

#[test]
fn worker_config_default_has_sticky_routing_disabled() {
    let config = WorkerConfig::default();
    assert!(
        config.sticky_timeout.is_zero(),
        "sticky routing must be off by default; got sticky_timeout={:?}",
        config.sticky_timeout
    );
}

#[test]
fn with_sticky_routing_enables_the_lease_ttl() {
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::from_secs(10),
    });
    assert_eq!(
        config.sticky_timeout,
        Duration::from_secs(10),
        "with_sticky_routing should set sticky_timeout to lease_ttl"
    );
}

#[test]
fn with_sticky_routing_can_be_called_multiple_times_and_last_wins() {
    let config = WorkerConfig::default()
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(5),
        })
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(15),
        });
    assert_eq!(config.sticky_timeout, Duration::from_secs(15));
}

#[test]
fn with_sticky_routing_zero_lease_ttl_disables_sticky() {
    let config = WorkerConfig::default()
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(10),
        })
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::ZERO,
        });
    assert!(
        config.sticky_timeout.is_zero(),
        "zero lease_ttl should disable sticky routing"
    );
}

// ---------------------------------------------------------------------------
// Metric constants
// ---------------------------------------------------------------------------

#[test]
fn cache_hit_metric_constant_has_correct_name() {
    assert_eq!(
        METRIC_WORKFLOW_CACHE_HIT,
        "harvest.workflow.cache_hit",
        "cache hit metric must follow harvest.<noun>.<instrument> naming"
    );
}

#[test]
fn cache_miss_metric_constant_has_correct_name() {
    assert_eq!(
        METRIC_WORKFLOW_CACHE_MISS,
        "harvest.workflow.cache_miss",
        "cache miss metric must follow harvest.<noun>.<instrument> naming"
    );
}

// ---------------------------------------------------------------------------
// MetricsRecorder trait — default no-op implementations
// ---------------------------------------------------------------------------

#[test]
fn no_op_metrics_implements_cache_hit_without_panic() {
    NoOpMetrics.record_workflow_cache_hit("my_workflow", "default");
}

#[test]
fn no_op_metrics_implements_cache_miss_without_panic() {
    NoOpMetrics.record_workflow_cache_miss("my_workflow", "default");
}
