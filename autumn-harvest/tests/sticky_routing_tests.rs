//! Unit tests for the sticky cross-worker routing feature (issue #235).
//!
//! These tests verify the public API surface introduced for opt-in sticky routing:
//! `StickyRoutingConfig`, `WorkerConfig::with_sticky_routing`, the two new metric
//! constants, and the corresponding `MetricsRecorder` trait methods.
//!
//! The integration section at the bottom uses a `CountingMetrics` recorder and
//! `WorkflowCache` directly to simulate the cache hit/miss lifecycle without
//! a database — demonstrating the warm-path optimisation that is the core
//! deliverable of issue #235.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_harvest::builder::{StickyRoutingConfig, WorkerConfig};
use autumn_harvest::cache::{CachedWorkflowState, WorkflowCache};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::telemetry::{
    METRIC_WORKFLOW_CACHE_HIT, METRIC_WORKFLOW_CACHE_MISS, MetricsRecorder, NoOpMetrics,
};

// ---------------------------------------------------------------------------
// CountingMetrics — a MetricsRecorder that accumulates hit/miss counts.
// Used by the integration tests below to assert that the correct metric is
// emitted on each task cycle.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct CountingMetrics {
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl CountingMetrics {
    fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::SeqCst)
    }
    fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::SeqCst)
    }
}

impl MetricsRecorder for CountingMetrics {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }
    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }
}

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
        METRIC_WORKFLOW_CACHE_HIT, "harvest.workflow.cache_hit",
        "cache hit metric must follow harvest.<noun>.<instrument> naming"
    );
}

#[test]
fn cache_miss_metric_constant_has_correct_name() {
    assert_eq!(
        METRIC_WORKFLOW_CACHE_MISS, "harvest.workflow.cache_miss",
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

// ---------------------------------------------------------------------------
// Cache lifecycle integration tests (no DB required).
//
// These tests simulate the cache hit/miss lifecycle that the worker hot path
// executes on every task cycle:
//
//   Task 1 (fresh execution):
//     cache.get() → None → cold full-history load → record_miss
//     workflow suspends → cache.insert(exec_id, snapshot)
//
//   Task 2 (same execution on same worker):
//     cache.get() → Some → delta load → record_hit
//     workflow suspends again → cache.insert(exec_id, updated_snapshot)
//
//   Task 3 (terminal outcome):
//     cache.get() → Some → delta load → record_hit
//     workflow completes → cache.remove(exec_id)
//
// The "3-worker scenario" from the issue acceptance criteria is modelled as
// three independent caches: only the cache that saw Task 1 produces a hit
// on Task 2.  The other two caches each produce a miss, matching the
// expected distribution (miss_count ≤ 1 on the owning worker; miss_count = 1
// on all others).
// ---------------------------------------------------------------------------

/// Helper that models a single task cycle on one worker's local cache:
///   1. Check cache (hit/miss).
///   2. Emit the appropriate metric.
///   3. Update the cache based on the simulated outcome.
fn simulate_task_cycle(
    cache: &mut WorkflowCache,
    metrics: &CountingMetrics,
    exec_id: uuid::Uuid,
    suspends: bool, // true → Suspended (cache insert), false → Completed (cache evict)
    events_after: Vec<WorkflowEvent>,
    next_event_id: i32,
) -> bool {
    let is_hit = cache.get(&exec_id).is_some();
    if is_hit {
        metrics.record_workflow_cache_hit("test_workflow", "default");
    } else {
        metrics.record_workflow_cache_miss("test_workflow", "default");
    }
    if suspends {
        cache.insert(
            exec_id,
            CachedWorkflowState {
                events: events_after,
                next_event_id,
            },
        );
    } else {
        cache.remove(&exec_id);
    }
    is_hit
}

#[test]
fn first_task_is_cache_miss_subsequent_tasks_on_same_worker_are_hits() {
    let metrics = CountingMetrics::default();
    let mut cache = WorkflowCache::new(64);
    let exec_id = uuid::Uuid::new_v4();

    // Task 1: cold path — execution not in cache yet.
    let hit = simulate_task_cycle(
        &mut cache,
        &metrics,
        exec_id,
        true,
        vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: chrono::Utc::now(),
        }],
        2,
    );
    assert!(!hit, "first task must be a cache miss");
    assert_eq!(metrics.miss_count(), 1);
    assert_eq!(metrics.hit_count(), 0);

    // Task 2: warm path — cache was populated by Task 1's suspension.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_id, true, vec![], 4);
    assert!(hit, "second task on same worker must be a cache hit");
    assert_eq!(metrics.miss_count(), 1);
    assert_eq!(metrics.hit_count(), 1);

    // Task 3: warm path again before terminal completion.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_id, false, vec![], 6);
    assert!(hit, "third task on same worker must be a cache hit");
    assert_eq!(metrics.miss_count(), 1, "miss count must stay at 1");
    assert_eq!(metrics.hit_count(), 2);

    // After completion the entry must be evicted.
    assert!(
        cache.get(&exec_id).is_none(),
        "completed execution must be evicted"
    );
}

#[test]
fn three_worker_scenario_only_owning_worker_gets_cache_hits() {
    // Simulate a 3-worker fleet.  Each worker has its own independent cache.
    // Execution is "sticky" to worker-0: it is the only one that retains the
    // warm cache entry, so it is the only one that records hits on follow-up tasks.
    let metrics_w0 = CountingMetrics::default();
    let metrics_w1 = CountingMetrics::default();
    let metrics_w2 = CountingMetrics::default();

    let mut cache_w0 = WorkflowCache::new(64);
    let mut cache_w1 = WorkflowCache::new(64);
    let mut cache_w2 = WorkflowCache::new(64);

    let exec_id = uuid::Uuid::new_v4();

    // --- Round 1: task lands on worker-0 (cold path) ---
    simulate_task_cycle(&mut cache_w0, &metrics_w0, exec_id, true, vec![], 2);
    assert_eq!(metrics_w0.miss_count(), 1);
    assert_eq!(metrics_w0.hit_count(), 0);

    // --- Round 2: follow-up task lands on worker-0 (warm path) ---
    simulate_task_cycle(&mut cache_w0, &metrics_w0, exec_id, true, vec![], 4);
    assert_eq!(metrics_w0.miss_count(), 1, "owner: still only 1 miss");
    assert_eq!(metrics_w0.hit_count(), 1, "owner: first hit");

    // --- Hypothetical: same task lands on worker-1 instead (miss, no cache) ---
    simulate_task_cycle(&mut cache_w1, &metrics_w1, exec_id, true, vec![], 4);
    assert_eq!(metrics_w1.miss_count(), 1, "w1 has no cache entry: miss");
    assert_eq!(metrics_w1.hit_count(), 0);

    // --- Hypothetical: same task lands on worker-2 instead (miss, no cache) ---
    simulate_task_cycle(&mut cache_w2, &metrics_w2, exec_id, true, vec![], 4);
    assert_eq!(metrics_w2.miss_count(), 1, "w2 has no cache entry: miss");
    assert_eq!(metrics_w2.hit_count(), 0);

    // Workers w1 and w2 each got exactly 1 miss (the full reload).
    // Worker w0 (the sticky owner) accumulated 1 miss + 1 hit.
    // Total hit rate on w0 after two tasks = 50%; with N follow-up tasks → approaches 100%.
    assert_eq!(metrics_w0.miss_count() + metrics_w0.hit_count(), 2);
    assert_eq!(metrics_w1.miss_count() + metrics_w1.hit_count(), 1);
    assert_eq!(metrics_w2.miss_count() + metrics_w2.hit_count(), 1);
}

#[test]
fn cache_eviction_by_lru_pressure_causes_miss_on_next_task() {
    let metrics = CountingMetrics::default();
    let mut cache = WorkflowCache::new(2); // tiny cache to force eviction
    let exec_a = uuid::Uuid::new_v4();
    let exec_b = uuid::Uuid::new_v4();
    let exec_c = uuid::Uuid::new_v4();

    // Populate cache: A first, then B.  A is now LRU, B is MRU.
    simulate_task_cycle(&mut cache, &metrics, exec_a, true, vec![], 2);
    simulate_task_cycle(&mut cache, &metrics, exec_b, true, vec![], 2);
    assert_eq!(cache.len(), 2);

    // Touch A to make it MRU.  B is now LRU.
    assert!(cache.get(&exec_a).is_some());

    // Insert C — cache is full, so B (the LRU) is evicted.
    simulate_task_cycle(&mut cache, &metrics, exec_c, true, vec![], 2);
    assert_eq!(cache.len(), 2);

    // A is still present (was MRU before the eviction).
    assert!(cache.get(&exec_a).is_some(), "exec_a should be in cache");
    // C was just inserted.
    assert!(cache.get(&exec_c).is_some(), "exec_c should be in cache");

    // B follow-up: evicted → miss even though same worker.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_b, true, vec![], 4);
    assert!(!hit, "evicted entry must cause a cache miss");
}

#[test]
fn counting_metrics_tracks_hit_miss_independently() {
    let m = CountingMetrics::default();
    assert_eq!(m.hit_count(), 0);
    assert_eq!(m.miss_count(), 0);

    m.record_workflow_cache_hit("wf", "q");
    m.record_workflow_cache_hit("wf", "q");
    m.record_workflow_cache_miss("wf", "q");

    assert_eq!(m.hit_count(), 2);
    assert_eq!(m.miss_count(), 1);
}
