//! Per-worker outlier (gray-failure) detection (issue #1815).
//!
//! A heartbeat shows that a worker is alive. It does not show that the worker
//! is healthy. A worker can stay alive and still fail a high share of tasks or
//! run them slowly. Huang et al. (HotOS'17) call this a gray failure.
//!
//! Each worker keeps a rolling [`TaskOutcomeWindow`] of its own task outcomes.
//! The heartbeat publishes a [`WorkerTaskStats`] snapshot of that window.
//! [`detect_outliers`] then compares each worker against the median of its
//! peers on two dimensions: task failure ratio and p99 task latency.
//!
//! The comparison uses the **peer** median, not a fixed threshold. A fault
//! that hits the whole fleet moves the median too, so it flags no worker. That
//! is a fleet alert, not a gray failure.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Default number of samples a [`TaskOutcomeWindow`] keeps.
pub const DEFAULT_WINDOW_CAPACITY: usize = 1024;

/// Default maximum sample age in a [`TaskOutcomeWindow`].
pub const DEFAULT_WINDOW_MAX_AGE: Duration = Duration::from_secs(300);

/// One dimension on which a worker can be an outlier.
///
/// It is also the bounded `dimension` label on
/// [`METRIC_WORKER_OUTLIER`](crate::telemetry::METRIC_WORKER_OUTLIER).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutlierDimension {
    /// The share of failed tasks.
    FailureRatio,
    /// The p99 task latency.
    LatencyP99,
}

impl OutlierDimension {
    /// Every dimension, in a stable order.
    pub const ALL: [Self; 2] = [Self::FailureRatio, Self::LatencyP99];

    /// Stable string form, used as a metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailureRatio => "failure_ratio",
            Self::LatencyP99 => "latency_p99",
        }
    }
}

/// A snapshot of one worker's recent task outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTaskStats {
    /// Tasks in the window.
    pub tasks: u32,
    /// Failed tasks in the window.
    pub failures: u32,
    /// The p99 task latency in milliseconds. `None` when the window is empty.
    pub p99_latency_ms: Option<u64>,
}

impl WorkerTaskStats {
    /// The share of failed tasks, from 0.0 to 1.0. `None` with no tasks.
    #[must_use]
    pub fn failure_ratio(&self) -> Option<f64> {
        (self.tasks > 0).then(|| f64::from(self.failures) / f64::from(self.tasks))
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    failed: bool,
    latency: Duration,
}

/// A rolling window of one worker's task outcomes.
///
/// The window drops a sample when it is older than `max_age`, or when the
/// window holds `capacity` newer samples. So a worker that heals stops being
/// an outlier after `max_age` at most.
///
/// The samples stay in time order. Two threads can record out of order, so
/// each insert finds its place by timestamp.
#[derive(Debug)]
pub struct TaskOutcomeWindow {
    samples: Mutex<VecDeque<Sample>>,
    capacity: usize,
    max_age: Duration,
}

impl Default for TaskOutcomeWindow {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW_CAPACITY, DEFAULT_WINDOW_MAX_AGE)
    }
}

impl TaskOutcomeWindow {
    /// Create a window. A `capacity` of 0 becomes 1.
    #[must_use]
    pub fn new(capacity: usize, max_age: Duration) -> Self {
        Self {
            samples: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            max_age,
        }
    }

    /// Record one task outcome that ends now.
    pub fn record(&self, failed: bool, latency: Duration) {
        self.record_at(Instant::now(), failed, latency);
    }

    /// Record one task outcome that ends at `at`.
    ///
    /// A full window evicts its oldest sample, which can be this one.
    pub fn record_at(&self, at: Instant, failed: bool, latency: Duration) {
        let mut samples = self.lock();
        // A late sample lands near the back, so the insert moves few samples.
        let index = samples.partition_point(|s| s.at <= at);
        samples.insert(
            index,
            Sample {
                at,
                failed,
                latency,
            },
        );
        if samples.len() > self.capacity {
            samples.pop_front();
        }
    }

    /// Snapshot the window as it is now.
    #[must_use]
    pub fn snapshot(&self) -> WorkerTaskStats {
        self.snapshot_at(Instant::now())
    }

    /// Snapshot the window as it is at `now`.
    #[must_use]
    pub fn snapshot_at(&self, now: Instant) -> WorkerTaskStats {
        let mut samples = self.lock();
        // The samples are in time order, so the expired ones are at the front.
        while samples
            .front()
            .is_some_and(|s| now.saturating_duration_since(s.at) > self.max_age)
        {
            samples.pop_front();
        }
        let mut latencies: Vec<Duration> = samples.iter().map(|s| s.latency).collect();
        let failures = samples.iter().filter(|s| s.failed).count();
        drop(samples);
        latencies.sort_unstable();
        WorkerTaskStats {
            tasks: saturating_u32(latencies.len()),
            failures: saturating_u32(failures),
            p99_latency_ms: nearest_rank_p99(&latencies)
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// A poisoned lock still holds valid samples, so the window keeps them.
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Sample>> {
        self.samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Thresholds for [`detect_outliers`].
#[derive(Debug, Clone, PartialEq)]
pub struct OutlierConfig {
    /// A worker with fewer tasks than this is not judged, and is not a peer.
    pub min_samples: u32,
    /// The fewest judged peers that a comparison needs. A value of 0 acts
    /// as 1, because a median needs at least one value.
    pub min_peers: usize,
    /// The failure ratio must exceed the peer median by this much (0.0 to 1.0).
    pub failure_ratio_margin: f64,
    /// The failure ratio must also be this many times the peer median.
    pub failure_ratio_factor: f64,
    /// The p99 latency must be this many times the peer median.
    pub latency_factor: f64,
    /// The p99 latency must also exceed the peer median by this many ms.
    pub latency_floor_ms: u64,
}

impl Default for OutlierConfig {
    fn default() -> Self {
        Self {
            min_samples: 20,
            min_peers: 2,
            failure_ratio_margin: 0.2,
            failure_ratio_factor: 2.0,
            latency_factor: 3.0,
            latency_floor_ms: 100,
        }
    }
}

/// One worker that [`detect_outliers`] flags.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerOutlier {
    /// The flagged worker.
    pub worker_id: String,
    /// The dimensions on which the worker is an outlier. Never empty.
    pub dimensions: Vec<OutlierDimension>,
    /// The worker's own stats.
    pub stats: WorkerTaskStats,
    /// The median failure ratio of the worker's peers.
    pub peer_median_failure_ratio: Option<f64>,
    /// The median p99 latency of the worker's peers, in milliseconds.
    pub peer_median_p99_latency_ms: Option<u64>,
}

/// The dimensions on which `own` is an outlier against `peers`.
///
/// `peers` must not hold `own`. The function ignores a peer below
/// `min_samples`.
#[must_use]
pub fn outlier_dimensions(
    own: &WorkerTaskStats,
    peers: &[WorkerTaskStats],
    config: &OutlierConfig,
) -> Vec<OutlierDimension> {
    let Some(own_ratio) = judged(own, config).and_then(WorkerTaskStats::failure_ratio) else {
        return Vec::new();
    };
    let peers: Vec<&WorkerTaskStats> = peers
        .iter()
        .filter(|p| judged(p, config).is_some())
        .collect();
    let min_peers = config.min_peers.max(1);
    let mut dimensions = Vec::new();

    let peer_ratios: Vec<f64> = peers.iter().filter_map(|p| p.failure_ratio()).collect();
    if peer_ratios.len() >= min_peers {
        let median = median_f64(peer_ratios);
        if own_ratio - median >= config.failure_ratio_margin
            && own_ratio >= config.failure_ratio_factor * median
        {
            dimensions.push(OutlierDimension::FailureRatio);
        }
    }

    let peer_p99s: Vec<u64> = peers.iter().filter_map(|p| p.p99_latency_ms).collect();
    if let Some(own_p99) = own.p99_latency_ms
        && peer_p99s.len() >= min_peers
    {
        let median = median_u64(peer_p99s);
        #[allow(clippy::cast_precision_loss)]
        let over_factor = own_p99 as f64 >= config.latency_factor * median as f64;
        if over_factor && own_p99.saturating_sub(median) >= config.latency_floor_ms {
            dimensions.push(OutlierDimension::LatencyP99);
        }
    }
    dimensions
}

/// Flag each worker in `fleet` that is an outlier against its peers.
///
/// `fleet` holds one entry per worker. The result keeps the order of `fleet`.
#[must_use]
pub fn detect_outliers(
    fleet: &[(String, WorkerTaskStats)],
    config: &OutlierConfig,
) -> Vec<WorkerOutlier> {
    fleet
        .iter()
        .enumerate()
        .filter_map(|(i, (worker_id, own))| {
            let peers: Vec<WorkerTaskStats> = fleet
                .iter()
                .enumerate()
                .filter(|(j, (_, peer))| *j != i && judged(peer, config).is_some())
                .map(|(_, (_, peer))| *peer)
                .collect();
            let dimensions = outlier_dimensions(own, &peers, config);
            if dimensions.is_empty() {
                return None;
            }
            Some(WorkerOutlier {
                worker_id: worker_id.clone(),
                dimensions,
                stats: *own,
                peer_median_failure_ratio: non_empty(
                    peers
                        .iter()
                        .filter_map(WorkerTaskStats::failure_ratio)
                        .collect(),
                )
                .map(median_f64),
                peer_median_p99_latency_ms: non_empty(
                    peers.iter().filter_map(|p| p.p99_latency_ms).collect(),
                )
                .map(median_u64),
            })
        })
        .collect()
}

/// [`detect_outliers`] within each cohort of `fleet`.
///
/// Each entry is `(worker_id, cohort, stats)`. A cohort groups workers that do
/// the same work, such as workers that poll the same queues. A worker is
/// judged only against peers in its own cohort. So a worker on a slow queue
/// is not an outlier against workers on a fast queue. The result is ordered
/// by cohort, then by the order of `fleet`.
#[must_use]
pub fn detect_outliers_in_cohorts(
    fleet: &[(String, String, WorkerTaskStats)],
    config: &OutlierConfig,
) -> Vec<WorkerOutlier> {
    let mut cohorts: std::collections::BTreeMap<&str, Vec<(String, WorkerTaskStats)>> =
        std::collections::BTreeMap::new();
    for (worker_id, cohort, stats) in fleet {
        cohorts
            .entry(cohort.as_str())
            .or_default()
            .push((worker_id.clone(), *stats));
    }
    cohorts
        .values()
        .flat_map(|members| detect_outliers(members, config))
        .collect()
}

/// `Some(stats)` when the worker has enough tasks to judge.
fn judged<'a>(stats: &'a WorkerTaskStats, config: &OutlierConfig) -> Option<&'a WorkerTaskStats> {
    (stats.tasks > 0 && stats.tasks >= config.min_samples).then_some(stats)
}

fn non_empty<T>(values: Vec<T>) -> Option<Vec<T>> {
    (!values.is_empty()).then_some(values)
}

/// The median of a non-empty list. An even list gives the mean of the two
/// middle values.
fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[mid - 1], values[mid])
    } else {
        values[mid]
    }
}

/// The median of a non-empty list. An even list gives the floor of the mean
/// of the two middle values.
fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        u64::midpoint(values[mid - 1], values[mid])
    } else {
        values[mid]
    }
}

/// The nearest-rank p99 of a sorted list. `None` when the list is empty.
fn nearest_rank_p99(sorted: &[Duration]) -> Option<Duration> {
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    // The rank is ceil(0.99 * n), computed in integers.
    let rank = (99 * n).div_ceil(100).max(1);
    Some(sorted[rank - 1])
}

fn saturating_u32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE: Vec<OutlierDimension> = Vec::new();

    fn stats(tasks: u32, failures: u32, p99_latency_ms: u64) -> WorkerTaskStats {
        WorkerTaskStats {
            tasks,
            failures,
            p99_latency_ms: Some(p99_latency_ms),
        }
    }

    fn fleet(entries: &[(&str, WorkerTaskStats)]) -> Vec<(String, WorkerTaskStats)> {
        entries
            .iter()
            .map(|(id, s)| ((*id).to_string(), *s))
            .collect()
    }

    /// The issue #1815 RED test: one worker fails 50% of its tasks while its
    /// peers fail none. The signal flags that worker and no other.
    #[test]
    fn worker_failing_half_its_tasks_is_flagged_and_healthy_peers_are_not() {
        let window = TaskOutcomeWindow::default();
        for i in 0..100 {
            window.record(i % 2 == 0, Duration::from_millis(40));
        }
        let sick = window.snapshot();
        assert_eq!(sick.tasks, 100);
        assert_eq!(sick.failures, 50);

        let healthy = stats(100, 0, 40);
        let outliers = detect_outliers(
            &fleet(&[
                ("w-sick", sick),
                ("w-1", healthy),
                ("w-2", healthy),
                ("w-3", healthy),
            ]),
            &OutlierConfig::default(),
        );

        assert_eq!(outliers.len(), 1, "only the sick worker: {outliers:?}");
        assert_eq!(outliers[0].worker_id, "w-sick");
        assert_eq!(outliers[0].dimensions, vec![OutlierDimension::FailureRatio]);
        assert_eq!(outliers[0].peer_median_failure_ratio, Some(0.0));
        assert_eq!(outliers[0].peer_median_p99_latency_ms, Some(40));
    }

    #[test]
    fn worker_view_flags_itself_against_peers() {
        let config = OutlierConfig::default();
        let peers = [stats(100, 0, 40), stats(100, 1, 40), stats(100, 0, 45)];
        assert_eq!(
            outlier_dimensions(&stats(100, 50, 40), &peers, &config),
            vec![OutlierDimension::FailureRatio]
        );
        assert_eq!(
            outlier_dimensions(&stats(100, 0, 40), &peers, &config),
            NONE
        );
    }

    #[test]
    fn a_fleet_wide_fault_flags_no_worker() {
        let half = stats(100, 50, 40);
        let outliers = detect_outliers(
            &fleet(&[("a", half), ("b", half), ("c", half), ("d", half)]),
            &OutlierConfig::default(),
        );
        assert!(outliers.is_empty(), "{outliers:?}");
    }

    #[test]
    fn slow_worker_is_flagged_on_latency_only() {
        let outliers = detect_outliers(
            &fleet(&[
                ("slow", stats(100, 0, 2_000)),
                ("a", stats(100, 0, 100)),
                ("b", stats(100, 0, 110)),
                ("c", stats(100, 0, 90)),
            ]),
            &OutlierConfig::default(),
        );
        assert_eq!(outliers.len(), 1);
        assert_eq!(outliers[0].worker_id, "slow");
        assert_eq!(outliers[0].dimensions, vec![OutlierDimension::LatencyP99]);
        assert_eq!(outliers[0].peer_median_p99_latency_ms, Some(100));
    }

    #[test]
    fn small_latency_gap_under_the_floor_is_not_flagged() {
        // 30 ms is 10x the peers, but only 27 ms above them.
        let outliers = detect_outliers(
            &fleet(&[
                ("w", stats(100, 0, 30)),
                ("a", stats(100, 0, 3)),
                ("b", stats(100, 0, 3)),
            ]),
            &OutlierConfig::default(),
        );
        assert!(outliers.is_empty(), "{outliers:?}");
    }

    #[test]
    fn worker_below_min_samples_is_neither_judged_nor_a_peer() {
        let config = OutlierConfig::default();
        let outliers = detect_outliers(
            &fleet(&[
                ("tiny", stats(10, 5, 40)),
                ("a", stats(100, 0, 40)),
                ("b", stats(100, 0, 40)),
            ]),
            &config,
        );
        assert!(outliers.is_empty(), "{outliers:?}");

        // "tiny" does not count as a peer, so "sick" has one peer only.
        let outliers = detect_outliers(
            &fleet(&[
                ("sick", stats(100, 50, 40)),
                ("tiny", stats(10, 0, 40)),
                ("a", stats(100, 0, 40)),
            ]),
            &config,
        );
        assert!(outliers.is_empty(), "{outliers:?}");
    }

    #[test]
    fn too_few_peers_flags_nothing() {
        let outliers = detect_outliers(
            &fleet(&[("sick", stats(100, 50, 40)), ("a", stats(100, 0, 40))]),
            &OutlierConfig::default(),
        );
        assert!(outliers.is_empty(), "{outliers:?}");
    }

    #[test]
    fn failure_ratio_needs_both_margin_and_factor() {
        let config = OutlierConfig::default();
        let peers = [stats(100, 30, 40), stats(100, 30, 40)];
        // +25 points over a 30% median is under 2x the median.
        assert_eq!(
            outlier_dimensions(&stats(100, 55, 40), &peers, &config),
            NONE
        );
        // 70% clears both rules.
        assert_eq!(
            outlier_dimensions(&stats(100, 70, 40), &peers, &config),
            vec![OutlierDimension::FailureRatio]
        );
    }

    #[test]
    fn failure_ratio_is_none_without_tasks() {
        assert_eq!(WorkerTaskStats::default().failure_ratio(), None);
        assert_eq!(stats(4, 1, 1).failure_ratio(), Some(0.25));
    }

    #[test]
    fn window_drops_samples_older_than_max_age() {
        let window = TaskOutcomeWindow::new(100, Duration::from_secs(60));
        let start = Instant::now();
        window.record_at(start, true, Duration::from_millis(500));
        window.record_at(
            start + Duration::from_secs(30),
            false,
            Duration::from_millis(10),
        );

        let early = window.snapshot_at(start + Duration::from_secs(45));
        assert_eq!((early.tasks, early.failures), (2, 1));

        let late = window.snapshot_at(start + Duration::from_secs(75));
        assert_eq!((late.tasks, late.failures), (1, 0));
        assert_eq!(late.p99_latency_ms, Some(10));
    }

    #[test]
    fn window_drops_expired_samples_inserted_out_of_order() {
        let window = TaskOutcomeWindow::new(100, Duration::from_secs(60));
        let start = Instant::now();
        // The newer success lands first, then the older failure.
        window.record_at(
            start + Duration::from_secs(30),
            false,
            Duration::from_millis(10),
        );
        window.record_at(start, true, Duration::from_millis(500));
        let snap = window.snapshot_at(start + Duration::from_secs(75));
        assert_eq!((snap.tasks, snap.failures), (1, 0));
        assert_eq!(snap.p99_latency_ms, Some(10));
    }

    #[test]
    fn full_window_keeps_the_newest_samples_when_an_older_one_lands_late() {
        let window = TaskOutcomeWindow::new(2, Duration::from_secs(60));
        let start = Instant::now();
        window.record_at(start + Duration::from_secs(2), false, Duration::ZERO);
        window.record_at(start + Duration::from_secs(3), false, Duration::ZERO);
        // An older failure lands late. It is the oldest, so it is evicted.
        window.record_at(start + Duration::from_secs(1), true, Duration::ZERO);
        let snap = window.snapshot_at(start + Duration::from_secs(3));
        assert_eq!((snap.tasks, snap.failures), (2, 0));
    }

    #[test]
    fn window_keeps_at_most_capacity_samples() {
        let window = TaskOutcomeWindow::new(3, Duration::from_secs(60));
        let now = Instant::now();
        for _ in 0..5 {
            window.record_at(now, true, Duration::from_millis(1));
        }
        window.record_at(now, false, Duration::from_millis(1));
        let snap = window.snapshot_at(now);
        assert_eq!((snap.tasks, snap.failures), (3, 2));
    }

    #[test]
    fn p99_uses_the_nearest_rank() {
        let window = TaskOutcomeWindow::default();
        let now = Instant::now();
        for ms in 1..=100 {
            window.record_at(now, false, Duration::from_millis(ms));
        }
        assert_eq!(window.snapshot_at(now).p99_latency_ms, Some(99));
        assert_eq!(TaskOutcomeWindow::default().snapshot().p99_latency_ms, None);
    }

    #[test]
    fn worker_on_a_slow_queue_is_judged_only_against_its_cohort() {
        let fleet = vec![
            (
                "transcode".to_string(),
                "[\"video\"]".to_string(),
                stats(100, 0, 90_000),
            ),
            (
                "mail-1".to_string(),
                "[\"email\"]".to_string(),
                stats(100, 0, 50),
            ),
            (
                "mail-2".to_string(),
                "[\"email\"]".to_string(),
                stats(100, 0, 50),
            ),
            (
                "mail-3".to_string(),
                "[\"email\"]".to_string(),
                stats(100, 50, 50),
            ),
        ];
        let outliers = detect_outliers_in_cohorts(&fleet, &OutlierConfig::default());
        assert_eq!(outliers.len(), 1, "{outliers:?}");
        assert_eq!(outliers[0].worker_id, "mail-3");
        assert_eq!(outliers[0].dimensions, vec![OutlierDimension::FailureRatio]);
    }

    #[test]
    fn zero_min_peers_with_no_peers_flags_nothing_and_does_not_panic() {
        let config = OutlierConfig {
            min_peers: 0,
            ..OutlierConfig::default()
        };
        assert_eq!(
            outlier_dimensions(&stats(100, 90, 9_000), &[], &config),
            NONE
        );
        let alone = detect_outliers(&fleet(&[("only", stats(100, 90, 9_000))]), &config);
        assert_eq!(alone, Vec::<WorkerOutlier>::new());
    }

    #[test]
    fn dimension_labels_are_stable() {
        assert_eq!(OutlierDimension::FailureRatio.as_str(), "failure_ratio");
        assert_eq!(OutlierDimension::LatencyP99.as_str(), "latency_p99");
        assert_eq!(
            serde_json::to_value(OutlierDimension::LatencyP99).unwrap(),
            "latency_p99"
        );
    }
}
