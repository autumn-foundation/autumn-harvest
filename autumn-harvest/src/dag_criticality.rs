//! Monte Carlo criticality analysis for DAG definitions.
//!
//! Slack and leverage assume that each task has one fixed duration. Real tasks
//! vary from run to run. A task with a little slack can join the critical path
//! when it runs slow. A critical task can leave it when it runs fast.
//!
//! This module gives each task a range of durations instead of one value. Each
//! range is a triangular distribution with a minimum, a most likely value and a
//! maximum. The analyzer samples every task, runs the critical path method, and
//! repeats the trial many times.
//!
//! The result answers two questions. The criticality index of a task is the
//! share of trials in which it has zero slack. The makespan distribution gives
//! percentiles and the chance to meet a deadline.
//!
//! The analyzer is pure and deterministic. A fixed seed drives a `SplitMix64`
//! generator, so the same inputs always give the same report. It sends no SQL
//! and reads no clock.

use crate::dag::DagDefinition;
use crate::dag_slack::to_duration;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;

/// Triangular range of durations for one task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationRange {
    min: Duration,
    mode: Duration,
    max: Duration,
}

impl DurationRange {
    /// Create a range. Return `None` unless `min <= mode <= max`.
    #[must_use]
    pub fn new(min: Duration, mode: Duration, max: Duration) -> Option<Self> {
        (min <= mode && mode <= max).then_some(Self { min, mode, max })
    }

    /// Create a range that always gives the same duration.
    #[must_use]
    pub const fn fixed(duration: Duration) -> Self {
        Self {
            min: duration,
            mode: duration,
            max: duration,
        }
    }

    /// Shortest duration of the range.
    #[must_use]
    pub const fn min(&self) -> Duration {
        self.min
    }

    /// Most likely duration of the range.
    #[must_use]
    pub const fn mode(&self) -> Duration {
        self.mode
    }

    /// Longest duration of the range.
    #[must_use]
    pub const fn max(&self) -> Duration {
        self.max
    }

    /// Map a uniform sample `u` in `[0, 1)` to nanoseconds by the inverse CDF.
    fn sample_nanos(&self, u: f64) -> u128 {
        let a = self.min.as_secs_f64();
        let c = self.mode.as_secs_f64();
        let b = self.max.as_secs_f64();
        let width = b - a;
        if width <= 0.0 {
            return self.min.as_nanos();
        }
        let split = (c - a) / width;
        let secs = if u < split {
            a + (u * width * (c - a)).sqrt()
        } else {
            b - ((1.0 - u) * width * (b - c)).sqrt()
        };
        // The clamp keeps rounding inside the range.
        Duration::from_secs_f64(secs.clamp(a, b))
            .clamp(self.min, self.max)
            .as_nanos()
    }
}

/// Criticality figures for one task.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskCriticality {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Number of trials in which the task has zero slack.
    pub critical_trials: u32,
    /// Share of trials in which the task has zero slack, from 0.0 to 1.0.
    pub criticality: f64,
}

/// Result of a criticality analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct CriticalityReport {
    /// Number of trials.
    pub trials: u32,
    /// Makespan of each trial, in ascending order.
    pub makespans: Vec<Duration>,
    /// One entry per task, in topological index order.
    pub tasks: Vec<TaskCriticality>,
}

impl CriticalityReport {
    /// Nearest-rank percentile of the makespan. `q` is clamped to `[0, 1]`.
    ///
    /// The median is `percentile(0.5)`. The worst trial is `percentile(1.0)`.
    #[must_use]
    pub fn percentile(&self, q: f64) -> Duration {
        let n = self.makespans.len();
        if n == 0 {
            return Duration::ZERO;
        }
        let rank = (q.clamp(0.0, 1.0) * count_f64(n)).ceil();
        // The rank lies in `[0, n]`, so the cast neither wraps nor truncates.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rank = rank as usize;
        self.makespans[rank.saturating_sub(1).min(n - 1)]
    }

    /// Share of trials that finish within `deadline`, from 0.0 to 1.0.
    #[must_use]
    pub fn probability_within(&self, deadline: Duration) -> f64 {
        if self.makespans.is_empty() {
            return 1.0;
        }
        let met = self.makespans.partition_point(|m| *m <= deadline);
        count_f64(met) / count_f64(self.makespans.len())
    }

    /// Tasks by descending criticality. Ties keep topological order.
    #[must_use]
    pub fn ranked(&self) -> Vec<&TaskCriticality> {
        let mut ranked: Vec<&TaskCriticality> = self.tasks.iter().collect();
        ranked.sort_by_key(|task| std::cmp::Reverse(task.critical_trials));
        ranked
    }
}

/// Analyzer that samples task durations and counts critical trials.
pub struct DagCriticalityAnalyzer {
    dag: DagDefinition,
    ranges: HashMap<String, DurationRange>,
    default_range: DurationRange,
    trials: NonZeroU32,
    seed: u64,
}

impl DagCriticalityAnalyzer {
    /// Create an analyzer with 1000 trials and seed zero.
    ///
    /// A task with no estimate takes exactly one second, as in `DagSlackAnalyzer`.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            ranges: HashMap::new(),
            default_range: DurationRange::fixed(Duration::from_secs(1)),
            trials: NonZeroU32::new(1000).unwrap_or(NonZeroU32::MIN),
            seed: 0,
        }
    }

    /// Set the range of an activity that has no estimate.
    #[must_use]
    pub const fn with_default_range(mut self, range: DurationRange) -> Self {
        self.default_range = range;
        self
    }

    /// Set the duration range of an activity by name.
    #[must_use]
    pub fn estimate(mut self, name: &str, range: DurationRange) -> Self {
        self.ranges.insert(name.to_string(), range);
        self
    }

    /// Set the number of trials.
    #[must_use]
    pub const fn with_trials(mut self, trials: NonZeroU32) -> Self {
        self.trials = trials;
        self
    }

    /// Set the seed of the generator.
    #[must_use]
    pub const fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Run every trial and count how often each task is critical.
    ///
    /// A `start_to_close` timeout caps the sampled duration of its task. The
    /// task cannot run longer than its timeout.
    #[must_use]
    pub fn analyze(&self) -> CriticalityReport {
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();
        let ranges: Vec<&DurationRange> = tasks
            .iter()
            .map(|task| {
                self.ranges
                    .get(&task.activity_name)
                    .unwrap_or(&self.default_range)
            })
            .collect();
        let caps: Vec<u128> = tasks
            .iter()
            .map(|task| task.start_to_close.map_or(u128::MAX, |t| t.as_nanos()))
            .collect();
        let mut downstreams: Vec<Vec<usize>> = vec![Vec::new(); tasks.len()];
        for (i, task) in tasks.iter().enumerate() {
            for &up in &task.upstreams {
                downstreams[up].push(i);
            }
        }

        let mut rng = SplitMix64(self.seed);
        let mut critical = vec![0u32; tasks.len()];
        let mut makespans = Vec::with_capacity(self.trials.get() as usize);
        let mut durations = vec![0u128; tasks.len()];
        let mut earliest_start = vec![0u128; tasks.len()];
        let mut latest_finish = vec![0u128; tasks.len()];

        for _ in 0..self.trials.get() {
            for (i, range) in ranges.iter().enumerate() {
                durations[i] = range.sample_nanos(rng.next_unit()).min(caps[i]);
            }
            for level in levels {
                for &i in level {
                    earliest_start[i] = tasks[i]
                        .upstreams
                        .iter()
                        .map(|&up| earliest_start[up].saturating_add(durations[up]))
                        .max()
                        .unwrap_or(0);
                }
            }
            let makespan = (0..tasks.len())
                .map(|i| earliest_start[i].saturating_add(durations[i]))
                .max()
                .unwrap_or(0);
            for level in levels.iter().rev() {
                for &i in level {
                    latest_finish[i] = downstreams[i]
                        .iter()
                        .map(|&down| latest_finish[down].saturating_sub(durations[down]))
                        .min()
                        .unwrap_or(makespan);
                }
            }
            for i in 0..tasks.len() {
                if earliest_start[i].saturating_add(durations[i]) == latest_finish[i] {
                    critical[i] += 1;
                }
            }
            makespans.push(to_duration(makespan));
        }
        makespans.sort_unstable();

        let trials = self.trials.get();
        CriticalityReport {
            trials,
            makespans,
            tasks: tasks
                .iter()
                .enumerate()
                .map(|(index, task)| TaskCriticality {
                    index,
                    name: task.activity_name.clone(),
                    critical_trials: critical[index],
                    criticality: f64::from(critical[index]) / f64::from(trials),
                })
                .collect(),
        }
    }
}

/// Convert a count to `f64`. A count above `u32::MAX` saturates.
fn count_f64(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}

/// `SplitMix64` generator. It is small, fast and needs no dependency.
struct SplitMix64(u64);

impl SplitMix64 {
    const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform sample in `[0, 1)` from the top 52 bits.
    ///
    /// The bits fill the mantissa of a number in `[1, 2)`, so no cast is needed.
    fn next_unit(&mut self) -> f64 {
        f64::from_bits(0x3FF0_0000_0000_0000 | (self.next_u64() >> 12)) - 1.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_slack::DagSlackAnalyzer;

    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn range(min: u64, mode: u64, max: u64) -> DurationRange {
        DurationRange::new(secs(min), secs(mode), secs(max)).unwrap()
    }

    /// Diamond: a -> b -> d and a -> c -> d.
    fn diamond() -> DagDefinition {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta);
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        builder.build().unwrap()
    }

    fn criticality(report: &CriticalityReport, name: &str) -> f64 {
        report
            .tasks
            .iter()
            .find(|t| t.name == name)
            .unwrap()
            .criticality
    }

    #[test]
    fn range_rejects_an_unordered_triple() {
        assert!(DurationRange::new(secs(2), secs(1), secs(3)).is_none());
        assert!(DurationRange::new(secs(1), secs(4), secs(3)).is_none());
        assert!(DurationRange::new(secs(1), secs(1), secs(1)).is_some());
    }

    #[test]
    fn fixed_ranges_match_the_slack_report() {
        let dag = diamond();
        let report = DagCriticalityAnalyzer::new(dag.clone())
            .estimate("a", DurationRange::fixed(secs(2)))
            .estimate("b", DurationRange::fixed(secs(10)))
            .estimate("c", DurationRange::fixed(secs(3)))
            .estimate("d", DurationRange::fixed(secs(1)))
            .with_trials(NonZeroU32::new(20).unwrap())
            .analyze();
        let slack = DagSlackAnalyzer::new(dag)
            .mock_duration("a", secs(2))
            .mock_duration("b", secs(10))
            .mock_duration("c", secs(3))
            .mock_duration("d", secs(1))
            .analyze();

        assert_eq!(report.percentile(0.0), slack.makespan);
        assert_eq!(report.percentile(1.0), slack.makespan);
        let critical: Vec<usize> = report
            .tasks
            .iter()
            .filter(|t| t.critical_trials == report.trials)
            .map(|t| t.index)
            .collect();
        assert_eq!(critical, slack.critical_indices());
        assert_eq!(criticality(&report, "c"), 0.0);
    }

    #[test]
    fn overlapping_branches_share_the_critical_path() {
        // b usually wins, but a slow c beats a fast b.
        let report = DagCriticalityAnalyzer::new(diamond())
            .estimate("a", DurationRange::fixed(secs(1)))
            .estimate("b", range(4, 6, 8))
            .estimate("c", range(2, 4, 7))
            .estimate("d", DurationRange::fixed(secs(1)))
            .analyze();

        let b = criticality(&report, "b");
        let c = criticality(&report, "c");
        assert!(b > c, "b {b} should beat c {c}");
        assert!(c > 0.0, "c should be critical in some trials");
        assert!(b + c >= 1.0, "one branch is always critical");
        assert_eq!(criticality(&report, "a"), 1.0);
        assert_eq!(criticality(&report, "d"), 1.0);
        assert_eq!(report.ranked()[0].name, "a");
        assert_eq!(report.ranked().last().unwrap().name, "c");
    }

    #[test]
    fn makespans_stay_inside_the_path_bounds() {
        let report = DagCriticalityAnalyzer::new(diamond())
            .estimate("a", range(1, 2, 3))
            .estimate("b", range(4, 6, 8))
            .estimate("c", range(2, 4, 7))
            .estimate("d", range(1, 1, 2))
            .analyze();

        // The fastest run is a + b + d at their minimums.
        assert!(report.percentile(0.0) >= secs(6));
        // The slowest run is a + b + d at their maximums.
        assert!(report.percentile(1.0) <= secs(13));
        assert!(report.percentile(0.5) <= report.percentile(0.9));
        assert_eq!(report.makespans.len(), 1000);
    }

    #[test]
    fn deadline_probability_spans_zero_to_one() {
        let report = DagCriticalityAnalyzer::new(diamond())
            .estimate("b", range(4, 6, 8))
            .analyze();

        assert_eq!(report.probability_within(secs(5)), 0.0);
        assert_eq!(report.probability_within(secs(10)), 1.0);
        let median = report.probability_within(report.percentile(0.5));
        assert!((0.5..=0.6).contains(&median), "median share {median}");
    }

    #[test]
    fn same_seed_gives_the_same_report() {
        let run = |seed| {
            DagCriticalityAnalyzer::new(diamond())
                .estimate("b", range(4, 6, 8))
                .estimate("c", range(2, 4, 7))
                .with_seed(seed)
                .analyze()
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7).makespans, run(8).makespans);
    }

    #[test]
    fn timeout_caps_the_sampled_duration() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(a).start_to_close(secs(3));
        let dag = builder.build().unwrap();
        let report = DagCriticalityAnalyzer::new(dag)
            .estimate("a", range(2, 5, 9))
            .analyze();

        assert_eq!(report.percentile(1.0), secs(3));
        assert!(report.percentile(0.0) >= secs(2));
    }

    #[test]
    fn empty_dag_has_no_tasks_and_zero_makespan() {
        let dag = DagBuilder::new().build().unwrap();
        let report = DagCriticalityAnalyzer::new(dag)
            .with_trials(NonZeroU32::new(3).unwrap())
            .analyze();

        assert_eq!(report.tasks, Vec::new());
        assert_eq!(report.percentile(0.5), Duration::ZERO);
        assert_eq!(report.probability_within(Duration::ZERO), 1.0);
    }
}
