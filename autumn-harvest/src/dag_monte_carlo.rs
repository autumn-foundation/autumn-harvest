//! Monte Carlo Simulator for DAG definitions.
//!
//! Provides a way to estimate the statistical distribution of a DAG's total execution
//! time by running many iterations with random activity durations.

use crate::dag::DagDefinition;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;

/// Probability distribution for an activity's duration.
#[derive(Debug, Clone, Copy)]
pub enum DurationDistribution {
    /// A fixed, constant duration.
    Fixed(Duration),
    /// A uniform distribution between a minimum and maximum duration.
    Uniform {
        /// The minimum duration.
        min: Duration,
        /// The maximum duration.
        max: Duration,
    },
}

impl DurationDistribution {
    /// Sample a duration from the distribution using the given RNG.
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Duration {
        match self {
            Self::Fixed(duration) => *duration,
            Self::Uniform { min, max } => {
                if min >= max {
                    *min
                } else {
                    let min_nanos = min.as_nanos();
                    let max_nanos = max.as_nanos();
                    let sampled = rng.gen_range(min_nanos..=max_nanos);
                    let secs = u64::try_from(sampled / 1_000_000_000).unwrap();
                    let nanos = (sampled % 1_000_000_000) as u32;
                    Duration::new(secs, nanos)
                }
            }
        }
    }
}

/// Result of a Monte Carlo simulation.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    /// The number of iterations run.
    pub iterations: usize,
    /// Minimum total duration observed.
    pub min_duration: Duration,
    /// Maximum total duration observed.
    pub max_duration: Duration,
    /// Mean total duration.
    pub mean_duration: Duration,
    /// 50th percentile (median) duration.
    pub p50_duration: Duration,
    /// 90th percentile duration.
    pub p90_duration: Duration,
    /// 99th percentile duration.
    pub p99_duration: Duration,
}

/// A Monte Carlo simulator to estimate DAG execution times under uncertainty.
pub struct DagMonteCarloSimulator {
    dag: DagDefinition,
    activity_distributions: HashMap<String, DurationDistribution>,
    default_distribution: DurationDistribution,
}

impl DagMonteCarloSimulator {
    /// Create a new simulator for the given DAG definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_distributions: HashMap::new(),
            default_distribution: DurationDistribution::Fixed(Duration::from_secs(1)),
        }
    }

    /// Set a default duration distribution for activities whose duration is not explicitly mocked.
    #[must_use]
    pub const fn with_default_distribution(mut self, distribution: DurationDistribution) -> Self {
        self.default_distribution = distribution;
        self
    }

    /// Mock the expected duration distribution of an activity by name.
    #[must_use]
    pub fn mock_distribution(mut self, name: &str, distribution: DurationDistribution) -> Self {
        self.activity_distributions
            .insert(name.to_string(), distribution);
        self
    }

    /// Run the Monte Carlo simulation for a given number of iterations.
    #[must_use]
    pub fn simulate(&self, iterations: usize) -> MonteCarloResult {
        if iterations == 0 || self.dag.tasks().is_empty() {
            return MonteCarloResult {
                iterations,
                min_duration: Duration::ZERO,
                max_duration: Duration::ZERO,
                mean_duration: Duration::ZERO,
                p50_duration: Duration::ZERO,
                p90_duration: Duration::ZERO,
                p99_duration: Duration::ZERO,
            };
        }

        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        let mut rng = rand::thread_rng();
        let mut total_durations = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let mut task_end_times = vec![Duration::ZERO; tasks.len()];

            for level in levels {
                for &task_index in level {
                    let task = &tasks[task_index];

                    let distribution = self
                        .activity_distributions
                        .get(&task.activity_name)
                        .unwrap_or(&self.default_distribution);

                    let duration = distribution.sample(&mut rng);

                    let mut start_time = Duration::ZERO;
                    for &up_idx in &task.upstreams {
                        start_time = start_time.max(task_end_times[up_idx]);
                    }

                    task_end_times[task_index] = start_time + duration;
                }
            }

            let max_end_time = task_end_times.into_iter().max().unwrap_or(Duration::ZERO);
            total_durations.push(max_end_time);
        }

        total_durations.sort_unstable();

        let sum: u128 = total_durations.iter().map(Duration::as_nanos).sum();
        let mean_nanos = sum / (iterations as u128);

        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let p50_idx = (iterations as f64 * 0.50) as usize;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let p90_idx = (iterations as f64 * 0.90) as usize;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let p99_idx = (iterations as f64 * 0.99) as usize;

        let mean_secs = u64::try_from(mean_nanos / 1_000_000_000).unwrap_or(u64::MAX);
        let mean_nanos_rem = (mean_nanos % 1_000_000_000) as u32;

        MonteCarloResult {
            iterations,
            min_duration: total_durations[0],
            max_duration: total_durations[iterations - 1],
            mean_duration: Duration::new(mean_secs, mean_nanos_rem),
            p50_duration: total_durations[p50_idx.min(iterations.saturating_sub(1))],
            p90_duration: total_durations[p90_idx.min(iterations.saturating_sub(1))],
            p99_duration: total_durations[p99_idx.min(iterations.saturating_sub(1))],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    fn activity_a() {}
    fn activity_b() {}

    #[test]
    fn test_duration_distribution_fixed() {
        let mut rng = rand::thread_rng();
        let dist = DurationDistribution::Fixed(Duration::from_secs(5));
        assert_eq!(dist.sample(&mut rng), Duration::from_secs(5));
    }

    #[test]
    fn test_duration_distribution_uniform() {
        let mut rng = rand::thread_rng();
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(4);
        let dist = DurationDistribution::Uniform { min, max };

        for _ in 0..100 {
            let sample = dist.sample(&mut rng);
            assert!(sample >= min && sample <= max);
        }
    }

    #[test]
    fn test_monte_carlo_empty_dag() {
        let builder = DagBuilder::new();
        let dag = builder.build().unwrap();

        let simulator = DagMonteCarloSimulator::new(dag);
        let result = simulator.simulate(10);

        assert_eq!(result.iterations, 10);
        assert_eq!(result.min_duration, Duration::ZERO);
        assert_eq!(result.max_duration, Duration::ZERO);
    }

    #[test]
    fn test_monte_carlo_linear_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let simulator = DagMonteCarloSimulator::new(dag)
            .mock_distribution(
                "activity_a",
                DurationDistribution::Fixed(Duration::from_secs(2)),
            )
            .mock_distribution(
                "activity_b",
                DurationDistribution::Fixed(Duration::from_secs(3)),
            );

        let result = simulator.simulate(5);

        assert_eq!(result.min_duration, Duration::from_secs(5));
        assert_eq!(result.max_duration, Duration::from_secs(5));
        assert_eq!(result.mean_duration, Duration::from_secs(5));
        assert_eq!(result.p50_duration, Duration::from_secs(5));
        assert_eq!(result.p90_duration, Duration::from_secs(5));
        assert_eq!(result.p99_duration, Duration::from_secs(5));
    }

    #[test]
    fn test_monte_carlo_uniform_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let simulator = DagMonteCarloSimulator::new(dag)
            .mock_distribution(
                "activity_a",
                DurationDistribution::Uniform {
                    min: Duration::from_secs(1),
                    max: Duration::from_secs(3),
                },
            )
            .mock_distribution(
                "activity_b",
                DurationDistribution::Uniform {
                    min: Duration::from_secs(2),
                    max: Duration::from_secs(4),
                },
            );

        let result = simulator.simulate(100);

        assert!(result.min_duration >= Duration::from_secs(3));
        assert!(result.max_duration <= Duration::from_secs(7));
    }
}
