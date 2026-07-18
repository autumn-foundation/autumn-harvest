#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
//! Monte Carlo simulation for DAG execution.
//!
//! Provides a probabilistic environment to simulate DAG executions over many
//! iterations to estimate realistic timeline metrics (like p50, p90, p99
//! completion times) when activity durations are variable.

use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;

use crate::dag::DagDefinition;
use crate::dag_profiler::DagProfiler;

/// A statistical distribution of execution durations.
#[derive(Debug, Clone, Copy)]
pub enum DurationDistribution {
    /// A fixed duration.
    Fixed(Duration),
    /// A uniform distribution between a minimum and maximum duration.
    Uniform { min: Duration, max: Duration },
}

impl DurationDistribution {
    /// Sample a duration from the distribution using the provided random number generator.
    pub fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Duration {
        match self {
            Self::Fixed(duration) => *duration,
            Self::Uniform { min, max } => {
                if min >= max {
                    *min
                } else {
                    let min_micros = min.as_micros();
                    let max_micros = max.as_micros();
                    // Using micros to maintain a reasonable granularity for the simulation.
                    let sampled_micros = rng.gen_range(min_micros..=max_micros);
                    Duration::from_micros(u64::try_from(sampled_micros).unwrap_or(u64::MAX))
                }
            }
        }
    }
}

/// The result of a Monte Carlo profile run.
#[derive(Debug, Clone)]
pub struct MonteCarloProfile {
    /// The number of iterations run.
    pub iterations: usize,
    /// The minimum observed total duration.
    pub min_duration: Duration,
    /// The maximum observed total duration.
    pub max_duration: Duration,
    /// The 50th percentile total duration.
    pub p50_duration: Duration,
    /// The 90th percentile total duration.
    pub p90_duration: Duration,
    /// The 99th percentile total duration.
    pub p99_duration: Duration,
    /// The peak concurrency observed across all iterations.
    pub peak_concurrency: usize,
}

/// A Monte Carlo simulator to profile DAG execution probabilistically.
pub struct DagMonteCarlo {
    dag: DagDefinition,
    activity_distributions: HashMap<String, DurationDistribution>,
    default_distribution: DurationDistribution,
    iterations: usize,
}

impl DagMonteCarlo {
    /// Create a new Monte Carlo simulator for the given DAG definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_distributions: HashMap::new(),
            default_distribution: DurationDistribution::Fixed(Duration::from_secs(1)),
            iterations: 1000,
        }
    }

    /// Set the number of simulation iterations to run.
    #[must_use]
    pub const fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    /// Set a default distribution for activities without a specific mock.
    #[must_use]
    pub const fn with_default_distribution(mut self, distribution: DurationDistribution) -> Self {
        self.default_distribution = distribution;
        self
    }

    /// Mock the duration distribution of a specific activity by name.
    #[must_use]
    pub fn mock_distribution(mut self, name: &str, distribution: DurationDistribution) -> Self {
        self.activity_distributions
            .insert(name.to_string(), distribution);
        self
    }

    /// Run the simulation.
    #[must_use]
    pub fn profile(&self) -> MonteCarloProfile {
        if self.iterations == 0 {
            return MonteCarloProfile {
                iterations: 0,
                min_duration: Duration::ZERO,
                max_duration: Duration::ZERO,
                p50_duration: Duration::ZERO,
                p90_duration: Duration::ZERO,
                p99_duration: Duration::ZERO,
                peak_concurrency: 0,
            };
        }

        let mut rng = rand::thread_rng();
        let mut durations = Vec::with_capacity(self.iterations);
        let mut global_peak_concurrency = 0;

        let tasks = self.dag.tasks();

        for _ in 0..self.iterations {
            let mut profiler = DagProfiler::new(self.dag.clone());

            for task in tasks {
                let name = &task.activity_name;
                let distribution = self
                    .activity_distributions
                    .get(name)
                    .unwrap_or(&self.default_distribution);

                let sampled_duration = distribution.sample(&mut rng);
                profiler = profiler.mock_duration(name, sampled_duration);
            }

            let profile = profiler.profile();
            durations.push(profile.total_duration);

            if profile.peak_concurrency > global_peak_concurrency {
                global_peak_concurrency = profile.peak_concurrency;
            }
        }

        durations.sort_unstable();

        let min_duration = *durations.first().unwrap_or(&Duration::ZERO);
        let max_duration = *durations.last().unwrap_or(&Duration::ZERO);

        let p50_idx = (self.iterations as f64 * 0.50).round() as usize;
        let p90_idx = (self.iterations as f64 * 0.90).round() as usize;
        let p99_idx = (self.iterations as f64 * 0.99).round() as usize;

        let p50_duration = *durations
            .get(p50_idx.saturating_sub(1))
            .unwrap_or(&max_duration);
        let p90_duration = *durations
            .get(p90_idx.saturating_sub(1))
            .unwrap_or(&max_duration);
        let p99_duration = *durations
            .get(p99_idx.saturating_sub(1))
            .unwrap_or(&max_duration);

        MonteCarloProfile {
            iterations: self.iterations,
            min_duration,
            max_duration,
            p50_duration,
            p90_duration,
            p99_duration,
            peak_concurrency: global_peak_concurrency,
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
    fn test_duration_distribution_sample() {
        let mut rng = rand::thread_rng();

        let fixed = DurationDistribution::Fixed(Duration::from_secs(5));
        assert_eq!(fixed.sample(&mut rng), Duration::from_secs(5));

        let uniform = DurationDistribution::Uniform {
            min: Duration::from_secs(1),
            max: Duration::from_secs(3),
        };
        for _ in 0..10 {
            let sample = uniform.sample(&mut rng);
            assert!(sample >= Duration::from_secs(1));
            assert!(sample <= Duration::from_secs(3));
        }
    }

    #[test]
    fn test_monte_carlo_profile_linear() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let mc = DagMonteCarlo::new(dag)
            .with_iterations(100)
            .mock_distribution(
                "activity_a",
                DurationDistribution::Fixed(Duration::from_secs(2)),
            )
            .mock_distribution(
                "activity_b",
                DurationDistribution::Uniform {
                    min: Duration::from_secs(2),
                    max: Duration::from_secs(4),
                },
            );

        let profile = mc.profile();

        assert_eq!(profile.iterations, 100);
        assert!(profile.min_duration >= Duration::from_secs(4));
        assert!(profile.max_duration <= Duration::from_secs(6));
        assert_eq!(profile.peak_concurrency, 1);

        // p50 should be roughly around 5s.
        assert!(profile.p50_duration >= Duration::from_secs(4));
        assert!(profile.p50_duration <= Duration::from_secs(6));
    }

    #[test]
    fn test_monte_carlo_profile_zero_iterations() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        let mc = DagMonteCarlo::new(dag).with_iterations(0);
        let profile = mc.profile();

        assert_eq!(profile.iterations, 0);
        assert_eq!(profile.min_duration, Duration::ZERO);
        assert_eq!(profile.peak_concurrency, 0);
    }
}
