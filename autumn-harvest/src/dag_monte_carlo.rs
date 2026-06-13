//! Stochastic Monte Carlo Simulator for Directed Acyclic Graphs (DAGs).
//!
//! Provides a probabilistic environment to simulate a DAG execution over thousands
//! of iterations. Unlike the deterministic `DagSimulator` or `DagProfiler`, this allows
//! activity runtimes to be drawn from statistical distributions, and activities to have
//! a random chance of failure. This produces aggregate metrics like completion rate
//! and duration percentiles (P50, P90, P99).

use crate::dag::DagDefinition;
use crate::policy::TaskStatus;
use std::collections::HashMap;
use std::time::Duration;

/// A stochastic mock function for a DAG activity.
///
/// It returns a random duration representing how long the activity took,
/// and a Result indicating whether it ultimately succeeded or failed.
pub type StochasticActivityMock = Box<dyn Fn() -> (Duration, Result<(), String>) + Send + Sync>;

/// Aggregate results from a Monte Carlo simulation.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    /// Number of iterations run.
    pub iterations: usize,
    /// Number of times the overall DAG reached a completely successful state
    /// (e.g., sink nodes succeeded). For simplicity, we define "DAG success"
    /// as no task failing.
    pub successful_iterations: usize,
    /// Percentage of successful iterations (0.0 to 100.0).
    pub success_rate: f64,
    /// Mean duration of the successful iterations.
    pub mean_duration: Duration,
    /// The P50 (median) duration of the successful iterations.
    pub p50_duration: Duration,
    /// The P90 duration of the successful iterations.
    pub p90_duration: Duration,
    /// The P99 duration of the successful iterations.
    pub p99_duration: Duration,
}

/// A stochastic simulator for a DAG.
pub struct DagMonteCarlo {
    dag: DagDefinition,
    activity_mocks: HashMap<String, StochasticActivityMock>,
    default_duration: Duration,
    iterations: usize,
}

impl DagMonteCarlo {
    /// Create a new Monte Carlo simulator for the given definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_mocks: HashMap::new(),
            default_duration: Duration::from_secs(1),
            iterations: 1000, // Default to 1000 iterations
        }
    }

    /// Set the number of iterations to run.
    #[must_use]
    pub const fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    /// Set a default duration for unmocked activities.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Register a stochastic mock implementation for an activity by name.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn() -> (Duration, Result<(), String>) + Send + Sync + 'static,
    {
        self.activity_mocks.insert(name.to_string(), Box::new(mock));
        self
    }

    /// Run the Monte Carlo simulation.
    ///
    /// Returns the aggregated statistics.
    #[must_use]
    pub fn run(&self) -> MonteCarloResult {
        if self.iterations == 0 {
            return MonteCarloResult {
                iterations: 0,
                successful_iterations: 0,
                success_rate: 0.0,
                mean_duration: Duration::ZERO,
                p50_duration: Duration::ZERO,
                p90_duration: Duration::ZERO,
                p99_duration: Duration::ZERO,
            };
        }

        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        let mut successful_iterations = 0;
        let mut success_durations = Vec::new();

        for _ in 0..self.iterations {
            let mut task_states = vec![TaskStatus::Skipped; tasks.len()];
            let mut task_end_times = vec![Duration::ZERO; tasks.len()];
            let mut any_failed = false;

            for level in levels {
                for &task_index in level {
                    let task = &tasks[task_index];

                    let upstream_statuses: Vec<TaskStatus> = task
                        .upstreams
                        .iter()
                        .map(|&up_idx| task_states[up_idx])
                        .collect();

                    if task.trigger_rule.should_run(&upstream_statuses) {
                        let mut start_time = Duration::ZERO;
                        for &up_idx in &task.upstreams {
                            start_time = start_time.max(task_end_times[up_idx]);
                        }

                        let (duration, res) = self
                            .activity_mocks
                            .get(&task.activity_name)
                            .map_or((self.default_duration, Ok(())), |mock| mock());

                        task_end_times[task_index] = start_time + duration;

                        if res.is_ok() {
                            task_states[task_index] = TaskStatus::Succeeded;
                        } else {
                            task_states[task_index] = TaskStatus::Failed;
                            any_failed = true;
                        }
                    } else {
                        task_states[task_index] = TaskStatus::Skipped;
                    }
                }
            }

            if !any_failed {
                successful_iterations += 1;
                // Find max end time to get total duration for this iteration
                let total_duration = task_end_times.into_iter().max().unwrap_or(Duration::ZERO);
                success_durations.push(total_duration);
            }
        }

        success_durations.sort_unstable();

        #[allow(clippy::cast_precision_loss)]
        let success_rate = (successful_iterations as f64 / self.iterations as f64) * 100.0;
        let mean_duration = if successful_iterations > 0 {
            let total: Duration = success_durations.iter().sum();
            #[allow(clippy::cast_possible_truncation)]
            {
                total / (successful_iterations as u32)
            }
        } else {
            Duration::ZERO
        };

        let get_percentile = |p: f64| -> Duration {
            if successful_iterations == 0 {
                return Duration::ZERO;
            }
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_sign_loss)]
            let mut idx = (successful_iterations as f64 * p).floor() as usize;
            if idx >= successful_iterations {
                idx = successful_iterations - 1;
            }
            success_durations[idx]
        };

        MonteCarloResult {
            iterations: self.iterations,
            successful_iterations,
            success_rate,
            mean_duration,
            p50_duration: get_percentile(0.50),
            p90_duration: get_percentile(0.90),
            p99_duration: get_percentile(0.99),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::sync::{Arc, Mutex};

    fn activity_a() {}
    fn activity_b() {}

    #[test]
    fn test_monte_carlo_deterministic_mock() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let mc = DagMonteCarlo::new(dag)
            .with_iterations(100)
            .mock_activity("activity_a", || (Duration::from_secs(2), Ok(())))
            .mock_activity("activity_b", || (Duration::from_secs(3), Ok(())));

        let res = mc.run();

        assert_eq!(res.iterations, 100);
        assert_eq!(res.successful_iterations, 100);
        assert!((res.success_rate - 100.0).abs() < f64::EPSILON);
        assert_eq!(res.mean_duration, Duration::from_secs(5));
        assert_eq!(res.p50_duration, Duration::from_secs(5));
        assert_eq!(res.p99_duration, Duration::from_secs(5));
    }

    #[test]
    fn test_monte_carlo_random_failures() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        // 50% chance of failure
        let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(42)));
        let mc =
            DagMonteCarlo::new(dag)
                .with_iterations(1000)
                .mock_activity("activity_a", move || {
                    let mut rng = rng.lock().unwrap();
                    if rng.gen_bool(0.5) {
                        (Duration::from_secs(1), Ok(()))
                    } else {
                        (Duration::from_secs(1), Err("Boom".into()))
                    }
                });

        let res = mc.run();

        assert_eq!(res.iterations, 1000);
        // Approx 500 should succeed.
        assert!(res.successful_iterations >= 450 && res.successful_iterations <= 550);
        assert!(res.success_rate >= 45.0 && res.success_rate <= 55.0);
    }

    #[test]
    fn test_monte_carlo_random_duration() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(42)));
        let mc =
            DagMonteCarlo::new(dag)
                .with_iterations(1000)
                .mock_activity("activity_a", move || {
                    let secs = rng.lock().unwrap().gen_range(1..=10);
                    (Duration::from_secs(secs), Ok(()))
                });

        let res = mc.run();

        assert_eq!(res.successful_iterations, 1000);
        assert!(
            res.mean_duration >= Duration::from_secs(4)
                && res.mean_duration <= Duration::from_secs(6)
        );
        assert!(
            res.p50_duration >= Duration::from_secs(4)
                && res.p50_duration <= Duration::from_secs(6)
        );
        assert!(
            res.p90_duration >= Duration::from_secs(8)
                && res.p90_duration <= Duration::from_secs(10)
        );
        assert!(res.p99_duration == Duration::from_secs(10));
    }
}
