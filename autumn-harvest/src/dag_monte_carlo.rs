//! Monte Carlo simulation for Directed Acyclic Graphs (DAGs).
//!
//! Provides a stochastic environment to test DAG configurations,
//! evaluate execution timelines under randomized conditions, and model
//! SLA breach probabilities.

use crate::dag::DagDefinition;
use crate::policy::TaskStatus;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for a Monte Carlo simulation run.
#[derive(Debug, Clone)]
pub struct MonteCarloConfig {
    /// Number of iterations to run.
    pub iterations: usize,
    /// Optional global Service Level Agreement (SLA) duration limit.
    pub global_sla: Option<Duration>,
}

impl Default for MonteCarloConfig {
    fn default() -> Self {
        Self {
            iterations: 1000,
            global_sla: None,
        }
    }
}

/// Stochastic properties for a single task.
#[derive(Debug, Clone)]
pub struct TaskSimulationModel {
    /// Base duration of the task.
    pub base_duration: Duration,
    /// Maximum additional jitter duration.
    pub max_jitter: Duration,
    /// Probability of task failure (0.0 to 1.0).
    pub failure_probability: f64,
}

impl Default for TaskSimulationModel {
    fn default() -> Self {
        Self {
            base_duration: Duration::from_secs(1),
            max_jitter: Duration::ZERO,
            failure_probability: 0.0,
        }
    }
}

impl TaskSimulationModel {
    /// Create a new model with specific properties.
    #[must_use]
    pub const fn new(
        base_duration: Duration,
        max_jitter: Duration,
        failure_probability: f64,
    ) -> Self {
        Self {
            base_duration,
            max_jitter,
            failure_probability,
        }
    }

    /// Sample a randomized duration and outcome.
    fn sample(&self, rng: &mut impl Rng) -> (Duration, bool) {
        let jitter = if self.max_jitter > Duration::ZERO {
            // Random duration up to max_jitter
            // u64::MAX nanos is approx 584 years, so this is safe for reasonable durations
            let jitter_nanos = rng.gen_range(0..=self.max_jitter.as_nanos());
            // Safe unwrap because the random value is bounded by a valid Duration
            Duration::from_nanos(u64::try_from(jitter_nanos).unwrap_or(u64::MAX))
        } else {
            Duration::ZERO
        };

        let duration = self.base_duration.saturating_add(jitter);
        let success = rng.gen_range(0.0..1.0) >= self.failure_probability;

        (duration, success)
    }
}

/// The result of a Monte Carlo simulation.
#[derive(Debug, Clone)]
pub struct MonteCarloResult {
    /// Total number of iterations run.
    pub total_iterations: usize,
    /// Number of iterations that succeeded.
    pub successful_iterations: usize,
    /// Success rate (0.0 to 1.0).
    pub success_rate: f64,
    /// Mean completion time of successful iterations.
    pub mean_duration: Duration,
    /// P99 completion time of successful iterations (or 0 if none).
    pub p99_duration: Duration,
    /// Number of successful iterations that breached the SLA.
    pub sla_breaches: usize,
    /// Rate of successful iterations that breached the SLA (0.0 to 1.0).
    pub sla_breach_rate: f64,
}

/// In-memory stochastic simulator for DAGs.
pub struct DagMonteCarlo {
    dag: DagDefinition,
    config: MonteCarloConfig,
    task_models: HashMap<String, TaskSimulationModel>,
    default_model: TaskSimulationModel,
}

impl DagMonteCarlo {
    /// Create a new Monte Carlo simulator for the given definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            config: MonteCarloConfig::default(),
            task_models: HashMap::new(),
            default_model: TaskSimulationModel::default(),
        }
    }

    /// Set the simulation configuration.
    #[must_use]
    pub const fn with_config(mut self, config: MonteCarloConfig) -> Self {
        self.config = config;
        self
    }

    /// Set a default model for unconfigured activities.
    #[must_use]
    pub const fn with_default_model(mut self, model: TaskSimulationModel) -> Self {
        self.default_model = model;
        self
    }

    /// Configure a specific activity by name.
    #[must_use]
    pub fn model_activity(mut self, name: &str, model: TaskSimulationModel) -> Self {
        self.task_models.insert(name.to_string(), model);
        self
    }

    /// Run the Monte Carlo simulation.
    #[must_use]
    #[allow(
        clippy::too_many_lines,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn run(&self) -> MonteCarloResult {
        let mut rng = rand::thread_rng();
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        let mut successful_iterations = 0;
        let mut sla_breaches = 0;
        let mut successful_durations = Vec::with_capacity(self.config.iterations);

        if tasks.is_empty() {
            return MonteCarloResult {
                total_iterations: self.config.iterations,
                successful_iterations: self.config.iterations,
                success_rate: 1.0,
                mean_duration: Duration::ZERO,
                p99_duration: Duration::ZERO,
                sla_breaches: 0,
                sla_breach_rate: 0.0,
            };
        }

        for _ in 0..self.config.iterations {
            let mut task_states = vec![TaskStatus::Skipped; tasks.len()];
            let mut task_end_times = vec![None; tasks.len()];
            let mut run_failed = false;
            let mut max_end_time = Duration::ZERO;

            for level in levels {
                for &task_index in level {
                    let task = &tasks[task_index];

                    // Gather states of all upstream dependencies
                    let upstream_statuses: Vec<TaskStatus> = task
                        .upstreams
                        .iter()
                        .map(|&up_idx| task_states[up_idx])
                        .collect();

                    // Evaluate trigger rule
                    if task.trigger_rule.should_run(&upstream_statuses) {
                        let model = self
                            .task_models
                            .get(&task.activity_name)
                            .unwrap_or(&self.default_model);

                        let (duration, success) = model.sample(&mut rng);

                        if success {
                            task_states[task_index] = TaskStatus::Succeeded;

                            // Calculate start time based on upstreams
                            let mut start_time = Duration::ZERO;
                            for &up_idx in &task.upstreams {
                                if let Some(up_end) = task_end_times[up_idx] {
                                    start_time = start_time.max(up_end);
                                }
                            }

                            let end_time = start_time + duration;
                            task_end_times[task_index] = Some(end_time);
                            max_end_time = max_end_time.max(end_time);
                        } else {
                            task_states[task_index] = TaskStatus::Failed;
                            // A simple model: if any task fails and we don't have
                            // complex error handling, we consider the iteration failed
                            // (or at least we track if the final critical tasks failed).
                            // For this basic simulator, we track if *any* task fails that
                            // was supposed to run.
                            run_failed = true;
                        }
                    } else {
                        // Task not triggered, remains Skipped.
                        task_states[task_index] = TaskStatus::Skipped;
                    }
                }
            }

            // Check if any leaf nodes (tasks with no downstreams) failed
            // A more robust check might look at specific terminal nodes,
            // but for simple DAGs, `run_failed` works.
            if !run_failed {
                successful_iterations += 1;
                successful_durations.push(max_end_time);

                if let Some(sla) = self.config.global_sla
                    && max_end_time > sla
                {
                    sla_breaches += 1;
                }
            }
        }

        let success_rate = if self.config.iterations > 0 {
            successful_iterations as f64 / self.config.iterations as f64
        } else {
            0.0
        };

        let mean_duration = if successful_iterations > 0 {
            let total_nanos: u128 = successful_durations
                .iter()
                .map(std::time::Duration::as_nanos)
                .sum();
            let mean_nanos = total_nanos / (successful_iterations as u128);
            Duration::from_nanos(u64::try_from(mean_nanos).unwrap_or(u64::MAX))
        } else {
            Duration::ZERO
        };

        let p99_duration = if successful_iterations > 0 {
            successful_durations.sort_unstable();
            let idx = (successful_iterations as f64 * 0.99).ceil() as usize;
            let idx = idx.saturating_sub(1).min(successful_iterations - 1);
            successful_durations[idx]
        } else {
            Duration::ZERO
        };

        let sla_breach_rate = if successful_iterations > 0 {
            sla_breaches as f64 / successful_iterations as f64
        } else {
            0.0
        };

        MonteCarloResult {
            total_iterations: self.config.iterations,
            successful_iterations,
            success_rate,
            mean_duration,
            p99_duration,
            sla_breaches,
            sla_breach_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}
    fn activity_d() {}

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_monte_carlo_linear() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let sim = DagMonteCarlo::new(dag)
            .with_config(MonteCarloConfig {
                iterations: 100,
                global_sla: Some(Duration::from_secs(10)),
            })
            .model_activity(
                "activity_a",
                TaskSimulationModel::new(Duration::from_secs(2), Duration::ZERO, 0.0),
            )
            .model_activity(
                "activity_b",
                TaskSimulationModel::new(Duration::from_secs(3), Duration::ZERO, 0.0),
            );

        let result = sim.run();

        assert_eq!(result.total_iterations, 100);
        assert_eq!(result.successful_iterations, 100);
        assert_eq!(result.success_rate, 1.0);
        assert_eq!(result.mean_duration, Duration::from_secs(5));
        assert_eq!(result.p99_duration, Duration::from_secs(5));
        assert_eq!(result.sla_breaches, 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_monte_carlo_parallel() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let branch1 = builder.activity(activity_b).upstream(&start);
        let branch2 = builder.activity(activity_c).upstream(&start);
        let _end = builder
            .activity(activity_d)
            .upstream(&branch1)
            .upstream(&branch2);
        let dag = builder.build().unwrap();

        let sim = DagMonteCarlo::new(dag)
            .with_config(MonteCarloConfig {
                iterations: 50,
                global_sla: None,
            })
            .model_activity(
                "activity_a",
                TaskSimulationModel::new(Duration::from_secs(1), Duration::ZERO, 0.0),
            )
            .model_activity(
                "activity_b",
                TaskSimulationModel::new(Duration::from_secs(4), Duration::ZERO, 0.0),
            )
            .model_activity(
                "activity_c",
                TaskSimulationModel::new(Duration::from_secs(2), Duration::ZERO, 0.0),
            )
            .model_activity(
                "activity_d",
                TaskSimulationModel::new(Duration::from_secs(1), Duration::ZERO, 0.0),
            );

        let result = sim.run();

        // Critical path: A (1) -> B (4) -> D (1) = 6s
        assert_eq!(result.success_rate, 1.0);
        assert_eq!(result.mean_duration, Duration::from_secs(6));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_monte_carlo_failure() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        let sim = DagMonteCarlo::new(dag)
            .with_config(MonteCarloConfig {
                iterations: 1000,
                global_sla: None,
            })
            // 100% failure rate
            .model_activity(
                "activity_a",
                TaskSimulationModel::new(Duration::from_secs(1), Duration::ZERO, 1.0),
            );

        let result = sim.run();

        assert_eq!(result.successful_iterations, 0);
        assert_eq!(result.success_rate, 0.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_monte_carlo_jitter() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a);
        let dag = builder.build().unwrap();

        let sim = DagMonteCarlo::new(dag)
            .with_config(MonteCarloConfig {
                iterations: 1000,
                global_sla: Some(Duration::from_millis(1500)),
            })
            // Base 1s, jitter up to 1s (total 1s-2s uniform)
            .model_activity(
                "activity_a",
                TaskSimulationModel::new(Duration::from_secs(1), Duration::from_secs(1), 0.0),
            );

        let result = sim.run();

        assert_eq!(result.success_rate, 1.0);
        assert!(result.mean_duration > Duration::from_millis(1400));
        assert!(result.mean_duration < Duration::from_millis(1600));
        assert!(result.p99_duration > Duration::from_millis(1900));
        // SLA breach rate should be ~50%
        assert!(result.sla_breach_rate > 0.4);
        assert!(result.sla_breach_rate < 0.6);
    }
}
