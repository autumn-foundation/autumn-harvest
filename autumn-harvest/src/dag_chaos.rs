//! Chaos engineering simulator for Directed Acyclic Graphs (DAGs).
//!
//! Repeatedly simulates DAG execution with injected failures to calculate
//! task success rates and identify critical failure paths.

use crate::dag::DagDefinition;
use crate::dag_simulator::DagSimulator;
use crate::policy::TaskStatus;
use std::collections::HashMap;

/// A simple deterministic linear congruential generator (LCG).
struct Lcg(u32);

impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        #[allow(clippy::cast_precision_loss)]
        let numerator = self.0 as f32;
        #[allow(clippy::cast_precision_loss)]
        let denominator = u32::MAX as f32;
        numerator / denominator
    }
}

/// Statistics for a single task across all chaos iterations.
#[derive(Debug, Clone, Default)]
pub struct TaskChaosStats {
    /// Number of times the task succeeded.
    pub successes: usize,
    /// Number of times the task failed (due to chaos injection or logic).
    pub failures: usize,
    /// Number of times the task was skipped (e.g. upstream failure).
    pub skips: usize,
}

/// The result of a chaos simulation run.
#[derive(Debug, Clone)]
pub struct ChaosReport {
    /// Total number of iterations executed.
    pub total_iterations: usize,
    /// Statistics for each task, indexed by their topological order index.
    pub task_stats: Vec<TaskChaosStats>,
}

/// Repeatedly simulates a `DagDefinition` while randomly injecting failures.
pub struct DagChaosSimulator {
    dag: DagDefinition,
    iterations: usize,
    base_failure_rate: f32,
    task_failure_rates: HashMap<String, f32>,
    seed: u32,
}

impl DagChaosSimulator {
    /// Create a new chaos simulator with default settings.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            iterations: 100,
            base_failure_rate: 0.1,
            task_failure_rates: HashMap::new(),
            seed: 42,
        }
    }

    /// Set the number of simulation iterations.
    #[must_use]
    pub const fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    /// Set the global probability (0.0 to 1.0) of any task failing.
    #[must_use]
    pub const fn with_base_failure_rate(mut self, rate: f32) -> Self {
        self.base_failure_rate = rate;
        self
    }

    /// Override the failure probability for a specific task by name.
    #[must_use]
    pub fn with_task_failure_rate(mut self, task_name: &str, rate: f32) -> Self {
        self.task_failure_rates.insert(task_name.to_string(), rate);
        self
    }

    /// Run the chaos simulation and aggregate the results.
    #[must_use]
    pub fn run(&self) -> ChaosReport {
        let mut rng = Lcg(self.seed);
        let num_tasks = self.dag.tasks().len();
        let mut stats = vec![TaskChaosStats::default(); num_tasks];

        for _ in 0..self.iterations {
            // Clone the DAG for this iteration
            let mut sim = DagSimulator::new(self.dag.clone());

            // Register a mock for every task that injects failures based on its rate
            for task in self.dag.tasks() {
                let rate = *self
                    .task_failure_rates
                    .get(&task.activity_name)
                    .unwrap_or(&self.base_failure_rate);
                let random_val = rng.next_f32();
                let should_fail = random_val < rate;

                if should_fail {
                    sim = sim.mock_activity(&task.activity_name, || {
                        Err("Chaos Injected Failure".into())
                    });
                } else {
                    sim = sim.mock_activity(&task.activity_name, || Ok(()));
                }
            }

            let result = sim.run();

            for (i, status) in result.task_states.iter().enumerate() {
                match status {
                    TaskStatus::Succeeded => stats[i].successes += 1,
                    TaskStatus::Failed => stats[i].failures += 1,
                    TaskStatus::Skipped => stats[i].skips += 1,
                }
            }
        }

        ChaosReport {
            total_iterations: self.iterations,
            task_stats: stats,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    const fn activity_a() {}
    const fn activity_b() {}

    #[test]
    fn test_chaos_zero_failure() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let report = DagChaosSimulator::new(dag)
            .with_iterations(10)
            .with_base_failure_rate(0.0)
            .run();

        assert_eq!(report.task_stats[a.index()].successes, 10);
        assert_eq!(report.task_stats[b.index()].successes, 10);
    }

    #[test]
    fn test_chaos_total_failure() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let report = DagChaosSimulator::new(dag)
            .with_iterations(10)
            .with_base_failure_rate(1.0)
            .run();

        assert_eq!(report.task_stats[a.index()].failures, 10);
        assert_eq!(report.task_stats[b.index()].skips, 10); // Skipped due to A failing
    }
}
