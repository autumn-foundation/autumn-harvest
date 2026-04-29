//! In-memory simulator for Directed Acyclic Graphs (DAGs).
//!
//! Provides a fast, deterministic environment to test DAG configurations,
//! evaluate trigger rules, and verify execution ordering without requiring
//! a database or background workers.

use std::collections::HashMap;

use crate::dag::DagDefinition;
use crate::policy::TaskStatus;

/// A synchronous mock function for a DAG activity.
pub type DagActivityMockFn = Box<dyn Fn() -> Result<(), String> + Send + Sync>;

/// The result of a simulated DAG execution.
#[derive(Debug, Clone)]
pub struct DagSimulatorResult {
    /// The final status of each task in the DAG, indexed by their topological order index.
    pub task_states: Vec<TaskStatus>,
}

impl DagSimulatorResult {
    /// Helper to get the state of a specific task by its index.
    #[must_use]
    pub fn get_status(&self, index: usize) -> Option<&TaskStatus> {
        self.task_states.get(index)
    }
}

/// In-memory simulator for executing a compiled `DagDefinition`.
///
/// Executes the DAG topologically level-by-level, evaluating trigger rules
/// for each task. Activity behavior is stubbed via `.mock_activity()`.
pub struct DagSimulator {
    dag: DagDefinition,
    activity_mocks: HashMap<String, DagActivityMockFn>,
}

impl DagSimulator {
    /// Create a new DAG simulator for the given definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_mocks: HashMap::new(),
        }
    }

    /// Register a mock implementation for an activity by name.
    ///
    /// When the DAG schedules this activity, the mock is called
    /// synchronously. If it returns `Ok`, the task succeeds. If `Err`, it fails.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: &str, mock: F) -> Self
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        self.activity_mocks.insert(name.to_string(), Box::new(mock));
        self
    }

    /// Run the DAG to completion.
    ///
    /// Evaluates `execution_levels` in order. For each task, evaluates
    /// its `trigger_rule` against the states of its upstreams. If triggered,
    /// invokes the registered mock (or defaults to success if unmocked).
    #[must_use]
    pub fn run(&self) -> DagSimulatorResult {
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        // Initialize all states as Skipped (or we could use a pending state if needed)
        // We will assign the final state when we visit the node.
        let mut task_states = vec![TaskStatus::Skipped; tasks.len()];

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
                    // Task triggered! Run the mock.
                    let mock_res = self
                        .activity_mocks
                        .get(&task.activity_name)
                        .map_or(Ok(()), |mock| mock());

                    task_states[task_index] = match mock_res {
                        Ok(()) => TaskStatus::Succeeded,
                        Err(_) => TaskStatus::Failed,
                    };
                } else {
                    // Task not triggered, remains Skipped.
                    task_states[task_index] = TaskStatus::Skipped;
                }
            }
        }

        DagSimulatorResult { task_states }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::policy::{TaskStatus, TriggerRule};

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_linear_dag_success() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let sim = DagSimulator::new(dag);
        let result = sim.run();

        assert_eq!(result.task_states[a.index()], TaskStatus::Succeeded);
        assert_eq!(result.task_states[b.index()], TaskStatus::Succeeded);
    }

    #[test]
    fn test_upstream_failure_skips_downstream_all_success() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a); // Default rule is AllSuccess
        let dag = builder.build().unwrap();

        let sim = DagSimulator::new(dag).mock_activity("activity_a", || Err("Fail".into()));
        let result = sim.run();

        assert_eq!(result.task_states[a.index()], TaskStatus::Failed);
        assert_eq!(result.task_states[b.index()], TaskStatus::Skipped);
    }

    #[test]
    fn test_upstream_failure_triggers_downstream_all_done() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder
            .activity(activity_b)
            .upstream(&a)
            .trigger_rule(TriggerRule::AllDone);
        let dag = builder.build().unwrap();

        let sim = DagSimulator::new(dag).mock_activity("activity_a", || Err("Fail".into()));
        let result = sim.run();

        assert_eq!(result.task_states[a.index()], TaskStatus::Failed);
        assert_eq!(result.task_states[b.index()], TaskStatus::Succeeded); // Mock defaults to Ok
    }

    #[test]
    fn test_complex_trigger_rules() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a); // Index 0
        let b = builder.activity(activity_b); // Index 1

        // C waits for A or B to succeed
        let c = builder
            .activity(activity_c)
            .upstream(&a)
            .upstream(&b)
            .trigger_rule(TriggerRule::OneSuccess);

        let dag = builder.build().unwrap();

        // If A fails but B succeeds, C should run.
        let sim = DagSimulator::new(dag).mock_activity("activity_a", || Err("Fail".into()));
        let result = sim.run();

        assert_eq!(result.task_states[a.index()], TaskStatus::Failed);
        assert_eq!(result.task_states[b.index()], TaskStatus::Succeeded);
        assert_eq!(result.task_states[c.index()], TaskStatus::Succeeded);
    }
}
