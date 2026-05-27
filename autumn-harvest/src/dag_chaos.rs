//! Chaos engineering analyzer for DAG definitions.
//!
//! Provides utilities to systematically inject failures into a DAG execution
//! and verify the structural resilience of the workflow.

use crate::dag::DagDefinition;
#[cfg(any(test, feature = "testing"))]
use crate::dag_simulator::DagSimulator;
#[cfg(any(test, feature = "testing"))]
use crate::policy::TaskStatus;

/// Result of a single simulated chaos scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChaosScenarioResult {
    /// The name of the activity that was intentionally failed.
    pub failed_activity: String,
    /// The number of tasks that successfully completed despite the failure.
    pub successful_tasks: usize,
    /// The number of tasks that were skipped due to the failure.
    pub skipped_tasks: usize,
    /// The number of tasks that failed (including the injected failure and any cascading failures).
    pub failed_tasks: usize,
}

/// Comprehensive report from a chaos analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChaosResult {
    /// The total number of tasks in the DAG.
    pub total_tasks: usize,
    /// Results for each injected failure scenario.
    pub scenarios: Vec<ChaosScenarioResult>,
    /// The list of activities that, if failed, cause the entire DAG to fail or be skipped (single points of failure).
    pub single_points_of_failure: Vec<String>,
}

/// Analyzer that injects failures into a DAG to evaluate resilience.
pub struct DagChaosAnalyzer {
    dag: DagDefinition,
}

impl DagChaosAnalyzer {
    /// Create a new analyzer for the given DAG definition.
    #[must_use]
    pub const fn new(dag: DagDefinition) -> Self {
        Self { dag }
    }

    /// Run the chaos analysis.
    ///
    /// Systematically fails one task at a time and simulates the DAG execution to see
    /// the cascading effects. Returns a comprehensive `ChaosResult` report.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn analyze(&self) -> ChaosResult {
        let tasks = self.dag.tasks();
        let total_tasks = tasks.len();
        let mut scenarios = Vec::new();
        let mut spofs = Vec::new();

        for target_task in tasks {
            let target_activity = target_task.activity_name.clone();

            let simulator = DagSimulator::new(self.dag.clone())
                .mock_activity(&target_activity, || Err("Chaos Injected".into()));

            let result = simulator.run();

            let mut successful_tasks = 0;
            let mut skipped_tasks = 0;
            let mut failed_tasks = 0;

            for status in &result.task_states {
                match status {
                    TaskStatus::Succeeded => successful_tasks += 1,
                    TaskStatus::Skipped => skipped_tasks += 1,
                    TaskStatus::Failed => failed_tasks += 1,
                }
            }

            scenarios.push(ChaosScenarioResult {
                failed_activity: target_activity.clone(),
                successful_tasks,
                skipped_tasks,
                failed_tasks,
            });

            // If only the failed task ran (and failed) or everything else skipped, it's a SPOF
            // A task is a SPOF if no other tasks can succeed when it fails.
            // Exception: tasks with no downstreams might not be SPOFs for the whole DAG,
            // but for simplicity, we define SPOF as "causes all subsequent tasks to skip/fail".
            if successful_tasks == 0 && total_tasks > 1 {
                spofs.push(target_activity);
            }
        }

        ChaosResult {
            total_tasks,
            scenarios,
            single_points_of_failure: spofs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::policy::TriggerRule;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}

    #[test]
    fn test_linear_dag_chaos() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b).upstream(&a);
        let _c = builder.activity(activity_c).upstream(&b);
        let dag = builder.build().unwrap();

        let analyzer = DagChaosAnalyzer::new(dag);
        let result = analyzer.analyze();

        assert_eq!(result.total_tasks, 3);
        assert_eq!(result.scenarios.len(), 3);

        // Failing A skips B and C. (0 success, 2 skipped, 1 failed)
        assert_eq!(result.scenarios[0].failed_activity, "activity_a");
        assert_eq!(result.scenarios[0].successful_tasks, 0);
        assert_eq!(result.scenarios[0].skipped_tasks, 2);
        assert_eq!(result.scenarios[0].failed_tasks, 1);

        // A is a SPOF because when it fails, nothing else succeeds.
        assert!(
            result
                .single_points_of_failure
                .contains(&"activity_a".to_string())
        );
    }

    #[test]
    fn test_resilient_dag_chaos() {
        let mut builder = DagBuilder::new();
        // A and B run in parallel. C runs if EITHER succeeds.
        let a = builder.activity(activity_a);
        let b = builder.activity(activity_b);
        let _c = builder
            .activity(activity_c)
            .upstream(&a)
            .upstream(&b)
            .trigger_rule(TriggerRule::OneSuccess);
        let dag = builder.build().unwrap();

        let analyzer = DagChaosAnalyzer::new(dag);
        let result = analyzer.analyze();

        assert_eq!(result.total_tasks, 3);

        // Failing A leaves B to succeed, which triggers C.
        // So 2 tasks succeed (B, C), 0 skip, 1 fail (A).
        let scenario_a = result
            .scenarios
            .iter()
            .find(|s| s.failed_activity == "activity_a")
            .unwrap();
        assert_eq!(scenario_a.successful_tasks, 2);
        assert_eq!(scenario_a.failed_tasks, 1);

        // Neither A nor B should be a SPOF.
        assert!(result.single_points_of_failure.is_empty());
    }
}
