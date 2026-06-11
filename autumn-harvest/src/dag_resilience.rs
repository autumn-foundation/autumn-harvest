//! DAG Resilience Analyzer.
//!
//! Evaluates the robustness of a DAG by systematically simulating the failure
//! of individual activities to determine their "blast radius" and identify
//! Single Points of Failure (SPOF).

use crate::dag::DagDefinition;
use crate::dag_simulator::DagSimulator;
use crate::policy::TaskStatus;
use std::collections::HashSet;

/// The result of analyzing a single activity's failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityFailureImpact {
    /// The name of the failed activity.
    pub failed_activity_name: String,
    /// The number of tasks that were skipped or failed as a result.
    pub blast_radius_size: usize,
    /// Whether this activity's failure caused all sink nodes (final outcomes) to fail or be skipped.
    pub is_spof: bool,
}

/// Result of a DAG resilience analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResilienceReport {
    /// The impact of each individual activity failing.
    pub impacts: Vec<ActivityFailureImpact>,
    /// A list of activity names that are Single Points of Failure.
    pub single_points_of_failure: Vec<String>,
}

/// Analyzer to determine the resilience of a DAG to activity failures.
pub struct DagResilienceAnalyzer {
    dag: DagDefinition,
}

impl DagResilienceAnalyzer {
    /// Create a new analyzer for the given DAG definition.
    #[must_use]
    pub const fn new(dag: DagDefinition) -> Self {
        Self { dag }
    }

    /// Analyze the DAG by failing each activity one by one.
    ///
    /// # Panics
    /// Panics if the internal DAG simulator produces results with a different number of tasks than the DAG definition.
    #[must_use]
    pub fn analyze(&self) -> ResilienceReport {
        let tasks = self.dag.tasks();
        if tasks.is_empty() {
            return ResilienceReport {
                impacts: Vec::new(),
                single_points_of_failure: Vec::new(),
            };
        }

        // Identify sink nodes (nodes with no downstreams)
        let mut is_sink = vec![true; tasks.len()];
        for task in tasks {
            for &up_idx in &task.upstreams {
                is_sink[up_idx] = false;
            }
        }
        let sink_indices: Vec<usize> = is_sink
            .into_iter()
            .enumerate()
            .filter_map(|(i, b)| b.then_some(i))
            .collect();

        // Baseline: what happens if nothing fails?
        let baseline_sim = DagSimulator::new(self.dag.clone());
        let baseline_result = baseline_sim.run();

        // Find all unique activity names
        let mut unique_activities = HashSet::new();
        for task in tasks {
            unique_activities.insert(task.activity_name.clone());
        }

        let mut impacts = Vec::new();
        let mut spofs = Vec::new();

        for target_activity in &unique_activities {
            let target_clone = target_activity.clone();
            let sim = DagSimulator::new(self.dag.clone())
                .mock_activity(&target_clone, || Err("Injected Failure".into()));

            let result = sim.run();

            let mut blast_radius_size = 0;
            for i in 0..tasks.len() {
                let baseline_status = baseline_result.get_status(i).unwrap();
                let new_status = result.get_status(i).unwrap();

                if matches!(baseline_status, TaskStatus::Succeeded)
                    && !matches!(new_status, TaskStatus::Succeeded)
                {
                    blast_radius_size += 1;
                }
            }

            let mut all_sinks_failed = true;
            for &sink_idx in &sink_indices {
                if matches!(result.get_status(sink_idx).unwrap(), TaskStatus::Succeeded) {
                    all_sinks_failed = false;
                    break;
                }
            }

            impacts.push(ActivityFailureImpact {
                failed_activity_name: target_activity.clone(),
                blast_radius_size,
                is_spof: all_sinks_failed,
            });

            if all_sinks_failed {
                spofs.push(target_activity.clone());
            }
        }

        impacts.sort_by(|a, b| a.failed_activity_name.cmp(&b.failed_activity_name));
        spofs.sort();

        ResilienceReport {
            impacts,
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
    fn activity_d() {}

    #[test]
    fn test_linear_dag_spof() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let analyzer = DagResilienceAnalyzer::new(dag);
        let report = analyzer.analyze();

        // In A -> B, if A fails, B skips. Blast radius = 2. Sinks fail.
        // If B fails, B fails. Blast radius = 1. Sinks fail.
        assert_eq!(
            report.single_points_of_failure,
            vec!["activity_a", "activity_b"]
        );

        let impact_a = report
            .impacts
            .iter()
            .find(|i| i.failed_activity_name == "activity_a")
            .unwrap();
        assert_eq!(impact_a.blast_radius_size, 2);

        let impact_b = report
            .impacts
            .iter()
            .find(|i| i.failed_activity_name == "activity_b")
            .unwrap();
        assert_eq!(impact_b.blast_radius_size, 1);
    }

    #[test]
    fn test_resilient_dag_all_done() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder
            .activity(activity_b)
            .upstream(&a)
            .trigger_rule(TriggerRule::AllDone);
        let dag = builder.build().unwrap();

        let analyzer = DagResilienceAnalyzer::new(dag);
        let report = analyzer.analyze();

        // A -> B (AllDone). If A fails, B still succeeds. Sink (B) succeeds.
        // So A is NOT a SPOF.
        assert_eq!(report.single_points_of_failure, vec!["activity_b"]);

        let impact_a = report
            .impacts
            .iter()
            .find(|i| i.failed_activity_name == "activity_a")
            .unwrap();
        assert_eq!(impact_a.blast_radius_size, 1);
        assert!(!impact_a.is_spof);
    }

    #[test]
    fn test_one_success_fallback() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let b1 = builder.activity(activity_b).upstream(&start);
        let b2 = builder.activity(activity_c).upstream(&start);
        let _end = builder
            .activity(activity_d)
            .upstream(&b1)
            .upstream(&b2)
            .trigger_rule(TriggerRule::OneSuccess);
        let dag = builder.build().unwrap();

        let analyzer = DagResilienceAnalyzer::new(dag);
        let report = analyzer.analyze();

        // 'activity_a' is SPOF because if it fails, b1 and b2 skip, so 'end' skips.
        // 'activity_d' is SPOF because if it fails, the only sink fails.
        // 'activity_b' and 'activity_c' are NOT SPOF. If one fails, the other succeeds, so 'end' runs.
        assert_eq!(
            report.single_points_of_failure,
            vec!["activity_a", "activity_d"]
        );

        let impact_b = report
            .impacts
            .iter()
            .find(|i| i.failed_activity_name == "activity_b")
            .unwrap();
        assert_eq!(impact_b.blast_radius_size, 1);
        assert!(!impact_b.is_spof);
    }
}
