//! Critical Path Analyzer for DAG Definitions.
//!
//! Provides utilities to calculate the longest execution path through a DAG,
//! identifying the bottleneck tasks that determine the overall execution duration.

use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::time::Duration;

/// Result of a critical path analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriticalPathResult {
    /// The total estimated duration of the critical path.
    pub total_duration: Duration,
    /// The topological indices of the tasks that form the critical path.
    pub path_indices: Vec<usize>,
    /// The names of the activities that form the critical path.
    pub path_names: Vec<String>,
}

/// Analyzer to determine the critical path of a DAG.
///
/// The critical path represents the sequence of dependent tasks that determines the total execution
/// time of the graph. Understanding the critical path is essential for optimizing workflow performance.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::dag::DagBuilder;
/// use autumn_harvest::critical_path::CriticalPathAnalyzer;
/// use std::time::Duration;
///
/// const fn fast_task() {}
/// const fn slow_task() {}
/// const fn final_task() {}
///
/// let mut builder = DagBuilder::new();
/// let fast = builder.activity(fast_task);
/// let slow = builder.activity(slow_task);
/// let fin = builder.activity(final_task).upstream(&fast).upstream(&slow);
/// let dag = builder.build().unwrap();
///
/// let analyzer = CriticalPathAnalyzer::new(dag)
///     .mock_duration("fast_task", Duration::from_secs(1))
///     .mock_duration("slow_task", Duration::from_secs(10))
///     .mock_duration("final_task", Duration::from_secs(2));
///
/// let result = analyzer.analyze();
/// assert_eq!(result.total_duration, Duration::from_secs(12));
/// assert_eq!(result.path_names, vec!["slow_task", "final_task"]);
/// ```
pub struct CriticalPathAnalyzer {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
}

impl CriticalPathAnalyzer {
    /// Create a new analyzer for the given DAG definition.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Set a default duration for activities whose duration is not explicitly mocked.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Mock the expected duration of an activity by name.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Calculate the critical path of the DAG.
    ///
    /// The critical path is the longest path through the graph from any source node
    /// to any sink node, which dictates the minimum possible execution time of the entire DAG.
    #[must_use]
    pub fn analyze(&self) -> CriticalPathResult {
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        if tasks.is_empty() {
            return CriticalPathResult {
                total_duration: Duration::ZERO,
                path_indices: Vec::new(),
                path_names: Vec::new(),
            };
        }

        let mut distances = vec![Duration::ZERO; tasks.len()];
        let mut predecessors = vec![None; tasks.len()];

        for level in levels {
            for &task_index in level {
                let task = &tasks[task_index];
                let duration = task.start_to_close.unwrap_or_else(|| {
                    self.activity_durations
                        .get(&task.activity_name)
                        .copied()
                        .unwrap_or(self.default_duration)
                });

                // Find the maximum distance among upstreams
                let mut max_upstream_dist = Duration::ZERO;
                let mut best_pred = None;

                for &up_idx in &task.upstreams {
                    if distances[up_idx] >= max_upstream_dist {
                        // >= because we want the last one in case of tie, or simply just >
                        // Actually, just > is fine.
                        if distances[up_idx] > max_upstream_dist || best_pred.is_none() {
                            max_upstream_dist = distances[up_idx];
                            best_pred = Some(up_idx);
                        }
                    }
                }

                distances[task_index] = max_upstream_dist + duration;
                predecessors[task_index] = best_pred;
            }
        }

        // Identify sink nodes (nodes with no downstreams)
        let mut is_sink = vec![true; tasks.len()];
        for task in tasks {
            for &up_idx in &task.upstreams {
                is_sink[up_idx] = false;
            }
        }

        // Find the sink node with the maximum total distance
        let mut max_dist = Duration::ZERO;
        let mut end_node = None;

        for (i, &dist) in distances.iter().enumerate() {
            if is_sink[i] && (dist > max_dist || end_node.is_none()) {
                max_dist = dist;
                end_node = Some(i);
            }
        }

        let mut path_indices = Vec::new();
        let mut current = end_node;

        while let Some(node) = current {
            path_indices.push(node);
            current = predecessors[node];
        }

        path_indices.reverse();

        let path_names = path_indices
            .iter()
            .map(|&i| tasks[i].activity_name.clone())
            .collect();

        CriticalPathResult {
            total_duration: max_dist,
            path_indices,
            path_names,
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
    fn test_empty_dag() {
        let builder = DagBuilder::new();
        let dag = builder.build().unwrap();
        let analyzer = CriticalPathAnalyzer::new(dag);
        let result = analyzer.analyze();

        assert_eq!(result.total_duration, Duration::ZERO);
        assert!(result.path_indices.is_empty());
    }

    #[test]
    fn test_linear_dag() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a);
        let _b = builder.activity(activity_b).upstream(&a);
        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag)
            .mock_duration("activity_a", Duration::from_secs(10))
            .mock_duration("activity_b", Duration::from_secs(5));

        let result = analyzer.analyze();

        assert_eq!(result.total_duration, Duration::from_secs(15));
        assert_eq!(result.path_indices, vec![0, 1]);
        assert_eq!(
            result.path_names,
            vec!["activity_a".to_string(), "activity_b".to_string()]
        );
    }

    #[test]
    fn test_branching_dag() {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a); // 0

        let branch1 = builder.activity(activity_b).upstream(&start); // 1, short
        let branch2 = builder.activity(activity_c).upstream(&start); // 2, long

        let _end = builder
            .activity(activity_d)
            .upstream(&branch1)
            .upstream(&branch2); // 3

        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag)
            .mock_duration("activity_a", Duration::from_secs(1))
            .mock_duration("activity_b", Duration::from_secs(2)) // branch 1 = 3s total
            .mock_duration("activity_c", Duration::from_secs(10)) // branch 2 = 11s total
            .mock_duration("activity_d", Duration::from_secs(1)); // end = 12s total

        let result = analyzer.analyze();

        assert_eq!(result.total_duration, Duration::from_secs(12));
        assert_eq!(result.path_indices, vec![0, 2, 3]);
        assert_eq!(
            result.path_names,
            vec![
                "activity_a".to_string(),
                "activity_c".to_string(),
                "activity_d".to_string(),
            ]
        );
    }

    #[test]
    fn test_start_to_close_override() {
        let mut builder = DagBuilder::new();
        let _a = builder
            .activity(activity_a)
            .start_to_close(Duration::from_secs(42));
        let dag = builder.build().unwrap();

        let analyzer =
            CriticalPathAnalyzer::new(dag).mock_duration("activity_a", Duration::from_secs(1)); // mock should be ignored

        let result = analyzer.analyze();

        assert_eq!(result.total_duration, Duration::from_secs(42));
    }
}
