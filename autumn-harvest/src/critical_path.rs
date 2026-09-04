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

/// Below this many distinct `activity_durations` entries, [`CriticalPathAnalyzer::analyze`]
/// resolves each task's duration via a linear scan of a flat slice instead of
/// a `HashMap` probe; at or above it, scanning is worse (up to `O(tasks *
/// names)` for a pathologically high-cardinality caller). See the doc
/// comment at the scan's call site for the measured crossover this was set
/// below -- **the binding constraint is the miss (`default_duration`
/// fallback) case, not the hit case**: a miss always scans every entry
/// (no early exit is possible), while a hit terminates early on average, so
/// the miss-case crossover is substantially lower and is what this limit
/// must respect to guarantee "never worse than the `HashMap` baseline" for
/// every caller, not just one with a 100%-hit-rate mock set.
const LINEAR_SCAN_CARDINALITY_LIMIT: usize = 5;

/// Minimum number of tasks that must actually need a name-based duration
/// lookup before [`CriticalPathAnalyzer::analyze`] builds the flat scan
/// table at all. Building it -- a `HashMap::iter()` walk, a heap
/// allocation and a sort -- costs more than just doing a handful of
/// `HashMap::get` calls directly when there are only a few lookups to
/// amortize that cost against (a small DAG analyzed repeatedly, not the
/// wide-fan-out shape this optimization targets). See the doc comment at
/// the scan's call site for the measured crossover this was set above.
const MIN_LOOKUPS_TO_AMORTIZE_TABLE_BUILD: usize = 20;

/// Analyzer to determine the critical path of a DAG.
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

        // `activity_durations` is keyed by activity NAME, not by task, so a
        // DAG whose tasks reuse a small set of activity types (a wide
        // fan-out map stage running the same handful of activities, the
        // common case `mock_duration`'s own doc example matches) looks up
        // the SAME few keys once per task. Hashing a `String` key with
        // `HashMap`'s default SipHash on every one of those repeat lookups
        // measurably dominates `analyze`'s own cost (see
        // `benches/critical_path_profile.rs`).
        //
        // Measured crossover (standalone harness, `HashMap<String, Duration>`
        // vs `Vec<(&str, Duration)>`, 80,000 lookups, callgrind Ir), for BOTH
        // regimes `activity_durations` traffic can be:
        //
        // - All-hit (every task's name is mocked), `activity_shard_NNNN`-shaped
        //   keys: 8 names, linear 15.2M vs hash 26.0M; 16 names, roughly at
        //   parity (26.2M vs 26.0M); 24+ names, linear already loses (36.3M vs
        //   26.0M).
        // - All-miss (`default_duration` fallback; same-length/shape keys so
        //   `str` equality cannot short-circuit on length -- the honest worst
        //   case, since a miss must walk every entry): 5 names, linear 17.8M
        //   vs hash 23.3M; parity at 7 names (23.40M vs 23.41M); 8+ names,
        //   linear already loses (26.0M vs 23.5M).
        //
        // The miss regime's crossover (~7) is the binding constraint, not the
        // hit regime's (~16): a miss always scans every entry (no early exit
        // is possible), so a caller that mocks only some of its activities --
        // entirely ordinary usage, not a pathological input -- pays the
        // miss cost on every unmocked task. `LINEAR_SCAN_CARDINALITY_LIMIT`
        // is set with margin below the miss crossover, so this optimization
        // only ever engages where it is a clear win under EITHER regime and
        // always falls back to the original (never-worse-than-baseline)
        // `HashMap` path otherwise. Behaviorally identical either way: both
        // the `HashMap` and the collected slice hold each name at most once,
        // so lookup results never change.
        //
        // Three more conditions guard the table so it costs nothing when it
        // would go unused, be unamortized, or be measured unreliably:
        //
        // - A task with its own `start_to_close` never consults
        //   `activity_durations` at all (`unwrap_or_else`'s closure short
        //   circuits). The ORIGINAL code paid zero activity-durations cost
        //   for such a task, so a caller whose tasks are all overridden --
        //   e.g. `test_start_to_close_override`'s own shape -- must still
        //   pay zero.
        // - Building the table -- a `HashMap::iter()` walk, a heap
        //   allocation and a sort -- has its own fixed cost, paid once per
        //   `analyze()` call regardless of how many tasks it then serves.
        //   Measured (standalone harness, cardinality 5 -- the maximum this
        //   optimization allows -- 80,000 total lookups split across
        //   repeated `analyze()`-shaped calls of varying size): at 5
        //   lookups needed per call, build+scan costs 23.6M Ir vs the
        //   direct-`HashMap` baseline's 19.2M (+23%, a REGRESSION); at 8,
        //   parity (18.6M vs 19.1M); at 10+, a clear win (17.6M vs 19.1M,
        //   widening). `MIN_LOOKUPS_TO_AMORTIZE_TABLE_BUILD` is set with
        //   margin above that measured crossover, so a small DAG analyzed
        //   repeatedly -- not the wide-fan-out shape this optimization
        //   targets -- still takes the original, always-safe path.
        // - `HashMap::iter()`'s order is randomized per process
        //   (`RandomState`), so collecting it directly would make the
        //   linear scan's hit position -- and therefore instruction counts
        //   -- vary run to run for reasons having nothing to do with the
        //   code, undermining the deterministic-counter evidence this
        //   optimization is justified by. Sorting by name fixes the scan
        //   order so repeated measurements of the same binary are
        //   comparable.
        // Checked in cheapest-first order so a high-cardinality caller (who
        // can never take the fast path at all, per the limit above) never
        // pays the O(tasks) `lookups_needed` scan either: `.then()`/
        // `.filter()` short-circuit, so `activity_durations.len()` (O(1)) is
        // read before `tasks` is walked, not after.
        let duration_lookup: Option<Vec<(&str, Duration)>> = (self.activity_durations.len()
            <= LINEAR_SCAN_CARDINALITY_LIMIT)
            .then(|| {
                tasks
                    .iter()
                    .filter(|task| task.start_to_close.is_none())
                    .count()
            })
            .filter(|&lookups_needed| lookups_needed >= MIN_LOOKUPS_TO_AMORTIZE_TABLE_BUILD)
            .map(|_| {
                let mut lookup: Vec<(&str, Duration)> = self
                    .activity_durations
                    .iter()
                    .map(|(name, &duration)| (name.as_str(), duration))
                    .collect();
                lookup.sort_unstable_by_key(|&(name, _)| name);
                lookup
            });

        for level in levels {
            for &task_index in level {
                let task = &tasks[task_index];
                let duration = task.start_to_close.unwrap_or_else(|| {
                    duration_lookup.as_ref().map_or_else(
                        || {
                            self.activity_durations
                                .get(&task.activity_name)
                                .copied()
                                .unwrap_or(self.default_duration)
                        },
                        |lookup| {
                            lookup
                                .iter()
                                .find(|(name, _)| *name == task.activity_name)
                                .map_or(self.default_duration, |&(_, duration)| duration)
                        },
                    )
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
