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

/// Cardinality bound for the linear duration scan in [`CriticalPathAnalyzer::analyze`].
///
/// At or below this many distinct `activity_durations` entries, each task's
/// duration comes from a linear scan of a flat slice, not a `HashMap` probe.
/// Above it, the analyzer uses the `HashMap`, because a scan costs up to
/// `O(tasks * names)`. The scan's call site records the measured crossover.
/// **The miss (`default_duration` fallback) case binds, not the hit case.** A
/// miss scans every entry, and a hit exits early on average. So the miss
/// crossover is lower. This limit respects it, so the scan is never worse than
/// the `HashMap` baseline for any caller, whatever its hit rate.
const LINEAR_SCAN_CARDINALITY_LIMIT: usize = 5;

/// Minimum count of name-based duration lookups before
/// [`CriticalPathAnalyzer::analyze`] builds the flat scan table.
///
/// The build is a `HashMap::iter()` walk, a heap allocation and a sort. With
/// only a few lookups, direct `HashMap::get` calls cost less. That is a
/// small DAG analyzed repeatedly, not the wide fan-out this targets. The
/// scan's call site records the measured crossover.
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
    /// The critical path is the longest path from any source node to any sink
    /// node. It sets the minimum possible execution time of the DAG.
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

        // Nanoseconds, not `Duration`: distances accumulate across levels.
        // Comparing already-saturated `Duration` values loses information.
        // Two branches that both overflow to `Duration::MAX` would then
        // tie. Predecessor selection would keep whichever was visited
        // first, instead of the true longest path (review finding on PR
        // #1711, `critical_path.rs`). `u128` nanoseconds holds the exact
        // sum with no realistic overflow risk. It takes roughly 1.8e10
        // chained `Duration::MAX` tasks to reach `u128::MAX`, and
        // `saturating_add` below covers even that case. Only the final
        // reported `total_duration` saturates down to `Duration`.
        let mut distances = vec![0u128; tasks.len()];
        let mut predecessors = vec![None; tasks.len()];
        // A node is a sink only when no other node names it as an upstream.
        // Sink detection folds into the main DP loop below, instead of
        // running as a second full pass over `tasks`/`task.upstreams`. The
        // DP loop already visits every task-upstream edge exactly once, in
        // the same order `for task in tasks { for &up_idx in
        // &task.upstreams }` would. So marking `is_sink[up_idx] = false`
        // there produces the identical result. Level order does not affect
        // which indices are named as an upstream, only when that fact is
        // discovered.
        let mut is_sink = vec![true; tasks.len()];

        // `activity_durations` is keyed by activity NAME, not by task. A wide
        // fan-out that reuses a few activity types looks up the SAME few keys
        // once per task. That is the case `mock_duration`'s doc example
        // shows. SipHash on each repeat `String` lookup dominates `analyze`
        // (see `benches/critical_path_profile.rs`).
        //
        // Measured crossover (standalone harness, `HashMap<String, Duration>`
        // vs `Vec<(&str, Duration)>`, 80,000 lookups, callgrind Ir), for BOTH
        // regimes `activity_durations` traffic can be:
        //
        // - All-hit (every task's name is mocked), `activity_shard_NNNN` keys.
        //   8 names: linear 15.2M vs hash 26.0M. 16 names: near parity (26.2M
        //   vs 26.0M). 24+ names: linear loses (36.3M vs 26.0M).
        // - All-miss (`default_duration` fallback). Keys share length and
        //   shape, so `str` equality cannot exit early on length. This is the
        //   honest worst case, because a miss walks every entry. 5 names:
        //   linear 17.8M vs hash 23.3M. Parity at 7 names (23.40M vs 23.41M).
        //   8+ names: linear loses (26.0M vs 23.5M).
        //
        // The miss crossover (~7) binds, not the hit crossover (~16). A miss
        // scans every entry. A caller that mocks only some activities pays
        // the miss cost on each unmocked task, and that is ordinary usage.
        // `LINEAR_SCAN_CARDINALITY_LIMIT` sits with margin below the miss
        // crossover. So the scan engages only where it wins under EITHER
        // regime, and the `HashMap` path serves every other case. Results
        // are identical either way: both structures hold each name at most
        // once.
        //
        // Building the table (a `HashMap::iter()` walk, a heap allocation and
        // a sort) has a fixed cost per `analyze()` call. Measured with the
        // standalone harness at cardinality 5, the maximum allowed. The
        // harness splits 80,000 lookups across `analyze()`-shaped calls, and
        // each lookup goes through the table:
        //
        // - 5 lookups per call: build+scan 23.6M Ir vs `HashMap` 19.2M (+23%,
        //   a REGRESSION).
        // - 8 lookups: parity (18.6M vs 19.1M).
        // - 10+ lookups: a clear, widening win (17.6M vs 19.1M).
        //
        // `MIN_LOOKUPS_TO_AMORTIZE_TABLE_BUILD` sits with margin above that
        // crossover.
        //
        // An EXACT count from a pre-pass over `tasks` makes this decision. A
        // running counter in the main loop cannot be correct. Whatever its
        // threshold, a DAG whose count lands just above it builds the table
        // with almost no lookups left. In the worst case the table serves one
        // lookup. The build costs about 8 `HashMap` lookups. So a build at
        // count 20 with 1-7 lookups left is a regression that no threshold
        // removes. With the exact count, ALL `lookups_needed` lookups use a
        // built table, which is the shape of the crossover measurement.
        //
        // The pre-pass is one O(tasks) scan of `Option::is_none()`, with no
        // hashing and no allocation. The cheapest gate runs first.
        // `tasks.len()` upper-bounds `lookups_needed`, because a set
        // `start_to_close` only LOWERS the count. So a DAG with fewer tasks
        // than the threshold skips the scan. An override-heavy large DAG
        // still pays O(tasks). That cost is bounded: one cheap check per task,
        // beside the per-task work of the DP loop below. It never dominates
        // `analyze` for a DAG large enough to matter.
        //
        // `HashMap::iter()` order is random per process (`RandomState`).
        // Unsorted, the scan's hit position and instruction counts would vary
        // run to run. That would undermine the deterministic-counter evidence
        // for this optimization. Sorting by name fixes the scan order.
        let duration_lookup: Option<Vec<(&str, Duration)>> = (self.activity_durations.len()
            <= LINEAR_SCAN_CARDINALITY_LIMIT
            && tasks.len() >= MIN_LOOKUPS_TO_AMORTIZE_TABLE_BUILD)
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

                let mut max_upstream_dist: u128 = 0;
                let mut best_pred = None;

                for &up_idx in &task.upstreams {
                    is_sink[up_idx] = false;
                    if distances[up_idx] >= max_upstream_dist {
                        // On a tie, the first predecessor found stays the best one.
                        if distances[up_idx] > max_upstream_dist || best_pred.is_none() {
                            max_upstream_dist = distances[up_idx];
                            best_pred = Some(up_idx);
                        }
                    }
                }

                distances[task_index] = max_upstream_dist.saturating_add(duration.as_nanos());
                predecessors[task_index] = best_pred;
            }
        }

        let mut max_dist: u128 = 0;
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
            total_duration: nanos_to_duration_saturating(max_dist),
            path_indices,
            path_names,
        }
    }
}

/// Convert an accumulated nanosecond total to a `Duration`, saturating at
/// `Duration::MAX` instead of overflowing. Only the final reported total
/// saturates; the DP loop above compares exact `u128` nanoseconds so a
/// saturated intermediate value can never corrupt the longest-path choice.
const fn nanos_to_duration_saturating(nanos: u128) -> Duration {
    const MAX_NANOS: u128 = u64::MAX as u128 * 1_000_000_000 + 999_999_999;
    if nanos >= MAX_NANOS {
        Duration::MAX
    } else {
        // The `nanos >= MAX_NANOS` guard above proves `nanos / 1_000_000_000`
        // fits in `u64`.
        #[allow(clippy::cast_possible_truncation)]
        let secs = (nanos / 1_000_000_000) as u64;
        let subsec_nanos = (nanos % 1_000_000_000) as u32;
        Duration::new(secs, subsec_nanos)
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
        assert_eq!(result.path_indices, [] as [usize; 0]);
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

    /// Snag boundary repro: a caller-supplied `start_to_close` of
    /// `Duration::MAX` on an upstream task used to panic. The DP loop added
    /// the next task's own duration on top and overflowed. `Duration::add`
    /// is unconditional, unlike bare integer `+`, which panics only when
    /// `overflow-checks` is on. `analyze` must degrade to a saturated total
    /// instead of crashing.
    #[test]
    fn test_start_to_close_max_duration_does_not_panic() {
        let mut builder = DagBuilder::new();
        let a = builder.activity(activity_a).start_to_close(Duration::MAX);
        let _b = builder
            .activity(activity_b)
            .start_to_close(Duration::from_secs(1))
            .upstream(&a);
        let dag = builder.build().unwrap();

        let analyzer = CriticalPathAnalyzer::new(dag);
        let result = analyzer.analyze();

        assert_eq!(
            result.total_duration,
            Duration::MAX,
            "saturated, not panicked"
        );
        assert_eq!(result.path_indices, vec![0, 1]);
    }

    fn activity_e() {}
    fn activity_f() {}

    /// Review finding on PR #1711 (`critical_path.rs:259`). Saturating each
    /// intermediate `Duration` before comparing can tie two branches with
    /// different true totals at `Duration::MAX`. Predecessor selection then
    /// keeps whichever was visited first, not the actually-longer branch.
    /// Two converging branches both overflow a `Duration` sum, but branch
    /// `mid2`'s exact nanosecond total is larger than `mid1`'s. `analyze`
    /// must still pick `mid2` as the critical path.
    #[test]
    fn test_saturated_branches_keep_correct_ordering() {
        let mut builder = DagBuilder::new();
        let root = builder.activity(activity_a).start_to_close(Duration::ZERO);

        let branch1 = builder
            .activity(activity_b)
            .start_to_close(Duration::MAX)
            .upstream(&root);
        let mid1 = builder
            .activity(activity_c)
            .start_to_close(Duration::from_secs(1))
            .upstream(&branch1);

        let branch2 = builder
            .activity(activity_d)
            .start_to_close(Duration::new(u64::MAX - 5, 0))
            .upstream(&root);
        let mid2 = builder
            .activity(activity_e)
            .start_to_close(Duration::from_secs(10))
            .upstream(&branch2);

        let _end = builder
            .activity(activity_f)
            .start_to_close(Duration::ZERO)
            .upstream(&mid1)
            .upstream(&mid2);

        let dag = builder.build().unwrap();
        let analyzer = CriticalPathAnalyzer::new(dag);
        let result = analyzer.analyze();

        assert_eq!(result.total_duration, Duration::MAX);
        assert_eq!(
            result.path_indices,
            vec![0, 3, 4, 5],
            "critical path must run through mid2 (branch2's descendant), \
             the branch with the larger exact total, not mid1"
        );
    }
}
